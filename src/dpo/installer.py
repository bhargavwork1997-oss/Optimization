"""One-time installer: access gate -> ordered/parallel setup -> verification -> scheduled jobs.

The access gate runs first and changes nothing. If any BLOCKER is found it raises
`InstallBlocked` with every problem and its fix in one message, so an admin fixes them in one go.

Plan (dependencies decide order; independent steps run in parallel):

    gate ─► governance_base ─┬─► synthetic_data* ─► pipeline ─► viewer_layer ─► verify ─► schedule_jobs
                             ├─► optimization_lab (optional, parallel)
                             └─► regression_tests (optional, dev profiles only, parallel)
    * only for synthetic profiles
"""
from __future__ import annotations

import json
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Callable, Optional

from .config import DPOConfig, normalize_host

REQUIRED_SOURCES = ("system.billing.usage", "system.billing.list_prices",
                    "system.lakeflow.jobs", "system.lakeflow.job_run_timeline")
REQUIRED_SYSTEM_SCHEMAS = ("billing", "lakeflow")
OPTIONAL_SYSTEM_SCHEMAS = ("query", "compute")
GATE_SCHEMA = "check string, severity string, detail string, fix string"


class InstallBlocked(RuntimeError):
    """Raised by the access gate. Nothing has been changed when this is raised."""


# =============================================================== access gate ==
def _row(check, severity, detail, fix=""):
    return (check, severity, detail, fix)


def classify_preflight(cfg: DPOConfig, pre: list[tuple]) -> list[tuple]:
    """Map governance.preflight() statuses to OK / INFO / WARN / BLOCKER."""
    out = []
    for check, status, detail, fix in pre:
        sev = {"OK": "OK", "INFO": "INFO", "WARN": "WARN", "EMPTY": "WARN", "UNKNOWN": "WARN"}.get(status)
        if check == "admin rights":
            sev = "OK" if status == "OK" else "BLOCKER"
            fix = fix or (f"Ask an account admin to add you to `{cfg.group('admins')}`; installation creates "
                          f"schemas and grants, which only DPO admins may do")
        elif check == "run rights":
            continue                                           # implied by admin rights
        elif check.startswith("group "):
            sev = "OK" if status == "OK" else "BLOCKER"
        elif check == "catalog" and status == "MISSING":
            sev = "WARN"
            detail = f"{detail}; the installer will create it (needs CREATE CATALOG)"
        elif check.startswith("source "):
            table = check.split(" ", 1)[1]
            if status == "DENIED":
                sev = "BLOCKER" if table in REQUIRED_SOURCES else "WARN"
                if sev == "WARN":
                    detail += " (optional: rules using it stay silent)"
        out.append(_row(check, sev or "WARN", detail, fix))
    return out


def _grantees(spark, securable: str) -> set:
    """Principals with SELECT (or ALL) on a securable, lower-cased; empty set if not visible."""
    try:
        rows = spark.sql(f"SHOW GRANTS ON {securable}").collect()
    except Exception:
        return set()
    out = set()
    for r in rows:
        d = r.asDict()
        if str(d.get("ActionType", "")).upper() in ("SELECT", "ALL PRIVILEGES", "ALL_PRIVILEGES"):
            out.add(str(d.get("Principal", "")).lower())
    return out


def check_operator_system_access(spark, cfg: DPOConfig) -> list[tuple]:
    """The scheduled job runs as an operator: it must be able to read the system tables."""
    if cfg.source_mode != "system" or not cfg.governance["enforce"]:
        return []
    ops = cfg.group("operators").lower()
    if cfg.governance["grant_system_access"]:
        return [_row("operators_system_tables", "INFO", "grant_system_access=true: the installer grants it "
                     "(requires metastore admin)")]
    catalog_level = ops in _grantees(spark, "CATALOG system")
    out = []
    for sch in REQUIRED_SYSTEM_SCHEMAS + OPTIONAL_SYSTEM_SCHEMAS:
        ok = catalog_level or ops in _grantees(spark, f"SCHEMA system.{sch}")
        required = sch in REQUIRED_SYSTEM_SCHEMAS
        out.append(_row(f"operators_system_{sch}", "OK" if ok else ("BLOCKER" if required else "WARN"),
                        "SELECT granted" if ok else
                        f"no direct SELECT for `{cfg.group('operators')}`: the scheduled pipeline would fail"
                        + ("" if required else " to read this optional source"),
                        "" if ok else f"GRANT USE SCHEMA, SELECT ON SCHEMA system.{sch} TO `{cfg.group('operators')}` "
                        f"(if access comes through a parent group, list this check in accepted_checks)"))
    return out


def check_workspace_binding(cfg: DPOConfig, host: Optional[str]) -> list[tuple]:
    h = normalize_host(host)
    allowed = [normalize_host(x) for x in cfg.workspace_hosts]
    if allowed and h and h not in allowed:
        return [_row("workspace binding", "BLOCKER",
                     f"profile '{cfg.profile}' is bound to {', '.join(allowed)}, but this is {h}",
                     "Open the notebook in the right workspace, or add this host to workspace_hosts in the profile")]
    if allowed and not h:
        return [_row("workspace binding", "WARN", "could not read this workspace's host to confirm the binding")]
    if not allowed and cfg.governance["enforce"]:
        return [_row("workspace binding", "WARN", f"profile '{cfg.profile}' is not bound to a workspace",
                     f"Add \"workspace_hosts\": [\"{h or '<host>'}\"] to the profile so it cannot run elsewhere")]
    return [_row("workspace binding", "OK", f"{h or 'host unknown'} matches profile '{cfg.profile}'")]


def check_profile_fit(cfg: DPOConfig, host: Optional[str]) -> list[tuple]:
    out = []
    h = normalize_host(host) or ""
    if not cfg.governance["enforce"] and (h.endswith("azuredatabricks.net") or h.startswith("adb-")):
        out.append(_row("profile fit", "WARN", f"profile '{cfg.profile}' has no access control but this looks like an "
                        "enterprise workspace", "Use enterprise_dev or enterprise_prod here"))
    if not cfg.governance["enforce"] and cfg.source_mode == "system":
        out.append(_row("profile fit", "BLOCKER", "real system tables with governance off: anyone could read the results",
                        "Use an enterprise profile (governance.enforce = true) to analyse real telemetry"))
    return out


def check_service_principals(cfg: DPOConfig, run_as: str, admin_run_as: str, sdk=None) -> list[tuple]:
    """Pipeline SP must be an operator and NOT an admin; governance SP must be an admin."""
    out = []
    if not cfg.governance["enforce"]:
        return out
    if not run_as:
        out.append(_row("pipeline run-as", "WARN", "no service principal given: the scheduled pipeline will run as you",
                        f"Create an SP in `{cfg.group('operators')}` only and set pipeline_run_as"))
    if not admin_run_as:
        out.append(_row("governance run-as", "WARN", "no admin service principal given: the governance job will run as you",
                        f"Create an SP in `{cfg.group('admins')}` and set governance_run_as"))
    if not (run_as or admin_run_as):
        return out
    if sdk is None:
        out.append(_row("service principals", "WARN", "Databricks SDK not available: membership not verified"))
        return out

    def members(group_name):
        try:
            gs = list(sdk.groups.list(filter=f'displayName eq "{group_name}"', attributes="id,displayName,members"))
            return {m.value for g in gs for m in (g.members or [])}
        except Exception:
            return None

    def sp_id(app_id):
        try:
            sps = list(sdk.service_principals.list(filter=f'applicationId eq "{app_id}"'))
            return sps[0].id if sps else ""
        except Exception:
            return None

    ops, admins = members(cfg.group("operators")), members(cfg.group("admins"))
    for label, app, must, must_not in (("pipeline run-as", run_as, ("operators", ops), ("admins", admins)),
                                       ("governance run-as", admin_run_as, ("admins", admins), None)):
        if not app:
            continue
        sid = sp_id(app)
        if sid is None or must[1] is None:
            out.append(_row(label, "WARN", f"could not verify {app} (no permission to list groups or SPs)"))
            continue
        if sid == "":
            out.append(_row(label, "BLOCKER", f"service principal {app} not found in this workspace",
                            "Add the SP to the workspace (account console → workspace → permissions)"))
            continue
        if sid not in must[1]:
            out.append(_row(label, "BLOCKER", f"{app} is not a direct member of `{cfg.group(must[0])}`",
                            f"Add it to `{cfg.group(must[0])}` (nested membership is not verified)"))
            continue
        if must_not and must_not[1] is not None and sid in must_not[1]:
            out.append(_row(label, "BLOCKER", f"{app} is also in `{cfg.group(must_not[0])}`: the pipeline identity "
                            "could change who sees what", f"Remove it from `{cfg.group(must_not[0])}`"))
            continue
        out.append(_row(label, "OK", f"{app} is in `{cfg.group(must[0])}` only"))
    return out


def access_gate(spark, cfg: DPOConfig, host: Optional[str] = None, run_as: str = "", admin_run_as: str = "",
                sdk=None, accepted: tuple = ()) -> list[tuple]:
    from .governance import preflight
    rows = (check_workspace_binding(cfg, host) + check_profile_fit(cfg, host)
            + classify_preflight(cfg, preflight(spark, cfg))
            + check_operator_system_access(spark, cfg)
            + check_service_principals(cfg, run_as, admin_run_as, sdk))
    # an admin may explicitly accept a known false positive (e.g. access via a parent group)
    return [(c, "ACCEPTED" if s == "BLOCKER" and c in accepted else s, d, f) for c, s, d, f in rows]


def blockers(rows: list[tuple]) -> list[tuple]:
    return [r for r in rows if r[1] == "BLOCKER"]


def blocked_message(cfg: DPOConfig, rows: list[tuple]) -> str:
    b = blockers(rows)
    lines = [f"DPO install BLOCKED for profile '{cfg.profile}': {len(b)} access-control problem(s). "
             f"Nothing was created or changed.", ""]
    for i, (check, _, detail, fix) in enumerate(b, 1):
        lines.append(f"{i}. [{check}] {detail}")
        if fix:
            lines.append(f"   Fix: {fix}")
    lines += ["", "Fix these, then run the install notebook again (it is safe to re-run)."]
    return "\n".join(lines)


# ============================================================== step runner ==
@dataclass
class Step:
    key: str
    title: str
    fn: Callable[[], object]
    deps: tuple = ()
    critical: bool = True
    enabled: bool = True
    result: dict = field(default_factory=dict)


def run_plan(steps: list[Step], max_parallel: int = 3, on_event: Callable[[str], None] = print) -> list[dict]:
    """Run steps as soon as their dependencies succeed; independent steps run in parallel.

    A failed critical step stops scheduling new work (running steps finish); its dependents are
    SKIPPED. A failed optional step only skips its own dependents.
    """
    by_key = {s.key: s for s in steps}
    status = {s.key: ("DISABLED" if not s.enabled else "PENDING") for s in steps}
    detail, secs = {}, {}
    for s in steps:
        unknown = [d for d in s.deps if d not in by_key]
        if unknown:
            raise ValueError(f"step {s.key} depends on unknown steps {unknown}")
    stop = False
    running = {}

    def deps_state(s):
        st = [status[d] for d in s.deps]
        if any(x in ("FAILED", "SKIPPED", "BLOCKED") for x in st):
            return "dead"
        if all(x in ("SUCCESS", "DISABLED") for x in st):
            return "ready"
        return "wait"

    def timed(s):
        t = time.time()
        try:
            return "SUCCESS", s.fn(), time.time() - t
        except Exception as exc:   # noqa: BLE001 - reported, not swallowed
            return "FAILED", f"{type(exc).__name__}: {exc}", time.time() - t

    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        while True:
            for s in steps:
                if status[s.key] != "PENDING":
                    continue
                ds = deps_state(s)
                if ds == "dead" or (stop and ds != "wait"):
                    status[s.key] = "SKIPPED"
                    detail[s.key] = "a step it depends on failed" if ds == "dead" else "install stopped"
                    on_event(f"⏭  {s.title}: skipped ({detail[s.key]})")
                elif ds == "ready" and not stop:
                    status[s.key] = "RUNNING"
                    on_event(f"▶  {s.title}")
                    running[pool.submit(timed, s)] = s
            if stop:
                for s in steps:
                    if status[s.key] == "PENDING":
                        status[s.key], detail[s.key] = "SKIPPED", "install stopped"
            if not running:
                break
            done, _ = wait(list(running), return_when=FIRST_COMPLETED)
            for fut in done:
                s = running.pop(fut)
                st, res, t = fut.result()
                status[s.key], secs[s.key] = st, round(t, 1)
                detail[s.key] = res if isinstance(res, str) else json.dumps(res, default=str)[:1500]
                on_event(f"{'✔' if st == 'SUCCESS' else '✖'}  {s.title} ({secs[s.key]} s)"
                         + ("" if st == "SUCCESS" else f": {detail[s.key][:300]}"))
                if st == "FAILED" and s.critical:
                    stop = True
    return [{"step": s.key, "title": s.title, "status": status[s.key], "critical": s.critical,
             "seconds": secs.get(s.key), "detail": detail.get(s.key, "")} for s in steps]


# ================================================================ plan steps ==
def _strict_admin_apply(spark, cfg, run_id):
    from .governance import admin_apply
    res = admin_apply(spark, cfg, run_id=run_id)
    errors = [r for r in res["rows"] if r[3] == "ERROR"]
    if errors:
        first = "\n".join(f"- {r[2][:160]} -> {r[4][:200]}" for r in errors[:5])
        raise RuntimeError(f"{len(errors)} governance statement(s) failed:\n{first}")
    return {"statements": len(res["rows"]), "skipped": sum(r[3] == "SKIPPED" for r in res["rows"]),
            "drift": len(res["drift"])}


def verify(spark, cfg: DPOConfig) -> dict:
    from .governance import REPORTING_VIEWS, _exists
    problems = []
    st = {r.source: r.status for r in spark.table(cfg.t("ops", "source_status")).collect()}
    if cfg.source_mode == "system":
        for src in ("billing_usage", "list_prices", "jobs", "job_run_timeline"):
            if st.get(src) != "available":
                problems.append(f"required source {src} is {st.get(src)}")
    errs = spark.table(cfg.t("ops", "rule_status")).filter("status = 'ERROR'").count()
    if errs:
        problems.append(f"{errs} rule(s) errored (see ops.rule_status)")
    missing = [v for v in REPORTING_VIEWS if not _exists(spark, cfg.t("reporting", v))]
    if missing:
        problems.append(f"reporting views missing: {missing}")
    if problems:
        raise RuntimeError("; ".join(problems))
    n = spark.table(cfg.t("gold", "recommendations")).count()
    p1 = spark.table(cfg.t("gold", "recommendations")).filter("priority = 'P1'").count()
    return {"findings": n, "p1": p1, "sources": st}


def build_plan(spark, cfg: DPOConfig, run_id: str, run_notebook: Optional[Callable] = None,
               include_lab: bool = True, include_tests: Optional[bool] = None,
               schedule_fn: Optional[Callable] = None, regenerate_synthetic: bool = False) -> list[Step]:
    from . import pipeline, synth
    from .governance import _exists
    synthetic = cfg.source_mode == "synthetic"
    if include_tests is None:   # tests need to create dpo_test_* schemas: dev-type profiles only
        include_tests = (not cfg.governance["enforce"]) or bool(cfg.governance.get("operators_can_create_schemas"))

    def gen():
        if not regenerate_synthetic and _exists(spark, cfg.t("sim", "billing_usage")):
            return {"reused": "existing synthetic data"}
        return synth.generate(spark, cfg, scale="small", seed=42, end_date=cfg.end_date)["tables"]

    def run_pipe():
        ctx = pipeline.run_all(spark, cfg, run_id=run_id)
        return {l["stage"]: l["status"] for l in ctx.log}

    return [
        Step("governance_base", "Create catalog, schemas, security objects and grants",
             lambda: _strict_admin_apply(spark, cfg, f"{run_id}-base")),
        Step("synthetic_data", "Generate synthetic telemetry", gen, deps=("governance_base",), enabled=synthetic),
        Step("pipeline", "Run pipeline (bronze → silver → gold → rules → backlog)", run_pipe,
             deps=("governance_base", "synthetic_data")),
        Step("viewer_layer", "Build secured reporting views and apply final grants",
             lambda: _strict_admin_apply(spark, cfg, f"{run_id}-views"), deps=("pipeline",)),
        Step("optimization_lab", "Optimization lab (before/after proof)",
             lambda: run_notebook("./03_optimization_lab", {"profile": cfg.profile}, 3600),
             deps=("governance_base",), critical=False, enabled=include_lab and run_notebook is not None),
        Step("regression_tests", "Regression tests (isolated dpo_test_ schemas)",
             lambda: run_notebook("./04_regression_tests", {"profile": cfg.profile}, 3600),
             deps=("governance_base",), critical=False, enabled=include_tests and run_notebook is not None),
        Step("verify", "Verify sources, rules and reporting views", lambda: verify(spark, cfg),
             deps=("viewer_layer",)),
        Step("schedule_jobs", "Create or update scheduled jobs", schedule_fn or (lambda: None),
             deps=("verify",), critical=False, enabled=schedule_fn is not None),
    ]


# ============================================================ scheduled jobs ==
def job_specs(cfg: DPOConfig, notebook_dir: str, run_as: str = "", admin_run_as: str = "",
              active: bool = True) -> list[dict]:
    """Plain-dict job definitions (serverless: no cluster spec)."""
    pause = "UNPAUSED" if active else "PAUSED"
    enforce = cfg.governance["enforce"]
    acl_pipe = ([{"group_name": cfg.group("admins"), "permission_level": "CAN_MANAGE"},
                 {"group_name": cfg.group("operators"), "permission_level": "CAN_MANAGE_RUN"},
                 {"group_name": cfg.group("analysts"), "permission_level": "CAN_VIEW"}] if enforce else [])
    specs = [{
        "name": f"dpo-pipeline-{cfg.profile}",
        "tasks": [
            {"task_key": "generate_synthetic", "notebook_path": f"{notebook_dir}/01_generate_synthetic_data"},
            {"task_key": "run_pipeline", "notebook_path": f"{notebook_dir}/02_run_pipeline",
             "depends_on": ["generate_synthetic"]},
        ],
        "cron": "0 30 6 * * ?", "pause": pause, "run_as": run_as, "acl": acl_pipe,
    }]
    if enforce:
        specs.append({
            "name": f"dpo-governance-{cfg.profile}",
            "tasks": [{"task_key": "apply_governance", "notebook_path": f"{notebook_dir}/00_setup",
                       "base_parameters": {"apply_governance": "apply"}}],
            "cron": "0 0 8 * * ?", "pause": pause, "run_as": admin_run_as,
            "acl": [{"group_name": cfg.group("admins"), "permission_level": "CAN_MANAGE"}],
        })
    return specs


def _to_sdk(cfg: DPOConfig, spec: dict):
    from databricks.sdk.service import jobs as J
    tasks = [J.Task(task_key=t["task_key"],
                    notebook_task=J.NotebookTask(notebook_path=t["notebook_path"],
                                                 base_parameters=t.get("base_parameters")),
                    depends_on=[J.TaskDependency(task_key=d) for d in t.get("depends_on", [])] or None)
             for t in spec["tasks"]]
    settings = dict(
        name=spec["name"], tasks=tasks, max_concurrent_runs=1,
        parameters=[J.JobParameterDefinition(name="profile", default=cfg.profile)],
        schedule=J.CronSchedule(quartz_cron_expression=spec["cron"], timezone_id="Asia/Kolkata",
                                pause_status=J.PauseStatus[spec["pause"]]),
        tags={"app": "dpo-accelerator", "dpo_profile": cfg.profile, "deployed_by": "dpo-installer"},
        run_as=J.JobRunAs(service_principal_name=spec["run_as"]) if spec["run_as"] else None,
    )
    acl = [J.JobAccessControlRequest(group_name=a["group_name"],
                                     permission_level=J.JobPermissionLevel[a["permission_level"]])
           for a in spec["acl"]]
    return J, settings, acl


def bundle_managed_jobs(sdk, profile: str) -> list[dict]:
    """DPO jobs deployed by an asset bundle for this profile (dev-mode names carry a "[dev user]" prefix,
    so match on tags and deployment kind, not on name)."""
    found = []
    for j in sdk.jobs.list():
        s = getattr(j, "settings", None)
        tags = (getattr(s, "tags", None) or {}) if s else {}
        dep = getattr(s, "deployment", None) if s else None
        if tags.get("app") == "dpo-accelerator" and tags.get("dpo_profile") == profile and \
                dep is not None and str(getattr(dep, "kind", "")).endswith("BUNDLE"):
            found.append({"job": s.name, "action": "managed by asset bundle", "id": j.job_id})
    return found


def schedule_jobs(cfg: DPOConfig, sdk, notebook_dir: str, run_as: str = "", admin_run_as: str = "",
                  active: bool = True) -> list[dict]:
    """Create/update scheduled jobs for workspaces installed without the bundle. If the bundle already
    deployed DPO jobs for this profile, change nothing: the bundle is the source of truth."""
    managed = bundle_managed_jobs(sdk, cfg.profile)
    if managed:
        return managed
    out = []
    for spec in job_specs(cfg, notebook_dir, run_as, admin_run_as, active):
        J, settings, acl = _to_sdk(cfg, spec)
        existing = [j for j in sdk.jobs.list(name=spec["name"])]
        try:
            if existing:
                job_id = existing[0].job_id
                current = sdk.jobs.get(job_id).settings
                if current and current.deployment and str(current.deployment.kind).endswith("BUNDLE"):
                    out.append({"job": spec["name"], "action": "left alone (managed by asset bundle)", "id": job_id})
                    continue
                sdk.jobs.reset(job_id=job_id, new_settings=J.JobSettings(**settings))
                if acl:
                    sdk.jobs.update_permissions(job_id=str(job_id), access_control_list=acl)
                out.append({"job": spec["name"], "action": "updated", "id": job_id})
            else:
                job_id = sdk.jobs.create(access_control_list=acl or None, **settings).job_id
                out.append({"job": spec["name"], "action": "created", "id": job_id})
        except Exception as exc:
            hint = (" You need the 'Service principal: User' role on the run-as SP." if spec["run_as"] and
                    ("service" in str(exc).lower() or "permission" in str(exc).lower()) else "")
            raise RuntimeError(f"{spec['name']}: {str(exc).splitlines()[0][:300]}.{hint}") from exc
    return out
