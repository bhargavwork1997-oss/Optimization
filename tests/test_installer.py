"""Installer: step ordering/parallelism, access gate, job specs, and end-to-end install runs."""
import copy
import threading
import time
from types import SimpleNamespace as NS

import pytest

from conftest import local_cfg

ADMINS, OPS = "dpo-admins", "dpo-operators"


# ------------------------------------------------------------------ run_plan --
def _rec(log, key, secs=0.0, fail=False):
    def fn():
        log.append(("start", key, time.time()))
        time.sleep(secs)
        log.append(("end", key, time.time()))
        if fail:
            raise RuntimeError(f"{key} broke")
        return {"ok": key}
    return fn


def test_dependencies_order_and_parallel_branches():
    from dpo.installer import Step, run_plan
    log = []
    steps = [Step("a", "A", _rec(log, "a", .05)),
             Step("b", "B", _rec(log, "b", .3), deps=("a",)),
             Step("c", "C", _rec(log, "c", .3), deps=("a",)),
             Step("d", "D", _rec(log, "d"), deps=("b", "c"))]
    res = run_plan(steps, on_event=lambda m: None)
    assert [r["status"] for r in res] == ["SUCCESS"] * 4
    t = {(e, k): ts for e, k, ts in log}
    assert t["start", "b"] >= t["end", "a"] and t["start", "c"] >= t["end", "a"]
    assert t["start", "b"] < t["end", "c"] and t["start", "c"] < t["end", "b"]      # b and c overlapped
    assert t["start", "d"] >= max(t["end", "b"], t["end", "c"])


def test_critical_failure_stops_install_but_lets_running_steps_finish():
    from dpo.installer import Step, run_plan
    log = []
    steps = [Step("base", "base", _rec(log, "base")),
             Step("pipe", "pipe", _rec(log, "pipe", .05, fail=True), deps=("base",)),
             Step("lab", "lab", _rec(log, "lab", .3), deps=("base",), critical=False),
             Step("views", "views", _rec(log, "views"), deps=("pipe",)),
             Step("later", "later", _rec(log, "later"), deps=("lab",))]
    res = {r["step"]: r for r in run_plan(steps, on_event=lambda m: None)}
    assert res["pipe"]["status"] == "FAILED" and "pipe broke" in res["pipe"]["detail"]
    assert res["lab"]["status"] == "SUCCESS"                 # already running: allowed to finish
    assert res["views"]["status"] == "SKIPPED" and res["later"]["status"] == "SKIPPED"


def test_optional_failure_only_skips_its_dependents_and_disabled_counts_as_done():
    from dpo.installer import Step, run_plan
    log = []
    steps = [Step("base", "base", _rec(log, "base")),
             Step("off", "off", _rec(log, "off"), deps=("base",), enabled=False),
             Step("lab", "lab", _rec(log, "lab", fail=True), deps=("base",), critical=False),
             Step("after_lab", "after_lab", _rec(log, "x"), deps=("lab",)),
             Step("pipe", "pipe", _rec(log, "pipe"), deps=("base", "off"))]
    res = {r["step"]: r["status"] for r in run_plan(steps, on_event=lambda m: None)}
    assert res == {"base": "SUCCESS", "off": "DISABLED", "lab": "FAILED", "after_lab": "SKIPPED", "pipe": "SUCCESS"}


def test_unknown_dependency_rejected():
    from dpo.installer import Step, run_plan
    with pytest.raises(ValueError, match="unknown steps"):
        run_plan([Step("a", "a", lambda: 1, deps=("nope",))], on_event=lambda m: None)


# --------------------------------------------------------------- access gate --
def test_classify_preflight_and_single_error_message():
    from dpo.config import DPOConfig
    from dpo.installer import blocked_message, blockers, classify_preflight
    cfg = DPOConfig.from_profile("enterprise_prod")
    pre = [("identity", "INFO", "running as me@corp.com", ""),
           ("catalog", "MISSING", "dpo not found", "CREATE CATALOG dpo"),
           ("source system.billing.usage", "DENIED", "no access", "GRANT ..."),
           ("source system.query.history", "DENIED", "no access", "GRANT ..."),
           ("source system.compute.clusters", "EMPTY", "readable", ""),
           ("group admins", "MISSING", "dpo-admins", "Create account group"),
           ("admin rights", "NO", "me@corp.com is not a member of dpo-admins", ""),
           ("run rights", "NO", "x", "")]
    rows = {r[0]: r[1] for r in classify_preflight(cfg, pre)}
    assert rows == {"identity": "INFO", "catalog": "WARN", "source system.billing.usage": "BLOCKER",
                    "source system.query.history": "WARN", "source system.compute.clusters": "WARN",
                    "group admins": "BLOCKER", "admin rights": "BLOCKER"}
    msg = blocked_message(cfg, classify_preflight(cfg, pre))
    assert "3 access-control problem(s)" in msg and "Nothing was created or changed" in msg
    assert "Fix: Ask an account admin to add you to `dpo-admins`" in msg
    assert len(blockers(classify_preflight(cfg, pre))) == 3


def test_workspace_binding_and_profile_fit():
    from dpo.config import DPOConfig
    from dpo.installer import check_profile_fit, check_workspace_binding
    cfg = DPOConfig.from_profile("enterprise_prod", {"workspace_hosts": ["adb-1.1.azuredatabricks.net"]})
    assert check_workspace_binding(cfg, "https://adb-1.1.azuredatabricks.net/?o=1")[0][1] == "OK"
    assert check_workspace_binding(cfg, "adb-2.2.azuredatabricks.net")[0][1] == "BLOCKER"
    free = DPOConfig.from_profile("free_edition", {"source_mode": "system"})
    sev = {r[1] for r in check_profile_fit(free, "adb-2.2.azuredatabricks.net")}
    assert sev == {"WARN", "BLOCKER"}


def _fake_sdk(ops_members, admin_members, sps):
    def groups_list(filter, attributes):
        name = filter.split('"')[1]
        ids = {OPS: ops_members, ADMINS: admin_members}.get(name, [])
        return [NS(members=[NS(value=i) for i in ids])]

    def sps_list(filter):
        app = filter.split('"')[1]
        return [NS(id=sps[app])] if app in sps else []
    return NS(groups=NS(list=groups_list), service_principals=NS(list=sps_list))


def test_service_principal_least_privilege_checks():
    from dpo.config import DPOConfig
    from dpo.installer import check_service_principals
    cfg = DPOConfig.from_profile("enterprise_prod")
    sdk = _fake_sdk(ops_members=["1", "2"], admin_members=["2", "9"], sps={"app-ok": "1", "app-both": "2", "app-adm": "9"})
    sev = lambda r: {x[0]: (x[1], x[2]) for x in r}
    ok = sev(check_service_principals(cfg, "app-ok", "app-adm", sdk))
    assert ok["pipeline run-as"][0] == "OK" and ok["governance run-as"][0] == "OK"
    both = sev(check_service_principals(cfg, "app-both", "app-ok", sdk))
    assert both["pipeline run-as"][0] == "BLOCKER" and "also in `dpo-admins`" in both["pipeline run-as"][1]
    assert both["governance run-as"][0] == "BLOCKER"                        # operator SP is not an admin
    missing = sev(check_service_principals(cfg, "app-ghost", "", sdk))
    assert missing["pipeline run-as"][0] == "BLOCKER" and missing["governance run-as"][0] == "WARN"


# --------------------------------------------------------------- job creation --
class _FakeJobs:
    def __init__(self, existing=None):
        self.existing, self.calls = existing or {}, []

    def list(self, name):
        return [NS(job_id=self.existing[name][0])] if name in self.existing else []

    def get(self, job_id):
        kind = next(v[1] for v in self.existing.values() if v[0] == job_id)
        return NS(settings=NS(deployment=NS(kind=kind) if kind else None))

    def create(self, **kw):
        self.calls.append(("create", kw["name"], kw))
        return NS(job_id=100 + len(self.calls))

    def reset(self, job_id, new_settings):
        self.calls.append(("reset", new_settings.name, job_id))

    def update_permissions(self, job_id, access_control_list):
        self.calls.append(("perms", job_id, access_control_list))


def test_job_specs_use_real_sdk_types_and_least_privilege_acl():
    pytest.importorskip("databricks.sdk")
    from dpo.config import DPOConfig
    from dpo.installer import _to_sdk, job_specs
    cfg = DPOConfig.from_profile("enterprise_prod")
    specs = job_specs(cfg, "/Workspace/Repos/x/notebooks", run_as="app-op", admin_run_as="app-adm")
    assert [s["name"] for s in specs] == ["dpo-pipeline-enterprise_prod", "dpo-governance-enterprise_prod"]
    _, pipe, acl = _to_sdk(cfg, specs[0])
    assert pipe["run_as"].service_principal_name == "app-op"
    assert [t.task_key for t in pipe["tasks"]] == ["generate_synthetic", "run_pipeline"]
    assert {(a.group_name, a.permission_level.value) for a in acl} == {
        ("dpo-admins", "CAN_MANAGE"), ("dpo-operators", "CAN_MANAGE_RUN"), ("dpo-analysts", "CAN_VIEW")}
    _, gov, gacl = _to_sdk(cfg, specs[1])
    assert gov["run_as"].service_principal_name == "app-adm"
    assert [(a.group_name, a.permission_level.value) for a in gacl] == [("dpo-admins", "CAN_MANAGE")]
    free = DPOConfig.from_profile("free_edition")
    assert [s["name"] for s in job_specs(free, "/x")] == ["dpo-pipeline-free_edition"]


def test_schedule_jobs_create_update_and_respect_bundle():
    pytest.importorskip("databricks.sdk")
    from dpo.config import DPOConfig
    from dpo.installer import schedule_jobs
    cfg = DPOConfig.from_profile("enterprise_prod")
    fake = _FakeJobs(existing={"dpo-pipeline-enterprise_prod": (7, "JobDeploymentKind.BUNDLE"),
                               "dpo-governance-enterprise_prod": (8, None)})
    out = schedule_jobs(cfg, NS(jobs=fake), "/Workspace/x", "app-op", "app-adm")
    assert [o["action"] for o in out] == ["left alone (managed by asset bundle)", "updated"]
    assert [c[0] for c in fake.calls] == ["reset", "perms"]
    fresh = _FakeJobs()
    out = schedule_jobs(cfg, NS(jobs=fresh), "/Workspace/x")
    assert [o["action"] for o in out] == ["created", "created"]


# ------------------------------------------------------- end-to-end installs --
def test_full_install_free_edition_style(spark):
    from dpo import installer
    cfg = local_cfg(prefix="inst_")
    events = []
    res = installer.run_plan(installer.build_plan(spark, cfg, "inst-1"), on_event=events.append)
    st = {r["step"]: r["status"] for r in res}
    assert st == {"governance_base": "SUCCESS", "synthetic_data": "SUCCESS", "pipeline": "SUCCESS",
                  "viewer_layer": "SUCCESS", "optimization_lab": "DISABLED", "regression_tests": "DISABLED",
                  "verify": "SUCCESS", "schedule_jobs": "DISABLED"}
    verify = next(r for r in res if r["step"] == "verify")["detail"]
    assert '"findings": 17' in verify
    # idempotent: a second install reuses data and succeeds
    res2 = installer.run_plan(installer.build_plan(spark, cfg, "inst-2"), on_event=lambda m: None)
    assert all(r["status"] in ("SUCCESS", "DISABLED") for r in res2)
    assert "reused" in next(r for r in res2 if r["step"] == "synthetic_data")["detail"]


def _enforced(base, groups, user):
    cfg = copy.deepcopy(base)
    cfg.governance.update(enforce=True, simulate_user=user, simulate_membership={g: True for g in groups})
    return cfg


def test_gate_blocks_non_admin_and_changes_nothing(spark):
    from dpo import installer
    cfg = _enforced(local_cfg(prefix="instgate_"), [OPS], "op@corp.com")
    rows = installer.access_gate(spark, cfg, host=None)
    b = installer.blockers(rows)
    assert [r[0] for r in b] == ["admin rights"]
    msg = installer.blocked_message(cfg, rows)
    assert "op@corp.com is not a member of dpo-admins" in msg and "Nothing was created" in msg
    assert not [d for d in spark.sql("SHOW DATABASES").collect() if d[0].startswith("instgate_")]
    # an explicitly accepted check is downgraded, never silently ignored
    acc = installer.access_gate(spark, cfg, accepted=("admin rights",))
    assert ("admin rights", "ACCEPTED") in [(r[0], r[1]) for r in acc]


def test_full_install_enforced_as_admin_builds_secured_views(spark):
    from dpo import installer
    cfg = _enforced(local_cfg(prefix="instenf_"), [ADMINS], "admin@corp.com")
    cfg.governance["cost_center_groups"] = {"finance-cost-owners": ["cc-finance"]}
    assert not installer.blockers(installer.access_gate(spark, cfg))
    res = installer.run_plan(installer.build_plan(spark, cfg, "inst-enf"), on_event=lambda m: None)
    assert all(r["status"] in ("SUCCESS", "DISABLED") for r in res), res
    names = {r.tableName for r in spark.sql("SHOW TABLES IN instenf_reporting").collect()}
    assert "recommendations" in names
    audit = [(r.action, r.status) for r in spark.table("instenf_ops.audit_log").collect()]
    assert ("admin_apply", "APPLIED") in audit and ("pipeline_run", "SUCCESS") in audit
