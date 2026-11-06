# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Install DPO (run once; safe to re-run)
# MAGIC
# MAGIC **Purpose.** One notebook that takes an empty workspace to a working DPO installation: it checks access first,
# MAGIC then creates everything in the right order, running independent steps in parallel.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Who runs it** | Free Edition: you. Enterprise: a member of `dpo-admins` (or the `dpo_install` bundle job, which runs as the admin service principal) |
# MAGIC | **When** | first install; after changing a profile, groups or rules |
# MAGIC | **Reads** | the profile, system tables (enterprise) or synthetic data (Free Edition) |
# MAGIC | **Writes** | DPO catalog/schemas, grants, reporting views, audit log; optionally jobs, dashboards, Genie spaces |
# MAGIC | **How to run** | open → check the widgets at the top → **Run all** |
# MAGIC
# MAGIC | Stage | What happens | Runs |
# MAGIC |---|---|---|
# MAGIC | 1. Access gate | who you are, workspace binding, groups, system-table access for you **and** for the operators group, service-principal membership, AI endpoint and SQL warehouse readiness, risky settings. **Stops here with one error listing every problem and its fix. Nothing is changed.** | first |
# MAGIC | 2. Governance base | catalog, schemas, admin-only security schema, append-only audit log, grants, tags | after 1 |
# MAGIC | 3. Synthetic data | only on synthetic profiles | after 2 |
# MAGIC | 4. Pipeline | bronze → silver → gold → 17 rules → scored backlog → AI narratives → forecast and budget outlook | after 3 |
# MAGIC | 5. Viewer layer | secured reporting views, ownership, grants on new tables | after 4 |
# MAGIC | 6. Optimization lab | before/after proof on `samples.tpch` | **parallel** with 3–5 |
# MAGIC | 7. Regression tests | isolated `dpo_test_*` schemas (dev profiles only) | **parallel** with 3–5 |
# MAGIC | 8. Verify | required sources available, no rule errors, reporting views exist | after 5 |
# MAGIC | 9. Schedule | daily pipeline job (+ governance job on enterprise), run-as service principals, group permissions | after 8 |
# MAGIC | 10. Dashboards | *DPO · Cost optimization* (reporting views) and *DPO · Engineering* (gold/ops), no embedded credentials | **parallel** with 9 |
# MAGIC | 11. Genie spaces | *DPO · Ask cost optimization* (reporting views, no value sampling) and *DPO · Ask engineering* (gold/ops) | **parallel** with 9–10 |
# MAGIC
# MAGIC **Widgets.** `mode` (`install` or `check_only` = gate only) · `pipeline_run_as` / `governance_run_as` (service
# MAGIC principal **application IDs**, enterprise) · `include_lab` · `schedule` (`active`, `paused`, or `none` when the bundle
# MAGIC owns the jobs) · `dashboards` · `genie` · `warehouse` · `accepted_checks` (admins only: accept a known false-positive
# MAGIC blocker by name; the acceptance is written to the audit log).
# MAGIC
# MAGIC Every step is idempotent: re-running fixes a failed install.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show profile errors as an install message
# MAGIC Tells `_common` not to raise on an invalid profile, so the error is reported below as a clear install message.

# COMMAND ----------

DPO_DEFER_CFG_ERRORS = True  # show profile errors as an install error below, not a stack trace

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared bootstrap
# MAGIC Loads the DPO package, detects the profile, creates the shared widgets and builds `cfg` (see `_common`).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installer widgets and SDK
# MAGIC Reads the installer's own widgets, stops with a clear message if the profile is invalid, and opens a Databricks SDK
# MAGIC client (used for service-principal checks, jobs, dashboards and Genie). Without the SDK those optional steps are skipped.

# COMMAND ----------

import json
import uuid

from dpo import governance, installer
from dpo.installer import GATE_SCHEMA, InstallBlockedError

dbutils.widgets.dropdown("mode", "install", ["install", "check_only"], "Mode")
dbutils.widgets.text("pipeline_run_as", "", "Pipeline SP application ID (enterprise)")
dbutils.widgets.text("governance_run_as", "", "Governance SP application ID (enterprise)")
dbutils.widgets.dropdown("include_lab", "yes", ["yes", "no"], "Run optimization lab")
dbutils.widgets.dropdown("schedule", "active", ["active", "paused", "none"], "Scheduled jobs")
dbutils.widgets.text("accepted_checks", "", "Accept blockers (comma list of check names)")
dbutils.widgets.dropdown("dashboards", "yes", ["yes", "no"], "Publish dashboards")
dbutils.widgets.dropdown("genie", "yes", ["yes", "no"], "Create Genie spaces")
dbutils.widgets.text("warehouse", "", "SQL warehouse for dashboards and Genie (blank = first serverless)")

MODE = dbutils.widgets.get("mode")
RUN_AS = dbutils.widgets.get("pipeline_run_as").strip()
ADMIN_RUN_AS = dbutils.widgets.get("governance_run_as").strip()
ACCEPTED = tuple(c.strip() for c in dbutils.widgets.get("accepted_checks").split(",") if c.strip())
RUN_ID = f"install-{uuid.uuid4().hex[:8]}"

if CFG_ERROR is not None:
    raise InstallBlockedError(
        f"DPO install BLOCKED: profile '{dbutils.widgets.get('profile')}' is invalid. "
        f"Nothing was changed.\n\n1. [profile] {CFG_ERROR}\n   Fix: correct the file in "
        f"config/environments/, commit, pull the Git folder, run again."
    )
try:
    from databricks.sdk import WorkspaceClient

    SDK = WorkspaceClient()
except Exception:
    SDK = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Access gate (changes nothing)
# MAGIC Runs every check, shows them worst-first, and **raises one error listing all blockers with their fixes**. A blocked
# MAGIC attempt is written to the audit log. Warnings do not stop the install (for example: AI endpoint not ready, which
# MAGIC only means narratives use templates until it is). `check_only` stops here.

# COMMAND ----------

gate = installer.access_gate(
    spark, cfg, host=HOST, run_as=RUN_AS, admin_run_as=ADMIN_RUN_AS, sdk=SDK, accepted=ACCEPTED
)
order = {"BLOCKER": 0, "WARN": 1, "ACCEPTED": 2, "INFO": 3, "OK": 4}
display(spark.createDataFrame(sorted(gate, key=lambda r: order.get(r[1], 9)), GATE_SCHEMA))

if installer.blockers(gate):
    governance.record_audit(
        spark, cfg, RUN_ID, "install", "BLOCKED", json.dumps([r[0] for r in installer.blockers(gate)])
    )
    raise InstallBlockedError(installer.blocked_message(cfg, gate))

accepted = [r[0] for r in gate if r[1] == "ACCEPTED"]
if accepted:  # an admin overrode a blocker: keep a tamper-evident record of who and what
    governance.record_audit(spark, cfg, RUN_ID, "install", "BLOCKER_ACCEPTED", json.dumps(accepted))
warns = [r for r in gate if r[1] == "WARN"]
print(
    f"Access gate passed for profile '{cfg.profile}'"
    + (f" with {len(warns)} warning(s): " + "; ".join(r[0] for r in warns) if warns else ".")
)
if MODE == "check_only":
    dbutils.notebook.exit("check_only: access gate passed; nothing installed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2–11 · Install (ordered and parallel)
# MAGIC Builds the step plan and runs it: a step starts as soon as the steps it depends on have succeeded, up to three at a
# MAGIC time. A failed **required** step stops new work and skips its dependents; a failed **optional** step (lab, tests,
# MAGIC jobs, dashboards, Genie) is reported but does not fail the install. Dashboards and Genie spaces are published to
# MAGIC your user folder, never into the Git folder.

# COMMAND ----------

NOTEBOOK_DIR = None
try:
    p = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    NOTEBOOK_DIR = p.rsplit("/", 1)[0]
    if not NOTEBOOK_DIR.startswith("/Workspace"):
        NOTEBOOK_DIR = "/Workspace" + NOTEBOOK_DIR
except Exception:
    pass

schedule_fn = None
if dbutils.widgets.get("schedule") != "none":
    if SDK is None or NOTEBOOK_DIR is None:
        print("Scheduling skipped: Databricks SDK or notebook path unavailable; deploy the asset bundle instead.")
    else:

        def schedule_fn():
            return installer.schedule_jobs(
                cfg, SDK, NOTEBOOK_DIR, RUN_AS, ADMIN_RUN_AS, active=dbutils.widgets.get("schedule") == "active"
            )


dashboard_fn = None
if dbutils.widgets.get("dashboards") == "yes":
    if SDK is None or NOTEBOOK_DIR is None:
        print("Dashboards skipped: Databricks SDK or notebook path unavailable; deploy the asset bundle instead.")
    else:
        from dpo import dashboard

        # outside the Git folder, so publishing never dirties the repo
        DASH_DIR = f"/Workspace/Users/{SDK.current_user.me().user_name}/dpo-dashboards-{cfg.profile}"

        def dashboard_fn():
            return dashboard.publish_dashboards(cfg, SDK, REPO_ROOT, DASH_DIR, dbutils.widgets.get("warehouse").strip())


genie_fn = None
if dbutils.widgets.get("genie") == "yes":
    if SDK is None or NOTEBOOK_DIR is None:
        print("Genie spaces skipped: Databricks SDK or notebook path unavailable; deploy the asset bundle instead.")
    else:
        from dpo import genie

        GENIE_DIR = f"/Workspace/Users/{SDK.current_user.me().user_name}/dpo-genie-{cfg.profile}"

        def genie_fn():
            return genie.publish_genie_spaces(cfg, SDK, REPO_ROOT, GENIE_DIR, dbutils.widgets.get("warehouse").strip())


steps = installer.build_plan(
    spark,
    cfg,
    RUN_ID,
    run_notebook=lambda path, args, timeout: dbutils.notebook.run(path, timeout, args),
    include_lab=dbutils.widgets.get("include_lab") == "yes",
    schedule_fn=schedule_fn,
    dashboard_fn=dashboard_fn,
    genie_fn=genie_fn,
)
results = installer.run_plan(steps, max_parallel=3)

failed_critical = [r for r in results if r["status"] in ("FAILED",) and r["critical"]]
failed_optional = [r for r in results if r["status"] == "FAILED" and not r["critical"]]
governance.record_audit(
    spark,
    cfg,
    RUN_ID,
    "install",
    "FAILED" if failed_critical else ("PARTIAL" if failed_optional else "SUCCESS"),
    json.dumps({r["step"]: r["status"] for r in results}),
)
display(
    spark.createDataFrame(
        [(r["title"], r["status"], r["critical"], r["seconds"], r["detail"][:1000]) for r in results],
        "step string, status string, critical boolean, seconds double, detail string",
    )
)
if failed_critical:
    msg = "\n".join(f"- {r['title']}: {r['detail'][:600]}" for r in failed_critical)
    raise RuntimeError(f"DPO install FAILED at a required step (later steps skipped). Fix and run again:\n{msg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC Summary from the verify step, any optional step that needs attention, and the top 25 recommendations.

# COMMAND ----------

summary = next(r for r in results if r["step"] == "verify")["detail"]
print(f"DPO installed for profile '{cfg.profile}'. {summary}")
for r in failed_optional:
    print(f"Optional step needs attention: {r['title']}: {r['detail'][:300]}")
display(
    spark.sql(f"""
  SELECT r.priority, r.score, r.title, r.workload_key, round(r.est_monthly_savings, 2) AS est_monthly_savings,
         n.narrative
  FROM {cfg.t("gold", "recommendations")} r JOIN {cfg.t("gold", "recommendation_narratives")} n USING (finding_id)
  ORDER BY r.score DESC LIMIT 25""")
)
