# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Install DPO — run this once
# MAGIC **Click Run all.** The profile is detected from this workspace; you only fill the two run-as widgets on enterprise.
# MAGIC
# MAGIC | Stage | What happens | Runs |
# MAGIC |---|---|---|
# MAGIC | 1. Access gate | Who you are, workspace binding, groups, system-table access for you **and** for the operators group, service-principal membership, risky settings. **Stops here with one error listing every problem and its fix. Nothing is changed.** | first |
# MAGIC | 2. Governance base | catalog, schemas, admin-only security schema, append-only audit log, grants, tags | after 1 |
# MAGIC | 3. Synthetic data | only on synthetic profiles | after 2 |
# MAGIC | 4. Pipeline | bronze → silver → gold → 17 rules → scored backlog → narratives | after 3 |
# MAGIC | 5. Viewer layer | secured reporting views, ownership, grants on new tables | after 4 |
# MAGIC | 6. Optimization lab | before/after proof on `samples.tpch` | **parallel** with 3–5 |
# MAGIC | 7. Regression tests | isolated `dpo_test_*` schemas (dev profiles only) | **parallel** with 3–5 |
# MAGIC | 8. Verify | required sources available, no rule errors, reporting views exist | after 5 |
# MAGIC | 9. Schedule | daily pipeline job (+ governance job on enterprise), run-as service principals, group permissions | after 8 |
# MAGIC | 10. Dashboards | *DPO · Cost optimization* (reporting views) and *DPO · Engineering* (gold/ops), published without embedded credentials, group permissions | **parallel** with 9 |
# MAGIC
# MAGIC Safe to re-run: every step is idempotent. Set `mode = check_only` to run the access gate alone.

# COMMAND ----------

DPO_DEFER_CFG_ERRORS = True     # show profile errors as an install error below, not a stack trace

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import json
import uuid

from dpo import governance, installer
from dpo.installer import GATE_SCHEMA, InstallBlocked

dbutils.widgets.dropdown("mode", "install", ["install", "check_only"], "Mode")
dbutils.widgets.text("pipeline_run_as", "", "Pipeline SP application ID (enterprise)")
dbutils.widgets.text("governance_run_as", "", "Governance SP application ID (enterprise)")
dbutils.widgets.dropdown("include_lab", "yes", ["yes", "no"], "Run optimization lab")
dbutils.widgets.dropdown("schedule", "active", ["active", "paused", "none"], "Scheduled jobs")
dbutils.widgets.text("accepted_checks", "", "Accept blockers (comma list of check names)")
dbutils.widgets.dropdown("dashboards", "yes", ["yes", "no"], "Publish dashboards")
dbutils.widgets.text("warehouse", "", "SQL warehouse name for dashboards (blank = first serverless)")

MODE = dbutils.widgets.get("mode")
RUN_AS = dbutils.widgets.get("pipeline_run_as").strip()
ADMIN_RUN_AS = dbutils.widgets.get("governance_run_as").strip()
ACCEPTED = tuple(c.strip() for c in dbutils.widgets.get("accepted_checks").split(",") if c.strip())
RUN_ID = f"install-{uuid.uuid4().hex[:8]}"

if CFG_ERROR is not None:
    raise InstallBlocked(f"DPO install BLOCKED: profile '{dbutils.widgets.get('profile')}' is invalid. "
                         f"Nothing was changed.\n\n1. [profile] {CFG_ERROR}\n   Fix: correct the file in "
                         f"config/environments/, commit, pull the Git folder, run again.")
try:
    from databricks.sdk import WorkspaceClient
    SDK = WorkspaceClient()
except Exception:
    SDK = None

# COMMAND ----------

# MAGIC %md ## 1 · Access gate (changes nothing)

# COMMAND ----------

gate = installer.access_gate(spark, cfg, host=HOST, run_as=RUN_AS, admin_run_as=ADMIN_RUN_AS,
                             sdk=SDK, accepted=ACCEPTED)
order = {"BLOCKER": 0, "WARN": 1, "ACCEPTED": 2, "INFO": 3, "OK": 4}
display(spark.createDataFrame(sorted(gate, key=lambda r: order.get(r[1], 9)), GATE_SCHEMA))

if installer.blockers(gate):
    governance.record_audit(spark, cfg, RUN_ID, "install", "BLOCKED",
                            json.dumps([r[0] for r in installer.blockers(gate)]))
    raise InstallBlocked(installer.blocked_message(cfg, gate))

warns = [r for r in gate if r[1] == "WARN"]
print(f"Access gate passed for profile '{cfg.profile}'"
      + (f" with {len(warns)} warning(s): " + "; ".join(r[0] for r in warns) if warns else "."))
if MODE == "check_only":
    dbutils.notebook.exit("check_only: access gate passed; nothing installed")

# COMMAND ----------

# MAGIC %md ## 2–9 · Install (ordered and parallel)

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
        schedule_fn = lambda: installer.schedule_jobs(cfg, SDK, NOTEBOOK_DIR, RUN_AS, ADMIN_RUN_AS,
                                                      active=dbutils.widgets.get("schedule") == "active")

dashboard_fn = None
if dbutils.widgets.get("dashboards") == "yes":
    if SDK is None or NOTEBOOK_DIR is None:
        print("Dashboards skipped: Databricks SDK or notebook path unavailable; deploy the asset bundle instead.")
    else:
        from dpo import dashboard
        # outside the Git folder, so publishing never dirties the repo
        DASH_DIR = f"/Workspace/Users/{SDK.current_user.me().user_name}/dpo-dashboards-{cfg.profile}"
        dashboard_fn = lambda: dashboard.publish_dashboards(cfg, SDK, REPO_ROOT, DASH_DIR,
                                                            dbutils.widgets.get("warehouse").strip())

steps = installer.build_plan(
    spark, cfg, RUN_ID,
    run_notebook=lambda path, args, timeout: dbutils.notebook.run(path, timeout, args),
    include_lab=dbutils.widgets.get("include_lab") == "yes",
    schedule_fn=schedule_fn,
    dashboard_fn=dashboard_fn,
)
results = installer.run_plan(steps, max_parallel=3)

failed_critical = [r for r in results if r["status"] in ("FAILED",) and r["critical"]]
failed_optional = [r for r in results if r["status"] == "FAILED" and not r["critical"]]
governance.record_audit(spark, cfg, RUN_ID, "install",
                        "FAILED" if failed_critical else ("PARTIAL" if failed_optional else "SUCCESS"),
                        json.dumps({r["step"]: r["status"] for r in results}))
display(spark.createDataFrame([(r["title"], r["status"], r["critical"], r["seconds"], r["detail"][:1000])
                               for r in results],
                              "step string, status string, critical boolean, seconds double, detail string"))
if failed_critical:
    msg = "\n".join(f"- {r['title']}: {r['detail'][:600]}" for r in failed_critical)
    raise RuntimeError(f"DPO install FAILED at a required step (later steps skipped). Fix and run again:\n{msg}")

# COMMAND ----------

# MAGIC %md ## Result

# COMMAND ----------

summary = next(r for r in results if r["step"] == "verify")["detail"]
print(f"DPO installed for profile '{cfg.profile}'. {summary}")
for r in failed_optional:
    print(f"Optional step needs attention: {r['title']}: {r['detail'][:300]}")
display(spark.sql(f"""
  SELECT r.priority, r.score, r.title, r.workload_key, round(r.est_monthly_savings, 2) AS est_monthly_savings,
         n.narrative
  FROM {cfg.t('gold', 'recommendations')} r JOIN {cfg.t('gold', 'recommendation_narratives')} n USING (finding_id)
  ORDER BY r.score DESC LIMIT 25"""))
