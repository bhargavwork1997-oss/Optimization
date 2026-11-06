# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Governance re-apply (admins)
# MAGIC
# MAGIC **Purpose.** Re-applies the role model: catalog and schemas, grants, ownership, classification tags, the admin-only
# MAGIC `security` schema, the append-only audit log and the secured `reporting` views; then checks grant drift and, in
# MAGIC production, revokes it. First-time installation: use **`00_install`** instead.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Who runs it** | enterprise: a member of `dpo-admins` or the admin service principal (the daily `dpo-governance` job). The pipeline identity cannot. Free Edition: you |
# MAGIC | **When** | daily (job); after the first pipeline run (creates the views); after changing a profile, groups or `security.access_map` |
# MAGIC | **Reads** | profile, current grants, system-table access |
# MAGIC | **Writes** | grants, ownership, tags, `reporting` views, `ops.governance_log`, `ops.governance_drift` (only with `apply`) |
# MAGIC | **How to run** | set `apply_governance` = `plan_only` to review the SQL, then `apply` → **Run all** |
# MAGIC
# MAGIC Safe to re-run: every statement is idempotent. Objects that do not exist yet are reported as SKIPPED, not errors.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared bootstrap
# MAGIC Loads the DPO package, detects the profile, creates the shared widgets and builds `cfg` (see `_common`).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Preflight
# MAGIC Read-only checks with a fix for every gap: catalog exists, system tables readable, groups exist, your rights,
# MAGIC risky settings (for example governance switched off with real telemetry).

# COMMAND ----------

dbutils.widgets.dropdown("apply_governance", "plan_only", ["plan_only", "apply"], "Governance")
from dpo import governance

display(
    spark.createDataFrame(governance.preflight(spark, cfg), "check string, status string, detail string, fix string")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 · Governance plan / apply
# MAGIC `plan_only` lists every statement the role model needs without running it. `apply` runs them, refuses to continue
# MAGIC if you are not an admin, writes each result to `ops.governance_log`, then compares `SHOW GRANTS` with the role
# MAGIC model (drift). In production, extra principals are revoked.

# COMMAND ----------

mode = dbutils.widgets.get("apply_governance")
result = governance.admin_apply(spark, cfg, run_id="setup", dry_run=(mode != "apply"))
display(
    spark.createDataFrame(
        result["rows"], "section string, purpose string, statement string, status string, error string"
    )
)
if result["drift"]:
    display(
        spark.createDataFrame(
            result["drift"], "schema_name string, principal string, privilege string, status string, error string"
        )
    )
errors = [r for r in result["rows"] if r[3] == "ERROR"]
print(
    f"{mode}: {len(result['rows'])} statements, {len(errors)} errors, "
    f"{sum(r[3] == 'SKIPPED' for r in result['rows'])} skipped (objects not created yet)"
)
