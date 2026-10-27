# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup, preflight and governance (admins)
# MAGIC 1. **Preflight** – catalog, system-table access, groups, your rights, risky settings; with the fix for each gap.
# MAGIC 2. **Governance plan** – every statement the role model needs (`plan_only`), or applied (`apply`).
# MAGIC
# MAGIC `apply` creates the catalog, schemas, admin-only `security` schema (access map, identity salt),
# MAGIC the append-only audit log, grants, ownership, classification tags and the secured `reporting` views,
# MAGIC then audits grant drift (revoking it in prod).
# MAGIC
# MAGIC **Who runs this:** on enterprise profiles, only a member of the admins group (or the admin service
# MAGIC principal). The pipeline identity cannot. Re-run after the first pipeline run so the reporting views
# MAGIC are created, and after any change to profiles or groups. Safe to re-run.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.dropdown("apply_governance", "plan_only", ["plan_only", "apply"], "Governance")
from dpo import governance

display(spark.createDataFrame(governance.preflight(spark, cfg), "check string, status string, detail string, fix string"))

# COMMAND ----------

# MAGIC %md ### Governance plan / apply

# COMMAND ----------

mode = dbutils.widgets.get("apply_governance")
result = governance.admin_apply(spark, cfg, run_id="setup", dry_run=(mode != "apply"))
display(spark.createDataFrame(result["rows"], "section string, purpose string, statement string, status string, error string"))
if result["drift"]:
    display(spark.createDataFrame(result["drift"],
                                  "schema_name string, principal string, privilege string, status string, error string"))
errors = [r for r in result["rows"] if r[3] == "ERROR"]
print(f"{mode}: {len(result['rows'])} statements, {len(errors)} errors, "
      f"{sum(r[3] == 'SKIPPED' for r in result['rows'])} skipped (objects not created yet)")
