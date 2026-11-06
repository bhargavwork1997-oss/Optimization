# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Governance audit
# MAGIC
# MAGIC **Purpose.** Evidence for a security review: who can access DPO data, who ran DPO (and who was refused), who read
# MAGIC it, whether grants drifted from the role model, and whether the audit log is intact. Read-only.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Who runs it** | `dpo-admins` (sections 4 and 5 need admin rights; others show an error message instead of data) |
# MAGIC | **When** | monthly review; before and after an access change; on request of security/compliance |
# MAGIC | **Reads** | `SHOW GRANTS`, `ops.audit_log`, `system.access.audit`, `security.access_map`, audit-log table history |
# MAGIC | **Writes** | nothing (drift is reported, not revoked, here) |
# MAGIC
# MAGIC Review checklist: remove leavers from the DPO groups · confirm every principal in section 1 is a DPO group ·
# MAGIC investigate DENIED rows in section 3 and every `BLOCKER_ACCEPTED` · re-certify `security.access_map` rows ·
# MAGIC confirm the audit-log history shows only WRITE/CREATE operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared bootstrap
# MAGIC Loads the DPO package, detects the profile, creates the shared widgets and builds `cfg` (see `_common`).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from dpo import governance
from dpo.config import LAYERS

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Current grants on every DPO schema
# MAGIC Every principal and privilege per DPO schema. Expected: only the four DPO groups (and the service principals
# MAGIC through them).

# COMMAND ----------

rows = []
for layer in LAYERS:
    try:
        for r in spark.sql(f"SHOW GRANTS ON SCHEMA {cfg.schema(layer)}").collect():
            d = r.asDict()
            rows.append((cfg.schema(layer), d.get("Principal"), d.get("ActionType"), d.get("ObjectType")))
    except Exception as e:
        rows.append((cfg.schema(layer), None, str(e).split("\n")[0][:200], None))
display(spark.createDataFrame(rows, "schema_name string, principal string, privilege string, object_type string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 · Drift against the role model
# MAGIC Grants that exist but are not in the role model. Read-only here; the governance job revokes them in production
# MAGIC (`revoke_drift`).

# COMMAND ----------

display(
    spark.createDataFrame(
        governance.audit_drift(spark, cfg, "audit", revoke=False),
        "schema_name string, principal string, privilege string, status string, error string",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 · Who ran DPO, and who was refused
# MAGIC Latest 200 rows of the append-only audit log: authorisations (ALLOWED/DENIED), pipeline runs, installs and accepted
# MAGIC blockers.

# COMMAND ----------

display(
    spark.sql(f"""
  SELECT logged_at, principal, profile, action, status, detail, run_id
  FROM {cfg.t("ops", "audit_log")} ORDER BY logged_at DESC LIMIT 200""")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 · Who read DPO tables (Unity Catalog audit log)
# MAGIC Reads of DPO objects in the last 30 days from `system.access.audit`. Needs SELECT on that table (account admins grant it).

# COMMAND ----------

try:
    display(
        spark.sql(f"""
      SELECT event_time, user_identity.email AS principal, action_name,
             request_params.full_name_arg AS object_name, response.status_code
      FROM system.access.audit
      WHERE event_date >= current_date() - INTERVAL 30 DAYS
        AND service_name = 'unityCatalog'
        AND request_params.full_name_arg LIKE '{cfg.catalog}.{cfg.schema_prefix}%'
      ORDER BY event_time DESC LIMIT 500""")
    )
except Exception as e:
    print("system.access.audit not readable:", str(e).split("\n")[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 · Row-level access map
# MAGIC Individual users granted extra cost centres on top of the group mapping in the profile. Re-certify monthly.

# COMMAND ----------

display(spark.table(cfg.t("security", "access_map")))
# Admins only (operators, analysts and viewers have no access to the security schema):
# INSERT INTO <catalog>.<prefix>security.access_map VALUES ('lead@corp.com', 'cc-finance', current_user(), current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6 · Audit-log integrity
# MAGIC `ops.audit_log` is append-only (`delta.appendOnly`) and owned by admins: DELETE/UPDATE fail. The table history should
# MAGIC show only WRITE/CREATE operations.

# COMMAND ----------

try:
    display(spark.sql(f"SHOW TBLPROPERTIES {cfg.t('ops', 'audit_log')} ('delta.appendOnly')"))
    display(
        spark.sql(f"DESCRIBE HISTORY {cfg.t('ops', 'audit_log')}")
        .select("version", "timestamp", "userName", "operation")
        .limit(50)
    )
except Exception as e:
    print("audit log not available:", str(e).split("\n")[0])
