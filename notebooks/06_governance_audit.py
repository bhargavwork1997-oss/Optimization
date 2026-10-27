# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Governance audit
# MAGIC For security reviews: who can access DPO data, who ran it, and whether anything drifted from the role model.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from dpo import governance
from dpo.config import LAYERS

# COMMAND ----------

# MAGIC %md ### 1. Current grants on every DPO schema

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

# MAGIC %md ### 2. Drift against the role model (read-only here; revocation happens in the pipeline if the profile allows)

# COMMAND ----------

display(spark.createDataFrame(governance.audit_drift(spark, cfg, "audit", revoke=False),
                              "schema_name string, principal string, privilege string, status string, error string"))

# COMMAND ----------

# MAGIC %md ### 3. Who ran DPO, and who was refused

# COMMAND ----------

display(spark.sql(f"""
  SELECT logged_at, principal, profile, action, status, detail, run_id
  FROM {cfg.t('ops', 'audit_log')} ORDER BY logged_at DESC LIMIT 200"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Who read DPO tables (Unity Catalog audit log)
# MAGIC Needs `SELECT` on `system.access.audit` (account admins grant it).

# COMMAND ----------

try:
    display(spark.sql(f"""
      SELECT event_time, user_identity.email AS principal, action_name,
             request_params.full_name_arg AS object_name, response.status_code
      FROM system.access.audit
      WHERE event_date >= current_date() - INTERVAL 30 DAYS
        AND service_name = 'unityCatalog'
        AND request_params.full_name_arg LIKE '{cfg.catalog}.{cfg.schema_prefix}%'
      ORDER BY event_time DESC LIMIT 500"""))
except Exception as e:
    print("system.access.audit not readable:", str(e).split("\n")[0])

# COMMAND ----------

# MAGIC %md ### 5. Row-level access map (admins maintain this)

# COMMAND ----------

display(spark.table(cfg.t("security", "access_map")))
# Admins only (operators, analysts and viewers have no access to the security schema):
# INSERT INTO <catalog>.<prefix>security.access_map VALUES ('lead@corp.com', 'cc-finance', current_user(), current_timestamp())

# COMMAND ----------

# MAGIC %md ### 6. Audit-log integrity
# MAGIC `ops.audit_log` is append-only (`delta.appendOnly`) and owned by admins. Any DELETE/UPDATE attempt fails;
# MAGIC table history should show only WRITE/CREATE operations.

# COMMAND ----------

try:
    display(spark.sql(f"SHOW TBLPROPERTIES {cfg.t('ops', 'audit_log')} ('delta.appendOnly')"))
    display(spark.sql(f"DESCRIBE HISTORY {cfg.t('ops', 'audit_log')}")
            .select("version", "timestamp", "userName", "operation").limit(50))
except Exception as e:
    print("audit log not available:", str(e).split("\n")[0])
