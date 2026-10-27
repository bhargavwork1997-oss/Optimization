# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate synthetic telemetry
# MAGIC Writes `<prefix>sim.*` tables that mirror the Databricks system tables, with injected
# MAGIC anomaly scenarios (17), healthy controls and a `ground_truth` table.
# MAGIC
# MAGIC | scale | instances per scenario | days | approx. rows |
# MAGIC |---|---|---|---|
# MAGIC | small | 1 | 30 | ~20 k |
# MAGIC | medium | 5 | 30 | ~100 k |
# MAGIC | large | 40 | 60 | ~1.6 M |
# MAGIC | xl | 200 | 90 | ~12 M (watch the Free Edition daily quota) |

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

if cfg.source_mode != "synthetic":
    dbutils.notebook.exit(f"profile '{cfg.profile}' reads real system tables; synthetic generation skipped")

dbutils.widgets.dropdown("scale", "small", ["small", "medium", "large", "xl"], "Scale")
dbutils.widgets.text("seed", "42", "Seed")
dbutils.widgets.dropdown("chaos", "false", ["false", "true"], "Inject data defects")

from dpo import synth
import json

manifest = synth.generate(
    spark, cfg,
    scale=dbutils.widgets.get("scale"),
    seed=int(dbutils.widgets.get("seed")),
    end_date=cfg.end_date,
    chaos=dbutils.widgets.get("chaos") == "true",
)
print(json.dumps(manifest, indent=2))

# COMMAND ----------

display(spark.table(cfg.t("sim", "ground_truth")).orderBy("workload_type", "scenario"))
