# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate synthetic telemetry
# MAGIC
# MAGIC **Purpose.** Creates realistic stand-ins for the Databricks system tables, so DPO can be built, demonstrated and
# MAGIC tested without any real (client) data. Every one of the 17 rules has an injected scenario, plus healthy controls
# MAGIC that must **not** be flagged and a `ground_truth` table the tests compare against.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Who runs it** | anyone who may run the pipeline (operators, admins); Free Edition: you |
# MAGIC | **When** | before the first pipeline run on a synthetic profile; again to change scale or seed |
# MAGIC | **Reads** | nothing (generated from a seed) |
# MAGIC | **Writes** | `<prefix>sim.*` (billing, list prices, jobs, runs, queries, clusters, node timeline, inventory, ground truth) |
# MAGIC | **On enterprise profiles** | exits immediately: they read the real system tables |
# MAGIC
# MAGIC | scale | instances per scenario | days | approx. rows |
# MAGIC |---|---|---|---|
# MAGIC | small | 1 | 30 | ~20 k |
# MAGIC | medium | 5 | 30 | ~100 k |
# MAGIC | large | 40 | 60 | ~1.6 M |
# MAGIC | xl | 200 | 90 | ~12 M (watch the Free Edition daily quota) |
# MAGIC
# MAGIC Widgets: `scale`, `seed` (same seed = same data), `chaos` (inject duplicates, nulls, negative DBUs, schema drift).
# MAGIC Free Edition is non-commercial: **never load client telemetry into it**; this generator is the way to use it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared bootstrap
# MAGIC Loads the DPO package, detects the profile, creates the shared widgets and builds `cfg` (see `_common`).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Generate
# MAGIC Skips on system-table profiles; otherwise writes the synthetic tables and prints a manifest (rows per table and
# MAGIC which defects were injected when `chaos` is on).

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

# MAGIC %md
# MAGIC ### 2 · What was injected
# MAGIC One row per scenario: the workload, the rule that should fire (or `none` for healthy controls).

# COMMAND ----------

display(spark.table(cfg.t("sim", "ground_truth")).orderBy("workload_type", "scenario"))
