# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Run the DPO pipeline
# MAGIC
# MAGIC **Purpose.** The daily analysis. Reads billing, job, query and compute telemetry and produces the scored
# MAGIC optimisation backlog, AI narratives and the budget outlook.
# MAGIC
# MAGIC ```
# MAGIC runtime guard + authorization ─► bronze ─► silver ─► gold ─► 17 rules ─► scored backlog ─► AI narratives ─► forecast ─► own cost ─► views
# MAGIC (right workspace, safe profile,   contracts  DQ,      cost        findings     S = 0.35V+0.25C   ai_query        ai_forecast   rebuilt
# MAGIC  operator or admin)               minimised  dedup,   attribution              +0.20F+0.20R      (cached)        + budgets
# MAGIC                                   SQL        pricing
# MAGIC ```
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Who runs it** | the `dpo-pipeline` job (pipeline service principal, member of `dpo-operators` only); operators or admins by hand |
# MAGIC | **When** | daily (06:30 IST by default); by hand after changing rules or budgets |
# MAGIC | **Reads** | `system.*` (enterprise) or `<prefix>sim.*` (synthetic) |
# MAGIC | **Writes** | bronze, silver, gold, ops (run log, DQ, rule status, audit log) |
# MAGIC | **Permissions** | refused before touching data if the caller is not an operator/admin, the profile is bound to another workspace, or real telemetry would land in an ungoverned schema; every decision is audited |
# MAGIC
# MAGIC Every run is a full recompute of the look-back window: safe to re-run, and re-running fixes a failed run.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared bootstrap
# MAGIC Loads the DPO package, detects the profile, creates the shared widgets and builds `cfg` (see `_common`).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Run all stages
# MAGIC Prints one row per stage: status, seconds, rows written and notes. The `narrate` row shows how many narratives were
# MAGIC AI-generated, cached or rejected; the `forecast` row shows whether `ai_forecast` or the baseline was used and why.

# COMMAND ----------

from dpo import pipeline
from pyspark.sql import functions as F

ctx = pipeline.run_all(spark, cfg)
display(spark.createDataFrame(ctx.log))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 · Source availability and data quality
# MAGIC `source_status`: which system tables were readable and complete. `dq_report`: rows in, out, quarantined and
# MAGIC de-duplicated per source. `rule_status`: findings and errors per rule.

# COMMAND ----------

display(spark.table(cfg.t("ops", "source_status")))
display(spark.table(cfg.t("ops", "dq_report")))
display(spark.table(cfg.t("ops", "rule_status")).orderBy("rule_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 · Prioritised backlog
# MAGIC Highest score first, with the evidence (metric vs threshold), the estimated saving (a hypothesis until validated)
# MAGIC and the narrative.

# COMMAND ----------

display(spark.sql(f"""
  SELECT r.rank, r.priority, r.score, r.title, r.workload_key,
         round(r.metric_value, 3) AS metric, r.threshold,
         round(r.est_monthly_savings, 2) AS est_monthly_savings, n.narrative
  FROM {cfg.t('gold', 'recommendations')} r
  JOIN {cfg.t('gold', 'recommendation_narratives')} n USING (finding_id)
  ORDER BY r.rank"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 · Budget outlook
# MAGIC Spend so far this month + forecast for the remaining days per cost centre, against the budgets in the
# MAGIC profile. `method` says whether Databricks `ai_forecast` or the weekday-average baseline was used.

# COMMAND ----------

if spark.catalog.tableExists(cfg.t("gold", "budget_outlook")):
    display(spark.table(cfg.t("gold", "budget_outlook")).orderBy("status", "cost_center"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 · What DPO itself costs
# MAGIC DPO's own jobs and the dashboard / Genie queries on DPO tables, found in the same billing data, against
# MAGIC the savings it has found. Billing lags a few hours, so the run that just finished appears next time.

# COMMAND ----------

if spark.catalog.tableExists(cfg.t("gold", "dpo_cost_summary")):
    display(spark.table(cfg.t("gold", "dpo_cost_summary")))
    display(spark.table(cfg.t("gold", "dpo_run_cost")).orderBy(F.col("start_time").desc()).limit(10))
