# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Run the DPO pipeline
# MAGIC bronze → silver → gold → rules → recommendations → narratives.
# MAGIC Full recompute of the look-back window: safe to re-run, and re-running fixes a failed run.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from dpo import pipeline

ctx = pipeline.run_all(spark, cfg)
display(spark.createDataFrame(ctx.log))

# COMMAND ----------

# MAGIC %md ### Source availability and data quality

# COMMAND ----------

display(spark.table(cfg.t("ops", "source_status")))
display(spark.table(cfg.t("ops", "dq_report")))
display(spark.table(cfg.t("ops", "rule_status")).orderBy("rule_id"))

# COMMAND ----------

# MAGIC %md ### Prioritised backlog

# COMMAND ----------

display(spark.sql(f"""
  SELECT r.rank, r.priority, r.score, r.title, r.workload_key,
         round(r.metric_value, 3) AS metric, r.threshold,
         round(r.est_monthly_savings, 2) AS est_monthly_savings, n.narrative
  FROM {cfg.t('gold', 'recommendations')} r
  JOIN {cfg.t('gold', 'recommendation_narratives')} n USING (finding_id)
  ORDER BY r.rank"""))
