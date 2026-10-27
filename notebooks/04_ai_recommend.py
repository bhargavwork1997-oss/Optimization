# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · AI recommendations
# MAGIC For the top non-healthy cost drivers, asks an LLM (via `ai_query` on a Databricks Foundation Model
# MAGIC endpoint) for root cause, concrete fix, estimated saving and risk.
# MAGIC
# MAGIC If the endpoint isn't available (quota, region, Free Edition limits), it **falls back to rule-based
# MAGIC recommendations** so the pipeline and dashboard still work. The `source` column says which was used.

# COMMAND ----------

dbutils.widgets.text("model_endpoint", "databricks-meta-llama-3-3-70b-instruct", "Model serving endpoint")
dbutils.widgets.text("top_n", "10", "Jobs to analyse (keep small on Free Edition)")
ENDPOINT = dbutils.widgets.get("model_endpoint")
TOP_N = int(dbutils.widgets.get("top_n"))
SRC = "workspace.dpo.cost_categorized"
TARGET = "workspace.dpo.ai_recommendations"

# COMMAND ----------

PROMPT_SQL = """
CONCAT(
  'You are a senior Databricks cost-optimization architect. Analyse this workload and answer in at most 120 words ',
  'with exactly four labelled lines: Root cause:, Fix:, Estimated saving:, Risk:. ',
  'If SQL is provided and is inefficient, include a rewritten query in the Fix line. ',
  'Consider: SQL optimization, data model changes, partitioning/liquid clustering, right-sizing/auto-termination, ',
  'job vs all-purpose compute, and replatforming simple pipelines to native Azure services (ADF, Functions). ',
  'Job: ', job_name,
  '. Workload: ', workload_type,
  '. SKU: ', sku,
  '. 90-day cost USD: ', cost_90d,
  '. Avg CPU: ', cpu,
  '. Avg idle min/day: ', idle_min,
  '. Avg spill GB: ', spill,
  '. Avg failed retries/day: ', retry_rate,
  '. Avg workers: ', workers,
  '. Detected issue: ', waste_category,
  '. Scheduled job on all-purpose compute: ', CAST(wrong_compute_type AS STRING),
  '. Suggested lever: ', optimization_lever,
  '. SQL: ', COALESCE(query_text, 'n/a')
)
"""

FALLBACK_SQL = """
CASE optimization_lever
  WHEN 'RIGHTSIZE_AUTOTERMINATE' THEN 'Root cause: cluster is oversized or left idle. Fix: set auto-termination to 10-15 min, reduce workers, enable autoscaling. Estimated saving: 30-40%. Risk: low.'
  WHEN 'OPTIMIZE_SQL'            THEN 'Root cause: inefficient SQL (SELECT *, non-sargable filters, cross joins, heavy spill). Fix: project only needed columns, rewrite year(col)=X as a range filter, replace CROSS JOIN with an equi-join, check the query profile for skew. Estimated saving: 25-35%. Risk: medium, validate results.'
  WHEN 'REPLATFORM_CANDIDATE'    THEN 'Root cause: simple copy/ingest work running on Spark. Fix: move to an ADF Copy activity or Azure Function writing to ADLS; keep Databricks for transformations. Estimated saving: 40-60%. Risk: medium, new platform to operate.'
  WHEN 'PARTITION_OR_CLUSTER'    THEN 'Root cause: date-filtered queries scan the full table. Fix: apply liquid clustering (CLUSTER BY date column) and run OPTIMIZE. Estimated saving: 15-25%. Risk: low.'
  WHEN 'DATA_MODEL_REVIEW'       THEN 'Root cause: MERGE over a large unclustered target, frequent retries. Fix: cluster the target on the merge key, enable deletion vectors, pre-filter the source to changed rows. Estimated saving: 10-20%. Risk: medium.'
  ELSE 'Root cause: no clear inefficiency. Fix: monitor. Estimated saving: 0%. Risk: none.'
END
"""

BASE = f"""
SELECT job_name, workload_type, sku, cost_90d, est_saving_usd_90d, waste_category, optimization_lever,
       cpu, idle_min, spill, retry_rate, workers, wrong_compute_type, query_text
FROM {SRC}
WHERE waste_category <> 'HEALTHY' OR optimization_lever <> 'NO_ACTION'
ORDER BY cost_90d DESC
LIMIT {TOP_N}
"""

# COMMAND ----------

def build(use_ai: bool):
    rec = f"ai_query('{ENDPOINT}', {PROMPT_SQL})" if use_ai else FALLBACK_SQL
    source = "'AI'" if use_ai else "'RULES'"
    return spark.sql(f"""
        SELECT job_name, workload_type, cost_90d, est_saving_usd_90d, waste_category, optimization_lever,
               {rec} AS recommendation, {source} AS source, current_timestamp() AS generated_at
        FROM ({BASE})
    """)

try:
    result = build(use_ai=True)
    result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)
    print(f"AI recommendations written using endpoint {ENDPOINT}")
except Exception as e:
    print(f"ai_query unavailable ({type(e).__name__}: {str(e)[:200]}). Falling back to rule-based recommendations.")
    build(use_ai=False).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)

# COMMAND ----------

display(spark.table(TARGET).orderBy("cost_90d", ascending=False))
