-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Gold: what generates cost, and where to optimize
-- MAGIC Aggregates per job, assigns a **waste category** (why it's expensive) and an **optimization lever**
-- MAGIC (what to do about it), plus a conservative savings estimate.
-- MAGIC
-- MAGIC Thresholds live in the `rules` CTE so they can be tuned per client without touching the logic.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.dpo.cost_categorized
COMMENT 'Per-job 90-day cost with waste category, optimization lever and estimated savings'
AS
WITH rules AS (
  SELECT
    30    AS idle_min_threshold,      -- avg idle minutes/day on interactive clusters
    0.25  AS low_cpu_threshold,       -- avg CPU utilisation
    10.0  AS spill_gb_threshold,      -- avg spill per run
    0.5   AS retry_rate_threshold     -- avg failed retries per day
),
agg AS (
  SELECT
    job_name, workload_type, sku,
    ANY_VALUE(query_text)            AS query_text,
    COUNT(*)                         AS run_days,
    ROUND(SUM(cost_usd), 2)          AS cost_90d,
    ROUND(SUM(dbus), 1)              AS dbus_90d,
    ROUND(AVG(avg_cpu_util), 2)      AS cpu,
    ROUND(AVG(idle_minutes), 1)      AS idle_min,
    ROUND(AVG(spill_gb), 1)          AS spill,
    ROUND(AVG(failed_retries), 2)    AS retry_rate,
    ROUND(AVG(num_workers), 1)       AS workers
  FROM workspace.dpo.usage_cost
  GROUP BY ALL
),
categorized AS (
  SELECT a.*,
    ROUND(cost_90d / SUM(cost_90d) OVER () * 100, 1) AS pct_of_total,
    (sku = 'ALL_PURPOSE_COMPUTE' AND workload_type <> 'INTERACTIVE') AS wrong_compute_type,  -- move to job compute
    CASE
      WHEN workload_type = 'INTERACTIVE' AND idle_min > r.idle_min_threshold         THEN 'IDLE_WASTE'
      WHEN spill > r.spill_gb_threshold                                              THEN 'SQL_INEFFICIENT'
      WHEN cpu < r.low_cpu_threshold                                                 THEN 'OVERSIZED_CLUSTER'
      WHEN retry_rate > r.retry_rate_threshold                                       THEN 'FAILURE_RERUNS'
      WHEN sku = 'ALL_PURPOSE_COMPUTE' AND workload_type <> 'INTERACTIVE'            THEN 'WRONG_COMPUTE_TYPE'
      ELSE 'HEALTHY'
    END AS waste_category,
    CASE
      WHEN workload_type = 'INGEST'                                                         THEN 'REPLATFORM_CANDIDATE'
      WHEN spill > r.spill_gb_threshold
        OR query_text RLIKE '(?i)select \\*|select distinct \\*|cross join|year\\('        THEN 'OPTIMIZE_SQL'
      WHEN query_text RLIKE '(?i)^\\s*merge'                                                THEN 'DATA_MODEL_REVIEW'
      WHEN cpu < r.low_cpu_threshold OR idle_min > r.idle_min_threshold                     THEN 'RIGHTSIZE_AUTOTERMINATE'
      WHEN query_text RLIKE '(?i)where .*(dt|date)|group by'                                THEN 'PARTITION_OR_CLUSTER'
      ELSE 'NO_ACTION'
    END AS optimization_lever
  FROM agg a CROSS JOIN rules r
),
scored AS (
  -- Conservative, explainable savings assumptions (tune per client after the Optimization Lab)
  SELECT *,
    LEAST(0.7,
      CASE optimization_lever
        WHEN 'RIGHTSIZE_AUTOTERMINATE' THEN 0.35
        WHEN 'OPTIMIZE_SQL'            THEN 0.30
        WHEN 'REPLATFORM_CANDIDATE'    THEN 0.50
        WHEN 'PARTITION_OR_CLUSTER'    THEN 0.20
        WHEN 'DATA_MODEL_REVIEW'       THEN 0.15
        ELSE 0.0
      END
      + CASE WHEN wrong_compute_type THEN 0.20 ELSE 0.0 END) AS est_saving_pct
  FROM categorized
)
SELECT *, ROUND(cost_90d * est_saving_pct, 2) AS est_saving_usd_90d
FROM scored;

-- COMMAND ----------

SELECT job_name, workload_type, cost_90d, pct_of_total, waste_category, optimization_lever,
       est_saving_pct, est_saving_usd_90d
FROM workspace.dpo.cost_categorized
ORDER BY cost_90d DESC;

-- COMMAND ----------

-- Headline numbers for the client deck
SELECT
  ROUND(SUM(cost_90d), 0)                                   AS total_cost_90d,
  ROUND(SUM(est_saving_usd_90d), 0)                         AS est_saving_90d,
  ROUND(SUM(est_saving_usd_90d) / SUM(cost_90d) * 100, 1)   AS est_saving_pct,
  ROUND(SUM(est_saving_usd_90d) * 365 / 90, 0)              AS est_saving_annualized
FROM workspace.dpo.cost_categorized;
