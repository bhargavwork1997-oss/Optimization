-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01b · Real source: Unity Catalog system tables (client workspace only)
-- MAGIC Use **instead of** `01_generate_logs` on a paid workspace where system tables are enabled
-- MAGIC (`system.billing`, `system.compute`, `system.lakeflow`). Produces the same column contract as the
-- MAGIC synthetic generator, so notebooks 02–04 run unchanged.
-- MAGIC
-- MAGIC **Validate against the client workspace before relying on it** — system table schemas evolve.
-- MAGIC Metrics with no direct system-table source are marked `-- APPROX` and documented in `docs/data-contract.md`.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.dpo.raw_usage_logs AS
WITH usage AS (
  SELECT
    u.usage_date,
    u.usage_metadata.job_id      AS job_id,
    u.usage_metadata.cluster_id  AS cluster_id,
    u.billing_origin_product     AS product,
    u.sku_name,
    SUM(u.usage_quantity)        AS dbus
  FROM system.billing.usage u
  WHERE u.usage_unit = 'DBU'
    AND u.usage_date >= date_sub(current_date(), 90)
  GROUP BY ALL
),
jobs AS (          -- latest name per job (SCD2 table)
  SELECT job_id, name
  FROM system.lakeflow.jobs
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
),
clusters AS (      -- latest config per cluster (SCD2 table)
  SELECT cluster_id, cluster_name, worker_node_type, worker_count, cluster_source
  FROM system.compute.clusters
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) = 1
),
cpu AS (           -- daily average CPU per cluster
  SELECT cluster_id, CAST(start_time AS DATE) AS usage_date,
         AVG(cpu_user_percent + cpu_system_percent) / 100 AS avg_cpu_util,
         COUNT_IF(cpu_user_percent + cpu_system_percent < 5) AS low_cpu_minutes   -- APPROX idle
  FROM system.compute.node_timeline
  WHERE start_time >= date_sub(current_date(), 90)
  GROUP BY ALL
)
SELECT
  u.usage_date,
  COALESCE(j.name, c.cluster_name, u.product)                      AS job_name,
  u.cluster_id,
  CASE
    WHEN u.product = 'JOBS'          THEN 'ETL_JOB'
    WHEN u.product = 'SQL'           THEN 'SQL'
    WHEN u.product IN ('DLT', 'LAKEFLOW_DECLARATIVE_PIPELINES') THEN 'DLT'
    WHEN u.product = 'INTERACTIVE'   THEN 'INTERACTIVE'
    ELSE u.product
  END                                                              AS workload_type,
  CASE
    WHEN u.sku_name LIKE '%SERVERLESS%'    THEN 'SERVERLESS_JOBS'
    WHEN u.sku_name LIKE '%ALL_PURPOSE%'   THEN 'ALL_PURPOSE_COMPUTE'
    WHEN u.sku_name LIKE '%JOBS%'          THEN 'JOBS_COMPUTE'
    WHEN u.sku_name LIKE '%SQL%'           THEN 'SQL_PRO'
    WHEN u.sku_name LIKE '%DLT%'           THEN 'DLT_ADVANCED'
    ELSE u.sku_name
  END                                                              AS sku,
  c.worker_node_type                                               AS node_type,
  c.worker_count                                                   AS num_workers,
  CAST(NULL AS DOUBLE)                                             AS runtime_hours,   -- APPROX: fill from system.lakeflow.job_run_timeline if needed
  ROUND(u.dbus, 2)                                                 AS dbus,
  ROUND(cpu.avg_cpu_util, 2)                                       AS avg_cpu_util,
  COALESCE(cpu.low_cpu_minutes, 0)                                 AS idle_minutes,
  CAST(0 AS DOUBLE)                                                AS spill_gb,        -- APPROX: SQL warehouses only, see system.query.history
  0                                                                AS failed_retries,  -- APPROX: count from system.lakeflow.job_run_timeline result_state
  CAST(NULL AS STRING)                                             AS query_text
FROM usage u
LEFT JOIN jobs j      ON u.job_id = j.job_id
LEFT JOIN clusters c  ON u.cluster_id = c.cluster_id
LEFT JOIN cpu         ON u.cluster_id = cpu.cluster_id AND u.usage_date = cpu.usage_date;

-- COMMAND ----------

SELECT workload_type, sku, COUNT(*) rows, ROUND(SUM(dbus), 1) dbus
FROM workspace.dpo.raw_usage_logs GROUP BY ALL ORDER BY dbus DESC;
