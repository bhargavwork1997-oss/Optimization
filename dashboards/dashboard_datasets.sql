-- DPO Dashboard — datasets
-- Dashboards -> Create dashboard -> Data -> "Create from SQL" for each block, named as in its header.
-- Defaults: Free Edition (catalog `workspace`, prefix `dpo_`). Enterprise prod: replace `workspace.dpo_` with `dpo.`.
--
-- GOVERNANCE
--   Page "Cost overview" (datasets 1-7) reads the `reporting` views, which filter rows by cost centre,
--   hash identities and hide account-wide totals per viewer.
--   Publish WITHOUT embedded credentials ("individual data permissions") so each viewer's own access
--   applies. Embedded credentials make every viewer see data with the publisher's rights.
--   Page "Engineering" (datasets 8-11) reads gold/ops directly: share it with dpo-analysts only.

-- 1 ds_kpis -> counters
SELECT
  round(sum(window_cost), 2)                                         AS window_cost,
  round(sum(monthly_cost), 2)                                        AS run_rate_30d,
  round(sum(untagged_cost) / nullif(sum(window_cost), 0) * 100, 1)  AS untagged_pct,
  (SELECT count(*) FROM workspace.dpo_reporting.recommendations WHERE priority = 'P1') AS p1_findings,
  (SELECT round(sum(est_monthly_savings), 2) FROM workspace.dpo_reporting.recommendations) AS est_monthly_savings_unvalidated
FROM workspace.dpo_reporting.workload_summary;

-- 2 ds_cost_trend -> stacked area (x usage_date, y cost, colour billing_origin_product)
SELECT usage_date, billing_origin_product, round(sum(cost), 2) AS cost
FROM workspace.dpo_reporting.cost_by_dimension
GROUP BY ALL ORDER BY usage_date;

-- 3 ds_top_workloads -> bar
SELECT workload_key, workload_type, cost_center, round(monthly_cost, 2) AS monthly_cost, attribution_types, active_days
FROM workspace.dpo_reporting.workload_summary
ORDER BY monthly_cost DESC LIMIT 20;

-- 4 ds_cost_by_owner -> table (run_as is hashed for viewers)
SELECT cost_center, run_as, round(sum(cost), 2) AS cost
FROM workspace.dpo_reporting.cost_by_dimension
GROUP BY ALL ORDER BY cost DESC LIMIT 50;

-- 5 ds_backlog -> table
SELECT r.priority, r.score, r.category, r.title, r.workload_key, r.cost_center,
       round(r.metric_value, 3) AS metric_value, r.threshold,
       round(r.est_monthly_savings, 2) AS est_monthly_savings, r.status, n.narrative, n.narrative_source
FROM workspace.dpo_reporting.recommendations r
JOIN workspace.dpo_reporting.recommendation_narratives n USING (finding_id)
ORDER BY r.score DESC;

-- 6 ds_value_effort -> scatter (x feasibility, y value, size savings)
SELECT title, workload_key, V AS value_score, F AS feasibility, R AS reversibility,
       coalesce(est_monthly_savings, 0) AS est_monthly_savings, priority
FROM workspace.dpo_reporting.recommendations;

-- 7 ds_job_scorecard -> table
SELECT job_name, cost_center, runs, failed, round(failure_rate * 100, 1) AS failure_pct,
       p50_duration_s, p95_duration_s, round(monthly_cost, 2) AS monthly_cost,
       round(cost_per_success, 2) AS cost_per_success
FROM workspace.dpo_reporting.job_scorecard ORDER BY monthly_cost DESC NULLS LAST;

-- ======================= Engineering page (dpo-analysts only) =======================

-- 8 ds_query_shapes -> table
SELECT fingerprint, statement_shape, executions, round(monthly_cost, 2) AS monthly_cost,
       round(avg_duration_ms / 1000, 1) AS avg_s, round(avg_read_bytes / power(1024, 3), 2) AS avg_read_gb,
       round(cache_hit_rate, 2) AS cache_hit_rate, round(spill_share, 2) AS spill_share
FROM workspace.dpo_gold.query_fingerprint_stats ORDER BY monthly_cost DESC LIMIT 25;

-- 9 ds_lab_results -> grouped bar (x experiment, y before_sec and after_sec)
SELECT experiment, rule_ids, before_sec, after_sec, improvement_pct
FROM workspace.dpo_gold.lab_results
QUALIFY row_number() OVER (PARTITION BY experiment ORDER BY run_at DESC) = 1;

-- 10 ds_pipeline_health -> table
SELECT run_id, stage, status, seconds, rows_out, message, run_ts
FROM workspace.dpo_ops.run_log ORDER BY run_ts DESC, stage LIMIT 100;

-- 11 ds_data_quality -> table
SELECT source, rows_in, rows_out, rows_quarantined, duplicates_removed, reasons, run_ts
FROM workspace.dpo_ops.dq_report;
