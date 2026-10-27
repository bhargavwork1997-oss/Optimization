-- DPO Dashboard — datasets
-- In Databricks: Dashboards → Create dashboard → Data tab → "Create from SQL" for each block below,
-- named as in the header comment. Then build the widgets listed in docs/dashboard-guide.md.

-- ds_kpis  → 4 counter widgets
SELECT
  ROUND(SUM(cost_90d), 0)                                  AS total_cost_90d,
  ROUND(SUM(est_saving_usd_90d), 0)                        AS est_saving_90d,
  ROUND(SUM(est_saving_usd_90d) / SUM(cost_90d) * 100, 1)  AS est_saving_pct,
  ROUND(SUM(est_saving_usd_90d) * 365 / 90, 0)             AS est_saving_annualized
FROM workspace.dpo.cost_categorized;

-- ds_daily_cost  → line chart (x: usage_date, y: cost_usd, color: workload_type)
SELECT usage_date, workload_type, ROUND(SUM(cost_usd), 2) AS cost_usd
FROM workspace.dpo.usage_cost
GROUP BY ALL;

-- ds_by_waste  → bar chart (x: waste_category, y: cost_90d)
SELECT waste_category, ROUND(SUM(cost_90d), 2) AS cost_90d, COUNT(*) AS jobs
FROM workspace.dpo.cost_categorized
GROUP BY ALL;

-- ds_savings_by_lever  → bar chart (x: optimization_lever, y: est_saving_usd_90d)
SELECT optimization_lever, ROUND(SUM(est_saving_usd_90d), 2) AS est_saving_usd_90d
FROM workspace.dpo.cost_categorized
WHERE optimization_lever <> 'NO_ACTION'
GROUP BY ALL;

-- ds_top_jobs  → table
SELECT job_name, workload_type, sku, cost_90d, pct_of_total, cpu, idle_min, spill,
       waste_category, wrong_compute_type, optimization_lever, est_saving_usd_90d
FROM workspace.dpo.cost_categorized
ORDER BY cost_90d DESC
LIMIT 10;

-- ds_recommendations  → table
SELECT job_name, cost_90d, est_saving_usd_90d, waste_category, optimization_lever, source, recommendation
FROM workspace.dpo.ai_recommendations
ORDER BY cost_90d DESC;

-- ds_lab_results  → bar chart (x: experiment, y: before_sec & after_sec)
SELECT experiment, lever, before_sec, after_sec, improvement_pct
FROM workspace.dpo.lab_results
QUALIFY ROW_NUMBER() OVER (PARTITION BY experiment ORDER BY run_at DESC) = 1;
