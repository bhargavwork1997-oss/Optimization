# Dashboard guide

Build after notebooks 00–05 have run once.

1. **Dashboards → Create dashboard**, name it `DPO – Cost Optimization`.
2. **Data** tab → for each block in `dashboards/dashboard_datasets.sql`, click **Create from SQL**, paste it, and
   rename the dataset to the name in its header comment (`ds_kpis`, `ds_daily_cost`, ...).
3. **Canvas** tab → add widgets:

| Row | Widget | Dataset | Config |
|---|---|---|---|
| 1 | Counter ×4 | `ds_kpis` | total_cost_90d · est_saving_90d · est_saving_pct · est_saving_annualized |
| 2 | Line | `ds_daily_cost` | x `usage_date`, y `SUM(cost_usd)`, color `workload_type` |
| 3 | Bar | `ds_by_waste` | x `waste_category`, y `cost_90d` |
| 3 | Bar | `ds_savings_by_lever` | x `optimization_lever`, y `est_saving_usd_90d` |
| 4 | Table | `ds_top_jobs` | all columns |
| 5 | Table | `ds_recommendations` | widen `recommendation` column |
| 6 | Bar (grouped) | `ds_lab_results` | x `experiment`, y `before_sec` and `after_sec` |

4. **Publish**. Add a schedule (dashboard ⋮ → Schedule) to refresh after the daily job.
5. Export: dashboard ⋮ → **Export** → save the `.lvdash.json` into `dashboards/` and commit, so the layout is versioned.
