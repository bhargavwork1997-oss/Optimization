# Dashboard guide

Build after `02_run_pipeline` has run once (and, on enterprise, after `00_setup` apply created the views).

1. **Dashboards → Create dashboard** `DPO – Cost Optimization`.
2. **Data** tab → for each block in `dashboards/dashboard_datasets.sql`, **Create from SQL**, paste it, rename the
   dataset to the name in its header (`ds_kpis`, `ds_cost_trend`, …). On enterprise prod replace `workspace.dpo_` with `dpo.`.
3. **Page "Cost overview"** (datasets 1–7, `reporting` views):

| Row | Widget | Dataset | Config |
|---|---|---|---|
| 1 | Counter ×5 | `ds_kpis` | window_cost · run_rate_30d · untagged_pct · p1_findings · est_monthly_savings_unvalidated |
| 2 | Area (stacked) | `ds_cost_trend` | x `usage_date`, y `cost`, colour `billing_origin_product` |
| 3 | Bar | `ds_top_workloads` | x `workload_key`, y `monthly_cost` |
| 3 | Table | `ds_cost_by_owner` | all columns |
| 4 | Table | `ds_backlog` | widen `narrative` |
| 5 | Scatter | `ds_value_effort` | x `feasibility`, y `value_score`, size `est_monthly_savings`, colour `priority` |
| 6 | Table | `ds_job_scorecard` | all columns |

4. **Page "Engineering"** (datasets 8–11, gold/ops): query shapes table, lab results grouped bar
   (x `experiment`, y `before_sec`, `after_sec`), pipeline health, data quality.
5. **Publish** → choose **individual data permissions (no embedded credentials)**. Share "Cost overview" with
   `dpo-viewers`, the whole dashboard with `dpo-analysts`.
6. Schedule a refresh after the daily job. Export the `.lvdash.json` into `dashboards/` and commit it.
