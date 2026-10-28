# Dashboard guide

The dashboards are **code**. Nothing is built by hand in the workspace, and the same definition works in Free
Edition and enterprise: `src/dpo/dashboard.py` generates one copy per profile with that profile's table names
(`workspace.dpo_reporting.x` on Free Edition, `dpo.reporting.x` in enterprise prod).

```
src/dpo/dashboard.py ──python -m dpo.dashboard──► dashboards/<profile>/{cost_optimization,engineering}.lvdash.json
                                                        │
                       databricks bundle deploy -t <target>   (resources.dashboards in databricks.yml)
                       or notebooks/00_install (Git folder, no CLI)
```

## What you get

### DPO · Cost optimization — reads `reporting` views only (safe for viewers)

| Page | Widgets | Dataset |
|---|---|---|
| Overview | filters: usage date, cost center, product | — |
| | counters: window cost · 30-day run rate · est. savings / month (unvalidated) · P1 findings · open findings · untagged spend | `ds_cost_daily`, `ds_workloads`, `ds_backlog` |
| | stacked bar: daily cost by product; bar: cost by cost center | `ds_cost_daily` |
| | bars: findings by category and est. savings by category, coloured by priority | `ds_backlog` |
| | table: top workloads (type, cost center, monthly cost, share of visible spend, attribution, active days, findings, rule ids) | `ds_workloads` |
| Recommendations | header with the scoring formula; filters: priority, category, cost center | — |
| | backlog table: rank, priority, score, rule, category, finding, workload, savings, status, metric vs threshold, why flagged (narrative), recommended action | `ds_backlog` |
| | scatter: value (V) vs feasibility (F) by priority; stacked bar: score breakdown of the top 15 (0.35·V, 0.25·C, 0.20·F, 0.20·R) | `ds_backlog`, `ds_score_parts` |

### DPO · Engineering — reads gold and ops (analysts and operators only)

| Page | Widgets |
|---|---|
| Jobs & queries | counters: jobs, failed runs, job cost, SQL cost · job reliability scorecard (failure rate over the R06 threshold in orange) · most expensive query shapes (spill highlighted, rule ids) |
| Pipeline health | counters: failed stages, missing sources, rows quarantined · latest run stages · recent runs · source availability · data quality · rule status |

## Differences from the design canvas

AI/BI dashboards are read-only, so a few interactive pieces of the design map to something close:

| Design | In the dashboard |
|---|---|
| Detail panel for the selected finding | backlog table columns: evidence (metric, observed, threshold), *Why flagged*, *Recommended action*; click a cell to see the full text |
| Accept & create story / Dismiss | create the Azure DevOps work item from the row (reference the rule id); dismiss = disable or tune the rule in `config/rules.json` |
| Window: last 7 / 28 / 90 days | usage-date range filter on the daily charts; workload KPIs use the profile's `lookback_days` window |
| Source toggle (synthetic / system) | the profile decides it; the header states the source |
| Bubble size = savings | colour = priority; savings are in the backlog table and the savings-by-category chart |
| Share of total | *share of visible spend*: account-wide shares are masked for viewers, so the share is of the rows you can see |
| Export CSV | each table's menu → Download (CSV) |
| Pipeline status strip | Engineering → Pipeline health (viewers have no access to ops tables) |

## Deploy

**With the bundle (recommended).** `databricks bundle deploy -t <target>` creates or updates both dashboards and
publishes them without embedded credentials. The SQL warehouse is resolved by name: `Serverless Starter Warehouse`
on Free Edition, `dpo-sql` in enterprise (rename the `warehouse_id` lookup in `databricks.yml` to yours). In
development targets the dashboards are named `[dev <you>] DPO · …`.

From the web UI (Git folder): open the bundle in the **Deployments** pane and click **Deploy**; the dashboards
appear under the bundle's `resources` folder and in **Dashboards**.

**Without the CLI.** `notebooks/00_install` publishes both dashboards to
`/Workspace/Users/<you>/dpo-dashboards-<profile>` (outside the Git folder) with the same settings and group
permissions. Widget `warehouse` = warehouse name (blank = first serverless). If bundle-deployed DPO jobs exist
for the profile, the installer leaves dashboards to the bundle.

Run the pipeline once (`dpo_install`) before opening the dashboards; empty tables show empty widgets.

## Change a dashboard

1. Edit `src/dpo/dashboard.py` (datasets in `datasets()`, layout in `_cost_pages()` / `_engineering_pages()`;
   the grid is 6 columns wide).
2. `cd src && python -m dpo.dashboard` → regenerates `dashboards/*/`.
3. `pytest -q tests/test_dashboard.py tests/test_bundle.py` — checks the files are current, the cost dashboard reads
   only `reporting`, the layout has no overlaps, every dataset and widget query runs on pipeline output, and a
   viewer sees only their cost centre.
4. Commit and deploy.

Changes made in the dashboard editor are overwritten by the next deploy. To prototype in the UI, copy the
dashboard, edit the copy, then port the change into `dashboard.py`.

## Access

| Group | Cost optimization | Engineering |
|---|---|---|
| dpo-admins | CAN_MANAGE | CAN_MANAGE |
| dpo-operators | CAN_RUN | CAN_RUN |
| dpo-analysts | CAN_RUN | CAN_RUN |
| dpo-viewers | CAN_RUN | — |

Every reader also needs CAN_USE on the warehouse. Data access still comes from Unity Catalog grants: a viewer who
opens the engineering dashboard through a forwarded link sees errors, not data.
