# DPO Accelerator — Databricks Processing Optimization

Analyzes Databricks usage logs to show **what generates cost**, **why** (waste category), **what to do about it**
(optimization lever), and **how much it saves** — with AI-written recommendations and a lab that proves the savings.

Levers covered: SQL optimization · data model changes · partitioning / liquid clustering ·
right-sizing & auto-termination · job vs all-purpose compute · replatforming simple pipelines to native Azure compute.

## Architecture

```
Bronze  raw_usage_logs       01_generate_logs (POC, synthetic)  |  01b_system_tables_source (client, real)
        ref_sku_price        00_setup
Silver  usage_cost           02_silver_cost        USD cost per job / cluster / day
Gold    cost_categorized     03_gold_categorize    waste_category + optimization_lever + est. savings
        ai_recommendations   04_ai_recommend       LLM fix per top spender (rule-based fallback)
Lab     lab_results          05_optimization_lab   before/after proof on samples.tpch
UI      AI/BI dashboard      dashboards/dashboard_datasets.sql
Orchestration                resources/dpo_pipeline.job.yml (00 → 01 → 02 → 03 → 04, daily, serverless)
```

All tables live in `workspace.dpo`.

## Quick start — Databricks Free Edition

> Community Edition was retired on 1 Jan 2026; Free Edition replaces it. Free Edition is **serverless-only and
> non-commercial** — use it for the POC/demo with synthetic data only. Never load client logs into it.

1. Sign up at https://login.databricks.com (Free Edition).
2. **Settings → Linked accounts → Git integration** → GitHub + personal access token.
3. **Workspace → Create → Git folder** → paste this repo's URL.
4. Run notebooks in order on serverless compute: `00` → `01` → `02` → `03` → `04` → `05`.
5. Build the dashboard: follow [docs/dashboard-guide.md](docs/dashboard-guide.md).
6. Create the daily job — either:
   - **UI:** Jobs & Pipelines → Create job → add tasks 00, 01, 02, 03, 04 with dependencies, or
   - **CLI:** set `host` in `databricks.yml`, then
     ```bash
     databricks auth login --host <workspace-url>
     databricks bundle validate
     databricks bundle deploy -t dev
     databricks bundle run dpo_pipeline -t dev
     ```

## Running on a client workspace

| Change | Where |
|---|---|
| Use real system tables instead of synthetic data | `databricks bundle deploy -t client` (switches ingest to `01b`) |
| Contracted DBU rates | `00_setup.sql` → `ref_sku_price` |
| Catalog name | replace `workspace.dpo` in notebooks and dashboard SQL |
| More jobs analyzed by AI | `top_n` parameter in the job / notebook 04 |
| Thresholds (idle, CPU, spill, retries) | `rules` CTE in `03_gold_categorize.sql` |

Validate `01b` against the client workspace first — see [docs/data-contract.md](docs/data-contract.md).

## Repo layout

```
notebooks/   00_setup.sql · 01_generate_logs.py · 01b_system_tables_source.sql · 02_silver_cost.sql
             03_gold_categorize.sql · 04_ai_recommend.py · 05_optimization_lab.py
dashboards/  dashboard_datasets.sql
resources/   dpo_pipeline.job.yml
docs/        dashboard-guide.md · data-contract.md · ado-backlog.md
databricks.yml
```

## Work tracking

Backlog lives in Azure DevOps (DEM Task Management System) — see [docs/ado-backlog.md](docs/ado-backlog.md).
Link commits to work items with `AB#<id>` in the commit message.
