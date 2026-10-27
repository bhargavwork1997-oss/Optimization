# ADO backlog — DEM Task Management System

Create these under one Feature, **Databricks Processing Optimization (DPO) Accelerator**.
Reference work item IDs in commits as `AB#<id>` so they link to this repo.

| Epic | User story | Acceptance criteria | Artifact |
|---|---|---|---|
| E1 Discovery & Ingestion | Set up Free Edition workspace + Git folder | Repo cloned in workspace; notebooks run | README |
| | Create schema and DBU price reference | `ref_sku_price` populated | 00_setup.sql |
| | Generate synthetic usage logs | 90 days × 8 workloads in `raw_usage_logs` | 01_generate_logs.py |
| | System-tables source for client workspaces | Same column contract; checklist passed | 01b_system_tables_source.sql, data-contract.md |
| E2 Cost Attribution | Attribute USD cost per job/day | No unpriced SKUs; daily trend query | 02_silver_cost.sql |
| | Categorize waste and optimization lever | Every job has category, lever, est. saving | 03_gold_categorize.sql |
| E3 AI Recommendations | LLM recommendations for top spenders | Top-N rows with root cause/fix/saving/risk; fallback works | 04_ai_recommend.py |
| E4 Optimization Lab | Prove clustering, projection, broadcast levers | `lab_results` shows before/after; profile screenshots saved | 05_optimization_lab.py |
| E5 Dashboard & Packaging | Build and publish AI/BI dashboard | 6 rows of widgets; exported JSON committed | dashboard-guide.md |
| | Daily job via Asset Bundle | `bundle deploy` succeeds; job run green | databricks.yml, resources/ |
| | Client assessment template | Deck/report with top spenders, levers, savings, lab evidence | — |

## Branching
- `main` — demo-ready, protected
- `feature/AB<id>-short-name` — one branch per user story, PR into `main`
