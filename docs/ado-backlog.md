# ADO backlog — DEM Task Management System

Create under one Epic, **Databricks Processing Optimization (DPO) Accelerator**.
Reference work items in commits as `AB#<id>` so they link to this repo.

| Feature | User story | Acceptance criteria | Artifact |
|---|---|---|---|
| F1 Service & governance | Environment profiles | free_edition / enterprise_dev / enterprise_prod validate; deploy without code change | `config/environments/`, `databricks.yml` |
| | Role model and grants | 4 groups, 2 service principals; operators cannot grant or redefine views | `governance.py`, `GOVERNANCE.md` |
| | Threat model reviewed | Threats, mitigations and residual risks signed off by client security | build guide §12 |
| F2 Telemetry | Bronze contracts for 9 sources | `ops.source_status` lists every source; missing columns tolerated | `pipeline.py` |
| | Silver DQ and pricing | Quarantine + DQ report; contract prices applied | `pipeline.py`, `dq_report` |
| | Cost attribution | Gold total equals silver total | `workload_cost_daily` |
| F3 Diagnostics | 17 rules | Each fires on its scenario only; zero false positives on controls | `rules.py`, `rules.json` |
| F4 AI analysis | Narratives with fallback | Template always present; AI optional, labelled, no SQL sent | `recommend.py` |
| F5 Optimisation | Optimisation lab | `gold.lab_results` before/after per lever | `03_optimization_lab` |
| | Benchmark approved fixes | Before/after evidence attached to the ADO item | client work |
| F6 Delivery | Dashboard | Overview on reporting views, published without embedded credentials | `dashboards/` |
| | Tests in CI and workspace | pytest green on PR; `dpo-tests` job green in dev | `tests/`, `04`, `05` |

## Branching

- `main` — demo-ready, protected, CODEOWNERS review for governance/config/deployment files
- `feature/AB<id>-short-name` — one branch per story, PR into `main`
