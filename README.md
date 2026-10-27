# DPO Accelerator — Databricks Processing Optimization

Analyses Databricks system telemetry to show **what generates cost**, **why** (17 evidence-based rules),
**what to do about it** (prioritised backlog, score `S = 0.35V + 0.25C + 0.20F + 0.20R`) and **whether a
fix type works** (optimisation lab). Runs on serverless, so it works on Free Edition and enterprise accounts.

Levers covered: SQL optimisation and anti-patterns · data layout (pruning, small files, clustering) ·
right-sizing and auto-termination · job vs all-purpose compute · reliability and skew · cost spikes and
regressions · tagging · replatforming plain file copies to native Azure compute.

## Architecture

```
system.* tables  or  synthetic twins (dpo_sim)        <- profile.source_mode
        │
bronze   column contracts, workspace scope, raw SQL minimised
silver   DQ + quarantine, de-dup, SCD latest, contract/list pricing, SQL fingerprints
gold     cost attribution (direct / allocated / idle), scorecards, 17 rules, scored backlog, narratives
reporting  secured views: row-level by cost centre, salted identity hashing   (admin-owned)
security   access map + identity salt                                          (admins only)
ops        run log, DQ, rule status, append-only audit log, governance log
```

## Environments: one code base, three profiles

| Profile (`config/environments/`) | Bundle target | Catalog | Sources | Governance |
|---|---|---|---|---|
| `free_edition` | `dev` | `workspace` (`dpo_*` schemas) | synthetic | not enforced |
| `enterprise_dev` | `enterprise_dev` | `dpo_dev` | system tables | enforced, drift reported |
| `enterprise_prod` | `enterprise_prod` | `dpo` | system tables | enforced, drift revoked, two service principals |

> Free Edition is non-commercial and has no access control: **never load client telemetry into it.**

Going live at a client: edit `enterprise_prod.json` (catalog, storage, `workspace_ids`, contract prices,
group names), deploy the `enterprise_prod` target, have an admin run `00_setup` with `apply`. No code changes.
See `GOVERNANCE.md` and the threat model (build guide, section 12).

## Quick start — Free Edition

1. **Workspace → Create → Git folder** → this repo.
2. `notebooks/00_setup` → profile `free_edition` → `apply` → Run all.
3. `notebooks/01_generate_synthetic_data` → `scale=small` → Run all.
4. `notebooks/02_run_pipeline` → Run all. The last cell is the backlog.
5. `notebooks/04_regression_tests` → every check `passed = true`.
6. Optional: `03_optimization_lab` (before/after proof), dashboard from `dashboards/dashboard_datasets.sql`.

CLI: `databricks bundle deploy -t dev && databricks bundle run dpo_pipeline -t dev`
(the `dev` target uses your `Optimization` CLI profile).

## Local tests (no Databricks)

```bash
pip install pyspark==3.5.3 pytest     # Java 17+
pytest -q -m "not slow"                # unit, regression (golden), chaos, governance, security
pytest -q -m slow                      # 5x volume
```

## Repo layout

```
config/       rules.json (17 rules) · environments/*.json (profiles)
src/dpo/      config · governance · transforms · synth · pipeline · rules · recommend · validation
notebooks/    00_setup · 01_generate_synthetic_data · 02_run_pipeline · 03_optimization_lab
              04_regression_tests · 05_stress_tests · 06_governance_audit · _common
dashboards/   dashboard_datasets.sql
docs/         data-contract.md · dashboard-guide.md · ado-backlog.md
resources/    dpo_jobs.yml          databricks.yml (targets dev / enterprise_dev / enterprise_prod)
tests/        pytest suites + golden/small_seed42.json
GOVERNANCE.md · .github/ (CI, CODEOWNERS)
```

## Migrating from v1 (`workspace.dpo`)

v1 tables (`raw_usage_logs`, `usage_cost`, `cost_categorized`, `ai_recommendations`, `ref_sku_price`,
`lab_results`) are not used any more. After the first successful v2 run, an owner can remove them:

```sql
-- review first: SHOW TABLES IN workspace.dpo;
DROP SCHEMA IF EXISTS workspace.dpo CASCADE;
```

| v1 | v2 |
|---|---|
| `ref_sku_price` | `system.billing.list_prices` + profile `contract_prices` / `price_multiplier` |
| `01_generate_logs` / `01b_system_tables_source` | `01_generate_synthetic_data` / `source_mode = system` |
| `02_silver_cost`, `03_gold_categorize` | `02_run_pipeline` (bronze → silver → gold → 17 rules) |
| waste categories / levers | rules R01–R17 (R16 SQL anti-patterns, R17 replatform candidates) |
| `04_ai_recommend` | `narrate` stage: same four-line prompt, template fallback, hardened |
| `05_optimization_lab` | `03_optimization_lab` → `gold.lab_results` |

## Work tracking

Backlog in Azure DevOps (DEM Task Management System) — see `docs/ado-backlog.md`.
Branch `feature/AB<id>-short-name`; reference work items as `AB#<id>` in commits.

Estimated savings are hypotheses until a benchmark validates them.
