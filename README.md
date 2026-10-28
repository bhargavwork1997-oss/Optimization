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

See `GOVERNANCE.md` and the threat model (build guide, section 12).

## Deploy — Databricks Asset Bundles (recommended)

The bundle (`databricks.yml` + `resources/`) is the deployment unit: code, jobs, run-as identities and
permissions per environment. Validated with `databricks bundle validate` for all three targets and against the
CLI's schema in CI.

| Target | Mode | Runs as | Jobs | Dashboards (warehouse lookup) |
|---|---|---|---|---|
| `dev` (Free Edition) | development | you | `dpo_pipeline`, `dpo_install`, `dpo_tests` | both · `Serverless Starter Warehouse` |
| `enterprise_dev` | development | deployer | + `dpo_governance`; per-job group permissions | both · `dpo-sql`, group permissions |
| `enterprise_prod` | production | `sp-dpo` (pipeline), `sp-dpo-admin` (install, governance) | install/governance admins-only | both · `dpo-sql`, group permissions |

```bash
databricks bundle validate -t enterprise_prod --profile <client-cli-profile>
databricks bundle deploy   -t enterprise_prod --profile <client-cli-profile>
databricks bundle run dpo_install -t enterprise_prod --profile <client-cli-profile>   # first time
```

`dpo_install` runs the access gate (one error listing every problem, nothing changed), then setup → first
pipeline run → secured views → verify, with the lab and tests alongside. It never creates jobs itself: the
bundle owns them. CI: `.github/workflows/deploy.yml` (manual, per environment, OAuth service principal).

Before the first enterprise deploy: platform admins create the groups and the two service principals
(`GOVERNANCE.md`); edit `config/environments/enterprise_prod.json`; rename the SP lookups in `databricks.yml`
if yours differ. Workspace URLs are not stored in `databricks.yml`; they come from the CLI profile or CI secrets.

## Install without the CLI (Git folder)

**Workspace → Create → Git folder** → open `notebooks/00_install` → **Run all**. The profile is detected from
the workspace address (`workspace_hosts`); on enterprise fill `pipeline_run_as` / `governance_run_as`. Same
access gate and steps as `dpo_install`; it also creates the scheduled jobs, unless bundle-deployed DPO jobs for
this profile already exist (detected by tag), in which case it changes nothing. `mode = check_only` runs the
gate alone.

Day to day: `02_run_pipeline` (manual run), `06_governance_audit` (monthly), the dashboards.

## Dashboards (as code)

Two AI/BI dashboards per profile, generated from the profile by `src/dpo/dashboard.py` into
`dashboards/<profile>/*.lvdash.json` and deployed by the bundle (`resources.dashboards`) — nothing to build by hand.

| Dashboard | Pages | Reads | Who |
|---|---|---|---|
| **DPO · Cost optimization** | Overview (KPIs, daily cost by product, cost by cost centre, findings and savings by category, top workloads) · Recommendations (scored backlog with evidence, narrative and action; value vs feasibility; score breakdown) | `reporting` views only | viewers, analysts, operators |
| **DPO · Engineering** | Jobs & queries (reliability scorecard with R06 highlight, expensive query shapes) · Pipeline health (latest run, recent runs, sources, data quality, rule status) | gold, ops | analysts, operators |

Both are published **without embedded credentials**, so each reader's own grants apply (row-level security,
hashed identities). Readers need **CAN_USE** on the SQL warehouse. To change a dashboard, edit
`src/dpo/dashboard.py`, run `cd src && python -m dpo.dashboard`, commit the regenerated files and deploy;
CI fails if the files are stale. Edits made in the UI are overwritten by the next deploy. Without the CLI,
`00_install` publishes them too (widgets `dashboards`, `warehouse`). Details: `docs/dashboard-guide.md`.

## Local tests (no Databricks)

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt   # Java 17; not Python 3.14+
pytest -q -m "not slow"                # unit, regression (golden), chaos, governance, security
pytest -q -m slow                      # 5x volume
```

## Repo layout

```
config/       rules.json (17 rules) · environments/*.json (profiles)
src/dpo/      config · governance · installer · transforms · synth · pipeline · rules · recommend · validation · dashboard
notebooks/    00_install · 00_setup · 01_generate_synthetic_data · 02_run_pipeline · 03_optimization_lab
              04_regression_tests · 05_stress_tests · 06_governance_audit · _common
dashboards/   <profile>/cost_optimization.lvdash.json · <profile>/engineering.lvdash.json (generated)
docs/         data-contract.md · dashboard-guide.md · ado-backlog.md
resources/    dpo_jobs.yml          databricks.yml (targets dev / enterprise_dev / enterprise_prod, dashboards)
tests/        pytest suites (incl. test_bundle.py) + golden/small_seed42.json
GOVERNANCE.md · .github/ (CI with bundle-schema check, deploy workflow, CODEOWNERS)
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
