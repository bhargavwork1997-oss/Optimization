# Security review

Scope: all code, notebooks, profiles, bundle, CI/CD and generated dashboards/Genie spaces in this repository, as of
2026-11-06. This complements the threat model in `GOVERNANCE.md` (role model, controls) with a line-by-line
review.

Method:
- **Manual review** of every SQL construction site, permission path, input (widgets, job parameters, profiles,
  metadata, model output) and output (tables, views, dashboards, Genie, logs).
- **Static analysis** with Bandit on `src/` and `notebooks/`: 40 × B608 (SQL from strings), 9 × B110, 3 × B112,
  3 × B101. Every B608 site was reviewed (below); the others are deliberate best-effort calls.
- **Secret scan** for tokens, keys and passwords: none found. Gitleaks also runs in CI.
- **Tests**: every fix below has a test.

Dependency audit (`pip-audit`) could not reach its vulnerability database from the review environment. It now runs
in CI (`sast` job) on every pull request.

## Findings fixed in this review

| # | Severity | Finding | Fix | Test |
|---|---|---|---|---|
| 1 | **High** | **Real telemetry into an ungoverned schema.** The checks "profile bound to this workspace" and "no real system tables with governance off" ran only in the installer. The pipeline job grants operators *Run now with different parameters*, so `profile=free_edition, source_mode=system` in an enterprise workspace would copy billing, identities and SQL shapes into `workspace.dpo_*` with no row-level security. | `governance.runtime_guard` runs on **every** pipeline run (and every `authorize`) before any data is read; refusals are audited. | `test_pipeline_refuses_real_telemetry_with_governance_off`, `test_pipeline_refuses_profile_bound_to_another_workspace` |
| 2 | **High** | **Catalog redirect.** On an enforced profile the `catalog` parameter could point the run at a catalog that every user can read (for example a default workspace catalog), moving results out of the governed catalog. | Enforced runs must write to the profile's own catalog, or to one listed in `governance.allowed_catalogs`. | `test_enforced_run_cannot_be_redirected_to_another_catalog` |
| 3 | Medium | **Profile name path traversal.** The profile name (widget, job parameter) was joined into a file path, so `../../x` could load another JSON file as configuration. | Profile names must match `^[a-z0-9_]{1,64}$`. | `test_profile_name_cannot_be_a_path` |
| 4 | Medium | **Identifier injection from metadata.** Table-inventory names from `information_schema` were wrapped in backticks without escaping. A table named with a backtick could break out of the identifier in `DESCRIBE DETAIL`. | Backticks are doubled in every metadata-derived identifier. | `test_inventory_escapes_backticks_in_metadata_names` |
| 5 | Medium | **Free-text service-principal IDs.** `pipeline_run_as` / `governance_run_as` went into SCIM filters and job settings unchecked. | Only application IDs (UUID) are accepted; anything else is a BLOCKER. | `test_service_principal_least_privilege_checks` |
| 6 | Medium | **Cached AI text trusted blindly.** `gold.ai_narrative_cache` is writable by operators; cached text was shown without re-checking. | Cached narratives are re-validated (four labels, no links, length cap) before use. | `test_bad_or_injected_model_output_is_rejected` |
| 7 | Medium | **Unpinned CI action.** `databricks/setup-cli@main` ran unreviewed code with deploy credentials. | Pinned to the v1.22.1 commit SHA. | — |
| 8 | Medium | **Prod deploy from any branch.** `workflow_dispatch` could deploy an unreviewed branch to production. | The deploy job refuses `enterprise_prod` unless run from `main`; workflow inputs reach the shell only through `env`. Also configure the GitHub environment (below). | — |
| 9 | Low | **Unbounded inputs.** `lookback_days` (scan cost) and the lab's `runs` (each run scans ~6 M rows) had no upper bound; `end_date` was not format-checked before use. | `lookback_days` 7–400, `runs` 1–10, `end_date` must be `YYYY-MM-DD`. | `test_config_rejects_unsafe_runtime_values` |
| 10 | Low | **Test schemas in production.** `04`/`05` could be run by hand on a production profile. | They exit unless the profile allows test schemas (dev profiles). | — |
| 11 | Low | **Accepted blockers not audited.** An admin could override a gate blocker without a trace. | `BLOCKER_ACCEPTED` row with the check names in `ops.audit_log`. | — |
| 12 | Info | **Unfair lab comparison.** The broadcast experiment compared a clustered table with an unclustered one, mixing two levers. | Both sides use the same table; only the hint changes. | — |

## Reviewed and acceptable

| Area | Why it is safe |
|---|---|
| SQL built from strings (40 Bandit B608) | Interpolated values are only (a) identifiers allow-listed by `DPOConfig.validate` (catalog, prefix, groups, cost centres, SKU patterns, warehouse id, endpoint), (b) numbers cast with `int()`/`float()`, (c) dates parsed with `date.fromisoformat`, or (d) static text in code. No widget, metadata or model value reaches SQL without one of these. |
| Row-level security | Dynamic views run as their admin owner; operators cannot redefine them (reporting schema is read-only for operators). Viewer filters come from the profile (validated) and `security.access_map` (admin-only). |
| Identity protection | Salted SHA-256; salt generated in-database with `uuid()`, readable by admins only; a dictionary of e-mail addresses cannot reverse the hash (tested). |
| Inference | Account-wide totals and ranks are NULL for viewers; dashboards and Genie compute shares of *visible* rows only. |
| Raw SQL | Comments and literals are stripped at bronze (`retain_raw_sql: false`): customer IDs or secrets in query text are never stored. |
| AI narratives | Model input is metrics and rule metadata only (tested); Databricks-hosted models only unless `ai_allow_external` is approved; injection-resistant prompt; strict output format; labelled output; template fallback. |
| ai_forecast | Runs on your SQL warehouse as the pipeline identity; the statement contains only validated table names, a date and numbers; outputs are row-filtered for viewers. |
| Genie | Queries run as the asker. Prompt-matching value sampling is off for every column of the viewer space (otherwise the author's values leak across cost centres). Only admins can edit or manage, since managers can read all conversations. |
| Dashboards | Published without embedded credentials (test-enforced), so each reader's own grants apply. |
| Audit log | Append-only Delta table owned by admins; every authorization decision, run, install and accepted blocker is recorded. |
| Test hooks | Simulated group membership and users are refused whenever a real catalog is configured. |
| Bundle | Production deploys to the deployer's own folder (never `/Workspace/Shared`); per-job permissions keep the admin-identity jobs admin-only. |
| `try/except: pass` (Bandit B110/B112) | Best-effort calls only: read-only Spark conf on serverless, optional log retention, metadata a table may not have. None of them skips an authorization or validation step. |

## Residual risks and recommendations

| Risk | Recommendation |
|---|---|
| **This repository is public.** A client's profile (catalog, groups, cost centres, **budgets**, workspace host) committed here would be public. | Deploy from a **private fork** per client. Keep only placeholders in the public repo. Never commit `budgets`, `workspace_hosts` or contract prices of a real client. |
| The Free Edition workspace host is committed (`databricks.yml` dev target, `free_edition.json`). | Acceptable for a personal sandbox that holds synthetic data only. Remove it if the workspace ever holds anything else. |
| GitHub environment protection is configured outside the repo. | `enterprise_prod` environment: required reviewers, deployment branch `main` only, secrets scoped to the environment. Turn on branch protection with "Require review from Code Owners". |
| Operators can write gold, so they could alter numbers viewers see. | By design (the pipeline identity must write). Detect it through the audit log (who ran) and Delta history of gold tables. Keep the operators group to the service principal plus a few people. |
| Foundation-model data residency. | Before enabling AI in a regulated client, confirm that the endpoint serves from an approved region (Serving page) and that the model is approved. |
| `ai_forecast` is a preview feature. | The `method` column and run log show when the baseline was used; nothing pretends to be AI. |
| Action versions pinned by tag (`actions/checkout@v4`, ...). | Pin by commit SHA too if your organisation requires it. |

## How to re-run this review

```bash
pip install bandit pip-audit
bandit -q -r src notebooks -s B608,B101,B110,B112     # must be clean
bandit -q -r src notebooks -t B608                     # list SQL sites to review after a change
pip-audit -r requirements-dev.txt
pytest -q -m "not slow"                                # includes every security regression test
```
