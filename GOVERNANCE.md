# DPO governance

DPO reads cost, job, query and identity data for a whole Databricks account. Treat its outputs as
**confidential**. Access is granted to groups only, never to individuals. Line-by-line findings and fixes:
`docs/security-review.md`.

## Role model

| Role (group) | Runs pipeline | Changes access | Reads | Identities | Cost centres |
|---|---|---|---|---|---|
| `dpo-admins` (+ admin SP) | yes | yes | everything incl. security, quarantine, audit | real | all |
| `dpo-operators` (+ pipeline SP) | yes | **no** | data layers, reporting | real | all |
| `dpo-analysts` | no | no | silver, gold, reporting, ops health | real | all |
| `dpo-viewers` | no | no | `reporting` views only | salted hash | only mapped |

Viewer row visibility comes from `cost_center_groups` in the profile and from rows admins add to
`security.access_map`. A viewer with no mapping sees no rows. Columns that reveal account-wide totals
(`share_of_total`, `cost_rank`, `rank`) are hidden from viewers.

## Two identities, two paths

| Path | Identity | Can do | Cannot do |
|---|---|---|---|
| Pipeline (`dpo-pipeline` job, `02_run_pipeline`) | pipeline SP in `dpo-operators` | read system tables, write sim/bronze/silver/gold/ops, append audit | grant, re-own, create or redefine reporting views, read or edit `security`, alter/delete audit rows |
| Governance (`dpo-governance` job, `00_setup` apply) | admin SP or human in `dpo-admins` | create catalog/schemas, grants, ownership, tags, reporting views, revoke drift | — |

## Controls

| Control | Mechanism |
|---|---|
| Run authorization | membership check before any read or write; denial written to `ops.audit_log` |
| Runtime guard | on every run: profile must be bound to this workspace, real system tables never with governance off, enforced runs only into the profile's catalog (or `governance.allowed_catalogs`) |
| Least privilege | per-role GRANTs, re-applied daily by the governance job |
| No privilege widening by operators | reporting schema read-only for operators; views owned by admins; `security` schema admin-only |
| Row-level security | dynamic views filter by cost centre per reader |
| Identity protection | salted SHA-256 (salt in `security.settings`, admin-only); unsalted dictionary attack fails |
| Inference protection | account-wide totals and ranks nulled for viewers |
| Data minimisation | raw SQL never persisted: comments and literals stripped at bronze (`retain_raw_sql: false`) |
| Tamper-evident audit | `ops.audit_log` append-only (`delta.appendOnly`), owned by admins; `system.access.audit` for reads |
| Drift | `SHOW GRANTS` vs role model; extra principals revoked in prod |
| Input validation | catalog, schema, group, cost-centre, SKU-pattern, storage and endpoint values allow-listed before any SQL |
| AI | narratives and forecast on in all profiles; only Databricks-hosted models (`databricks-*`, `system.ai.*`) unless approved; model input is metrics and rule metadata only (no IDs, names, cost centres, users or SQL); output must match a four-label format, is labelled and cached; forecast runs as the pipeline identity on your SQL warehouse; template / baseline fallback is recorded (`narrative_source`, `method`). See `docs/ai-features.md` |
| Test hooks | simulated membership refused whenever a catalog is configured |
| Workspace scope | `workspace_ids` limits account-wide system tables to approved workspaces |
| Code & jobs | bundle permissions by group; prod runs as service principals; CODEOWNERS on profiles, rules, governance, dashboards, Genie spaces, deployment |
| Dashboards | as code; no embedded credentials; viewer dashboard on `reporting` only; engineering dashboard never shared with viewers |
| Genie | as code; queries run as the asker; viewer space on `reporting` only with value sampling off; engineering space never shared with viewers; only admins can edit or manage |
| Secrets in git | CI secret scan; `.gitignore` blocks data exports |

## One-time setup by platform admins (enterprise)

1. Account console → create groups `dpo-admins`, `dpo-operators`, `dpo-analysts`, `dpo-viewers` and any
   cost-centre groups; assign them to the workspace.
2. Create two service principals: `sp-dpo` (add to `dpo-operators` only) and `sp-dpo-admin` (add to `dpo-admins`).
3. Metastore admin grants system-table access:
   ```sql
   GRANT USE CATALOG ON CATALOG system TO `dpo-operators`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.billing  TO `dpo-operators`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.lakeflow TO `dpo-operators`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.query    TO `dpo-operators`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.compute  TO `dpo-operators`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.access   TO `dpo-admins`;
   GRANT CREATE CATALOG ON METASTORE TO `dpo-admins`;   -- or pre-create catalog dpo
   ```
4. Create a serverless SQL warehouse named `dpo-sql` (or rename the `warehouse_id` lookup in `databricks.yml`) and
   give `dpo-admins`, `dpo-operators`, `dpo-analysts` and `dpo-viewers` **CAN_USE** on it. The dashboards and Genie spaces run on it,
   and the pipeline runs `ai_forecast` on it. Give `dpo-operators` **CAN_QUERY** on the AI narrative endpoint
   (`ai_endpoint` in the profile; Serving page) and confirm the model is approved for your data residency.
5. Deploy with the bundle from CI (`.github/workflows/deploy.yml`) or the CLI. The deploy identity needs the
   "Service principal: User" role on `sp-dpo` and `sp-dpo-admin`; the bundle resolves both by name.
6. `databricks bundle run dpo_install -t enterprise_prod`. It runs as `sp-dpo-admin`, verifies all of the above
   with the access gate (one error listing every gap, nothing changed) and then performs setup, the first
   pipeline run and the reporting views. Without the CLI, an admin can run `notebooks/00_install` instead.

Bundle-level safeguards (enforced by `tests/test_bundle.py`): production deploys to the deployer's own folder,
never `/Workspace/Shared` (writable by all users); permissions are per job, so `dpo_install` and
`dpo_governance` (admin identity) are admins-only and operators can run only the pipeline.

## Dashboards

Deployed as code (`resources.dashboards`, generated by `src/dpo/dashboard.py`):

| Dashboard | Reads | Permissions (bundle) |
|---|---|---|
| DPO · Cost optimization | `reporting` views only | admins CAN_MANAGE; operators, analysts, viewers CAN_RUN |
| DPO · Engineering | gold, ops | admins CAN_MANAGE; operators, analysts CAN_RUN — **never viewers** |

Both are published **without embedded credentials**. With embedded credentials every reader would see data
with the publisher's rights, which bypasses row-level security. `tests/test_bundle.py` fails if
`embed_credentials` is not `false` or if viewers get the engineering dashboard; `tests/test_dashboard.py` fails
if the cost dashboard reads anything but `reporting` and runs it as a viewer to prove rows are filtered.
Dashboard permission is not data permission: a reader without SELECT sees errors, not data.
Readers need CAN_USE on the SQL warehouse; grant it to the four groups.

## Genie spaces

Deployed as code (`resources.genie_spaces`, generated by `src/dpo/genie.py`):

| Genie space | Tables | Permissions (bundle) |
|---|---|---|
| DPO · Ask cost optimization | `reporting` views only | admins CAN_MANAGE; operators, analysts, viewers CAN_RUN |
| DPO · Ask engineering | gold + ops health (`run_log`, `source_status`, `dq_report`, `rule_status`) | admins CAN_MANAGE; operators, analysts CAN_RUN — **never viewers** |

| Threat | Control |
|---|---|
| Genie answers with the author's rights | It does not: Unity Catalog evaluates every Genie query as the asker. Only warehouse *compute* uses the author's credentials, so readers do not need warehouse grants for Genie. |
| **Prompt matching leaks rows across cost centres.** Format assistance / entity matching sample column values with the *author's* rights and store them in the space's shared context, visible to every user. On the row-filtered `reporting` views an admin author would expose other cost centres' workload keys, job names and cost centres to viewers. | `enable_format_assistance` and `enable_entity_matching` are `false` for **every** column of every table in the cost space (`tests/test_genie.py` fails otherwise, and checks the column list against the real views). In the engineering space they are off for `run_as`. Do not switch them on in the UI: the next deploy turns them off again, but values sampled in between stay in the space until then. |
| Viewer reaches gold/ops through Genie | The cost space lists only `reporting` views; the engineering space is not shared with viewers, and viewers have no SELECT on gold/ops anyway (a space never grants data access). |
| Editors change instructions, or managers read others' questions | CAN_EDIT / CAN_MANAGE only for admins (managers can review every user's conversations in the monitoring tab). Everyone else gets CAN_RUN. |
| Genie generates writes | Instructions require SELECT only; Genie executes read-only SQL; readers have no write grants on DPO schemas. |
| Inference of account-wide totals | Masked columns stay NULL for viewers; instructions tell Genie to compute shares only "of the spend you can see" and not to reconstruct totals or identities. |
| Client data in the space definition | Instructions, sample questions, example SQL and benchmarks are generic and generated from code; tests reject e-mail addresses in the space text. |
| Audit, security, quarantine exposed | Never added to any space (test-enforced). |

Monthly: open each space's **Monitoring** tab (admins) for questions that probe other cost centres or identities,
and run the **Benchmarks** to catch answer-quality drift after schema changes.

## Review cadence

Monthly: `06_governance_audit` — drift, denied runs, readers of DPO tables, audit-log history; remove leavers;
re-certify `security.access_map`. Rotate the identity salt only with a plan (hashes change for all viewers).
