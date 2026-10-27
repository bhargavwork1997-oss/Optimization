# DPO governance

DPO reads cost, job, query and identity data for a whole Databricks account. Treat its outputs as
**confidential**. Access is granted to groups only, never to individuals. The full threat model is in
the build guide (section 12).

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
| Least privilege | per-role GRANTs, re-applied daily by the governance job |
| No privilege widening by operators | reporting schema read-only for operators; views owned by admins; `security` schema admin-only |
| Row-level security | dynamic views filter by cost centre per reader |
| Identity protection | salted SHA-256 (salt in `security.settings`, admin-only); unsalted dictionary attack fails |
| Inference protection | account-wide totals and ranks nulled for viewers |
| Data minimisation | raw SQL never persisted: comments and literals stripped at bronze (`retain_raw_sql: false`) |
| Tamper-evident audit | `ops.audit_log` append-only (`delta.appendOnly`), owned by admins; `system.access.audit` for reads |
| Drift | `SHOW GRANTS` vs role model; extra principals revoked in prod |
| Input validation | catalog, schema, group, cost-centre, SKU-pattern, storage and endpoint values allow-listed before any SQL |
| AI | off in enterprise profiles; only Databricks-hosted endpoints unless approved; metrics only, no SQL or names; output labelled |
| Test hooks | simulated membership refused whenever a catalog is configured |
| Workspace scope | `workspace_ids` limits account-wide system tables to approved workspaces |
| Code & jobs | bundle permissions by group; prod runs as service principals; CODEOWNERS on profiles, rules, governance, deployment |
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
4. Put the workspace host in `workspace_hosts` of `enterprise_prod.json` (the profile then refuses to run in
   any other workspace).
5. A `dpo-admins` member opens `notebooks/00_install`, enters the two service-principal application IDs and
   clicks **Run all**. The access gate verifies all of the above and stops with a single error listing every gap;
   once it passes, setup, first pipeline run, reporting views and both scheduled jobs are done in one go.

## Dashboards

Build on the `reporting` views and publish **without embedded credentials**. With embedded credentials every
viewer sees data with the publisher's rights, which bypasses row-level security.

## Review cadence

Monthly: `06_governance_audit` — drift, denied runs, readers of DPO tables, audit-log history; remove leavers;
re-certify `security.access_map`. Rotate the identity salt only with a plan (hashes change for all viewers).
