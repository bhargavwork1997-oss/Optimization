# Data contract — `workspace.dpo.raw_usage_logs`

Every ingest source (synthetic or real) must produce exactly these columns. Notebooks 02–04 depend only on this contract.

| Column | Type | Meaning | Synthetic (01) | System tables (01b) |
|---|---|---|---|---|
| usage_date | DATE | Day of usage | generated | `system.billing.usage.usage_date` |
| job_name | STRING | Job / pipeline / cluster name | fixed list | `system.lakeflow.jobs.name`, else cluster name |
| cluster_id | STRING | Compute identifier | hash of name | `usage_metadata.cluster_id` |
| workload_type | STRING | ETL_JOB, INTERACTIVE, SQL, DLT, INGEST | fixed | mapped from `billing_origin_product` |
| sku | STRING | Normalized SKU family (joins `ref_sku_price`) | fixed | mapped from `sku_name` |
| node_type | STRING | Worker VM type | random | `system.compute.clusters.worker_node_type` |
| num_workers | INT | Worker count | random | `system.compute.clusters.worker_count` |
| runtime_hours | DOUBLE | Hours run that day | random | **APPROX** — NULL; derive from `system.lakeflow.job_run_timeline` |
| dbus | DOUBLE | DBUs consumed | profile-based | `SUM(usage_quantity)` |
| avg_cpu_util | DOUBLE | 0–1 average CPU | profile-based | `system.compute.node_timeline` |
| idle_minutes | INT | Minutes near-idle | profile-based | **APPROX** — minutes with CPU < 5% |
| spill_gb | DOUBLE | Disk spill | profile-based | **APPROX** — 0; SQL warehouses via `system.query.history` |
| failed_retries | INT | Failed attempts | profile-based | **APPROX** — 0; count failed runs in `job_run_timeline` |
| query_text | STRING | Representative SQL | fixed | **APPROX** — NULL; SQL warehouses via `system.query.history.statement_text` |

## Client-onboarding checklist for 01b
- [ ] System schemas enabled by a metastore admin (`billing`, `compute`, `lakeflow`, `query`)
- [ ] Service principal has `SELECT` on those schemas
- [ ] `SELECT * FROM system.billing.usage LIMIT 10` returns rows
- [ ] `billing_origin_product` and `sku_name` values reviewed against the CASE mappings in 01b
- [ ] APPROX columns filled where the client needs them (runtime, retries, spill, SQL text)
- [ ] `ref_sku_price` updated with contracted rates
