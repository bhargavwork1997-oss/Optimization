# Data contract — bronze sources

Every source, real or synthetic, is projected to the same column contract (`SPECS` in `src/dpo/pipeline.py`).
Synthetic twins in `<prefix>sim` use the same names as the system tables, so silver and gold run unchanged.
A column missing from a source becomes a typed NULL and is listed in `ops.source_status.missing_columns`.

| Bronze table | System table | Key columns | Notes |
|---|---|---|---|
| `billing_usage` | `system.billing.usage` | record_id, workspace_id, sku_name, usage_date, usage_start/end_time, usage_quantity, custom_tags, usage_metadata.{job_id, job_run_id, cluster_id, warehouse_id, notebook_id, dlt_pipeline_id}, identity_metadata.run_as | spine of attribution; filtered to `workspace_ids` |
| `list_prices` | `system.billing.list_prices` | sku_name, price_start/end_time, pricing.effective_list.default | SCD join on usage_end_time; overridden by `contract_prices` |
| `jobs` | `system.lakeflow.jobs` | job_id, name, creator_id, tags, change_time | latest version per job |
| `job_run_timeline` | `system.lakeflow.job_run_timeline` | run_id, period_start/end_time, result_state, compute_ids | hourly slices collapsed to runs |
| `job_task_run_timeline` | `system.lakeflow.job_task_run_timeline` | run_id, job_run_id, task_key, period_* | task skew |
| `query_history` | `system.query.history` | statement_id, statement_text, durations, read_bytes/files, pruned_files, spilled_local_bytes, from_result_cache, compute.warehouse_id | `statement_text` stored only as normalised shape (comments and literals removed) unless `retain_raw_sql` |
| `clusters` | `system.compute.clusters` | cluster_id, cluster_source, worker_count, auto_termination_minutes, dbr_version, tags, change_time | latest version per cluster |
| `node_timeline` | `system.compute.node_timeline` | cluster_id, start/end_time, cpu_user/system_percent, mem_used_percent | utilisation; empty on serverless-only workspaces |
| `table_inventory` | `information_schema.tables` + `DESCRIBE DETAIL` | num_files, size_bytes, clustering_columns | only catalogs in `inventory_catalogs` |

## Client onboarding checklist

- [ ] System schemas enabled (`billing`, `lakeflow`, `query`, `compute`, `access`)
- [ ] `dpo-operators` has SELECT on them; `00_setup` preflight shows OK for each
- [ ] `workspace_ids` set to the workspaces in scope (preflight warns if empty)
- [ ] `contract_prices` / `price_multiplier` reflect the client contract
- [ ] `inventory_catalogs` agreed (table-layout scan reads metadata only)
- [ ] `retain_raw_sql` stays `false` unless the client approved storing raw SQL
- [ ] First run: review `ops.source_status` and `ops.dq_report` with the client before any finding
