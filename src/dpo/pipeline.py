"""Bronze -> Silver -> Gold pipeline.

Every stage is a full, deterministic recompute of the look-back window, so a
re-run produces identical tables (idempotent) and a failed run is fixed by
simply running again.
"""
from __future__ import annotations

import datetime as dt
import json
import time
import uuid
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import LAYERS, DPOConfig
from .transforms import (project, query_fingerprint, normalize_sql, safe_div, split_valid,
                         workload_key, workload_type)

# ---------------------------------------------------------------------------
# Bronze column contracts.  Same contract for system tables and sim twins.
# ---------------------------------------------------------------------------
SPECS = {
    "billing_usage": [
        ("record_id", "record_id", "string"), ("workspace_id", "workspace_id", "string"),
        ("sku_name", "sku_name", "string"), ("cloud", "cloud", "string"),
        ("usage_start_time", "usage_start_time", "timestamp"),
        ("usage_end_time", "usage_end_time", "timestamp"), ("usage_date", "usage_date", "date"),
        ("usage_unit", "usage_unit", "string"), ("usage_quantity", "usage_quantity", "double"),
        ("record_type", "record_type", "string"),
        ("billing_origin_product", "billing_origin_product", "string"),
        ("custom_tags", "custom_tags", "map<string,string>"),
        ("job_id", "usage_metadata.job_id", "string"), ("job_run_id", "usage_metadata.job_run_id", "string"),
        ("cluster_id", "usage_metadata.cluster_id", "string"),
        ("warehouse_id", "usage_metadata.warehouse_id", "string"),
        ("notebook_id", "usage_metadata.notebook_id", "string"),
        ("dlt_pipeline_id", "usage_metadata.dlt_pipeline_id", "string"),
        ("node_type", "usage_metadata.node_type", "string"),
        ("run_as", ["identity_metadata.run_as", "identity_metadata.owned_by"], "string"),
    ],
    "list_prices": [
        ("sku_name", "sku_name", "string"), ("cloud", "cloud", "string"),
        ("currency_code", "currency_code", "string"), ("usage_unit", "usage_unit", "string"),
        ("price_start_time", "price_start_time", "timestamp"),
        ("price_end_time", "price_end_time", "timestamp"),
        ("list_price", ["pricing.effective_list.default", "pricing.default"], "double"),
    ],
    "jobs": [
        ("workspace_id", "workspace_id", "string"), ("job_id", "job_id", "string"),
        ("name", "name", "string"), ("creator_id", "creator_id", "string"),
        ("run_as", "run_as", "string"), ("tags", "tags", "map<string,string>"),
        ("change_time", "change_time", "timestamp"), ("delete_time", "delete_time", "timestamp"),
    ],
    "job_run_timeline": [
        ("workspace_id", "workspace_id", "string"), ("job_id", "job_id", "string"),
        ("run_id", "run_id", "string"), ("period_start_time", "period_start_time", "timestamp"),
        ("period_end_time", "period_end_time", "timestamp"), ("result_state", "result_state", "string"),
        ("termination_code", "termination_code", "string"), ("trigger_type", "trigger_type", "string"),
        ("run_type", "run_type", "string"), ("compute_ids", "compute_ids", "array<string>"),
    ],
    "job_task_run_timeline": [
        ("workspace_id", "workspace_id", "string"), ("job_id", "job_id", "string"),
        ("task_run_id", "run_id", "string"), ("job_run_id", "job_run_id", "string"),
        ("task_key", "task_key", "string"), ("period_start_time", "period_start_time", "timestamp"),
        ("period_end_time", "period_end_time", "timestamp"), ("result_state", "result_state", "string"),
        ("compute_ids", "compute_ids", "array<string>"),
    ],
    "query_history": [
        ("statement_id", "statement_id", "string"), ("workspace_id", "workspace_id", "string"),
        ("executed_by", "executed_by", "string"), ("execution_status", "execution_status", "string"),
        ("statement_text", "statement_text", "string"), ("statement_type", "statement_type", "string"),
        ("compute_type", "compute.type", "string"), ("warehouse_id", "compute.warehouse_id", "string"),
        ("cluster_id", "compute.cluster_id", "string"),
        ("total_duration_ms", "total_duration_ms", "long"),
        ("execution_duration_ms", "execution_duration_ms", "long"),
        ("compilation_duration_ms", "compilation_duration_ms", "long"),
        ("waiting_at_capacity_duration_ms", "waiting_at_capacity_duration_ms", "long"),
        ("waiting_for_compute_duration_ms", "waiting_for_compute_duration_ms", "long"),
        ("total_task_duration_ms", "total_task_duration_ms", "long"),
        ("read_bytes", "read_bytes", "long"), ("read_files", "read_files", "long"),
        ("pruned_files", "pruned_files", "long"), ("read_rows", "read_rows", "long"),
        ("produced_rows", "produced_rows", "long"), ("spilled_local_bytes", "spilled_local_bytes", "long"),
        ("shuffle_read_bytes", "shuffle_read_bytes", "long"),
        ("from_result_cache", "from_result_cache", "boolean"),
        ("start_time", "start_time", "timestamp"), ("end_time", "end_time", "timestamp"),
        ("client_application", "client_application", "string"),
    ],
    "clusters": [
        ("workspace_id", "workspace_id", "string"), ("cluster_id", "cluster_id", "string"),
        ("cluster_name", "cluster_name", "string"), ("owned_by", "owned_by", "string"),
        ("cluster_source", "cluster_source", "string"), ("worker_count", "worker_count", "int"),
        ("min_autoscale_workers", "min_autoscale_workers", "int"),
        ("max_autoscale_workers", "max_autoscale_workers", "int"),
        ("auto_termination_minutes", "auto_termination_minutes", "int"),
        ("dbr_version", "dbr_version", "string"), ("driver_node_type", "driver_node_type", "string"),
        ("worker_node_type", "worker_node_type", "string"), ("tags", "tags", "map<string,string>"),
        ("change_time", "change_time", "timestamp"), ("delete_time", "delete_time", "timestamp"),
    ],
    "node_timeline": [
        ("cluster_id", "cluster_id", "string"), ("instance_id", "instance_id", "string"),
        ("start_time", "start_time", "timestamp"), ("end_time", "end_time", "timestamp"),
        ("driver", "driver", "boolean"), ("cpu_user_percent", "cpu_user_percent", "double"),
        ("cpu_system_percent", "cpu_system_percent", "double"),
        ("mem_used_percent", "mem_used_percent", "double"), ("node_type", "node_type", "string"),
    ],
    "table_inventory": [
        ("table_catalog", "table_catalog", "string"), ("table_schema", "table_schema", "string"),
        ("table_name", "table_name", "string"), ("num_files", "num_files", "long"),
        ("size_bytes", "size_bytes", "long"), ("partition_columns", "partition_columns", "array<string>"),
        ("clustering_columns", "clustering_columns", "array<string>"),
        ("last_modified", "last_modified", "timestamp"), ("table_owner", "table_owner", "string"),
    ],
}

# column used to restrict each source to the look-back window
WINDOW_COL = {
    "billing_usage": "usage_date", "job_run_timeline": "period_start_time",
    "job_task_run_timeline": "period_start_time", "query_history": "start_time",
    "node_timeline": "start_time",
}


class Ctx:
    """Run context shared by all stages."""

    def __init__(self, spark: SparkSession, cfg: DPOConfig, run_id: str | None = None):
        cfg.validate()
        self.spark, self.cfg = spark, cfg
        self.run_id = run_id or uuid.uuid4().hex[:12]
        self.run_ts = dt.datetime.utcnow().replace(microsecond=0)
        end = dt.date.fromisoformat(cfg.end_date) if cfg.end_date else dt.date.today()
        self.end_date = end
        self.start_date = end - dt.timedelta(days=cfg.lookback_days)
        self.window_days = cfg.lookback_days
        self.monthly = 30.0 / cfg.lookback_days
        self.log: list[dict] = []
        try:
            spark.conf.set("spark.sql.session.timeZone", "UTC")
        except Exception:  # some serverless confs are read-only
            pass

    def t(self, layer, name):
        return self.cfg.t(layer, name)

    def read(self, layer, name) -> DataFrame:
        return self.spark.table(self.t(layer, name))

    def save(self, df: DataFrame, layer: str, name: str) -> int:
        (df.write.format(self.cfg.table_format).mode("overwrite")
           .option("overwriteSchema", "true").saveAsTable(self.t(layer, name)))
        return self.spark.table(self.t(layer, name)).count()

    def exists(self, full_name: str) -> bool:
        try:
            self.spark.table(full_name).limit(0).collect()
            return True
        except Exception:
            return False


def ensure_schemas(ctx: Ctx) -> None:
    """Data-layer schemas (IF NOT EXISTS). When governance is enforced the pipeline never
    creates the catalog, reporting or security objects: admins own those (00_setup)."""
    from .governance import plan_setup
    enforce = ctx.cfg.governance["enforce"]
    for st in plan_setup(ctx.cfg):
        if enforce and (st.purpose == "dedicated catalog" or "reporting" in st.purpose):
            continue
        try:
            ctx.spark.sql(st.sql)
        except Exception as exc:
            if st.purpose == "dedicated catalog":   # catalog may pre-exist without CREATE rights
                continue
            raise PermissionError(f"cannot create {st.sql.split()[-1]}: an admin must run 00_setup first "
                                  f"({str(exc).splitlines()[0][:200]})") from exc


def _timed(ctx: Ctx, stage: str, fn: Callable[[], dict]) -> dict:
    t = time.time()
    status, msg, out = "SUCCESS", "", {}
    try:
        out = fn() or {}
    except Exception as exc:  # recorded, then re-raised
        status, msg = "FAILED", f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        ctx.log.append({"run_id": ctx.run_id, "stage": stage, "status": status,
                        "seconds": round(time.time() - t, 2), "rows_out": json.dumps(out),
                        "message": msg, "run_ts": ctx.run_ts})
    return out


# ===========================================================================
# BRONZE
# ===========================================================================
def collect_table_inventory(ctx: Ctx) -> DataFrame | None:
    """System mode: table layout via information_schema + DESCRIBE DETAIL."""
    spark, rows = ctx.spark, []
    for cat in ctx.cfg.inventory_catalogs:
        names = spark.sql(
            f"SELECT table_catalog, table_schema, table_name, table_owner "
            f"FROM {cat}.information_schema.tables "
            f"WHERE table_schema <> 'information_schema' AND table_type IN ('MANAGED','EXTERNAL') "
            f"LIMIT {int(ctx.cfg.inventory_max_tables)}").collect()
        for r in names:
            fqn = f"`{r.table_catalog}`.`{r.table_schema}`.`{r.table_name}`"
            try:
                d = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0].asDict()
            except Exception:
                continue
            if d.get("format") != "delta":
                continue
            rows.append((r.table_catalog, r.table_schema, r.table_name, d.get("numFiles"),
                         d.get("sizeInBytes"), d.get("partitionColumns") or [],
                         d.get("clusteringColumns") or [], d.get("lastModified"), r.table_owner))
    if not rows:
        return None
    return spark.createDataFrame(rows, "table_catalog string, table_schema string, table_name string, "
                                       "num_files long, size_bytes long, partition_columns array<string>, "
                                       "clustering_columns array<string>, last_modified timestamp, "
                                       "table_owner string")


def run_bronze(ctx: Ctx) -> dict:
    status_rows, counts = [], {}
    for name, spec in SPECS.items():
        src = ctx.cfg.source(name)
        df, msg = None, ""
        try:
            if name == "table_inventory" and ctx.cfg.source_mode == "system":
                df = collect_table_inventory(ctx)
            elif src and ctx.exists(src):
                df = ctx.spark.table(src)
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"[:500]
        if df is None:
            # keep downstream stable: an empty, correctly-typed bronze table
            empty = ctx.spark.createDataFrame([], ", ".join(f"{a} {t}" for a, _, t in spec))
            counts[name] = ctx.save(empty.withColumn("_ingested_at", F.lit(ctx.run_ts)), "bronze", name)
            status_rows.append((name, src or "information_schema", "missing", 0, msg or "not found", ""))
            continue
        projected, missing_cols = project(df, spec)
        if name == "query_history" and not ctx.cfg.governance["retain_raw_sql"]:
            # data minimisation: raw SQL (literals, comments) is never persisted
            projected = projected.withColumn("statement_text", normalize_sql(F.col("statement_text")))
        if ctx.cfg.workspace_ids and "workspace_id" in projected.columns:
            # system tables are account-wide: keep only workspaces this deployment is authorised for
            projected = projected.filter(F.col("workspace_id").isin([str(w) for w in ctx.cfg.workspace_ids]))
        wcol = WINDOW_COL.get(name)
        if wcol:
            projected = projected.filter(
                (F.to_date(F.col(wcol)) >= F.lit(ctx.start_date)) &
                (F.to_date(F.col(wcol)) <= F.lit(ctx.end_date)))
        counts[name] = ctx.save(projected.withColumn("_ingested_at", F.lit(ctx.run_ts)), "bronze", name)
        status_rows.append((name, src or "information_schema", "available", counts[name], msg,
                            ",".join(missing_cols)))
    status = ctx.spark.createDataFrame(
        status_rows, "source string, table_name string, status string, rows long, message string, "
                     "missing_columns string").withColumn("run_id", F.lit(ctx.run_id)) \
        .withColumn("run_ts", F.lit(ctx.run_ts))
    ctx.save(status, "ops", "source_status")
    return counts


# ===========================================================================
# SILVER  (validated, de-duplicated, canonical)
# ===========================================================================
def _latest(df: DataFrame, key: str, order: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col(order).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def run_silver(ctx: Ctx) -> dict:
    cfg, b = ctx.cfg, lambda n: ctx.read("bronze", n)
    quarantine, dq, counts = [], [], {}

    def dq_track(source, raw: DataFrame, valid: DataFrame, invalid: DataFrame, final: DataFrame):
        reasons = {r["dq_reason"]: r["n"] for r in
                   invalid.groupBy("dq_reason").agg(F.count(F.lit(1)).alias("n")).collect()}
        n_raw, n_bad, n_final = raw.count(), sum(reasons.values()), final.count()
        dq.append((source, n_raw, n_final, n_bad, n_raw - n_bad - n_final, json.dumps(reasons)))
        quarantine.append(invalid.select(F.lit(source).alias("source"), "dq_reason",
                                         F.to_json(F.struct(*[c for c in invalid.columns
                                                              if c != "dq_reason"])).alias("record")))

    # ---- prices -------------------------------------------------------------
    prices = b("list_prices").filter(F.col("list_price").isNotNull())

    # ---- usage + cost ---------------------------------------------------------
    raw = b("billing_usage")
    valid, invalid = split_valid(raw, {
        "null_record_id": F.col("record_id").isNotNull(),
        "negative_quantity": F.col("usage_quantity") >= 0,
        "end_before_start": F.col("usage_end_time") >= F.col("usage_start_time"),
        "null_date": F.col("usage_date").isNotNull(),
    })
    dedup = valid.dropDuplicates(["record_id"])
    u, p = dedup.alias("u"), prices.alias("p")
    joined = u.join(p, (F.col("u.sku_name") == F.col("p.sku_name")) &
                    (F.col("u.usage_end_time") > F.col("p.price_start_time")) &
                    (F.col("p.price_end_time").isNull() |
                     (F.col("u.usage_end_time") <= F.col("p.price_end_time"))), "left") \
        .select("u.*", F.col("p.list_price"), F.col("p.currency_code"))
    contract = F.lit(None).cast("double")
    for pat, price in reversed(list(cfg.contract_prices.items())):   # first matching pattern wins
        contract = F.when(F.col("sku_name").like(pat), F.lit(float(price))).otherwise(contract)
    joined = (joined.withColumn("contract_price", contract)
              .withColumn("unit_price", F.coalesce(F.col("contract_price"),
                                                   F.col("list_price") * F.lit(float(cfg.price_multiplier))))
              .withColumn("price_source", F.when(F.col("contract_price").isNotNull(), "contract")
                          .when(F.col("list_price").isNotNull(), "list").otherwise(None)))
    no_price = joined.filter(F.col("unit_price").isNull()).withColumn("dq_reason", F.lit("no_price")) \
                     .drop("contract_price", "unit_price", "price_source")
    priced = joined.filter(F.col("unit_price").isNotNull())
    tag = F.expr(f"try_element_at(custom_tags, '{cfg.cost_center_tag}')")
    wk = workload_key(F.col("job_id"), F.col("warehouse_id"), F.col("dlt_pipeline_id"),
                      F.col("cluster_id"), F.col("notebook_id"), F.col("billing_origin_product"))
    usage_cost = priced.select(
        "record_id", "workspace_id", "usage_date", "usage_start_time", "usage_end_time", "sku_name",
        "billing_origin_product", F.col("usage_quantity").alias("dbus"), "list_price",
        F.lit(float(cfg.price_multiplier)).alias("price_multiplier"), "unit_price", "price_source",
        (F.col("usage_quantity") * F.col("unit_price")).alias("cost"),
        "currency_code",
        "job_id", "job_run_id", "cluster_id", "warehouse_id", "notebook_id", "dlt_pipeline_id",
        "run_as", tag.alias("cost_center"), tag.isNotNull().alias("is_tagged"),
        wk.alias("workload_key"), workload_type(wk).alias("workload_type"),
        ((F.unix_timestamp("usage_end_time") - F.unix_timestamp("usage_start_time")) / 3600.0)
        .alias("usage_hours"))
    dq_track("billing_usage", raw, valid, invalid.unionByName(no_price.select(*invalid.columns)), usage_cost)
    counts["usage_cost"] = ctx.save(usage_cost, "silver", "usage_cost")

    # ---- jobs (SCD: latest version) ----------------------------------------------
    jobs = _latest(b("jobs").filter(F.col("job_id").isNotNull()), "job_id", "change_time") \
        .filter(F.col("delete_time").isNull()) \
        .select("job_id", F.col("name").alias("job_name"), "creator_id", "run_as",
                F.expr(f"try_element_at(tags, '{cfg.cost_center_tag}')").alias("cost_center"))
    counts["jobs"] = ctx.save(jobs, "silver", "jobs")

    # ---- job runs (timeline slices -> one row per run) ------------------------
    raw = b("job_run_timeline")
    valid, invalid = split_valid(raw, {
        "null_run_id": F.col("run_id").isNotNull(),
        "end_before_start": F.col("period_end_time") >= F.col("period_start_time"),
    })
    slices = valid.dropDuplicates(["run_id", "period_start_time"])
    runs = (slices.groupBy("workspace_id", "job_id", "run_id")
            .agg(F.min("period_start_time").alias("start_time"),
                 F.max("period_end_time").alias("end_time"),
                 F.max("result_state").alias("result_state"),
                 F.max("termination_code").alias("termination_code"),
                 F.array_distinct(F.flatten(F.collect_list("compute_ids"))).alias("compute_ids"))
            .withColumn("duration_s", F.unix_timestamp("end_time") - F.unix_timestamp("start_time"))
            .withColumn("run_date", F.to_date("start_time"))
            .withColumn("is_success", F.col("result_state") == "SUCCEEDED")
            .withColumn("is_failure", F.col("result_state").isin("FAILED", "ERROR", "TIMED_OUT"))
            .join(jobs.select("job_id", "job_name", "cost_center"), "job_id", "left"))
    dq_track("job_run_timeline", raw, valid, invalid, slices)
    counts["job_runs"] = ctx.save(runs, "silver", "job_runs")

    # ---- task runs ----------------------------------------------------------------
    raw = b("job_task_run_timeline")
    valid, invalid = split_valid(raw, {
        "null_task_run_id": F.col("task_run_id").isNotNull(),
        "end_before_start": F.col("period_end_time") >= F.col("period_start_time"),
    })
    slices = valid.dropDuplicates(["task_run_id", "period_start_time"])
    tasks = (slices.groupBy("job_id", "job_run_id", "task_run_id", "task_key")
             .agg(F.min("period_start_time").alias("start_time"),
                  F.max("period_end_time").alias("end_time"),
                  F.max("result_state").alias("result_state"))
             .withColumn("duration_s", F.unix_timestamp("end_time") - F.unix_timestamp("start_time")))
    dq_track("job_task_run_timeline", raw, valid, invalid, slices)
    counts["task_runs"] = ctx.save(tasks, "silver", "task_runs")

    # ---- clusters (SCD: latest config) -------------------------------------------
    clusters = _latest(b("clusters").filter(F.col("cluster_id").isNotNull()), "cluster_id", "change_time") \
        .withColumn("is_all_purpose", F.col("cluster_source").isin("UI", "API")) \
        .withColumn("dbr_major", F.expr(r"try_cast(regexp_extract(dbr_version, '^(\\d+)', 1) AS INT)")) \
        .withColumn("cost_center", F.expr(f"try_element_at(tags, '{cfg.cost_center_tag}')"))
    counts["clusters"] = ctx.save(clusters, "silver", "clusters")

    # ---- utilisation ---------------------------------------------------------------
    raw = b("node_timeline")
    valid, invalid = split_valid(raw, {
        "null_cluster": F.col("cluster_id").isNotNull(),
        "cpu_out_of_range": F.col("cpu_user_percent").between(0, 100),
        "mem_out_of_range": F.col("mem_used_percent").between(0, 100),
    })
    clean = valid.dropDuplicates(["instance_id", "start_time"])
    util = (clean.withColumn("util_date", F.to_date("start_time"))
            .groupBy("cluster_id", "util_date")
            .agg(F.avg(F.col("cpu_user_percent") + F.coalesce(F.col("cpu_system_percent"), F.lit(0.0)))
                  .alias("avg_cpu"),
                 F.avg("mem_used_percent").alias("avg_mem"),
                 F.max("mem_used_percent").alias("max_mem"),
                 (F.sum(F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 3600.0)
                  .alias("node_hours")))
    dq_track("node_timeline", raw, valid, invalid, clean)
    counts["cluster_util_daily"] = ctx.save(util, "silver", "cluster_util_daily")

    # ---- queries -------------------------------------------------------------------
    raw = b("query_history")
    valid, invalid = split_valid(raw, {
        "null_statement_id": F.col("statement_id").isNotNull(),
        "null_start": F.col("start_time").isNotNull(),
        "negative_duration": F.col("total_duration_ms") >= 0,
    })
    dedup = valid.dropDuplicates(["statement_id"])
    queries = dedup.select(
        "statement_id", "workspace_id", "executed_by", "execution_status", "statement_type",
        "warehouse_id", "cluster_id", "compute_type", "total_duration_ms", "execution_duration_ms",
        "waiting_at_capacity_duration_ms", "total_task_duration_ms", "read_bytes", "read_files",
        "pruned_files", "spilled_local_bytes", "shuffle_read_bytes", "from_result_cache", "start_time",
        F.to_date("start_time").alias("query_date"),
        query_fingerprint(F.col("statement_text")).alias("fingerprint"),
        F.substring(normalize_sql(F.col("statement_text")), 1, 300).alias("statement_shape"))
    dq_track("query_history", raw, valid, invalid, queries)
    counts["queries"] = ctx.save(queries, "silver", "queries")

    # ---- table layout ---------------------------------------------------------------
    layout = b("table_inventory").filter(F.col("table_name").isNotNull()).select(
        "*", F.concat_ws(".", "table_catalog", "table_schema", "table_name").alias("fqn"),
        (safe_div(F.col("size_bytes").cast("double"), F.col("num_files").cast("double")) / 1048576.0)
        .alias("avg_file_mb"))
    counts["table_layout"] = ctx.save(layout, "silver", "table_layout")

    # ---- DQ outputs -------------------------------------------------------------------
    dq_df = ctx.spark.createDataFrame(
        dq, "source string, rows_in long, rows_out long, rows_quarantined long, "
            "duplicates_removed long, reasons string") \
        .withColumn("run_id", F.lit(ctx.run_id)).withColumn("run_ts", F.lit(ctx.run_ts))
    ctx.save(dq_df, "ops", "dq_report")
    q = quarantine[0]
    for x in quarantine[1:]:
        q = q.unionByName(x)
    counts["quarantine"] = ctx.save(q.withColumn("run_id", F.lit(ctx.run_id)), "ops", "quarantine")
    return counts


# ===========================================================================
# GOLD  (attribution, scorecards)
# ===========================================================================
def _workload_cost_center(ctx: Ctx, usage: DataFrame, clusters: DataFrame) -> DataFrame:
    """Owning cost centre per workload: drives row-level visibility in reporting views."""
    jobs = ctx.read("silver", "jobs")
    parts = [
        usage.select("workload_key", "cost_center"),
        jobs.select(F.concat(F.lit("job:"), "job_id").alias("workload_key"), "cost_center"),
        clusters.select(F.concat(F.lit("cluster:"), "cluster_id").alias("workload_key"), "cost_center"),
    ]
    u = parts[0].unionByName(parts[1]).unionByName(parts[2]).filter(F.col("cost_center").isNotNull())
    return u.groupBy("workload_key").agg(F.max("cost_center").alias("cost_center"))


def run_gold(ctx: Ctx) -> dict:
    s = lambda n: ctx.read("silver", n)
    usage, runs, clusters = s("usage_cost"), s("job_runs"), s("clusters")
    counts = {}

    # ---- 1. per-run cost (direct) ---------------------------------------------
    run_cost = (usage.filter(F.col("job_run_id").isNotNull())
                .groupBy("job_id", "job_run_id").agg(F.sum("cost").alias("cost"), F.sum("dbus").alias("dbus")))
    counts["run_cost"] = ctx.save(run_cost, "gold", "run_cost")

    # ---- 2. all-purpose cluster cost -> jobs that ran on it (allocated) -------
    ap_day = (usage.filter(F.col("workload_type") == "cluster")
              .groupBy("cluster_id", F.col("usage_date").alias("d"))
              .agg(F.sum("cost").alias("cost"), F.sum("dbus").alias("dbus"),
                   F.sum("usage_hours").alias("uptime_h"),
                   F.sum(F.when(~F.col("is_tagged"), F.col("cost")).otherwise(0.0)).alias("untagged_cost")))
    ap_ids = clusters.filter("is_all_purpose").select("cluster_id")
    job_on = (runs.select("job_id", "run_date", "duration_s", F.explode("compute_ids").alias("cluster_id"))
              .join(ap_ids, "cluster_id")
              .groupBy("cluster_id", F.col("run_date").alias("d"), "job_id")
              .agg((F.sum("duration_s") / 3600.0).alias("job_h")))
    tot = job_on.groupBy("cluster_id", "d").agg(F.sum("job_h").alias("tot_h"))
    shares = (ap_day.join(tot, ["cluster_id", "d"], "left").fillna({"tot_h": 0.0})
              .withColumn("alloc_frac", F.least(F.lit(1.0), F.coalesce(safe_div(F.col("tot_h"), F.col("uptime_h")),
                                                                         F.lit(0.0)))))
    allocated = (shares.join(job_on, ["cluster_id", "d"])
                 .withColumn("w", F.col("alloc_frac") * F.col("job_h") / F.col("tot_h"))
                 .select(F.concat(F.lit("job:"), "job_id").alias("workload_key"), F.lit("job").alias("workload_type"),
                         F.col("d").alias("usage_date"), F.lit("allocated").alias("attribution"),
                         (F.col("cost") * F.col("w")).alias("cost"), (F.col("dbus") * F.col("w")).alias("dbus"),
                         (F.col("untagged_cost") * F.col("w")).alias("untagged_cost")))
    residual = shares.select(
        F.concat(F.lit("cluster:"), "cluster_id").alias("workload_key"), F.lit("cluster").alias("workload_type"),
        F.col("d").alias("usage_date"), F.lit("residual_idle").alias("attribution"),
        (F.col("cost") * (1 - F.col("alloc_frac"))).alias("cost"),
        (F.col("dbus") * (1 - F.col("alloc_frac"))).alias("dbus"),
        (F.col("untagged_cost") * (1 - F.col("alloc_frac"))).alias("untagged_cost"))
    direct = (usage.filter(F.col("workload_type") != "cluster")
              .groupBy("workload_key", "workload_type", "usage_date")
              .agg(F.sum("cost").alias("cost"), F.sum("dbus").alias("dbus"),
                   F.sum(F.when(~F.col("is_tagged"), F.col("cost")).otherwise(0.0)).alias("untagged_cost"))
              .withColumn("attribution", F.lit("direct")))
    cols = ["workload_key", "workload_type", "usage_date", "attribution", "cost", "dbus", "untagged_cost"]
    daily = (direct.select(*cols).unionByName(allocated.select(*cols)).unionByName(residual.select(*cols))
             .groupBy("workload_key", "workload_type", "usage_date", "attribution")
             .agg(F.sum("cost").alias("cost"), F.sum("dbus").alias("dbus"),
                  F.sum("untagged_cost").alias("untagged_cost")))
    counts["workload_cost_daily"] = ctx.save(daily, "gold", "workload_cost_daily")

    # ---- 3. workload summary ---------------------------------------------------
    total = daily.agg(F.sum("cost")).collect()[0][0] or 0.0
    summary = (daily.groupBy("workload_key", "workload_type")
               .agg(F.sum("cost").alias("window_cost"), F.sum("dbus").alias("window_dbus"),
                    F.sum("untagged_cost").alias("untagged_cost"),
                    F.countDistinct("usage_date").alias("active_days"),
                    F.array_sort(F.collect_set("attribution")).alias("attribution_types"))
               .join(_workload_cost_center(ctx, usage, clusters), "workload_key", "left")
               .withColumn("monthly_cost", F.col("window_cost") * F.lit(ctx.monthly))
               .withColumn("share_of_total", safe_div(F.col("window_cost"), F.lit(total)))
               .withColumn("cost_rank", F.dense_rank().over(Window.orderBy(F.col("window_cost").desc()))))
    counts["workload_summary"] = ctx.save(summary, "gold", "workload_summary")

    # ---- 4. job scorecard -----------------------------------------------------------
    job_cost = (daily.filter(F.col("workload_type") == "job")
                .groupBy(F.expr("substring(workload_key, 5)").alias("job_id"))
                .agg(F.sum("cost").alias("window_cost")))
    sc = (runs.groupBy("job_id", "job_name", "cost_center")
          .agg(F.count(F.lit(1)).alias("runs"), F.sum(F.col("is_success").cast("int")).alias("succeeded"),
               F.sum(F.col("is_failure").cast("int")).alias("failed"),
               F.percentile_approx("duration_s", 0.5).alias("p50_duration_s"),
               F.percentile_approx("duration_s", 0.95).alias("p95_duration_s"))
          .join(job_cost, "job_id", "left")
          .withColumn("failure_rate", safe_div(F.col("failed").cast("double"), F.col("runs").cast("double")))
          .withColumn("cost_per_success", safe_div(F.col("window_cost"), F.col("succeeded").cast("double")))
          .withColumn("monthly_cost", F.col("window_cost") * F.lit(ctx.monthly)))
    counts["job_scorecard"] = ctx.save(sc, "gold", "job_scorecard")

    # ---- 5. query cost: warehouse-day cost spread by task time -----------------
    q = s("queries")
    wh_day = (daily.filter(F.col("workload_type") == "warehouse")
              .select(F.expr("substring(workload_key, 11)").alias("warehouse_id"),
                      F.col("usage_date").alias("query_date"), "cost"))
    wt = F.coalesce(F.col("total_task_duration_ms"), F.col("total_duration_ms"), F.lit(0)).cast("double")
    qw = q.withColumn("wt", wt)
    qtot = qw.groupBy("warehouse_id", "query_date").agg(F.sum("wt").alias("wt_tot"))
    qcost = (qw.join(qtot, ["warehouse_id", "query_date"], "left").join(wh_day, ["warehouse_id", "query_date"], "left")
             .withColumn("query_cost", F.coalesce(F.col("cost") * safe_div(F.col("wt"), F.col("wt_tot")), F.lit(0.0))))
    fp = (qcost.groupBy("fingerprint")
          .agg(F.min("statement_shape").alias("statement_shape"),
               F.array_sort(F.collect_set("warehouse_id")).alias("warehouse_ids"),
               F.count(F.lit(1)).alias("executions"), F.countDistinct("query_date").alias("active_days"),
               F.sum("query_cost").alias("window_cost"), F.avg("total_duration_ms").alias("avg_duration_ms"),
               F.avg("read_bytes").alias("avg_read_bytes"), F.avg("read_files").alias("avg_read_files"),
               F.avg("pruned_files").alias("avg_pruned_files"),
               F.avg((F.col("spilled_local_bytes") > 1024**3).cast("double")).alias("spill_share"),
               F.max("spilled_local_bytes").alias("max_spill_bytes"),
               F.avg(F.col("from_result_cache").cast("double")).alias("cache_hit_rate"),
               F.min("statement_id").alias("sample_statement_id"))
          .withColumn("monthly_cost", F.col("window_cost") * F.lit(ctx.monthly)))
    counts["query_fingerprint_stats"] = ctx.save(fp, "gold", "query_fingerprint_stats")

    # ---- 6. cost by dimension (dashboard slicing) --------------------------------
    dims = usage.groupBy("usage_date", "workspace_id", "billing_origin_product", "sku_name",
                         F.coalesce("cost_center", F.lit("(untagged)")).alias("cost_center"),
                         F.coalesce("run_as", F.lit("(unknown)")).alias("run_as")) \
                .agg(F.sum("dbus").alias("dbus"), F.sum("cost").alias("cost"))
    counts["cost_by_dimension"] = ctx.save(dims, "gold", "cost_by_dimension")
    return counts


# ===========================================================================
# Orchestration
# ===========================================================================
DEFAULT_STAGES = ("bronze", "silver", "gold", "rules", "recommend", "narrate", "govern")


def run_all(spark: SparkSession, cfg: DPOConfig, stages: tuple = DEFAULT_STAGES,
            run_id: str | None = None) -> Ctx:
    from . import governance
    from .recommend import run_narratives, run_recommendations
    from .rules import run_rules

    ctx = Ctx(spark, cfg, run_id)
    # 1. who is asking? Refuse before reading or writing any data.
    ok, reason = governance.is_authorized(spark, cfg)
    if not ok:
        governance.record_audit(spark, cfg, ctx.run_id, "authorize", "DENIED", reason)
        raise PermissionError(reason)
    ensure_schemas(ctx)
    governance.record_audit(spark, cfg, ctx.run_id, "authorize", "ALLOWED", reason)
    table = {"bronze": run_bronze, "silver": run_silver, "gold": run_gold, "rules": run_rules,
             "recommend": run_recommendations, "narrate": run_narratives,
             "govern": governance.run_govern}
    status = "FAILED"
    try:
        for st in stages:
            _timed(ctx, st, lambda st=st: table[st](ctx))
        status = "SUCCESS"
    finally:
        governance.record_audit(spark, cfg, ctx.run_id, "pipeline_run", status,
                                f"stages={','.join(stages)} window={ctx.start_date}..{ctx.end_date}")
        log = spark.createDataFrame(ctx.log, "run_id string, stage string, status string, seconds double, "
                                             "rows_out string, message string, run_ts timestamp")
        name = ctx.t("ops", "run_log")
        mode = "append" if ctx.exists(name) else "overwrite"
        log.write.format(cfg.table_format).mode(mode).saveAsTable(name)
    return ctx
