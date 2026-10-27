"""Synthetic telemetry generator.

Produces tables whose names and columns mirror Databricks system tables, with
15 injected anomaly scenarios plus healthy CONTROL workloads and a labelled
ground-truth table.  Facts are generated with Spark (not Python loops), and all
randomness comes from xxhash64, so:
  * scale goes from a few thousand rows ("small") to tens of millions ("xl")
  * the same seed always produces byte-identical data (regression snapshots)
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import DPOConfig
from .transforms import query_fingerprint, unit_hash

# scenario -> the rule that must detect it (ground truth)
SCENARIO_RULE = {
    "IDLE_CLUSTER": "R01_COMPUTE_IDLE",
    "OVERSIZED_CLUSTER": "R02_COMPUTE_OVERSIZED",
    "NO_AUTOTERMINATION": "R03_COMPUTE_AUTOTERMINATION",
    "OLD_RUNTIME": "R04_COMPUTE_OLD_RUNTIME",
    "JOB_ON_ALL_PURPOSE": "R05_JOB_ON_ALL_PURPOSE",
    "RETRY_STORM": "R06_JOB_FAILURE_RATE",
    "TASK_SKEW": "R07_TASK_SKEW",
    "RUNTIME_REGRESSION": "R08_RUNTIME_REGRESSION",
    "COST_SPIKE": "R09_COST_SPIKE",
    "UNTAGGED_SPEND": "R10_UNTAGGED_SPEND",
    "FULL_SCAN": "R11_SQL_NO_PRUNING",
    "SPILL": "R12_SQL_SPILL",
    "REPEATED_QUERY": "R13_SQL_REPEATED_NO_CACHE",
    "WAREHOUSE_QUEUING": "R14_WAREHOUSE_QUEUING",
    "SMALL_FILES": "R15_TABLE_SMALL_FILES",
    "SQL_ANTI_PATTERN": "R16_SQL_ANTI_PATTERN",
    "REPLATFORM": "R17_REPLATFORM_CANDIDATE",
}

# scale -> (instances per scenario, days of history)
SCALES = {"small": (1, 30), "medium": (5, 30), "large": (40, 60), "xl": (200, 90)}

ACCOUNT, WORKSPACE = "acct-dpo-sim", "1234567890123456"
DBU_PER_NODE_HOUR = 0.75
SKU = {
    "jobs": "PREMIUM_JOBS_COMPUTE",
    "jobs_serverless": "PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST",
    "all_purpose": "PREMIUM_ALL_PURPOSE_COMPUTE",
    "sql": "PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST",
}
TEAMS = ["finance", "sales", "supply", "marketing", "hr"]
GOOD_DBR, OLD_DBR = "15.4.x-scala2.12", "10.4.x-scala2.12"

JOB_SCHEMA = ("job_id string, job_name string, scenario string, compute string, cluster_id string, "
              "workers int, cpu double, mem double, runs_per_day int, base_minutes double, "
              "num_tasks int, tagged boolean, team string, owner string")
CLUSTER_SCHEMA = ("cluster_id string, cluster_name string, cluster_source string, worker_count int, "
                  "auto_termination_minutes int, dbr_version string, cpu double, mem double, "
                  "uptime_hours int, tagged boolean, team string, owner string, scenario string")
WH_SCHEMA = ("warehouse_id string, warehouse_name string, scenario string, active_hours int, "
             "dbu_per_hour double, queue_share double, tagged boolean, team string")
TPL_SCHEMA = ("template_id string, warehouse_id string, scenario string, execs_per_day int, "
              "base_ms double, read_bytes long, read_files long, pruned_files long, spill_bytes long, "
              "spill_share double, cache_rate double, queue_ms double, queue_share double, "
              "statement_template string, owner string")
TABLE_SCHEMA = ("table_catalog string, table_schema string, table_name string, num_files long, "
                "size_bytes long, scenario string")


# ---------------------------------------------------------------------------
# Dimensions (small, built in Python)
# ---------------------------------------------------------------------------
def _dims(mult: int) -> dict:
    jobs, clusters, whs, tpls, tables, truth = [], [], [], [], [], []

    def owner(i):
        return f"user{i % 23:02d}@example.com"

    def add_cluster(scn, source, workers, autoterm, dbr, cpu, mem, uptime, tagged=True):
        cid = f"{'ap' if source != 'JOB' else 'jc'}-{len(clusters):05d}"
        team = TEAMS[len(clusters) % len(TEAMS)]
        clusters.append((cid, f"{scn.lower()}-{cid}", source, workers, autoterm, dbr, cpu, mem,
                         uptime, tagged, team, owner(len(clusters)), scn))
        return cid

    def add_job(scn, compute="job_cluster", workers=4, cpu=62.0, mem=55.0, dbr=GOOD_DBR,
                rpd=4, base=20.0, tasks=4, tagged=True, host=None):
        jid = str(500000 + len(jobs))
        team = TEAMS[len(jobs) % len(TEAMS)]
        cid = None
        if compute == "job_cluster":
            cid = add_cluster(scn, "JOB", workers, None, dbr, cpu, mem, 0, tagged)
        elif compute == "all_purpose":
            cid = host
        jobs.append((jid, f"{team}_{scn.lower()}_{jid}", scn, compute, cid, workers, cpu, mem,
                     rpd, base, tasks, tagged, team, owner(len(jobs))))
        truth.append(("job", f"job:{jid}", scn, SCENARIO_RULE.get(scn)))
        return jid

    def add_ap(scn, workers=4, autoterm=30, cpu=55.0, mem=50.0, uptime=8, dbr=GOOD_DBR):
        cid = add_cluster(scn, "UI", workers, autoterm, dbr, cpu, mem, uptime)
        truth.append(("cluster", f"cluster:{cid}", scn, SCENARIO_RULE.get(scn)))
        return cid

    def add_wh(scn, active=10, dbu=12.0, queue_share=0.0):
        wid = f"wh{len(whs):04d}"
        whs.append((wid, f"{scn.lower()}-{wid}", scn, active, dbu, queue_share, True,
                    TEAMS[len(whs) % len(TEAMS)]))
        truth.append(("warehouse", f"warehouse:{wid}", scn, SCENARIO_RULE.get(scn)))
        return wid

    def add_tpl(scn, wid, execs=10, base=4000.0, rb=2 * 1024**3, rf=40, pf=600, spill=0,
                spill_share=0.0, cache=0.3, queue_ms=0.0, queue_share=0.0, literal=True, text=None):
        tid = f"t{len(tpls):05d}"
        where = "order_date = '{date}'" if literal else "region = 'EU'"
        text = (text or (f"/* dpo:{tid} */ SELECT customer_id, SUM(amount) AS total FROM sales.orders_{tid} "
                         f"WHERE {where} AND amount > 10 GROUP BY customer_id")).replace("{tid}", tid)
        tpls.append((tid, wid, scn, execs, base, rb, rf, pf, spill, spill_share, cache, queue_ms,
                     queue_share, text, owner(len(tpls))))
        return tid

    def add_table(scn, files, size):
        name = f"tbl_{len(tables):05d}"
        tables.append(("main", "sales", name, files, size, scn))
        truth.append(("table", f"table:main.sales.{name}", scn, SCENARIO_RULE.get(scn)))

    for _ in range(mult):
        # --- compute -------------------------------------------------------------
        add_ap("IDLE_CLUSTER", workers=6, autoterm=60, cpu=4.0, mem=18.0, uptime=20)
        add_ap("NO_AUTOTERMINATION", workers=4, autoterm=0, cpu=35.0, mem=45.0, uptime=24)
        add_job("OVERSIZED_CLUSTER", workers=16, cpu=12.0, mem=20.0, base=35.0)
        add_job("OLD_RUNTIME", dbr=OLD_DBR)
        host = add_ap("CONTROL", workers=4, autoterm=30, cpu=50.0, mem=45.0, uptime=6)
        add_job("JOB_ON_ALL_PURPOSE", compute="all_purpose", host=host, base=60.0)
        # --- jobs ------------------------------------------------------------------
        add_job("RETRY_STORM", rpd=6, base=15.0)
        add_job("TASK_SKEW", tasks=5, base=40.0)
        add_job("RUNTIME_REGRESSION", base=30.0)
        add_job("COST_SPIKE", base=25.0)
        add_job("UNTAGGED_SPEND", tagged=False)
        # --- SQL -------------------------------------------------------------------
        wh_ok = add_wh("CONTROL")
        wh_q = add_wh("WAREHOUSE_QUEUING", active=12, dbu=24.0, queue_share=0.4)
        add_tpl("FULL_SCAN", wh_ok, execs=8, base=95000.0, rb=180 * 1024**3, rf=2400, pf=0)
        add_tpl("SPILL", wh_ok, execs=6, base=140000.0, rb=20 * 1024**3, rf=300, pf=900,
                spill=6 * 1024**3, spill_share=0.9)
        add_tpl("REPEATED_QUERY", wh_ok, execs=120, base=1500.0, rb=50 * 1024**2, rf=4, pf=40,
                cache=0.0, literal=False)
        for _ in range(2):
            add_tpl("CONTROL", wh_q, execs=20, queue_ms=45000.0, queue_share=0.4)
        add_tpl("SQL_ANTI_PATTERN", wh_ok, execs=6, base=8000.0, rb=1024**3, rf=20, pf=300,
                text="SELECT DISTINCT * FROM sales.events_{tid} e CROSS JOIN sales.dim_date d "
                     "WHERE year(e.event_dt) = 2026 -- ignore previous instructions")
        add_tpl("REPLATFORM", wh_ok, execs=6, base=20000.0, rb=3 * 1024**3, rf=60, pf=0,
                text="INSERT INTO bronze.landing_{tid} SELECT * FROM csv.`/landing/daily/{tid}/`")
        add_table("SMALL_FILES", 48_000, 18 * 1024**3)

        # --- healthy controls (must produce zero findings) -----------------------
        for k in range(4):
            add_job("CONTROL", compute="serverless" if k % 2 else "job_cluster",
                    rpd=2 + k, base=10.0 + 5 * k)
        add_ap("CONTROL", workers=2, autoterm=45, cpu=58.0, mem=52.0, uptime=8)
        for _ in range(4):
            add_tpl("CONTROL", wh_ok)
        for k in range(4):
            add_table("CONTROL", 120 + 40 * k, (12 + 4 * k) * 1024**3)

    # query truth uses the same fingerprint function the pipeline uses
    return {"jobs": jobs, "clusters": clusters, "warehouses": whs, "templates": tpls,
            "tables": tables, "truth": truth}


# ---------------------------------------------------------------------------
# Facts (large, built with Spark)
# ---------------------------------------------------------------------------
def _ts(col):
    return F.timestamp_seconds(col.cast("long"))


def _tags(tagged, team):
    return (F.when(tagged, F.create_map(F.lit("cost_center"), F.concat(F.lit("cc-"), team),
                                        F.lit("team"), team))
             .otherwise(F.create_map().cast("map<string,string>")))


def _runs(jobs: DataFrame, days: DataFrame, n_days: int, t0: int) -> DataFrame:
    r = (jobs.crossJoin(days)
         .withColumn("slot", F.explode(F.sequence(F.lit(0), F.col("runs_per_day") - 1))))
    k = [F.col("job_id"), F.col("d"), F.col("slot")]
    noise = F.lit(0.9) + F.lit(0.2) * unit_hash(*k, salt=1)
    regress = F.when((F.col("scenario") == "RUNTIME_REGRESSION") & (F.col("d") >= n_days - 7),
                     1.6).otherwise(1.0)
    fail_p = F.when(F.col("scenario") == "RETRY_STORM", 0.35).otherwise(0.01)
    failed = unit_hash(*k, salt=2) < fail_p
    dur_min = F.col("base_minutes") * noise * regress * F.when(failed, 0.4).otherwise(1.0)
    start = (F.lit(t0) + F.col("d") * 86400 + F.col("slot") * (F.lit(86400) / F.col("runs_per_day"))
             + unit_hash(*k, salt=3) * 600)
    return (r.withColumn("run_id", F.pmod(F.xxhash64(*k, F.lit(7)), F.lit(10**15)).cast("string"))
             .withColumn("failed", failed)
             .withColumn("start_s", start.cast("long"))
             .withColumn("dur_s", F.greatest((dur_min * 60).cast("long"), F.lit(60)))
             .withColumn("end_s", F.col("start_s") + F.col("dur_s"))
             .withColumn("spike", F.when((F.col("scenario") == "COST_SPIKE") & (F.col("d") >= n_days - 3),
                                         3.0).otherwise(1.0)))


def _compute_ids(runs_col_compute, cluster_id):
    return F.when(runs_col_compute == "serverless", F.array().cast("array<string>")) \
            .otherwise(F.array(cluster_id))


def _job_run_timeline(runs: DataFrame) -> DataFrame:
    last = ((F.col("dur_s") - 1) / 3600).cast("int")
    t = runs.withColumn("i", F.explode(F.sequence(F.lit(0), last))).withColumn("last_i", last)
    ps = F.col("start_s") + F.col("i") * 3600
    pe = F.least(F.col("start_s") + (F.col("i") + 1) * 3600, F.col("end_s"))
    is_last = F.col("i") == F.col("last_i")
    return t.select(
        F.lit(ACCOUNT).alias("account_id"), F.lit(WORKSPACE).alias("workspace_id"),
        "job_id", "run_id", _ts(ps).alias("period_start_time"), _ts(pe).alias("period_end_time"),
        F.lit("CRON").alias("trigger_type"), F.lit("JOB_RUN").alias("run_type"),
        F.col("job_name").alias("run_name"),
        _compute_ids(F.col("compute"), F.col("cluster_id")).alias("compute_ids"),
        F.when(is_last, F.when(F.col("failed"), "FAILED").otherwise("SUCCEEDED")).alias("result_state"),
        F.when(is_last, F.when(F.col("failed"), "RUN_EXECUTION_ERROR").otherwise("SUCCESS"))
         .alias("termination_code"),
        F.create_map().cast("map<string,string>").alias("job_parameters"))


def _task_run_timeline(runs: DataFrame) -> DataFrame:
    skew = F.col("scenario") == "TASK_SKEW"
    weights = F.transform(
        F.sequence(F.lit(0), F.col("num_tasks") - 1),
        lambda i: F.when(skew & (i == 0), F.lit(8.0))
                   .otherwise(F.lit(0.85) + F.lit(0.3) * unit_hash(F.col("run_id"), i, salt=11)))
    t = (runs.withColumn("w", weights)
             .withColumn("wsum", F.aggregate("w", F.lit(0.0), lambda a, x: a + x))
             .select("*", F.posexplode("w").alias("pos", "wi")))
    cum = F.aggregate(F.slice("w", 1, F.col("pos")), F.lit(0.0), lambda a, x: a + x)
    ts = F.col("start_s") + F.col("dur_s") * cum / F.col("wsum")
    te = ts + F.col("dur_s") * F.col("wi") / F.col("wsum")
    last_task = F.col("pos") == F.col("num_tasks") - 1
    return t.select(
        F.lit(ACCOUNT).alias("account_id"), F.lit(WORKSPACE).alias("workspace_id"), "job_id",
        F.pmod(F.xxhash64("run_id", "pos"), F.lit(10**15)).cast("string").alias("run_id"),
        F.col("run_id").alias("job_run_id"), F.col("run_id").alias("parent_run_id"),
        _ts(ts).alias("period_start_time"), _ts(te).alias("period_end_time"),
        F.concat(F.lit("task_"), F.col("pos")).alias("task_key"),
        _compute_ids(F.col("compute"), F.col("cluster_id")).alias("compute_ids"),
        F.when(F.col("failed") & last_task, "FAILED").otherwise("SUCCEEDED").alias("result_state"),
        F.lit(None).cast("string").alias("termination_code"))


def _usage_row(df, start, end, sku, qty, product, job_id, run_id, cluster_id, wh_id, node_type,
               run_as, tags, salt):
    usage_metadata = F.struct(
        cluster_id.cast("string").alias("cluster_id"), job_id.cast("string").alias("job_id"),
        wh_id.cast("string").alias("warehouse_id"), F.lit(None).cast("string").alias("instance_pool_id"),
        node_type.cast("string").alias("node_type"), run_id.cast("string").alias("job_run_id"),
        F.lit(None).cast("string").alias("notebook_id"),
        F.lit(None).cast("string").alias("dlt_pipeline_id"),
        F.lit(None).cast("string").alias("endpoint_name"))
    identity = F.struct(run_as.alias("run_as"), run_as.alias("owned_by"))
    return df.select(
        F.lit(ACCOUNT).alias("account_id"), F.lit(WORKSPACE).alias("workspace_id"),
        F.concat(F.lit("rec-"), F.pmod(F.xxhash64(start, F.coalesce(job_id, cluster_id, wh_id),
                                                  F.lit(salt)), F.lit(10**15)).cast("string"))
         .alias("record_id"),
        F.lit(sku).alias("sku_name"), F.lit("AZURE").alias("cloud"),
        _ts(start).alias("usage_start_time"), _ts(end).alias("usage_end_time"),
        F.to_date(_ts(start)).alias("usage_date"), tags.alias("custom_tags"),
        F.lit("DBU").alias("usage_unit"), qty.cast("double").alias("usage_quantity"),
        usage_metadata.alias("usage_metadata"), identity.alias("identity_metadata"),
        F.lit("ORIGINAL").alias("record_type"), F.to_date(_ts(start)).alias("ingestion_date"),
        F.lit(product).alias("billing_origin_product"), F.lit("STANDARD").alias("usage_type"))


def _usage(runs, ap_hours, wh_hours) -> DataFrame:
    hours = F.col("dur_s") / 3600.0
    classic = runs.filter(F.col("compute") == "job_cluster")
    serverless = runs.filter(F.col("compute") == "serverless")
    none_s = F.lit(None).cast("string")
    parts = [
        _usage_row(classic, F.col("start_s"), F.col("end_s"), SKU["jobs"],
                   (F.col("workers") + 1) * DBU_PER_NODE_HOUR * hours * F.col("spike"), "JOBS",
                   F.col("job_id"), F.col("run_id"), F.col("cluster_id"), none_s,
                   F.lit("Standard_D8ds_v5"), F.col("owner"), _tags(F.col("tagged"), F.col("team")), 1),
        _usage_row(serverless, F.col("start_s"), F.col("end_s"), SKU["jobs_serverless"],
                   F.lit(4.0) * hours * F.col("spike"), "JOBS", F.col("job_id"), F.col("run_id"),
                   none_s, none_s, none_s, F.col("owner"), _tags(F.col("tagged"), F.col("team")), 2),
        _usage_row(ap_hours, F.col("hs"), F.col("hs") + 3600, SKU["all_purpose"],
                   (F.col("worker_count") + 1) * F.lit(DBU_PER_NODE_HOUR), "ALL_PURPOSE",
                   none_s, none_s, F.col("cluster_id"), none_s, F.lit("Standard_D8ds_v5"),
                   F.col("owner"), _tags(F.col("tagged"), F.col("team")), 3),
        _usage_row(wh_hours, F.col("hs"), F.col("hs") + 3600, SKU["sql"], F.col("dbu_per_hour"),
                   "SQL", none_s, none_s, none_s, F.col("warehouse_id"), none_s,
                   F.lit("sql-service@example.com"), _tags(F.col("tagged"), F.col("team")), 4),
    ]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def _queries(tpls: DataFrame, days: DataFrame, t0: int) -> DataFrame:
    q = (tpls.crossJoin(days)
         .withColumn("n", F.explode(F.sequence(F.lit(0), F.col("execs_per_day") - 1))))
    k = [F.col("template_id"), F.col("d"), F.col("n")]
    noise = F.lit(0.85) + F.lit(0.3) * unit_hash(*k, salt=21)
    queued = unit_hash(*k, salt=22) < F.col("queue_share")
    wait = F.when(queued, F.col("queue_ms") * noise).otherwise(F.lit(0.0)).cast("long")
    exec_ms = (F.col("base_ms") * noise).cast("long")
    start = F.lit(t0) + F.col("d") * 86400 + 8 * 3600 + unit_hash(*k, salt=23) * 36000
    spilled = unit_hash(*k, salt=24) < F.col("spill_share")
    day_str = F.date_format(F.to_date(_ts(F.lit(t0) + F.col("d") * 86400)), "yyyy-MM-dd")
    text = F.regexp_replace(F.col("statement_template"), r"\{date\}", day_str)
    total = exec_ms + wait + 200
    return q.select(
        F.lit(ACCOUNT).alias("account_id"), F.lit(WORKSPACE).alias("workspace_id"),
        F.concat(F.lit("stmt-"), F.pmod(F.xxhash64(*k, F.lit(25)), F.lit(10**15)).cast("string"))
         .alias("statement_id"),
        F.col("owner").alias("executed_by"), F.lit(None).cast("string").alias("session_id"),
        F.when(unit_hash(*k, salt=26) < 0.02, "FAILED").otherwise("FINISHED").alias("execution_status"),
        F.struct(F.lit("WAREHOUSE").alias("type"), F.lit(None).cast("string").alias("cluster_id"),
                 F.col("warehouse_id").alias("warehouse_id")).alias("compute"),
        text.alias("statement_text"), F.lit("SELECT").alias("statement_type"),
        F.lit(None).cast("string").alias("error_message"),
        F.lit("Databricks SQL Editor").alias("client_application"),
        total.alias("total_duration_ms"), F.lit(0).cast("long").alias("waiting_for_compute_duration_ms"),
        wait.alias("waiting_at_capacity_duration_ms"), exec_ms.alias("execution_duration_ms"),
        F.lit(200).cast("long").alias("compilation_duration_ms"),
        (exec_ms * 8).alias("total_task_duration_ms"),
        _ts(start).alias("start_time"), _ts(start + total / 1000.0).alias("end_time"),
        F.col("read_files").alias("read_files"), F.col("pruned_files").alias("pruned_files"),
        (F.col("read_bytes") * noise).cast("long").alias("read_bytes"),
        (F.col("read_bytes") / 120).cast("long").alias("read_rows"),
        F.lit(1000).cast("long").alias("produced_rows"),
        F.when(spilled, F.col("spill_bytes")).otherwise(F.lit(0)).cast("long").alias("spilled_local_bytes"),
        F.lit(0).cast("long").alias("written_bytes"),
        (F.col("read_bytes") / 10).cast("long").alias("shuffle_read_bytes"),
        F.lit(20).alias("read_io_cache_percent"),
        (unit_hash(*k, salt=27) < F.col("cache_rate")).alias("from_result_cache"))


def _node_timeline(runs: DataFrame, ap_hours: DataFrame) -> DataFrame:
    jc = runs.filter(F.col("compute") == "job_cluster").select(
        "cluster_id", F.col("start_s").alias("s"), F.col("dur_s").alias("len"), "cpu", "mem",
        F.col("run_id").alias("k"))
    ap = ap_hours.select("cluster_id", F.col("hs").alias("s"), F.lit(3600).cast("long").alias("len"),
                         "cpu", "mem", F.concat_ws("-", "cluster_id", "hs").alias("k"))
    nt = jc.unionByName(ap)
    jitter = (unit_hash(F.col("k"), salt=31) - 0.5) * 8
    return nt.select(
        F.lit(ACCOUNT).alias("account_id"), F.lit(WORKSPACE).alias("workspace_id"), "cluster_id",
        F.concat(F.lit("i-"), F.col("k")).alias("instance_id"), _ts(F.col("s")).alias("start_time"),
        _ts(F.col("s") + F.col("len")).alias("end_time"), F.lit(False).alias("driver"),
        F.greatest(F.lit(0.5), F.col("cpu") * 0.8 + jitter).alias("cpu_user_percent"),
        (F.col("cpu") * 0.2).alias("cpu_system_percent"), F.lit(1.0).alias("cpu_wait_percent"),
        F.least(F.lit(99.0), F.col("mem") + jitter).alias("mem_used_percent"),
        F.lit(0.0).alias("mem_swap_percent"), F.lit("Standard_D8ds_v5").alias("node_type"))


def _prices(spark: SparkSession, t0: int) -> DataFrame:
    change = dt.datetime.utcfromtimestamp(t0 + 10 * 86400)
    old = dt.datetime(2020, 1, 1)
    rows = [
        (SKU["jobs"], old, change, 0.13),            # price change inside the window: SCD join test
        (SKU["jobs"], change, None, 0.15),
        (SKU["jobs_serverless"], old, None, 0.35),
        (SKU["all_purpose"], old, None, 0.55),
        (SKU["sql"], old, None, 0.70),
    ]
    df = spark.createDataFrame(rows, "sku_name string, price_start_time timestamp, "
                                     "price_end_time timestamp, p double")
    price = F.struct(F.col("p").alias("default"))
    return df.select(
        F.lit(ACCOUNT).alias("account_id"), "price_start_time", "price_end_time", "sku_name",
        F.lit("AZURE").alias("cloud"), F.lit("USD").alias("currency_code"), F.lit("DBU").alias("usage_unit"),
        F.struct(F.col("p").alias("default"), price.alias("promotional"),
                 price.alias("effective_list")).alias("pricing"))


def generate(spark: SparkSession, cfg: DPOConfig, scale: str = "small", seed: int = 42,
             end_date: Optional[str] = None, chaos: bool = False,
             drop_sources: tuple = ()) -> dict:
    """Build all sim tables. Returns a manifest with row counts and injected-defect counts."""
    if scale not in SCALES:
        raise ValueError(f"scale must be one of {list(SCALES)}")
    mult, n_days = SCALES[scale]
    end = dt.date.fromisoformat(end_date or cfg.end_date or dt.date.today().isoformat())
    t0 = int(dt.datetime(end.year, end.month, end.day).replace(tzinfo=dt.timezone.utc).timestamp()) \
        - n_days * 86400
    t0 += (seed % 7) * 60          # seed nudges timestamps so different seeds differ

    d = _dims(mult)
    jobs = spark.createDataFrame(d["jobs"], JOB_SCHEMA)
    clusters = spark.createDataFrame(d["clusters"], CLUSTER_SCHEMA)
    whs = spark.createDataFrame(d["warehouses"], WH_SCHEMA)
    tpls = spark.createDataFrame(d["templates"], TPL_SCHEMA)
    tables = spark.createDataFrame(d["tables"], TABLE_SCHEMA)
    days = spark.range(n_days).select(F.col("id").cast("int").alias("d"))

    runs = _runs(jobs, days, n_days, t0)
    ap = clusters.filter(F.col("cluster_source") != "JOB")
    ap_hours = (ap.crossJoin(days)
                .withColumn("h", F.explode(F.sequence(F.lit(0), F.col("uptime_hours") - 1)))
                .withColumn("hs", (F.lit(t0) + F.col("d") * 86400 +
                                   F.when(F.col("uptime_hours") >= 24, 0).otherwise(8 * 3600)
                                   + F.col("h") * 3600).cast("long")))
    wh_hours = (whs.crossJoin(days)
                .withColumn("h", F.explode(F.sequence(F.lit(0), F.col("active_hours") - 1)))
                .withColumn("hs", (F.lit(t0) + F.col("d") * 86400 + 7 * 3600 + F.col("h") * 3600)
                            .cast("long")))

    t_change = F.lit(dt.datetime.utcfromtimestamp(t0 - 86400))
    jobs_tbl = jobs.select(
        F.lit(ACCOUNT).alias("account_id"), F.lit(WORKSPACE).alias("workspace_id"), "job_id",
        F.col("job_name").alias("name"), F.lit(None).cast("string").alias("description"),
        F.col("owner").alias("creator_id"), _tags(F.col("tagged"), F.col("team")).alias("tags"),
        t_change.alias("change_time"), F.lit(None).cast("timestamp").alias("delete_time"),
        F.col("owner").alias("run_as"))
    # SCD trap: an older version of the first job with a different name must be ignored
    stale_job = jobs_tbl.limit(1).withColumn("name", F.lit("STALE_NAME")) \
                        .withColumn("change_time", F.lit(dt.datetime(2024, 1, 1)))
    clusters_tbl = clusters.select(
        F.lit(ACCOUNT).alias("account_id"), F.lit(WORKSPACE).alias("workspace_id"), "cluster_id",
        "cluster_name", F.col("owner").alias("owned_by"),
        F.lit(dt.datetime(2024, 6, 1)).alias("create_time"), F.lit(None).cast("timestamp").alias("delete_time"),
        F.lit("Standard_D8ds_v5").alias("driver_node_type"), F.lit("Standard_D8ds_v5").alias("worker_node_type"),
        "worker_count", F.lit(None).cast("int").alias("min_autoscale_workers"),
        F.lit(None).cast("int").alias("max_autoscale_workers"), "auto_termination_minutes",
        F.lit(True).alias("enable_elastic_disk"), _tags(F.col("tagged"), F.col("team")).alias("tags"),
        "cluster_source", "dbr_version", t_change.alias("change_time"),
        F.to_date(t_change).alias("change_date"), F.lit("USER_ISOLATION").alias("data_security_mode"))
    # SCD trap: a healthy all-purpose cluster used to have auto-termination disabled
    stale_cluster = (clusters_tbl.filter((F.col("cluster_source") == "UI") &
                                         F.col("cluster_name").startswith("control")).limit(1)
                     .withColumn("auto_termination_minutes", F.lit(0))
                     .withColumn("change_time", F.lit(dt.datetime(2024, 6, 1))))

    out = {
        "billing_usage": _usage(runs, ap_hours, wh_hours),
        "list_prices": _prices(spark, t0),
        "jobs": jobs_tbl.unionByName(stale_job),
        "job_run_timeline": _job_run_timeline(runs),
        "job_task_run_timeline": _task_run_timeline(runs),
        "query_history": _queries(tpls, days, t0),
        "clusters": clusters_tbl.unionByName(stale_cluster),
        "node_timeline": _node_timeline(runs, ap_hours),
        "table_inventory": tables.select(
            "table_catalog", "table_schema", "table_name", "num_files", "size_bytes",
            F.array().cast("array<string>").alias("partition_columns"),
            F.array().cast("array<string>").alias("clustering_columns"),
            F.lit(dt.datetime.utcfromtimestamp(t0 + n_days * 86400)).alias("last_modified"),
            F.lit("data-eng@example.com").alias("table_owner")),
    }

    injected = {}
    if chaos:
        out, injected = _inject_chaos(out)

    truth = spark.createDataFrame(d["truth"], "workload_type string, workload_key string, "
                                              "scenario string, expected_rule string")
    q_truth = tpls.select(
        F.lit("query").alias("workload_type"),
        F.concat(F.lit("query:"), query_fingerprint(F.regexp_replace(
            "statement_template", r"\{date\}", "2000-01-01"))).alias("workload_key"),
        "scenario", F.lit(None).cast("string").alias("expected_rule"))
    out["ground_truth"] = truth.unionByName(_map_rules(q_truth))

    for name in drop_sources:
        out.pop(name, None)

    manifest = {"scale": scale, "seed": seed, "days": n_days, "end_date": end.isoformat(),
                "tables": {}, "injected": injected, "dropped": list(drop_sources)}
    _ensure_schema(spark, cfg, "sim")
    for name, df in out.items():
        _save(df, cfg, cfg.t("sim", name))
        manifest["tables"][name] = spark.table(cfg.t("sim", name)).count()
    for name in drop_sources:
        spark.sql(f"DROP TABLE IF EXISTS {cfg.t('sim', name)}")
    return manifest


def _map_rules(q_truth: DataFrame) -> DataFrame:
    expr = F.lit(None).cast("string")
    for scn, rule in SCENARIO_RULE.items():
        expr = F.when(F.col("scenario") == scn, F.lit(rule)).otherwise(expr)
    return q_truth.withColumn("expected_rule", expr)


def _inject_chaos(out: dict) -> tuple[dict, dict]:
    """Corrupt the sim data the way real telemetry gets corrupted."""
    u = out["billing_usage"]

    def pick(df, mod, salt):
        keys = [F.col(f.name) for f in df.schema.fields if not isinstance(f.dataType, T.MapType)]
        return df.filter(F.pmod(F.xxhash64(*keys, F.lit(salt)), F.lit(mod)) == 0)

    dups = pick(u, 50, 1)
    null_ids = pick(u, 200, 2).withColumn("record_id", F.lit(None).cast("string"))
    negative = pick(u, 201, 3).withColumn("usage_quantity", F.lit(-5.0)) \
                              .withColumn("record_id", F.concat(F.col("record_id"), F.lit("-neg")))
    bad_sku = pick(u, 202, 4).withColumn("sku_name", F.lit("PREMIUM_MYSTERY_SKU")) \
                             .withColumn("record_id", F.concat(F.col("record_id"), F.lit("-sku")))
    backwards = pick(u, 203, 5).withColumn("usage_end_time", F.col("usage_start_time") - F.expr("INTERVAL 1 HOUR")) \
                               .withColumn("record_id", F.concat(F.col("record_id"), F.lit("-time")))
    out["billing_usage"] = u.unionByName(dups).unionByName(null_ids).unionByName(negative) \
                            .unionByName(bad_sku).unionByName(backwards)

    jr = out["job_run_timeline"]
    jr_dups = pick(jr, 100, 6)
    jr_back = pick(jr, 150, 7).withColumn("run_id", F.concat(F.col("run_id"), F.lit("x"))) \
                              .withColumn("period_end_time", F.col("period_start_time") - F.expr("INTERVAL 5 MINUTES"))
    out["job_run_timeline"] = jr.unionByName(jr_dups).unionByName(jr_back)

    q = out["query_history"]
    q_null = pick(q, 150, 8).withColumn("statement_id", F.lit(None).cast("string"))
    out["query_history"] = q.unionByName(q_null).withColumn("dpo_schema_drift_col", F.lit("unexpected"))

    nt = out["node_timeline"]
    nt_bad = pick(nt, 150, 9).withColumn("cpu_user_percent", F.lit(250.0)) \
                             .withColumn("instance_id", F.concat(F.col("instance_id"), F.lit("-bad")))
    out["node_timeline"] = nt.unionByName(nt_bad)

    injected = {"usage_duplicates": dups.count(), "usage_null_ids": null_ids.count(),
                "usage_negative": negative.count(), "usage_unknown_sku": bad_sku.count(),
                "usage_backwards_time": backwards.count(), "runs_duplicate_slices": jr_dups.count(),
                "runs_backwards_time": jr_back.count(), "queries_null_id": q_null.count(),
                "node_bad_cpu": nt_bad.count()}
    return out, injected


def _ensure_schema(spark: SparkSession, cfg: DPOConfig, layer: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")


def _save(df: DataFrame, cfg: DPOConfig, name: str) -> None:
    (df.write.format(cfg.table_format).mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(name))
