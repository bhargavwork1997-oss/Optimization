"""Deterministic diagnostic rules.

Each rule reads silver/gold tables and returns rows with a fixed contract:
    workload_key, metric_name, metric_value, threshold, est_monthly_savings, evidence
Metadata (title, category, severity, C/F/R scores, action) lives in config/rules.json
so thresholds can be tuned per client without code changes.
"""
from __future__ import annotations

import datetime as dt
import time
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .transforms import safe_div

RULES: dict[str, Callable] = {}
OUT_COLS = ["workload_key", "metric_name", "metric_value", "threshold", "est_monthly_savings", "evidence"]


def rule(rule_id: str):
    def deco(fn):
        RULES[rule_id] = fn
        return fn
    return deco


def _out(df: DataFrame, key, metric: str, value, threshold: float, savings, **evidence) -> DataFrame:
    ev = F.to_json(F.struct(*[c.alias(k) for k, c in evidence.items()])) if evidence else F.lit("{}")
    return df.select(key.alias("workload_key"), F.lit(metric).alias("metric_name"),
                     value.cast("double").alias("metric_value"), F.lit(float(threshold)).alias("threshold"),
                     savings.cast("double").alias("est_monthly_savings"), ev.alias("evidence"))


# ---------------------------------------------------------------- helpers ----
def _cluster_monthly(ctx) -> DataFrame:
    u = ctx.read("silver", "usage_cost")
    return (u.filter(F.col("cluster_id").isNotNull())
            .groupBy("cluster_id", F.col("usage_date"))
            .agg(F.sum("cost").alias("cost"), F.sum("usage_hours").alias("hours"))
            .groupBy("cluster_id")
            .agg((F.sum("cost") * F.lit(ctx.monthly)).alias("monthly_cost"),
                 F.avg("hours").alias("avg_daily_uptime_h"), F.count(F.lit(1)).alias("days")))


def _cluster_util(ctx) -> DataFrame:
    return (ctx.read("silver", "cluster_util_daily").groupBy("cluster_id")
            .agg(F.avg("avg_cpu").alias("avg_cpu"), F.avg("avg_mem").alias("avg_mem"),
                 F.max("max_mem").alias("peak_mem")))


def _job_monthly(ctx) -> DataFrame:
    return (ctx.read("gold", "workload_summary").filter(F.col("workload_type") == "job")
            .select(F.expr("substring(workload_key, 5)").alias("job_id"), "monthly_cost"))


def _cluster_to_job(ctx) -> DataFrame:
    return (ctx.read("silver", "job_runs").select("job_id", F.explode("compute_ids").alias("cluster_id"))
            .distinct())


def _owner_key(ctx, clusters: DataFrame) -> DataFrame:
    """Job clusters are reported against their job; other clusters against themselves."""
    m = _cluster_to_job(ctx)
    return (clusters.join(m, "cluster_id", "left")
            .withColumn("workload_key",
                        F.when((F.col("cluster_source") == "JOB") & F.col("job_id").isNotNull(),
                               F.concat(F.lit("job:"), "job_id"))
                         .otherwise(F.concat(F.lit("cluster:"), "cluster_id"))))


# ---------------------------------------------------------------- compute ----
@rule("R01_COMPUTE_IDLE")
def r01(ctx, th):
    c = ctx.read("silver", "clusters").filter("is_all_purpose")
    df = c.join(_cluster_util(ctx), "cluster_id").join(_cluster_monthly(ctx), "cluster_id")
    df = df.filter((F.col("avg_cpu") < th["max_avg_cpu_pct"]) &
                   (F.col("avg_daily_uptime_h") >= th["min_daily_uptime_h"]))
    sav = F.col("monthly_cost") * F.least(F.lit(0.8), 1 - F.col("avg_cpu") / th["target_cpu_pct"])
    return _out(df, F.concat(F.lit("cluster:"), "cluster_id"), "avg_cpu_pct", F.col("avg_cpu"),
                th["max_avg_cpu_pct"], sav, cluster_name=F.col("cluster_name"),
                avg_daily_uptime_h=F.round("avg_daily_uptime_h", 1), avg_mem_pct=F.round("avg_mem", 1),
                workers=F.col("worker_count"), monthly_cost=F.round("monthly_cost", 2))


@rule("R02_COMPUTE_OVERSIZED")
def r02(ctx, th):
    c = ctx.read("silver", "clusters").filter(~F.col("is_all_purpose"))
    df = (c.filter(F.coalesce("max_autoscale_workers", "worker_count") >= th["min_workers"])
          .join(_cluster_util(ctx), "cluster_id")
          .filter((F.col("avg_cpu") < th["max_avg_cpu_pct"]) & (F.col("avg_mem") < th["max_avg_mem_pct"])))
    df = _owner_key(ctx, df).join(_job_monthly(ctx), "job_id", "left")
    sav = F.coalesce(F.col("monthly_cost"), F.lit(0.0)) * F.least(
        F.lit(0.6), 1 - F.greatest("avg_cpu", "avg_mem") / th["target_util_pct"])
    return _out(df, F.col("workload_key"), "avg_cpu_pct", F.col("avg_cpu"), th["max_avg_cpu_pct"], sav,
                cluster_id=F.col("cluster_id"), workers=F.coalesce("max_autoscale_workers", "worker_count"),
                avg_mem_pct=F.round("avg_mem", 1), peak_mem_pct=F.round("peak_mem", 1))


@rule("R03_COMPUTE_AUTOTERMINATION")
def r03(ctx, th):
    c = ctx.read("silver", "clusters").filter("is_all_purpose")
    df = c.join(_cluster_monthly(ctx), "cluster_id").filter(
        F.col("auto_termination_minutes").isNull() | (F.col("auto_termination_minutes") == 0) |
        (F.col("auto_termination_minutes") > th["max_minutes"]))
    return _out(df, F.concat(F.lit("cluster:"), "cluster_id"), "auto_termination_minutes",
                F.coalesce("auto_termination_minutes", F.lit(0)), th["max_minutes"],
                F.col("monthly_cost") * th["savings_fraction"], cluster_name=F.col("cluster_name"),
                avg_daily_uptime_h=F.round("avg_daily_uptime_h", 1), monthly_cost=F.round("monthly_cost", 2))


@rule("R04_COMPUTE_OLD_RUNTIME")
def r04(ctx, th):
    c = ctx.read("silver", "clusters").filter(F.col("dbr_major") < th["min_dbr_major"])
    df = _owner_key(ctx, c.join(_cluster_monthly(ctx), "cluster_id"))
    return _out(df, F.col("workload_key"), "dbr_major", F.col("dbr_major"), th["min_dbr_major"],
                F.col("monthly_cost") * th["savings_fraction"], cluster_id=F.col("cluster_id"),
                dbr_version=F.col("dbr_version"), monthly_cost=F.round("monthly_cost", 2))


# ------------------------------------------------------------------- jobs ----
@rule("R05_JOB_ON_ALL_PURPOSE")
def r05(ctx, th):
    runs = ctx.read("silver", "job_runs")
    ap = ctx.read("silver", "clusters").filter("is_all_purpose").select("cluster_id").distinct()
    on_ap = (runs.select("job_id", "run_id", F.explode("compute_ids").alias("cluster_id"))
             .join(ap, "cluster_id").select("job_id", "run_id").distinct()
             .groupBy("job_id").agg(F.count(F.lit(1)).alias("ap_runs")))
    df = (runs.groupBy("job_id", "job_name").agg(F.count(F.lit(1)).alias("runs"))
          .join(on_ap, "job_id").withColumn("ap_share", F.col("ap_runs") / F.col("runs"))
          .filter(F.col("ap_share") >= th["min_share_of_runs"]).join(_job_monthly(ctx), "job_id", "left"))
    return _out(df, F.concat(F.lit("job:"), "job_id"), "share_runs_on_all_purpose", F.col("ap_share"),
                th["min_share_of_runs"], F.coalesce("monthly_cost", F.lit(0.0)) * th["savings_fraction"],
                job_name=F.col("job_name"), runs=F.col("runs"), monthly_cost=F.round("monthly_cost", 2))


@rule("R06_JOB_FAILURE_RATE")
def r06(ctx, th):
    sc = ctx.read("gold", "job_scorecard")
    runs, rc = ctx.read("silver", "job_runs"), ctx.read("gold", "run_cost")
    failed_cost = (runs.filter("is_failure").join(rc.select(F.col("job_run_id").alias("run_id"), "cost"),
                                                  "run_id", "left")
                   .groupBy("job_id").agg(F.sum(F.coalesce("cost", F.lit(0.0))).alias("failed_cost")))
    df = (sc.filter((F.col("runs") >= th["min_runs"]) & (F.col("failure_rate") >= th["max_failure_rate"]))
          .join(failed_cost, "job_id", "left"))
    return _out(df, F.concat(F.lit("job:"), "job_id"), "failure_rate", F.col("failure_rate"),
                th["max_failure_rate"], F.coalesce("failed_cost", F.lit(0.0)) * F.lit(ctx.monthly),
                job_name=F.col("job_name"), runs=F.col("runs"), failed=F.col("failed"),
                p95_duration_s=F.col("p95_duration_s"))


@rule("R07_TASK_SKEW")
def r07(ctx, th):
    t = ctx.read("silver", "task_runs")
    per_run = (t.groupBy("job_id", "job_run_id")
               .agg(F.count(F.lit(1)).alias("n_tasks"), F.max("duration_s").alias("max_s"),
                    F.percentile_approx("duration_s", 0.5).alias("med_s"))
               .filter(F.col("n_tasks") >= th["min_tasks"])
               .withColumn("ratio", safe_div(F.col("max_s").cast("double"), F.col("med_s").cast("double"))))
    df = (per_run.groupBy("job_id")
          .agg(F.count(F.lit(1)).alias("runs"),
               F.avg((F.col("ratio") >= th["min_max_to_median"]).cast("double")).alias("skewed_share"),
               F.percentile_approx("ratio", 0.5).alias("median_ratio"))
          .filter(F.col("skewed_share") >= th["min_share_of_runs"])
          .join(_job_monthly(ctx), "job_id", "left"))
    return _out(df, F.concat(F.lit("job:"), "job_id"), "median_max_to_median_task_ratio",
                F.col("median_ratio"), th["min_max_to_median"],
                F.coalesce("monthly_cost", F.lit(0.0)) * th["savings_fraction"],
                runs=F.col("runs"), skewed_share=F.round("skewed_share", 2))


@rule("R08_RUNTIME_REGRESSION")
def r08(ctx, th):
    cut = ctx.end_date - dt.timedelta(days=th["recent_days"])
    r = ctx.read("silver", "job_runs").filter("is_success").withColumn("recent", F.col("run_date") > F.lit(cut))
    m = (r.groupBy("job_id", "job_name")
         .agg(F.percentile_approx(F.when(F.col("recent"), F.col("duration_s")), 0.5).alias("recent_p50"),
              F.percentile_approx(F.when(~F.col("recent"), F.col("duration_s")), 0.5).alias("prior_p50"),
              F.sum(F.col("recent").cast("int")).alias("n_recent"),
              F.sum((~F.col("recent")).cast("int")).alias("n_prior"))
         .withColumn("ratio", safe_div(F.col("recent_p50").cast("double"), F.col("prior_p50").cast("double"))))
    daily = (ctx.read("gold", "workload_cost_daily").filter(F.col("usage_date") > F.lit(cut))
             .filter(F.col("workload_type") == "job")
             .groupBy(F.expr("substring(workload_key, 5)").alias("job_id"))
             .agg((F.sum("cost") / th["recent_days"]).alias("recent_daily_cost")))
    df = (m.filter((F.col("n_recent") >= th["min_runs_each"]) & (F.col("n_prior") >= th["min_runs_each"]) &
                   (F.col("ratio") >= th["min_ratio"])).join(daily, "job_id", "left"))
    sav = F.coalesce("recent_daily_cost", F.lit(0.0)) * 30 * (1 - 1 / F.col("ratio"))
    return _out(df, F.concat(F.lit("job:"), "job_id"), "recent_vs_prior_p50_runtime", F.col("ratio"),
                th["min_ratio"], sav, job_name=F.col("job_name"), recent_p50_s=F.col("recent_p50"),
                prior_p50_s=F.col("prior_p50"))


@rule("R09_COST_SPIKE")
def r09(ctx, th):
    cut = ctx.end_date - dt.timedelta(days=th["recent_days"])
    d = (ctx.read("gold", "workload_cost_daily").filter(F.col("workload_type") != "unattributed")
         .groupBy("workload_key", "usage_date").agg(F.sum("cost").alias("cost"))
         .withColumn("recent", F.col("usage_date") >= F.lit(cut)))
    m = (d.groupBy("workload_key")
         .agg(F.avg(F.when(F.col("recent"), F.col("cost"))).alias("recent_avg"),
              F.percentile_approx(F.when(~F.col("recent"), F.col("cost")), 0.5).alias("baseline_median"),
              F.sum((~F.col("recent")).cast("int")).alias("baseline_days"))
         .withColumn("ratio", safe_div(F.col("recent_avg"), F.col("baseline_median"))))
    df = m.filter((F.col("baseline_days") >= th["min_baseline_days"]) & (F.col("ratio") >= th["min_ratio"]) &
                  (F.col("recent_avg") >= th["min_daily_cost"]))
    sav = (F.col("recent_avg") - F.col("baseline_median")) * 30
    return _out(df, F.col("workload_key"), "recent_vs_baseline_daily_cost", F.col("ratio"), th["min_ratio"],
                sav, recent_avg_daily=F.round("recent_avg", 2), baseline_median_daily=F.round("baseline_median", 2))


@rule("R10_UNTAGGED_SPEND")
def r10(ctx, th):
    s = ctx.read("gold", "workload_summary").filter(~F.col("workload_type").isin("unattributed"))
    df = (s.withColumn("untagged_share", safe_div(F.col("untagged_cost"), F.col("window_cost")))
          .filter((F.col("untagged_share") >= th["min_untagged_share"]) &
                  (F.col("monthly_cost") >= th["min_monthly_cost"])))
    return _out(df, F.col("workload_key"), "untagged_cost_share", F.col("untagged_share"),
                th["min_untagged_share"], F.lit(None), monthly_cost=F.round("monthly_cost", 2),
                tag_key=F.lit(ctx.cfg.cost_center_tag))


# -------------------------------------------------------------------- SQL ----
def _fp(ctx):
    return ctx.read("gold", "query_fingerprint_stats")


@rule("R11_SQL_NO_PRUNING")
def r11(ctx, th):
    f = _fp(ctx).withColumn("prune_ratio", safe_div(F.col("avg_pruned_files"),
                                                    F.col("avg_read_files") + F.col("avg_pruned_files")))
    df = f.filter((F.col("executions") >= th["min_executions"]) &
                  (F.col("avg_read_files") >= th["min_read_files"]) &
                  (F.col("avg_read_bytes") >= th["min_read_gb"] * 1024**3) &
                  (F.coalesce("prune_ratio", F.lit(0.0)) < th["max_prune_ratio"]))
    return _out(df, F.concat(F.lit("query:"), "fingerprint"), "file_prune_ratio",
                F.coalesce("prune_ratio", F.lit(0.0)), th["max_prune_ratio"],
                F.col("monthly_cost") * th["savings_fraction"], executions=F.col("executions"),
                avg_read_gb=F.round(F.col("avg_read_bytes") / 1024**3, 1),
                avg_read_files=F.round("avg_read_files", 0), sample_statement_id=F.col("sample_statement_id"),
                statement_shape=F.substring("statement_shape", 1, 160))


@rule("R12_SQL_SPILL")
def r12(ctx, th):
    df = _fp(ctx).filter((F.col("executions") >= th["min_executions"]) &
                         (F.col("spill_share") >= th["min_spill_share"]))
    return _out(df, F.concat(F.lit("query:"), "fingerprint"), "share_executions_spilling_1gb",
                F.col("spill_share"), th["min_spill_share"], F.col("monthly_cost") * th["savings_fraction"],
                executions=F.col("executions"), max_spill_gb=F.round(F.col("max_spill_bytes") / 1024**3, 1),
                sample_statement_id=F.col("sample_statement_id"),
                statement_shape=F.substring("statement_shape", 1, 160))


@rule("R13_SQL_REPEATED_NO_CACHE")
def r13(ctx, th):
    df = (_fp(ctx).withColumn("per_day", safe_div(F.col("executions").cast("double"),
                                                  F.col("active_days").cast("double")))
          .filter((F.col("per_day") >= th["min_executions_per_day"]) &
                  (F.col("cache_hit_rate") < th["max_cache_hit_rate"])))
    return _out(df, F.concat(F.lit("query:"), "fingerprint"), "executions_per_day", F.col("per_day"),
                th["min_executions_per_day"], F.col("monthly_cost") * th["savings_fraction"],
                cache_hit_rate=F.round("cache_hit_rate", 3), sample_statement_id=F.col("sample_statement_id"),
                statement_shape=F.substring("statement_shape", 1, 160))


@rule("R14_WAREHOUSE_QUEUING")
def r14(ctx, th):
    q = ctx.read("silver", "queries").filter(F.col("warehouse_id").isNotNull())
    df = (q.groupBy("warehouse_id")
          .agg(F.count(F.lit(1)).alias("queries"),
               F.avg((F.col("waiting_at_capacity_duration_ms") > th["queue_ms"]).cast("double")).alias("queued_share"),
               F.percentile_approx("waiting_at_capacity_duration_ms", 0.95).alias("p95_queue_ms"))
          .filter((F.col("queries") >= th["min_queries"]) & (F.col("queued_share") >= th["min_queued_share"])))
    return _out(df, F.concat(F.lit("warehouse:"), "warehouse_id"), "share_queries_queued",
                F.col("queued_share"), th["min_queued_share"], F.lit(None), queries=F.col("queries"),
                p95_queue_s=F.round(F.col("p95_queue_ms") / 1000, 1))


@rule("R15_TABLE_SMALL_FILES")
def r15(ctx, th):
    df = ctx.read("silver", "table_layout").filter(
        (F.col("num_files") >= th["min_files"]) & (F.col("avg_file_mb") < th["max_avg_file_mb"]))
    return _out(df, F.concat(F.lit("table:"), "fqn"), "avg_file_mb", F.col("avg_file_mb"),
                th["max_avg_file_mb"], F.lit(None), num_files=F.col("num_files"),
                size_gb=F.round(F.col("size_bytes") / 1024**3, 1),
                clustering_columns=F.col("clustering_columns"))


def _pattern_rule(ctx, th, metric):
    """Statement-shape patterns from config (regex on literal-stripped, lower-cased SQL)."""
    pats = th["patterns"]
    f = _fp(ctx).filter((F.col("executions") >= th["min_executions"]) &
                        (F.col("active_days") >= th["min_active_days"]))
    hits = F.filter(F.array(*[F.when(F.col("statement_shape").rlike(rx), F.lit(name))
                              for name, rx in pats.items()]), lambda x: x.isNotNull())
    df = f.withColumn("hits", hits).filter(F.size("hits") > 0)
    return _out(df, F.concat(F.lit("query:"), "fingerprint"), metric, F.size("hits"), 1,
                F.col("monthly_cost") * th["savings_fraction"], patterns=F.col("hits"),
                executions=F.col("executions"), sample_statement_id=F.col("sample_statement_id"),
                statement_shape=F.substring("statement_shape", 1, 160))


@rule("R16_SQL_ANTI_PATTERN")
def r16(ctx, th):
    return _pattern_rule(ctx, th, "anti_patterns_found")


@rule("R17_REPLATFORM_CANDIDATE")
def r17(ctx, th):
    return _pattern_rule(ctx, th, "file_copy_patterns_found")


# ------------------------------------------------------------------ engine ---
def run_rules(ctx) -> dict:
    meta = ctx.cfg.rules()
    parts, status = [], []
    for rid, m in meta.items():
        if not m.get("enabled", True):
            status.append((rid, "DISABLED", 0, 0.0, ""))
            continue
        fn = RULES.get(rid)
        t = time.time()
        try:
            df = fn(ctx, m["thresholds"]).select(*OUT_COLS)
            n = df.count()
            parts.append(df.withColumn("rule_id", F.lit(rid)))
            status.append((rid, "OK", n, round(time.time() - t, 2), ""))
        except Exception as exc:  # one broken rule never stops the others
            status.append((rid, "ERROR", 0, round(time.time() - t, 2), f"{type(exc).__name__}: {exc}"[:1000]))

    meta_df = ctx.spark.createDataFrame(
        [(r, m["title"], m["category"], m["severity"], float(m["confidence"]), float(m["feasibility"]),
          float(m["reversibility"]), m["action"]) for r, m in meta.items()],
        "rule_id string, title string, category string, severity string, confidence double, "
        "feasibility double, reversibility double, action string")
    if parts:
        f = parts[0]
        for p in parts[1:]:
            f = f.unionByName(p)
    else:
        f = ctx.spark.createDataFrame([], "workload_key string, metric_name string, metric_value double, "
                                          "threshold double, est_monthly_savings double, evidence string, "
                                          "rule_id string")
    # owning cost centre (row-level security key); SQL findings inherit their warehouse's
    ws = ctx.read("gold", "workload_summary").select("workload_key", "cost_center")
    q_cc = (_fp(ctx).select(F.concat(F.lit("query:"), "fingerprint").alias("workload_key"),
                            F.explode("warehouse_ids").alias("wh"))
            .join(ws.withColumnRenamed("workload_key", "wk"), F.concat(F.lit("warehouse:"), F.col("wh")) == F.col("wk"))
            .groupBy("workload_key").agg(F.max("cost_center").alias("cost_center")))
    cc_map = ws.unionByName(q_cc).groupBy("workload_key").agg(F.max("cost_center").alias("cost_center"))
    findings = (f.join(meta_df, "rule_id").join(cc_map, "workload_key", "left")
                .withColumn("finding_id", F.substring(F.sha2(F.concat_ws("|", "rule_id", "workload_key"), 256), 1, 16))
                .withColumn("workload_type", F.substring_index("workload_key", ":", 1))
                .withColumn("period_start", F.lit(ctx.start_date)).withColumn("period_end", F.lit(ctx.end_date))
                .withColumn("run_id", F.lit(ctx.run_id)).withColumn("detected_at", F.lit(ctx.run_ts)))
    n = ctx.save(findings, "gold", "findings")
    st = ctx.spark.createDataFrame(status, "rule_id string, status string, findings long, seconds double, "
                                           "message string").withColumn("run_id", F.lit(ctx.run_id))
    ctx.save(st, "ops", "rule_status")
    return {"findings": n, "rule_errors": sum(1 for s in status if s[1] == "ERROR")}
