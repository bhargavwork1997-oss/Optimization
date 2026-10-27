"""Checks shared by pytest (local) and the Databricks test notebooks.

Every check returns (name, passed, detail) so a notebook can write a results
table and a CI job can fail on the first False.
"""
from __future__ import annotations

import json

from pyspark.sql import functions as F

from .transforms import checksum


def detection(ctx, skip_rules: tuple = (), strict: bool = True) -> list[tuple]:
    """Recall on injected anomalies + zero findings on healthy controls."""
    truth = ctx.spark.table(ctx.cfg.t("sim", "ground_truth"))
    fired = (ctx.read("gold", "findings").groupBy("workload_key")
             .agg(F.array_sort(F.collect_set("rule_id")).alias("fired")))
    j = truth.join(fired, "workload_key", "left")
    res = []

    anomalies = j.filter(F.col("expected_rule").isNotNull() & ~F.col("expected_rule").isin(*skip_rules)
                         if skip_rules else F.col("expected_rule").isNotNull()).collect()
    missed = [(r.workload_key, r.expected_rule) for r in anomalies
              if not r.fired or r.expected_rule not in r.fired]
    res.append(("recall_injected_anomalies", not missed,
                f"{len(anomalies) - len(missed)}/{len(anomalies)} detected; missed={missed[:10]}"))
    if strict:
        extra = [(r.workload_key, [x for x in r.fired if x != r.expected_rule]) for r in anomalies
                 if r.fired and any(x != r.expected_rule for x in r.fired)]
        res.append(("no_cross_firing_on_anomalies", not extra, f"extra={extra[:10]}"))

    controls = j.filter(F.col("scenario") == "CONTROL").collect()
    fp = [(r.workload_key, r.fired) for r in controls if r.fired]
    res.append(("zero_findings_on_controls", not fp, f"{len(controls)} controls; false_positives={fp[:10]}"))
    return res


def accounting(ctx) -> list[tuple]:
    """Attribution must conserve money: gold total == silver total."""
    s = ctx.read("silver", "usage_cost").agg(F.sum("cost")).collect()[0][0] or 0.0
    g = ctx.read("gold", "workload_cost_daily").agg(F.sum("cost")).collect()[0][0] or 0.0
    neg = ctx.read("gold", "workload_cost_daily").filter(F.col("cost") < -1e-9).count()
    return [("cost_conserved_silver_to_gold", abs(s - g) <= max(0.01, 1e-6 * s), f"silver={s:.4f} gold={g:.4f}"),
            ("no_negative_allocations", neg == 0, f"negative_rows={neg}")]


def scoring(ctx) -> list[tuple]:
    r = ctx.read("gold", "recommendations")
    bad = r.filter((F.col("score") < 0) | (F.col("score") > 100) | F.col("score").isNull()).count()
    dup = r.count() - r.select("finding_id").distinct().count()
    ranks = r.agg(F.min("rank"), F.max("rank"), F.count(F.lit(1))).collect()[0]
    contiguous = ranks[2] == 0 or (ranks[0] == 1 and ranks[1] == ranks[2])
    return [("scores_within_0_100", bad == 0, f"out_of_range={bad}"),
            ("finding_ids_unique", dup == 0, f"duplicates={dup}"),
            ("ranks_contiguous", contiguous, f"min={ranks[0]} max={ranks[1]} n={ranks[2]}")]


def pipeline_health(ctx) -> list[tuple]:
    st = ctx.spark.table(ctx.cfg.t("ops", "rule_status")).filter(F.col("status") == "ERROR").collect()
    return [("no_rule_errors", not st, f"errors={[(r.rule_id, r.message[:120]) for r in st]}")]


def dq_expectations(ctx, injected: dict) -> list[tuple]:
    """Chaos run: every injected defect must be caught and quarantined or de-duplicated."""
    rep = {r.source: r for r in ctx.spark.table(ctx.cfg.t("ops", "dq_report")).collect()}
    u, jr = rep["billing_usage"], rep["job_run_timeline"]
    ur, jrr = json.loads(u.reasons), json.loads(jr.reasons)
    qr = json.loads(rep["query_history"].reasons)
    nr = json.loads(rep["node_timeline"].reasons)
    checks = [
        ("dq_usage_duplicates_removed", u.duplicates_removed >= injected["usage_duplicates"],
         f"{u.duplicates_removed} >= {injected['usage_duplicates']}"),
        ("dq_usage_null_ids", ur.get("null_record_id", 0) >= injected["usage_null_ids"], str(ur)),
        ("dq_usage_negative", sum(v for k, v in ur.items() if "negative_quantity" in k) >= injected["usage_negative"], str(ur)),
        ("dq_usage_unknown_sku", ur.get("no_price", 0) >= injected["usage_unknown_sku"], str(ur)),
        ("dq_usage_backwards", sum(v for k, v in ur.items() if "end_before_start" in k) >= injected["usage_backwards_time"], str(ur)),
        ("dq_runs_duplicate_slices", jr.duplicates_removed >= injected["runs_duplicate_slices"],
         f"{jr.duplicates_removed} >= {injected['runs_duplicate_slices']}"),
        ("dq_runs_backwards", jrr.get("end_before_start", 0) >= injected["runs_backwards_time"], str(jrr)),
        ("dq_queries_null_id", qr.get("null_statement_id", 0) >= injected["queries_null_id"], str(qr)),
        ("dq_node_bad_cpu", nr.get("cpu_out_of_range", 0) >= injected["node_bad_cpu"], str(nr)),
    ]
    return checks


def snapshot(ctx) -> dict:
    """Regression fingerprint of the outputs that matter."""
    f = ctx.read("gold", "findings")
    by_rule = {r.rule_id: r.n for r in f.groupBy("rule_id").agg(F.count(F.lit(1)).alias("n")).collect()}
    total = ctx.read("gold", "workload_cost_daily").agg(F.sum("cost")).collect()[0][0] or 0.0
    top = [r.finding_id for r in ctx.read("gold", "recommendations").orderBy("rank").limit(10).collect()]
    n, h = checksum(ctx.read("gold", "findings").select("finding_id", "rule_id", "workload_key",
                                                         F.round("metric_value", 4).alias("v")))
    return {"findings_by_rule": dict(sorted(by_rule.items())), "total_cost": round(total, 2),
            "top10_finding_ids": top, "findings_checksum": [n, h]}


def run_all_checks(ctx, injected: dict | None = None, skip_rules: tuple = (), strict: bool = True):
    res = detection(ctx, skip_rules, strict) + accounting(ctx) + scoring(ctx) + pipeline_health(ctx)
    if injected:
        res += dq_expectations(ctx, injected)
    return res
