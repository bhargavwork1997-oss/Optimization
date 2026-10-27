"""Aggressive suite: corrupted input, missing sources, empty windows, scale, determinism."""
import os
import time

import pytest

from conftest import local_cfg


def test_chaos_input_is_quarantined_and_detection_still_holds(spark):
    from dpo import pipeline, synth
    from dpo.validation import run_all_checks
    cfg = local_cfg(prefix="chaos_")
    manifest = synth.generate(spark, cfg, scale="small", seed=7, end_date="2026-10-27", chaos=True)
    assert all(v > 0 for v in manifest["injected"].values()), manifest["injected"]
    ctx = pipeline.run_all(spark, cfg, run_id="chaos")
    failed = [r for r in run_all_checks(ctx, injected=manifest["injected"]) if not r[1]]
    assert not failed, failed
    q = spark.table(cfg.t("ops", "quarantine"))
    assert q.count() >= sum(v for k, v in manifest["injected"].items() if "duplicate" not in k)


def test_schema_drift_extra_column_is_ignored(spark):
    cfg = local_cfg(prefix="chaos_")
    cols = spark.table(cfg.t("bronze", "query_history")).columns
    assert "dpo_schema_drift_col" not in cols


def test_missing_sources_degrade_gracefully(spark):
    """No node_timeline (typical on serverless-only workspaces) -> utilisation rules go quiet, rest works."""
    from dpo import pipeline, synth
    from dpo.validation import run_all_checks
    cfg = local_cfg(prefix="missing_")
    synth.generate(spark, cfg, scale="small", seed=3, end_date="2026-10-27",
                   drop_sources=("node_timeline", "table_inventory"))
    ctx = pipeline.run_all(spark, cfg, run_id="missing")
    status = {r.source: r.status for r in spark.table(cfg.t("ops", "source_status")).collect()}
    assert status["node_timeline"] == "missing" and status["table_inventory"] == "missing"
    skip = ("R01_COMPUTE_IDLE", "R02_COMPUTE_OVERSIZED", "R15_TABLE_SMALL_FILES")
    failed = [r for r in run_all_checks(ctx, skip_rules=skip) if not r[1]]
    assert not failed, failed


def test_empty_window_produces_empty_outputs_not_errors(spark):
    from dpo import pipeline
    cfg = local_cfg(prefix="reg_", end_date="2031-01-01")   # data exists only for 2026
    ctx = pipeline.run_all(spark, cfg, run_id="empty")
    assert all(l["status"] == "SUCCESS" for l in ctx.log)
    assert spark.table(cfg.t("gold", "findings")).filter("rule_id <> 'R15_TABLE_SMALL_FILES'").count() == 0


def test_generator_is_deterministic(spark):
    from dpo import synth
    from dpo.transforms import checksum
    a, b = local_cfg(prefix="det_a_"), local_cfg(prefix="det_b_")
    synth.generate(spark, a, "small", seed=11, end_date="2026-10-27")
    synth.generate(spark, b, "small", seed=11, end_date="2026-10-27")
    for name in ("billing_usage", "job_run_timeline", "query_history", "node_timeline"):
        assert checksum(spark.table(a.t("sim", name))) == checksum(spark.table(b.t("sim", name))), name


@pytest.mark.slow
def test_medium_scale_recall_and_throughput(spark):
    """5x every scenario: detection must not degrade with volume or neighbours."""
    from dpo import pipeline, synth
    from dpo.validation import run_all_checks
    cfg = local_cfg(prefix="scale_")
    m = synth.generate(spark, cfg, scale="medium", seed=99, end_date="2026-10-27")
    t = time.time()
    ctx = pipeline.run_all(spark, cfg, run_id="scale")
    secs = time.time() - t
    failed = [r for r in run_all_checks(ctx) if not r[1]]
    assert not failed, failed
    rows = sum(m["tables"].values())
    print(f"medium scale: {rows} sim rows, pipeline {secs:.1f}s")
    from dpo.synth import SCENARIO_RULE
    assert spark.table(cfg.t("gold", "findings")).count() == len(SCENARIO_RULE) * 5


def test_results_do_not_depend_on_machine_timezone(spark):
    """Regression: naive datetimes made the golden total differ in IST (12899.55) vs UTC (12899.30)."""
    import time as _time
    from dpo import synth
    from dpo.transforms import checksum
    old = os.environ.get("TZ")
    sums = []
    try:
        for i, tz in enumerate(("UTC", "Asia/Kolkata", "America/Los_Angeles")):
            os.environ["TZ"] = tz
            _time.tzset()
            cfg = local_cfg(prefix=f"tz{i}_")
            synth.generate(spark, cfg, "small", seed=42, end_date="2026-10-27")
            sums.append(tuple(checksum(spark.table(cfg.t("sim", n)))
                              for n in ("list_prices", "jobs", "clusters", "billing_usage", "table_inventory")))
    finally:
        if old is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old
        _time.tzset()
    assert sums[0] == sums[1] == sums[2]
