"""Regression suite: fixed seed -> fixed findings.

If a code or rule change alters what the tool reports, the golden snapshot
fails and the diff is reviewed in the PR (then refreshed with
DPO_UPDATE_GOLDEN=1 pytest tests/test_regression.py).
"""
import json
import os

import pytest

from conftest import local_cfg

GOLDEN = os.path.join(os.path.dirname(__file__), "golden", "small_seed42.json")


@pytest.fixture(scope="module")
def ctx(spark):
    from dpo import pipeline, synth
    cfg = local_cfg(prefix="reg_")
    synth.generate(spark, cfg, scale="small", seed=42, end_date="2026-10-27")
    return pipeline.run_all(spark, cfg, run_id="regression")


def test_all_checks_pass(ctx):
    from dpo.validation import run_all_checks
    failed = [r for r in run_all_checks(ctx) if not r[1]]
    assert not failed, failed


def test_every_stage_logged_success(ctx):
    assert [l["stage"] for l in ctx.log] == ["bronze", "silver", "gold", "rules", "recommend", "narrate", "govern"]
    assert all(l["status"] == "SUCCESS" for l in ctx.log)


def test_golden_snapshot(ctx):
    from dpo.validation import snapshot
    snap = snapshot(ctx)
    if os.environ.get("DPO_UPDATE_GOLDEN") or not os.path.exists(GOLDEN):
        os.makedirs(os.path.dirname(GOLDEN), exist_ok=True)
        with open(GOLDEN, "w") as fh:
            json.dump(snap, fh, indent=2)
        pytest.skip("golden snapshot written")
    with open(GOLDEN) as fh:
        gold = json.load(fh)
    assert snap["findings_by_rule"] == gold["findings_by_rule"]
    assert snap["total_cost"] == pytest.approx(gold["total_cost"], rel=1e-6)
    assert snap["top10_finding_ids"] == gold["top10_finding_ids"]
    assert snap["findings_checksum"] == gold["findings_checksum"]


def test_idempotent_rerun(spark, ctx):
    """Running the pipeline again on the same inputs changes nothing."""
    from dpo import pipeline
    from dpo.validation import snapshot
    before = snapshot(ctx)
    again = pipeline.run_all(spark, ctx.cfg, run_id="regression-rerun")
    assert snapshot(again) == before


def test_price_change_inside_window_is_respected(spark, ctx):
    """Jobs SKU price moves 0.13 -> 0.15 on day 10; both prices must appear, never doubled."""
    u = spark.table(ctx.cfg.t("silver", "usage_cost")).filter("sku_name = 'PREMIUM_JOBS_COMPUTE'")
    prices = sorted(r.list_price for r in u.select("list_price").distinct().collect())
    assert prices == [0.13, 0.15]
    b = spark.table(ctx.cfg.t("bronze", "billing_usage")).count()
    s = spark.table(ctx.cfg.t("silver", "usage_cost")).count()
    assert b == s  # clean data: the SCD price join must not duplicate rows


def test_scd_latest_version_wins(spark, ctx):
    jobs = spark.table(ctx.cfg.t("silver", "jobs"))
    assert jobs.filter("job_name = 'STALE_NAME'").count() == 0
    assert jobs.count() == jobs.select("job_id").distinct().count()


def test_recommendations_have_narratives(spark, ctx):
    n = spark.table(ctx.cfg.t("gold", "recommendation_narratives"))
    assert n.filter("narrative IS NULL OR length(narrative) < 40").count() == 0
