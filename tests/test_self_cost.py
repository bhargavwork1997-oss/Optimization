"""DPO's own cost: found in the same telemetry, counted once, reported against the savings it proposes."""
import os

import pytest
from pyspark.sql import functions as F

from conftest import local_cfg

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="module")
def built(spark):
    from dpo import pipeline, synth
    cfg = local_cfg(prefix="self_")
    cfg.rules_path = os.path.join(ROOT, "config", "rules.json")
    synth.generate(spark, cfg, scale="small", seed=42, end_date="2026-10-27")
    pipeline.run_all(spark, cfg, run_id="self")
    return cfg


def test_dpo_jobs_recognised_by_tag_or_name(spark):
    from dpo.pipeline import is_dpo_job
    rows = [("dpo-pipeline-enterprise_prod", {}), ("[dev ana@corp.com] dpo-tests-enterprise_dev", {}),
            ("nightly_etl", {"app": "dpo-accelerator"}), ("nightly_etl", {"app": "other"}),
            ("my-dpo-pipeline-copy", {}), (None, None)]
    df = spark.createDataFrame(rows, "name string, tags map<string,string>")
    got = [r[0] for r in df.select(is_dpo_job(F.col("name"), F.col("tags"))).collect()]
    assert got == [True, True, True, False, False, False]


def test_queries_reading_dpo_schemas_are_flagged(spark, built):
    q = spark.table(built.t("silver", "queries"))
    dpo = q.filter("reads_dpo")
    assert dpo.count() > 0
    assert all("self_reporting." in r.statement_shape for r in dpo.select("statement_shape").limit(20).collect())
    assert q.filter("NOT reads_dpo AND statement_shape LIKE '%self_reporting.%'").count() == 0


def test_components_and_no_double_counting(spark, built):
    d = spark.table(built.t("gold", "dpo_cost_daily"))
    by = {r.component: r.cost for r in d.groupBy("component").agg(F.sum("cost").alias("cost")).collect()}
    assert by.keys() == {"jobs", "sql"} and all(v > 0 for v in by.values())
    # jobs component == billing of the DPO job, exactly
    u = spark.table(built.t("silver", "usage_cost")).join(
        spark.table(built.t("silver", "jobs")).filter("is_dpo").select("job_id"), "job_id")
    assert by["jobs"] == pytest.approx(u.agg(F.sum("cost")).collect()[0][0])
    # sql component is warehouse cost only: never more than the warehouses' total
    wh = spark.table(built.t("gold", "workload_cost_daily")).filter("workload_type = 'warehouse'")
    assert by["sql"] < wh.agg(F.sum("cost")).collect()[0][0]


def test_cost_per_run_matches_billing(spark, built):
    r = spark.table(built.t("gold", "dpo_run_cost"))
    assert r.count() == 30 and r.filter("start_time IS NULL OR cost <= 0").count() == 0
    jobs = spark.table(built.t("gold", "dpo_cost_daily")).filter("component = 'jobs'")
    assert r.agg(F.sum("cost")).collect()[0][0] == pytest.approx(jobs.agg(F.sum("cost")).collect()[0][0])


def test_summary_arithmetic(spark, built):
    s = spark.table(built.t("gold", "dpo_cost_summary")).collect()[0]
    assert s.dpo_monthly_cost == pytest.approx(s.jobs_monthly + s.sql_monthly + s.tagged_monthly, abs=0.02)
    assert s.savings_to_cost_ratio == pytest.approx(s.est_monthly_savings / s.dpo_monthly_cost, rel=0.01)
    total = spark.table(built.t("silver", "usage_cost")).agg(F.sum("cost")).collect()[0][0]
    assert s.share_of_total_spend == pytest.approx(s.dpo_window_cost / total, rel=0.01)
    assert 0 < s.share_of_total_spend < 0.01 and s.dpo_job_runs == 30


def test_self_cost_is_never_exposed_to_viewers():
    """Account-wide totals: analysts/operators only (gold), never a reporting view or the viewer dashboard."""
    from dpo.governance import REPORTING_VIEWS
    from dpo.config import DPOConfig
    from dpo import dashboard, genie
    assert not any(t.startswith("dpo_") for _, t, _, _ in REPORTING_VIEWS.values())
    cfg = DPOConfig.from_profile("enterprise_prod")
    assert "dpo_cost" not in str(dashboard.build(cfg, "cost_optimization"))
    assert "dpo_cost" not in str(genie.build(cfg, "cost_optimization"))
    assert "dpo_cost_summary" in str(genie.build(cfg, "engineering"))
