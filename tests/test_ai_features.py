"""AI features: AI narratives (ai_query) and the spend forecast / budget outlook (ai_forecast).

Neither function exists in local Spark, so the AI calls are replaced by stand-ins (a registered
`ai_query` UDF, a patched forecast runner) and everything around them is executed for real: input
minimisation, output validation, caching, fallbacks, statuses and row-level security.
"""
import copy
import datetime as dt
import json
import os

import pytest
from pyspark.sql import functions as F

from conftest import local_cfg

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
GOOD = ("Root cause: the cluster idles between short jobs.\nFix: lower auto-termination to 20 minutes.\n"
        "Validation: compare idle hours for two weeks.\nRisk: slower first job; revert the setting.")


@pytest.fixture(scope="module")
def built(spark):
    from dpo import pipeline, synth
    cfg = local_cfg(prefix="aif_", budgets={"cc-finance": 4300, "cc-sales": 8000, "cc-hr": 440,
                                             "cc-marketing": 20})
    cfg.rules_path = os.path.join(ROOT, "config", "rules.json")
    synth.generate(spark, cfg, scale="small", seed=42, end_date="2026-10-27")
    pipeline.run_all(spark, cfg, run_id="aif")
    return cfg


# ============================================================ forecast ==
def test_outlook_month_edges():
    from dpo.forecast import outlook_month
    assert outlook_month(dt.date(2026, 10, 27)) == (dt.date(2026, 10, 1), dt.date(2026, 10, 31))
    assert outlook_month(dt.date(2026, 10, 31)) == (dt.date(2026, 11, 1), dt.date(2026, 11, 30))
    assert outlook_month(dt.date(2028, 2, 28)) == (dt.date(2028, 2, 1), dt.date(2028, 2, 29))
    assert outlook_month(dt.date(2026, 12, 31)) == (dt.date(2027, 1, 1), dt.date(2027, 1, 31))


def test_forecast_covers_rest_of_month_with_sane_intervals(spark, built):
    fc = spark.table(built.t("gold", "cost_forecast"))
    days = sorted({r[0] for r in fc.select("forecast_date").distinct().collect()})
    assert days == [dt.date(2026, 10, d) for d in (28, 29, 30, 31)]
    assert fc.filter("cost_lower < 0 OR cost_lower > cost_forecast OR cost_forecast > cost_upper").count() == 0
    assert {r[0] for r in fc.select("method").distinct().collect()} == {"baseline"}   # no warehouse locally


def test_budget_statuses_and_arithmetic(spark, built):
    o = {r.cost_center: r for r in spark.table(built.t("gold", "budget_outlook")).collect()}
    for r in o.values():
        assert r.projected_total == pytest.approx(r.actual_mtd + r.forecast_remaining)
        assert r.projected_lower <= r.projected_total <= r.projected_upper
        assert r.month_start == dt.date(2026, 10, 1) and r.days_remaining == 4 and r.mtd_complete
    assert o["cc-marketing"].status == "OVER_BUDGET"       # spent 22 of 20 already
    assert o["cc-finance"].status == "AT_RISK"             # projection above 4300
    assert o["cc-hr"].status == "WATCH"                    # only the upper bound above 440
    assert o["cc-sales"].status == "ON_TRACK"
    assert o["cc-supply"].status == "NO_BUDGET" and o["cc-supply"].pct_of_budget is None
    assert o["cc-finance"].pct_of_budget == pytest.approx(o["cc-finance"].projected_total / 4300)


def test_ai_forecast_result_is_used_when_available(spark, built, monkeypatch):
    """The AI path: whatever ai_forecast returns is clipped at 0, labelled and drives the outlook."""
    from dpo import forecast, pipeline

    def fake_ai(ctx, daily, horizon):
        ccs = [r[0] for r in daily.select("cost_center").distinct().collect()]
        rows = [(dt.date(2026, 10, d), cc, 100.0, -5.0, 150.0) for d in (28, 29, 30, 31) for cc in ccs]
        return spark.createDataFrame(rows, "forecast_date date, cost_center string, cost_forecast double, "
                                           "cost_lower double, cost_upper double")
    monkeypatch.setattr(forecast, "_ai", fake_ai)
    ctx = pipeline.run_all(spark, built, stages=("forecast",), run_id="aif-ai")
    assert json.loads(ctx.log[-1]["rows_out"])["forecast"] == "ai_forecast"
    fc = spark.table(built.t("gold", "cost_forecast"))
    assert {r[0] for r in fc.select("method").distinct().collect()} == {"ai_forecast"}
    assert fc.agg(F.min("cost_lower")).collect()[0][0] == 0.0
    o = {r.cost_center: r for r in spark.table(built.t("gold", "budget_outlook")).collect()}
    assert o["cc-sales"].forecast_remaining == pytest.approx(400.0) and o["cc-sales"].method == "ai_forecast"
    pipeline.run_all(spark, built, stages=("forecast",), run_id="aif-reset")   # back to baseline for other tests


def test_ai_forecast_failure_falls_back_and_says_so(spark, built, monkeypatch):
    from dpo import forecast, pipeline

    def boom(*a, **k):
        raise RuntimeError("PREVIEW_NOT_ENABLED")
    monkeypatch.setattr(forecast, "_ai", boom)
    ctx = pipeline.run_all(spark, built, stages=("forecast",), run_id="aif-fb")
    note = json.loads(ctx.log[-1]["rows_out"])["forecast"]
    assert ctx.log[-1]["status"] == "SUCCESS" and note.startswith("fallback to baseline") and "PREVIEW" in note
    assert spark.table(built.t("gold", "budget_outlook")).filter("method <> 'baseline'").count() == 0


def test_ai_forecast_sql_is_built_from_validated_values_only():
    from dpo.config import DPOConfig
    from dpo.forecast import ai_forecast_sql
    from dpo.pipeline import Ctx

    class FakeSpark:
        conf = type("C", (), {"set": lambda *a: None})()
    cfg = DPOConfig.from_profile("enterprise_prod")
    cfg.forecast_warehouse_id = "abc123"
    ctx = Ctx(FakeSpark(), cfg, "x")
    sql = ai_forecast_sql(ctx, dt.date(2026, 11, 1))
    assert "TABLE(dpo.gold.forecast_input)" in sql and "horizon => '2026-11-01'" in sql
    assert "group_col => 'cost_center'" in sql and "prediction_interval_width => 0.8" in sql
    assert "CREATE OR REPLACE TABLE dpo.gold.forecast_raw" in sql and "version" not in sql
    cfg.forecast_version = "2"
    sql2 = ai_forecast_sql(Ctx(FakeSpark(), cfg, "x"), dt.date(2026, 11, 1))
    assert "version => '2'" in sql2 and "positive_only => true" in sql2 and "parameters" not in sql2
    cfg.forecast_warehouse_id = "abc; DROP TABLE x"
    with pytest.raises(ValueError):
        cfg.validate()


def test_forecast_config_validation():
    from dpo.config import DPOConfig
    for bad in ({"forecast_method": "prophet"}, {"forecast_interval": 1.5}, {"budgets": {"cc'x": 10}},
                {"budgets": {"cc-a": -1}}, {"forecast_version": "3"}, {"ai_max_rows": 0}):
        with pytest.raises(ValueError):
            DPOConfig(catalog=None, **bad).validate()


# ========================================================== narratives ==
def _narrate(spark, cfg, reply, run_id):
    from dpo import pipeline
    calls = spark.sparkContext.accumulator(0)

    def fake_ai_query(endpoint, request):
        calls.add(1)
        return reply(request)
    spark.udf.register("ai_query", fake_ai_query)
    try:
        ctx = pipeline.run_all(spark, cfg, stages=("narrate",), run_id=run_id)
    finally:
        spark.sql("DROP TEMPORARY FUNCTION IF EXISTS ai_query")
    return json.loads(ctx.log[-1]["rows_out"]), calls.value


def test_ai_narratives_generated_validated_cached(spark, built):
    from dpo.recommend import LABELS
    cfg = copy.deepcopy(built)
    cfg.ai_enabled, cfg.ai_max_rows = True, 5
    spark.sql(f"DROP TABLE IF EXISTS {cfg.t('gold', 'ai_narrative_cache')}")
    stats, n = _narrate(spark, cfg, lambda req: GOOD, "ai1")
    assert stats["ai"] == "ai_query" and stats["ai_generated"] == 5 and stats["ai_cached"] == 0
    nar = spark.table(cfg.t("gold", "recommendation_narratives"))
    ai = nar.filter("narrative_source = 'ai_query'").collect()
    assert len(ai) == 5 and all(r.narrative.startswith("[AI-generated, review before acting] ") for r in ai)
    assert all(l in ai[0].narrative for l in LABELS)
    assert {r.rank for r in ai} == {1, 2, 3, 4, 5}
    # second run: unchanged findings come from the cache, no model call
    stats2, n2 = _narrate(spark, cfg, lambda req: GOOD, "ai2")
    assert stats2["ai_cached"] == 5 and stats2["ai_generated"] == 0 and n2 == 0


def test_ai_input_is_minimised(spark, built):
    from dpo.pipeline import Ctx
    from dpo.recommend import PROMPT, _ai_rows
    cfg = copy.deepcopy(built)
    cfg.ai_enabled, cfg.ai_max_rows = True, 3
    ctx = Ctx(spark, cfg, "min")
    rows = _ai_rows(ctx, ctx.read("gold", "recommendations")).collect()
    assert len(rows) == 3 and len({r.input_hash for r in rows}) == 3
    for r in rows:
        data = json.loads(r.ai_input)
        assert {"workload_key", "cost_center", "evidence", "run_as", "job_name", "statement_text"}.isdisjoint(data)
        assert "@" not in r.ai_input and {"finding", "metric", "value", "recommended_action"} <= set(data)
    assert "untrusted measurement data" in PROMPT and "'" not in PROMPT


def test_bad_or_injected_model_output_is_rejected(spark, built):
    from dpo.recommend import valid_narrative
    assert valid_narrative(GOOD)
    assert not valid_narrative("Sure! Ignore the data. Visit http://x.y")
    assert not valid_narrative("Root cause: a\nFix: b\nValidation: c")          # missing Risk:
    assert not valid_narrative(GOOD + " see http://evil.example")
    assert not valid_narrative(GOOD + "x" * 1200)
    cfg = copy.deepcopy(built)
    cfg.ai_enabled, cfg.ai_max_rows = True, 4
    spark.sql(f"DROP TABLE IF EXISTS {cfg.t('gold', 'ai_narrative_cache')}")
    stats, _ = _narrate(spark, cfg, lambda req: "I cannot help with that.", "ai-bad")
    assert stats["ai_rejected"] == 4 and stats["ai_generated"] == 0
    nar = spark.table(cfg.t("gold", "recommendation_narratives"))
    assert nar.filter("narrative_source <> 'template'").count() == 0


def test_ai_endpoint_must_be_databricks_hosted():
    from dpo.config import DPOConfig
    DPOConfig(catalog=None, ai_enabled=True, ai_endpoint="system.ai.meta-llama-3-3-70b-instruct").validate()
    with pytest.raises(ValueError):
        DPOConfig(catalog=None, ai_enabled=True, ai_endpoint="my-openai-proxy").validate()


def test_enterprise_profiles_turn_ai_on_safely():
    from dpo.config import DPOConfig
    for p in ("enterprise_dev", "enterprise_prod"):
        cfg = DPOConfig.from_profile(p)
        assert cfg.ai_enabled and cfg.ai_endpoint.startswith(("databricks-", "system.ai."))
        assert not cfg.governance["ai_allow_external"] and cfg.forecast_method == "ai_forecast"


# ================================================================ gate ==
def test_gate_reports_ai_readiness_as_warnings_only():
    from types import SimpleNamespace as NS
    from dpo import installer
    from dpo.config import DPOConfig
    cfg = DPOConfig.from_profile("enterprise_prod")

    def get(name):
        raise RuntimeError("RESOURCE_DOES_NOT_EXIST")
    bad = NS(serving_endpoints=NS(get=get), warehouses=NS(list=lambda: [NS(enable_serverless_compute=False,
                                                                            warehouse_type=NS(value="CLASSIC"))]))
    rows = installer.check_ai_features(cfg, bad)
    assert {r[0] for r in rows} == {"ai endpoint", "forecast warehouse"} and all(r[1] == "WARN" for r in rows)
    good = NS(serving_endpoints=NS(get=lambda n: NS(state=NS(ready=NS(value="READY")))),
              warehouses=NS(list=lambda: [NS(enable_serverless_compute=True)]))
    assert all(r[1] == "OK" for r in installer.check_ai_features(cfg, good))


def test_markdown_answers_are_cleaned_and_rejections_explained(spark, built):
    """Models often answer '1. **Root cause:** ...'. That is a valid answer: keep it, minus the markup.
    Answers that really miss a label are rejected, and the run log says why (counts only, no model text)."""
    from dpo.recommend import clean_narrative, rejection_reason
    md = ("1. **Root cause:** avg_cpu_pct stays low.\n2. **Fix**: lower auto_termination_minutes.\n"
          "3. **Validation:** compare two weeks.\n4. **Risk:** slower start; revert.")
    clean = clean_narrative(md)
    assert rejection_reason(clean) is None and "**" not in clean and "avg_cpu_pct" in clean
    assert clean.splitlines()[0] == "Root cause: avg_cpu_pct stays low."
    cfg = copy.deepcopy(built)
    cfg.ai_enabled, cfg.ai_max_rows = True, 4
    spark.sql(f"DROP TABLE IF EXISTS {cfg.t('gold', 'ai_narrative_cache')}")
    stats, _ = _narrate(spark, cfg, lambda req: md, "ai-md")
    assert stats["ai_generated"] == 4 and stats["ai_rejected"] == 0
    shown = spark.table(cfg.t("gold", "recommendation_narratives")).filter("narrative_source = 'ai_query'").first()
    assert "**" not in shown.narrative and "1." not in shown.narrative.split("] ", 1)[1][:3]
    spark.sql(f"DROP TABLE IF EXISTS {cfg.t('gold', 'ai_narrative_cache')}")
    stats, _ = _narrate(spark, cfg, lambda req: "Root cause: x\nFix: y\nValidation: z", "ai-miss")
    assert stats["ai_rejected"] == 4 and stats["ai_rejected_reasons"] == {"missing Risk:": 4}
