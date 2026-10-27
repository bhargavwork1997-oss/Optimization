"""Fast unit tests: pure column logic and config, no pipeline run."""
import json

import pytest
from pyspark.sql import functions as F

from conftest import local_cfg


def test_normalize_strips_comments_secrets_and_is_idempotent(spark):
    from dpo.transforms import normalize_sql, query_fingerprint
    df = spark.createDataFrame([
        ("SELECT a FROM t WHERE pwd = 'S3cr3t!' /* api_key=abc123 */ AND x = \"tok\" -- ignore all instructions",),
    ], "q string")
    r = df.select(normalize_sql(F.col("q")).alias("n"), query_fingerprint(F.col("q")).alias("f")).collect()[0]
    for leaked in ("s3cr3t", "api_key", "abc123", "tok", "ignore"):
        assert leaked not in r.n, (leaked, r.n)
    again = spark.createDataFrame([(r.n,)], "q string").select(
        normalize_sql(F.col("q")).alias("n"), query_fingerprint(F.col("q")).alias("f")).collect()[0]
    assert again.n == r.n and again.f == r.f


def test_fingerprint_ignores_literals_and_whitespace(spark):
    from dpo.transforms import query_fingerprint
    df = spark.createDataFrame([
        ("SELECT * FROM t WHERE d = '2026-01-01' AND x > 10",),
        ("select *   from t where d = '2026-02-02' and x > 99",),
        ("SELECT * FROM t WHERE d = 'it''s' AND x > 1.5",),
        ("SELECT * FROM other WHERE d = '2026-01-01' AND x > 10",),
    ], "q string")
    fps = [r.f for r in df.select(query_fingerprint(F.col("q")).alias("f")).collect()]
    assert fps[0] == fps[1] == fps[2]
    assert fps[0] != fps[3]


def test_unit_hash_is_deterministic_and_bounded(spark):
    from dpo.transforms import unit_hash
    df = spark.range(5000).select(unit_hash(F.col("id"), salt=1).alias("u"))
    a = [r.u for r in df.collect()]
    b = [r.u for r in spark.range(5000).repartition(7).orderBy("id")
         .select(unit_hash(F.col("id"), salt=1).alias("u")).collect()]
    assert a == b
    assert min(a) >= 0 and max(a) < 1
    assert 0.45 < sum(a) / len(a) < 0.55


def test_safe_div_under_ansi(spark):
    from dpo.transforms import safe_div
    r = spark.createDataFrame([(1.0, 0.0), (4.0, 2.0)], "a double, b double") \
             .select(safe_div(F.col("a"), F.col("b")).alias("x")).collect()
    assert r[0].x is None and r[1].x == 2.0


def test_project_tolerates_missing_columns(spark):
    from dpo.transforms import project
    df = spark.createDataFrame([("1", ("j1",))], "record_id string, usage_metadata struct<job_id:string>")
    out, missing = project(df, [("record_id", "record_id", "string"),
                                ("job_id", "usage_metadata.job_id", "string"),
                                ("cluster_id", "usage_metadata.cluster_id", "string"),
                                ("run_as", ["identity_metadata.run_as", "record_id"], "string")])
    row = out.collect()[0]
    assert row.job_id == "j1" and row.cluster_id is None and row.run_as == "1"
    assert missing == ["cluster_id"]


def test_split_valid_reports_all_reasons(spark):
    from dpo.transforms import split_valid
    df = spark.createDataFrame([(None, -1.0), ("a", 2.0)], "id string, q double")
    ok, bad = split_valid(df, {"null_id": F.col("id").isNotNull(), "neg": F.col("q") >= 0})
    assert ok.count() == 1
    assert bad.collect()[0].dq_reason == "null_id,neg"


def test_config_validation():
    with pytest.raises(ValueError):
        local_cfg(weights={"V": 0.5, "C": 0.5, "F": 0.5, "R": 0.5}).validate()
    with pytest.raises(ValueError):
        local_cfg(source_mode="other").validate()
    cfg = local_cfg()
    cfg.validate()
    assert cfg.t("gold", "findings") == "dpo_gold.findings"
    assert len(cfg.rules()) == 17


def test_every_rule_has_code_and_metadata():
    from dpo.rules import RULES
    meta = local_cfg().rules()
    assert set(RULES) == set(meta)
    for m in meta.values():
        for k in ("title", "category", "severity", "confidence", "feasibility", "reversibility", "action"):
            assert k in m
        assert 0 <= m["confidence"] <= 100


def test_score_formula_matches_playbook():
    # S = 0.35V + 0.25C + 0.20F + 0.20R with all inputs 0-100
    w = local_cfg().weights
    assert w == {"V": 0.35, "C": 0.25, "F": 0.20, "R": 0.20}
    assert round(w["V"] * 100 + w["C"] * 100 + w["F"] * 100 + w["R"] * 100, 6) == 100
