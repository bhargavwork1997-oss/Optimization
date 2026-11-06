"""Genie spaces as code: files are current, follow the serialized_space rules the API enforces, read only
allowed layers, never sample values from secured views, and every example/benchmark SQL runs against the
real pipeline output."""

import json
import os
import re
from pathlib import Path

import pytest

from conftest import local_cfg

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PROFILES = sorted(f[:-5] for f in os.listdir(os.path.join(ROOT, "config", "environments")) if f.endswith(".json"))
HEX32 = re.compile(r"^[0-9a-f]{32}$")
WRITE_SQL = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|GRANT|REVOKE|TRUNCATE)\b", re.I)


def _cfg(profile):
    from dpo.config import DPOConfig

    cfg = DPOConfig.from_profile(profile)
    cfg.rules_path = os.path.join(ROOT, "config", "rules.json")
    return cfg


@pytest.mark.parametrize("profile", PROFILES)
def test_generated_files_are_current(profile):
    from dpo.genie import SPACES, file_path, render

    for key in SPACES:
        path = file_path(ROOT, profile, key)
        assert os.path.isfile(path), f"missing {path}: run `cd src && python -m dpo.genie`"
        assert Path(path).read_text(encoding="utf-8") == render(_cfg(profile), key), (
            f"{path} is stale: run `cd src && python -m dpo.genie` and commit"
        )


def _ids(space):
    out = []

    def walk(x):
        if isinstance(x, dict):
            if "id" in x:
                out.append(x["id"])
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(space)
    return out


@pytest.mark.parametrize("profile", PROFILES)
def test_serialized_space_rules(profile):
    """Version 2; 32-hex unique ids; arrays sorted; text fields are lists of strings; no id on tables;
    benchmark SQL inside answer[].content; limits."""
    from dpo.genie import SPACES, build

    seen = set()
    for key in SPACES:
        s = build(_cfg(profile), key)
        assert s["version"] == 2
        ids = _ids(s)
        assert all(HEX32.match(i) for i in ids) and len(ids) == len(set(ids)) and not (seen & set(ids))
        seen |= set(ids)
        tables = s["data_sources"]["tables"]
        assert [t["identifier"] for t in tables] == sorted(t["identifier"] for t in tables)
        for t in tables:
            assert "id" not in t
            cc = [c["column_name"] for c in t.get("column_configs", [])]
            assert cc == sorted(cc) and len(cc) == len(set(cc))
        ins = s["instructions"]
        for arr in (
            s["config"]["sample_questions"],
            ins["text_instructions"],
            ins["example_question_sqls"],
            ins["sql_snippets"]["measures"],
            s["benchmarks"]["questions"],
        ):
            assert [x["id"] for x in arr] == sorted(x["id"] for x in arr)
        assert len(ins["text_instructions"]) == 1
        for q in s["config"]["sample_questions"]:
            assert isinstance(q["question"], list) and all(isinstance(x, str) for x in q["question"])
        for e in ins["example_question_sqls"]:
            assert isinstance(e["question"], list) and isinstance(e["sql"], list) and len(e["sql"]) > 1
        for b in s["benchmarks"]["questions"]:
            assert "sql" not in b and b["answer"][0]["format"] == "SQL"
            assert isinstance(b["answer"][0]["content"], list)
        assert len(s["benchmarks"]["questions"]) <= 50


@pytest.mark.parametrize("profile", PROFILES)
def test_spaces_read_only_their_allowed_layers(profile):
    from dpo.config import LAYERS
    from dpo.genie import ALLOWED_LAYERS, build

    cfg = _cfg(profile)
    for key, allowed in ALLOWED_LAYERS.items():
        s = build(cfg, key)
        idents = [t["identifier"] for t in s["data_sources"]["tables"]]
        assert idents and all(any(i.startswith(cfg.schema(layer) + ".") for layer in allowed) for i in idents), (
            key,
            idents,
        )
        sql = json.dumps(s["instructions"]["example_question_sqls"]) + json.dumps(s["benchmarks"])
        assert "system." not in sql.lower()
        for layer in set(LAYERS) - set(allowed):
            assert not re.search(re.escape(cfg.schema(layer)) + r"\.\w+", sql), f"{key} reads {layer}"
        for name in ("audit_log", "governance_log", "quarantine", "access_map", "settings"):
            assert not any(i.endswith("." + name) for i in idents), f"{key} exposes {name}"


@pytest.mark.parametrize("profile", PROFILES)
def test_no_value_sampling_on_secured_views(profile):
    """Prompt matching samples values with the author's rights and shares them with every reader: on the
    row-filtered reporting views that would leak other cost centres. Every column must be switched off."""
    from dpo.genie import TABLES, build

    cfg = _cfg(profile)
    s = build(cfg, "cost_optimization")
    for t in s["data_sources"]["tables"]:
        layer_name = next(k for k in TABLES["cost_optimization"] if cfg.t(*k) == t["identifier"])
        cols = {c["column_name"]: c for c in t["column_configs"]}
        assert set(cols) == set(TABLES["cost_optimization"][layer_name]), t["identifier"]
        assert all(
            c["enable_format_assistance"] is False and c["enable_entity_matching"] is False for c in cols.values()
        )
        assert not any(k in c for c in cols.values() for k in ("get_example_values", "build_value_dictionary"))
    eng = {t["identifier"]: t for t in build(cfg, "engineering")["data_sources"]["tables"]}
    run_as = {c["column_name"]: c for c in eng[cfg.t("gold", "cost_by_dimension")]["column_configs"]}["run_as"]
    assert run_as["enable_format_assistance"] is False and run_as["enable_entity_matching"] is False


@pytest.mark.parametrize("profile", PROFILES)
def test_static_text_is_read_only_and_client_free(profile):
    from dpo.genie import build

    for key in ("cost_optimization", "engineering"):
        s = build(_cfg(profile), key)
        for e in s["instructions"]["example_question_sqls"]:
            assert not WRITE_SQL.search("".join(e["sql"])), e["question"]
        text = json.dumps(s)
        assert "@" not in text, "no e-mail addresses / identities in shared Genie text"


def test_acl():
    from dpo.genie import genie_acl

    prod = _cfg("enterprise_prod")
    cost = {a["group_name"]: a["permission_level"] for a in genie_acl(prod, "cost_optimization")}
    eng = {a["group_name"]: a["permission_level"] for a in genie_acl(prod, "engineering")}
    assert cost[prod.group("viewers")] == "CAN_RUN" and prod.group("viewers") not in eng
    managers = [g for acl in (cost, eng) for g, lvl in acl.items() if lvl in ("CAN_MANAGE", "CAN_EDIT")]
    assert set(managers) == {prod.group("admins")}, "only admins may edit or see all conversations"
    assert genie_acl(_cfg("free_edition"), "engineering") == []


# ----------------------------------------------------------- executed queries --
@pytest.fixture(scope="module")
def built(spark):
    from dpo import pipeline, synth
    from dpo.governance import apply, build_views

    cfg = local_cfg(prefix="gni_")
    cfg.rules_path = os.path.join(ROOT, "config", "rules.json")
    synth.generate(spark, cfg, scale="small", seed=42, end_date="2026-10-27")
    pipeline.run_all(spark, cfg, run_id="gni")
    rows = apply(spark, cfg, build_views(spark, cfg), "gni")
    assert all(r[3] == "OK" for r in rows), rows
    return cfg


def test_declared_columns_match_tables(spark, built):
    from dpo.genie import TABLES

    for key, tables in TABLES.items():
        for (layer, name), cols in tables.items():
            actual = spark.table(built.t(layer, name)).columns
            if cols is not None:
                assert cols == actual, (key, layer, name, set(cols) ^ set(actual))


@pytest.mark.parametrize("key", ["cost_optimization", "engineering"])
def test_every_example_and_benchmark_runs(spark, built, key):
    from dpo.genie import build

    s = build(built, key)
    sqls = ["".join(e["sql"]) for e in s["instructions"]["example_question_sqls"]] + [
        "".join(b["answer"][0]["content"]) for b in s["benchmarks"]["questions"]
    ]
    assert sqls
    for sql in sqls:
        df = spark.sql(sql)
        assert df.columns and df.limit(5).count() >= 0
    for m in s["instructions"]["sql_snippets"]["measures"]:
        table = m["sql"][0].split("(")[-1].split(".")[0].strip()
        layer = "reporting" if key == "cost_optimization" else "gold"
        spark.sql(f"SELECT {m['sql'][0]} AS v FROM {built.t(layer, table)} AS {table}").collect()


def test_cost_examples_return_rows(spark, built):
    from dpo.genie import build

    s = build(built, "cost_optimization")
    for e in s["instructions"]["example_question_sqls"]:
        assert spark.sql("".join(e["sql"])).count() > 0, e["question"]
