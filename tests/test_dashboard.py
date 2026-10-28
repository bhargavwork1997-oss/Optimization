"""Dashboards as code: generated files are current, read only allowed layers, are structurally valid,
and every dataset and widget query runs against tables produced by the real pipeline."""
import json
import os
import re

import pytest

from conftest import local_cfg

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PROFILES = sorted(f[:-5] for f in os.listdir(os.path.join(ROOT, "config", "environments")) if f.endswith(".json"))


def _cfg(profile):
    from dpo.config import DPOConfig
    cfg = DPOConfig.from_profile(profile)
    cfg.rules_path = os.path.join(ROOT, "config", "rules.json")
    return cfg


@pytest.mark.parametrize("profile", PROFILES)
def test_generated_files_are_current(profile):
    from dpo.dashboard import DASHBOARDS, file_path, render
    for key in DASHBOARDS:
        path = file_path(ROOT, profile, key)
        assert os.path.isfile(path), f"missing {path}: run `python -m dpo.dashboard` from src/"
        assert open(path, encoding="utf-8").read() == render(_cfg(profile), key), \
            f"{path} is stale: run `cd src && python -m dpo.dashboard` and commit"


@pytest.mark.parametrize("profile", PROFILES)
def test_dashboards_read_only_their_allowed_layers(profile):
    """The viewer dashboard must never touch gold/ops/silver (that would bypass row-level security)."""
    from dpo.config import LAYERS
    from dpo.dashboard import ALLOWED_LAYERS, build
    cfg = _cfg(profile)
    for key, allowed in ALLOWED_LAYERS.items():
        sql = " ".join(" ".join(d["queryLines"]) for d in build(cfg, key)["datasets"])
        assert "system." not in sql.lower()
        for layer in LAYERS:
            used = re.search(re.escape(cfg.schema(layer)) + r"\.\w+", sql) is not None
            assert layer in allowed or not used, f"{key} reads {layer}"
        refs = set(re.findall(r"\b(?:FROM|JOIN)\s+([\w.]+)", sql))
        assert refs and all(any(r.startswith(cfg.schema(l) + ".") for l in allowed) for r in refs), (key, refs)


@pytest.mark.parametrize("profile", PROFILES)
def test_structure_is_valid(profile):
    from dpo.dashboard import DASHBOARDS, build
    for key in DASHBOARDS:
        d = build(_cfg(profile), key)
        ds_names = {x["name"] for x in d["datasets"]}
        names = set()
        for page in d["pages"]:
            assert page["pageType"] == "PAGE_TYPE_CANVAS" and page["layout"]
            cells = set()
            for item in page["layout"]:
                w, pos = item["widget"], item["position"]
                assert w["name"] not in names, w["name"]
                names.add(w["name"])
                assert 0 <= pos["x"] and pos["x"] + pos["width"] <= 6 and pos["height"] > 0, (w["name"], pos)
                for x in range(pos["x"], pos["x"] + pos["width"]):
                    for y in range(pos["y"], pos["y"] + pos["height"]):
                        assert (x, y) not in cells, f"{key}/{page['name']}: {w['name']} overlaps at {(x, y)}"
                        cells.add((x, y))
                for q in w.get("queries", []):
                    assert q["query"]["datasetName"] in ds_names, (w["name"], q["query"]["datasetName"])
                spec = w.get("spec")
                if spec and spec["widgetType"].startswith("filter-"):
                    qnames = {q["name"] for q in w["queries"]}
                    assert {f["queryName"] for f in spec["encodings"]["fields"]} == qnames
                elif spec:
                    fields = {f["name"] for f in w["queries"][0]["query"]["fields"]}
                    enc = spec["encodings"]
                    used = [c["fieldName"] for c in enc.get("columns", [])] + \
                           [v["fieldName"] for v in enc.values() if isinstance(v, dict) and "fieldName" in v]
                    assert set(used) <= fields, (w["name"], set(used) - fields)


def test_publish_settings_and_acl():
    from dpo.dashboard import dashboard_acl
    prod = _cfg("enterprise_prod")
    cost = {a["group_name"]: a["permission_level"] for a in dashboard_acl(prod, "cost_optimization")}
    eng = {a["group_name"]: a["permission_level"] for a in dashboard_acl(prod, "engineering")}
    assert cost[prod.group("viewers")] == "CAN_RUN" and cost[prod.group("admins")] == "CAN_MANAGE"
    assert prod.group("viewers") not in eng, "engineering reads gold/ops: never viewers"
    assert dashboard_acl(_cfg("free_edition"), "engineering") == []


# ----------------------------------------------------------- executed queries --
@pytest.fixture(scope="module")
def built(spark):
    from dpo import pipeline, synth
    from dpo.governance import apply, build_views
    cfg = local_cfg(prefix="dsh_")
    cfg.rules_path = os.path.join(ROOT, "config", "rules.json")
    synth.generate(spark, cfg, scale="small", seed=42, end_date="2026-10-27")
    pipeline.run_all(spark, cfg, run_id="dsh")
    rows = apply(spark, cfg, build_views(spark, cfg), "dsh")
    assert all(r[3] == "OK" for r in rows), rows
    return cfg


def _widget_sql(dataset_sql, query):
    fields = [f for f in query["fields"] if "associative_filter_predicate_group" not in f["expression"]]
    sel = ", ".join(f"{f['expression']} AS `{f['name']}`" for f in fields)
    sql = f"SELECT {sel} FROM ({dataset_sql}) t"
    agg = re.compile(r"^(SUM|COUNT|COUNT_IF|AVG|MIN|MAX)\(", re.I)
    if not query.get("disaggregated", False):
        keys = [f["expression"] for f in fields if not agg.match(f["expression"])]
        if keys:
            sql += " GROUP BY " + ", ".join(keys)
    return sql


@pytest.mark.parametrize("key", ["cost_optimization", "engineering"])
def test_every_dataset_and_widget_query_runs(spark, built, key):
    from dpo.dashboard import build
    d = build(built, key)
    sql = {x["name"]: "".join(x["queryLines"]) for x in d["datasets"]}
    for name, q in sql.items():
        n = spark.sql(q).count()
        assert n > 0, f"{key}/{name} returned no rows on the reference data set"
    for page in d["pages"]:
        for item in page["layout"]:
            for q in item["widget"].get("queries", []):
                spark.sql(_widget_sql(sql[q["query"]["datasetName"]], q["query"])).collect()


def test_overview_counters_reconcile_with_gold(spark, built):
    from dpo.dashboard import datasets
    ds = {k: v[1] for k, v in datasets(built)["cost_optimization"].items()}
    window = spark.sql(f"SELECT sum(cost) FROM ({ds['ds_cost_daily']})").first()[0]
    gold = spark.sql(f"SELECT sum(cost) FROM {built.t('gold', 'cost_by_dimension')}").first()[0]
    assert abs(window - gold) < 1e-6
    rr = spark.sql(f"SELECT sum(monthly_cost) FROM ({ds['ds_workloads']})").first()[0]
    gold_rr = spark.sql(f"SELECT round(sum(round(monthly_cost, 2)), 2) FROM {built.t('gold', 'workload_summary')}").first()[0]
    assert abs(rr - gold_rr) < 0.01
    n = spark.sql(f"SELECT count(*), sum(is_p1) FROM ({ds['ds_backlog']})").first()
    assert n[0] == spark.table(built.t("gold", "recommendations")).count() and n[1] >= 1


def test_viewer_dashboard_respects_row_level_security(spark, built):
    """Run the cost dashboard's datasets as a viewer mapped to one cost centre: no other rows appear."""
    import copy
    from dpo.dashboard import datasets
    from dpo.governance import apply, build_views, plan_security
    cfg = copy.deepcopy(built)
    apply(spark, cfg, plan_security(cfg), "dsh-sec")
    cfg.governance.update(enforce=True, simulate_user="viewer@corp.com",
                          simulate_membership={"finance-cost-owners": True},
                          cost_center_groups={"finance-cost-owners": ["cc-finance"]})
    apply(spark, cfg, build_views(spark, cfg), "dsh-viewer")
    try:
        for name, (_, q) in datasets(cfg)["cost_optimization"].items():
            centres = {r[0] for r in spark.sql(f"SELECT DISTINCT cost_center FROM ({q})").collect()}
            assert centres <= {"cc-finance"}, (name, centres)
    finally:
        apply(spark, built, build_views(spark, built), "dsh-restore")
