"""Governance, security hardening and portability tests.

Unity Catalog statements (GRANT, OWNER, TAGS) cannot run on local Spark, so they are verified
as generated SQL. Row filtering, salted identity hashing, inference masking, authorization,
audit, raw-SQL minimisation and pricing ARE executed locally with simulated group membership.
"""
import copy

import pytest
from pyspark.sql import functions as F

from conftest import local_cfg

ADMINS, OPS, ANA, VIEW = "dpo-admins", "dpo-operators", "dpo-analysts", "dpo-viewers"


# ------------------------------------------------------------------ profiles --
def test_all_profiles_load_and_validate():
    from dpo.config import DPOConfig
    for name in ("free_edition", "enterprise_dev", "enterprise_prod"):
        cfg = DPOConfig.from_profile(name)
        cfg.validate()
        assert cfg.profile == name
    free, prod = DPOConfig.from_profile("free_edition"), DPOConfig.from_profile("enterprise_prod")
    assert free.schema("gold") == "workspace.dpo_gold" and not free.governance["enforce"]
    assert prod.schema("gold") == "dpo.gold" and prod.governance["enforce"] and prod.source_mode == "system"
    assert not prod.ai_enabled and not prod.governance["retain_raw_sql"]


def test_profile_overrides_and_errors():
    from dpo.config import DPOConfig
    cfg = DPOConfig.from_profile("enterprise_prod", {"catalog": "acme_dpo", "lookback_days": 45, "end_date": ""})
    assert cfg.catalog == "acme_dpo" and cfg.lookback_days == 45 and cfg.end_date is None
    assert cfg.group("admins") == ADMINS
    with pytest.raises(ValueError, match="unknown profile"):
        DPOConfig.from_profile("nope")
    with pytest.raises(ValueError, match="unknown config keys"):
        DPOConfig.from_dict({"catalg": "x"})


@pytest.mark.parametrize("field,value,msg", [
    ("catalog", "dpo; DROP CATALOG x", "illegal characters in catalog"),
    ("schema_prefix", "a`b", "illegal characters in schema_prefix"),
    ("managed_location", "abfss://x/'; DROP", "managed_location"),
    ("inventory_catalogs", ["ok", "bad name"], "inventory catalog"),
    ("contract_prices", {"%JOBS%'": 0.1}, "bad contract price"),
    ("ai_endpoint", "x'); DROP", "illegal ai_endpoint"),
])
def test_sql_injection_inputs_are_rejected(field, value, msg):
    from dpo.config import DPOConfig
    cfg = DPOConfig.from_profile("enterprise_prod")
    setattr(cfg, field, value)
    with pytest.raises(ValueError, match=msg):
        cfg.validate()


def test_group_and_cost_center_injection_rejected():
    from dpo.config import DPOConfig
    cfg = DPOConfig.from_profile("enterprise_prod")
    cfg.governance["groups"]["viewers"] = "evil'); DROP"
    with pytest.raises(ValueError, match="illegal character in group"):
        cfg.validate()
    cfg = DPOConfig.from_profile("enterprise_prod")
    cfg.governance["cost_center_groups"] = {"g": ["cc') OR true --"]}
    with pytest.raises(ValueError, match="illegal character in cost centre"):
        cfg.validate()


def test_test_hooks_refused_against_unity_catalog():
    from dpo.config import DPOConfig
    cfg = DPOConfig.from_profile("enterprise_prod")
    cfg.governance["simulate_membership"] = {OPS: True}
    with pytest.raises(ValueError, match="test-only"):
        cfg.validate()


def test_external_ai_endpoint_needs_explicit_approval():
    from dpo.config import DPOConfig
    cfg = DPOConfig.from_profile("enterprise_prod", {"ai_enabled": True, "ai_endpoint": "openai-gpt"})
    with pytest.raises(ValueError, match="Databricks-hosted"):
        cfg.validate()
    cfg.governance["ai_allow_external"] = True
    cfg.validate()


# --------------------------------------------------------------- grant plan ---
def _sql(stmts):
    return [s.sql for s in stmts]


def test_grant_plan_least_privilege():
    from dpo.config import DPOConfig
    from dpo.governance import plan_grants, plan_security, plan_setup
    cfg = DPOConfig.from_profile("enterprise_prod", {"managed_location": "abfss://dpo@acct.dfs.core.windows.net/"})
    setup, grants = _sql(plan_setup(cfg)), _sql(plan_grants(cfg))
    assert setup[0] == "CREATE CATALOG IF NOT EXISTS dpo MANAGED LOCATION 'abfss://dpo@acct.dfs.core.windows.net/'"
    assert f"ALTER CATALOG dpo OWNER TO `{ADMINS}`" in grants
    assert all(f"ALTER SCHEMA dpo.{l} OWNER TO `{ADMINS}`" in grants for l in ("bronze", "reporting", "security"))
    assert f"ALTER TABLE dpo.ops.audit_log OWNER TO `{ADMINS}`" in grants
    assert f"ALTER VIEW dpo.reporting.recommendations OWNER TO `{ADMINS}`" in grants

    def who(grp):
        return " ".join(g for g in grants if f"`{grp}`" in g and not g.startswith("ALTER"))
    # viewers: catalog browse + reporting only
    assert sorted(g for g in grants if f"`{VIEW}`" in g) == sorted([
        f"GRANT USE CATALOG ON CATALOG dpo TO `{VIEW}`",
        f"GRANT USE SCHEMA, SELECT ON SCHEMA dpo.reporting TO `{VIEW}`"])
    # analysts: never raw/sensitive objects
    for forbidden in ("dpo.bronze", "dpo.sim", "dpo.security", "quarantine", "audit_log"):
        assert forbidden not in who(ANA), forbidden
    # operators: can write data layers but cannot create in reporting or touch security
    assert f"GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA dpo.gold TO `{OPS}`" in grants
    assert f"GRANT USE SCHEMA, SELECT ON SCHEMA dpo.reporting TO `{OPS}`" in grants
    assert "dpo.security" not in who(OPS)
    assert not any("reporting" in g and "CREATE" in g and f"`{OPS}`" in g for g in grants)
    assert not any("system." in g for g in grants)
    sec = _sql(plan_security(DPOConfig.from_profile("enterprise_prod")))
    assert any("audit_log" in s and "'delta.appendOnly' = 'true'" in s for s in sec)


def test_system_grants_only_when_enabled():
    from dpo.config import DPOConfig
    from dpo.governance import plan_grants
    cfg = DPOConfig.from_profile("enterprise_prod")
    cfg.governance["grant_system_access"] = True
    sys_grants = [g for g in _sql(plan_grants(cfg)) if "system." in g]
    assert f"GRANT USE SCHEMA, SELECT ON SCHEMA system.billing TO `{OPS}`" in sys_grants
    assert f"GRANT USE SCHEMA, SELECT ON SCHEMA system.access TO `{ADMINS}`" in sys_grants


def test_free_edition_creates_no_catalog_and_filters_nothing():
    from dpo.config import DPOConfig
    from dpo.governance import plan_setup, privileged_expr, row_visibility_expr
    cfg = DPOConfig.from_profile("free_edition")
    assert not any("CREATE CATALOG" in s for s in _sql(plan_setup(cfg)))
    assert privileged_expr(cfg) == "true" and row_visibility_expr(cfg) == "true"


# ------------------------------------------------- executed governance tests --
@pytest.fixture(scope="module")
def gov(spark):
    from dpo import governance, pipeline, synth
    cfg = local_cfg(prefix="gov_")
    synth.generate(spark, cfg, scale="small", seed=42, end_date="2026-10-27")
    pipeline.run_all(spark, cfg, run_id="gov")
    governance.apply(spark, cfg, governance.plan_security(cfg), "gov-sec")
    return cfg


def _as(base, groups, user="someone@corp.com", cc_groups=None):
    cfg = copy.deepcopy(base)
    cfg.governance.update(enforce=True, simulate_user=user,
                          simulate_membership={g: True for g in groups},
                          cost_center_groups=cc_groups if cc_groups is not None else {"finance-cost-owners": ["cc-finance"]})
    return cfg


def _views(spark, cfg):
    from dpo.governance import apply, build_views
    rows = apply(spark, cfg, build_views(spark, cfg), "gov-test")
    assert all(r[3] == "OK" for r in rows), rows


def test_viewer_sees_only_own_cost_center(spark, gov):
    _views(spark, _as(gov, ["finance-cost-owners"]))
    for v in ("recommendations", "workload_summary", "cost_by_dimension", "job_scorecard"):
        ccs = {r[0] for r in spark.table(f"gov_reporting.{v}").select("cost_center").distinct().collect()}
        assert ccs == {"cc-finance"}, (v, ccs)


def test_viewer_identities_salted_hashed(spark, gov):
    _views(spark, _as(gov, ["finance-cost-owners"]))
    masked = {r.run_as for r in spark.table("gov_reporting.cost_by_dimension").select("run_as").distinct().collect()}
    assert masked and all(u.startswith("user-") and "@" not in u for u in masked)
    raw = [r.run_as for r in spark.table("gov_gold.cost_by_dimension").filter("cost_center = 'cc-finance'")
           .select("run_as").distinct().collect()]
    unsalted = {r.h for r in spark.createDataFrame([(u,) for u in raw], "u string")
                .select(F.concat(F.lit("user-"), F.substring(F.sha2(F.lower("u"), 256), 1, 12)).alias("h")).collect()}
    assert not masked & unsalted          # a dictionary of employee emails cannot reverse the hash


def test_viewer_cannot_infer_account_totals(spark, gov):
    _views(spark, _as(gov, ["finance-cost-owners"]))
    ws = spark.table("gov_reporting.workload_summary")
    assert ws.filter(F.col("share_of_total").isNotNull() | F.col("cost_rank").isNotNull()).count() == 0
    recs = spark.table("gov_reporting.recommendations")
    assert recs.filter(F.col("rank").isNotNull() | F.col("evidence").isNotNull()).count() == 0


def test_access_map_grants_individual_users_extra_rows(spark, gov):
    spark.sql("INSERT INTO gov_security.access_map VALUES ('lead@corp.com', 'cc-sales', 'admin', current_timestamp())")
    _views(spark, _as(gov, ["finance-cost-owners"], user="LEAD@corp.com"))
    ccs = {r[0] for r in spark.table("gov_reporting.recommendations").select("cost_center").distinct().collect()}
    assert ccs == {"cc-finance", "cc-sales"}


def test_unmapped_viewer_sees_nothing(spark, gov):
    _views(spark, _as(gov, [VIEW], cc_groups={}))
    assert spark.table("gov_reporting.recommendations").count() == 0
    assert spark.table("gov_reporting.cost_by_dimension").count() == 0


def test_analyst_sees_everything_unmasked(spark, gov):
    _views(spark, _as(gov, [ANA]))
    assert spark.table("gov_reporting.recommendations").count() == spark.table("gov_gold.recommendations").count()
    assert spark.table("gov_reporting.cost_by_dimension").filter(F.col("run_as").contains("@")).count() > 0
    assert spark.table("gov_reporting.workload_summary").filter(F.col("share_of_total").isNotNull()).count() > 0


def test_unauthorised_user_cannot_run_pipeline_and_is_audited(spark, gov):
    from dpo import pipeline
    cfg = _as(gov, [VIEW], user="intruder@corp.com")
    with pytest.raises(PermissionError, match="intruder@corp.com is not a member"):
        pipeline.run_all(spark, cfg, run_id="denied")
    denied = spark.table("gov_ops.audit_log").filter("run_id = 'denied'").collect()
    assert [(r.principal, r.action, r.status) for r in denied] == [("intruder@corp.com", "authorize", "DENIED")]


def test_operator_runs_but_cannot_administer(spark, gov):
    from dpo import governance
    op = _as(gov, [OPS], user="sp-dpo")
    assert governance.is_authorized(spark, op)[0]
    assert not governance.is_authorized(spark, _as(gov, [ANA]))[0]          # analysts read, never run
    with pytest.raises(PermissionError):
        governance.admin_apply(spark, op, run_id="op-admin")                  # operators cannot change grants
    assert governance.is_authorized(spark, _as(gov, [ADMINS]), roles=("admins",))[0]


def test_enforced_pipeline_never_redefines_reporting_views(spark, gov):
    """An operator run verifies the admin-owned views; it must not CREATE OR REPLACE them."""
    from dpo import pipeline
    _views(spark, _as(gov, ["finance-cost-owners"]))        # admin-defined, filtered views
    before = spark.table("gov_reporting.recommendations").count()
    pipeline.run_all(spark, _as(gov, [OPS], user="sp-dpo"), run_id="op-run")
    log = spark.table("gov_ops.governance_log").collect()
    assert log and all(r.section == "verify" for r in log)
    assert spark.table("gov_reporting.recommendations").count() == before


def test_successful_run_is_audited(spark, gov):
    rows = spark.table("gov_ops.audit_log").filter("run_id = 'gov'").orderBy("logged_at").collect()
    assert [(r.action, r.status) for r in rows] == [("authorize", "ALLOWED"), ("pipeline_run", "SUCCESS")]


def test_raw_sql_never_persisted(spark, gov):
    texts = [r.statement_text for r in spark.table("gov_bronze.query_history").select("statement_text").limit(2000).collect()]
    assert texts and not any("'" in t or "/*" in t or "--" in t for t in texts)
    assert not any("ignore previous" in t for t in texts)


def test_ai_failure_falls_back_to_templates(spark, gov):
    from dpo import pipeline
    cfg = copy.deepcopy(gov)
    cfg.ai_enabled = True                      # ai_query does not exist locally -> must fall back
    ctx = pipeline.run_all(spark, cfg, stages=("narrate",), run_id="ai")
    assert ctx.log[-1]["status"] == "SUCCESS" and "fallback" in ctx.log[-1]["rows_out"]
    n = spark.table("gov_gold.recommendation_narratives")
    assert n.filter("narrative_source <> 'template'").count() == 0 and n.count() > 0


# ----------------------------------------------------------------- portability --
def test_workspace_scoping_contract_and_discount_pricing(spark, gov):
    from dpo import pipeline
    base_cost = spark.table("gov_silver.usage_cost").agg(F.sum("cost")).collect()[0][0]

    pipeline.run_all(spark, local_cfg(prefix="gov_", workspace_ids=["999"]), stages=("bronze",), run_id="scope")
    assert spark.table("gov_bronze.billing_usage").count() == 0

    disc = local_cfg(prefix="gov_", price_multiplier=0.8)
    pipeline.run_all(spark, disc, stages=("bronze", "silver"), run_id="disc")
    assert spark.table("gov_silver.usage_cost").agg(F.sum("cost")).collect()[0][0] == pytest.approx(base_cost * 0.8)

    con = local_cfg(prefix="gov_", contract_prices={"%ALL_PURPOSE%": 0.40})
    pipeline.run_all(spark, con, stages=("bronze", "silver"), run_id="contract")
    u = spark.table("gov_silver.usage_cost")
    ap = u.filter("sku_name LIKE '%ALL_PURPOSE%'").select("unit_price", "price_source").distinct().collect()
    assert [(r.unit_price, r.price_source) for r in ap] == [(0.40, "contract")]
    assert u.filter("sku_name NOT LIKE '%ALL_PURPOSE%' AND price_source <> 'list'").count() == 0
