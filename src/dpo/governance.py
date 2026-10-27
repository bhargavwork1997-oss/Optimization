"""Governance for the DPO accelerator (Unity Catalog).

Role model (group names come from the profile):

  admins     own catalog, schemas, reporting views, audit log; manage grants; read everything
  operators  run the pipeline (job service principal); read/write the data layers only
  analysts   read silver, gold, reporting and ops health tables; see identities
  viewers    read `reporting` views only, filtered to their cost centres, identities salted-hashed

Two execution paths, deliberately separated:

  pipeline (operators)  authorize -> ensure data schemas -> stages -> verify governance (read-only)
  admin    (admins)     admin_apply(): security schema, grants, ownership, reporting views,
                        append-only audit log, drift revocation

The pipeline identity can never change grants, re-define reporting views or edit the
access map, so a compromised or careless operator cannot widen what viewers see.

Everything is generated as SQL first: reviewable (dry run) and unit-testable without a workspace.
"""
from __future__ import annotations

import getpass
from dataclasses import dataclass
from typing import Optional

from .config import DATA_LAYERS, LAYERS, SYSTEM_SOURCES, DPOConfig

SHARED_CATALOGS = {"workspace", "main", "hive_metastore", "system", "samples"}

# reporting view -> (source layer, source table, identity columns hashed for viewers,
#                    columns nulled for viewers because they reveal account-wide totals)
REPORTING_VIEWS = {
    # evidence can hold SQL shapes from other teams on a shared warehouse: analysts only
    "recommendations": ("gold", "recommendations", [], ["rank", "savings_rank", "evidence"]),
    "recommendation_narratives": ("gold", "recommendation_narratives", [], ["rank"]),
    "workload_summary": ("gold", "workload_summary", [], ["share_of_total", "cost_rank"]),
    "job_scorecard": ("gold", "job_scorecard", ["creator_id"], []),
    "cost_by_dimension": ("gold", "cost_by_dimension", ["run_as"], []),
}
ANALYST_OPS_TABLES = ("run_log", "dq_report", "source_status", "rule_status", "test_results",
                      "perf_results", "governance_log", "governance_drift")
SYSTEM_SCHEMAS = ("billing", "lakeflow", "query", "compute", "access")
AUDIT_COLUMNS = ("principal STRING, profile STRING, source_mode STRING, action STRING, status STRING, "
                 "detail STRING, run_id STRING, logged_at TIMESTAMP")


@dataclass
class Stmt:
    section: str
    purpose: str
    sql: str
    requires_table: Optional[str] = None


# --------------------------------------------------------------- expressions --
def _p(principal: str) -> str:
    return f"`{principal}`"


def member(cfg: DPOConfig, group: str) -> str:
    sim = cfg.governance.get("simulate_membership")
    if sim is not None:
        return "true" if sim.get(group) else "false"
    return f"{cfg.governance['membership_function']}('{group}')"


def current_user_expr(cfg: DPOConfig) -> str:
    u = cfg.governance.get("simulate_user")
    return f"'{u}'" if u else "current_user()"


def privileged_expr(cfg: DPOConfig) -> str:
    """Members who see every row and real identities in reporting views."""
    if not cfg.governance["enforce"]:
        return "true"
    return "(" + " OR ".join(member(cfg, cfg.group(r)) for r in ("admins", "operators", "analysts")) + ")"


def row_visibility_expr(cfg: DPOConfig, cc_col: str = "cost_center") -> str:
    if not cfg.governance["enforce"]:
        return "true"
    cc = f"coalesce({cc_col}, '(untagged)')"
    parts = [privileged_expr(cfg)]
    for grp, centres in sorted(cfg.governance["cost_center_groups"].items()):
        vals = ", ".join(f"'{c}'" for c in centres)          # validated by DPOConfig.validate
        parts.append(f"({member(cfg, grp)} AND {cc} IN ({vals}))")
    parts.append(f"EXISTS (SELECT 1 FROM {cfg.t('security', 'access_map')} m "
                 f"WHERE lower(m.principal) = lower({current_user_expr(cfg)}) AND m.cost_center = {cc})")
    return "(" + "\n   OR ".join(parts) + ")"


def identity_expr(cfg: DPOConfig, col: str) -> str:
    if not (cfg.governance["enforce"] and cfg.governance["mask_identities_for_viewers"]):
        return f"`{col}`"
    salt = (f"(SELECT max(setting_value) FROM {cfg.t('security', 'settings')} "
            f"WHERE setting_key = 'identity_salt')")
    return (f"CASE WHEN {privileged_expr(cfg)} THEN `{col}` WHEN `{col}` IS NULL THEN NULL "
            f"ELSE concat('user-', substr(sha2(concat({salt}, lower(`{col}`)), 256), 1, 12)) END")


def masked_expr(cfg: DPOConfig, col: str) -> str:
    if not cfg.governance["enforce"]:
        return f"`{col}`"
    return f"CASE WHEN {privileged_expr(cfg)} THEN `{col}` END"


# ----------------------------------------------------------------- statements --
def plan_setup(cfg: DPOConfig) -> list[Stmt]:
    """Catalog + data-layer schemas. Safe for the pipeline identity (IF NOT EXISTS)."""
    out = []
    if cfg.catalog and cfg.catalog not in SHARED_CATALOGS:
        loc = f" MANAGED LOCATION '{cfg.managed_location}'" if cfg.managed_location else ""
        out.append(Stmt("setup", "dedicated catalog", f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}{loc}"))
    for layer in DATA_LAYERS + ("reporting",):
        out.append(Stmt("setup", f"{layer} schema", f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}"))
    return out


def plan_security(cfg: DPOConfig) -> list[Stmt]:
    """Admin-only objects: access map, identity salt, append-only audit log."""
    fmt = cfg.table_format
    sec = cfg.schema("security")
    out = [
        Stmt("security", "admin-only schema", f"CREATE SCHEMA IF NOT EXISTS {sec}"),
        Stmt("security", "row-level access map (admins maintain)",
             f"CREATE TABLE IF NOT EXISTS {cfg.t('security', 'access_map')} "
             f"(principal STRING, cost_center STRING, granted_by STRING, granted_at TIMESTAMP) USING {fmt}"),
        Stmt("security", "secret salt for identity hashing",
             f"CREATE TABLE IF NOT EXISTS {cfg.t('security', 'settings')} "
             f"(setting_key STRING, setting_value STRING) USING {fmt}"),
        Stmt("security", "generate salt once",
             f"INSERT INTO {cfg.t('security', 'settings')} SELECT 'identity_salt', "
             f"concat(uuid(), uuid()) WHERE NOT EXISTS (SELECT 1 FROM {cfg.t('security', 'settings')} "
             f"WHERE setting_key = 'identity_salt')"),
    ]
    props = " TBLPROPERTIES ('delta.appendOnly' = 'true')" if fmt == "delta" else ""
    out.append(Stmt("security", "append-only audit log",
                    f"CREATE TABLE IF NOT EXISTS {cfg.t('ops', 'audit_log')} ({AUDIT_COLUMNS}) USING {fmt}{props}"))
    if fmt == "delta":
        out.append(Stmt("security", "enforce append-only on an existing audit log",
                        f"ALTER TABLE {cfg.t('ops', 'audit_log')} SET TBLPROPERTIES ('delta.appendOnly' = 'true')",
                        requires_table=cfg.t("ops", "audit_log")))
    return out


def plan_grants(cfg: DPOConfig) -> list[Stmt]:
    g, gv = cfg.governance, cfg.governance["groups"]
    admins, ops, ana, view = (_p(gv[r]) for r in ("admins", "operators", "analysts", "viewers"))
    out: list[Stmt] = []
    dedicated = cfg.catalog and cfg.catalog not in SHARED_CATALOGS

    if g["manage_ownership"]:
        if dedicated:
            out.append(Stmt("ownership", "admins own the catalog", f"ALTER CATALOG {cfg.catalog} OWNER TO {admins}"))
        for layer in LAYERS:
            out.append(Stmt("ownership", f"admins own {layer}", f"ALTER SCHEMA {cfg.schema(layer)} OWNER TO {admins}"))
        audit = cfg.t("ops", "audit_log")
        out.append(Stmt("ownership", "operators can append, never alter or purge the audit log",
                        f"ALTER TABLE {audit} OWNER TO {admins}", requires_table=audit))
        for v in REPORTING_VIEWS:
            fq = cfg.t("reporting", v)
            out.append(Stmt("ownership", "admins own reporting views", f"ALTER VIEW {fq} OWNER TO {admins}",
                            requires_table=fq))

    if cfg.catalog:
        for who in (admins, ops, ana, view):
            out.append(Stmt("access", "browse the catalog", f"GRANT USE CATALOG ON CATALOG {cfg.catalog} TO {who}"))

    if cfg.catalog and g.get("operators_can_create_schemas"):
        out.append(Stmt("access", "operators: create test schemas (dev only)",
                        f"GRANT CREATE SCHEMA ON CATALOG {cfg.catalog} TO {ops}"))
    for layer in LAYERS:
        out.append(Stmt("access", "admins: full control", f"GRANT ALL PRIVILEGES ON SCHEMA {cfg.schema(layer)} TO {admins}"))
    for layer in DATA_LAYERS:
        out.append(Stmt("access", "operators: write data layers",
                        f"GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA {cfg.schema(layer)} TO {ops}"))
    # reporting is read-only for operators: they cannot create a view that viewers would inherit
    out.append(Stmt("access", "operators: read reporting (no create)",
                    f"GRANT USE SCHEMA, SELECT ON SCHEMA {cfg.schema('reporting')} TO {ops}"))
    for layer in ("silver", "gold", "reporting"):
        out.append(Stmt("access", "analysts: read curated layers",
                        f"GRANT USE SCHEMA, SELECT ON SCHEMA {cfg.schema(layer)} TO {ana}"))
    out.append(Stmt("access", "analysts: browse ops", f"GRANT USE SCHEMA ON SCHEMA {cfg.schema('ops')} TO {ana}"))
    for t in ANALYST_OPS_TABLES:
        fq = cfg.t("ops", t)
        out.append(Stmt("access", "analysts: pipeline health only",
                        f"GRANT SELECT ON TABLE {fq} TO {ana}", requires_table=fq))
    out.append(Stmt("access", "viewers: reporting views only",
                    f"GRANT USE SCHEMA, SELECT ON SCHEMA {cfg.schema('reporting')} TO {view}"))

    if g["grant_system_access"]:
        out.append(Stmt("system", "operators read system tables", f"GRANT USE CATALOG ON CATALOG system TO {ops}"))
        for sch in SYSTEM_SCHEMAS:
            who = admins if sch == "access" else ops
            out.append(Stmt("system", "read system tables",
                            f"GRANT USE SCHEMA, SELECT ON SCHEMA system.{sch} TO {who}"))

    cls = g["classification"]
    for layer in LAYERS:
        s = cfg.schema(layer)
        extra = ", 'contains' = 'sql_text'" if layer == "bronze" else ""
        out.append(Stmt("tags", "classification tags",
                        f"ALTER SCHEMA {s} SET TAGS ('data_classification' = '{cls}', "
                        f"'managed_by' = 'dpo-accelerator', 'dpo_layer' = '{layer}'{extra})"))
        out.append(Stmt("tags", "schema comment",
                        f"COMMENT ON SCHEMA {s} IS 'DPO {layer} layer. Group access only; see GOVERNANCE.md'"))
    return out


def build_views(spark, cfg: DPOConfig) -> list[Stmt]:
    """Dynamic views: run with the owner's rights, filter rows and mask columns per reader."""
    out = []
    for view, (layer, table, ident, masked) in REPORTING_VIEWS.items():
        src = cfg.t(layer, table)
        try:
            cols = spark.table(src).columns
        except Exception:
            continue

        def col_sql(c):
            if c in ident:
                return f"{identity_expr(cfg, c)} AS `{c}`"
            if c in masked:
                return f"{masked_expr(cfg, c)} AS `{c}`"
            return f"`{c}`"
        sel = ",\n  ".join(col_sql(c) for c in cols)
        where = row_visibility_expr(cfg, "src.cost_center" if "cost_center" in cols else "CAST(NULL AS STRING)")
        out.append(Stmt("views", f"secured view over {table}",
                        f"CREATE OR REPLACE VIEW {cfg.t('reporting', view)} AS\nSELECT\n  {sel}\n"
                        f"FROM {src} AS src\nWHERE {where}"))
    return out


# --------------------------------------------------------------------- apply --
def _exists(spark, fq: str) -> bool:
    try:
        spark.table(fq).limit(0).collect()
        return True
    except Exception:
        return False


def apply(spark, cfg: DPOConfig, stmts: list[Stmt], run_id: str = "", dry_run: bool = False) -> list[tuple]:
    rows = []
    for s in stmts:
        if dry_run:
            rows.append((s.section, s.purpose, s.sql, "PLANNED", ""))
            continue
        if s.requires_table and not _exists(spark, s.requires_table):
            rows.append((s.section, s.purpose, s.sql, "SKIPPED", "object not created yet"))
            continue
        try:
            spark.sql(s.sql)
            rows.append((s.section, s.purpose, s.sql, "OK", ""))
        except Exception as exc:
            rows.append((s.section, s.purpose, s.sql, "ERROR", str(exc).split("\n")[0][:500]))
    _write(spark, cfg, "governance_log", rows,
           "section string, purpose string, statement string, status string, error string", run_id)
    errors = [r for r in rows if r[3] == "ERROR"]
    if errors and cfg.governance["fail_on_error"] and not dry_run:
        raise RuntimeError(f"{len(errors)} governance statements failed; first: {errors[0][2]} -> {errors[0][4]}")
    return rows


def _write(spark, cfg, table, rows, schema, run_id, mode="overwrite"):
    from pyspark.sql import functions as F
    empty = not rows
    if empty:
        rows = [tuple([None] * len(schema.split(",")))]
    df = spark.createDataFrame(rows, schema)
    if empty:
        df = df.limit(0)
    df = df.withColumn("run_id", F.lit(run_id)).withColumn("logged_at", F.current_timestamp())
    fq = cfg.t("ops", table)
    if mode == "append" and _exists(spark, fq):
        df.write.format(cfg.table_format).mode("append").saveAsTable(fq)
    else:
        df.write.format(cfg.table_format).mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq)


# ----------------------------------------------------------------- identity ---
def current_user(spark, cfg: DPOConfig) -> str:
    if cfg.governance.get("simulate_user"):
        return cfg.governance["simulate_user"]
    try:
        return spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    except Exception:
        return getpass.getuser()


def _is_member_of(spark, cfg: DPOConfig, roles: tuple) -> tuple[bool, str]:
    expr = " OR ".join(member(cfg, cfg.group(r)) for r in roles)
    try:
        return bool(spark.sql(f"SELECT ({expr}) AS ok").collect()[0]["ok"]), ""
    except Exception as exc:
        return False, f"membership check failed ({type(exc).__name__})"


def is_authorized(spark, cfg: DPOConfig, roles: tuple = ("admins", "operators")) -> tuple[bool, str]:
    """Defence in depth: Unity Catalog grants are the real boundary; this stops runs early and audits them."""
    g = cfg.governance
    user = current_user(spark, cfg)
    if not g["enforce"]:
        return True, f"{user}: governance not enforced in profile '{cfg.profile}'"
    if roles != ("admins",) and user.lower() in {p.lower() for p in g["authorized_principals"]}:
        return True, f"{user}: listed in authorized_principals"
    ok, err = _is_member_of(spark, cfg, roles)
    groups = " or ".join(cfg.group(r) for r in roles)
    if ok:
        return True, f"{user}: member of {groups}"
    return False, err or (f"{user} is not a member of {groups}. Ask a DPO admin to add you, "
                          f"or run as the DPO service principal.")


def authorize(spark, cfg: DPOConfig, run_id: str = "", roles: tuple = ("admins", "operators")) -> str:
    ok, reason = is_authorized(spark, cfg, roles)
    record_audit(spark, cfg, run_id, "authorize", "ALLOWED" if ok else "DENIED", reason)
    if not ok:
        raise PermissionError(reason)
    return reason


def record_audit(spark, cfg: DPOConfig, run_id: str, action: str, status: str, detail: str) -> None:
    try:
        row = [(current_user(spark, cfg), cfg.profile, cfg.source_mode, action, status, detail[:2000])]
        _write(spark, cfg, "audit_log", row, "principal string, profile string, source_mode string, "
               "action string, status string, detail string", run_id, mode="append")
    except Exception:
        pass  # a denied caller usually cannot write either; system.access.audit still records the attempt


# --------------------------------------------------------------------- drift --
def audit_drift(spark, cfg: DPOConfig, run_id: str = "", revoke: bool = False) -> list[tuple]:
    """Principals holding grants on DPO schemas outside the role model."""
    g = cfg.governance
    expected = {p.lower() for p in list(g["groups"].values()) + list(g["authorized_principals"])}
    me = current_user(spark, cfg).lower()
    rows = []
    for layer in LAYERS:
        s = cfg.schema(layer)
        try:
            grants = spark.sql(f"SHOW GRANTS ON SCHEMA {s}").collect()
        except Exception as exc:
            rows.append((s, None, None, "UNKNOWN", str(exc).split("\n")[0][:300]))
            continue
        for r in grants:
            d = r.asDict()
            principal, action, otype = d.get("Principal"), d.get("ActionType"), d.get("ObjectType")
            if not principal or principal.lower() in expected or otype != "SCHEMA":
                continue
            status = "DRIFT"
            if revoke and principal.lower() != me:
                try:
                    spark.sql(f"REVOKE ALL PRIVILEGES ON SCHEMA {s} FROM {_p(principal)}")
                    status = "REVOKED"
                except Exception as exc:
                    status = f"REVOKE_FAILED: {str(exc).split(chr(10))[0][:200]}"
            rows.append((s, principal, action, status, ""))
    _write(spark, cfg, "governance_drift", rows,
           "schema_name string, principal string, privilege string, status string, error string", run_id)
    return rows


# ----------------------------------------------------------------- admin path --
def plan_table_ownership(spark, cfg: DPOConfig) -> list[Stmt]:
    """Data-layer tables owned by the operators group, so whichever operator identity runs next
    (a person during install, the service principal afterwards) can rewrite them. Operators still
    cannot grant viewers anything useful: viewers have no USE SCHEMA on data layers."""
    out = []
    ops = _p(cfg.group("operators"))
    for layer in DATA_LAYERS:
        try:
            tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {cfg.schema(layer)}").collect()
                      if not getattr(r, "isTemporary", False)]
        except Exception:
            continue
        for tb in sorted(tables):
            if layer == "ops" and tb == "audit_log":
                continue                                   # stays admin-owned and append-only
            out.append(Stmt("ownership", "operators own data-layer tables",
                            f"ALTER TABLE {cfg.t(layer, tb)} OWNER TO {ops}"))
    return out


def admin_plan(spark, cfg: DPOConfig) -> list[Stmt]:
    stmts = plan_setup(cfg) + plan_security(cfg)
    if cfg.governance["apply_grants"] and cfg.catalog:
        grants = plan_grants(cfg)
        views = build_views(spark, cfg)
        own = [s for s in grants if s.section == "ownership"]
        if cfg.governance["manage_ownership"]:
            own += plan_table_ownership(spark, cfg)
        rest = [s for s in grants if s.section != "ownership"]
        return stmts + views + own + rest            # views exist before they are re-owned
    return stmts + build_views(spark, cfg)


def admin_apply(spark, cfg: DPOConfig, run_id: str = "admin", dry_run: bool = False) -> dict:
    """Run by a dpo-admins member (00_setup) or the admin service principal (governance job)."""
    authorize(spark, cfg, run_id, roles=("admins",))
    rows = apply(spark, cfg, admin_plan(spark, cfg), run_id, dry_run)
    drift = []
    if not dry_run and cfg.governance["apply_grants"] and cfg.catalog:
        drift = audit_drift(spark, cfg, run_id, revoke=cfg.governance["revoke_drift"])
    record_audit(spark, cfg, run_id, "admin_apply", "DRY_RUN" if dry_run else "APPLIED",
                 f"{len(rows)} statements, {sum(r[3] == 'ERROR' for r in rows)} errors")
    return {"rows": rows, "drift": drift}


def run_govern(ctx) -> dict:
    """Pipeline stage. Free Edition (not enforced): build views. Enforced: verify only, never change."""
    spark, cfg = ctx.spark, ctx.cfg
    if not cfg.governance["enforce"]:
        rows = apply(spark, cfg, build_views(spark, cfg), ctx.run_id)
        purge_logs(spark, cfg)
        return {"views": sum(r[3] == "OK" for r in rows), "mode": "built"}
    checks = []
    for v in REPORTING_VIEWS:
        fq = cfg.t("reporting", v)
        checks.append(("verify", f"reporting view {v}", fq, "OK" if _exists(spark, fq) else "MISSING",
                       "" if _exists(spark, fq) else "an admin must run 00_setup with apply"))
    _write(spark, cfg, "governance_log", checks,
           "section string, purpose string, statement string, status string, error string", ctx.run_id)
    drift = audit_drift(spark, cfg, ctx.run_id, revoke=False) if cfg.catalog else []
    purge_logs(spark, cfg)
    return {"views_missing": sum(c[3] == "MISSING" for c in checks),
            "drift": sum(1 for d in drift if d[3] == "DRIFT"), "mode": "verified"}


# ----------------------------------------------------------------- preflight --
def preflight(spark, cfg: DPOConfig) -> list[tuple]:
    """(check, status, detail, fix) — run before the first deployment in a workspace."""
    res = []
    user = current_user(spark, cfg)
    res.append(("identity", "INFO", f"running as {user}, profile '{cfg.profile}'", ""))
    if cfg.catalog:
        try:
            spark.sql(f"DESCRIBE CATALOG {cfg.catalog}").collect()
            res.append(("catalog", "OK", f"{cfg.catalog} exists", ""))
        except Exception:
            fix = (f"CREATE CATALOG {cfg.catalog}" +
                   (f" MANAGED LOCATION '{cfg.managed_location}'" if cfg.managed_location else
                    "  -- add MANAGED LOCATION if the metastore has no default storage"))
            res.append(("catalog", "MISSING", f"{cfg.catalog} not found or not visible", fix))
    if cfg.source_mode == "system":
        for name, table in SYSTEM_SOURCES.items():
            if not table:
                continue
            try:
                n = len(spark.sql(f"SELECT 1 FROM {table} LIMIT 1").collect())
                res.append((f"source {table}", "OK" if n else "EMPTY", "readable", ""))
            except Exception as exc:
                sch = table.split(".")[1]
                res.append((f"source {table}", "DENIED", str(exc).split("\n")[0][:200],
                            f"GRANT USE SCHEMA, SELECT ON SCHEMA system.{sch} TO `{cfg.group('operators')}`"))
        if not cfg.workspace_ids:
            res.append(("workspace scope", "WARN", "workspace_ids empty: all workspaces in the account are analysed",
                        "Set workspace_ids in the profile to the workspaces this engagement covers"))
    if cfg.governance["enforce"]:
        try:
            names = {r[0].lower() for r in spark.sql("SHOW GROUPS").collect()}
            for role, grp in cfg.governance["groups"].items():
                ok = grp.lower() in names
                res.append((f"group {role}", "OK" if ok else "MISSING", grp,
                            "" if ok else f"Create account group '{grp}' and assign it to this workspace"))
        except Exception as exc:
            res.append(("groups", "UNKNOWN", str(exc).split("\n")[0][:200], "Verify groups in the account console"))
        for roles, label in ((("admins",), "admin rights"), (("admins", "operators"), "run rights")):
            ok, reason = is_authorized(spark, cfg, roles)
            res.append((label, "OK" if ok else "NO", reason, ""))
        if cfg.governance["retain_raw_sql"]:
            res.append(("raw SQL", "WARN", "retain_raw_sql=true keeps query literals in bronze",
                        "Set retain_raw_sql=false unless the client approved storing raw SQL"))
        if cfg.ai_enabled:
            res.append(("AI", "WARN", f"ai_query enabled on {cfg.ai_endpoint}",
                        "Confirm written approval to send findings to model serving"))
    else:
        res.append(("governance", "WARN", f"not enforced in profile '{cfg.profile}'",
                    "Use an enterprise_* profile for shared or client workspaces"))
    return res


def purge_logs(spark, cfg: DPOConfig) -> None:
    """Retention for operational logs. The audit log is append-only and retained by admins."""
    if cfg.table_format != "delta":
        return
    days = int(cfg.log_retention_days)
    for t, col in (("run_log", "run_ts"), ("test_results", "run_ts"), ("governance_log", "logged_at")):
        fq = cfg.t("ops", t)
        if _exists(spark, fq):
            try:
                spark.sql(f"DELETE FROM {fq} WHERE {col} < current_timestamp() - INTERVAL {days} DAYS")
            except Exception:
                pass
