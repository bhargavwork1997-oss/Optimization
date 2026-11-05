"""Genie spaces (Genie agents) as code: ask the DPO backlog questions in plain language.

Two spaces per profile, generated from the profile like the dashboards, with the same audiences:

  cost_optimization   "DPO · Ask cost optimization". Tables: ONLY the `reporting` views. Genie runs every
                      query as the person asking, so row-level security, identity hashing and inference
                      masking apply exactly as in the dashboard. Audience: viewers, analysts, operators.
  engineering         "DPO · Ask engineering". Tables: gold + ops health. Audience: analysts, operators.

Security choices (see GOVERNANCE.md, "Genie"):
  * Format assistance / entity matching ("prompt matching") samples column values with the AUTHOR's rights
    and stores them in the space's shared context. On dynamic views that would show every reader values
    from rows they cannot query (other cost centres' workloads, job names). So the cost space turns both
    off for every column, and the engineering space turns them off for identity columns.
  * No audit, governance, quarantine or security tables in any space.
  * Instructions, sample questions, example SQL and benchmarks are static text with no client data.

Generated files: genie/<profile>/<key>.geniespace.json (deployed by resources.genie_spaces in the bundle,
or by the installer without the CLI).

    python -m dpo.genie                # regenerate all profiles (tests fail if the files are stale)
"""
from __future__ import annotations

import hashlib
import json
import os
from typing import Optional

from .config import DPOConfig

SPACES = {
    "cost_optimization": "DPO · Ask cost optimization",
    "engineering": "DPO · Ask engineering",
}
DESCRIPTIONS = {
    "cost_optimization": "Ask about Databricks spend and the prioritised optimisation backlog. Answers use "
                         "only the secured reporting views, so you see only the cost centres you are mapped to.",
    "engineering": "Ask about job and query efficiency, cost attribution, rule findings and DPO pipeline health "
                   "(gold and ops tables). For DPO analysts and operators.",
}
ALLOWED_LAYERS = {"cost_optimization": ("reporting",), "engineering": ("gold", "ops")}

# Tables per space and their columns (checked against real pipeline output in tests/test_genie.py).
TABLES = {
    "cost_optimization": {
        ("reporting", "budget_outlook"): ["cost_center", "month_start", "month_end", "as_of_date", "days_remaining", "budget", "actual_mtd", "forecast_remaining", "projected_total", "projected_lower", "projected_upper", "pct_of_budget", "budget_gap", "status", "method", "mtd_complete"],
        ("reporting", "cost_forecast"): ["forecast_date", "cost_center", "cost_forecast", "cost_lower", "cost_upper", "method"],
        ("reporting", "cost_by_dimension"): ["usage_date", "workspace_id", "billing_origin_product", "sku_name",
                                             "cost_center", "run_as", "dbus", "cost"],
        ("reporting", "job_scorecard"): ["job_id", "job_name", "cost_center", "runs", "succeeded", "failed",
                                         "p50_duration_s", "p95_duration_s", "window_cost", "failure_rate",
                                         "cost_per_success", "monthly_cost"],
        ("reporting", "recommendation_narratives"): ["finding_id", "rank", "priority", "workload_key", "rule_id",
                                                     "cost_center", "narrative", "narrative_source"],
        ("reporting", "recommendations"): [
            "workload_key", "rule_id", "metric_name", "metric_value", "threshold", "est_monthly_savings",
            "evidence", "title", "category", "severity", "confidence", "feasibility", "reversibility", "action",
            "cost_center", "finding_id", "workload_type", "period_start", "period_end", "run_id", "detected_at",
            "severity_score", "savings_rank", "V", "C", "F", "R", "score", "priority", "status",
            "validation_required", "rank"],
        ("reporting", "workload_summary"): ["workload_key", "workload_type", "window_cost", "window_dbus",
                                            "untagged_cost", "active_days", "attribution_types", "cost_center",
                                            "monthly_cost", "share_of_total", "cost_rank"],
    },
    "engineering": {
        ("gold", "budget_outlook"): None,
        ("gold", "cost_forecast"): None,
        ("gold", "dpo_cost_daily"): None,
        ("gold", "dpo_cost_summary"): None,
        ("gold", "dpo_run_cost"): None,
        ("gold", "cost_by_dimension"): ["usage_date", "workspace_id", "billing_origin_product", "sku_name",
                                        "cost_center", "run_as", "dbus", "cost"],
        ("gold", "job_scorecard"): None,
        ("gold", "query_fingerprint_stats"): None,
        ("gold", "recommendation_narratives"): None,
        ("gold", "recommendations"): None,
        ("gold", "run_cost"): None,
        ("gold", "workload_cost_daily"): None,
        ("gold", "workload_summary"): None,
        ("ops", "dq_report"): None,
        ("ops", "rule_status"): None,
        ("ops", "run_log"): None,
        ("ops", "source_status"): None,
    },
}
# Columns whose values must never be sampled into a space's shared context.
NO_SAMPLING = {"engineering": {("gold", "cost_by_dimension"): ["run_as"]}}


def _id(key: str, kind: str, n: int) -> str:
    """Stable 32-hex id: regenerating gives identical files, so the bundle sees no spurious change."""
    return hashlib.sha256(f"dpo-genie:{key}:{kind}:{n}".encode()).hexdigest()[:32]


def _lines(text: str) -> list[str]:
    parts = text.strip("\n").split("\n")
    return [p + "\n" for p in parts[:-1]] + [parts[-1]]


def _off(col: str) -> dict:
    # serialized_space version 2 names; the API rejects the v1 names get_example_values/build_value_dictionary
    return {"column_name": col, "enable_format_assistance": False, "enable_entity_matching": False}


# ============================================================== content ==
def _instructions(cfg: DPOConfig, key: str) -> str:
    cur = cfg.currency
    common = f"""
You answer questions about Databricks cost and the DPO optimisation backlog. All money is in {cur}.
Data covers a rolling {cfg.lookback_days}-day window; monthly_cost is the window cost scaled to 30 days.
Scoring: score = 0.35*V + 0.25*C + 0.20*F + 0.20*R (V value, C confidence, F feasibility, R reversibility,
each 0-1). Priority: P1 if score >= 0.75, P2 if >= 0.60, else P3. Order backlogs by score DESC, finding_id.
est_monthly_savings is a hypothesis until a benchmark validates it: always call it "estimated" and never
add it up as guaranteed savings. status NEEDS_REVIEW means nobody has validated the finding yet.
workload_key is '<type>:<id>', e.g. 'job:123', 'cluster:abc', 'query:<fingerprint>', 'warehouse:<id>'.
A NULL cost_center is untagged spend: show it as '(untagged)'.
Rule ids R01-R17 (rule_id like 'R01_COMPUTE_IDLE'): R01 idle all-purpose cluster, R02 oversized job
cluster, R03 auto-termination, R04 old runtime, R05 job on all-purpose compute, R06 job failure rate,
R07 task skew, R08 runtime regression, R09 daily cost spike, R10 untagged spend, R11 no file pruning,
R12 spill, R13 repeated query without cache, R14 warehouse queuing, R15 small files, R16 SQL anti-pattern,
R17 plain file copy that could move to native cloud copy tools.
Budget outlook (budget_outlook, one row per cost centre for the current month): actual_mtd is spend so
far this month, forecast_remaining the forecast for the rest of the month, projected_total = actual_mtd +
forecast_remaining, projected_lower/projected_upper the prediction interval. status: OVER_BUDGET (already
above budget), AT_RISK (projection above budget), WATCH (only the upper bound is above), ON_TRACK, NO_BUDGET
(no budget set). method says how the forecast was made: ai_forecast (Databricks AI forecasting) or baseline
(weekday average). cost_forecast holds the daily forecast with lower and upper bounds. Forecasts are
estimates: always give the range, not just the point value.
Never write INSERT, UPDATE, DELETE, MERGE, CREATE, DROP, GRANT or any other statement that changes data
or access. Answer with SELECT queries only."""
    if key == "cost_optimization":
        return common + """
Tables: recommendations = scored backlog (one row per finding); recommendation_narratives = plain-language
explanation per finding (join on finding_id); workload_summary = cost per workload; job_scorecard = job
reliability and cost; cost_by_dimension = daily cost by product, SKU, workspace, cost centre and run_as.
These are secured views: each person sees only the cost centres they are mapped to. For some people
share_of_total, cost_rank, rank, savings_rank and evidence are NULL by design and run_as is a hashed id.
When asked for a share or a rank, compute it over the visible rows (e.g. with a window function) and say
it is "of the spend you can see". Never try to work out account-wide totals, other cost centres or real
identities, and do not explain how the masking works beyond saying some values are restricted."""
    return common + """
Tables: gold.recommendations = scored backlog; gold.recommendation_narratives = explanation per finding;
gold.workload_summary and gold.workload_cost_daily = cost per workload (attribution direct, allocated or
idle); gold.job_scorecard = job runs, failures, p50/p95 duration, cost per success; gold.run_cost = cost per
job run; gold.query_fingerprint_stats = SQL statement shapes (fingerprint, executions, read bytes, pruning,
spill, cache hit rate); gold.cost_by_dimension = daily cost by product, SKU, workspace, cost centre, run_as.
Pipeline health (ops, one row per stage/source/rule per run_id): run_log, source_status, dq_report,
rule_status. For "latest run" filter run_id to the run with the newest run_ts in ops.run_log.
Join recommendations to job_scorecard with workload_key = concat('job:', job_id), and to
query_fingerprint_stats with workload_key = concat('query:', fingerprint).
DPO's own cost: gold.dpo_cost_summary (one row: dpo_monthly_cost, split jobs/sql/tagged, share_of_total_spend,
est_monthly_savings, savings_to_cost_ratio), gold.dpo_cost_daily (by day, component, item), gold.dpo_run_cost (per
DPO job run). Billing lags hours, so the latest run may show no cost yet.
Prefer aggregates; when listing people (run_as) only do so if the question asks for it."""


def _sample_questions(key: str) -> list[str]:
    if key == "cost_optimization":
        return ["Which cost centres are at risk of going over budget this month?",
                "What are my top 10 P1 recommendations by score?",
                "How much did we spend per cost centre in the last 30 days?",
                "Which workloads cost the most per month?",
                "What is the estimated monthly saving by category?",
                "Which jobs fail most often and what do they cost?"]
    return ["Which query shapes scan the most data without pruning?",
            "Which jobs have the highest cost per successful run?",
            "Did the latest DPO pipeline run succeed?",
            "Which sources were missing in the latest run?",
            "How much spend is idle or allocated rather than direct?",
            "Which rules produced the most findings?",
            "What does DPO itself cost and does it pay for itself?"]


def _examples(cfg: DPOConfig, key: str) -> list[tuple[str, str]]:
    """(question, SQL) pairs; also used as benchmarks. Each SQL is executed in tests/test_genie.py."""
    if key == "cost_optimization":
        r = lambda n: cfg.t("reporting", n)
        return [
            ("What are the top 10 recommendations by score?", f"""
SELECT rec.priority, round(rec.score, 3) AS score, rec.rule_id, rec.title, rec.workload_key,
       coalesce(rec.cost_center, '(untagged)') AS cost_center,
       round(rec.est_monthly_savings, 2) AS estimated_monthly_savings, rec.action, n.narrative
FROM {r('recommendations')} rec
LEFT JOIN {r('recommendation_narratives')} n ON n.finding_id = rec.finding_id
ORDER BY rec.score DESC, rec.finding_id
LIMIT 10"""),
            ("What did each cost centre spend in the window?", f"""
SELECT coalesce(cost_center, '(untagged)') AS cost_center, round(sum(cost), 2) AS cost,
       round(sum(dbus), 1) AS dbus
FROM {r('cost_by_dimension')}
GROUP BY 1
ORDER BY cost DESC"""),
            ("What is the estimated monthly saving per category and priority?", f"""
SELECT category, priority, count(*) AS findings,
       round(sum(coalesce(est_monthly_savings, 0)), 2) AS estimated_monthly_savings
FROM {r('recommendations')}
GROUP BY category, priority
ORDER BY estimated_monthly_savings DESC"""),
            ("Which workloads cost the most per month, with their share of visible spend?", f"""
SELECT workload_key, workload_type, coalesce(cost_center, '(untagged)') AS cost_center,
       round(monthly_cost, 2) AS monthly_cost,
       round(monthly_cost / nullif(sum(monthly_cost) OVER (), 0) * 100, 1) AS share_of_visible_pct
FROM {r('workload_summary')}
ORDER BY monthly_cost DESC
LIMIT 10"""),
            ("Which jobs fail most often?", f"""
SELECT job_name, runs, failed, round(failure_rate * 100, 1) AS failure_pct,
       round(monthly_cost, 2) AS monthly_cost, round(cost_per_success, 2) AS cost_per_success
FROM {r('job_scorecard')}
WHERE runs > 0
ORDER BY failure_rate DESC, monthly_cost DESC
LIMIT 10"""),
            ("Which cost centres are at risk of going over budget this month?", f"""
SELECT cost_center, status, round(budget, 2) AS budget, round(actual_mtd, 2) AS spent_so_far,
       round(projected_total, 2) AS projected_month_total,
       round(projected_lower, 2) AS projected_low, round(projected_upper, 2) AS projected_high,
       round(pct_of_budget * 100, 1) AS pct_of_budget, days_remaining, method
FROM {r('budget_outlook')}
ORDER BY CASE status WHEN 'OVER_BUDGET' THEN 1 WHEN 'AT_RISK' THEN 2 WHEN 'WATCH' THEN 3
                     WHEN 'ON_TRACK' THEN 4 ELSE 5 END, pct_of_budget DESC"""),
            ("Show daily cost by product", f"""
SELECT usage_date, billing_origin_product AS product, round(sum(cost), 2) AS cost
FROM {r('cost_by_dimension')}
GROUP BY usage_date, billing_origin_product
ORDER BY usage_date, product"""),
        ]
    g = lambda n: cfg.t("gold", n)
    o = lambda n: cfg.t("ops", n)
    return [
        ("Which query shapes read the most data per run without pruning?", f"""
SELECT q.fingerprint, q.statement_shape, q.executions,
       round(q.avg_read_bytes / power(1024, 3), 2) AS avg_read_gb,
       round(q.avg_pruned_files / nullif(q.avg_read_files + q.avg_pruned_files, 0) * 100, 1) AS pruned_pct,
       round(q.monthly_cost, 2) AS monthly_cost
FROM {g('query_fingerprint_stats')} q
ORDER BY q.avg_read_bytes DESC
LIMIT 10"""),
        ("Which jobs have the highest cost per successful run?", f"""
SELECT job_name, runs, succeeded, round(cost_per_success, 2) AS cost_per_success,
       round(monthly_cost, 2) AS monthly_cost, p95_duration_s
FROM {g('job_scorecard')}
WHERE succeeded > 0
ORDER BY cost_per_success DESC
LIMIT 10"""),
        ("Did the latest DPO pipeline run succeed?", f"""
SELECT stage, status, round(seconds, 1) AS seconds, rows_out, message, run_ts
FROM {o('run_log')}
WHERE run_id = (SELECT max_by(run_id, run_ts) FROM {o('run_log')})
ORDER BY run_ts, stage"""),
        ("Which sources were not available in the latest run?", f"""
SELECT source AS source_name, table_name, status, missing_columns, message
FROM {o('source_status')}
WHERE run_id = (SELECT max_by(run_id, run_ts) FROM {o('source_status')}) AND status <> 'available'"""),
        ("How is spend split by attribution type?", f"""
SELECT attribution, round(sum(cost), 2) AS cost,
       round(sum(cost) / nullif(sum(sum(cost)) OVER (), 0) * 100, 1) AS share_pct
FROM {g('workload_cost_daily')}
GROUP BY attribution
ORDER BY cost DESC"""),
        ("What does DPO itself cost and does it pay for itself?", f"""
SELECT dpo_monthly_cost, jobs_monthly, sql_monthly, tagged_monthly,
       round(share_of_total_spend * 100, 3) AS share_of_spend_pct,
       est_monthly_savings, savings_to_cost_ratio
FROM {g('dpo_cost_summary')}"""),
        ("Which rules produced the most findings and what could they save?", f"""
SELECT rule_id, count(*) AS findings, count_if(priority = 'P1') AS p1_findings,
       round(sum(coalesce(est_monthly_savings, 0)), 2) AS estimated_monthly_savings
FROM {g('recommendations')}
GROUP BY rule_id
ORDER BY findings DESC, rule_id"""),
    ]


def _measures(cfg: DPOConfig, key: str) -> list[dict]:
    t = "recommendations"
    ms = [("estimated_monthly_savings", "Estimated monthly savings", f"SUM(COALESCE({t}.est_monthly_savings, 0))"),
          ("p1_findings", "P1 findings", f"COUNT_IF({t}.priority = 'P1')"),
          ("total_cost", "Total cost", "SUM(cost_by_dimension.cost)")]
    if key == "engineering":
        ms.append(("job_failure_rate", "Job failure rate", "SUM(job_scorecard.failed) / NULLIF(SUM(job_scorecard.runs), 0)"))
    return [{"id": _id(key, "measure", i), "alias": a, "display_name": d, "sql": [s]}
            for i, (a, d, s) in enumerate(ms)]


# ================================================================ build ==
def build(cfg: DPOConfig, key: str) -> dict:
    """Build the definition for one profile and key (deterministic: same input, identical output)."""
    tables = []
    for (layer, name), cols in TABLES[key].items():
        entry = {"identifier": cfg.t(layer, name)}
        if key == "cost_optimization":      # dynamic views: sample nothing (see module docstring)
            off = cols
        else:
            off = NO_SAMPLING.get(key, {}).get((layer, name), [])
        if off:
            entry["column_configs"] = sorted((_off(c) for c in off), key=lambda c: c["column_name"])
        tables.append(entry)
    examples = _examples(cfg, key)
    space = {
        "version": 2,
        "config": {"sample_questions": sorted(
            ({"id": _id(key, "sample", i), "question": [q]} for i, q in enumerate(_sample_questions(key))),
            key=lambda x: x["id"])},
        "data_sources": {"tables": sorted(tables, key=lambda x: x["identifier"])},
        "instructions": {
            "text_instructions": [{"id": _id(key, "text", 0), "content": _lines(_instructions(cfg, key))}],
            "example_question_sqls": sorted(
                ({"id": _id(key, "example", i), "question": [q], "sql": _lines(s)}
                 for i, (q, s) in enumerate(examples)), key=lambda x: x["id"]),
            "sql_snippets": {"measures": sorted(_measures(cfg, key), key=lambda x: x["id"])},
        },
        "benchmarks": {"questions": sorted(
            ({"id": _id(key, "benchmark", i), "question": [q], "answer": [{"format": "SQL", "content": _lines(s)}]}
             for i, (q, s) in enumerate(examples)), key=lambda x: x["id"])},
    }
    return space


def render(cfg: DPOConfig, key: str) -> str:
    """Serialised definition, as written to the generated file."""
    return json.dumps(build(cfg, key), indent=2, ensure_ascii=False) + "\n"


def file_path(root: str, profile: str, key: str) -> str:
    """Path of the generated file for a profile and key."""
    return os.path.join(root, "genie", profile, f"{key}.geniespace.json")


def write_all(root: str, profiles: Optional[list[str]] = None) -> list[str]:
    """Regenerate the files for every profile (or the given ones) and return their paths."""
    env_dir = os.path.join(root, "config", "environments")
    profiles = profiles or sorted(f[:-5] for f in os.listdir(env_dir) if f.endswith(".json"))
    out = []
    for p in profiles:
        cfg = DPOConfig.from_profile(p)
        cfg.rules_path = os.path.join(root, "config", "rules.json")
        for key in SPACES:
            path = file_path(root, p, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(render(cfg, key))
            out.append(path)
    return out


# ================================================= install without the CLI ==
def genie_acl(cfg: DPOConfig, key: str) -> list[dict]:
    """CAN_RUN = ask questions. CAN_EDIT/CAN_MANAGE stay with admins: managers can read every user's
    conversations in the space's monitoring view, and editors can change instructions."""
    if not cfg.governance["enforce"]:
        return []
    acl = [{"group_name": cfg.group("admins"), "permission_level": "CAN_MANAGE"},
           {"group_name": cfg.group("analysts"), "permission_level": "CAN_RUN"},
           {"group_name": cfg.group("operators"), "permission_level": "CAN_RUN"}]
    if key == "cost_optimization":
        acl.append({"group_name": cfg.group("viewers"), "permission_level": "CAN_RUN"})
    return acl


def _list_spaces(sdk) -> list:
    out, token = [], None
    while True:
        resp = sdk.genie.list_spaces(page_token=token) if token else sdk.genie.list_spaces()
        out += list(resp.spaces or [])
        token = getattr(resp, "next_page_token", None)
        if not token:
            return out


def publish_genie_spaces(cfg: DPOConfig, sdk, root: str, parent_path: str, warehouse_name: str = "") -> list[dict]:
    """Create or update both Genie spaces in `parent_path` and set group permissions. Leaves them alone when
    the asset bundle manages DPO for this profile."""
    from .dashboard import find_warehouse
    from .installer import bundle_managed_jobs
    if bundle_managed_jobs(sdk, cfg.profile):
        return [{"genie_space": s, "action": "managed by asset bundle"} for s in SPACES.values()]
    from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
    wh = find_warehouse(sdk, warehouse_name)
    sdk.workspace.mkdirs(parent_path)
    # Only spaces in our folder: a same-named space elsewhere belongs to someone else. The API may return
    # parent_path without the "/Workspace" prefix, or omit it in list responses.
    norm = lambda p: (p or "").removeprefix("/Workspace").rstrip("/")
    existing = {s.title: s for s in _list_spaces(sdk)
                if s.parent_path is None or norm(s.parent_path) == norm(parent_path)}
    out = []
    for key, title in SPACES.items():
        name = f"{title} ({cfg.profile})"
        with open(file_path(root, cfg.profile, key), encoding="utf-8") as fh:
            body = fh.read()
        if name in existing:
            s = sdk.genie.update_space(existing[name].space_id, title=name, description=DESCRIPTIONS[key],
                                       warehouse_id=wh, serialized_space=body)
            action = "updated"
        else:
            s = sdk.genie.create_space(warehouse_id=wh, serialized_space=body, title=name,
                                       description=DESCRIPTIONS[key], parent_path=parent_path)
            action = "created"
        acl = genie_acl(cfg, key)
        if acl:
            sdk.permissions.set("genie", s.space_id, access_control_list=[
                AccessControlRequest(group_name=a["group_name"],
                                     permission_level=PermissionLevel(a["permission_level"])) for a in acl])
        out.append({"genie_space": name, "action": action, "id": s.space_id})
    return out


if __name__ == "__main__":       # python -m dpo.genie
    here = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    for p in write_all(here):
        print(os.path.relpath(p, here))
