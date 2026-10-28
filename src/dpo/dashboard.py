"""AI/BI (Lakeview) dashboards as code.

Two dashboards per profile, generated from the profile so table names are always right
(Free Edition `workspace.dpo_gold.x`, enterprise prod `dpo.gold.x`):

  cost_optimization   pages Overview + Recommendations. Reads ONLY the `reporting` views, so row-level
                      security, identity hashing and inference masking apply per viewer.
                      Audience: dpo-viewers, dpo-analysts, dpo-operators.
  engineering         pages Jobs & queries + Pipeline health. Reads gold/ops directly.
                      Audience: dpo-analysts, dpo-operators (never viewers).

Generated files live in dashboards/<profile>/ and are deployed by the asset bundle (resources.dashboards)
or, without the CLI, by the installer (`publish_dashboards`). Both publish WITHOUT embedded credentials,
so every reader queries with their own rights.

    python -m dpo.dashboard            # regenerate all profiles (CI fails if the files are stale)
"""
from __future__ import annotations

import json
import os
import re
from typing import Optional

from .config import DPOConfig

DASHBOARDS = {
    "cost_optimization": "DPO · Cost optimization",
    "engineering": "DPO · Engineering",
}
# Column names the dashboard engine reserves: when it materializes a dataset as a metric view, a column
# called `source` is read as `source.*` and fails with INVALID_EXTRACT_BASE_FIELD_TYPE.
RESERVED_COLUMNS = ("source",)
# layers each dashboard may read; enforced by tests/test_dashboard.py
ALLOWED_LAYERS = {"cost_optimization": ("reporting",), "engineering": ("gold", "ops")}

BLUE, ORANGE, INK = "#1F4E99", "#C2551A", "#15191E"
PALETTE = [BLUE, "#E0A030", "#2E8B57", ORANGE, "#7FA7D9", "#8A3A0B", "#99C9A6", "#F2B8A0", "#919191", "#5B6B7A"]
CURRENCY_SYMBOL = {"USD": "$", "EUR": "€", "GBP": "£", "INR": "₹", "AUD": "A$", "CAD": "C$"}

_COLUMN_DEFAULTS = {
    "allowHTML": False, "allowSearch": False, "booleanValues": ["false", "true"], "highlightLinks": False,
    "imageHeight": "", "imageTitleTemplate": "{{ @ }}", "imageUrlTemplate": "{{ @ }}", "imageWidth": "",
    "linkOpenInNewTab": True, "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}",
    "linkUrlTemplate": "{{ @ }}", "preserveWhitespace": False, "useMonospaceFont": False, "visible": True,
}


# =============================================================== datasets ==
def datasets(cfg: DPOConfig) -> dict[str, dict[str, tuple[str, str]]]:
    """{dashboard: {dataset_name: (display_name, sql)}}"""
    r = lambda n: cfg.t("reporting", n)
    g = lambda n: cfg.t("gold", n)
    o = lambda n: cfg.t("ops", n)
    latest = lambda n: f"(SELECT max_by(run_id, run_ts) FROM {o(n)})"
    cc = "coalesce(cost_center, '(untagged)')"

    cost = {
        "ds_cost_daily": ("Daily cost", f"""
SELECT usage_date, billing_origin_product AS product, {cc} AS cost_center, workspace_id, sku_name,
       cost, dbus
FROM {r('cost_by_dimension')}"""),
        "ds_workloads": ("Workloads", f"""
SELECT w.workload_key, w.workload_type, {cc} AS cost_center,
       round(w.monthly_cost, 2) AS monthly_cost, round(w.window_cost, 2) AS window_cost,
       round(w.untagged_cost, 2) AS untagged_cost, w.active_days,
       array_join(w.attribution_types, ', ') AS attribution,
       -- share of what THIS reader can see (account-wide share is masked for viewers)
       round(w.monthly_cost / nullif(sum(w.monthly_cost) OVER (), 0) * 100, 1) AS share_of_visible_pct,
       coalesce(f.findings, 0) AS findings, coalesce(f.rule_ids, '') AS rule_ids
FROM {r('workload_summary')} w
LEFT JOIN (SELECT workload_key, count(*) AS findings,
                  array_join(array_sort(collect_set(rule_id)), ', ') AS rule_ids
           FROM {r('recommendations')} GROUP BY workload_key) f USING (workload_key)"""),
        "ds_backlog": ("Backlog", f"""
SELECT row_number() OVER (ORDER BY rec.score DESC, rec.finding_id) AS backlog_rank,
       rec.priority, round(rec.score, 3) AS score, rec.rule_id, rec.category, rec.title,
       rec.workload_key, rec.workload_type, coalesce(rec.cost_center, '(untagged)') AS cost_center,
       rec.metric_name, round(rec.metric_value, 3) AS metric_value, rec.threshold,
       round(coalesce(rec.est_monthly_savings, 0), 2) AS est_monthly_savings, rec.status,
       round(rec.V, 2) AS value_v, round(rec.C, 2) AS confidence_c,
       round(rec.F, 2) AS feasibility_f, round(rec.R, 2) AS reversibility_r,
       CASE WHEN rec.priority = 'P1' THEN 1 ELSE 0 END AS is_p1,
       rec.action, n.narrative, n.narrative_source, rec.period_end
FROM {r('recommendations')} rec
LEFT JOIN {r('recommendation_narratives')} n USING (finding_id)"""),
        "ds_score_parts": ("Score breakdown", f"""
SELECT concat(lpad(CAST(backlog_rank AS STRING), 2, '0'), ' · ', rule_id) AS finding,
       priority, category, cost_center, component, weighted
FROM (SELECT row_number() OVER (ORDER BY score DESC, finding_id) AS backlog_rank, priority, category,
             rule_id, coalesce(cost_center, '(untagged)') AS cost_center,
             0.35 * V AS v, 0.25 * C AS c, 0.20 * F AS f, 0.20 * R AS r
      FROM {r('recommendations')})
LATERAL VIEW stack(4, 'Value 0.35·V', v, 'Confidence 0.25·C', c,
                      'Feasibility 0.20·F', f, 'Reversibility 0.20·R', r) s AS component, weighted
WHERE backlog_rank <= 15"""),
    }

    eng = {
        "ds_jobs": ("Job scorecard", f"""
SELECT j.job_name, j.job_id, {cc} AS cost_center, j.runs, j.succeeded, j.failed,
       round(j.failure_rate * 100, 1) AS failure_pct, j.p50_duration_s, j.p95_duration_s,
       round(j.monthly_cost, 2) AS monthly_cost, round(j.cost_per_success, 2) AS cost_per_success,
       coalesce(f.rule_ids, '') AS rule_ids
FROM {g('job_scorecard')} j
LEFT JOIN (SELECT substring(workload_key, 5) AS job_id,
                  array_join(array_sort(collect_set(rule_id)), ', ') AS rule_ids
           FROM {g('recommendations')} WHERE workload_key LIKE 'job:%' GROUP BY 1) f USING (job_id)"""),
        "ds_queries": ("Query shapes", f"""
SELECT q.fingerprint, q.statement_shape, q.executions, q.active_days,
       round(q.monthly_cost, 2) AS monthly_cost, round(q.avg_duration_ms / 1000, 1) AS avg_s,
       round(q.avg_read_bytes / power(1024, 3), 2) AS avg_read_gb,
       round(q.cache_hit_rate * 100, 1) AS cache_hit_pct, round(q.spill_share * 100, 1) AS spill_pct,
       coalesce(f.rule_ids, '') AS rule_ids
FROM {g('query_fingerprint_stats')} q
LEFT JOIN (SELECT substring(workload_key, 7) AS fingerprint,
                  array_join(array_sort(collect_set(rule_id)), ', ') AS rule_ids
           FROM {g('recommendations')} WHERE workload_key LIKE 'query:%' GROUP BY 1) f USING (fingerprint)"""),
        "ds_last_run": ("Latest run stages", f"""
SELECT stage, status, round(seconds, 1) AS seconds, rows_out, message, run_id, run_ts,
       CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END AS is_failed
FROM {o('run_log')} WHERE run_id = {latest('run_log')}"""),
        "ds_runs": ("Recent runs", f"""
SELECT run_id, min(run_ts) AS started,
       CASE WHEN count_if(status = 'FAILED') > 0 THEN 'FAILED'
            ELSE 'SUCCESS' END AS status,
       count(*) AS stages, count_if(status = 'FAILED') AS failed_stages, round(sum(seconds), 1) AS seconds
FROM {o('run_log')} GROUP BY run_id"""),
        "ds_sources": ("Source availability", f"""
SELECT source AS source_name, table_name, status, rows, missing_columns, message,
       CASE WHEN status = 'available' THEN 0 ELSE 1 END AS not_available
FROM {o('source_status')} WHERE run_id = {latest('source_status')}"""),
        "ds_dq": ("Data quality", f"""
SELECT source AS source_name, rows_in, rows_out, rows_quarantined, duplicates_removed, reasons
FROM {o('dq_report')} WHERE run_id = {latest('dq_report')}"""),
        "ds_rules": ("Rule status", f"""
SELECT rule_id, status, findings, round(seconds, 2) AS seconds, message
FROM {o('rule_status')} WHERE run_id = {latest('run_log')}"""),
    }
    return {"cost_optimization": cost, "engineering": eng}


# ================================================================ widgets ==
class _Builder:
    def __init__(self, cfg: DPOConfig, key: str):
        self.cfg, self.key, self.names = cfg, key, set()
        self.money = CURRENCY_SYMBOL.get(cfg.currency, "") + "0,0.00"

    def _name(self, base: str) -> str:
        n = re.sub(r"[^a-z0-9_]+", "_", base.lower()).strip("_")[:60]
        assert n not in self.names, f"duplicate widget name {n}"
        self.names.add(n)
        return n

    @staticmethod
    def _pos(x, y, w, h):
        return {"x": x, "y": y, "width": w, "height": h}

    def text(self, name, lines, x, y, w, h):
        return {"widget": {"name": self._name(name), "multilineTextboxSpec": {"lines": lines}},
                "position": self._pos(x, y, w, h)}

    def counter(self, name, ds, expr, title, x, y, w=1, h=2, desc=None):
        n = self._name(name)
        frame = {"showTitle": True, "title": title}
        if desc:
            frame.update(showDescription=True, description=desc)
        return {"widget": {
            "name": n,
            "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": False,
                                                         "fields": [{"name": "value", "expression": expr}]}}],
            "spec": {"version": 2, "widgetType": "counter", "frame": frame,
                     "encodings": {"value": {"fieldName": "value", "displayName": title,
                                             "format": {"type": "number-plain", "abbreviation": "compact",
                                                        "decimalPlaces": {"type": "max", "places": 1}}}}}},
            "position": self._pos(x, y, w, h)}

    def bar(self, name, ds, x_field, y_expr, title, x, y, w, h, color=None, x_scale="categorical",
            x_title=None, y_title=None, desc=None):
        fields = [{"name": x_field, "expression": f"`{x_field}`"}, {"name": "y", "expression": y_expr}]
        enc = {"x": {"fieldName": x_field, "displayName": x_title or x_field, "scale": {"type": x_scale},
                     "axis": {"title": x_title or x_field}},
               "y": {"fieldName": "y", "displayName": y_title or "value", "scale": {"type": "quantitative"},
                     "axis": {"title": y_title or "value"}},
               "label": {"show": False}}
        if color:
            fields.append({"name": color, "expression": f"`{color}`"})
            enc["color"] = {"fieldName": color, "displayName": color, "scale": {"type": "categorical"}}
        frame = {"showTitle": True, "title": title}
        if desc:
            frame.update(showDescription=True, description=desc)
        return {"widget": {
            "name": self._name(name),
            "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": False, "fields": fields}}],
            "spec": {"version": 3, "widgetType": "bar", "encodings": enc, "frame": frame,
                     "mark": {"colors": PALETTE}}},
            "position": self._pos(x, y, w, h)}

    def scatter(self, name, ds, xf, yf, color, title, x, y, w, h, x_title, y_title, desc=None):
        frame = {"showTitle": True, "title": title}
        if desc:
            frame.update(showDescription=True, description=desc)
        return {"widget": {
            "name": self._name(name),
            "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": True, "fields": [
                {"name": f, "expression": f"`{f}`"} for f in (xf, yf, color)]}}],
            "spec": {"version": 3, "widgetType": "scatter", "frame": frame, "mark": {"colors": PALETTE},
                     "encodings": {
                         "x": {"fieldName": xf, "displayName": x_title, "scale": {"type": "quantitative"},
                               "axis": {"title": x_title}},
                         "y": {"fieldName": yf, "displayName": y_title, "scale": {"type": "quantitative"},
                               "axis": {"title": y_title}},
                         "color": {"fieldName": color, "displayName": color, "scale": {"type": "categorical"}}}}},
            "position": self._pos(x, y, w, h)}

    def table(self, name, ds, columns, title, x, y, w, h, desc=None):
        """columns: [(field, title, kind[, cellFormat])], kind in string|int|float|money|datetime|pct."""
        cols = []
        for i, c in enumerate(columns):
            field, ctitle, kind = c[:3]
            col = dict(_COLUMN_DEFAULTS, fieldName=field, displayName=ctitle, title=ctitle, order=100000 + i)
            if kind == "string":
                col.update(type="string", displayAs="string", alignContent="left")
            elif kind == "datetime":
                col.update(type="datetime", displayAs="datetime", alignContent="right")
            else:
                col.update(type="integer" if kind == "int" else "float", displayAs="number", alignContent="right",
                           numberFormat={"int": "0,0", "money": self.money, "pct": "0.0", "float": "0.00"}[kind])
            if len(c) > 3:
                col["cellFormat"] = c[3]
            cols.append(col)
        frame = {"showTitle": True, "title": title}
        if desc:
            frame.update(showDescription=True, description=desc)
        return {"widget": {
            "name": self._name(name),
            "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": True,
                                                         "fields": [{"name": c[0], "expression": f"`{c[0]}`"}
                                                                    for c in columns]}}],
            "spec": {"version": 1, "widgetType": "table", "frame": frame, "encodings": {"columns": cols},
                     "invisibleColumns": [], "allowHTMLByDefault": False, "itemsPerPage": 25,
                     "paginationSize": "default", "condensed": True, "withRowNumber": False}},
            "position": self._pos(x, y, w, h)}

    def filter(self, name, kind, field, title, targets, x, y, w=2, h=1):
        """kind: multi-select | single-select | date-range-picker; targets: datasets carrying `field`."""
        n = self._name(name)
        queries, enc = [], []
        for ds in targets:
            qn = f"{n}_{ds}"
            queries.append({"name": qn, "query": {"datasetName": ds, "disaggregated": False, "fields": [
                {"name": field, "expression": f"`{field}`"},
                {"name": f"{field}_associativity", "expression": "COUNT_IF(`associative_filter_predicate_group`)"}]}})
            enc.append({"fieldName": field, "displayName": title, "queryName": qn})
        return {"widget": {"name": n, "queries": queries,
                           "spec": {"version": 2, "widgetType": f"filter-{kind}", "encodings": {"fields": enc},
                                    "frame": {"showTitle": True, "title": title}}},
                "position": self._pos(x, y, w, h)}


def _over(column: str, threshold: float, color: str = ORANGE) -> dict:
    return {"default": {"foregroundColor": INK},
            "rules": [{"if": {"column": column, "fn": ">", "literal": f"{threshold:g}"},
                       "value": {"foregroundColor": color}}]}


# ================================================================== pages ==
def _cost_pages(cfg: DPOConfig) -> list[dict]:
    b = _Builder(cfg, "cost_optimization")
    cur = cfg.currency
    src = "system tables" if cfg.source_mode == "system" else "synthetic telemetry"
    cc_targets = ("ds_cost_daily", "ds_workloads", "ds_backlog")

    overview = [
        b.text("ov_header", [
            "## DPO · Cost overview",
            f"Profile **{cfg.profile}** · source **{src}** · lookback **{cfg.lookback_days} days** · "
            f"amounts in **{cur}**. Rows are limited to the cost centres you are allowed to see. "
            "Estimated savings are hypotheses until a benchmark validates them."], 0, 0, 6, 2),
        b.filter("ov_f_date", "date-range-picker", "usage_date", "Usage date (daily charts)", ("ds_cost_daily",),
                 0, 2, 2, 1),
        b.filter("ov_f_cc", "multi-select", "cost_center", "Cost center", cc_targets, 2, 2, 2, 1),
        b.filter("ov_f_product", "multi-select", "product", "Product", ("ds_cost_daily",), 4, 2, 2, 1),
        b.counter("ov_k_window", "ds_cost_daily", "SUM(`cost`)", f"Window cost ({cur})", 0, 3),
        b.counter("ov_k_runrate", "ds_workloads", "SUM(`monthly_cost`)", f"30-day run rate ({cur})", 1, 3),
        b.counter("ov_k_savings", "ds_backlog", "SUM(`est_monthly_savings`)", f"Est. savings / month ({cur})",
                  2, 3, desc="Unvalidated estimate"),
        b.counter("ov_k_p1", "ds_backlog", "SUM(`is_p1`)", "P1 findings", 3, 3),
        b.counter("ov_k_open", "ds_backlog", "COUNT(`backlog_rank`)", "Open findings", 4, 3),
        b.counter("ov_k_untagged", "ds_workloads", "SUM(`untagged_cost`)", f"Untagged spend ({cur})", 5, 3,
                  desc="Target: under 5% of window cost (R10)"),
        b.bar("ov_daily_product", "ds_cost_daily", "usage_date", "SUM(`cost`)", "Daily cost by product",
              0, 5, 4, 6, color="product", x_scale="temporal", x_title="Usage date", y_title=f"Cost ({cur})",
              desc="Spikes are flagged by R09 (cost spike) in the backlog"),
        b.bar("ov_cost_center", "ds_cost_daily", "cost_center", "SUM(`cost`)", "Cost by cost center",
              4, 5, 2, 6, x_title="Cost center", y_title=f"Cost ({cur})"),
        b.bar("ov_findings_cat", "ds_backlog", "category", "COUNT(`backlog_rank`)", "Findings by category",
              0, 11, 3, 6, color="priority", x_title="Category", y_title="Findings"),
        b.bar("ov_savings_cat", "ds_backlog", "category", "SUM(`est_monthly_savings`)",
              "Est. monthly savings by category", 3, 11, 3, 6, color="priority", x_title="Category",
              y_title=f"Savings / month ({cur})"),
        b.table("ov_top_workloads", "ds_workloads", [
            ("workload_key", "Workload", "string"), ("workload_type", "Type", "string"),
            ("cost_center", "Cost center", "string"), ("monthly_cost", "Monthly cost", "money"),
            ("share_of_visible_pct", "Share of visible %", "pct"), ("attribution", "Attribution", "string"),
            ("active_days", "Active days", "int"), ("findings", "Findings", "int"),
            ("rule_ids", "Rules", "string")],
            "Top workloads by cost", 0, 17, 6, 9, desc="Sort by any column; download as CSV from the widget menu"),
    ]

    recs = [
        b.text("rec_header", [
            "## Recommendations backlog",
            "Score **S = 0.35·V + 0.25·C + 0.20·F + 0.20·R** (value, confidence, feasibility, reversibility; "
            "each 0–1). **P1** = act this sprint, **P2** = plan, **P3** = backlog.",
            "Each row carries the evidence (metric vs threshold), why it was flagged (narrative) and the "
            "recommended action. Accepting a finding = create the work item in Azure DevOps and reference the "
            "rule id; dismissed findings are handled by disabling or tuning the rule in `config/rules.json`."],
            0, 0, 6, 2),
        b.filter("rec_f_priority", "multi-select", "priority", "Priority", ("ds_backlog", "ds_score_parts"), 0, 2),
        b.filter("rec_f_category", "multi-select", "category", "Category", ("ds_backlog", "ds_score_parts"), 2, 2),
        b.filter("rec_f_cc", "multi-select", "cost_center", "Cost center", ("ds_backlog", "ds_score_parts"), 4, 2),
        b.table("rec_backlog", "ds_backlog", [
            ("backlog_rank", "#", "int"), ("priority", "Priority", "string"), ("score", "Score", "float"),
            ("rule_id", "Rule", "string"), ("category", "Category", "string"), ("title", "Finding", "string"),
            ("workload_key", "Workload", "string"), ("cost_center", "Cost center", "string"),
            ("est_monthly_savings", "Savings / month", "money"), ("status", "Status", "string"),
            ("metric_name", "Metric", "string"), ("metric_value", "Observed", "float"),
            ("threshold", "Threshold", "float"), ("narrative", "Why flagged", "string"),
            ("action", "Recommended action", "string"), ("narrative_source", "Narrative source", "string")],
            "Backlog (highest score first)", 0, 3, 6, 10,
            desc="AI narratives are labelled; review before acting"),
        b.scatter("rec_value_feasibility", "ds_backlog", "feasibility_f", "value_v", "priority",
                  "Value vs feasibility", 0, 13, 3, 7, "Feasibility (F)", "Value (V)",
                  desc="Top right = quick wins · top left = big bets · bottom right = easy, low value"),
        b.bar("rec_score_parts", "ds_score_parts", "finding", "SUM(`weighted`)", "Score breakdown, top 15",
              3, 13, 3, 7, color="component", x_title="Finding", y_title="Weighted score"),
    ]
    return [
        {"name": "overview", "displayName": "Overview", "pageType": "PAGE_TYPE_CANVAS", "layout": overview},
        {"name": "recommendations", "displayName": "Recommendations", "pageType": "PAGE_TYPE_CANVAS",
         "layout": recs},
    ]


def _engineering_pages(cfg: DPOConfig) -> list[dict]:
    b = _Builder(cfg, "engineering")
    cur = cfg.currency
    fail_pct = 100 * float(cfg.rules().get("R06_JOB_FAILURE_RATE", {}).get("thresholds", {})
                           .get("max_failure_rate", 0.15))
    jobs = [
        b.text("jq_header", [
            "## Jobs & queries",
            f"Analysts and operators only (reads gold directly, real identities). Failure rates over "
            f"**{fail_pct:g}%** are highlighted (R06)."], 0, 0, 6, 1),
        b.filter("jq_f_cc", "multi-select", "cost_center", "Cost center", ("ds_jobs",), 0, 1),
        b.counter("jq_k_jobs", "ds_jobs", "COUNT(`job_id`)", "Jobs", 2, 1, 1, 2),
        b.counter("jq_k_failed", "ds_jobs", "SUM(`failed`)", "Failed runs", 3, 1, 1, 2),
        b.counter("jq_k_jobcost", "ds_jobs", "SUM(`monthly_cost`)", f"Job cost / month ({cur})", 4, 1, 1, 2),
        b.counter("jq_k_querycost", "ds_queries", "SUM(`monthly_cost`)", f"SQL cost / month ({cur})", 5, 1, 1, 2),
        b.table("jq_jobs", "ds_jobs", [
            ("job_name", "Job", "string"), ("runs", "Runs", "int"), ("failed", "Failed", "int"),
            ("failure_pct", "Failure rate %", "pct", _over("failure_pct", fail_pct)),
            ("p50_duration_s", "p50 (s)", "int"), ("p95_duration_s", "p95 (s)", "int"),
            ("monthly_cost", "Monthly cost", "money"), ("cost_per_success", "Cost / success", "money"),
            ("cost_center", "Cost center", "string"), ("rule_ids", "Rules", "string")],
            "Job reliability scorecard", 0, 3, 6, 8),
        b.table("jq_queries", "ds_queries", [
            ("fingerprint", "Fingerprint", "string"), ("statement_shape", "Statement shape", "string"),
            ("executions", "Executions", "int"), ("monthly_cost", "Monthly cost", "money"),
            ("avg_s", "Avg time (s)", "float"), ("avg_read_gb", "Avg read (GB)", "float"),
            ("cache_hit_pct", "Cache hit %", "pct"), ("spill_pct", "Spill share %", "pct", _over("spill_pct", 0)),
            ("rule_ids", "Rules", "string")],
            "Most expensive query shapes", 0, 11, 6, 8,
            desc="Literals and comments are stripped (retain_raw_sql = false)"),
    ]
    health = [
        b.text("ph_header", ["## Pipeline health",
                             "Latest run of the DPO pipeline, recent runs, source availability and data quality."],
               0, 0, 6, 1),
        b.counter("ph_k_failed", "ds_last_run", "SUM(`is_failed`)", "Failed stages (latest run)",
                  0, 1, 2, 2),
        b.counter("ph_k_unavailable", "ds_sources", "SUM(`not_available`)", "Sources missing", 2, 1, 2, 2),
        b.counter("ph_k_quarantined", "ds_dq", "SUM(`rows_quarantined`)", "Rows quarantined", 4, 1, 2, 2),
        b.table("ph_last_run", "ds_last_run", [
            ("stage", "Stage", "string"), ("status", "Status", "string"), ("seconds", "Seconds", "float"),
            ("rows_out", "Rows out", "string"), ("message", "Message", "string"), ("run_ts", "At", "datetime")],
            "Latest run", 0, 3, 3, 7),
        b.table("ph_runs", "ds_runs", [
            ("started", "Started", "datetime"), ("status", "Status", "string"), ("stages", "Stages", "int"),
            ("failed_stages", "Failed", "int"), ("seconds", "Seconds", "float"), ("run_id", "Run", "string")],
            "Recent runs", 3, 3, 3, 7),
        b.table("ph_sources", "ds_sources", [
            ("source_name", "Source", "string"), ("table_name", "Table", "string"), ("status", "Status", "string"),
            ("rows", "Rows in window", "int"), ("missing_columns", "Missing columns", "string"),
            ("message", "Message", "string")], "Source availability", 0, 10, 3, 7),
        b.table("ph_dq", "ds_dq", [
            ("source_name", "Source", "string"), ("rows_in", "Rows in", "int"), ("rows_out", "Rows out", "int"),
            ("rows_quarantined", "Quarantined", "int"), ("duplicates_removed", "Duplicates removed", "int"),
            ("reasons", "Reasons", "string")], "Data quality", 3, 10, 3, 7),
        b.table("ph_rules", "ds_rules", [
            ("rule_id", "Rule", "string"), ("status", "Status", "string"), ("findings", "Findings", "int"),
            ("seconds", "Seconds", "float"), ("message", "Message", "string")], "Rule status (latest run)",
            0, 17, 6, 7),
    ]
    return [
        {"name": "jobs_queries", "displayName": "Jobs & queries", "pageType": "PAGE_TYPE_CANVAS", "layout": jobs},
        {"name": "pipeline_health", "displayName": "Pipeline health", "pageType": "PAGE_TYPE_CANVAS",
         "layout": health},
    ]


# ================================================================== build ==
def build(cfg: DPOConfig, key: str) -> dict:
    ds = datasets(cfg)[key]
    pages = _cost_pages(cfg) if key == "cost_optimization" else _engineering_pages(cfg)
    return {
        "datasets": [{"name": n, "displayName": d, "queryLines": [l + "\n" for l in sql.strip().splitlines()]}
                     for n, (d, sql) in ds.items()],
        "pages": pages,
        "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}},
    }


def render(cfg: DPOConfig, key: str) -> str:
    return json.dumps(build(cfg, key), indent=2, ensure_ascii=False) + "\n"


def file_path(root: str, profile: str, key: str) -> str:
    return os.path.join(root, "dashboards", profile, f"{key}.lvdash.json")


def write_all(root: str, profiles: Optional[list[str]] = None) -> list[str]:
    env_dir = os.path.join(root, "config", "environments")
    profiles = profiles or sorted(f[:-5] for f in os.listdir(env_dir) if f.endswith(".json"))
    out = []
    for p in profiles:
        cfg = DPOConfig.from_profile(p)
        cfg.rules_path = os.path.join(root, "config", "rules.json")
        for key in DASHBOARDS:
            path = file_path(root, p, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(render(cfg, key))
            out.append(path)
    return out


# ================================================= install without the CLI ==
def find_warehouse(sdk, name: str = "") -> str:
    """Warehouse id by name; else the first serverless warehouse; else the first warehouse."""
    whs = list(sdk.warehouses.list())
    if name:
        for w in whs:
            if w.name == name:
                return w.id
        raise RuntimeError(f"SQL warehouse '{name}' not found or not visible to you. "
                           f"Visible: {', '.join(w.name for w in whs) or 'none'}")
    if not whs:
        raise RuntimeError("No SQL warehouse visible to you: create one (or get CAN_USE) and run again.")
    return next((w.id for w in whs if getattr(w, "enable_serverless_compute", False)), whs[0].id)


def dashboard_acl(cfg: DPOConfig, key: str) -> list[dict]:
    """Group permissions; empty when governance is not enforced (Free Edition has no groups)."""
    if not cfg.governance["enforce"]:
        return []
    acl = [{"group_name": cfg.group("admins"), "permission_level": "CAN_MANAGE"},
           {"group_name": cfg.group("analysts"), "permission_level": "CAN_RUN"},
           {"group_name": cfg.group("operators"), "permission_level": "CAN_RUN"}]
    if key == "cost_optimization":
        acl.append({"group_name": cfg.group("viewers"), "permission_level": "CAN_RUN"})
    return acl


def publish_dashboards(cfg: DPOConfig, sdk, root: str, parent_path: str, warehouse_name: str = "") -> list[dict]:
    """Create or update both dashboards in `parent_path`, publish them without embedded credentials and set
    group permissions. Leaves dashboards alone when the asset bundle manages DPO for this profile."""
    from .installer import bundle_managed_jobs
    if bundle_managed_jobs(sdk, cfg.profile):
        return [{"dashboard": d, "action": "managed by asset bundle"} for d in DASHBOARDS.values()]
    from databricks.sdk.service.dashboards import Dashboard
    from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
    wh = find_warehouse(sdk, warehouse_name)
    sdk.workspace.mkdirs(parent_path)
    existing = {d.display_name: d for d in sdk.lakeview.list() if (d.path or "").startswith(parent_path + "/")}
    out = []
    for key, title in DASHBOARDS.items():
        name = f"{title} ({cfg.profile})"
        with open(file_path(root, cfg.profile, key), encoding="utf-8") as fh:
            body = Dashboard(display_name=name, serialized_dashboard=fh.read(), warehouse_id=wh,
                             parent_path=parent_path)
        if name in existing:
            d = sdk.lakeview.update(existing[name].dashboard_id, body)
            action = "updated"
        else:
            d = sdk.lakeview.create(body)
            action = "created"
        sdk.lakeview.publish(d.dashboard_id, embed_credentials=False, warehouse_id=wh)
        acl = dashboard_acl(cfg, key)
        if acl:
            sdk.permissions.set("dashboards", d.dashboard_id, access_control_list=[
                AccessControlRequest(group_name=a["group_name"],
                                     permission_level=PermissionLevel(a["permission_level"])) for a in acl])
        out.append({"dashboard": name, "action": f"{action}, published (no embedded credentials)",
                    "id": d.dashboard_id})
    return out


if __name__ == "__main__":       # python -m dpo.dashboard
    here = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    for p in write_all(here):
        print(os.path.relpath(p, here))
