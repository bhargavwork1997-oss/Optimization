"""Spend forecast and budget outlook per cost centre.

Problem: the monthly bill is only known after the month ends; budget overruns are found too late to act.
Answer: forecast daily spend per cost centre to the end of the month with Databricks `ai_forecast`, add the
spend already incurred this month, and compare the projection (and its upper bound) with the budget.

`ai_forecast` is a Databricks SQL function (Pro or serverless SQL warehouse only), so the pipeline writes
the input series to gold.forecast_input and runs the forecast on the job's SQL warehouse through the
Statement Execution API, as the pipeline identity. If no warehouse is configured, the function is not
available, or the call fails, a transparent weekday-average baseline is used instead and recorded in the
`method` column, so the outlook is always produced and never silently pretends to be AI.

Outputs (gold; secured `reporting` views filter rows by cost centre like every other view):
  cost_forecast   forecast_date, cost_center, cost_forecast, cost_lower, cost_upper, method
  budget_outlook  one row per cost centre for the outlook month: actual month-to-date, forecast for the
                  remaining days, projected total with bounds, budget, % of budget, status
Status: OVER_BUDGET (already above) > AT_RISK (projection above) > WATCH (upper bound above) > ON_TRACK;
NO_BUDGET when the profile has no budget for the cost centre.
"""

from __future__ import annotations

import datetime as dt
import time
from statistics import NormalDist
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:  # annotations only: no runtime import (optional SDK, no import cycles)
    from pyspark.sql import DataFrame

    from .config import DPOConfig
    from .pipeline import Ctx


UNTAGGED = "(untagged)"
MIN_HISTORY_DAYS = 14  # fewer observed days -> no forecast (too little signal for weekly patterns)


def outlook_month(as_of: dt.date) -> tuple[dt.date, dt.date]:
    """Month the outlook is for: the month of the first day not yet observed. Returns (first, last) day."""
    d = as_of + dt.timedelta(days=1)
    first = d.replace(day=1)
    nxt = (first + dt.timedelta(days=32)).replace(day=1)
    return first, nxt - dt.timedelta(days=1)


def _daily(ctx):
    """Daily cost per cost centre over the window, with 0 for days without spend (ai_forecast does not
    fill gaps itself)."""
    c = ctx.read("gold", "cost_by_dimension")
    d = c.groupBy("usage_date", F.coalesce("cost_center", F.lit(UNTAGGED)).alias("cost_center")).agg(
        F.sum("cost").alias("cost")
    )
    bounds = d.agg(F.min("usage_date").alias("lo"), F.max("usage_date").alias("hi")).collect()[0]
    if bounds.lo is None:
        return None, None
    days = ctx.spark.sql(f"SELECT explode(sequence(DATE'{bounds.lo}', DATE'{bounds.hi}')) AS usage_date")
    spine = days.crossJoin(d.select("cost_center").distinct())
    return (
        spine.join(d, ["usage_date", "cost_center"], "left").select(
            "usage_date", "cost_center", F.coalesce("cost", F.lit(0.0)).cast("double").alias("cost")
        )
    ), bounds.hi


# --------------------------------------------------------------- methods --
def baseline(ctx: Ctx, daily: DataFrame, horizon: dt.date) -> DataFrame:
    """Weekday average x recent trend; interval from the spread of daily residuals."""
    z = NormalDist().inv_cdf(0.5 + ctx.cfg.forecast_interval / 2)
    dow = F.dayofweek("usage_date")
    last = daily.agg(F.max("usage_date")).collect()[0][0]
    stats = daily.withColumn("dow", dow)
    by_dow = stats.groupBy("cost_center", "dow").agg(F.avg("cost").alias("dow_mean"))
    lvl = stats.groupBy("cost_center").agg(
        F.avg("cost").alias("mean_all"),
        F.avg(F.when(F.col("usage_date") > F.date_sub(F.lit(last), 7), F.col("cost"))).alias("mean_7d"),
    )
    resid = (
        stats.join(by_dow, ["cost_center", "dow"])
        .groupBy("cost_center")
        .agg(F.coalesce(F.stddev(F.col("cost") - F.col("dow_mean")), F.lit(0.0)).alias("sd"))
    )
    trend = F.least(
        F.lit(2.0),
        F.greatest(
            F.lit(0.5),
            F.coalesce(F.col("mean_7d") / F.when(F.col("mean_all") > 0, F.col("mean_all")), F.lit(1.0)),
        ),
    )
    future = ctx.spark.sql(
        f"SELECT explode(sequence(DATE'{last + dt.timedelta(days=1)}', "
        f"DATE'{horizon - dt.timedelta(days=1)}')) AS forecast_date"
    )
    # per-cost-centre stats joined first (tiny equi join), so the cross join builds the
    # date x cost-centre grid against one consolidated stats table
    cc_stats = lvl.join(resid, "cost_center")
    fc = (
        future.crossJoin(cc_stats)
        .withColumn("dow", F.dayofweek("forecast_date"))
        .join(by_dow, ["cost_center", "dow"], "left")
        .withColumn("cost_forecast", F.coalesce("dow_mean", "mean_all") * trend)
    )
    return fc.select(
        "forecast_date",
        "cost_center",
        "cost_forecast",
        (F.col("cost_forecast") - z * F.col("sd")).alias("cost_lower"),
        (F.col("cost_forecast") + z * F.col("sd")).alias("cost_upper"),
    )


def ai_forecast_sql(ctx: Ctx, horizon: dt.date) -> str:
    """CREATE TABLE ... AS ai_forecast(...) over gold.forecast_input. Only validated identifiers, a date
    and numbers are interpolated."""
    cfg = ctx.cfg
    if cfg.forecast_version == "2":
        extra = ",\n  positive_only => true,\n  version => '2'"
    else:
        extra = ",\n  parameters => '{\"global_floor\": 0}'" + (
            ",\n  version => '1'" if cfg.forecast_version == "1" else ""
        )
    return (
        f"CREATE OR REPLACE TABLE {ctx.t('gold', 'forecast_raw')} AS\n"
        f"SELECT * FROM ai_forecast(\n  TABLE({ctx.t('gold', 'forecast_input')}),\n"
        f"  horizon => '{horizon.isoformat()}',\n  time_col => 'usage_date',\n  value_col => 'cost',\n"
        f"  group_col => 'cost_center',\n  prediction_interval_width => {float(cfg.forecast_interval)},\n"
        f"  frequency => 'D'{extra})"
    )


def run_on_warehouse(cfg: DPOConfig, sql: str, timeout_s: int = 900) -> None:
    """Run one statement on the SQL warehouse as the current identity (the job's run-as)."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient()
    r = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=cfg.forecast_warehouse_id, wait_timeout="50s"
    )
    deadline = time.time() + timeout_s
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            w.statement_execution.cancel_execution(r.statement_id)
            raise TimeoutError("ai_forecast did not finish in time")
        time.sleep(5)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        err = r.status.error.message if r.status.error else r.status.state.value
        raise RuntimeError(f"ai_forecast failed: {err}"[:500])


def _ai(ctx, daily, horizon: dt.date):
    if not ctx.cfg.catalog:
        raise RuntimeError("ai_forecast needs Unity Catalog and a SQL warehouse")
    if not ctx.cfg.forecast_warehouse_id:
        raise RuntimeError("no SQL warehouse configured (forecast_warehouse_id)")
    ctx.save(daily, "gold", "forecast_input")
    run_on_warehouse(ctx.cfg, ai_forecast_sql(ctx, horizon))
    raw = ctx.read("gold", "forecast_raw")
    return raw.select(
        F.col("usage_date").cast("date").alias("forecast_date"),
        "cost_center",
        "cost_forecast",
        "cost_lower",
        "cost_upper",
    )


# ------------------------------------------------------------------ stage --
def run_forecast(ctx: Ctx) -> dict:
    """Forecast stage: daily forecast per cost centre to month end (ai_forecast or baseline) and the budget
    outlook. Skips with a note when there is no data or fewer than 14 days of history.
    """
    cfg = ctx.cfg
    if not cfg.forecast_enabled:
        return {"forecast": "disabled"}
    daily, as_of = _daily(ctx)
    if daily is None:
        return {"forecast": "no cost data"}
    observed_days = daily.select("usage_date").distinct().count()
    first, last = outlook_month(as_of)
    horizon = last + dt.timedelta(days=1)  # right-exclusive
    if observed_days < MIN_HISTORY_DAYS:
        return {"forecast": f"skipped: {observed_days} days of history (< {MIN_HISTORY_DAYS})"}

    method, note = "baseline", "baseline"
    if cfg.forecast_method == "ai_forecast":
        try:
            fc = _ai(ctx, daily, horizon).cache()
            fc.count()
            method, note = "ai_forecast", "ai_forecast"
        except Exception as exc:  # noqa: BLE001 - recorded in run_log and in the method column
            note = f"fallback to baseline: {type(exc).__name__}: {str(exc)[:200]}"
    if method == "baseline":
        fc = baseline(ctx, daily, horizon)
    fc = fc.filter((F.col("forecast_date") > F.lit(as_of)) & (F.col("forecast_date") < F.lit(horizon))).select(
        "forecast_date",
        "cost_center",
        *[
            F.greatest(F.lit(0.0), F.col(c).cast("double")).alias(c)
            for c in ("cost_forecast", "cost_lower", "cost_upper")
        ],
        F.lit(method).alias("method"),
    )
    n_fc = ctx.save(fc, "gold", "cost_forecast")

    # ---- budget outlook ---------------------------------------------------------
    mtd = (
        daily.filter((F.col("usage_date") >= F.lit(first)) & (F.col("usage_date") <= F.lit(as_of)))
        .groupBy("cost_center")
        .agg(F.sum("cost").alias("actual_mtd"))
    )
    rest = (
        ctx.read("gold", "cost_forecast")
        .groupBy("cost_center")
        .agg(
            F.sum("cost_forecast").alias("forecast_remaining"),
            F.sum("cost_lower").alias("lower_remaining"),
            F.sum("cost_upper").alias("upper_remaining"),
        )
    )
    budgets = ctx.spark.createDataFrame(
        [(k, float(v)) for k, v in cfg.budgets.items()], "cost_center string, budget double"
    )
    ccs = daily.select("cost_center").distinct()
    days_left = (last - as_of).days if as_of < last else 0
    o = (
        ccs.join(mtd, "cost_center", "left")
        .join(rest, "cost_center", "left")
        .join(budgets, "cost_center", "left")
        .fillna(0.0, ["actual_mtd", "forecast_remaining", "lower_remaining", "upper_remaining"])
        .withColumn("projected_total", F.col("actual_mtd") + F.col("forecast_remaining"))
        .withColumn("projected_lower", F.col("actual_mtd") + F.col("lower_remaining"))
        .withColumn("projected_upper", F.col("actual_mtd") + F.col("upper_remaining"))
        .withColumn("pct_of_budget", F.when(F.col("budget") > 0, F.col("projected_total") / F.col("budget")))
        .withColumn(
            "status",
            F.when(F.col("budget").isNull(), "NO_BUDGET")
            .when(F.col("actual_mtd") > F.col("budget"), "OVER_BUDGET")
            .when(F.col("projected_total") > F.col("budget"), "AT_RISK")
            .when(F.col("projected_upper") > F.col("budget"), "WATCH")
            .otherwise("ON_TRACK"),
        )
        .withColumn("budget_gap", F.col("projected_total") - F.col("budget"))
        .select(
            "cost_center",
            F.lit(first).alias("month_start"),
            F.lit(last).alias("month_end"),
            F.lit(as_of).alias("as_of_date"),
            F.lit(days_left).alias("days_remaining"),
            "budget",
            "actual_mtd",
            "forecast_remaining",
            "projected_total",
            "projected_lower",
            "projected_upper",
            "pct_of_budget",
            "budget_gap",
            "status",
            F.lit(method).alias("method"),
            F.lit(first >= ctx.start_date).alias("mtd_complete"),
        )
    )
    n_o = ctx.save(o, "gold", "budget_outlook")
    return {"cost_forecast": n_fc, "budget_outlook": n_o, "forecast": note}
