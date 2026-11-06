"""What DPO itself costs, and whether it pays for itself.

DPO runs on Databricks, so its own processing is billed like any other workload and shows up in the same
system tables it analyses. This stage finds that footprint and reports it next to the savings DPO proposes.

How DPO's own usage is recognised:
  jobs    billing rows of DPO jobs (silver.jobs.is_dpo: tag app=dpo-accelerator, or a dpo-pipeline/install/
          governance/tests job name). Covers the serverless job compute of the pipeline, installer, governance
          and test jobs, and any AI-function usage billed to those jobs.
  sql     SQL-warehouse cost of queries that read DPO schemas (silver.queries.reads_dpo): dashboards, Genie,
          the ai_forecast statement, analysts' ad hoc queries. Warehouse-day cost is spread by task time, the
          same allocation the rules use.
  tagged  other billing rows tagged app=dpo-accelerator (e.g. a serverless budget policy), not counted above.
          Warehouse rows are left to `sql` so nothing is counted twice.

Outputs (gold, analysts and operators only: they include account-wide totals):
  dpo_cost_daily    usage_date, component, item, product, cost, dbus
  dpo_run_cost      cost per DPO job run (billing arrives hours later, so the latest run fills in next time)
  dpo_cost_summary  one row: DPO cost per month by component, share of all spend, estimated savings found,
                    and the savings-to-cost ratio

Limits: pay-per-token model calls (AI narratives) are billed to the model endpoint; when billing does not tie
them to the DPO job they are not in these numbers. The run log records how many calls each run made
(`narrate`: ai_generated) so they can be estimated. Billing lags by a few hours, so today's runs are partial.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:  # annotations only: no runtime import (optional SDK, no import cycles)
    from .pipeline import Ctx


COLS = ["usage_date", "component", "item", "product", "cost", "dbus"]


def run_self_cost(ctx: Ctx) -> dict:
    """Self-cost stage: DPO's daily cost by component, cost per DPO job run, and the savings-to-cost summary."""
    from .pipeline import query_costs

    usage, jobs = ctx.read("silver", "usage_cost"), ctx.read("silver", "jobs")
    dpo_jobs = jobs.filter("is_dpo").select("job_id", "job_name")

    job_usage = usage.join(dpo_jobs, "job_id")
    jobs_daily = (
        job_usage.groupBy(
            "usage_date", F.col("job_name").alias("item"), F.col("billing_origin_product").alias("product")
        )
        .agg(F.sum("cost").alias("cost"), F.sum("dbus").alias("dbus"))
        .withColumn("component", F.lit("jobs"))
    )

    tagged_daily = (
        usage.filter((F.col("app_tag") == "dpo-accelerator") & F.col("warehouse_id").isNull())
        .join(dpo_jobs.select("job_id"), "job_id", "left_anti")
        .groupBy("usage_date", F.col("workload_key").alias("item"), F.col("billing_origin_product").alias("product"))
        .agg(F.sum("cost").alias("cost"), F.sum("dbus").alias("dbus"))
        .withColumn("component", F.lit("tagged"))
    )

    q = query_costs(ctx.read("silver", "queries"), ctx.read("gold", "workload_cost_daily"))
    sql_daily = (
        q.filter("reads_dpo")
        .groupBy(
            F.col("query_date").alias("usage_date"),
            F.coalesce("client_application", F.lit("(unknown client)")).alias("item"),
        )
        .agg(F.sum("query_cost").alias("cost"))
        .select(
            "usage_date",
            "item",
            F.lit("SQL").alias("product"),
            "cost",
            F.lit(None).cast("double").alias("dbus"),
            F.lit("sql").alias("component"),
        )
    )

    daily = jobs_daily.select(*COLS).unionByName(tagged_daily.select(*COLS)).unionByName(sql_daily.select(*COLS))
    n_daily = ctx.save(daily, "gold", "dpo_cost_daily")

    runs = ctx.read("silver", "job_runs").select(
        F.col("run_id").alias("job_run_id"), "start_time", "end_time", "result_state", "duration_s"
    )
    per_run = (
        job_usage.filter(F.col("job_run_id").isNotNull())
        .groupBy("job_id", "job_name", "job_run_id")
        .agg(F.sum("cost").alias("cost"), F.sum("dbus").alias("dbus"))
        .join(runs, "job_run_id", "left")
    )
    n_runs = ctx.save(per_run, "gold", "dpo_run_cost")

    # ---- summary: does DPO pay for itself? ----------------------------------------
    m = float(ctx.monthly)
    by = {
        r.component: float(r.cost or 0.0)
        for r in ctx.read("gold", "dpo_cost_daily").groupBy("component").agg(F.sum("cost").alias("cost")).collect()
    }
    dpo_window = float(sum(by.values(), 0.0))
    total_window = float(usage.agg(F.sum("cost")).collect()[0][0] or 0.0)
    recs = (
        ctx.read("gold", "recommendations")
        .agg(
            F.sum(F.coalesce("est_monthly_savings", F.lit(0.0))).alias("all"),
            F.sum(F.when(F.col("priority") == "P1", F.coalesce("est_monthly_savings", F.lit(0.0)))).alias("p1"),
        )
        .collect()[0]
    )
    savings, p1 = float(recs["all"] or 0.0), float(recs["p1"] or 0.0)
    dpo_month = dpo_window * m
    row = (
        ctx.window_days,
        round(dpo_window, 2),
        round(dpo_month, 2),
        round(by.get("jobs", 0.0) * m, 2),
        round(by.get("sql", 0.0) * m, 2),
        round(by.get("tagged", 0.0) * m, 2),
        round(dpo_window / total_window, 6) if total_window else None,
        round(savings, 2),
        round(p1, 2),
        round(savings / dpo_month, 1) if dpo_month else None,
        ctx.read("gold", "dpo_run_cost").count(),
        ctx.run_ts,
    )
    summary = ctx.spark.createDataFrame(
        [row],
        (
            "window_days int, dpo_window_cost double, dpo_monthly_cost double, jobs_monthly double, "
            "sql_monthly double, tagged_monthly double, share_of_total_spend double, "
            "est_monthly_savings double, p1_est_monthly_savings double, savings_to_cost_ratio double, "
            "dpo_job_runs long, computed_at timestamp"
        ),
    )
    ctx.save(summary, "gold", "dpo_cost_summary")
    return {
        "dpo_cost_daily": n_daily,
        "dpo_run_cost": n_runs,
        "dpo_monthly_cost": row[2],
        "savings_to_cost_ratio": row[9],
    }
