# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Optimization lab: prove the levers
# MAGIC
# MAGIC **Purpose.** Before a fix is proposed to a workload owner, show that the fix type works. Each experiment runs the
# MAGIC same query before and after one optimisation lever on the public `samples.tpch` data, and records the timings in
# MAGIC `gold.lab_results`. No client data is copied.
# MAGIC
# MAGIC | # | Lever | Rules it backs | Before | After |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | Clustering + sargable predicate | R11, R16 | unclustered table, `year(col) = X` | liquid clustering, range predicate |
# MAGIC | 2 | Column projection | R16 | `SELECT *` | needed columns only |
# MAGIC | 3 | Broadcast small dimension | R12 | default join | `BROADCAST` hint (same table, only the hint changes) |
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Who runs it** | operators or admins (it creates tables); the installer runs it once |
# MAGIC | **Reads** | `samples.tpch` (skipped if the workspace has no samples) |
# MAGIC | **Writes** | temporary `lab_*` tables in the sim schema (dropped at the end), `gold.lab_results` (appended) |
# MAGIC
# MAGIC Timings on shared serverless compute are noisy: each query runs `runs` times (1–10) and the median is kept. For client
# MAGIC evidence also compare *files read / bytes read* in Query History → Query Profile, which are deterministic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared bootstrap
# MAGIC Loads the DPO package, detects the profile, creates the shared widgets and builds `cfg` (see `_common`).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Set-up
# MAGIC Authorises the caller (creates tables: operators/admins only), checks `samples.tpch` is available and defines the
# MAGIC timing helpers.

# COMMAND ----------

import contextlib
import datetime as dt
import statistics
import time

from dpo import governance

dbutils.widgets.text("runs", "3", "Runs per query")
RUNS = min(max(int(dbutils.widgets.get("runs")), 1), 10)  # bounded: each run scans ~6 M rows
governance.authorize(spark, cfg, run_id="lab")  # creates tables: admins / operators only
LAB = cfg.schema("sim")  # sandbox schema, never client data

try:
    spark.sql("SELECT 1 FROM samples.tpch.lineitem LIMIT 1").collect()
except Exception as e:
    dbutils.notebook.exit(f"samples.tpch is not available in this workspace ({type(e).__name__}); lab skipped")

with contextlib.suppress(Exception):  # not settable on serverless; medians smooth out caching
    spark.conf.set("spark.databricks.io.cache.enabled", "false")


def timed(sql: str) -> float:
    times = []
    for _ in range(RUNS):
        t0 = time.time()
        spark.sql(sql).collect()
        times.append(time.time() - t0)
    return round(statistics.median(times), 3)


results = []


def experiment(name, rules, before_sql, after_sql):
    b, a = timed(before_sql), timed(after_sql)
    results.append((name, rules, b, a, round((b - a) / b * 100, 1) if b else None, dt.datetime.now(dt.timezone.utc)))
    print(f"{name}: before {b}s, after {a}s")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 · Experiments
# MAGIC Builds an unclustered and a liquid-clustered copy of `lineitem`, then runs the three before/after pairs.

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {LAB}.lab_lineitem_raw AS SELECT * FROM samples.tpch.lineitem")
spark.sql(
    f"CREATE OR REPLACE TABLE {LAB}.lab_lineitem_opt CLUSTER BY (l_shipdate) AS SELECT * FROM samples.tpch.lineitem"
)
spark.sql(f"OPTIMIZE {LAB}.lab_lineitem_opt")

experiment(
    "clustering_and_range_filter",
    "R11,R16",
    f"""SELECT l_returnflag, SUM(l_extendedprice) FROM {LAB}.lab_lineitem_raw
        WHERE year(l_shipdate) = 1995 GROUP BY 1""",
    f"""SELECT l_returnflag, SUM(l_extendedprice) FROM {LAB}.lab_lineitem_opt
        WHERE l_shipdate >= '1995-01-01' AND l_shipdate < '1996-01-01' GROUP BY 1""",
)
experiment(
    "select_star_vs_projection",
    "R16",
    f"SELECT * FROM {LAB}.lab_lineitem_raw WHERE l_quantity > 49",
    f"SELECT l_orderkey, l_quantity, l_extendedprice FROM {LAB}.lab_lineitem_raw WHERE l_quantity > 49",
)
experiment(
    "broadcast_small_dim",
    "R12",
    f"""SELECT n.n_name, SUM(l.l_extendedprice)
        FROM {LAB}.lab_lineitem_raw l
        JOIN samples.tpch.orders o   ON l.l_orderkey = o.o_orderkey
        JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
        JOIN samples.tpch.nation n   ON c.c_nationkey = n.n_nationkey
        GROUP BY 1""",
    f"""SELECT /*+ BROADCAST(n) */ n.n_name, SUM(l.l_extendedprice)
        FROM {LAB}.lab_lineitem_raw l
        JOIN samples.tpch.orders o   ON l.l_orderkey = o.o_orderkey
        JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
        JOIN samples.tpch.nation n   ON c.c_nationkey = n.n_nationkey
        GROUP BY 1""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 · Record and clean up
# MAGIC Appends the results to `gold.lab_results` and drops the temporary copies so no 6 M-row tables are left behind.

# COMMAND ----------

schema = (
    "experiment string, rule_ids string, before_sec double, after_sec double, improvement_pct double, run_at timestamp"
)
spark.createDataFrame(results, schema).write.mode("append").saveAsTable(cfg.t("gold", "lab_results"))
for t in ("lab_lineitem_raw", "lab_lineitem_opt"):
    spark.sql(f"DROP TABLE IF EXISTS {LAB}.{t}")  # do not leave 6 M-row copies behind
display(spark.table(cfg.t("gold", "lab_results")).orderBy("run_at", ascending=False))
