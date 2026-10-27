# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Optimization Lab — prove the levers
# MAGIC Before/after experiments on the built-in `samples.tpch` dataset, one per optimisation lever the rules
# MAGIC recommend. Results go to `gold.lab_results` as evidence that a fix type works before it is proposed to a
# MAGIC workload owner. Uses only public sample data: no client data is copied.
# MAGIC
# MAGIC | # | Lever | Rules it backs | Before | After |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | Clustering + sargable predicate | R11, R16 | unclustered table, `year(col)=X` | liquid clustering, range predicate |
# MAGIC | 2 | Column projection | R16 | `SELECT *` | needed columns only |
# MAGIC | 3 | Broadcast small dimension | R12 | default join | `BROADCAST` hint |
# MAGIC
# MAGIC Timings on shared serverless compute are noisy: each query runs `runs` times and the median is kept.
# MAGIC For client evidence also compare *files read / bytes read* in Query History → Query Profile (deterministic).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import datetime as dt
import statistics
import time

from dpo import governance

dbutils.widgets.text("runs", "3", "Runs per query")
RUNS = int(dbutils.widgets.get("runs"))
governance.authorize(spark, cfg, run_id="lab")           # creates tables: admins / operators only
LAB = cfg.schema("sim")                                   # sandbox schema, never client data

try:
    spark.sql("SELECT 1 FROM samples.tpch.lineitem LIMIT 1").collect()
except Exception as e:
    dbutils.notebook.exit(f"samples.tpch is not available in this workspace ({type(e).__name__}); lab skipped")

try:
    spark.conf.set("spark.databricks.io.cache.enabled", "false")
except Exception:
    pass  # not settable on serverless; medians smooth out caching


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
    results.append((name, rules, b, a, round((b - a) / b * 100, 1) if b else None, dt.datetime.utcnow()))
    print(f"{name}: before {b}s, after {a}s")

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {LAB}.lab_lineitem_raw AS SELECT * FROM samples.tpch.lineitem")
spark.sql(f"CREATE OR REPLACE TABLE {LAB}.lab_lineitem_opt CLUSTER BY (l_shipdate) AS SELECT * FROM samples.tpch.lineitem")
spark.sql(f"OPTIMIZE {LAB}.lab_lineitem_opt")

experiment(
    "clustering_and_range_filter", "R11,R16",
    f"""SELECT l_returnflag, SUM(l_extendedprice) FROM {LAB}.lab_lineitem_raw
        WHERE year(l_shipdate) = 1995 GROUP BY 1""",
    f"""SELECT l_returnflag, SUM(l_extendedprice) FROM {LAB}.lab_lineitem_opt
        WHERE l_shipdate >= '1995-01-01' AND l_shipdate < '1996-01-01' GROUP BY 1""",
)
experiment(
    "select_star_vs_projection", "R16",
    f"SELECT * FROM {LAB}.lab_lineitem_raw WHERE l_quantity > 49",
    f"SELECT l_orderkey, l_quantity, l_extendedprice FROM {LAB}.lab_lineitem_raw WHERE l_quantity > 49",
)
experiment(
    "broadcast_small_dim", "R12",
    f"""SELECT n.n_name, SUM(l.l_extendedprice)
        FROM {LAB}.lab_lineitem_raw l
        JOIN samples.tpch.orders o   ON l.l_orderkey = o.o_orderkey
        JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
        JOIN samples.tpch.nation n   ON c.c_nationkey = n.n_nationkey
        GROUP BY 1""",
    f"""SELECT /*+ BROADCAST(n) */ n.n_name, SUM(l.l_extendedprice)
        FROM {LAB}.lab_lineitem_opt l
        JOIN samples.tpch.orders o   ON l.l_orderkey = o.o_orderkey
        JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
        JOIN samples.tpch.nation n   ON c.c_nationkey = n.n_nationkey
        GROUP BY 1""",
)

# COMMAND ----------

schema = "experiment string, rule_ids string, before_sec double, after_sec double, improvement_pct double, run_at timestamp"
spark.createDataFrame(results, schema).write.mode("append").saveAsTable(cfg.t("gold", "lab_results"))
for t in ("lab_lineitem_raw", "lab_lineitem_opt"):
    spark.sql(f"DROP TABLE IF EXISTS {LAB}.{t}")          # do not leave 6 M-row copies behind
display(spark.table(cfg.t("gold", "lab_results")).orderBy("run_at", ascending=False))
