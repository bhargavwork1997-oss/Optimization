# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Optimization Lab — prove the savings
# MAGIC Runs before/after experiments on the built-in `samples.tpch` dataset for each optimization lever and
# MAGIC records timings in `workspace.dpo.lab_results`. Use the results (plus Query Profile screenshots) as client
# MAGIC evidence that the levers in notebook 03 are real.
# MAGIC
# MAGIC | # | Lever | Before | After |
# MAGIC |---|---|---|---|
# MAGIC | 1 | PARTITION_OR_CLUSTER + OPTIMIZE_SQL | unclustered table, `year(col)=X` | liquid clustering, range predicate |
# MAGIC | 2 | OPTIMIZE_SQL | `SELECT *` | column projection |
# MAGIC | 3 | OPTIMIZE_SQL | join on large dim, no hint | broadcast hint on small dim |
# MAGIC
# MAGIC Timings on shared serverless compute are noisy: each query runs `RUNS` times and the median is kept.
# MAGIC For the client deck, also open **Query History → Query Profile** and compare *files read / bytes read*,
# MAGIC which are deterministic.

# COMMAND ----------

import time, statistics, datetime as dt

dbutils.widgets.text("runs", "3", "Runs per query")
RUNS = int(dbutils.widgets.get("runs"))
S = "workspace.dpo"

for k, v in {"spark.databricks.io.cache.enabled": "false"}.items():
    try:
        spark.conf.set(k, v)
    except Exception:
        pass  # not settable on serverless; medians still smooth out caching

def timed(sql: str) -> float:
    times = []
    for _ in range(RUNS):
        t0 = time.time()
        spark.sql(sql).collect()
        times.append(time.time() - t0)
    return round(statistics.median(times), 3)

results = []
def experiment(name, lever, before_sql, after_sql):
    b, a = timed(before_sql), timed(after_sql)
    results.append((name, lever, b, a, round((b - a) / b * 100, 1) if b else None, dt.datetime.now()))
    print(f"{name}: before {b}s, after {a}s")

# COMMAND ----------

# MAGIC %md ### Build lab tables

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {S}.lineitem_raw AS SELECT * FROM samples.tpch.lineitem")
spark.sql(f"CREATE OR REPLACE TABLE {S}.lineitem_opt CLUSTER BY (l_shipdate) AS SELECT * FROM samples.tpch.lineitem")
spark.sql(f"OPTIMIZE {S}.lineitem_opt")
print("Lab tables ready")

# COMMAND ----------

# MAGIC %md ### Experiment 1 — clustering + sargable predicate

# COMMAND ----------

experiment(
    "clustering_and_range_filter", "PARTITION_OR_CLUSTER",
    f"""SELECT l_returnflag, SUM(l_extendedprice) FROM {S}.lineitem_raw
        WHERE year(l_shipdate) = 1995 GROUP BY 1""",
    f"""SELECT l_returnflag, SUM(l_extendedprice) FROM {S}.lineitem_opt
        WHERE l_shipdate >= '1995-01-01' AND l_shipdate < '1996-01-01' GROUP BY 1""",
)

# COMMAND ----------

# MAGIC %md ### Experiment 2 — column projection

# COMMAND ----------

experiment(
    "select_star_vs_projection", "OPTIMIZE_SQL",
    f"SELECT * FROM {S}.lineitem_raw WHERE l_quantity > 49",
    f"SELECT l_orderkey, l_quantity, l_extendedprice FROM {S}.lineitem_raw WHERE l_quantity > 49",
)

# COMMAND ----------

# MAGIC %md ### Experiment 3 — broadcast small dimension

# COMMAND ----------

experiment(
    "broadcast_small_dim", "OPTIMIZE_SQL",
    f"""SELECT n.n_name, SUM(l.l_extendedprice)
        FROM {S}.lineitem_raw l
        JOIN samples.tpch.orders o   ON l.l_orderkey = o.o_orderkey
        JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
        JOIN samples.tpch.nation n   ON c.c_nationkey = n.n_nationkey
        GROUP BY 1""",
    f"""SELECT /*+ BROADCAST(n) */ n.n_name, SUM(l.l_extendedprice)
        FROM {S}.lineitem_opt l
        JOIN samples.tpch.orders o   ON l.l_orderkey = o.o_orderkey
        JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
        JOIN samples.tpch.nation n   ON c.c_nationkey = n.n_nationkey
        GROUP BY 1""",
)

# COMMAND ----------

# MAGIC %md ### Save results

# COMMAND ----------

cols = ["experiment", "lever", "before_sec", "after_sec", "improvement_pct", "run_at"]
spark.createDataFrame(results, cols).write.mode("append").saveAsTable(f"{S}.lab_results")
display(spark.table(f"{S}.lab_results").orderBy("run_at", ascending=False))
