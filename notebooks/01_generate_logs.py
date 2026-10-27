# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate synthetic cluster/job usage logs
# MAGIC Produces `workspace.dpo.raw_usage_logs` — 90 days of usage for a realistic mix of workloads,
# MAGIC each seeded with a known cost problem so the categorization and AI layers have something to find.
# MAGIC
# MAGIC **Column contract** (keep identical when swapping in real system tables — see `01b_system_tables_source`):
# MAGIC `usage_date, job_name, cluster_id, workload_type, sku, node_type, num_workers, runtime_hours, dbus,
# MAGIC avg_cpu_util, idle_minutes, spill_gb, failed_retries, query_text`

# COMMAND ----------

import random
import zlib
import datetime as dt
from pyspark.sql import Row

dbutils.widgets.text("days", "90", "Days of history")
dbutils.widgets.text("start_date", "2026-07-01", "Start date (YYYY-MM-DD)")
DAYS = int(dbutils.widgets.get("days"))
START = dt.date.fromisoformat(dbutils.widgets.get("start_date"))
TARGET = "workspace.dpo.raw_usage_logs"

# COMMAND ----------

# Each job carries a deliberate inefficiency profile:
# name, workload_type, sku, base_dbu, cpu_range, idle_range, spill_range, retry_weights, query_text
JOBS = [
    ("sales_daily_etl", "ETL_JOB", "JOBS_COMPUTE", 40, (0.45, 0.85), (0, 10), (12, 45), [0, 0, 0, 1],
     "SELECT * FROM sales s JOIN customers c ON s.cid = c.id WHERE year(s.dt) = 2026"),
    ("adhoc_analyst_nb", "INTERACTIVE", "ALL_PURPOSE_COMPUTE", 25, (0.05, 0.30), (40, 120), (0, 2), [0],
     None),
    ("finance_dashboard", "SQL", "SQL_PRO", 30, (0.40, 0.80), (0, 5), (0, 4), [0, 0, 1],
     "SELECT region, SUM(amt) FROM fact_txn WHERE txn_dt >= current_date() - 30 GROUP BY region"),
    ("raw_file_copy", "INGEST", "JOBS_COMPUTE", 18, (0.05, 0.20), (0, 5), (0, 1), [0],
     "INSERT INTO bronze.landing SELECT * FROM csv.`/landing/daily/`"),
    ("ml_feature_build", "ETL_JOB", "ALL_PURPOSE_COMPUTE", 60, (0.30, 0.70), (0, 15), (20, 60), [0, 1, 2],
     "SELECT DISTINCT * FROM events e CROSS JOIN dim_date d WHERE e.event_dt = d.dt"),
    ("dlt_orders", "DLT", "DLT_ADVANCED", 22, (0.55, 0.90), (0, 5), (0, 3), [0, 0, 0, 0, 1],
     None),
    ("customer_360_merge", "ETL_JOB", "JOBS_COMPUTE", 35, (0.50, 0.85), (0, 5), (0, 5), [1, 2, 2, 3],
     "MERGE INTO gold.customer_360 t USING silver.customer_updates s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *"),
    ("inventory_snapshot", "ETL_JOB", "SERVERLESS_JOBS", 12, (0.60, 0.90), (0, 3), (0, 2), [0],
     "SELECT sku, warehouse, qty FROM silver.inventory WHERE snapshot_dt = current_date()"),
]

def stable_id(name: str) -> str:
    return f"c-{zlib.crc32(name.encode()) % 10000:04d}"

random.seed(42)
rows = []
for d in range(DAYS):
    day = START + dt.timedelta(days=d)
    weekend = day.weekday() >= 5
    for name, wl, sku, base, cpu, idle, spill, retries, q in JOBS:
        if weekend and wl == "INTERACTIVE":
            continue  # analysts don't work weekends
        growth = 1 + d / (DAYS * 2)  # data volume grows ~50% across the window
        rows.append(Row(
            usage_date=day,
            job_name=name,
            cluster_id=stable_id(name),
            workload_type=wl,
            sku=sku,
            node_type=random.choice(["Standard_DS3_v2", "Standard_E8s_v3", "Standard_E16s_v3"]),
            num_workers=random.choice([2, 4, 8, 16]),
            runtime_hours=round(random.uniform(0.3, 6.0), 2),
            dbus=round(base * growth * random.uniform(0.6, 1.4), 2),
            avg_cpu_util=round(random.uniform(*cpu), 2),
            idle_minutes=random.randint(*idle),
            spill_gb=round(random.uniform(*spill), 1),
            failed_retries=random.choice(retries),
            query_text=q,
        ))

df = spark.createDataFrame(rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)
print(f"Wrote {df.count()} rows to {TARGET}")

# COMMAND ----------

display(spark.table(TARGET).groupBy("job_name", "workload_type").count().orderBy("job_name"))
