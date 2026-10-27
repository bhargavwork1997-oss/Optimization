# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Aggressive / stress tests
# MAGIC 1. **Chaos** – duplicates, null ids, negative DBUs, unknown SKUs, time travel, schema drift, impossible CPU.
# MAGIC 2. **Missing sources** – node_timeline and table inventory absent (typical serverless-only workspace).
# MAGIC 3. **Empty window** – no data in range; every stage must still succeed.
# MAGIC 4. **Volume** – `large` (≈1.6 M rows) or `xl` (≈12 M); detection must stay at 100 % recall, timings logged.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.dropdown("volume_scale", "large", ["medium", "large", "xl"], "Volume test scale")
import time
from pyspark.sql import functions as F
from dpo import pipeline, synth, validation

results, perf = [], []

def record(suite, checks):
    for n, p, d in checks:
        results.append((suite, n, bool(p), d))

# 1. chaos ------------------------------------------------------------------------
c1 = build_cfg(schema_prefix="dpo_chaos_", source_mode="synthetic", end_date="2026-10-27")
m = synth.generate(spark, c1, "small", seed=7, end_date="2026-10-27", chaos=True)
ctx = pipeline.run_all(spark, c1, run_id="nb-chaos")
record("chaos", validation.run_all_checks(ctx, injected=m["injected"]))

# 2. missing sources ------------------------------------------------------------------
c2 = build_cfg(schema_prefix="dpo_missing_", source_mode="synthetic", end_date="2026-10-27")
synth.generate(spark, c2, "small", seed=3, end_date="2026-10-27", drop_sources=("node_timeline", "table_inventory"))
ctx = pipeline.run_all(spark, c2, run_id="nb-missing")
record("missing_sources", validation.run_all_checks(
    ctx, skip_rules=("R01_COMPUTE_IDLE", "R02_COMPUTE_OVERSIZED", "R15_TABLE_SMALL_FILES")))

# 3. empty window ---------------------------------------------------------------------
c3 = build_cfg(schema_prefix="dpo_missing_", source_mode="synthetic", end_date="2031-01-01")
ctx = pipeline.run_all(spark, c3, run_id="nb-empty")
record("empty_window", [("all_stages_succeeded", all(l["status"] == "SUCCESS" for l in ctx.log), str(ctx.log))])

# 4. volume ---------------------------------------------------------------------------
scale = dbutils.widgets.get("volume_scale")
c4 = build_cfg(schema_prefix="dpo_scale_", source_mode="synthetic", end_date="2026-10-27",
               lookback_days={"medium": 30, "large": 60, "xl": 90}[scale])
t = time.time()
m = synth.generate(spark, c4, scale, seed=99, end_date="2026-10-27")
gen_s = time.time() - t
ctx = pipeline.run_all(spark, c4, run_id=f"nb-scale-{scale}")
rows = sum(m["tables"].values())
for l in ctx.log:
    perf.append((scale, l["stage"], float(l["seconds"]), rows))
perf.append((scale, "generate", gen_s, rows))
record(f"volume_{scale}", validation.run_all_checks(ctx))

# report ------------------------------------------------------------------------------
res = (spark.createDataFrame(results, "suite string, check string, passed boolean, detail string")
       .withColumn("run_ts", F.current_timestamp()))
res.write.mode("append").saveAsTable(cfg.t("ops", "test_results"))
pdf = (spark.createDataFrame(perf, "scale string, stage string, seconds double, sim_rows long")
       .withColumn("run_ts", F.current_timestamp()))
pdf.write.mode("append").saveAsTable(cfg.t("ops", "perf_results"))
display(res)
display(pdf)

failed = [r for r in results if not r[2]]
assert not failed, f"{len(failed)} stress checks failed: {failed}"
print(f"All {len(results)} stress checks passed on {rows:,} synthetic rows")
