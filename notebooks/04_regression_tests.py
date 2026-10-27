# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Regression tests (in-workspace)
# MAGIC Uses an isolated prefix (`dpo_test_`) so it never touches your real outputs.
# MAGIC Fixed seed + fixed end date → the findings must match `tests/golden/small_seed42.json`.
# MAGIC Fails the notebook (and therefore the job) on any failed check.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import json, os
from pyspark.sql import functions as F
from dpo import pipeline, synth, validation

tcfg = build_cfg(schema_prefix="dpo_test_", source_mode="synthetic", end_date="2026-10-27", lookback_days=30)
synth.generate(spark, tcfg, scale="small", seed=42, end_date="2026-10-27")
ctx = pipeline.run_all(spark, tcfg, run_id="nb-regression")

results = validation.run_all_checks(ctx)

with open(os.path.join(REPO_ROOT, "tests", "golden", "small_seed42.json")) as fh:
    golden = json.load(fh)
snap = validation.snapshot(ctx)
results += [
    ("golden_findings_by_rule", snap["findings_by_rule"] == golden["findings_by_rule"],
     json.dumps(snap["findings_by_rule"])),
    ("golden_total_cost", abs(snap["total_cost"] - golden["total_cost"]) < 0.05,
     f"{snap['total_cost']} vs {golden['total_cost']}"),
    ("golden_top10_order", snap["top10_finding_ids"] == golden["top10_finding_ids"], ""),
]

res_df = (spark.createDataFrame([(n, bool(p), d) for n, p, d in results], "check string, passed boolean, detail string")
          .withColumn("suite", F.lit("regression")).withColumn("run_ts", F.current_timestamp()))
res_df.write.mode("append").saveAsTable(cfg.t("ops", "test_results"))
display(res_df)

failed = [r for r in results if not r[1]]
assert not failed, f"{len(failed)} regression checks failed: {failed}"
print(f"All {len(results)} regression checks passed")
