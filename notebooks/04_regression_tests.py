# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Regression tests (in the workspace)
# MAGIC
# MAGIC **Purpose.** Proves that a change did not alter results. Generates the fixed synthetic data set (seed 42, end date
# MAGIC 2026-10-27), runs the full pipeline into isolated `dpo_test_*` schemas and compares with the golden snapshot in
# MAGIC `tests/golden/small_seed42.json`: findings per rule, total cost, order of the top 10.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Who runs it** | operators or admins; the installer runs it on dev profiles; the `dpo-tests` job |
# MAGIC | **Where** | Free Edition and enterprise **dev** only. Exits on profiles that do not allow test schemas (production) |
# MAGIC | **Reads** | nothing real: synthetic data only |
# MAGIC | **Writes** | `dpo_test_*` schemas, `ops.test_results` (appended) |
# MAGIC | **Result** | the notebook (and the job) **fails** if any check fails |
# MAGIC
# MAGIC The same checks run locally and in CI with `pytest`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared bootstrap
# MAGIC Loads the DPO package, detects the profile, creates the shared widgets and builds `cfg` (see `_common`).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Run the reference pipeline and compare with the golden snapshot
# MAGIC Recall/precision checks against the injected ground truth, data-quality and governance checks, then the three golden
# MAGIC comparisons.

# COMMAND ----------

import json
import os

from pyspark.sql import functions as F

from dpo import pipeline, synth, validation

if cfg.governance["enforce"] and not cfg.governance.get("operators_can_create_schemas"):
    # test suites create extra dpo_*test schemas: never in a production catalog
    dbutils.notebook.exit(f"profile '{cfg.profile}' does not allow test schemas; run the tests in a dev workspace")


tcfg = build_cfg(schema_prefix="dpo_test_", source_mode="synthetic", end_date="2026-10-27", lookback_days=30)
synth.generate(spark, tcfg, scale="small", seed=42, end_date="2026-10-27")
ctx = pipeline.run_all(spark, tcfg, run_id="nb-regression")

results = validation.run_all_checks(ctx)

with open(os.path.join(REPO_ROOT, "tests", "golden", "small_seed42.json")) as fh:
    golden = json.load(fh)
snap = validation.snapshot(ctx)
results += [
    (
        "golden_findings_by_rule",
        snap["findings_by_rule"] == golden["findings_by_rule"],
        json.dumps(snap["findings_by_rule"]),
    ),
    (
        "golden_total_cost",
        abs(snap["total_cost"] - golden["total_cost"]) < 0.05,
        f"{snap['total_cost']} vs {golden['total_cost']}",
    ),
    ("golden_top10_order", snap["top10_finding_ids"] == golden["top10_finding_ids"], ""),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 · Record and assert
# MAGIC Appends every check to `ops.test_results` and fails the notebook if any check failed.

# COMMAND ----------

res_df = (
    spark.createDataFrame([(n, bool(p), d) for n, p, d in results], "check string, passed boolean, detail string")
    .withColumn("suite", F.lit("regression"))
    .withColumn("run_ts", F.current_timestamp())
)
res_df.write.mode("append").saveAsTable(cfg.t("ops", "test_results"))
display(res_df)

failed = [r for r in results if not r[1]]
assert not failed, f"{len(failed)} regression checks failed: {failed}"
print(f"All {len(results)} regression checks passed")
