"""Local Spark fixture configured like Databricks serverless (ANSI on, UTC)."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession
    wh = tmp_path_factory.mktemp("warehouse")
    s = (SparkSession.builder.master("local[4]").appName("dpo-tests")
         .config("spark.sql.warehouse.dir", str(wh))
         .config("spark.sql.ansi.enabled", "true")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .config("spark.driver.extraJavaOptions", "-Derby.system.home=" + str(wh))
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def local_cfg(prefix="dpo_", **kw):
    from dpo.config import DPOConfig
    base = dict(catalog=None, schema_prefix=prefix, table_format="parquet", end_date="2026-10-27")
    base.update(kw)
    return DPOConfig(**base)
