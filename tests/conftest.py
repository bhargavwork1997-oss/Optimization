"""Local Spark fixture configured like Databricks serverless (ANSI on, UTC)."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# PySpark 3.5 (the Databricks-compatible line pinned in requirements-dev.txt) breaks on Python 3.14+.
if sys.version_info >= (3, 14):
    pytest.exit(f"Python {sys.version.split()[0]} is not supported by PySpark 3.5. Use Python 3.11 "
                f"(CI version): python3.11 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt",
                returncode=4)
# Spark workers must use this interpreter, not whatever `python3` is first on PATH.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


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
