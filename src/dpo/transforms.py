"""Reusable, side-effect-free column helpers (safe under ANSI mode / serverless)."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def safe_div(num: Column, den: Column) -> Column:
    """Division that returns NULL instead of raising DIVIDE_BY_ZERO under ANSI."""
    return F.when(den.isNotNull() & (den != 0), num / den)


def unit_hash(*cols: Column, salt: int) -> Column:
    """Deterministic pseudo-random number in [0,1) from column values.

    Unlike rand(seed) it does not depend on partitioning, so synthetic data is
    byte-for-byte reproducible on a laptop and on serverless.
    """
    return F.pmod(F.xxhash64(*cols, F.lit(salt)), F.lit(1_000_000)) / F.lit(1_000_000.0)


def normalize_sql(col: Column) -> Column:
    """Lower-case; strip comments, string/number literals; collapse whitespace.

    Comments are removed because they can carry secrets or prompt-injection text,
    literals because they carry data values. Idempotent: normalize(normalize(x)) == normalize(x).
    """
    s = F.regexp_replace(col, r"(?s)/\*.*?\*/", " ")
    s = F.regexp_replace(s, r"--[^\n]*", " ")
    s = F.lower(s)
    s = F.regexp_replace(s, r"\bx'[0-9a-f]*'", "?")
    s = F.regexp_replace(s, r"'(?:[^'\\]|''|\\.)*'", "?")
    s = F.regexp_replace(s, r'"(?:[^"\\]|""|\\.)*"', "?")
    s = F.regexp_replace(s, r"\b\d+(\.\d+)?\b", "?")
    s = F.regexp_replace(s, r"\s+", " ")
    return F.trim(s)


def query_fingerprint(col: Column) -> Column:
    """Stable 16-char id for 'the same query with different literals'."""
    return F.substring(F.sha2(normalize_sql(col), 256), 1, 16)


def workload_key(job_id: Column, warehouse_id: Column, pipeline_id: Column,
                 cluster_id: Column, notebook_id: Column, product: Column) -> Column:
    return (F.when(job_id.isNotNull(), F.concat(F.lit("job:"), job_id))
             .when(warehouse_id.isNotNull(), F.concat(F.lit("warehouse:"), warehouse_id))
             .when(pipeline_id.isNotNull(), F.concat(F.lit("pipeline:"), pipeline_id))
             .when(cluster_id.isNotNull(), F.concat(F.lit("cluster:"), cluster_id))
             .when(notebook_id.isNotNull(), F.concat(F.lit("notebook:"), notebook_id))
             .otherwise(F.concat(F.lit("unattributed:"), F.coalesce(product, F.lit("UNKNOWN")))))


def workload_type(key: Column) -> Column:
    return F.substring_index(key, ":", 1)


# ---------------------------------------------------------------------------
# Schema-tolerant projection: real system tables evolve; synthetic twins may lag.
# A missing column becomes a typed NULL instead of failing the run.
# ---------------------------------------------------------------------------
def _has_path(schema: T.StructType, path: str) -> bool:
    cur: T.DataType = schema
    for part in path.split("."):
        if not isinstance(cur, T.StructType):
            return False
        match = [f for f in cur.fields if f.name.lower() == part.lower()]
        if not match:
            return False
        cur = match[0].dataType
    return True


def project(df: DataFrame, spec: list[tuple]) -> tuple[DataFrame, list[str]]:
    """spec = [(alias, path_or_list_of_paths, spark_type_string)].

    Returns the projected frame and the list of aliases that were missing.
    """
    cols, missing = [], []
    for alias, paths, dtype in spec:
        paths = paths if isinstance(paths, (list, tuple)) else [paths]
        present = [p for p in paths if _has_path(df.schema, p)]
        if not present:
            missing.append(alias)
            cols.append(F.lit(None).cast(dtype).alias(alias))
        else:
            cols.append(F.coalesce(*[F.col(p).cast(dtype) for p in present]).alias(alias))
    return df.select(*cols), missing


def split_valid(df: DataFrame, checks: dict[str, Column]) -> tuple[DataFrame, DataFrame]:
    """Return (valid, invalid). invalid carries a comma-separated dq_reason."""
    reasons = F.concat_ws(",", F.array(*[
        F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in checks.items()
    ]))
    tagged = df.withColumn("dq_reason", reasons)
    valid = tagged.filter(F.col("dq_reason") == "").drop("dq_reason")
    invalid = tagged.filter(F.col("dq_reason") != "")
    return valid, invalid


def checksum(df: DataFrame) -> tuple[int, int]:
    """(row_count, order-independent content hash) for regression snapshots."""
    # maps cannot be hashed in Spark; their JSON form can
    cols = [F.to_json(F.col(f.name)) if isinstance(f.dataType, T.MapType) else F.col(f.name)
            for f in df.schema.fields]
    row = df.select(F.xxhash64(*cols).alias("h")).agg(
        F.count(F.lit(1)).alias("n"), F.expr("bit_xor(h)").alias("x")).collect()[0]
    return int(row["n"]), int(row["x"] or 0)
