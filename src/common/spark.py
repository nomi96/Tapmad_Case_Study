"""
common.spark
------------
One place to build and tune the Spark session, so every entry point gets the same
performance configuration.

Tuning rationale (this workload is ~50k-500k rows/day/operator, i.e. megabytes, so
the costs that matter are SHUFFLES and SMALL FILES, not raw compute):

* Adaptive Query Execution (AQE) -- coalesces post-shuffle partitions and handles
  skewed joins at runtime, and lets Spark pick broadcast joins based on the *actual*
  measured size rather than stale estimates.
* autoBroadcastJoinThreshold bumped to 64 MB -- at our volumes almost every dimension
  (bridge, dim_plan, fx, churn) and even a day's txn table is broadcast-eligible, which
  removes the shuffle from those joins entirely.
* shuffle.partitions kept modest -- the default 200 produces hundreds of tiny
  partitions for MB-scale data (all scheduling overhead, no parallelism gain). AQE
  coalesces anyway, but a sane starting point helps.
* Delta optimizeWrite + autoCompact -- the pipeline MERGEs/overwrites every day, which
  is a classic small-file generator; these compact on write so reads stay fast without
  a separate OPTIMIZE job.

We deliberately do NOT cache/repartition broadly: at this scale the inputs are cheap to
re-scan, Spark prunes columns and partitions automatically for Delta, and pinning
frames in memory would cost more than it saves.
"""
from __future__ import annotations
from pyspark.sql import SparkSession

_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": str(64 * 1024 * 1024),  # 64 MB
    "spark.sql.shuffle.partitions": "64",
    # Delta small-file management (Databricks/Fabric)
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
}


def tune(spark: SparkSession) -> SparkSession:
    """Apply the performance configuration to an existing session (idempotent)."""
    for k, v in _CONF.items():
        try:
            spark.conf.set(k, v)
        except Exception:
            # some confs are static / unavailable off-Databricks; ignore gracefully
            pass
    return spark


def session(app_name: str) -> SparkSession:
    """Build (or get) a tuned Spark session for a job entry point."""
    builder = SparkSession.builder.appName(app_name)
    for k, v in _CONF.items():
        builder = builder.config(k, v)
    return tune(builder.getOrCreate())
