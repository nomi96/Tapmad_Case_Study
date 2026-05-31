"""
common.io
---------
Idempotency primitives. Three write patterns are used across the pipeline:

1. merge_upsert     - bronze/silver: re-sends and corrections converge to ONE row
                      per natural key, keeping the latest version. Safe to re-run.

2. overwrite_partition - gold daily mart: re-running a business_date replaces only
                      that day's partition (dynamic partition overwrite). Idempotent
                      per day. GUARDED by the closed-period control table.

3. append_audit     - run log: append-only, every (re)run leaves a trail.

The closed-period guard is the heart of "re-statable history without changing
already-closed months". Gold writes for a business_date inside a CLOSED month are
rejected; the orchestrator routes that data to the late-arrival adjustment path.
"""
from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from . import config as C


def _spark() -> SparkSession:
    return SparkSession.getActiveSession()


def merge_upsert(df: DataFrame, target_path: str, keys: list[str],
                 order_col: str = "ingest_ts", partition_by: list[str] | None = None) -> None:
    """Upsert keeping the latest row per natural key.

    `order_col` breaks ties when an operator re-sends a corrected row: the row with
    the greatest (file_arrival_date, ingest_ts) wins. We rank first so the MERGE
    source has exactly one row per key (Delta MERGE errors on multiple matches).
    """
    spark = _spark()
    from pyspark.sql.window import Window
    w = Window.partitionBy(*keys).orderBy(F.col("file_arrival_date").desc_nulls_last(),
                                          F.col(order_col).desc_nulls_last())
    deduped = (df.withColumn("_rn", F.row_number().over(w))
                 .where(F.col("_rn") == 1).drop("_rn"))

    if not DeltaTable.isDeltaTable(spark, target_path):
        writer = deduped.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)
        return

    tgt = DeltaTable.forPath(spark, target_path)
    cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    (tgt.alias("t")
        .merge(deduped.alias("s"), cond)
        # only overwrite when the incoming row is actually newer (idempotent re-runs)
        .whenMatchedUpdateAll(condition="s.record_hash <> t.record_hash")
        .whenNotMatchedInsertAll()
        .execute())


def overwrite_partition(df: DataFrame, target_path: str, partition_by: list[str],
                        guard_closed: bool = True) -> None:
    """Idempotent per-partition overwrite for gold marts.

    Dynamic partition overwrite => re-running business_date=D replaces ONLY D.
    Guarded so we never silently rewrite a closed accounting month.
    """
    spark = _spark()
    if guard_closed:
        assert_partitions_open(df)
    (df.write.format("delta")
       .mode("overwrite")
       .option("partitionOverwriteMode", "dynamic")
       .partitionBy(*partition_by)
       .option("mergeSchema", "false")
       .save(target_path))


def append_audit(df: DataFrame, target_path: str) -> None:
    df.write.format("delta").mode("append").save(target_path)


# ----- SCD Type 2 (effective-dated dimensions) -------------------------------
HIGH_DATE = "9999-12-31"


def scd2_merge(df: DataFrame, target_path: str, keys: list[str],
               eff_col: str = "effective_from", partition_by: list[str] | None = None) -> None:
    """Slowly-Changing-Dimension Type 2 upsert.

    `df` carries the natural `keys`, an `effective_from` date, and a `record_hash`
    over the *tracked* attributes (price, currency, billing_period, ...). When a
    tracked attribute changes for an existing key we close out the current version
    (set effective_to = new effective_from, is_current = false) and append a new
    current version; brand-new keys are appended directly. Re-running with an
    unchanged hash is a no-op, so the daily dimension load is idempotent.

    Type 2 (not Type 1) is required here precisely because reconciliation re-runs
    history: an amount must be validated against the plan price that was in effect
    *at the transaction's date*, not the latest price. A point-in-time join
    (business_date in [effective_from, effective_to)) gives that.
    """
    spark = _spark()
    src = (df.withColumn("is_current", F.lit(True))
             .withColumn("effective_to", F.lit(HIGH_DATE).cast("date")))

    if not DeltaTable.isDeltaTable(spark, target_path):
        writer = src.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)
        return

    tgt = DeltaTable.forPath(spark, target_path)
    keycond = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    # Phase 1 -- close out current rows whose tracked hash changed.
    (tgt.alias("t")
        .merge(src.alias("s"), f"{keycond} AND t.is_current = true")
        .whenMatchedUpdate(condition="t.record_hash <> s.record_hash",
                           set={"is_current": F.lit(False),
                                "effective_to": F.col(f"s.{eff_col}")})
        .execute())

    # Phase 2 -- append brand-new keys + the just-closed (changed) versions.
    # After Phase 1 a changed key has NO current row, so it falls through here.
    current = (spark.read.format("delta").load(target_path)
                    .where("is_current = true")
                    .select(*keys, F.col("record_hash").alias("_curhash")))
    new_versions = (src.join(current, keys, "left")
                       .where(F.col("_curhash").isNull() | (F.col("record_hash") != F.col("_curhash")))
                       .drop("_curhash"))
    new_versions.write.format("delta").mode("append").save(target_path)


def scd2_asof(spark, dim_path: str, keys: list[str], as_of_col: str = "business_date"):
    """Return a function that point-in-time joins a fact df to an SCD2 dimension:
    fact.<as_of_col> in [dim.effective_from, dim.effective_to). Used by the engine
    to resolve the plan price / account-user identity in effect at txn time."""
    dim = spark.read.format("delta").load(dim_path)

    def _join(fact: DataFrame, suffix: str = "_dim") -> DataFrame:
        cond = [fact[k] == dim[k] for k in keys]
        cond.append(fact[as_of_col] >= dim["effective_from"])
        cond.append(fact[as_of_col] < dim["effective_to"])
        return fact.join(dim, cond, "left")
    return _join


# ----- closed-period control -------------------------------------------------
def assert_partitions_open(df: DataFrame) -> None:
    """Raise if df contains business_dates that fall in a CLOSED month.

    The orchestrator should have split late-arriving closed-month rows into the
    adjustment path BEFORE calling overwrite_partition; this is the backstop.
    """
    spark = _spark()
    ctrl_path = C.control("recon_period_control")
    if not DeltaTable.isDeltaTable(spark, ctrl_path):
        return  # no control table yet => nothing closed
    closed = (spark.read.format("delta").load(ctrl_path)
                   .where(F.col("status") == "closed")
                   .select("period_month").distinct())
    offending = (df.select(F.date_format("business_date", "yyyy-MM").alias("period_month"))
                   .distinct().join(closed, "period_month", "inner"))
    n = offending.count()
    if n > 0:
        months = [r.period_month for r in offending.collect()]
        raise RuntimeError(
            f"Refusing to overwrite gold partitions in CLOSED month(s): {months}. "
            f"Route this data through the late-arrival adjustment path instead.")


def is_month_closed(spark: SparkSession, period_month: str) -> bool:
    ctrl_path = C.control("recon_period_control")
    if not DeltaTable.isDeltaTable(spark, ctrl_path):
        return False
    cnt = (spark.read.format("delta").load(ctrl_path)
                .where((F.col("period_month") == period_month) & (F.col("status") == "closed"))
                .count())
    return cnt > 0
