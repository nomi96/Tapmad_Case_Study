"""
silver.build_platform_txn
-------------------------
Conform the internal OLTP entities into the same canonical schema as partner_txn,
so the reconciliation engine compares like-for-like.

Entities -> canonical txn_type:
    sub_initial            -> subscription_success (status in active/pending) ;
                              status='failed' rows become a non-revenue marker
    sub_recursion_success  -> recursion_success
    sub_recursion_failure  -> recursion_failure (carries failure_reason)
    (refund/cancel on the platform side are derived from churn + negative recursion
     if present; pure cancels also surface via churn events in the recon engine)

Also builds the ACCOUNT <-> USER bridge, which is what makes fallback matching
possible when partner_txn_id is missing:
  whenever a platform row DOES carry partner_txn_id, we can later learn the
  (operator_code, account_id) <-> (user_id, sub_id) linkage from the matched
  partner row. We persist a best-effort bridge table from historically matched
  pairs so future no-id rows can still be tied to a user.

Output:
  silver.platform_txn   (Delta, partitioned by business_date)
  silver.account_user_bridge (Delta) -- maintained by the recon engine, seeded here
"""
from __future__ import annotations
import argparse
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.common import config as C
from src.common import io as IO
from src.common import spark as SP
from src.common import transforms as T


def _platform_ts_business(df: DataFrame, ts_col: str) -> DataFrame:
    # OLTP timestamps are already UTC (CDC from a UTC-configured DB by convention).
    return (df.withColumn("txn_ts_utc", F.col(ts_col).cast("timestamp"))
              .withColumn("business_date", T.business_date(F.col(ts_col).cast("timestamp"), "UTC")))


def build(spark: SparkSession) -> None:
    cfg = C.load_operators()
    reporting_ccy = cfg["defaults"]["reporting_currency"]
    fx_df = spark.read.format("delta").load(C.silver("fx_rate_daily"))

    # ---- sub_initial -> subscription_success -------------------------------
    si = spark.read.format("delta").load(C.bronze("sub_initial")).where(~F.col("is_deleted"))
    si = _platform_ts_business(si, "created_ts")
    si_canon = si.select(
        F.lit("platform").alias("source_system"),
        F.col("operator_code"),
        F.col("partner_txn_id").cast("string"),
        F.lit(None).cast("string").alias("account_id"),
        F.col("user_id").cast("string"),
        F.col("sub_id").cast("string"),
        F.col("plan_id").cast("string").alias("plan_code"),
        F.lit("subscription_success").alias("txn_type"),
        F.lit(1).cast("int").alias("billing_cycle"),
        F.col("amount").cast("decimal(18,4)").alias("amount_original"),
        F.lit(reporting_ccy).alias("currency_original"),  # platform books in reporting ccy
        F.col("txn_ts_utc"), F.col("business_date"),
        F.lit(None).cast("string").alias("failure_reason"),
        F.col("source_file"), F.col("file_arrival_date"), F.col("ingest_ts"),
        F.col("status"),
    )
    # status='failed' initial is NOT revenue; mark it so it can't be a money break
    si_canon = si_canon.withColumn(
        "txn_type",
        F.when(F.col("status") == "failed", F.lit("recursion_failure")).otherwise(F.col("txn_type"))
    ).drop("status")

    # ---- recursion_success -------------------------------------------------
    rs = spark.read.format("delta").load(C.bronze("sub_recursion_success")).where(~F.col("is_deleted"))
    rs = _platform_ts_business(rs, "recurrence_ts")
    rs_canon = rs.select(
        F.lit("platform").alias("source_system"),
        F.col("operator_code"),
        F.col("partner_txn_id").cast("string"),
        F.lit(None).cast("string").alias("account_id"),
        F.col("user_id").cast("string"),
        F.col("sub_id").cast("string"),
        F.lit(None).cast("string").alias("plan_code"),
        F.lit("recursion_success").alias("txn_type"),
        F.col("billing_cycle").cast("int"),
        F.col("amount").cast("decimal(18,4)").alias("amount_original"),
        F.lit(reporting_ccy).alias("currency_original"),
        F.col("txn_ts_utc"), F.col("business_date"),
        F.lit(None).cast("string").alias("failure_reason"),
        F.col("source_file"), F.col("file_arrival_date"), F.col("ingest_ts"),
    )

    # ---- recursion_failure (no money, but needed for orphan/break context) -
    rf = spark.read.format("delta").load(C.bronze("sub_recursion_failure")).where(~F.col("is_deleted"))
    rf = _platform_ts_business(rf, "attempt_ts")
    rf_canon = rf.select(
        F.lit("platform").alias("source_system"),
        F.col("operator_code"),
        F.lit(None).cast("string").alias("partner_txn_id"),
        F.lit(None).cast("string").alias("account_id"),
        F.col("user_id").cast("string"),
        F.col("sub_id").cast("string"),
        F.lit(None).cast("string").alias("plan_code"),
        F.lit("recursion_failure").alias("txn_type"),
        F.lit(None).cast("int").alias("billing_cycle"),
        F.lit(0).cast("decimal(18,4)").alias("amount_original"),
        F.lit(reporting_ccy).alias("currency_original"),
        F.col("txn_ts_utc"), F.col("business_date"),
        F.col("failure_reason"),
        F.col("source_file"), F.col("file_arrival_date"), F.col("ingest_ts"),
    )

    union = reduce(lambda a, b: a.unionByName(b), [si_canon, rs_canon, rf_canon])
    union = T.add_fx(union, fx_df, reporting_ccy)
    union = (union
        .withColumn("record_hash",
                    T.record_hash("operator_code", "sub_id", "partner_txn_id",
                                  "txn_type", "amount_reporting", "txn_ts_utc"))
        .withColumn("event_id",
                    F.sha2(F.concat_ws("||", F.lit("platform"), "operator_code", "txn_type",
                                       F.coalesce("sub_id", "user_id"), "record_hash"), 256))
        .withColumn("is_correction", F.lit(False)))

    IO.merge_upsert(union, C.silver("platform_txn"),
                    keys=["event_id"], partition_by=["business_date"])

    # ---- maintain account<->user bridge (SCD TYPE 2) ------------------------
    # Learned from platform rows that DO carry a partner_txn_id: every exact-key
    # match teaches us an account_id<->user_id link, which strengthens fallback
    # matching for keyless operators over time. Effective-dated so a reassigned
    # account resolves point-in-time (the user it mapped to AT the txn date).
    bridge_seed = (union.where(F.col("partner_txn_id").isNotNull())
                        .select("operator_code", "partner_txn_id", "user_id", "sub_id")
                        .dropDuplicates())
    # join to partner_txn to discover the operator-side account_id for that txn
    ptxn = spark.read.format("delta").load(C.silver("partner_txn")) \
                .select("operator_code", "partner_txn_id", "account_id", "business_date")
    bridge = (bridge_seed.join(ptxn, ["operator_code", "partner_txn_id"], "inner")
                         .where(F.col("account_id").isNotNull())
                         .groupBy("operator_code", "account_id", "user_id", "sub_id")
                         # the link becomes effective from the first day we observed it
                         .agg(F.min("business_date").alias("effective_from"))
                         .withColumn("record_hash",
                                     T.record_hash("operator_code", "account_id", "user_id", "sub_id"))
                         .withColumn("ingest_ts", F.current_timestamp()))
    # Type-2 upsert: a changed account->user mapping closes out the old version and
    # opens a new one, rather than overwriting (which Type 1 merge_upsert would do).
    IO.scd2_merge(bridge, C.silver("account_user_bridge"),
                  keys=["operator_code", "account_id"], eff_col="effective_from",
                  partition_by=["operator_code"])


def main():
    spark = SP.session("silver_build_platform_txn")
    build(spark)


if __name__ == "__main__":
    main()
