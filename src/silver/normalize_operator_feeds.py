"""
silver.normalize_operator_feeds
-------------------------------
Turn each operator's bronze (raw, operator-shaped) into the ONE canonical
partner_txn schema, driven entirely by config/operators.yaml.

This is the answer to: "How do you normalize 6-7 differently-shaped operator feeds
into one canonical schema, and where does that live?"  -> Here, in silver, driven
by a declarative column_map + txn_type_map per operator. No per-operator code.

Per-operator quirks handled from config:
  * nested JSON paths (transaction.id) via dotted-path extraction
  * minor-unit amounts (amount_scale: 0.01)
  * decimal comma ("12,50")
  * epoch-millis / custom date formats / iso8601 timestamps
  * per-operator timezone -> UTC -> business_date
  * negative-amount-means-refund operators
  * operators with NO partner_txn_id (left null -> forces fallback match downstream)

Output: silver.partner_txn (Delta, partitioned by business_date), one conformed
row per operator transaction, deduped/idempotent via merge_upsert.
"""
from __future__ import annotations
import argparse
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F

from src.common import config as C
from src.common import io as IO
from src.common import spark as SP
from src.common import transforms as T

CANON_TYPES = {"subscription_success", "recursion_success",
               "recursion_failure", "refund", "cancel"}


def _col_from_path(path: str) -> Column:
    """Address a possibly-nested field. 'a.b.c' -> col('a')['b']['c']."""
    if "." not in path:
        return F.col(f"`{path}`")
    head, *rest = path.split(".")
    c = F.col(f"`{head}`")
    for r in rest:
        c = c.getField(r)
    return c


def _parse_ts(raw: Column, spec: dict) -> Column:
    fmt = spec.get("ts_format")
    if fmt == "epoch_millis":
        return (F.col("_ts_raw").cast("long") / 1000).cast("timestamp")
    if fmt == "iso8601":
        return F.to_timestamp("_ts_raw")  # Spark parses ISO-8601 (incl. offset)
    if fmt:  # explicit pattern, e.g. dd/MM/yyyy HH:mm:ss
        return F.to_timestamp("_ts_raw", fmt)
    return F.to_timestamp("_ts_raw")      # default ISO-ish


def _parse_amount(spec: dict) -> Column:
    amt = F.col("_amt_raw")
    if spec.get("decimal_comma"):
        amt = F.regexp_replace(amt, r"\.", "")          # remove thousands dots
        amt = F.regexp_replace(amt, r",", ".")          # comma -> decimal point
    amt = amt.cast("decimal(18,4)")
    scale = spec.get("amount_scale")
    if scale:
        amt = (amt * F.lit(scale)).cast("decimal(18,4)")
    return amt


def normalize_one(spark: SparkSession, operator_code: str, fx_df: DataFrame,
                  reporting_currency: str) -> DataFrame:
    cfg = C.load_operators()
    spec = cfg["operators"][operator_code]
    cmap = spec["column_map"]

    bronze = spark.read.format("delta").load(C.bronze(f"operator_{operator_code}"))

    # pull mapped raw fields into temp columns
    df = bronze
    df = df.withColumn("_account_raw", _col_from_path(cmap["msisdn_or_account"]))
    df = df.withColumn("_type_raw",    _col_from_path(cmap["txn_type"]))
    df = df.withColumn("_plan_raw",    _col_from_path(cmap["plan_code"]))
    df = df.withColumn("_amt_raw",     _col_from_path(cmap["amount"]))
    df = df.withColumn("_ts_raw",      _col_from_path(cmap["txn_ts"]))
    df = df.withColumn("_ptxn_raw",
                       _col_from_path(cmap["partner_txn_id"]) if "partner_txn_id" in cmap else F.lit(None))
    df = df.withColumn("_ccy_raw",
                       _col_from_path(cmap["currency"]) if "currency" in cmap else F.lit(None))

    # canonical txn_type via the per-operator map (lowercased keys for safety)
    type_map = {str(k).lower(): v for k, v in spec["txn_type_map"].items()}
    map_expr = F.create_map(*sum(([F.lit(k), F.lit(v)] for k, v in type_map.items()), []))
    txn_type = map_expr[F.lower(F.col("_type_raw").cast("string"))]

    amount = _parse_amount(spec)

    # operators that encode refunds as negative billing rows
    if spec.get("negative_amount_means_refund"):
        txn_type = F.when((amount < 0) & (txn_type == "recursion_success"), F.lit("refund")) \
                    .otherwise(txn_type)
        amount = F.abs(amount)

    currency = F.coalesce(F.upper(F.col("_ccy_raw").cast("string")), F.lit(spec["currency"]))
    ts_utc = T.to_utc(_parse_ts(F.col("_ts_raw"), spec), spec["timezone"])
    bdate = T.business_date(ts_utc, reporting_tz="UTC")

    canon = (df.select(
                F.lit("partner").alias("source_system"),
                F.lit(operator_code).alias("operator_code"),
                F.col("_ptxn_raw").cast("string").alias("partner_txn_id"),
                F.col("_account_raw").cast("string").alias("account_id"),
                F.lit(None).cast("string").alias("user_id"),
                F.lit(None).cast("string").alias("sub_id"),
                F.col("_plan_raw").cast("string").alias("plan_code"),
                txn_type.alias("txn_type"),
                F.lit(None).cast("int").alias("billing_cycle"),
                amount.alias("amount_original"),
                currency.alias("currency_original"),
                ts_utc.alias("txn_ts_utc"),
                bdate.alias("business_date"),
                F.lit(None).cast("string").alias("failure_reason"),
                F.col("source_file"),
                F.col("file_arrival_date"),
                F.col("ingest_ts"),
            )
            # data-quality flag: unknown txn_type that didn't map
            .withColumn("dq_unmapped_type",
                        F.col("txn_type").isNull() | ~F.col("txn_type").isin(*CANON_TYPES)))

    canon = T.add_fx(canon, fx_df, reporting_currency)

    # deterministic event_id + record_hash
    canon = (canon
        .withColumn("record_hash",
                    T.record_hash("operator_code", "partner_txn_id", "account_id",
                                  "txn_type", "amount_reporting", "txn_ts_utc"))
        .withColumn("event_id",
                    F.sha2(F.concat_ws("||", F.lit("partner"), "operator_code",
                                       F.coalesce("partner_txn_id", "record_hash")), 256))
        .withColumn("is_correction", F.lit(False)))  # set by merge logic / file_arrival ordering
    return canon


def run(spark: SparkSession) -> None:
    cfg = C.load_operators()
    canon_cfg = C.load_canonical()
    reporting_ccy = cfg["defaults"]["reporting_currency"]
    fx_df = spark.read.format("delta").load(C.silver("fx_rate_daily"))

    for op in C.enabled_operators(cfg):
        df = normalize_one(spark, op, fx_df, reporting_ccy)
        IO.merge_upsert(
            df, C.silver("partner_txn"),
            keys=["event_id"],
            partition_by=["business_date"],
        )


def main():
    spark = SP.session("silver_normalize_operator_feeds")
    run(spark)


if __name__ == "__main__":
    main()
