"""
gold.build_marts
----------------
Aggregate gold.fact_reconciliation_break into gold.reconciliation_daily — the
summary table Finance opens every morning, and the drill-down anchor.

reconciliation_daily grain: (business_date, operator_code)
  partner_txn_count, internal_txn_count, matched_count, break_count
  partner_amount_total, internal_amount_total, variance
  amount_mismatch_count, missing_on_platform_count, missing_at_partner_count,
  orphan_churn_count, late_arrival_count
  + recon_run_ts (lineage of which run produced these numbers)

The FK to fact_reconciliation_break is (business_date, operator_code): from any
summary cell Finance can drill straight to the individual offending rows.

Idempotency: rebuilt per business_date partition, guarded against closed months.
"""
from __future__ import annotations
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common import config as C
from src.common import io as IO
from src.common import spark as SP


def build(spark: SparkSession, business_date: str) -> None:
    fact = (spark.read.format("delta").load(C.gold("fact_reconciliation_break"))
            .where(F.col("business_date") == F.lit(business_date)))

    def cnt(cat):  # count rows in a break category
        return F.sum(F.when(F.col("break_category") == cat, 1).otherwise(0))

    # partner/internal txn counts = distinct events seen on each side
    daily = (fact.groupBy("business_date", "operator_code").agg(
        F.countDistinct("partner_event_id").alias("partner_txn_count"),
        F.countDistinct("platform_event_id").alias("internal_txn_count"),
        cnt("matched").alias("matched_count"),
        (cnt("amount_mismatch") + cnt("missing_on_platform") + cnt("missing_at_partner")
         + cnt("orphan_churn") + cnt("late_arrival")).alias("break_count"),
        F.sum(F.coalesce("partner_amount", F.lit(0))).alias("partner_amount_total"),
        F.sum(F.coalesce("internal_amount", F.lit(0))).alias("internal_amount_total"),
        cnt("amount_mismatch").alias("amount_mismatch_count"),
        cnt("missing_on_platform").alias("missing_on_platform_count"),
        cnt("missing_at_partner").alias("missing_at_partner_count"),
        cnt("orphan_churn").alias("orphan_churn_count"),
        cnt("late_arrival").alias("late_arrival_count"),
        F.max("recon_run_ts").alias("recon_run_ts"),
    ).withColumn("variance",
                 F.col("partner_amount_total") - F.col("internal_amount_total")))

    IO.overwrite_partition(daily, C.gold("reconciliation_daily"),
                           partition_by=["business_date", "operator_code"],
                           guard_closed=True)

    # append a run-log audit row (restatement trail)
    log = (daily.select("business_date", "operator_code", "break_count", "variance",
                        F.col("recon_run_ts"))
                .withColumn("run_kind", F.lit("daily_build")))
    IO.append_audit(log, C.control("recon_run_log"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--business-date", required=True)
    a = ap.parse_args()
    spark = SP.session("gold_build_marts")
    build(spark, a.business_date)


if __name__ == "__main__":
    main()
