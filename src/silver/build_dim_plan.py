"""
silver.build_dim_plan
---------------------
Builds the plan dimension as SLOWLY-CHANGING DIMENSION TYPE 2.

A plan's price / currency / billing terms change over time. Reconciliation re-runs
history, so a transaction must be validated against the price that was IN EFFECT at
its own date -- not the latest price. If we stored this Type 1 (overwrite), re-running
an already-closed day would pick up a newer price and silently change a published
number, breaking the restatement guarantee. Type 2 keeps every effective-dated version
and the engine resolves the right one with a point-in-time join:

    txn.business_date >= dim_plan.effective_from AND txn.business_date < dim_plan.effective_to

Source: control/plan_catalog (operator_code, plan_code, price_original, currency,
        billing_period, effective_from) -- one row per price version.
Output: silver.dim_plan -- effective-dated, is_current flagged, surrogate keyed.
"""
from __future__ import annotations
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common import config as C
from src.common import io as IO
from src.common import spark as SP
from src.common import transforms as T


def run(spark: SparkSession) -> None:
    cat = spark.read.format("delta").load(C.control("plan_catalog"))
    versioned = (cat
                 .withColumn("plan_sk",
                             T.record_hash("operator_code", "plan_code", "effective_from"))
                 .withColumn("record_hash",
                             T.record_hash("price_original", "currency", "billing_period"))
                 .withColumn("ingest_ts", F.current_timestamp())
                 .select("plan_sk", "operator_code", "plan_code", "price_original",
                         "currency", "billing_period", "effective_from",
                         "record_hash", "ingest_ts"))
    # scd2_merge sets effective_to / is_current and closes out superseded versions.
    IO.scd2_merge(versioned, C.silver("dim_plan"),
                  keys=["operator_code", "plan_code"], eff_col="effective_from",
                  partition_by=["operator_code"])


def main():
    spark = SP.session("silver_build_dim_plan")
    run(spark)


if __name__ == "__main__":
    argparse.ArgumentParser().parse_args()
    main()
