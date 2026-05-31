"""
silver.load_fx_rates
--------------------
FX feeds are sparse (no weekend/holiday quotes). Recon needs a rate for EVERY
business_date and currency, so we forward-fill the last known rate to a daily grain.
Using the as-of rate (not today's) is what makes restatement reproducible: re-running
2026-03-15 next year still applies the rate that was in effect on 2026-03-15.

Source: control/fx_raw (currency, rate_date, rate_to_reporting) — sparse.
Output: silver.fx_rate_daily — dense daily grain.
"""
from __future__ import annotations
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common import config as C
from src.common import io as IO
from src.common import spark as SP


def run(spark: SparkSession, rate_date: str) -> None:
    raw = spark.read.format("delta").load(C.control("fx_raw"))
    # build a calendar from min(rate_date) to rate_date
    bounds = raw.agg(F.min("rate_date").alias("lo")).collect()[0]
    cal = spark.sql(
        f"SELECT explode(sequence(to_date('{bounds.lo}'), to_date('{rate_date}'), interval 1 day)) AS rate_date")
    currencies = raw.select("currency").distinct()
    grid = cal.crossJoin(currencies)

    joined = grid.join(raw, ["currency", "rate_date"], "left")
    w = Window.partitionBy("currency").orderBy("rate_date") \
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    filled = joined.withColumn(
        "rate_to_reporting",
        F.last("rate_to_reporting", ignorenulls=True).over(w))

    IO.merge_upsert(filled.select("currency", "rate_date", "rate_to_reporting"),
                    C.silver("fx_rate_daily"),
                    keys=["currency", "rate_date"])


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--rate-date", required=True)
    a = ap.parse_args()
    spark = SP.session("silver_load_fx_rates")
    run(spark, a.rate_date)


if __name__ == "__main__":
    main()
