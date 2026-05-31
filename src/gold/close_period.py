"""
gold.close_period
-----------------
Freeze an accounting month. After this runs, the closed-period guard in common.io
rejects any gold partition overwrite for that month; late arrivals are routed to
the current open month as adjustment rows instead. This is the mechanism behind
"re-run any past day with latest data without changing already-closed months".
"""
from __future__ import annotations
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from src.common import config as C
from src.common import spark as SP


def close(spark: SparkSession, period_month: str, who: str = "month_close_job") -> None:
    ctrl = C.control("recon_period_control")
    row = spark.createDataFrame(
        [(period_month, "closed", who)], ["period_month", "status", "closed_by"]
    ).withColumn("closed_at", F.current_timestamp())

    if not DeltaTable.isDeltaTable(spark, ctrl):
        row.write.format("delta").save(ctrl)
        return
    (DeltaTable.forPath(spark, ctrl).alias("t")
        .merge(row.alias("s"), "t.period_month = s.period_month")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--period", required=True)
    a = ap.parse_args()
    spark = SP.session("gold_close_period")
    close(spark, a.period)


if __name__ == "__main__":
    main()
