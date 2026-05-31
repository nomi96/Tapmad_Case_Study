"""
bronze.ingest_oltp_cdc
----------------------
The internal OLTP exposes, PER OPERATOR, its own pair of tables:
    sub_initial_{op}, sub_recursion_success_{op}, sub_recursion_failure_{op}
plus the global user_churn_events. They arrive as CDC parquet in landing/oltp/.

This module answers the case-study question:
  "How do you join across operator-suffixed tables (sub_initial_telco_a, ...) —
   UNION ALL at read time, dynamic table discovery, or schema-on-read?"

Answer: DYNAMIC DISCOVERY + UNION ALL with operator_code injected as a column.
  * We discover the operator set from config (enabled_operators), not by scraping
    the filesystem, so an operator can be on/offboarded by a config flip and we
    never accidentally pick up a half-loaded table.
  * Each per-operator table is read, stamped with operator_code, and UNION ALL'd
    into one logical table per ENTITY (sub_initial, recursion_success, ...).
  * unionByName(allowMissingColumns=True) tolerates minor per-operator column
    drift without failing the whole batch.

CDC dedup: CDC can deliver multiple versions of a PK (insert then update). We keep
the latest by the CDC commit timestamp (`_commit_ts`) / operation, so bronze holds
the current state per PK. Deletes (op = 'D') are tombstoned, not dropped, so
recon can see that a platform row was retracted.
"""
from __future__ import annotations
import argparse
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.common import config as C
from src.common import io as IO
from src.common import spark as SP

ENTITIES = ["sub_initial", "sub_recursion_success", "sub_recursion_failure"]
PK = {
    "sub_initial": "sub_id",
    "sub_recursion_success": "recursion_id",
    "sub_recursion_failure": "failure_id",
}


def _read_operator_table(spark: SparkSession, entity: str, op: str, load_date: str) -> DataFrame | None:
    table = f"{entity}_{op}"
    path = C.landing_oltp(table, load_date)
    try:
        df = spark.read.parquet(path)
    except Exception:
        # operator may not have delivered this entity today; skip gracefully
        return None
    return df.withColumn("operator_code", F.lit(op))


def _union_entity(spark: SparkSession, entity: str, operators: list[str], load_date: str) -> DataFrame | None:
    parts = [d for op in operators
             if (d := _read_operator_table(spark, entity, op, load_date)) is not None]
    if not parts:
        return None
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)


def _dedupe_cdc(df: DataFrame, pk: str) -> DataFrame:
    """Keep latest CDC version per (operator_code, pk). Tombstone deletes."""
    from pyspark.sql.window import Window
    # CDC metadata columns are conventionally _commit_ts and _op (I/U/D).
    commit = F.col("_commit_ts") if "_commit_ts" in df.columns else F.current_timestamp()
    df = df.withColumn("_commit_ts", commit)
    w = Window.partitionBy("operator_code", pk).orderBy(F.col("_commit_ts").desc_nulls_last())
    latest = df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
    op = F.col("_op") if "_op" in df.columns else F.lit("U")
    return latest.withColumn("is_deleted", (op == F.lit("D")))


def ingest(spark: SparkSession, load_date: str) -> None:
    cfg = C.load_operators()
    operators = C.enabled_operators(cfg)

    # per-entity operator-suffixed tables -> one unioned bronze table per entity
    for entity in ENTITIES:
        unioned = _union_entity(spark, entity, operators, load_date)
        if unioned is None:
            continue
        pk = PK[entity]
        deduped = (_dedupe_cdc(unioned, pk)
                   .withColumn("source_file", F.input_file_name())
                   .withColumn("file_arrival_date", F.to_date(F.lit(load_date)))
                   .withColumn("ingest_ts", F.current_timestamp())
                   .withColumn("record_hash",
                               F.sha2(F.to_json(F.struct([c for c in unioned.columns])), 256)))
        IO.merge_upsert(
            deduped,
            target_path=C.bronze(entity),
            keys=["operator_code", pk],
            partition_by=["operator_code"],
        )

    # global churn table (not operator-suffixed)
    try:
        churn = (spark.read.parquet(C.landing_oltp("user_churn_events", load_date))
                 .withColumn("source_file", F.input_file_name())
                 .withColumn("file_arrival_date", F.to_date(F.lit(load_date)))
                 .withColumn("ingest_ts", F.current_timestamp()))
        churn = churn.withColumn(
            "record_hash",
            F.sha2(F.concat_ws("||", "user_id", "operator_code",
                               F.col("churn_ts").cast("string"), "churn_reason"), 256))
        IO.merge_upsert(churn, C.bronze("user_churn_events"),
                        keys=["user_id", "operator_code", "churn_ts"],
                        partition_by=["operator_code"])
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--load-date", required=True)
    a = ap.parse_args()
    spark = SP.session("bronze_oltp_cdc")
    ingest(spark, a.load_date)


if __name__ == "__main__":
    main()
