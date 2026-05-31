"""
bronze.ingest_operator_feeds
-----------------------------
Land raw operator files into bronze Delta AS-IS (schema-on-read), adding only
lineage metadata. We do NOT normalize here — bronze is the immutable, replayable
record of exactly what each operator sent, including their re-sends.

Why keep raw shape in bronze?
  * If we discover a mapping bug in silver next month, we re-derive silver from
    bronze without re-fetching from SFTP (the files may be gone).
  * Auditability: Finance can trace a number all the way back to "the bytes the
    operator sent on this date".

Idempotency: bronze is keyed on (operator_code, source_file, raw_row_hash). Re-
running the same file is a no-op MERGE. A re-sent CORRECTION (same partner_txn_id,
different content) is a NEW bronze row (different hash) carrying a later
file_arrival_date; silver decides which version wins.

Run (Databricks job, one task per operator, parameterized):
    python -m src.bronze.ingest_operator_feeds --operator telco_a --arrival 2026-05-29
"""
from __future__ import annotations
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.common import config as C
from src.common import io as IO
from src.common import spark as SP


def _read_raw(spark: SparkSession, spec: dict, path: str) -> DataFrame:
    fmt = spec["file_format"]
    if fmt == "csv":
        opts = spec.get("csv_options", {})
        reader = (spark.read.option("header", str(opts.get("header", True)).lower())
                            .option("sep", opts.get("delimiter", ","))
                            .option("quote", opts.get("quote", '"'))
                            .option("mode", "PERMISSIVE")               # keep bad rows, flag later
                            .option("columnNameOfCorruptRecord", "_corrupt"))
        # everything as string in bronze; typing happens in silver
        return reader.csv(path, inferSchema=False)
    if fmt == "json":
        multiline = spec.get("json_options", {}).get("multiline", False)
        return spark.read.option("multiLine", str(multiline).lower()).json(path)
    raise ValueError(f"Unsupported file_format: {fmt}")


def ingest(spark: SparkSession, operator_code: str, arrival_date: str) -> None:
    cfg = C.load_operators()
    spec = cfg["operators"][operator_code]
    src_path = C.landing_operator(operator_code, arrival_date)

    raw = _read_raw(spark, spec, src_path)

    # flatten nested JSON to top-level string columns so bronze is rectangular and
    # the silver mapper can address fields by the dotted paths in operators.yaml.
    raw = raw.select([F.col(c).cast("string").alias(c) for c in raw.columns]) \
        if spec["file_format"] == "csv" else raw

    enriched = (raw
        .withColumn("operator_code", F.lit(operator_code))
        .withColumn("source_file", F.input_file_name())
        .withColumn("file_arrival_date", F.to_date(F.lit(arrival_date)))
        .withColumn("ingest_ts", F.current_timestamp())
        # hash of the ORIGINAL columns only (exclude metadata) for dedup
        .withColumn("raw_row_hash",
                    F.sha2(F.to_json(F.struct(*[c for c in raw.columns])), 256)))

    IO.merge_upsert(
        enriched,
        target_path=C.bronze(f"operator_{operator_code}"),
        keys=["operator_code", "raw_row_hash"],
        partition_by=["file_arrival_date"],
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--operator", required=True)
    ap.add_argument("--arrival", required=True, help="file_arrival_date YYYY-MM-DD")
    a = ap.parse_args()
    spark = SP.session(f"bronze_operator_{a.operator}")
    ingest(spark, a.operator, a.arrival)


if __name__ == "__main__":
    main()
