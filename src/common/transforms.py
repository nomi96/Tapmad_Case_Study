"""
common.transforms
-----------------
Currency + timezone conformance. Two recurring sources of "false breaks":

  * Timezone: operator timestamps are operator-LOCAL. The platform (and Finance)
    reason in UTC. A renewal at 23:30 Karachi time on the 1st is 18:30 UTC on the
    1st, but a naive read could land it on a different calendar day -> a "missing"
    break on both sides. We convert to UTC first, THEN derive business_date.

  * Currency: operators bill in local currency. Recon math (variance, totals)
    must be in ONE reporting currency. We join an as-of FX rate keyed on
    (currency, business_date). Using the as-of rate (not today's rate) is what
    makes a re-run of an old day reproduce the same numbers -> restatement-safe.
"""
from __future__ import annotations
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


def to_utc(ts_col: Column, tz_col_or_literal) -> Column:
    """Convert an operator-local timestamp to UTC.

    tz can be a column (per-row tz from the feed) or a string literal (operator tz).
    Spark's to_utc_timestamp interprets ts as wall-clock time in the given zone.
    """
    tz = tz_col_or_literal if isinstance(tz_col_or_literal, Column) else F.lit(tz_col_or_literal)
    return F.to_utc_timestamp(ts_col, tz)


def business_date(ts_utc_col: Column, reporting_tz: str = "UTC") -> Column:
    """Calendar day in the reporting timezone. business_date is the recon grain."""
    local = F.from_utc_timestamp(ts_utc_col, reporting_tz)
    return F.to_date(local)


def add_fx(df: DataFrame, fx_df: DataFrame, reporting_currency: str = "USD") -> DataFrame:
    """Left-join an as-of FX rate and compute amount_reporting.

    fx_df schema: (currency string, rate_date date, rate_to_reporting decimal(18,8))
    'as-of' = the most recent rate on or before business_date. We pre-explode fx_df
    to a daily grain upstream (forward-filled) so this is a clean equi-join and
    avoids a skew-prone range join at scale.

    Rows already in reporting_currency get fx_rate = 1.0.
    Rows whose currency has NO rate get fx_rate = NULL and a data-quality flag so
    they surface as a controlled break instead of silently becoming 0.
    """
    j = (df.join(fx_df,
                 (df.currency_original == fx_df.currency) &
                 (df.business_date == fx_df.rate_date),
                 "left")
           .drop(fx_df.currency).drop(fx_df.rate_date))

    rate = (F.when(F.col("currency_original") == F.lit(reporting_currency), F.lit(1.0))
             .otherwise(F.col("rate_to_reporting")))
    return (j.withColumn("fx_rate", rate.cast("decimal(18,8)"))
             .withColumn("reporting_currency", F.lit(reporting_currency))
             .withColumn("amount_reporting",
                         (F.col("amount_original") * F.col("fx_rate")).cast("decimal(18,4)"))
             .withColumn("fx_missing", F.col("fx_rate").isNull())
             .drop("rate_to_reporting"))


def record_hash(*cols: str) -> Column:
    """Stable hash over business columns -> change detection for idempotent MERGE."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]), 256)
