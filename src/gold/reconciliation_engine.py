"""
gold.reconciliation_engine
===========================
The core matching + classification logic. Compares partner_txn (money source of
truth) against platform_txn (entitlement source of truth) for a target
business_date, plus a lookback window to absorb late arrivals and re-sends.

THE DECISION TREE  (this is the "hard part" the case study calls out)
---------------------------------------------------------------------
For each (operator_code, business_date) we partition both sides and classify:

  STAGE 0  Pre-clean
           - drop non-comparable rows (recursion_failure carries no money; kept
             only for orphan-churn context, not money matching)
           - both sides already deduped & currency/tz-conformed in silver

  STAGE 1  EXACT MATCH on (operator_code, partner_txn_id)   [both non-null]
           - high confidence (confidence = 1.0)
           - compare amount_reporting within tolerance:
                |Δ| <= max(abs_tol, pct_tol * partner_amount)  -> MATCHED
                else                                           -> AMOUNT_MISMATCH
           - matched/​mismatched rows leave the unmatched pools

  STAGE 2  FALLBACK MATCH for rows with NO usable partner_txn_id
           (operator D has none; some platform rows have null FK)
           - resolve partner.account_id -> user_id via account_user_bridge
             (identity must resolve, else the row cannot fallback at all)
           - LOOSE candidate key: (operator_code, bridged user_id, txn_type,
             business_date ± window). Amount is deliberately NOT a join predicate
             so a weak pair still forms and can be scored/rejected.
           - additive confidence: identity .60 + amount-within-tol .20 + plan .10
             + same-day .10 (weights in canonical_schema.yaml)
           - greedy 1:1 assignment by (confidence desc, nearest timestamp)
           - below fallback_min_confidence (0.80) we DO NOT match: the row stays a
             break tagged match_method = fallback_below_floor for analyst review,
             rather than risk a false positive that hides real money movement.
             Identity alone (.60) or identity+same-day (.70) is below floor by
             design -- monetary corroboration is required to auto-match a no-key txn.

  STAGE 3  RESIDUAL CLASSIFICATION
           - partner-only leftover  -> MISSING_ON_PLATFORM (billed, no entitlement)
           - platform-only leftover -> MISSING_AT_PARTNER (entitlement, no money)

  STAGE 4  ORPHAN CHURN overlay (independent of money match)
           - user churned on platform (churn_ts <= business_date) BUT partner has
             a recursion_success after churn_ts -> ORPHAN_CHURN (still billing)
           - platform shows active recursion but partner sent cancel/no row and a
             churn exists at partner -> also ORPHAN_CHURN
           - orphan rows are re-tagged (they may also be a money break; orphan_churn
             takes precedence in the category so Product sees silent churn)

  STAGE 5  LATE ARRIVAL overlay
           - any matched/break row whose file_arrival_date - business_date >=
             late_arrival_days is tagged LATE_ARRIVAL.
           - If business_date's month is OPEN: it simply restates that day on the
             next run (idempotent overwrite). If CLOSED: it is emitted as an
             ADJUSTMENT row stamped with adj_period = current open month and
             original_business_date = the closed day -> closed numbers never move.

Output rows feed gold.fact_reconciliation_break (one row per partner/platform
pairing or singleton) which the daily mart aggregates.
"""
from __future__ import annotations
import argparse
import datetime as dt
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common import config as C
from src.common import io as IO
from src.common import spark as SP


# --------------------------------------------------------------------------
def _within_tolerance(p_amt: Column, i_amt: Column, abs_tol: float, pct_tol: float) -> Column:
    delta = F.abs(p_amt - i_amt)
    bound = F.greatest(F.lit(abs_tol), F.lit(pct_tol) * F.abs(p_amt))
    return delta <= bound


def _load_window(spark, path, op, start, end) -> DataFrame:
    return (spark.read.format("delta").load(path)
            .where((F.col("operator_code") == op) &
                   (F.col("business_date") >= F.lit(start)) &
                   (F.col("business_date") <= F.lit(end))))


# --------------------------------------------------------------------------
def reconcile_operator(spark: SparkSession, op: str, business_date: str) -> DataFrame:
    cfg = C.load_operators()
    rc = C.load_canonical()["reconciliation"]
    abs_tol = rc["amount_tolerance_abs"]
    pct_tol = rc["amount_tolerance_pct"]
    win = rc["fallback_match_window_days"]
    late_days = rc["late_arrival_days"]
    min_conf = rc["fallback_min_confidence"]
    fw = rc.get("fallback_weights", {})
    w_identity = fw.get("identity", 0.60)
    w_amount = fw.get("amount_within_tol", 0.20)
    w_plan = fw.get("plan_match", 0.10)
    w_sameday = fw.get("same_business_date", 0.10)
    money_types = rc["revenue_types"] + rc["contra_revenue_types"]

    bd = dt.date.fromisoformat(business_date)
    lo = (bd - dt.timedelta(days=win)).isoformat()
    hi = (bd + dt.timedelta(days=win)).isoformat()

    partner = _load_window(spark, C.silver("partner_txn"), op, lo, hi) \
        .where(F.col("txn_type").isin(*money_types))
    platform = _load_window(spark, C.silver("platform_txn"), op, lo, hi) \
        .where(F.col("txn_type").isin(*money_types))

    # keep only the target day's "anchor" rows in the result but allow window rows
    # to satisfy a match (a partner txn on bd may match a platform txn on bd-1).
    # ---- STAGE 1: exact match on partner_txn_id ----------------------------
    p_keyed = partner.where(F.col("partner_txn_id").isNotNull())
    i_keyed = platform.where(F.col("partner_txn_id").isNotNull())

    exact = (p_keyed.alias("p").join(
                i_keyed.alias("i"),
                (F.col("p.operator_code") == F.col("i.operator_code")) &
                (F.col("p.partner_txn_id") == F.col("i.partner_txn_id")),
                "inner")
             .select(
                F.col("p.operator_code").alias("operator_code"),
                F.col("p.partner_txn_id").alias("partner_txn_id"),
                F.col("p.event_id").alias("partner_event_id"),
                F.col("i.event_id").alias("platform_event_id"),
                F.col("p.user_id").alias("p_user_id"),
                F.col("i.user_id").alias("i_user_id"),
                F.col("i.sub_id").alias("sub_id"),
                F.col("p.account_id").alias("account_id"),
                F.col("p.txn_type").alias("txn_type"),
                F.col("p.business_date").alias("business_date"),
                F.col("i.business_date").alias("platform_business_date"),
                F.col("p.amount_reporting").alias("partner_amount"),
                F.col("i.amount_reporting").alias("internal_amount"),
                F.greatest(F.col("p.file_arrival_date"), F.col("i.file_arrival_date")).alias("file_arrival_date"),
                F.lit(1.0).alias("match_confidence"),
                F.lit("partner_txn_id").alias("match_method"),
             ))
    exact = exact.withColumn(
        "break_category",
        F.when(_within_tolerance(F.col("partner_amount"), F.col("internal_amount"), abs_tol, pct_tol),
               F.lit("matched")).otherwise(F.lit("amount_mismatch")))

    matched_p_ids = exact.select("partner_event_id").distinct()
    matched_i_ids = exact.select("platform_event_id").distinct()

    # ---- STAGE 2: fallback match (no usable partner_txn_id) ----------------
    # bridge is a small per-operator lookup -> broadcast to avoid a shuffle.
    bridge = F.broadcast(
        spark.read.format("delta").load(C.silver("account_user_bridge"))
             .where(F.col("operator_code") == op)
             .select("operator_code", "account_id", "user_id"))

    p_nokey = (partner.join(matched_p_ids,
                            partner.event_id == matched_p_ids.partner_event_id, "left_anti")
                      .where(F.col("partner_txn_id").isNull())              # only no-key rows
                      .join(bridge, ["operator_code", "account_id"], "left")  # resolve user
                      .where(F.col("user_id").isNotNull()))                 # identity must resolve
    i_nokey = platform.join(matched_i_ids,
                            platform.event_id == matched_i_ids.platform_event_id, "left_anti")

    pb = (p_nokey
          .select("operator_code",
                  F.col("user_id").alias("p_user_id"),
                  "account_id", "plan_code", "txn_type",
                  F.col("amount_reporting").alias("partner_amount"),
                  F.col("txn_ts_utc").alias("p_ts"),
                  F.col("business_date").alias("business_date"),
                  F.col("file_arrival_date").alias("p_arrival"),
                  F.col("event_id").alias("partner_event_id")))
    ib = (i_nokey
          .select("operator_code",
                  F.col("user_id").alias("i_user_id"),
                  "sub_id", "plan_code", "txn_type",
                  F.col("amount_reporting").alias("internal_amount"),
                  F.col("txn_ts_utc").alias("i_ts"),
                  F.col("business_date").alias("platform_business_date"),
                  F.col("event_id").alias("platform_event_id")))

    # Candidate join is intentionally LOOSE: bridged identity + same txn_type + within
    # window. Amount is NOT a join predicate -- a weak pair must still FORM so the
    # confidence floor can reject it. That rejection is the false-positive guard.
    cand = (pb.alias("p").join(
                ib.alias("i"),
                (F.col("p.operator_code") == F.col("i.operator_code")) &
                (F.col("p.p_user_id") == F.col("i.i_user_id")) &       # bridged identity
                (F.col("p.txn_type") == F.col("i.txn_type")) &
                (F.abs(F.datediff(F.col("p.business_date"), F.col("i.platform_business_date"))) <= win),
                "inner"))
    # confidence: identity(0.60) + amount-within-tol(0.20) + plan(0.10) + same-day(0.10)
    # identity alone (0.60) or identity+day (0.70) is below the 0.80 floor on purpose:
    # we require monetary corroboration before auto-matching a no-key txn.
    conf = F.round(
        F.lit(w_identity)
        + F.when(_within_tolerance(F.col("p.partner_amount"), F.col("i.internal_amount"), abs_tol, pct_tol),
                 w_amount).otherwise(0.0)
        + F.when((F.col("p.plan_code").isNotNull()) & (F.col("p.plan_code") == F.col("i.plan_code")),
                 w_plan).otherwise(0.0)
        + F.when(F.col("p.business_date") == F.col("i.platform_business_date"), w_sameday).otherwise(0.0), 3)
    cand = cand.withColumn("match_confidence", conf) \
               .withColumn("ts_gap", F.abs(F.col("p.p_ts").cast("long") - F.col("i.i_ts").cast("long")))

    # greedy 1:1: best candidate per partner row, then ensure each platform row used once
    wp = Window.partitionBy("p.partner_event_id").orderBy(F.col("match_confidence").desc(), F.col("ts_gap").asc())
    ranked = cand.withColumn("_rp", F.row_number().over(wp)).where(F.col("_rp") == 1)
    wi = Window.partitionBy("i.platform_event_id").orderBy(F.col("match_confidence").desc(), F.col("ts_gap").asc())
    ranked = ranked.withColumn("_ri", F.row_number().over(wi)).where(F.col("_ri") == 1)

    best = ranked.where(F.col("match_confidence") >= F.lit(min_conf))
    # rows that had a candidate but fell below the floor -> flagged for analyst review
    rejected = ranked.where(F.col("match_confidence") < F.lit(min_conf))
    rej_p = rejected.select(F.col("p.partner_event_id").alias("rej_event_id"),
                            F.col("match_confidence").alias("rej_conf"))
    rej_i = rejected.select(F.col("i.platform_event_id").alias("rej_event_id"),
                            F.col("match_confidence").alias("rej_conf"))

    fb = best.select(
        F.col("p.operator_code").alias("operator_code"),
        F.lit(None).cast("string").alias("partner_txn_id"),
        F.col("p.partner_event_id").alias("partner_event_id"),
        F.col("i.platform_event_id").alias("platform_event_id"),
        F.col("p.p_user_id").alias("p_user_id"),
        F.col("i.i_user_id").alias("i_user_id"),
        F.col("i.sub_id").alias("sub_id"),
        F.col("p.account_id").alias("account_id"),
        F.col("p.txn_type").alias("txn_type"),
        F.col("p.business_date").alias("business_date"),
        F.col("i.platform_business_date").alias("platform_business_date"),
        F.col("p.partner_amount").alias("partner_amount"),
        F.col("i.internal_amount").alias("internal_amount"),
        F.col("p.p_arrival").alias("file_arrival_date"),
        F.col("match_confidence"),
        F.lit("fallback_identity").alias("match_method"),
    )
    fb = fb.withColumn(
        "break_category",
        F.when(_within_tolerance(F.col("partner_amount"), F.col("internal_amount"), abs_tol, pct_tol),
               F.lit("matched")).otherwise(F.lit("amount_mismatch")))

    paired = exact.unionByName(fb)

    # ---- STAGE 3: residual singletons --------------------------------------
    used_p = paired.select("partner_event_id").distinct()
    used_i = paired.select("platform_event_id").distinct()

    partner_only = (partner.join(used_p, partner.event_id == used_p.partner_event_id, "left_anti")
                    .where(F.col("business_date") == F.lit(business_date))   # anchor on target day
                    .alias("po")
                    .join(F.broadcast(rej_p), F.col("po.event_id") == rej_p.rej_event_id, "left")
                    .select(
                        "operator_code", "partner_txn_id",
                        F.col("event_id").alias("partner_event_id"),
                        F.lit(None).cast("string").alias("platform_event_id"),
                        F.col("user_id").alias("p_user_id"),
                        F.lit(None).cast("string").alias("i_user_id"),
                        F.lit(None).cast("string").alias("sub_id"),
                        "account_id", "txn_type", "business_date",
                        F.lit(None).cast("date").alias("platform_business_date"),
                        F.col("amount_reporting").alias("partner_amount"),
                        F.lit(None).cast("decimal(18,4)").alias("internal_amount"),
                        "file_arrival_date",
                        F.coalesce(F.col("rej_conf"), F.lit(0.0)).alias("match_confidence"),
                        F.when(F.col("rej_conf").isNotNull(), F.lit("fallback_below_floor"))
                         .otherwise(F.lit("unmatched")).alias("match_method"),
                        F.lit("missing_on_platform").alias("break_category")))

    platform_only = (platform.join(used_i, platform.event_id == used_i.platform_event_id, "left_anti")
                     .where(F.col("business_date") == F.lit(business_date))
                     .alias("io")
                     .join(F.broadcast(rej_i), F.col("io.event_id") == rej_i.rej_event_id, "left")
                     .select(
                         "operator_code",
                         F.col("partner_txn_id"),
                         F.lit(None).cast("string").alias("partner_event_id"),
                         F.col("event_id").alias("platform_event_id"),
                         F.lit(None).cast("string").alias("p_user_id"),
                         F.col("user_id").alias("i_user_id"),
                         "sub_id",
                         F.lit(None).cast("string").alias("account_id"),
                         "txn_type", "business_date",
                         F.col("business_date").alias("platform_business_date"),
                         F.lit(None).cast("decimal(18,4)").alias("partner_amount"),
                         F.col("amount_reporting").alias("internal_amount"),
                         "file_arrival_date",
                         F.coalesce(F.col("rej_conf"), F.lit(0.0)).alias("match_confidence"),
                         F.when(F.col("rej_conf").isNotNull(), F.lit("fallback_below_floor"))
                          .otherwise(F.lit("unmatched")).alias("match_method"),
                         F.lit("missing_at_partner").alias("break_category")))

    # restrict paired rows to those anchored on the target business_date too
    paired_anchor = paired.where(F.col("business_date") == F.lit(business_date))
    all_rows = paired_anchor.unionByName(partner_only).unionByName(platform_only)

    # ---- STAGE 4: orphan churn overlay -------------------------------------
    churn = (spark.read.format("delta").load(C.bronze("user_churn_events"))
             .where((F.col("operator_code") == op) & (~F.col("is_deleted") if "is_deleted" in
                     spark.read.format("delta").load(C.bronze("user_churn_events")).columns else F.lit(True)))
             .select("user_id", F.col("churn_ts").cast("timestamp")))
    churn_min = churn.groupBy("user_id").agg(F.min("churn_ts").alias("churn_ts"))

    # a partner success after the platform churn timestamp = still billing a churned user
    # churn_min is one row per churned user -> broadcast.
    all_rows = (all_rows.join(F.broadcast(churn_min),
                              all_rows.i_user_id == churn_min.user_id, "left")
                .withColumn("is_orphan_churn",
                            (F.col("txn_type") == "recursion_success") &
                            F.col("churn_ts").isNotNull() &
                            (F.col("business_date") > F.to_date("churn_ts")))
                .drop("user_id", "churn_ts"))
    all_rows = all_rows.withColumn(
        "break_category",
        F.when(F.col("is_orphan_churn"), F.lit("orphan_churn")).otherwise(F.col("break_category")))

    # ---- STAGE 5: late arrival overlay -------------------------------------
    all_rows = all_rows.withColumn(
        "lag_days", F.datediff(F.col("file_arrival_date"), F.col("business_date")))
    all_rows = all_rows.withColumn(
        "is_late", F.col("lag_days") >= F.lit(late_days))
    # late_arrival is recorded as its own category ONLY for rows that would not
    # otherwise be a money break (a late row that is also an amount_mismatch keeps
    # the more severe money category; the is_late flag is still set for routing).
    all_rows = all_rows.withColumn(
        "break_category",
        F.when(F.col("is_late") & (F.col("break_category") == "matched"),
               F.lit("late_arrival")).otherwise(F.col("break_category")))

    # closed-month routing: stamp adj_period for closed-month late arrivals
    period_month = F.date_format("business_date", "yyyy-MM")
    closed = (spark.read.format("delta").load(C.control("recon_period_control"))
              if IO.DeltaTable.isDeltaTable(spark, C.control("recon_period_control")) else None)
    if closed is not None:
        closed = closed.where(F.col("status") == "closed").select(
            F.col("period_month").alias("_cm"))
        all_rows = (all_rows.withColumn("period_month", period_month)
                    .join(closed, F.col("period_month") == F.col("_cm"), "left")
                    .withColumn("is_closed_month", F.col("_cm").isNotNull())
                    .drop("_cm"))
    else:
        all_rows = all_rows.withColumn("period_month", period_month) \
                           .withColumn("is_closed_month", F.lit(False))

    today_month = dt.date.today().strftime("%Y-%m")
    all_rows = (all_rows
        .withColumn("original_business_date",
                    F.when(F.col("is_closed_month"), F.col("business_date")))
        .withColumn("adj_period",
                    F.when(F.col("is_closed_month"), F.lit(today_month)).otherwise(F.col("period_month")))
        .withColumn("recon_run_ts", F.current_timestamp())
        .withColumn("variance",
                    F.coalesce(F.col("partner_amount"), F.lit(0)) -
                    F.coalesce(F.col("internal_amount"), F.lit(0)))
        .withColumn("break_id",
                    F.sha2(F.concat_ws("||",
                        F.coalesce("partner_event_id", F.lit("∅")),
                        F.coalesce("platform_event_id", F.lit("∅")),
                        "business_date"), 256)))

    # ---- SCD2 point-in-time plan-price validation --------------------------
    # Resolve plan_code per row (by event id, from the canonical txn tables), look up
    # the plan price IN EFFECT at business_date from dim_plan (Type 2 validity join),
    # convert to reporting currency with the as-of FX rate, and flag whether the money
    # side matches the contemporaneous contracted price. A mid-month price change
    # therefore cannot create a false mismatch when an old day is re-run.
    plan_map = (spark.read.format("delta").load(C.silver("partner_txn"))
                .select(F.col("event_id").alias("_pe"), F.col("plan_code").alias("_p_plan"))
                .where(F.col("_p_plan").isNotNull()))
    plat_plan = (spark.read.format("delta").load(C.silver("platform_txn"))
                 .select(F.col("event_id").alias("_ie"), F.col("plan_code").alias("_i_plan"))
                 .where(F.col("_i_plan").isNotNull()))
    all_rows = (all_rows
                .join(plan_map, all_rows.partner_event_id == F.col("_pe"), "left")
                .join(plat_plan, all_rows.platform_event_id == F.col("_ie"), "left")
                .withColumn("plan_code", F.coalesce("_p_plan", "_i_plan"))
                .drop("_pe", "_ie", "_p_plan", "_i_plan"))

    if IO.DeltaTable.isDeltaTable(spark, C.silver("dim_plan")):
        # dim_plan and fx_rate_daily are small reference tables -> broadcast both,
        # turning these non-equi / equi joins into shuffle-free map-side joins.
        dim = F.broadcast(spark.read.format("delta").load(C.silver("dim_plan")))
        fx = F.broadcast(spark.read.format("delta").load(C.silver("fx_rate_daily")))
        priced = (all_rows.alias("r").join(
                    dim.alias("d"),
                    (F.col("r.operator_code") == F.col("d.operator_code")) &
                    (F.col("r.plan_code") == F.col("d.plan_code")) &
                    (F.col("r.business_date") >= F.col("d.effective_from")) &
                    (F.col("r.business_date") < F.col("d.effective_to")), "left")
                  .join(fx.alias("fx"),
                        (F.col("d.currency") == F.col("fx.currency")) &
                        (F.col("r.business_date") == F.col("fx.rate_date")), "left"))
        all_rows = (priced
            .withColumn("expected_amount",
                        F.round(F.col("d.price_original") *
                                F.coalesce(F.col("fx.rate_to_reporting"), F.lit(1.0)), 4))
            .withColumn("plan_price_ok",
                        F.when(F.col("expected_amount").isNull(), F.lit(None).cast("boolean"))
                         .otherwise(_within_tolerance(
                             F.coalesce(F.col("r.partner_amount"), F.col("r.internal_amount")),
                             F.col("expected_amount"), abs_tol, pct_tol)))
            .select("r.*", "expected_amount", "plan_price_ok"))
    else:
        all_rows = (all_rows
                    .withColumn("expected_amount", F.lit(None).cast("decimal(18,4)"))
                    .withColumn("plan_price_ok", F.lit(None).cast("boolean")))

    return all_rows


def run(spark: SparkSession, business_date: str) -> None:
    SP.tune(spark)   # ensure AQE / broadcast / Delta auto-compact are on
    cfg = C.load_operators()
    frames = [reconcile_operator(spark, op, business_date) for op in C.enabled_operators(cfg)]
    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f)
    # gold fact table: idempotent per business_date partition, guarded for closed months
    IO.overwrite_partition(result, C.gold("fact_reconciliation_break"),
                           partition_by=["business_date", "operator_code"],
                           guard_closed=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--business-date", required=True)
    a = ap.parse_args()
    spark = SP.session("gold_reconciliation_engine")
    run(spark, a.business_date)


if __name__ == "__main__":
    main()
