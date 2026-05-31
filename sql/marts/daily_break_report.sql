-- =============================================================================
-- Daily break report — "the table Finance opens each morning"
-- =============================================================================
-- Param: :business_date  e.g. '2026-05-29'

-- A) One-line health summary per operator -------------------------------------
SELECT
  operator_code,
  partner_txn_count, internal_txn_count, matched_count, break_count,
  ROUND(partner_amount_total, 2)  AS partner_total,
  ROUND(internal_amount_total, 2) AS internal_total,
  ROUND(variance, 2)              AS variance,
  amount_mismatch_count, missing_on_platform_count, missing_at_partner_count,
  orphan_churn_count, late_arrival_count,
  ROUND(matched_count / NULLIF(matched_count + break_count, 0), 4) AS match_rate,
  recon_run_ts
FROM recon.gold.reconciliation_daily
WHERE business_date = :business_date
ORDER BY ABS(variance) DESC;          -- biggest money problems first


-- B) Drill-down: the actual offending rows, worst variance first --------------
SELECT
  operator_code, break_category, txn_type,
  partner_txn_id, sub_id, account_id,
  ROUND(partner_amount, 2)  AS partner_amount,
  ROUND(internal_amount, 2) AS internal_amount,
  ROUND(variance, 2)        AS variance,
  match_method, match_confidence, is_late, lag_days
FROM recon.gold.fact_reconciliation_break
WHERE business_date = :business_date
  AND break_category <> 'matched'
ORDER BY break_category, ABS(variance) DESC
LIMIT 500;


-- C) Fallback matches to manually review (false-positive guard) ---------------
--    Two buckets matter to an analyst:
--      * fallback_below_floor: a plausible identity was found but the amount did
--        NOT corroborate, so we refused to auto-match (left as a break on purpose).
--      * fallback_identity with a thin margin (< 0.90): auto-matched, but worth a
--        spot check because only identity + one corroborating signal cleared 0.80.
SELECT operator_code, partner_txn_id, sub_id, txn_type, break_category,
       partner_amount, internal_amount, match_method, match_confidence
FROM recon.gold.fact_reconciliation_break
WHERE business_date = :business_date
  AND ( match_method = 'fallback_below_floor'
        OR (match_method = 'fallback_identity' AND match_confidence < 0.90) )
ORDER BY match_confidence ASC;

-- D) Plan-price exceptions (SCD2 point-in-time) --------------------------------
--    expected_amount is the plan price that was IN EFFECT at business_date,
--    resolved from dim_plan via a Type-2 validity join. plan_price_ok = false means
--    the money side does not match the contemporaneous contracted price -- i.e. the
--    partner billed an amount inconsistent with the plan version then in force.
--    Because the price is resolved point-in-time, a mid-month price change does NOT
--    create a false exception when an earlier (possibly closed) day is re-run.
SELECT operator_code, partner_txn_id, sub_id, plan_code,
       partner_amount, internal_amount, expected_amount, break_category
FROM recon.gold.fact_reconciliation_break
WHERE business_date = :business_date
  AND plan_price_ok = false
ORDER BY ABS(COALESCE(partner_amount, internal_amount) - expected_amount) DESC;
