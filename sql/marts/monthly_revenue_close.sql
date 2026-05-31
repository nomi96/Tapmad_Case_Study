-- =============================================================================
-- Monthly revenue close  (the number Finance books)
-- =============================================================================
-- Design principles:
--   * Money source of truth = PARTNER side (that's where cash actually moved).
--   * Revenue = matched + amount_mismatch (we recognise the partner amount and
--     flag the variance) ; refunds are contra-revenue (subtract).
--   * missing_at_partner is NOT revenue (entitlement granted, no money) -> it is
--     a leakage line Finance reviews, not booked.
--   * missing_on_platform IS cash received -> booked, flagged for entitlement fix.
--   * Closed-month adjustments (late arrivals) are booked into adj_period, never
--     back into the closed month -> :period numbers are reproducible forever.
--   * Every figure carries an audit trail: drill from the close line to the break
--     fact to the silver event to the bronze raw row (source_file + record_hash).
--
-- Param: :period  e.g. '2026-05'
-- =============================================================================

-- 1) Headline close: recognised revenue per operator for the period -----------
WITH period_breaks AS (
  SELECT *
  FROM recon.gold.fact_reconciliation_break
  WHERE period_month = :period
    AND is_closed_month = false           -- in-period rows
  UNION ALL
  SELECT *
  FROM recon.gold.fact_reconciliation_break
  WHERE adj_period = :period
    AND is_closed_month = true            -- late arrivals booked INTO this period
),
classified AS (
  SELECT
    operator_code,
    break_category,
    txn_type,
    -- recognised cash = partner amount where partner has the money record
    CASE
      WHEN break_category IN ('matched','amount_mismatch','late_arrival') THEN partner_amount
      WHEN break_category = 'missing_on_platform'                        THEN partner_amount
      WHEN break_category = 'orphan_churn'                               THEN partner_amount
      ELSE 0
    END AS recognised_partner_cash,
    internal_amount,
    variance
  FROM period_breaks
)
SELECT
  operator_code,
  -- gross recognised revenue (subscription + recursion), refunds netted out
  ROUND(SUM(CASE WHEN txn_type IN ('subscription_success','recursion_success')
                 THEN recognised_partner_cash ELSE 0 END), 2)            AS gross_revenue,
  ROUND(SUM(CASE WHEN txn_type = 'refund'
                 THEN recognised_partner_cash ELSE 0 END), 2)            AS refunds,
  ROUND(SUM(CASE WHEN txn_type IN ('subscription_success','recursion_success')
                 THEN recognised_partner_cash ELSE 0 END)
        - SUM(CASE WHEN txn_type = 'refund'
                   THEN recognised_partner_cash ELSE 0 END), 2)          AS net_revenue,
  -- control totals: how much of net revenue sits behind an unresolved break
  ROUND(SUM(CASE WHEN break_category <> 'matched' THEN ABS(variance) ELSE 0 END), 2)
                                                                          AS variance_under_review,
  SUM(CASE WHEN break_category = 'missing_on_platform' THEN 1 ELSE 0 END) AS missing_on_platform_cnt,
  SUM(CASE WHEN break_category = 'missing_at_partner'  THEN 1 ELSE 0 END) AS leakage_cnt,
  SUM(CASE WHEN break_category = 'orphan_churn'        THEN 1 ELSE 0 END) AS orphan_churn_cnt
FROM classified
GROUP BY operator_code
ORDER BY operator_code;


-- 2) Audit trail: drill any close figure back to source ------------------------
--    Given an operator + category, list the individual breaks with full lineage.
-- Param: :period, :operator
SELECT
  f.break_id,
  f.business_date,
  f.break_category,
  f.txn_type,
  f.partner_amount,
  f.internal_amount,
  f.variance,
  f.match_method,
  f.match_confidence,
  -- partner-side lineage
  p.partner_txn_id        AS partner_txn_id,
  p.source_file           AS partner_source_file,
  p.record_hash           AS partner_record_hash,
  p.txn_ts_utc            AS partner_txn_ts_utc,
  -- platform-side lineage
  i.sub_id                AS platform_sub_id,
  i.source_file           AS platform_source_file,
  i.record_hash           AS platform_record_hash,
  i.txn_ts_utc            AS platform_txn_ts_utc,
  -- late-arrival provenance
  f.is_late, f.lag_days, f.original_business_date, f.adj_period
FROM recon.gold.fact_reconciliation_break f
LEFT JOIN recon.silver.partner_txn  p ON f.partner_event_id  = p.event_id
LEFT JOIN recon.silver.platform_txn i ON f.platform_event_id = i.event_id
WHERE (f.period_month = :period OR f.adj_period = :period)
  AND f.operator_code = :operator
ORDER BY f.break_category, ABS(f.variance) DESC;


-- 3) Reconciliation control check: does the mart tie to the fact? --------------
--    A close should never ship if the summary mart and the detail fact disagree.
SELECT
  d.business_date, d.operator_code,
  d.partner_amount_total                              AS mart_partner_total,
  f.partner_total                                     AS fact_partner_total,
  d.partner_amount_total - f.partner_total            AS tie_out_diff
FROM recon.gold.reconciliation_daily d
JOIN (
  SELECT business_date, operator_code,
         SUM(COALESCE(partner_amount,0)) AS partner_total
  FROM recon.gold.fact_reconciliation_break
  GROUP BY business_date, operator_code
) f ON d.business_date = f.business_date AND d.operator_code = f.operator_code
WHERE ABS(d.partner_amount_total - f.partner_total) > 0.01;   -- expect zero rows
