-- =============================================================================
-- Tapmad Reconciliation — Delta table DDL (Databricks SQL / Spark SQL)
-- Catalog/schema names assume Unity Catalog: recon.<zone>.<table>
-- Paths use ADLS Gen2 abfss locations (LOCATION clause) so tables are external
-- and survive cluster/metastore changes.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS recon;
CREATE SCHEMA  IF NOT EXISTS recon.bronze;
CREATE SCHEMA  IF NOT EXISTS recon.silver;
CREATE SCHEMA  IF NOT EXISTS recon.gold;
CREATE SCHEMA  IF NOT EXISTS recon.control;

-- ---------- SILVER: canonical partner & platform transactions ---------------
CREATE TABLE IF NOT EXISTS recon.silver.partner_txn (
  event_id           STRING,
  source_system      STRING,                 -- 'partner'
  operator_code      STRING,
  partner_txn_id     STRING,                 -- nullable
  account_id         STRING,
  user_id            STRING,                 -- null until bridged
  sub_id             STRING,
  plan_code          STRING,
  txn_type           STRING,
  billing_cycle      INT,
  amount_original    DECIMAL(18,4),
  currency_original  STRING,
  amount_reporting   DECIMAL(18,4),
  reporting_currency STRING,
  fx_rate            DECIMAL(18,8),
  fx_missing         BOOLEAN,
  txn_ts_utc         TIMESTAMP,
  business_date      DATE,
  failure_reason     STRING,
  dq_unmapped_type   BOOLEAN,
  source_file        STRING,
  file_arrival_date  DATE,
  record_hash        STRING,
  ingest_ts          TIMESTAMP,
  is_correction      BOOLEAN
) USING DELTA
PARTITIONED BY (business_date)
LOCATION 'abfss://recon@tapmadrecon.dfs.core.windows.net/silver/partner_txn'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

CREATE TABLE IF NOT EXISTS recon.silver.platform_txn (
  event_id           STRING,
  source_system      STRING,                 -- 'platform'
  operator_code      STRING,
  partner_txn_id     STRING,
  account_id         STRING,
  user_id            STRING,
  sub_id             STRING,
  plan_code          STRING,
  txn_type           STRING,
  billing_cycle      INT,
  amount_original    DECIMAL(18,4),
  currency_original  STRING,
  amount_reporting   DECIMAL(18,4),
  reporting_currency STRING,
  fx_rate            DECIMAL(18,8),
  fx_missing         BOOLEAN,
  txn_ts_utc         TIMESTAMP,
  business_date      DATE,
  failure_reason     STRING,
  source_file        STRING,
  file_arrival_date  DATE,
  record_hash        STRING,
  ingest_ts          TIMESTAMP,
  is_correction      BOOLEAN
) USING DELTA
PARTITIONED BY (business_date)
LOCATION 'abfss://recon@tapmadrecon.dfs.core.windows.net/silver/platform_txn'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- account <-> user bridge (enables fallback matching when partner_txn_id null)
-- SCD TYPE 2: an account/MSISDN can be reassigned to a different user over time
-- (number recycling, account transfer). Historical fallback must resolve to whoever
-- the account mapped to AT THE TXN DATE, so we keep effective-dated versions.
CREATE TABLE IF NOT EXISTS recon.silver.account_user_bridge (
  operator_code  STRING,
  account_id     STRING,
  user_id        STRING,
  sub_id         STRING,
  effective_from DATE,                  -- SCD2: version validity start (inclusive)
  effective_to   DATE,                  -- SCD2: validity end (exclusive); 9999-12-31 if current
  is_current     BOOLEAN,               -- SCD2: latest version flag
  record_hash    STRING,                -- hash over tracked attrs (user_id, sub_id)
  ingest_ts      TIMESTAMP
) USING DELTA
PARTITIONED BY (operator_code)
LOCATION 'abfss://recon@tapmadrecon.dfs.core.windows.net/silver/account_user_bridge'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- plan dimension. SCD TYPE 2: a plan's price/currency/billing terms change over
-- time. To validate a transaction amount we need the price IN EFFECT at the txn's
-- date -- never the latest price -- otherwise re-running a closed day would pick up
-- a newer price and silently change an already-published number. Point-in-time join:
--   txn.business_date BETWEEN effective_from AND (effective_to - 1 day).
CREATE TABLE IF NOT EXISTS recon.silver.dim_plan (
  plan_sk        STRING,                -- surrogate key (hash of natural key + effective_from)
  operator_code  STRING,
  plan_code      STRING,                -- natural key (with operator_code)
  price_original DECIMAL(18,4),
  currency       STRING,
  billing_period STRING,                -- monthly | weekly | daily ...
  effective_from DATE,
  effective_to   DATE,                  -- 9999-12-31 if current
  is_current     BOOLEAN,
  record_hash    STRING,                -- hash over (price_original, currency, billing_period)
  ingest_ts      TIMESTAMP
) USING DELTA
PARTITIONED BY (operator_code)
LOCATION 'abfss://recon@tapmadrecon.dfs.core.windows.net/silver/dim_plan'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- daily FX rates, forward-filled to daily grain (currency, rate_date) -> reporting
CREATE TABLE IF NOT EXISTS recon.silver.fx_rate_daily (
  currency          STRING,
  rate_date         DATE,
  rate_to_reporting DECIMAL(18,8)
) USING DELTA
LOCATION 'abfss://recon@tapmadrecon.dfs.core.windows.net/silver/fx_rate_daily'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- ---------- GOLD: break fact + daily mart -----------------------------------
CREATE TABLE IF NOT EXISTS recon.gold.fact_reconciliation_break (
  break_id              STRING,        -- PK (deterministic)
  business_date         DATE,
  operator_code         STRING,
  partner_txn_id        STRING,
  partner_event_id      STRING,        -- FK -> silver.partner_txn.event_id
  platform_event_id     STRING,        -- FK -> silver.platform_txn.event_id
  sub_id                STRING,
  account_id            STRING,
  p_user_id             STRING,
  i_user_id             STRING,
  txn_type              STRING,
  partner_amount        DECIMAL(18,4),
  internal_amount       DECIMAL(18,4),
  variance              DECIMAL(18,4),
  expected_amount       DECIMAL(18,4),  -- plan price in effect at business_date (dim_plan SCD2), in reporting ccy
  plan_price_ok         BOOLEAN,        -- did the money side match the contemporaneous plan price?
  plan_code             STRING,         -- plan resolved for this txn (drives the point-in-time price lookup)
  break_category        STRING,        -- matched|amount_mismatch|missing_on_platform|
                                       -- missing_at_partner|orphan_churn|late_arrival
  match_method          STRING,        -- partner_txn_id | fallback_identity | fallback_below_floor | unmatched
  match_confidence      DOUBLE,         -- 1.0 exact; fallback score (>=0.80 matched); rejected-candidate score for fallback_below_floor
  is_late               BOOLEAN,
  lag_days              INT,
  is_closed_month       BOOLEAN,
  original_business_date DATE,         -- set when row is a closed-month adjustment
  adj_period            STRING,        -- yyyy-MM the adjustment is booked into
  period_month          STRING,
  recon_run_ts          TIMESTAMP
) USING DELTA
PARTITIONED BY (business_date, operator_code)
LOCATION 'abfss://recon@tapmadrecon.dfs.core.windows.net/gold/fact_reconciliation_break'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

CREATE TABLE IF NOT EXISTS recon.gold.reconciliation_daily (
  business_date              DATE,
  operator_code              STRING,
  partner_txn_count          BIGINT,
  internal_txn_count         BIGINT,
  matched_count              BIGINT,
  break_count                BIGINT,
  partner_amount_total       DECIMAL(18,4),
  internal_amount_total      DECIMAL(18,4),
  variance                   DECIMAL(18,4),
  amount_mismatch_count      BIGINT,
  missing_on_platform_count  BIGINT,
  missing_at_partner_count   BIGINT,
  orphan_churn_count         BIGINT,
  late_arrival_count         BIGINT,
  recon_run_ts               TIMESTAMP
) USING DELTA
PARTITIONED BY (business_date, operator_code)
LOCATION 'abfss://recon@tapmadrecon.dfs.core.windows.net/gold/reconciliation_daily'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- ---------- CONTROL: period close + restatement audit -----------------------
CREATE TABLE IF NOT EXISTS recon.control.recon_period_control (
  period_month STRING,                 -- 'yyyy-MM'
  status       STRING,                 -- 'open' | 'closed'
  closed_at    TIMESTAMP,
  closed_by    STRING
) USING DELTA
LOCATION 'abfss://recon@tapmadrecon.dfs.core.windows.net/_control/recon_period_control'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

CREATE TABLE IF NOT EXISTS recon.control.recon_run_log (
  business_date DATE,
  operator_code STRING,
  break_count   BIGINT,
  variance      DECIMAL(18,4),
  recon_run_ts  TIMESTAMP,
  run_kind      STRING
) USING DELTA
LOCATION 'abfss://recon@tapmadrecon.dfs.core.windows.net/_control/recon_run_log'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- plan catalog (source feed for dim_plan SCD2): one row per plan price version.
CREATE TABLE IF NOT EXISTS recon.control.plan_catalog (
  operator_code  STRING,
  plan_code      STRING,
  price_original DECIMAL(18,4),
  currency       STRING,
  billing_period STRING,
  effective_from DATE
) USING DELTA
LOCATION 'abfss://recon@tapmadrecon.dfs.core.windows.net/_control/plan_catalog'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

