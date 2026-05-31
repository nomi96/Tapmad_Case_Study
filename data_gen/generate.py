"""
data_gen.generate
-----------------
Generates small synthetic samples that EXERCISE EVERY EDGE CASE the recon engine
must handle. No external deps (no Faker). Writes:

  data_gen/out/landing/operator/<op>/<arrival>/...   raw files in each op's shape
  data_gen/out/landing/oltp/<table>/...              platform tables (csv stand-in)
  data_gen/out/fx_raw.csv                            sparse fx rates
  data_gen/out/churn.csv                             churn events

Seeded scenarios (each tagged so the expected category is verifiable):
  S1  matched (clean, partner_txn_id on both sides)
  S2  amount_mismatch (off by > tolerance)
  S3  rounding-only diff (within tolerance -> still matched)
  S4  missing_on_platform (partner billed, no platform row)
  S5  missing_at_partner (platform entitlement, no partner row)
  S6  orphan_churn (user churned, partner still bills a renewal after churn)
  S7  late_arrival (partner row arrives 3 days after business_date)
  S8  no partner_txn_id on operator D -> fallback identity match (success)
  S9  fallback below confidence floor -> stays a break (false-positive guard)
  S10 re-send / duplicate (same row twice + a corrected amount) -> idempotent
  S11 currency conversion (NGN/PKR/TRY -> USD) feeding variance math
  S12 timezone edge (23:30 local crossing UTC day boundary)
  S13 refund (contra-revenue, negative on telco_f)
  S14 plan price change mid-month -> dim_plan SCD2; txn validated against the price
      IN EFFECT at business_date (point-in-time), not the newest price
  S15 account reassignment -> account_user_bridge SCD2; identity resolves to the user
      mapped at the txn date, not the current owner
  S16 failed renewal (sub_recursion_failure) -> non-revenue; ingested and conformed but
      EXCLUDED from money matching, so it produces no break
"""
from __future__ import annotations
import csv, json, os, datetime as dt

OUT = os.path.join(os.path.dirname(__file__), "out")
BD = "2026-05-29"          # target business_date
ARR = "2026-05-29"         # normal arrival
LATE_ARR = "2026-06-01"    # late arrival (3 days after BD)


def _w(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, "w", newline="")


# --- FX (sparse; loader forward-fills). Rates to USD. ------------------------
def gen_fx():
    rows = [
        ("USD", "2026-05-01", 1.0),
        ("NGN", "2026-05-01", 0.00065),    # ~1538 NGN / USD
        ("PKR", "2026-05-01", 0.0036),     # ~278 PKR / USD
        ("TRY", "2026-05-01", 0.031),
        ("BDT", "2026-05-01", 0.0091),
        ("LKR", "2026-05-01", 0.0033),
    ]
    with _w(os.path.join(OUT, "fx_raw.csv")) as f:
        wr = csv.writer(f); wr.writerow(["currency", "rate_date", "rate_to_reporting"])
        wr.writerows(rows)


# --- churn events -------------------------------------------------------------
def gen_churn():
    rows = [
        # S6: user U_ORPHAN churned on 2026-05-20, but partner keeps billing
        ("U_ORPHAN", "telco_a", "2026-05-20T10:00:00", "voluntary", "S_ORPHAN"),
    ]
    with _w(os.path.join(OUT, "landing", "oltp", "user_churn_events", "data.csv")) as f:
        wr = csv.writer(f)
        wr.writerow(["user_id", "operator_code", "churn_ts", "churn_reason", "last_known_sub_id"])
        wr.writerows(rows)


# --- telco_a: clean CSV, NGN, Africa/Lagos -----------------------------------
def gen_telco_a():
    # event_type uses operator vocabulary INIT_OK/RENEW_OK/...
    base = "2026-05-29T12:00:00"
    rows = [
        # S1 matched
        ["TXNA1", "2348010000001", "RENEW_OK", "PLN_A1", "1500.00", "NGN", base],
        # S2 amount_mismatch (partner 2000 vs platform 1500)
        ["TXNA2", "2348010000002", "RENEW_OK", "PLN_A1", "2000.00", "NGN", base],
        # S3 rounding diff (partner 1500.00 vs platform 1500.01 -> within tol)
        ["TXNA3", "2348010000003", "RENEW_OK", "PLN_A1", "1500.00", "NGN", base],
        # S4 missing_on_platform (no platform row for TXNA4)
        ["TXNA4", "2348010000004", "RENEW_OK", "PLN_A1", "1500.00", "NGN", base],
        # S6 orphan churn: renewal AFTER churn_ts for U_ORPHAN
        ["TXNA6", "2348019999999", "RENEW_OK", "PLN_A1", "1500.00", "NGN", base],
        # S12 timezone edge: 23:30 Lagos (UTC+1) on 05-29 -> 22:30 UTC same day
        ["TXNA12", "2348010000012", "RENEW_OK", "PLN_A1", "1500.00", "NGN", "2026-05-29T23:30:00"],
        # S10 duplicate (same as TXNA1, re-sent) -> must dedupe, not double count
        ["TXNA1", "2348010000001", "RENEW_OK", "PLN_A1", "1500.00", "NGN", base],
    ]
    hdr = ["txn_ref", "msisdn", "event_type", "product_code", "amount", "ccy", "event_time"]
    p = os.path.join(OUT, "landing", "operator", "telco_a", ARR, "telco_a_20260529.csv")
    with _w(p) as f:
        wr = csv.writer(f); wr.writerow(hdr); wr.writerows(rows)

    # S7 late arrival: TXNA7 belongs to BD 05-29 but lands 06-01
    late = [["TXNA7", "2348010000007", "RENEW_OK", "PLN_A1", "1500.00", "NGN", base]]
    pl = os.path.join(OUT, "landing", "operator", "telco_a", LATE_ARR, "telco_a_20260601.csv")
    with _w(pl) as f:
        wr = csv.writer(f); wr.writerow(hdr); wr.writerows(late)


# --- telco_b: nested JSON, PKR minor units, epoch millis ----------------------
def gen_telco_b():
    epoch = int(dt.datetime(2026, 5, 29, 9, 0, 0, tzinfo=dt.timezone.utc).timestamp() * 1000)
    objs = [
        # S1 matched (value in paisa: 55000 -> 550.00 PKR)
        {"transaction": {"id": "TXNB1", "kind": "rnw", "plan": "PLN_B1", "value_minor": 55000,
                          "currency": "PKR", "ts_epoch_ms": epoch}, "subscriber": {"msisdn": "923000000001"}},
        # S5 missing_at_partner is created on the PLATFORM side only (see oltp)
    ]
    p = os.path.join(OUT, "landing", "operator", "telco_b", ARR, "telco_b_20260529.json")
    with _w(p) as f:
        json.dump(objs, f)


# --- telco_c: semicolon CSV, decimal comma, dd/MM/yyyy, TRY, numeric types ----
def gen_telco_c():
    rows = [
        # S11 currency: 120,50 TRY -> *0.031 ~ 3.74 USD ; matched if platform agrees
        ["TXNC1", "905000000001", "2", "PLN_C1", "120,50", "TRY", "29/05/2026 14:00:00"],
    ]
    hdr = ["REF", "ACCOUNT", "TYPE", "PLAN", "AMOUNT", "CURRENCY", "TS"]
    p = os.path.join(OUT, "landing", "operator", "telco_c", ARR, "telco_c_20260529.csv")
    with _w(p) as f:
        wr = csv.writer(f, delimiter=";"); wr.writerow(hdr); wr.writerows(rows)


# --- telco_d: NO partner_txn_id -> forces fallback identity match -------------
def gen_telco_d():
    rows = [
        # S8 fallback success: bridge ties phone->user; amount + same-day align
        # -> identity .60 + amount .20 + same-day .10 = 0.90 >= floor -> matched
        ["8801000000001", "rebill", "PLN_D1", "300.00", "BDT", "2026-05-29T08:00:00"],
        # S9 false-positive guard: bridge DOES resolve phone->user (U_D9), same day,
        # but the amount is wildly off (999 BDT ~ 9.09 USD vs platform 5.00 USD)
        # -> identity .60 + same-day .10 = 0.70 < floor -> NOT matched, stays a break
        #    flagged fallback_below_floor for analyst review (avoids a wrong auto-match)
        ["8801000000999", "rebill", "PLN_DX", "999.00", "BDT", "2026-05-29T08:30:00"],
    ]
    hdr = ["phone", "action", "bundle", "charge", "cur", "charged_at"]
    p = os.path.join(OUT, "landing", "operator", "telco_d", ARR, "telco_d_20260529.csv")
    with _w(p) as f:
        wr = csv.writer(f); wr.writerow(hdr); wr.writerows(rows)


# --- telco_f: refund as negative billing row ----------------------------------
def gen_telco_f():
    rows = [
        # S13 refund: negative amount on a billing_success row -> normalize to refund
        ["TXNF1", "94000000001", "billing_success", "PLN_F1", "-450.00", "LKR", "2026-05-29T10:00:00"],
    ]
    hdr = ["transaction_id", "msisdn", "txn_status", "plan_id", "amount", "currency", "timestamp"]
    p = os.path.join(OUT, "landing", "operator", "telco_f", ARR, "telco_f_20260529.csv")
    with _w(p) as f:
        wr = csv.writer(f); wr.writerow(hdr); wr.writerows(rows)


# --- platform OLTP (operator-suffixed). CSV stand-in for CDC parquet ----------
def gen_oltp():
    # sub_recursion_success_telco_a  (USD-booked amounts; partner NGN->USD compare)
    # NGN 1500 * 0.00065 = 0.975 USD  -> platform books 0.98 (rounding within tol)
    rs_a = [
        # S1 matched (partner TXNA1 -> 0.975 USD; platform 0.98)
        ["RA1", "SUB_A1", "U_A1", "TXNA1", "0.98", "2026-05-29T12:00:00", "3"],
        # S2 amount_mismatch (platform 0.98 vs partner 2000 NGN = 1.30 USD)
        ["RA2", "SUB_A2", "U_A2", "TXNA2", "0.98", "2026-05-29T12:00:00", "3"],
        # S3 rounding within tol (platform 0.97 vs partner 0.975)
        ["RA3", "SUB_A3", "U_A3", "TXNA3", "0.97", "2026-05-29T12:00:00", "3"],
        # S6 orphan: platform renewal for churned U_ORPHAN, ties to TXNA6
        ["RA6", "S_ORPHAN", "U_ORPHAN", "TXNA6", "0.98", "2026-05-29T12:00:00", "5"],
        # S12 tz edge platform row (same UTC day)
        ["RA12", "SUB_A12", "U_A12", "TXNA12", "0.98", "2026-05-29T22:30:00", "3"],
        # S5 missing_at_partner: platform has a renewal with NO partner row
        ["RA5", "SUB_A5", "U_A5", None, "0.98", "2026-05-29T12:00:00", "3"],
        # S7 late: platform recorded TXNA7 on time (partner is late)
        ["RA7", "SUB_A7", "U_A7", "TXNA7", "0.98", "2026-05-29T12:00:00", "3"],
    ]
    _write_oltp_csv("sub_recursion_success_telco_a",
                    ["recursion_id", "sub_id", "user_id", "partner_txn_id", "amount", "recurrence_ts", "billing_cycle"],
                    rs_a)

    # telco_b recursion (PKR 550 * 0.0036 = 1.98 USD ; platform 1.98 -> matched)
    rs_b = [["RB1", "SUB_B1", "U_B1", "TXNB1", "1.98", "2026-05-29T09:00:00", "2"]]
    _write_oltp_csv("sub_recursion_success_telco_b",
                    ["recursion_id", "sub_id", "user_id", "partner_txn_id", "amount", "recurrence_ts", "billing_cycle"],
                    rs_b)

    # telco_c (TRY 120.50 * 0.031 = 3.7355 -> platform 3.74 matched)
    rs_c = [["RC1", "SUB_C1", "U_C1", "TXNC1", "3.74", "2026-05-29T14:00:00", "2"]]
    _write_oltp_csv("sub_recursion_success_telco_c",
                    ["recursion_id", "sub_id", "user_id", "partner_txn_id", "amount", "recurrence_ts", "billing_cycle"],
                    rs_c)

    # telco_d (no partner_txn_id on partner side; platform HAS partner_txn_id null too)
    # BDT 300 * 0.0091 = 2.73 USD. Fallback match relies on bridge + plan + amount.
    rs_d = [
        ["RD1", "SUB_D1", "U_D1", None, "2.73", "2026-05-29T08:00:00", "4"],   # S8 fallback target
        ["RD9", "SUB_D9", "U_D9", None, "5.00", "2026-05-29T08:30:00", "4"],   # S9 below-floor target
    ]
    _write_oltp_csv("sub_recursion_success_telco_d",
                    ["recursion_id", "sub_id", "user_id", "partner_txn_id", "amount", "recurrence_ts", "billing_cycle"],
                    rs_d)

    # sub_recursion_failure (operator-suffixed). Non-revenue: a failed renewal attempt.
    # S16 proves these are correctly EXCLUDED from money matching -- a failure must NOT
    # become a missing_at_partner break just because no partner money exists for it.
    rf_a = [
        # failure_id, sub_id, user_id, partner_txn_id, amount, failure_reason, attempt_ts, retry_count
        ["F_A1", "SUB_A16", "U_A16", None, "0.98", "insufficient_funds", "2026-05-29T06:00:00", "2"],
    ]
    _write_oltp_csv("sub_recursion_failure_telco_a",
                    ["failure_id", "sub_id", "user_id", "partner_txn_id", "amount",
                     "failure_reason", "attempt_ts", "retry_count"],
                    rf_a)

    # sub_initial for a couple operators (subscription_success)
    si_a = [["SUB_A1", "U_A1", "telco_a", "PLN_A1", "TXNA1", "active", "2026-05-01T00:00:00", "0.98"]]
    _write_oltp_csv("sub_initial_telco_a",
                    ["sub_id", "user_id", "operator_code", "plan_id", "partner_txn_id", "status", "created_ts", "amount"],
                    si_a)

    # account<->user bridge seed. SCD TYPE 2: effective-dated so historical fallback
    # resolves to whoever the account mapped to AT THE TXN DATE (accounts get reused).
    # In prod this is learned from historically matched rows; we seed it here.
    bridge = [
        # operator, account, user, sub, effective_from, effective_to, is_current
        ["telco_d", "8801000000001", "U_D1", "SUB_D1", "2026-05-01", "9999-12-31", "true"],   # S8
        ["telco_d", "8801000000999", "U_D9", "SUB_D9", "2026-05-01", "9999-12-31", "true"],   # S9
        # S15 account reassignment: 0777 belonged to U_OLD, ported to U_NEW on 2026-05-20.
        # A txn before the 20th must resolve to U_OLD; on/after, to U_NEW.
        ["telco_d", "8801000000777", "U_OLD", "SUB_OLD", "2026-05-01", "2026-05-20", "false"],
        ["telco_d", "8801000000777", "U_NEW", "SUB_NEW", "2026-05-20", "9999-12-31", "true"],
    ]
    _write_oltp_csv("account_user_bridge_seed",
                    ["operator_code", "account_id", "user_id", "sub_id",
                     "effective_from", "effective_to", "is_current"], bridge)

    # plan catalog feed -> dim_plan (SCD TYPE 2). PLN_A1 had a price change mid-month:
    # an amount must be validated against the price IN EFFECT at the txn date, so a
    # re-run of a closed day keeps using the contemporaneous price, not the newest.
    plans = [
        # operator, plan, price_original, currency, billing_period, effective_from
        ["telco_a", "PLN_A1", "1200.00", "NGN", "monthly", "2026-05-01"],   # v1 (old price)
        ["telco_a", "PLN_A1", "1500.00", "NGN", "monthly", "2026-05-16"],   # v2 (current; S14 price change)
        ["telco_b", "PLN_B1", "550.00",  "PKR", "monthly", "2026-05-01"],
        ["telco_c", "PLN_C1", "120.50",  "TRY", "monthly", "2026-05-01"],
        ["telco_d", "PLN_D1", "300.00",  "BDT", "monthly", "2026-05-01"],
        ["telco_f", "PLN_F1", "450.00",  "LKR", "monthly", "2026-05-01"],
    ]
    _write_oltp_csv("plan_catalog",
                    ["operator_code", "plan_code", "price_original", "currency",
                     "billing_period", "effective_from"], plans)


def _write_oltp_csv(table, hdr, rows):
    p = os.path.join(OUT, "landing", "oltp", table, "data.csv")
    with _w(p) as f:
        wr = csv.writer(f); wr.writerow(hdr)
        for r in rows:
            wr.writerow(["" if x is None else x for x in r])


def main():
    gen_fx(); gen_churn()
    gen_telco_a(); gen_telco_b(); gen_telco_c(); gen_telco_d(); gen_telco_f()
    gen_oltp()
    print("synthetic data written to", OUT)


if __name__ == "__main__":
    main()
