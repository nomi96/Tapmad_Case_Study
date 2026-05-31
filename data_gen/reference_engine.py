"""
data_gen.reference_engine
-------------------------
A pure-pandas mirror of the PySpark reconciliation pipeline, so the decision tree
can be RUN and verified without a Spark cluster. It is intentionally faithful to
src/silver/* and src/gold/reconciliation_engine.py:
  - config-driven normalization of each operator's native shape
  - per-operator timezone -> UTC -> business_date
  - FX to USD (as-of, forward-filled)
  - Stage 1 exact match -> Stage 2 fallback identity -> Stage 3 residual
  - Stage 4 orphan-churn overlay -> Stage 5 late-arrival overlay
  - idempotent dedup of re-sends

Outputs (to sample_output/):
  partner_txn.csv, platform_txn.csv,
  fact_reconciliation_break.csv, reconciliation_daily.csv,
  monthly_revenue_close.csv
"""
from __future__ import annotations
import os, json, glob, hashlib, datetime as dt
from zoneinfo import ZoneInfo
import yaml
import pandas as pd

HERE = os.path.dirname(__file__)
OUT = os.path.join(HERE, "out")
SAMPLE = os.path.join(os.path.dirname(HERE), "sample_output")
CFG = yaml.safe_load(open(os.path.join(os.path.dirname(HERE), "config", "operators.yaml")))
DEF = CFG["defaults"]
ABS_TOL = DEF["amount_tolerance_abs"]; PCT_TOL = DEF["amount_tolerance_pct"]
WIN = DEF["fallback_match_window_days"]; LATE = DEF["late_arrival_days"]
_CANON = yaml.safe_load(open(os.path.join(os.path.dirname(HERE), "config", "canonical_schema.yaml")))
_RC = _CANON["reconciliation"]
MIN_CONF = _RC["fallback_min_confidence"]
_FW = _RC.get("fallback_weights", {})
# fallback confidence weights (single source of truth = canonical_schema.yaml)
W_IDENTITY = _FW.get("identity", 0.60)   # bridge-resolved account->user (necessary, not sufficient)
W_AMOUNT = _FW.get("amount_within_tol", 0.20)
W_PLAN = _FW.get("plan_match", 0.10)
W_SAMEDAY = _FW.get("same_business_date", 0.10)
BD = "2026-05-29"
REV_TYPES = {"subscription_success", "recursion_success"}
MONEY_TYPES = REV_TYPES | {"refund"}


def sha(*xs):
    return hashlib.sha256("||".join("∅" if x is None else str(x) for x in xs).encode()).hexdigest()[:16]


def within_tol(p, i):
    if p is None or i is None:
        return False
    return abs(p - i) <= max(ABS_TOL, PCT_TOL * abs(p))


# ----- FX (forward-filled to daily) ------------------------------------------
def load_fx():
    fx = pd.read_csv(os.path.join(OUT, "fx_raw.csv"))
    fx["rate_date"] = pd.to_datetime(fx["rate_date"]).dt.date
    return {(r.currency): float(r.rate_to_reporting) for r in fx.itertuples()}  # one month, flat


FX = None
def to_usd(amount, ccy):
    if ccy == "USD":
        return round(float(amount), 4), 1.0
    rate = FX.get(ccy)
    if rate is None:
        return None, None
    return round(float(amount) * rate, 4), rate


# ----- generic nested getter -------------------------------------------------
def get_path(obj, path):
    cur = obj
    for part in path.split("."):
        if cur is None:
            return None
        cur = cur.get(part) if isinstance(cur, dict) else None
    return cur


# ----- normalize one operator -> canonical partner rows ----------------------
def normalize_operator(op, files_with_arrival):
    spec = CFG["operators"][op]
    cmap = spec["column_map"]
    tmap = {str(k).lower(): v for k, v in spec["txn_type_map"].items()}
    tz = ZoneInfo(spec["timezone"])
    rows = []
    for path, arrival in files_with_arrival:
        records = _read_raw(path, spec)
        for rec in records:
            ptxn = get_path(rec, cmap["partner_txn_id"]) if "partner_txn_id" in cmap else None
            account = get_path(rec, cmap["msisdn_or_account"])
            type_raw = get_path(rec, cmap["txn_type"])
            plan = get_path(rec, cmap["plan_code"])
            amt_raw = get_path(rec, cmap["amount"])
            ts_raw = get_path(rec, cmap["txn_ts"])
            ccy = get_path(rec, cmap["currency"]) if "currency" in cmap else None
            ccy = (ccy or spec["currency"]).upper()

            txn_type = tmap.get(str(type_raw).lower())
            amount = _parse_amount(amt_raw, spec)
            if spec.get("negative_amount_means_refund") and amount is not None and amount < 0 \
                    and txn_type == "recursion_success":
                txn_type = "refund"; amount = abs(amount)

            ts_utc = _parse_ts(ts_raw, spec, tz)
            bdate = ts_utc.date()
            amt_usd, rate = to_usd(amount, ccy)
            rh = sha(op, ptxn, account, txn_type, amt_usd, ts_utc.isoformat())
            event_id = sha("partner", op, ptxn if ptxn else rh)
            rows.append(dict(event_id=event_id, source_system="partner", operator_code=op,
                             partner_txn_id=ptxn, account_id=str(account), user_id=None, sub_id=None,
                             plan_code=plan, txn_type=txn_type, amount_original=amount,
                             currency_original=ccy, amount_reporting=amt_usd, fx_rate=rate,
                             txn_ts_utc=ts_utc, business_date=bdate, source_file=os.path.basename(path),
                             file_arrival_date=dt.date.fromisoformat(arrival), record_hash=rh))
    df = pd.DataFrame(rows)
    # idempotent dedup: keep latest by (file_arrival_date, event_id) per event_id
    df = (df.sort_values(["file_arrival_date"]).drop_duplicates("event_id", keep="last"))
    return df


def _read_raw(path, spec):
    fmt = spec["file_format"]
    if fmt == "json":
        txt = open(path).read().strip()
        if spec.get("json_options", {}).get("multiline", True) is False:
            return [json.loads(l) for l in txt.splitlines() if l.strip()]
        data = json.loads(txt)
        return data if isinstance(data, list) else [data]
    # csv
    delim = spec.get("csv_options", {}).get("delimiter", ",")
    df = pd.read_csv(path, sep=delim, dtype=str, keep_default_na=False)
    return df.to_dict("records")


def _parse_amount(raw, spec):
    if raw is None or raw == "":
        return None
    s = str(raw)
    if spec.get("decimal_comma"):
        s = s.replace(".", "").replace(",", ".")
    val = float(s)
    if spec.get("amount_scale"):
        val *= spec["amount_scale"]
    return round(val, 4)


def _parse_ts(raw, spec, tz):
    fmt = spec.get("ts_format")
    if fmt == "epoch_millis":
        return dt.datetime.fromtimestamp(int(raw) / 1000, tz=dt.timezone.utc)
    if fmt == "iso8601":
        d = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return d.astimezone(dt.timezone.utc) if d.tzinfo else d.replace(tzinfo=tz).astimezone(dt.timezone.utc)
    if fmt and fmt not in ("iso8601", "epoch_millis"):
        py = fmt.replace("dd", "%d").replace("MM", "%m").replace("yyyy", "%Y") \
                .replace("HH", "%H").replace("mm", "%M").replace("ss", "%S")
        d = dt.datetime.strptime(str(raw), py)
        return d.replace(tzinfo=tz).astimezone(dt.timezone.utc)
    # default ISO local
    d = dt.datetime.fromisoformat(str(raw))
    return d.replace(tzinfo=tz).astimezone(dt.timezone.utc) if d.tzinfo is None else d.astimezone(dt.timezone.utc)


# ----- platform_txn from OLTP -------------------------------------------------
def build_platform():
    rows = []
    for path in glob.glob(os.path.join(OUT, "landing", "oltp", "sub_recursion_success_*", "data.csv")):
        op = os.path.basename(os.path.dirname(path)).replace("sub_recursion_success_", "")
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        for r in df.itertuples():
            ts = dt.datetime.fromisoformat(r.recurrence_ts).replace(tzinfo=dt.timezone.utc)
            amt = round(float(r.amount), 4)
            ptxn = r.partner_txn_id or None
            rh = sha(op, r.sub_id, ptxn, "recursion_success", amt, ts.isoformat())
            rows.append(dict(event_id=sha("platform", op, "recursion_success", r.sub_id, rh),
                             source_system="platform", operator_code=op, partner_txn_id=ptxn,
                             account_id=None, user_id=r.user_id, sub_id=r.sub_id, plan_code=None,
                             txn_type="recursion_success", amount_original=amt, currency_original="USD",
                             amount_reporting=amt, fx_rate=1.0, txn_ts_utc=ts, business_date=ts.date(),
                             source_file=os.path.basename(path),
                             file_arrival_date=dt.date.fromisoformat(BD), record_hash=rh))
    for path in glob.glob(os.path.join(OUT, "landing", "oltp", "sub_initial_*", "data.csv")):
        op = os.path.basename(os.path.dirname(path)).replace("sub_initial_", "")
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        for r in df.itertuples():
            if r.status == "failed":
                continue
            ts = dt.datetime.fromisoformat(r.created_ts).replace(tzinfo=dt.timezone.utc)
            amt = round(float(r.amount), 4); ptxn = r.partner_txn_id or None
            rh = sha(op, r.sub_id, ptxn, "subscription_success", amt, ts.isoformat())
            rows.append(dict(event_id=sha("platform", op, "subscription_success", r.sub_id, rh),
                             source_system="platform", operator_code=op, partner_txn_id=ptxn,
                             account_id=None, user_id=r.user_id, sub_id=r.sub_id, plan_code=r.plan_id,
                             txn_type="subscription_success", amount_original=amt, currency_original="USD",
                             amount_reporting=amt, fx_rate=1.0, txn_ts_utc=ts, business_date=ts.date(),
                             source_file=os.path.basename(path),
                             file_arrival_date=dt.date.fromisoformat(BD), record_hash=rh))
    for path in glob.glob(os.path.join(OUT, "landing", "oltp", "sub_recursion_failure_*", "data.csv")):
        op = os.path.basename(os.path.dirname(path)).replace("sub_recursion_failure_", "")
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        for r in df.itertuples():
            ts = dt.datetime.fromisoformat(r.attempt_ts).replace(tzinfo=dt.timezone.utc)
            ptxn = r.partner_txn_id or None
            # recursion_failure is non-revenue: amount booked as 0; carries failure_reason.
            rh = sha(op, r.sub_id, ptxn, "recursion_failure", "0", ts.isoformat())
            rows.append(dict(event_id=sha("platform", op, "recursion_failure", r.sub_id, rh),
                             source_system="platform", operator_code=op, partner_txn_id=ptxn,
                             account_id=None, user_id=r.user_id, sub_id=r.sub_id, plan_code=None,
                             txn_type="recursion_failure", amount_original=0.0, currency_original="USD",
                             amount_reporting=0.0, fx_rate=1.0, txn_ts_utc=ts, business_date=ts.date(),
                             failure_reason=r.failure_reason,
                             source_file=os.path.basename(path),
                             file_arrival_date=dt.date.fromisoformat(BD), record_hash=rh))
    return pd.DataFrame(rows)


def load_bridge():
    """Load the SCD2 account<->user bridge (effective-dated versions)."""
    p = os.path.join(OUT, "landing", "oltp", "account_user_bridge_seed", "data.csv")
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    df["effective_from"] = df["effective_from"].apply(dt.date.fromisoformat)
    df["effective_to"] = df["effective_to"].apply(dt.date.fromisoformat)
    return df


def resolve_user_asof(bridge, op, account_id, business_date):
    """Point-in-time identity: the user this account mapped to AT business_date.
    This is what makes historical fallback reproducible across account reassignment."""
    if account_id is None:
        return None
    m = bridge[(bridge.operator_code == op) & (bridge.account_id == account_id)
               & (bridge.effective_from <= business_date) & (bridge.effective_to > business_date)]
    return None if m.empty else m.iloc[0]["user_id"]


def build_dim_plan():
    """Build the SCD2 plan dimension from the plan_catalog feed: close out each prior
    version at the next version's effective_from, flag the latest is_current."""
    p = os.path.join(OUT, "landing", "oltp", "plan_catalog", "data.csv")
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    df["price_original"] = df["price_original"].astype(float)
    df["effective_from"] = df["effective_from"].apply(dt.date.fromisoformat)
    rows = []
    for (op, plan), grp in df.groupby(["operator_code", "plan_code"]):
        grp = grp.sort_values("effective_from").reset_index(drop=True)
        for idx, r in grp.iterrows():
            eff_to = grp.loc[idx + 1, "effective_from"] if idx + 1 < len(grp) else dt.date(9999, 12, 31)
            rows.append(dict(operator_code=op, plan_code=plan, price_original=r["price_original"],
                             currency=r["currency"], billing_period=r["billing_period"],
                             effective_from=r["effective_from"], effective_to=eff_to,
                             is_current=(idx + 1 == len(grp))))
    return pd.DataFrame(rows)


def plan_price_asof(dim_plan, op, plan_code, business_date):
    """Expected price (in reporting currency) for a plan, IN EFFECT at business_date.
    Returns None if the plan/version is unknown for that date."""
    if plan_code is None:
        return None
    m = dim_plan[(dim_plan.operator_code == op) & (dim_plan.plan_code == plan_code)
                 & (dim_plan.effective_from <= business_date) & (dim_plan.effective_to > business_date)]
    if m.empty:
        return None
    row = m.iloc[0]
    usd, _rate = to_usd(row["price_original"], row["currency"])
    return None if usd is None else round(usd, 4)


def load_churn():
    p = os.path.join(OUT, "landing", "oltp", "user_churn_events", "data.csv")
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    df["churn_ts"] = pd.to_datetime(df["churn_ts"])
    return df.groupby("user_id")["churn_ts"].min().to_dict()


# ----- the recon decision tree ------------------------------------------------
def reconcile(partner, platform, bridge, churn, dim_plan):
    bd = dt.date.fromisoformat(BD)
    p = partner[partner.txn_type.isin(MONEY_TYPES)].copy()
    i = platform[platform.txn_type.isin(MONEY_TYPES)].copy()

    results = []
    used_p, used_i = set(), set()

    # STAGE 1: exact match on partner_txn_id
    p_keyed = p[p.partner_txn_id.notna()]
    i_keyed = i[i.partner_txn_id.notna()]
    merged = p_keyed.merge(i_keyed, on=["operator_code", "partner_txn_id"], suffixes=("_p", "_i"))
    for r in merged.itertuples():
        cat = "matched" if within_tol(r.amount_reporting_p, r.amount_reporting_i) else "amount_mismatch"
        results.append(_pair(r.operator_code, r.partner_txn_id, r.event_id_p, r.event_id_i,
                             r.user_id_i, r.sub_id_i, r.account_id_p, r.txn_type_p,
                             r.business_date_p, r.amount_reporting_p, r.amount_reporting_i,
                             max(r.file_arrival_date_p, r.file_arrival_date_i), 1.0, "partner_txn_id", cat))
        used_p.add(r.event_id_p); used_i.add(r.event_id_i)

    # STAGE 2: fallback identity match (no usable partner_txn_id)
    # Candidate join is intentionally LOOSE (identity + txn_type + window only) so a
    # weak-but-plausible pair still FORMS; the confidence floor is what rejects it.
    # That rejection is the false-positive guard we must demonstrate, not hide.
    # Identity is resolved POINT-IN-TIME from the SCD2 bridge: whoever the account
    # mapped to on the txn's business_date (handles account reassignment correctly).
    p_no = p[~p.event_id.isin(used_p) & p.partner_txn_id.isna()].copy()
    p_no["b_user_id"] = p_no.apply(
        lambda r: resolve_user_asof(bridge, r["operator_code"], r["account_id"], r["business_date"]), axis=1)
    i_no = i[~i.event_id.isin(used_i)]
    cands = []
    for pr in p_no.itertuples():
        b_user = getattr(pr, "b_user_id", None)
        if b_user is None or (isinstance(b_user, float) and pd.isna(b_user)):
            continue  # identity unresolved at this date -> cannot fallback at all
        for ir in i_no.itertuples():
            if pr.operator_code != ir.operator_code or b_user != ir.user_id:
                continue
            if pr.txn_type != ir.txn_type:
                continue
            if abs((pr.business_date - ir.business_date).days) > WIN:
                continue
            conf = W_IDENTITY
            if pr.plan_code and ir.plan_code and pr.plan_code == ir.plan_code:
                conf += W_PLAN
            if within_tol(pr.amount_reporting, ir.amount_reporting):
                conf += W_AMOUNT
            if pr.business_date == ir.business_date:
                conf += W_SAMEDAY
            conf = round(conf, 3)
            gap = abs((pr.txn_ts_utc - ir.txn_ts_utc).total_seconds())
            cands.append((conf, -gap, pr.event_id, ir.event_id, pr, ir))
    cands.sort(reverse=True)
    # best candidate confidence seen per event (used to flag below-floor rejects)
    best_conf_p, best_conf_i = {}, {}
    for conf, _, pe, ie, pr, ir in cands:
        best_conf_p[pe] = max(best_conf_p.get(pe, 0.0), conf)
        best_conf_i[ie] = max(best_conf_i.get(ie, 0.0), conf)
    for conf, _, pe, ie, pr, ir in cands:
        if pe in used_p or ie in used_i or conf < MIN_CONF:
            continue
        cat = "matched" if within_tol(pr.amount_reporting, ir.amount_reporting) else "amount_mismatch"
        results.append(_pair(pr.operator_code, None, pe, ie, ir.user_id, ir.sub_id, pr.account_id,
                             pr.txn_type, pr.business_date, pr.amount_reporting, ir.amount_reporting,
                             pr.file_arrival_date, conf, "fallback_identity", cat))
        used_p.add(pe); used_i.add(ie)

    # STAGE 3: residual singletons anchored on target day. A residual that HAD a
    # fallback candidate but scored below the floor is tagged fallback_below_floor
    # so analysts can review it (false-positive guard left it unmatched on purpose).
    def _resid_method_conf(eid, best_map):
        c = best_map.get(eid)
        if c is not None and c < MIN_CONF:
            return "fallback_below_floor", round(c, 3)
        return "unmatched", 0.0
    for pr in p[~p.event_id.isin(used_p)].itertuples():
        if pr.business_date != bd:
            continue
        method, conf = _resid_method_conf(pr.event_id, best_conf_p)
        results.append(_pair(pr.operator_code, pr.partner_txn_id, pr.event_id, None, None, None,
                             pr.account_id, pr.txn_type, pr.business_date, pr.amount_reporting, None,
                             pr.file_arrival_date, conf, method, "missing_on_platform"))
    for ir in i[~i.event_id.isin(used_i)].itertuples():
        if ir.business_date != bd:
            continue
        method, conf = _resid_method_conf(ir.event_id, best_conf_i)
        results.append(_pair(ir.operator_code, ir.partner_txn_id, None, ir.event_id, ir.user_id,
                             ir.sub_id, None, ir.txn_type, ir.business_date, None, ir.amount_reporting,
                             ir.file_arrival_date, conf, method, "missing_at_partner"))

    df = pd.DataFrame(results)
    # anchor paired rows on target day too
    df = df[df.business_date == bd].copy()

    # STAGE 4: orphan churn overlay
    def orphan(row):
        ct = churn.get(row["i_user_id"]) if row["i_user_id"] else None
        if ct is not None and row["txn_type"] == "recursion_success" \
                and pd.Timestamp(row["business_date"]) > ct.normalize():
            return True
        return False
    df["is_orphan"] = df.apply(orphan, axis=1)
    df.loc[df.is_orphan, "break_category"] = "orphan_churn"

    # STAGE 5: late arrival overlay
    df["lag_days"] = df.apply(lambda r: (r["file_arrival_date"] - r["business_date"]).days, axis=1)
    df["is_late"] = df["lag_days"] >= LATE
    df.loc[df.is_late & (df.break_category == "matched"), "break_category"] = "late_arrival"

    df["variance"] = df.partner_amount.fillna(0) - df.internal_amount.fillna(0)

    # SCD2 point-in-time validation: resolve the plan price IN EFFECT at business_date
    # from dim_plan and check whether the money side matches the contemporaneous price.
    # A mid-month price change therefore can't create a false mismatch on a re-run.
    plan_by_event = {}
    for _, rr in partner.iterrows():
        if rr.get("plan_code"):
            plan_by_event[rr["event_id"]] = rr["plan_code"]
    for _, rr in platform.iterrows():
        if rr.get("plan_code"):
            plan_by_event.setdefault(rr["event_id"], rr["plan_code"])

    def _expected(row):
        plan = plan_by_event.get(row["partner_event_id"]) or plan_by_event.get(row["platform_event_id"])
        return plan_price_asof(dim_plan, row["operator_code"], plan, row["business_date"])

    def _price_ok(row):
        exp = row["expected_amount"]
        if exp is None or pd.isna(exp):
            return None
        money = row["partner_amount"] if pd.notna(row["partner_amount"]) else row["internal_amount"]
        if money is None or pd.isna(money):
            return None
        return bool(within_tol(money, exp))

    df["plan_code"] = df.apply(
        lambda r: plan_by_event.get(r["partner_event_id"]) or plan_by_event.get(r["platform_event_id"]), axis=1)
    df["expected_amount"] = df.apply(_expected, axis=1)
    df["plan_price_ok"] = df.apply(_price_ok, axis=1)

    df["period_month"] = BD[:7]
    df["break_id"] = df.apply(lambda r: sha(r["partner_event_id"], r["platform_event_id"], r["business_date"]), axis=1)
    return df


def _pair(op, ptxn, pe, ie, iuser, sub, acct, ttype, bdate, pamt, iamt, arrival, conf, method, cat):
    return dict(operator_code=op, partner_txn_id=ptxn, partner_event_id=pe, platform_event_id=ie,
                i_user_id=iuser, sub_id=sub, account_id=acct, txn_type=ttype, business_date=bdate,
                partner_amount=pamt, internal_amount=iamt, file_arrival_date=arrival,
                match_confidence=conf, match_method=method, break_category=cat)


def build_daily(fact):
    g = fact.groupby("operator_code")
    rows = []
    for op, d in g:
        def c(cat): return int((d.break_category == cat).sum())
        rows.append(dict(business_date=BD, operator_code=op,
                         partner_txn_count=int(d.partner_event_id.notna().sum()),
                         internal_txn_count=int(d.platform_event_id.notna().sum()),
                         matched_count=c("matched"),
                         break_count=c("amount_mismatch") + c("missing_on_platform") + c("missing_at_partner") + c("orphan_churn") + c("late_arrival"),
                         partner_amount_total=round(d.partner_amount.fillna(0).sum(), 4),
                         internal_amount_total=round(d.internal_amount.fillna(0).sum(), 4),
                         variance=round(d.partner_amount.fillna(0).sum() - d.internal_amount.fillna(0).sum(), 4),
                         amount_mismatch_count=c("amount_mismatch"),
                         missing_on_platform_count=c("missing_on_platform"),
                         missing_at_partner_count=c("missing_at_partner"),
                         orphan_churn_count=c("orphan_churn"),
                         late_arrival_count=c("late_arrival")))
    return pd.DataFrame(rows).sort_values("operator_code")


def build_close(fact):
    rows = []
    for op, d in fact.groupby("operator_code"):
        rev = d[(d.txn_type.isin(REV_TYPES)) & (d.break_category.isin(
            ["matched", "amount_mismatch", "late_arrival", "missing_on_platform", "orphan_churn"]))]
        refunds = d[(d.txn_type == "refund")]
        gross = round(rev.partner_amount.fillna(0).sum(), 2)
        rfd = round(refunds.partner_amount.fillna(0).sum(), 2)
        rows.append(dict(operator_code=op, gross_revenue=gross, refunds=rfd,
                         net_revenue=round(gross - rfd, 2),
                         variance_under_review=round(d[d.break_category != "matched"].variance.abs().sum(), 2),
                         missing_on_platform_cnt=int((d.break_category == "missing_on_platform").sum()),
                         leakage_cnt=int((d.break_category == "missing_at_partner").sum()),
                         orphan_churn_cnt=int((d.break_category == "orphan_churn").sum())))
    return pd.DataFrame(rows).sort_values("operator_code")


def main():
    global FX
    FX = load_fx()
    os.makedirs(SAMPLE, exist_ok=True)

    # discover operator files (normal + late arrivals)
    partner_frames = []
    for op, spec in CFG["operators"].items():
        if not spec.get("enabled"):
            continue
        files = []
        for arr_dir in sorted(glob.glob(os.path.join(OUT, "landing", "operator", op, "*"))):
            arrival = os.path.basename(arr_dir)
            for fp in glob.glob(os.path.join(arr_dir, "*")):
                files.append((fp, arrival))
        if files:
            partner_frames.append(normalize_operator(op, files))
    partner = pd.concat(partner_frames, ignore_index=True)
    platform = build_platform()
    bridge = load_bridge()
    churn = load_churn()
    dim_plan = build_dim_plan()

    fact = reconcile(partner, platform, bridge, churn, dim_plan)
    daily = build_daily(fact)
    close = build_close(fact)

    partner.to_csv(os.path.join(SAMPLE, "partner_txn.csv"), index=False)
    platform.to_csv(os.path.join(SAMPLE, "platform_txn.csv"), index=False)
    dim_plan.to_csv(os.path.join(SAMPLE, "dim_plan.csv"), index=False)
    cols = ["break_id", "business_date", "operator_code", "break_category", "txn_type",
            "partner_txn_id", "sub_id", "account_id", "plan_code", "partner_amount", "internal_amount",
            "variance", "expected_amount", "plan_price_ok",
            "match_method", "match_confidence", "is_late", "lag_days",
            "partner_event_id", "platform_event_id"]
    fact[cols].sort_values(["break_category", "operator_code"]).to_csv(
        os.path.join(SAMPLE, "fact_reconciliation_break.csv"), index=False)
    daily.to_csv(os.path.join(SAMPLE, "reconciliation_daily.csv"), index=False)
    close.to_csv(os.path.join(SAMPLE, "monthly_revenue_close.csv"), index=False)

    print("=== reconciliation_daily ===")
    print(daily.to_string(index=False))
    print("\n=== break category counts ===")
    print(fact.break_category.value_counts().to_string())
    print("\n=== monthly_revenue_close ===")
    print(close.to_string(index=False))

    # ---- SCD2 demonstrations -------------------------------------------------
    print("\n=== dim_plan (SCD2) — PLN_A1 has two effective-dated versions ===")
    print(dim_plan[dim_plan.plan_code == "PLN_A1"][
        ["operator_code", "plan_code", "price_original", "currency",
         "effective_from", "effective_to", "is_current"]].to_string(index=False))
    early, late = dt.date(2026, 5, 10), dt.date(2026, 5, 29)
    print(f"\nPoint-in-time plan price (PLN_A1):"
          f"\n  as-of {early} -> {plan_price_asof(dim_plan, 'telco_a', 'PLN_A1', early)} USD  (old version)"
          f"\n  as-of {late} -> {plan_price_asof(dim_plan, 'telco_a', 'PLN_A1', late)} USD  (current version)")
    print("\n=== account_user_bridge (SCD2) — account 0777 reassigned 2026-05-20 ===")
    for d in (dt.date(2026, 5, 10), dt.date(2026, 5, 29)):
        print(f"  resolve 8801000000777 as-of {d} -> "
              f"{resolve_user_asof(bridge, 'telco_d', '8801000000777', d)}")


if __name__ == "__main__":
    main()
