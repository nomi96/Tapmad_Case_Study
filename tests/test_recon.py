"""
Tests for the reconciliation core. These run under pytest *and* standalone:

    pytest tests/                 # if pytest is installed
    python3 tests/test_recon.py   # plain-python fallback runner

They exercise the runnable reference engine (data_gen/reference_engine.py), which
mirrors the production PySpark decision tree in src/gold/reconciliation_engine.py.
Run data_gen/generate.py first (the standalone runner does this automatically).
"""
import os
import sys
import subprocess

HERE = os.path.dirname(__file__)
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "data_gen"))


def _ensure_sample():
    """Generate synthetic data + run the reference engine if output is missing."""
    sample = os.path.join(ROOT, "sample_output", "fact_reconciliation_break.csv")
    if not os.path.exists(sample):
        subprocess.run([sys.executable, "data_gen/generate.py"], cwd=ROOT, check=True)
        subprocess.run([sys.executable, "data_gen/reference_engine.py"], cwd=ROOT, check=True)


def _fact():
    import pandas as pd
    _ensure_sample()
    return pd.read_csv(os.path.join(ROOT, "sample_output", "fact_reconciliation_break.csv"))


# ---- tolerance / amount comparison ------------------------------------------
def test_within_tolerance_abs_and_pct():
    import reference_engine as R
    assert R.within_tol(0.975, 0.98) is True        # abs diff 0.005 <= 0.01
    assert R.within_tol(100.0, 100.4) is True        # 0.4 <= 0.5% of 100 (0.5)
    assert R.within_tol(0.98, 1.30) is False         # clear mismatch
    assert R.within_tol(None, 1.0) is False          # null never matches


# ---- every required break category must materialize -------------------------
def test_all_six_break_categories_present():
    f = _fact()
    required = {"matched", "amount_mismatch", "missing_on_platform",
                "missing_at_partner", "orphan_churn", "late_arrival"}
    assert required.issubset(set(f.break_category.unique()))


# ---- fallback (keyless) matching --------------------------------------------
def test_fallback_identity_match_succeeds():
    """S8: telco_d row with no partner_txn_id auto-matches via the bridge >= floor."""
    f = _fact()
    fb = f[f.match_method == "fallback_identity"]
    assert len(fb) >= 1
    assert (fb.match_confidence >= 0.80).all()
    assert (fb.break_category == "matched").any()


def test_false_positive_guard_holds_below_floor():
    """S9: identity resolves but amount disagrees -> NOT matched, flagged for review."""
    f = _fact()
    rej = f[f.match_method == "fallback_below_floor"]
    assert len(rej) >= 1                                  # the guard fired
    assert (rej.match_confidence < 0.80).all()            # genuinely below floor
    # a rejected fallback must remain a break, never a match
    assert not (rej.break_category.isin(["matched"])).any()


# ---- idempotency: re-running must be deterministic --------------------------
def test_engine_is_deterministic():
    import reference_engine as R
    R.FX = R.load_fx()
    # break_id is a deterministic hash of (partner_event, platform_event, date),
    # so the same inputs must yield the same primary keys on every run.
    f1 = _fact()
    subprocess.run([sys.executable, "data_gen/reference_engine.py"], cwd=ROOT, check=True)
    import pandas as pd
    f2 = pd.read_csv(os.path.join(ROOT, "sample_output", "fact_reconciliation_break.csv"))
    assert sorted(f1.break_id.tolist()) == sorted(f2.break_id.tolist())


# ---- variance ties out ------------------------------------------------------
def test_variance_equals_partner_minus_internal():
    f = _fact().fillna(0)
    recomputed = (f.partner_amount - f.internal_amount).round(4)
    assert (recomputed == f.variance.round(4)).all()


# ---- SCD2: plan dimension point-in-time correctness -------------------------
def test_dim_plan_scd2_versions_and_point_in_time():
    """PLN_A1 changed price mid-month: two effective-dated versions, and a
    point-in-time lookup returns the version in effect at the given date."""
    import datetime as dt
    import reference_engine as R
    R.FX = R.load_fx()
    dim = R.build_dim_plan()
    a1 = dim[(dim.operator_code == "telco_a") & (dim.plan_code == "PLN_A1")]
    assert len(a1) == 2                                  # two versions
    assert a1.is_current.sum() == 1                      # exactly one current
    assert (a1.effective_to == dt.date(9999, 12, 31)).sum() == 1
    early = R.plan_price_asof(dim, "telco_a", "PLN_A1", dt.date(2026, 5, 10))
    late = R.plan_price_asof(dim, "telco_a", "PLN_A1", dt.date(2026, 5, 29))
    assert early is not None and late is not None
    assert early < late                                  # old price < new price
    # re-running an early date must NOT pick up the newer price (restatement-safe)
    assert R.plan_price_asof(dim, "telco_a", "PLN_A1", dt.date(2026, 5, 10)) == early


def test_account_bridge_scd2_point_in_time_identity():
    """Account 0777 reassigned on 2026-05-20: identity resolves to the user mapped
    at the txn date, not the current owner."""
    import datetime as dt
    import reference_engine as R
    bridge = R.load_bridge()
    assert R.resolve_user_asof(bridge, "telco_d", "8801000000777", dt.date(2026, 5, 10)) == "U_OLD"
    assert R.resolve_user_asof(bridge, "telco_d", "8801000000777", dt.date(2026, 5, 29)) == "U_NEW"


def test_plan_price_ok_flags_partner_overbilling():
    """The amount_mismatch row is flagged as NOT matching the contemporaneous plan
    price, turning a bare variance into an actionable reason."""
    f = _fact()
    mism = f[f.break_category == "amount_mismatch"]
    assert len(mism) >= 1
    flagged = mism[(mism.expected_amount.notna()) & (mism.plan_price_ok == False)]  # noqa: E712
    assert len(flagged) >= 1


def test_recursion_failure_ingested_but_not_a_break():
    """A failed renewal (sub_recursion_failure) is non-revenue: it is ingested and
    conformed into platform_txn, but must be EXCLUDED from money matching, so it
    never produces a break (e.g. a spurious missing_at_partner)."""
    import pandas as pd
    import os
    _ensure_sample()
    pl = pd.read_csv(os.path.join(ROOT, "sample_output", "platform_txn.csv"))
    fail = pl[pl.txn_type == "recursion_failure"]
    assert len(fail) >= 1                                  # it WAS ingested/conformed
    f = _fact()
    failed_subs = set(fail.sub_id.astype(str))
    fact_subs = set(f.sub_id.astype(str))
    assert failed_subs.isdisjoint(fact_subs)               # but it produced NO break


if __name__ == "__main__":
    # plain-python runner (no pytest dependency)
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {t.__name__}: {e!r}")
        except Exception as e:  # noqa
            failed += 1
            print(f"ERROR {t.__name__}: {e!r}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
