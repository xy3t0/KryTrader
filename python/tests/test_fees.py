"""Tests for the fee accounting core (python/fees.py).

Pure stdlib: runnable without the app's runtime deps via
    python -m pytest tests/test_fees.py --noconftest
"""
from __future__ import annotations

import random
import sys
from decimal import Decimal as D
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fees  # noqa: E402
from fees import (  # noqa: E402
    DIRECT_MEMBER_PRECISION as DIRECT,
    NON_DIRECT_MEMBER_PRECISION as NONDIRECT,
    AccountingInvariantError,
    DuplicateFillError,
    FeeInputError,
    OrderFeeState,
    fill_revenue,
    quadratic_model_fee,
    settle_fill,
)


def _check_invariants(res, revenue, old_acc, precision):
    assert res.balance_delta == revenue - res.net_fee
    assert res.net_fee >= 0
    assert res.accumulator >= 0
    assert res.accumulator == old_acc + res.rounding_fee - res.rebate
    assert 0 <= res.rounding_fee < precision
    assert res.balance_delta % precision == 0  # balance stays on its grid
    assert res.rebate % precision == 0
    assert res.trade_fee % fees.MICRODOLLAR == 0


# ---- documented worked example -------------------------------------------

def test_doc_example_non_direct():
    """docs.kalshi.com/getting_started/fee_rounding: -$0.055 revenue, model fee
    $0.00363825 -> trade fee $0.003639, balance -$0.06, trade+rounding $0.005."""
    res = settle_fill(D("-0.055"), D("0.00363825"), NONDIRECT)
    assert res.trade_fee == D("0.003639")
    assert res.balance_delta == D("-0.06")
    assert res.trade_fee + res.rounding_fee == D("0.005")
    assert res.rebate == 0
    assert res.net_fee == D("0.005")
    _check_invariants(res, D("-0.055"), D(0), NONDIRECT)


# ---- one contract at 97c, coefficient 0.07 (explicitly selected) ---------

@pytest.mark.parametrize("precision,rounding,delta", [
    (DIRECT, D("0.000063"), D("-0.9721")),
    (NONDIRECT, D("0.007963"), D("-0.98")),
])
def test_one_lot_97c(precision, rounding, delta):
    rev = fill_revenue("0.97", 1, "buy")
    mf = quadratic_model_fee("0.97", 1, "0.07")
    assert mf == D("0.002037")
    res = settle_fill(rev, mf, precision)
    assert res.trade_fee == D("0.002037")
    assert res.rounding_fee == rounding
    assert res.rebate == 0
    assert res.balance_delta == delta
    _check_invariants(res, rev, D(0), precision)


def test_sell_positive_revenue_direct():
    rev = fill_revenue("0.97", 1, "sell")
    res = settle_fill(rev, D("0.002037"), DIRECT)
    assert rev == D("0.97")
    assert res.balance_delta == D("0.9679")  # 0.967963 floored
    assert res.rounding_fee == D("0.000063")
    _check_invariants(res, rev, D(0), DIRECT)


def test_negative_flooring_is_toward_minus_infinity():
    # Truncation toward zero would give -0.05 (a buyer paying LESS). Must be -0.06.
    res = settle_fill(D("-0.050001"), D(0), NONDIRECT)
    assert res.balance_delta == D("-0.06")
    assert res.rounding_fee == D("0.009999")


def test_trade_fee_rounds_up_to_microdollar():
    res = settle_fill(D("-0.5"), D("0.0000000001"), DIRECT)
    assert res.trade_fee == D("0.000001")


# ---- accumulator / rebate behaviour --------------------------------------

def test_rebate_surfaces_on_later_fill_and_money_is_conserved():
    st = OrderFeeState("o1", NONDIRECT)
    revs, results = [], []
    for i in range(3):
        rev = D("-0.05")
        results.append(st.apply_fill(f"f{i}", rev, D("0.000001")))
        revs.append(rev)
    # Fill 1: rounding 0.009999 accumulates, no rebate yet.
    assert results[0].rebate == 0
    assert results[0].accumulator == D("0.009999")
    # Fill 2: accumulator crosses 1c and this fill's own fee (t+r = 0.01) allows it.
    assert results[1].rebate == D("0.01")
    assert results[1].net_fee == 0
    assert results[1].balance_delta == D("-0.05")
    # Fill 3: same again.
    assert results[2].rebate == D("0.01")
    total_delta = sum(r.balance_delta for r in results)
    total_fee = sum(r.net_fee for r in results)
    assert total_delta == sum(revs) - total_fee
    assert st.accumulator == D("0.009997")


def test_rebate_deferred_when_current_fill_fee_too_small():
    # Accumulator would exceed 1c, but this fill's fee (t+r = 0.005) can't absorb a 1c rebate.
    res = settle_fill(D("-0.055"), D("0.00363825"), NONDIRECT, accumulator=D("0.009"))
    assert res.rebate == 0
    assert res.accumulator == D("0.010361")
    _check_invariants(res, D("-0.055"), D("0.009"), NONDIRECT)


def test_accumulator_may_exceed_precision():
    # Not an invariant violation: the cap can hold back a precision-sized amount.
    res = settle_fill(D("-0.055"), D("0.00363825"), NONDIRECT, accumulator=D("0.009"))
    assert res.accumulator >= NONDIRECT


def test_zero_fee_fill_with_existing_accumulator():
    res = settle_fill(D("-0.05"), D(0), NONDIRECT, accumulator=D("0.015"))
    assert res.trade_fee == 0
    assert res.rounding_fee == 0
    assert res.rebate == 0
    assert res.net_fee == 0
    assert res.balance_delta == D("-0.05")
    assert res.accumulator == D("0.015")


def test_maker_taker_transition_shares_one_accumulator():
    st = OrderFeeState("o1", NONDIRECT)
    st.apply_fill("taker-part", D("-0.05"), D("0.000001"))  # taker fill
    acc_after_taker = st.accumulator
    res = st.apply_fill("maker-part", D("-0.05"), D(0))      # rests, then fills as maker
    # The maker fill sees the taker fill's accumulator.
    assert res.accumulator == acc_after_taker + res.rounding_fee - res.rebate


def test_state_isolation_between_orders():
    a = OrderFeeState("a", NONDIRECT)
    b = OrderFeeState("b", NONDIRECT)
    a.apply_fill("f1", D("-0.05"), D("0.000001"))
    assert b.accumulator == 0
    res_b = b.apply_fill("f1", D("-0.05"), D("0.000001"))  # same fill id, other order: fine
    assert res_b.rebate == 0


def test_duplicate_fill_rejected_and_state_unchanged():
    st = OrderFeeState("o1", NONDIRECT)
    st.apply_fill("f1", D("-0.05"), D("0.000001"))
    before = st.to_dict()
    with pytest.raises(DuplicateFillError):
        st.apply_fill("f1", D("-0.05"), D("0.000001"))
    assert st.to_dict() == before


def test_failed_fill_leaves_state_unchanged():
    st = OrderFeeState("o1", NONDIRECT)
    before = st.to_dict()
    with pytest.raises(FeeInputError):
        st.apply_fill("f1", D("-0.05"), D("-0.001"))
    assert st.to_dict() == before


def test_serialization_round_trip():
    st = OrderFeeState("o1", DIRECT)
    st.apply_fill("f1", D("-0.97"), D("0.002037"))
    st.apply_fill("f2", D("-0.97"), D("0.002037"))
    d = st.to_dict()
    assert all(isinstance(d[k], str) for k in ("precision", "accumulator"))
    st2 = OrderFeeState.from_dict(d)
    assert st2 == st
    with pytest.raises(DuplicateFillError):
        st2.apply_fill("f2", D("-0.97"), D("0.002037"))


def test_from_dict_rejects_float_serialization():
    with pytest.raises(FeeInputError):
        OrderFeeState.from_dict({"order_id": "o", "precision": 0.01, "accumulator": "0"})


# ---- fractional counts (pure math; venue support verified separately) ----

def test_fractional_count_model_fee():
    mf = quadratic_model_fee("0.50", D("0.37"), "0.07")
    assert mf == D("0.0064750")
    rev = fill_revenue("0.50", D("0.37"), "buy")
    res = settle_fill(rev, mf, DIRECT)
    assert res.trade_fee == D("0.006475")
    _check_invariants(res, rev, D(0), DIRECT)


# ---- input validation -----------------------------------------------------

@pytest.mark.parametrize("bad", [0.5, True, None, "abc", D("NaN"), D("Infinity"), "-Infinity", [1]])
def test_to_decimal_rejects(bad):
    with pytest.raises(FeeInputError):
        fees.to_decimal(bad)


@pytest.mark.parametrize("kwargs", [
    dict(price="0", count=1, coefficient="0.07"),
    dict(price="1", count=1, coefficient="0.07"),
    dict(price="0.5", count=0, coefficient="0.07"),
    dict(price="0.5", count=-1, coefficient="0.07"),
    dict(price="0.5", count=1, coefficient="-0.07"),
    dict(price=0.5, count=1, coefficient="0.07"),
])
def test_quadratic_model_fee_rejects(kwargs):
    with pytest.raises(FeeInputError):
        quadratic_model_fee(**kwargs)


@pytest.mark.parametrize("args", [
    (D("-0.5"), D("-0.001"), DIRECT, D(0)),       # negative model fee
    (D("-0.5"), D("0.001"), DIRECT, D("-0.01")),  # negative accumulator
    (D("-0.5"), D("0.001"), D(0), D(0)),          # zero precision
    (D("-0.5"), D("0.001"), D("0.0000005"), D(0)),  # sub-microdollar precision
    (-0.5, D("0.001"), DIRECT, D(0)),             # float revenue
])
def test_settle_fill_rejects(args):
    with pytest.raises(FeeInputError):
        settle_fill(*args)


def test_fill_revenue_rejects_bad_action():
    with pytest.raises(FeeInputError):
        fill_revenue("0.5", 1, "hold")


def test_order_state_requires_ids():
    with pytest.raises(FeeInputError):
        OrderFeeState("", DIRECT)
    st = OrderFeeState("o", DIRECT)
    with pytest.raises(FeeInputError):
        st.apply_fill("", D("-0.5"), D(0))


# ---- invariant sweep -------------------------------------------------------

@pytest.mark.parametrize("precision", [DIRECT, NONDIRECT])
def test_invariants_hold_across_random_order_sequences(precision):
    rng = random.Random(20261030)
    for o in range(300):
        st = OrderFeeState(f"o{o}", precision)
        total_rev = total_fee = total_delta = D(0)
        for f in range(rng.randint(1, 8)):
            price = D(rng.randint(1, 99)) / 100
            count = D(rng.randint(1, 4000)) / 100  # includes fractional counts
            rev = fill_revenue(price, count, rng.choice(["buy", "sell"]))
            mf = quadratic_model_fee(price, count, rng.choice(["0.07", "0.0175", "0"]))
            old = st.accumulator
            res = st.apply_fill(f"f{f}", rev, mf)
            _check_invariants(res, rev, old, precision)
            total_rev += rev
            total_fee += res.net_fee
            total_delta += res.balance_delta
        assert total_delta == total_rev - total_fee
