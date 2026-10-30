"""Tests for research_wallet.py (A3 core). Stdlib-only:
    python -m pytest tests/test_research_wallet.py --noconftest
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db  # noqa: E402
import fee_context as fctx  # noqa: E402
import fee_ledger  # noqa: E402
import fee_schedule as fs  # noqa: E402
import fees  # noqa: E402
import research_wallet as rw  # noqa: E402

UTC = timezone.utc
T0 = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
MKT = "E1-M"


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    db.init_db()
    c = sqlite3.connect(str(db.db_path()))
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("BEGIN")
    fctx.register_account(c, kalshi_env="paper", account_id="research-1",
                          precision="0.0001", precision_source="configured")
    rw.create_wallet(c, wallet_id="w4", account_id="research-1",
                     policy=fctx.CONDITIONAL_PAPER, starting_cash="4")
    yield c
    c.close()


def context(conn, at, role="taker", mult="1", policy=fctx.CONDITIONAL_PAPER):
    J = fs.loads
    a = fs.AsOf(at - timedelta(seconds=5), at)
    return fctx.build_fee_context(
        conn, kalshi_env="paper", account_id="research-1", policy=policy, role=role, at=at,
        ttl=timedelta(seconds=30), evidence=("cap-a", "cap-b"),
        market=fs.parse_market({"ticker": MKT, "event_ticker": "E1"}, a),
        series=fs.parse_series(J('{"series":{"ticker":"S","fee_type":"quadratic",'
                                 '"fee_multiplier":' + mult + '}}'), a),
        series_changes=fs.parse_series_fee_changes(J('{"series_fee_change_arr":[]}'), a,
                                                   include_historical=True,
                                                   coverage=fs.Coverage.none()),
        event=fs.parse_event(J('{"event":{"event_ticker":"E1","series_ticker":"S"}}'), a),
        event_changes=fs.parse_event_fee_change_pages(
            [J('{"cursor":"","event_fee_changes":[]}')], a, coverage=fs.Coverage.none()))


def fill(conn, fid="f1", oid="o1", price="0.97", count="1", action="buy", side="yes",
         role="taker", at=None, ctx=None, wallet="w4"):
    at = at or T0 + timedelta(seconds=int(fid[1:]) if fid[1:].isdigit() else 0)
    return rw.apply_fill(conn, rw.FillEvent(
        wallet_id=wallet, order_id=oid, fill_id=fid, executed_at=at, ticker=MKT, side=side,
        action=action, role=role, price=price, count=count,
        fee_context=ctx or context(conn, at, role=role), evidence=("sim-1",)))


def test_wallet_starts_isolated_at_four_dollars(conn):
    w = rw.get_wallet(conn, "w4")
    assert (w.cash, w.reserved, w.starting_cash) == (D(4), 0, D(4))
    with pytest.raises(rw.WalletError):
        rw.create_wallet(conn, wallet_id="w4", account_id="research-1",
                         policy=fctx.CONDITIONAL_PAPER, starting_cash="100")
    assert not hasattr(rw, "reset") and not hasattr(rw, "top_up")


def test_buy_fill_exact_cash_fee_and_position(conn):
    r = fill(conn)
    assert r.fee == fees.settle_fill("-0.97", "0.002037", "0.0001")
    assert r.cash_delta == D("-0.9721") and r.wallet.cash == D("3.0279")
    assert rw.get_position(conn, "w4", MKT, "yes") == (D(1), D("0.9721"))
    assert rw.reconcile(conn, "w4")["ok"]


def test_fees_settled_per_fill_not_from_average_price(conn):
    # Two fragments at different prices on one order.
    a = fill(conn, "f1", price="0.40", count="1")
    b = fill(conn, "f2", price="0.60", count="1")
    per_fill = a.fee.trade_fee + b.fee.trade_fee
    avg = fees.ceil_to(fees.quadratic_model_fee("0.50", "2", "0.07"), "0.000001")
    assert per_fill == D("0.0336") and avg == D("0.035") and per_fill != avg


def test_sell_then_settlement_reconciles(conn):
    fill(conn, "f1", count="2", price="0.50")
    fill(conn, "f2", oid="o2", action="sell", count="1", price="0.70")
    assert rw.get_position(conn, "w4", MKT, "yes")[0] == D(1)
    paid = rw.settle(conn, wallet_id="w4", ticker=MKT, result="yes",
                     settled_at=T0 + timedelta(minutes=5), evidence=("settle-cap",))
    assert paid == D(1) and rw.get_position(conn, "w4", MKT, "yes") == (0, 0)
    rec = rw.reconcile(conn, "w4")
    assert rec["ok"]


def test_losing_settlement_pays_nothing(conn):
    fill(conn)
    cash = rw.get_wallet(conn, "w4").cash
    assert rw.settle(conn, wallet_id="w4", ticker=MKT, result="no", settled_at=T0,
                     evidence=("s",)) == 0
    assert rw.get_wallet(conn, "w4").cash == cash and rw.reconcile(conn, "w4")["ok"]


def test_cannot_sell_more_than_held(conn):
    fill(conn)
    with pytest.raises(rw.WalletError):
        fill(conn, "f2", oid="o2", action="sell", count="2")
    assert rw.reconcile(conn, "w4")["ok"]


def test_insufficient_funds_refused_without_side_effects(conn):
    with pytest.raises(rw.InsufficientFunds):
        fill(conn, count="5", price="0.97")       # ~$4.86 > $4
    assert rw.get_wallet(conn, "w4").cash == D(4)
    assert conn.execute("SELECT COUNT(*) FROM fee_fill_model").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM rw_events").fetchone()[0] == 0


def test_reservations_prevent_double_spending(conn):
    rw.reserve(conn, wallet_id="w4", order_id="o1", amount="3", at=T0)
    with pytest.raises(rw.InsufficientFunds):
        rw.reserve(conn, wallet_id="w4", order_id="o2", amount="1.5", at=T0)
    # Another order's fill cannot eat into o1's reservation.
    with pytest.raises(rw.InsufficientFunds):
        fill(conn, "f1", oid="o2", count="2", price="0.97")
    # o1's own fill consumes its reservation; the IOC remainder is released.
    fill(conn, "f2", oid="o1", count="1", price="0.97")
    assert rw.get_wallet(conn, "w4").reserved == D("2.0279")
    w = rw.release(conn, wallet_id="w4", order_id="o1", at=T0)
    assert w.reserved == 0 and w.cash == D("3.0279") and rw.reconcile(conn, "w4")["ok"]


def test_duplicate_fill_refused_and_wallet_unchanged(conn):
    fill(conn)
    before = rw.get_wallet(conn, "w4")
    with pytest.raises(rw.WalletError):
        fill(conn)
    assert rw.get_wallet(conn, "w4") == before and rw.reconcile(conn, "w4")["ok"]


def test_maker_taker_transition_on_one_order(conn):
    fill(conn, "f1", role="taker")
    r = fill(conn, "f2", role="maker")
    assert r.fee.trade_fee == 0 and rw.reconcile(conn, "w4")["ok"]


@pytest.mark.parametrize("count", ["0.001", "0", "-1", 1.0])
def test_unsupported_quantities_rejected_not_rounded(conn, count):
    with pytest.raises(rw.WalletError):
        fill(conn, count=count)


def test_fractional_quantity_supported(conn):
    r = fill(conn, count="0.25", price="0.50")
    assert rw.get_position(conn, "w4", MKT, "yes")[0] == D("0.25") and rw.reconcile(conn, "w4")["ok"]
    assert r.fee.trade_fee == D("0.004375")


def test_context_from_other_policy_refused(conn):
    fctx.register_account  # strict contexts are never usable today, so use a doctored one
    good = context(conn, T0)
    strict = fctx.FeeContext(**{**good.__dict__, "policy": fctx.STRICT})
    with pytest.raises(fctx.FeeContextError):
        fill(conn, ctx=strict, at=T0)


def test_unusable_or_expired_context_refused(conn):
    bad = fctx.FeeContext(**{**context(conn, T0).__dict__, "usable": False, "reason": "stale"})
    with pytest.raises(rw.WalletError):
        fill(conn, ctx=bad, at=T0)
    with pytest.raises(rw.WalletError):    # context for T0, fill a second later
        fill(conn, ctx=context(conn, T0), at=T0 + timedelta(seconds=1))


def test_fault_after_fee_ledger_write_rolls_back_everything(conn, monkeypatch):
    real = rw._set_cash

    def boom(*a, **k):
        raise RuntimeError("disk full")
    monkeypatch.setattr(rw, "_set_cash", boom)
    with pytest.raises(RuntimeError):
        fill(conn)
    monkeypatch.setattr(rw, "_set_cash", real)
    assert conn.execute("SELECT COUNT(*) FROM fee_fill_model").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM fee_order_state").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM rw_events").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM fee_evidence_refs").fetchone()[0] == 0
    fill(conn)   # and the same fill then succeeds cleanly
    assert rw.reconcile(conn, "w4")["ok"]


def test_requires_open_transaction(conn):
    conn.commit()
    with pytest.raises(rw.WalletError):
        rw.reserve(conn, wallet_id="w4", order_id="o9", amount="1", at=T0)


def test_restart_preserves_state_and_order_accumulator(tmp_path, monkeypatch, conn):
    fill(conn, "f1")
    conn.commit()
    c2 = sqlite3.connect(str(db.db_path()))   # a "restarted" process
    c2.execute("BEGIN")
    assert rw.get_wallet(c2, "w4").cash == D("3.0279")
    st = fee_ledger.get_order_state(c2, kalshi_env="paper", account_id="research-1", order_id="o1")
    assert st.accumulator == D("0.000063")
    c2.close()


def test_fill_records_evidence_references(conn):
    fill(conn)
    refs = {r[0] for r in conn.execute("SELECT evidence_ref FROM fee_evidence_refs")}
    assert refs == {"cap-a", "cap-b"}


def test_unresolved_settlement_not_accepted(conn):
    fill(conn)
    with pytest.raises(rw.WalletError):
        rw.settle(conn, wallet_id="w4", ticker=MKT, result="void", settled_at=T0, evidence=("s",))


def test_affordability_exact_under_low_precision_and_traps(conn):
    # Codex repro: under prec=4, $3.99996 cash could reserve $4.
    from decimal import Inexact, Rounded, localcontext
    rw.create_wallet(conn, wallet_id="w-edge", account_id="research-1",
                     policy=fctx.CONDITIONAL_PAPER, starting_cash="3.99996")
    with localcontext() as ctx:
        ctx.prec = 4
        with pytest.raises(rw.InsufficientFunds):
            rw.reserve(conn, wallet_id="w-edge", order_id="o1", amount="4", at=T0)
        ctx.traps[Inexact] = True
        ctx.traps[Rounded] = True
        rw.reserve(conn, wallet_id="w-edge", order_id="o2", amount="3.99995", at=T0)
        with pytest.raises(rw.InsufficientFunds):     # 0.00001 left; 0.00002 refused
            rw.reserve(conn, wallet_id="w-edge", order_id="o3", amount="0.00002", at=T0)
        w = rw.get_wallet(conn, "w-edge")
        assert w.available == D("0.00001")
    assert w.reserved <= w.cash and rw.reconcile(conn, "w-edge")["ok"]
