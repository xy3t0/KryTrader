"""Tests for sim_exec.py (taker IOC simulator). Stdlib-only, --noconftest."""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from fractions import Fraction as F
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import sim_exec as sx  # noqa: E402

UTC = timezone.utc
T0 = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
TK = "KXBTC15M-X-00"


def snap(seq, no=(), yes=(), t=None):
    return ({"type": "orderbook_snapshot", "seq": seq,
             "msg": {"market_ticker": TK, "yes_dollars_fp": [list(l) for l in yes],
                     "no_dollars_fp": [list(l) for l in no]}}, t or T0)


def delta(seq, side, price, d, t):
    return ({"type": "orderbook_delta", "seq": seq,
             "msg": {"market_ticker": TK, "side": side, "price_dollars": price, "delta_fp": d}}, t)


def tape(*msgs):
    tp = sx.BookTape(TK)
    for m, t in msgs:
        tp.apply(m, t)
    return tp


def mem():
    c = sqlite3.connect(":memory:")
    sx.init_schema(c)
    return c


@pytest.fixture
def ledger():
    return sx.LiquidityLedger(mem(), scope="test")


def buy(tp, ledger, limit="0.97", count="1", decided=T0, lat=timedelta(seconds=1), side="yes"):
    return sx.simulate_ioc_buy(tp, ledger, side=side, limit=limit, count=count,
                               decided_at=decided, latency=lat)


def test_fills_at_ask_from_opposite_bid(ledger):
    # NO bid at 0.04 => YES ask 0.96.
    r = buy(tape(snap(1, no=[("0.04", "5.00")])), ledger)
    assert r.reason == "filled" and r.fills == (sx.SimFill(F(96, 100), F(1)),)


def test_multilevel_cheapest_first_respects_limit(ledger):
    tp = tape(snap(1, no=[("0.04", "0.50"), ("0.03", "3.00"), ("0.01", "9.00")]))
    r = buy(tp, ledger, limit="0.97", count="2")
    assert [(f.price, f.count) for f in r.fills] == [(F(96, 100), F(1, 2)), (F(97, 100), F(3, 2))]
    assert r.unfilled == 0


def test_partial_ioc_leftover_cancelled(ledger):
    r = buy(tape(snap(1, no=[("0.04", "0.30")])), ledger, count="1")
    assert r.reason == "partial" and r.unfilled == F(7, 10)


def test_nothing_under_limit(ledger):
    r = buy(tape(snap(1, no=[("0.01", "5.00")])), ledger, limit="0.97")   # ask 0.99
    assert r.reason == "no_liquidity" and r.fills == ()


def test_depth_disappearing_during_latency(ledger):
    tp = tape(snap(1, no=[("0.04", "1.00")]),
              delta(2, "no", "0.04", "-1.00", T0 + timedelta(milliseconds=500)))
    r = buy(tp, ledger)       # arrives at T0+1s, after the depth left
    assert r.reason == "no_liquidity"


def test_no_fill_from_quote_arriving_after_order(ledger):
    tp = tape(snap(1, no=[]), delta(2, "no", "0.04", "1.00", T0 + timedelta(seconds=3)))
    assert buy(tp, ledger).reason == "no_liquidity"        # arrival T0+1s
    assert buy(tp, ledger, lat=timedelta(seconds=5)).reason == "filled"


def test_no_book_before_first_snapshot(ledger):
    tp = tape(snap(1, no=[("0.04", "1.00")], t=T0 + timedelta(seconds=2)))
    assert buy(tp, ledger).reason == "book_unavailable"


def test_sequence_gap_invalidates_until_snapshot(ledger):
    tp = tape(snap(1, no=[("0.04", "1.00")]),
              delta(3, "no", "0.04", "1.00", T0 + timedelta(milliseconds=200)))   # seq 2 missing
    assert buy(tp, ledger).reason == "book_unavailable"
    tp.apply(*snap(10, no=[("0.04", "1.00")], t=T0 + timedelta(seconds=2)))
    assert buy(tp, ledger, lat=timedelta(seconds=3)).reason == "filled"


def test_duplicate_delta_ignored(ledger):
    d = delta(2, "no", "0.04", "1.00", T0 + timedelta(milliseconds=100))
    tp = tape(snap(1, no=[]), d, d)
    assert tp.state_at(T0 + timedelta(seconds=1)).no == ((F(4, 100), F(1)),)


def test_liquidity_not_reused_across_orders_or_restart():
    conn = mem()
    tp = tape(snap(1, no=[("0.04", "1.50")]))
    a = buy(tp, sx.LiquidityLedger(conn, "s"))
    b = buy(tp, sx.LiquidityLedger(conn, "s"))          # a "restarted" ledger, same scope
    assert a.fills[0].count == 1 and b.fills[0].count == F(1, 2) and b.reason == "partial"
    assert buy(tp, sx.LiquidityLedger(conn, "other")).reason == "filled"   # scopes isolated


def test_consumption_clears_when_level_leaves_book():
    conn = mem()
    tp = tape(snap(1, no=[("0.04", "1.00")]))
    buy(tp, sx.LiquidityLedger(conn, "s"))
    tp.apply(*delta(2, "no", "0.04", "-1.00", T0 + timedelta(seconds=2)))
    tp.apply(*delta(3, "no", "0.04", "1.00", T0 + timedelta(seconds=3)))
    later = buy(tp, sx.LiquidityLedger(conn, "s"), decided=T0 + timedelta(seconds=3))
    assert later.reason == "filled"


def test_buying_no_lifts_yes_bids(ledger):
    r = buy(tape(snap(1, yes=[("0.05", "1.00")])), ledger, side="no", limit="0.96")
    assert r.fills == (sx.SimFill(F(95, 100), F(1)),)


def test_fractional_sizes_respect_granularity(ledger):
    r = buy(tape(snap(1, no=[("0.04", "0.335")])), ledger, count="1")
    assert r.fills[0].count == F(33, 100)


@pytest.mark.parametrize("kw", [dict(limit="1"), dict(limit="0"), dict(count="0.001"),
                                dict(count="0"), dict(side="maybe"), dict(lat=timedelta(seconds=-1))])
def test_bad_orders_rejected(ledger, kw):
    with pytest.raises(sx.SimError):
        buy(tape(snap(1)), ledger, **kw)


def test_out_of_order_receipt_refused():
    tp = tape(snap(1, t=T0 + timedelta(seconds=5)))
    with pytest.raises(sx.SimError):
        tp.apply(*snap(2, t=T0))


def test_deterministic_replay(ledger):
    msgs = [snap(1, no=[("0.04", "0.40"), ("0.03", "2.00")]),
            delta(2, "no", "0.04", "0.20", T0 + timedelta(milliseconds=300))]
    r1 = buy(tape(*msgs), sx.LiquidityLedger(mem(), "a"))
    r2 = buy(tape(*msgs), sx.LiquidityLedger(mem(), "a"))
    assert r1 == r2 and r1.fills[0].count == F(6, 10)
