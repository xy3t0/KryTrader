"""Tests for sim_exec.py (book tape, recorder, taker IOC simulator).
Stdlib-only: python -m pytest tests/test_sim_exec.py --noconftest
"""
from __future__ import annotations

import json
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
TK, TK2 = "KXBTC15M-X-00", "KXETH15M-X-00"


def at(s):
    return (T0 + timedelta(seconds=s)).isoformat()


def up(s=-2, gen=1):
    return {"t": at(s), "kind": "up", "gen": gen}


def down(s, gen=1):
    return {"t": at(s), "kind": "down", "gen": gen}


def alive(s, gen=1):
    return {"t": at(s), "kind": "alive", "gen": gen}


def snap(seq, no=(), yes=(), s=0.0, gen=1, sid=7, ticker=TK):
    raw = json.dumps({"type": "orderbook_snapshot", "sid": sid, "seq": seq,
                      "msg": {"market_ticker": ticker, "yes_dollars_fp": [list(l) for l in yes],
                              "no_dollars_fp": [list(l) for l in no]}})
    return {"t": at(s), "kind": "msg", "gen": gen, "raw": raw}


def delta(seq, side, price, d, s, gen=1, sid=7, ticker=TK):
    raw = json.dumps({"type": "orderbook_delta", "sid": sid, "seq": seq,
                      "msg": {"market_ticker": ticker, "side": side, "price_dollars": price,
                              "delta_fp": d}})
    return {"t": at(s), "kind": "msg", "gen": gen, "raw": raw}


def with_alive(events, until=12, gen=1, step=0.5):
    """Interleave liveness marks every `step` s, keeping time order."""
    marks = [alive(-2 + k * step, gen) for k in range(int((until + 2) / step) + 1)]
    return sorted(events + marks, key=lambda e: (e["t"], e["kind"] != "up"))


def tape(*events, live=True, until=12):
    evs = [up()] + list(events)
    return sx.BookTape.from_events(TK, with_alive(evs, until) if live else evs)


def mem():
    c = sqlite3.connect(":memory:")
    sx.init_schema(c)
    return c


@pytest.fixture
def ledger():
    return sx.LiquidityLedger(mem(), scope="test")


def buy(tp, ledger, limit="0.97", count="1", decided=T0, lat=1, side="yes"):
    return sx.simulate_ioc_buy(tp, ledger, side=side, limit=limit, count=count,
                               decided_at=decided, latency=timedelta(seconds=lat))


# ---- fills ------------------------------------------------------------------

def test_fills_at_ask_from_opposite_bid(ledger):
    r = buy(tape(snap(1, no=[("0.04", "5.00")])), ledger)
    assert r.reason == "filled" and r.fills == (sx.SimFill(F(96, 100), F(1)),)


def test_multilevel_cheapest_first_respects_limit(ledger):
    tp = tape(snap(1, no=[("0.04", "0.50"), ("0.03", "3.00"), ("0.01", "9.00")]))
    r = buy(tp, ledger, count="2")
    assert [(f.price, f.count) for f in r.fills] == [(F(96, 100), F(1, 2)), (F(97, 100), F(3, 2))]


def test_partial_ioc_leftover_cancelled(ledger):
    r = buy(tape(snap(1, no=[("0.04", "0.30")])), ledger)
    assert r.reason == "partial" and r.unfilled == F(7, 10)


def test_nothing_under_limit(ledger):
    assert buy(tape(snap(1, no=[("0.01", "5.00")])), ledger).reason == "no_liquidity"


def test_depth_disappearing_during_latency(ledger):
    tp = tape(snap(1, no=[("0.04", "1.00")]), delta(2, "no", "0.04", "-1.00", 0.5))
    assert buy(tp, ledger).reason == "no_liquidity"


def test_no_fill_from_quote_arriving_after_order(ledger):
    tp = tape(snap(1, no=[]), delta(2, "no", "0.04", "1.00", 3))
    assert buy(tp, ledger).reason == "no_liquidity"
    assert buy(tp, ledger, lat=5).reason == "filled"


def test_buying_no_lifts_yes_bids(ledger):
    r = buy(tape(snap(1, yes=[("0.05", "1.00")])), ledger, side="no", limit="0.96")
    assert r.fills == (sx.SimFill(F(95, 100), F(1)),)


def test_fractional_sizes_respect_granularity(ledger):
    assert buy(tape(snap(1, no=[("0.04", "0.335")])), ledger).fills[0].count == F(33, 100)


@pytest.mark.parametrize("kw", [dict(limit="1"), dict(limit="0"), dict(count="0.001"),
                                dict(count="0"), dict(side="maybe"), dict(lat=-1)])
def test_bad_orders_rejected(ledger, kw):
    with pytest.raises(sx.SimError):
        buy(tape(snap(1)), ledger, **kw)


# ---- validity -----------------------------------------------------------------

def test_no_book_before_first_snapshot(ledger):
    assert buy(tape(snap(1, no=[("0.04", "1.00")], s=2)), ledger).reason == "book_unavailable"


def test_sequence_gap_invalidates_until_snapshot(ledger):
    tp = tape(snap(1, no=[("0.04", "1.00")]), delta(3, "no", "0.04", "1.00", 0.2),
              snap(10, no=[("0.04", "1.00")], s=2))
    assert buy(tp, ledger).reason == "book_unavailable"
    assert buy(tp, ledger, lat=3).reason == "filled"


def test_duplicate_delta_ignored():
    d = delta(2, "no", "0.04", "1.00", 0.1)
    tp = tape(snap(1), d, d)
    assert tp.state_at(T0 + timedelta(seconds=1)).no == ((F(4, 100), F(1)),)


def test_old_snapshot_on_silent_connection_is_unusable(ledger):
    # Codex: an hour-old snapshot used to stay usable. Liveness marks stop.
    tp = tape(snap(1, no=[("0.04", "1.00")]), live=False)
    tp.feed(alive(0.5))
    assert tp.state_at(T0 + timedelta(seconds=1)) is not None
    assert tp.state_at(T0 + timedelta(seconds=6)) is None          # > MAX_SILENCE
    assert tp.state_at(T0 + timedelta(hours=1)) is None


def test_disconnect_invalidates_and_reconnect_needs_snapshot(ledger):
    gen2 = sorted([up(3, gen=2)] + [alive(3 + k * 0.5, gen=2) for k in range(20)]
                  + [snap(1, no=[("0.04", "1.00")], s=6, gen=2, sid=9)],
                  key=lambda e: (e["t"], e["kind"] != "up"))
    tp = sx.BookTape.from_events(TK, with_alive([up(), snap(1, no=[("0.04", "1.00")])], until=2)
                                 + [down(2.2)] + gen2)
    assert tp.state_at(T0 + timedelta(seconds=1)) is not None
    assert tp.state_at(T0 + timedelta(seconds=2.5)) is None        # down
    assert tp.state_at(T0 + timedelta(seconds=5)) is None          # up, but no new snapshot
    assert tp.state_at(T0 + timedelta(seconds=7)) is not None      # fresh snapshot on gen 2


def test_message_from_stale_generation_invalidates():
    tp = tape(snap(1, no=[("0.04", "1.00")]), delta(2, "no", "0.04", "1.00", 0.5, gen=0))
    assert tp.state_at(T0 + timedelta(seconds=1)) is None


def test_sid_change_without_snapshot_invalidates():
    tp = tape(snap(1, no=[("0.04", "1.00")]), delta(2, "no", "0.04", "1.00", 0.5, sid=8))
    assert tp.state_at(T0 + timedelta(seconds=1)) is None


@pytest.mark.parametrize("bad", [
    snap(1, no=[("0.04", "-1.00")]),          # negative size
    snap(1, no=[("1.20", "1.00")]),            # impossible price
    {"t": at(0), "kind": "msg", "gen": 1, "raw": "{not json"},
    {"t": at(0), "kind": "msg", "gen": 1, "raw": json.dumps(
        {"type": "orderbook_snapshot", "seq": 1, "msg": {"market_ticker": TK,
                                                         "no_dollars_fp": [["0.04"]]}})},
    {"t": at(0), "kind": "msg", "gen": 1, "raw": json.dumps(
        {"type": "orderbook_snapshot", "msg": {"market_ticker": TK, "no_dollars_fp": []}})},
])
def test_malformed_snapshot_never_becomes_valid(bad):
    tp = tape(bad)
    assert tp.state_at(T0 + timedelta(seconds=1)) is None


def test_negative_delta_invalidates():
    tp = tape(snap(1, no=[("0.04", "1.00")]), delta(2, "no", "0.04", "-2.00", 0.5))
    assert tp.state_at(T0 + timedelta(seconds=1)) is None


def test_explicit_invalidation_from_listener_failure():
    tp = tape(snap(1, no=[("0.04", "1.00")]),
              {"t": at(0.5), "kind": "invalidate", "reason": "listener error"})
    assert tp.state_at(T0 + timedelta(seconds=1)) is None


def test_interleaved_markets_ignored_by_other_tape():
    tp = tape(snap(1, no=[("0.04", "1.00")]),
              snap(1, no=[("0.30", "9.00")], s=0.1, ticker=TK2, sid=8),
              delta(2, "no", "0.30", "1.00", 0.2, ticker=TK2, sid=8))
    assert tp.state_at(T0 + timedelta(seconds=1)).no == ((F(4, 100), F(1)),)


def test_out_of_order_feed_refused():
    tp = tape(snap(1, s=5))
    with pytest.raises(sx.SimError):
        tp.feed(snap(2, s=0))


# ---- liquidity -------------------------------------------------------------------

def test_liquidity_not_reused_across_orders_or_restart():
    conn = mem()
    tp = tape(snap(1, no=[("0.04", "1.50")]))
    a = buy(tp, sx.LiquidityLedger(conn, "s"))
    b = buy(tp, sx.LiquidityLedger(conn, "s"))
    assert a.fills[0].count == 1 and b.fills[0].count == F(1, 2) and b.reason == "partial"
    assert buy(tp, sx.LiquidityLedger(conn, "other")).reason == "filled"


def test_consumption_clears_when_level_leaves_book():
    conn = mem()
    tp = tape(snap(1, no=[("0.04", "1.00")]), delta(2, "no", "0.04", "-1.00", 2),
              delta(3, "no", "0.04", "1.00", 3))
    buy(tp, sx.LiquidityLedger(conn, "s"))
    assert buy(tp, sx.LiquidityLedger(conn, "s"), decided=T0 + timedelta(seconds=3)).reason == "filled"


# ---- recorder / replay -------------------------------------------------------------

def test_recorder_round_trip_reproduces_simulation(tmp_path):
    events = with_alive([up(), snap(1, no=[("0.04", "0.40"), ("0.03", "2.00")]),
                         delta(2, "no", "0.04", "0.20", 0.3)])
    rec = sx.BookRecorder(tmp_path, "w1")
    live = sx.BookTape(TK)
    for ev in events:
        rec.write(ev)
        live.feed(ev)
    path, digest = rec.finalize()
    import hashlib
    assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest
    replayed = sx.BookTape.from_events(TK, sx.BookRecorder.load(path))
    r1 = buy(live, sx.LiquidityLedger(mem(), "a"))
    r2 = buy(replayed, sx.LiquidityLedger(mem(), "a"))
    assert r1 == r2 and r1.fills[0].count == F(6, 10)


def test_load_drops_torn_final_line_only(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text(json.dumps(up()) + "\n" + '{"t": "2026')
    assert sx.BookRecorder.load(p) == [up()]
    p.write_text('{"broken\n' + json.dumps(up()) + "\n")
    with pytest.raises(sx.SimError):
        sx.BookRecorder.load(p)


def test_restart_replay_continues_with_new_generation(tmp_path):
    # Before crash: gen 1. After restart: gen 2 with a fresh snapshot. No
    # "down" was recorded (crash), but the generation change still separates them.
    pre = with_alive([up(), snap(1, no=[("0.04", "1.00")])], until=2)
    post = [up(10, gen=2)] + [alive(10 + k * 0.5, gen=2) for k in range(10)]
    post = sorted(post + [snap(1, no=[("0.05", "1.00")], s=10.2, gen=2, sid=3)],
                  key=lambda e: e["t"])
    tp = sx.BookTape.from_events(TK, pre + post)
    assert tp.state_at(T0 + timedelta(seconds=9)) is None       # silent gap during the crash
    assert tp.state_at(T0 + timedelta(seconds=12)).no == ((F(5, 100), F(1)),)


@pytest.mark.parametrize("name", ["", "../x", "a/b", ".hidden"])
def test_recorder_rejects_bad_names(tmp_path, name):
    with pytest.raises(sx.SimError):
        sx.BookRecorder(tmp_path, name)


# ---- shared subscription (Kalshi: one orderbook sid, seq shared by markets) ----------

def stream(*events, until=12):
    return sx.BookStream.from_events(with_alive([up()] + list(events), until))


def test_interleaved_markets_on_one_sid_are_valid():
    st = stream(snap(1, no=[("0.04", "1.00")]), snap(2, no=[("0.30", "2.00")], s=0.1, ticker=TK2),
                delta(3, "no", "0.04", "1.00", 0.2), delta(4, "no", "0.30", "1.00", 0.3, ticker=TK2))
    t = T0 + timedelta(seconds=1)
    assert st.state_at(TK, t).no == ((F(4, 100), F(2)),)
    assert st.state_at(TK2, t).no == ((F(30, 100), F(3)),)


def test_gap_on_shared_sid_invalidates_every_market_until_resnapshot():
    st = stream(snap(1, no=[("0.04", "1.00")]), snap(2, no=[("0.30", "2.00")], s=0.1, ticker=TK2),
                delta(5, "no", "0.30", "1.00", 0.2, ticker=TK2))          # 3, 4 missing
    t = T0 + timedelta(seconds=1)
    assert st.state_at(TK, t) is None and st.state_at(TK2, t) is None
    assert sorted(st.needs_snapshot([TK, TK2])) == sorted([TK, TK2])
    st.feed(snap(6, no=[("0.04", "1.00")], s=12.1))                       # get_snapshot reply
    assert st.state_at(TK, T0 + timedelta(seconds=12.2)) is not None
    assert st.needs_snapshot([TK, TK2]) == [TK2]


def test_removing_one_market_keeps_the_others():
    st = stream(snap(1, no=[("0.04", "1.00")]), snap(2, no=[("0.30", "2.00")], s=0.1, ticker=TK2),
                {"t": at(0.5), "kind": "invalidate", "ticker": TK2, "reason": "delete_markets"},
                delta(3, "no", "0.04", "1.00", 0.6))
    t = T0 + timedelta(seconds=1)
    assert st.state_at(TK2, t) is None and st.state_at(TK, t).no == ((F(4, 100), F(2)),)


def test_market_added_after_connection_up_becomes_usable():
    # Codex repro: socket up -> track -> snapshot -> alive used to return None.
    events = with_alive([up(-10), snap(1, no=[("0.30", "1.00")], s=-9, ticker=TK2),
                         snap(2, no=[("0.04", "1.00")], s=3)], until=12)
    st = sx.BookStream.from_events(sorted(events, key=lambda e: (e["t"], e["kind"] != "up")))
    assert st.state_at(TK, T0 + timedelta(seconds=4)) is not None


def test_attach_header_replays_mid_stream_recording():
    # A recording that starts while the stream is at seq 41 on sid 7.
    events = [{"t": at(0), "kind": "attach", "gen": 1, "connected": True, "seqs": {"7": 41}},
              snap(42, no=[("0.04", "1.00")], s=0.1), delta(43, "no", "0.04", "1.00", 0.2)]
    st = sx.BookStream.from_events(with_alive(events, until=3)[1:] if False else
                                   sorted(events + [alive(k * 0.5) for k in range(1, 7)],
                                          key=lambda e: e["t"]))
    assert st.state_at(TK, T0 + timedelta(seconds=1)).no == ((F(4, 100), F(2)),)
    bad = [{"t": at(0), "kind": "attach", "gen": 1, "connected": True, "seqs": {"7": 41}},
           snap(42, no=[("0.04", "1.00")], s=0.1), delta(45, "no", "0.04", "1.00", 0.2)]
    st2 = sx.BookStream.from_events(sorted(bad + [alive(k * 0.5) for k in range(1, 7)],
                                           key=lambda e: e["t"]))
    assert st2.state_at(TK, T0 + timedelta(seconds=1)) is None        # gap after attach


def test_attach_invalidates_books_from_before_it():
    events = with_alive([up(), snap(1, no=[("0.04", "1.00")])], until=2)
    events += [{"t": at(2.5), "kind": "attach", "gen": 9, "connected": True, "seqs": {}}]
    events += [alive(3 + k * 0.5, gen=9) for k in range(6)]
    st = sx.BookStream.from_events(events)
    assert st.state_at(TK, T0 + timedelta(seconds=4)) is None


def test_recorder_sync_marks_durable_watermark(tmp_path):
    rec = sx.BookRecorder(tmp_path, "w")
    rec.write(up())
    assert rec.synced is None
    rec.sync()
    assert rec.synced == up()["t"]
    rec.close()


def test_execution_needs_evidence_past_arrival():
    events = with_alive([up(), snap(1, no=[("0.04", "1.00")])], until=0.5)
    tp = sx.BookTape.from_events(TK, events)
    assert tp.state_at(T0 + timedelta(seconds=1)) is not None                 # decision view
    assert tp.state_at(T0 + timedelta(seconds=1), confirmed=True) is None     # recording ends
    tp.feed(alive(1.5))
    assert tp.state_at(T0 + timedelta(seconds=1), confirmed=True) is not None


def test_confirmation_fails_if_connection_dropped_before_next_mark():
    events = with_alive([up(), snap(1, no=[("0.04", "1.00")])], until=0.5)
    events += [down(1.2), up(1.3, gen=2), alive(1.5, gen=2)]
    tp = sx.BookTape.from_events(TK, events)
    assert tp.state_at(T0 + timedelta(seconds=1), confirmed=True) is None
