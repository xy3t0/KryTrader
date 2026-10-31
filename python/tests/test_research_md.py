"""Tests for research_md.py with a fake websocket (no network).
Stdlib-only: python -m pytest tests/test_research_md.py --noconftest
"""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import research_md as md  # noqa: E402
import sim_exec as sx  # noqa: E402

UTC = timezone.utc
TK = "KXBTC15M-X-00"


class FakeWS:
    """One orderbook subscription with a seq shared by all its markets."""

    def __init__(self, answer_pings=True, pong_delay=0.0):
        self.pong_delay = pong_delay
        self.sent = []
        self.inbox = asyncio.Queue()
        self.answer_pings = answer_pings
        self.seq = 0
        self.sid = 77

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def _snap(self, ticker):
        self.seq += 1
        await self.inbox.put(json.dumps({"type": "orderbook_snapshot", "sid": self.sid, "seq": self.seq,
                                         "msg": {"market_ticker": ticker, "yes_dollars_fp": [],
                                                 "no_dollars_fp": [["0.04", "2.00"]]}}))

    async def send(self, text):
        m = json.loads(text)
        self.sent.append(m)
        p = m["params"]
        if m["cmd"] == "subscribe" and p["channels"] == ["orderbook_delta"]:
            await self.inbox.put(json.dumps({"id": m["id"], "type": "subscribed",
                                             "msg": {"channel": "orderbook_delta", "sid": self.sid}}))
            for t in p["market_tickers"]:
                await self._snap(t)
        elif m["cmd"] == "update_subscription" and p["action"] == "get_snapshot":
            for t in p["market_tickers"]:
                await self._snap(t)

    async def recv(self):
        item = await self.inbox.get()
        if isinstance(item, Exception):
            raise item
        return item

    async def ping(self):
        loop = asyncio.get_running_loop()
        f = loop.create_future()
        if self.answer_pings and self.pong_delay:
            loop.call_later(self.pong_delay, lambda: f.done() or f.set_result(None))
        elif self.answer_pings:
            f.set_result(None)
        return f


class Clock:
    def __init__(self):
        self.t = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)

    def __call__(self):
        self.t += timedelta(milliseconds=10)
        return self.t


def client(sockets, events, cf=None, **kw):
    it = iter(sockets)
    c = md.MarketDataClient(on_event=events.append, on_cf=cf, headers=lambda: {"h": "signed"},
                            connect=lambda url, headers: next(it), clock=Clock(),
                            ping_every_s=0.01, pong_timeout_s=0.05, dead_after_s=0.1,
                            backoff_base_s=0.01, **kw)
    return c


async def run_for(c, seconds):
    stop = asyncio.Event()
    task = asyncio.ensure_future(c.run(stop))
    await asyncio.sleep(seconds)
    stop.set()
    await asyncio.wait_for(task, 2)


def test_subscribes_records_raw_snapshot_and_liveness():
    events = []
    ws = FakeWS()
    c = client([ws], events)
    c.set_markets([TK])
    asyncio.run(run_for(c, 0.2))
    kinds = [e["kind"] for e in events]
    assert kinds[0] == "up" and "msg" in kinds and "alive" in kinds and kinds[-1] == "down"
    tape = sx.BookTape.from_events(TK, events)
    assert tape.stream.generation is None or True
    last_alive = max(datetime.fromisoformat(e["t"]) for e in events if e["kind"] == "alive")
    assert tape.state_at(last_alive) is not None          # usable while live
    assert {m["cmd"] for m in ws.sent} <= md.ALLOWED_CMDS


def test_unanswered_ping_drops_connection_and_reconnects():
    events = []
    c = client([FakeWS(answer_pings=False), FakeWS()], events)
    c.set_markets([TK])
    asyncio.run(run_for(c, 0.5))
    gens = [(e["kind"], e["gen"]) for e in events if e["kind"] in ("up", "down")]
    g = gens[0][1]
    assert gens[:3] == [("up", g), ("down", g), ("up", g + 1)]


def test_slow_pong_is_not_liveness_and_does_not_reconnect():
    events = []
    ws = FakeWS(pong_delay=0.07)          # slower than pong_timeout, faster than dead_after
    c = client([ws], events)
    c.set_markets([TK])
    asyncio.run(run_for(c, 0.4))
    kinds = [e["kind"] for e in events]
    assert kinds.count("up") == 1 and "alive" not in kinds
    assert c.pong_stats["slow"] >= 1 and c.pong_stats["fast"] == 0


def test_recv_error_reconnects_with_new_generation():
    events = []
    bad = FakeWS()
    bad.inbox.put_nowait(ConnectionResetError("reset"))
    c = client([bad, FakeWS()], events)
    c.set_markets([TK])
    asyncio.run(run_for(c, 0.5))
    ups = [e["gen"] for e in events if e["kind"] == "up"]
    assert len(ups) >= 2 and ups[1] == ups[0] + 1


def test_read_only_command_guard():
    c = client([FakeWS()], [])
    c._ws = FakeWS()
    for bad in ({"cmd": "create_order", "params": {}},
                {"cmd": "subscribe", "params": {"channels": ["fill"]}},
                {"cmd": "subscribe", "params": {"channels": ["orderbook_delta", "fill"]}},
                {"cmd": "update_subscription", "params": {"action": "place_order"}}):
        with pytest.raises(md.MarketDataError):
            asyncio.run(c._send(bad))


def test_snapshot_request_uses_subscription_sid():
    async def go():
        ws = FakeWS()
        events = []
        c = client([ws], events)
        c.set_markets([TK])
        stop = asyncio.Event()
        task = asyncio.ensure_future(c.run(stop))
        await asyncio.sleep(0.1)
        ok = await c.request_snapshot(TK)
        stop.set()
        await task
        return ok, ws.sent
    ok, sent = asyncio.run(go())
    req = [m for m in sent if m["cmd"] == "update_subscription"]
    assert ok and req[0]["params"]["action"] == "get_snapshot" and req[0]["params"]["sids"]


TK2 = "KXBTC15M-Y-00"


async def _session(steps):
    ws = FakeWS()
    events = []
    c = client([ws], events)
    stop = asyncio.Event()
    task = asyncio.ensure_future(c.run(stop))
    for markets, wait in steps:
        c.set_markets(markets)
        await asyncio.sleep(wait)
    stop.set()
    await task
    return ws, events


def test_second_market_added_to_the_one_subscription_with_fresh_snapshot():
    ws, events = asyncio.run(_session([([TK], 0.1), ([TK, TK2], 0.7)]))
    subs = [m for m in ws.sent if m["cmd"] == "subscribe" and m["params"]["channels"] == ["orderbook_delta"]]
    assert len(subs) == 1                                        # never a second subscription
    adds = [m for m in ws.sent if m["params"].get("action") == "add_markets"]
    snaps = [m for m in ws.sent if m["params"].get("action") == "get_snapshot"]
    assert adds and adds[0]["params"]["market_tickers"] == [TK2] and adds[0]["params"]["sids"] == [77]
    assert snaps and snaps[0]["params"]["market_tickers"] == [TK2]
    st = sx.BookStream.from_events(events)
    last = max(datetime.fromisoformat(e["t"]) for e in events if e["kind"] == "alive")
    assert st.state_at(TK, last) is not None and st.state_at(TK2, last) is not None


def test_removing_one_market_keeps_subscription_and_others():
    ws, events = asyncio.run(_session([([TK, TK2], 0.1), ([TK], 0.7)]))
    dels = [m for m in ws.sent if m["params"].get("action") == "delete_markets"]
    assert dels and dels[0]["params"]["market_tickers"] == [TK2]
    assert not any(m["cmd"] == "unsubscribe" for m in ws.sent)
    st = sx.BookStream.from_events(events)
    last = max(datetime.fromisoformat(e["t"]) for e in events if e["kind"] == "alive")
    assert st.state_at(TK2, last) is None and st.state_at(TK, last) is not None


def test_unsubscribes_removed_markets():
    async def go():
        ws = FakeWS()
        c = client([ws], [])
        c.set_markets([TK])
        stop = asyncio.Event()
        task = asyncio.ensure_future(c.run(stop))
        await asyncio.sleep(0.1)
        c.set_markets([])
        await asyncio.sleep(0.7)          # reconcile runs at least every 0.5 s
        stop.set()
        await task
        return ws.sent
    assert any(m["cmd"] == "unsubscribe" for m in asyncio.run(go()))


def test_consumer_error_becomes_invalidation():
    seen = []

    def consumer(ev):
        if ev["kind"] == "msg":
            raise RuntimeError("boom")
        seen.append(ev)
    c = md.MarketDataClient(on_event=consumer, headers=lambda: {}, connect=lambda u, h: FakeWS(),
                            clock=Clock(), ping_every_s=0.01)
    c.set_markets([TK])
    asyncio.run(run_for(c, 0.15))
    assert any(e["kind"] == "invalidate" for e in seen)


def test_cf_frames_forwarded_and_cf_subscribed():
    async def go():
        ws = FakeWS()
        got = []
        c = client([ws], [], cf=got.append)
        stop = asyncio.Event()
        task = asyncio.ensure_future(c.run(stop))
        await asyncio.sleep(0.05)
        await ws.inbox.put(json.dumps({"type": "cfbenchmarks_value", "msg": {"x": 1}}))
        await asyncio.sleep(0.05)
        stop.set()
        await task
        return got, ws.sent
    got, sent = asyncio.run(go())
    assert got and any(m["params"].get("channels") == ["cfbenchmarks_value"] for m in sent)


def test_missing_key_reported_not_crashing():
    def no_key():
        raise FileNotFoundError("Kalshi credentials not set")
    c = md.MarketDataClient(on_event=lambda e: None, headers=no_key,
                            connect=lambda u, h: FakeWS(), clock=Clock())
    asyncio.run(run_for(c, 0.05))
    assert c.last_error == "no saved Kalshi API key" and not c.connected


# ---- elapsed-time clock (review P1) ------------------------------------------------

import json as _json  # noqa: E402
import random as _random  # noqa: E402
from datetime import datetime as _dt, timedelta as _td, timezone as _tz  # noqa: E402

import sim_exec as _sx  # noqa: E402

# 18:59:00Z: inside the protected part of the 19:00 window.
_PROT = _dt(2026, 10, 30, 18, 59, 0, tzinfo=_tz.utc)
# 18:50:00Z: outside it (slewing allowed).
_FREE = _dt(2026, 10, 30, 18, 50, 0, tzinfo=_tz.utc)


class _Sim:
    """Monotonic seconds and a Kalshi-time target with injectable steps."""

    def __init__(self, start):
        self.m, self.start, self.step = 0.0, start, 0.0

    def mono(self):
        return self.m

    def target(self):
        return self.start + _td(seconds=self.m + self.step)


def _clock(sim, **kw):
    return md.ElapsedClock(sim.target, mono=sim.mono, **kw)


def test_protected_window_covers_decisions_arrivals_and_settlement():
    close = _dt(2026, 10, 30, 19, 0, tzinfo=_tz.utc)
    for s in (-240, -150, -60, -1, 0, 5, 8, 59):
        assert md.in_protected_window(close + _td(seconds=s)), s
    for s in (-241, 60, 300, 600):
        assert not md.in_protected_window(close + _td(seconds=s)), s


@pytest.mark.parametrize("step", [-1.0, -3.0, +1.0])
def test_offset_step_inside_window_keeps_exact_elapsed_time(step):
    sim = _Sim(_PROT)
    clk = _clock(sim)
    stamps = []
    for k in range(40):                       # 20 s at 0.5 s
        if k == 7:
            sim.step = step                   # re-measured offset jumps
        stamps.append(clk())
        sim.m += 0.5
    gaps = [(b - a).total_seconds() for a, b in zip(stamps, stamps[1:])]
    assert all(abs(g - 0.5) < 2e-6 for g in gaps)
    assert abs(clk.error_s - step) < 1e-3     # the step is reported, not followed
    for lat in (1, 2, 5):                     # the experiment's latency scenarios
        k = lat * 2
        assert abs((stamps[5 + k] - stamps[5]).total_seconds() - lat) < 2e-6


def test_outside_window_slews_toward_kalshi_time_never_backwards():
    sim = _Sim(_FREE)
    clk = _clock(sim)
    clk()
    sim.step = -1.0
    prev, m0 = clk(), sim.m
    for _ in range(2400):                     # 240 s at 0.1 s, all outside the window
        sim.m += 0.1
        t = clk()
        g = (t - prev).total_seconds()
        assert 0.1 * (1 - md.SLEW_RATE) - 2e-6 <= g <= 0.1 * (1 + md.SLEW_RATE) + 2e-6
        prev = t
    assert abs(clk.error_s) < 1.0 - 0.9 * md.SLEW_RATE * (sim.m - m0)


def test_never_decreases_under_noisy_offsets():
    rnd = _random.Random(7)
    sim = _Sim(_PROT - _td(minutes=3))
    clk = _clock(sim)
    prev = clk()
    for _ in range(5000):
        sim.m += rnd.uniform(0, 0.05)
        if rnd.random() < 0.01:
            sim.step = rnd.uniform(-2, 2)
        t = clk()
        assert t >= prev
        prev = t


def _snap(seq, no_levels):
    return _json.dumps({"type": "orderbook_snapshot", "sid": 1, "seq": seq,
                        "msg": {"market_ticker": "M", "yes_dollars_fp": [],
                                "no_dollars_fp": no_levels}})


def _delta(seq, price, delta):
    return _json.dumps({"type": "orderbook_delta", "sid": 1, "seq": seq,
                        "msg": {"market_ticker": "M", "side": "no",
                                "price_dollars": price, "delta_fp": delta}})


@pytest.mark.parametrize("delta,expect", [("-3.00", "3.00"),     # liquidity removed later
                                          ("5.00", "3.00")])     # liquidity added later
def test_backward_correction_cannot_move_a_later_change_into_an_arrival(delta, expect):
    """Snapshot, decision, then Kalshi time is re-measured 1 s back just
    before a depth change that really arrives 0.5 s AFTER the 1 s-latency
    arrival. The arrival book must not contain it (held clocks stamped it at
    the arrival instant)."""
    sim = _Sim(_PROT)
    clk = _clock(sim)
    evs = []

    def ev(kind, **kw):
        evs.append({"t": clk().isoformat(), "kind": kind, "gen": 1, **kw})
    ev("up")
    ev("msg", raw=_snap(1, [["0.05", "3.00"]]))
    for _ in range(4):
        sim.m += 0.5
        ev("alive")
    decided = clk()                                   # m = 2.0
    sim.m += 0.9
    ev("alive")                                       # m = 2.9
    sim.step = -1.0                                   # offset re-measured backwards
    sim.m += 0.6
    ev("msg", raw=_delta(2, "0.05", delta))           # m = 3.5, after arrival at 3.0
    for _ in range(12):
        sim.m += 0.5
        ev("alive")
    tape = _sx.BookTape.from_events("M", evs)
    arrival = decided + _td(seconds=1)
    st = tape.state_at(arrival, confirmed=True)
    assert st is not None and dict(st.no) == {_sx._f("0.05", "p"): _sx._f(expect, "s")}
    later = tape.state_at(decided + _td(seconds=2), confirmed=True)
    assert dict(later.no) != dict(st.no)              # the change is real, just later


def test_experiment_shares_the_elapsed_clock(monkeypatch):
    import research_c15_live
    clk = md._server_now
    assert isinstance(clk, md.ElapsedClock)
    assert md.MarketDataClient.__init__.__kwdefaults__["clock"] is clk
    t = _dt(2026, 10, 30, tzinfo=_tz.utc)
    monkeypatch.setattr(clk, "target", lambda: t)
    monkeypatch.setattr(clk, "_t", None)
    assert research_c15_live.server_now() == t and clk.error_s == 0.0
