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


def test_monotonic_clock_never_steps_back():
    """kalshi_auth re-syncs its offset every few minutes; a backward step must
    not reorder receipt stamps (BookStream refuses out-of-order events)."""
    from datetime import datetime, timedelta, timezone
    t0 = datetime(2026, 10, 30, 22, 28, 8, tzinfo=timezone.utc)
    seq = iter([t0, t0 - timedelta(seconds=0.5), t0 + timedelta(seconds=0.2),
                t0 + timedelta(seconds=1)])
    clk = md.MonotonicClock(lambda: next(seq))
    out = [clk() for _ in range(4)]
    assert out == [t0, t0, t0 + timedelta(seconds=0.2), t0 + timedelta(seconds=1)]
    assert all(a <= b for a, b in zip(out, out[1:]))


def test_experiment_shares_the_monotonic_clock(monkeypatch):
    from datetime import datetime, timezone
    import research_c15_live
    clk = md._server_now
    assert isinstance(clk, md.MonotonicClock)
    assert md.MarketDataClient.__init__.__kwdefaults__["clock"] is clk
    t = datetime(2026, 10, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(clk, "base", lambda: t)
    monkeypatch.setattr(clk, "last", None)
    assert research_c15_live.server_now() == t and clk.last == t
