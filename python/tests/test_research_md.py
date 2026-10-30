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
    def __init__(self, answer_pings=True):
        self.sent = []
        self.inbox = asyncio.Queue()
        self.answer_pings = answer_pings

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def send(self, text):
        self.sent.append(json.loads(text))
        m = json.loads(text)
        if m["cmd"] == "subscribe" and m["params"]["channels"] == ["orderbook_delta"]:
            await self.inbox.put(json.dumps({"id": m["id"], "type": "subscribed",
                                             "msg": {"channel": "orderbook_delta", "sid": 40 + m["id"]}}))
            await self.inbox.put(json.dumps({"type": "orderbook_snapshot", "sid": 40 + m["id"], "seq": 1,
                                             "msg": {"market_ticker": m["params"]["market_tickers"][0],
                                                     "yes_dollars_fp": [],
                                                     "no_dollars_fp": [["0.04", "2.00"]]}}))

    async def recv(self):
        item = await self.inbox.get()
        if isinstance(item, Exception):
            raise item
        return item

    async def ping(self):
        f = asyncio.get_running_loop().create_future()
        if self.answer_pings:
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
                            ping_every_s=0.01, pong_timeout_s=0.05, backoff_base_s=0.01, **kw)
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
    last_alive = max(datetime.fromisoformat(e["t"]) for e in events if e["kind"] == "alive")
    assert tape.state_at(last_alive) is not None          # usable while live
    assert {m["cmd"] for m in ws.sent} <= md.ALLOWED_CMDS


def test_unanswered_ping_drops_connection_and_reconnects():
    events = []
    c = client([FakeWS(answer_pings=False), FakeWS()], events)
    c.set_markets([TK])
    asyncio.run(run_for(c, 0.5))
    gens = [(e["kind"], e["gen"]) for e in events if e["kind"] in ("up", "down")]
    assert gens[:3] == [("up", 1), ("down", 1), ("up", 2)]


def test_recv_error_reconnects_with_new_generation():
    events = []
    bad = FakeWS()
    bad.inbox.put_nowait(ConnectionResetError("reset"))
    c = client([bad, FakeWS()], events)
    c.set_markets([TK])
    asyncio.run(run_for(c, 0.5))
    assert ("up", 2) in [(e["kind"], e["gen"]) for e in events]
    assert c.generation == 2


def test_read_only_command_guard():
    c = client([FakeWS()], [])
    c._ws = FakeWS()
    for bad in ({"cmd": "create_order", "params": {}},
                {"cmd": "subscribe", "params": {"channels": ["fill"]}},
                {"cmd": "subscribe", "params": {"channels": ["orderbook_delta", "fill"]}},
                {"cmd": "update_subscription", "params": {"action": "add_markets"}}):
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
