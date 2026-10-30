"""Production-shaped tests for research_c15_live.py: the captured Kalshi
payloads (2026-10-30) served through the real capture/service stack, a fake
read-only market-data socket, and an injected clock. No network.
Stdlib-only: python -m pytest tests/test_research_c15_live.py --noconftest
"""
from __future__ import annotations

import asyncio
import email.utils
import importlib.util
import json
import sqlite3
import sys
import types
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
for _name in ("httpx", "websockets", "httpx2"):
    if _name not in sys.modules and importlib.util.find_spec(_name) is None:
        _m = types.ModuleType(_name)
        _m.__getattr__ = lambda attr: type(attr, (), {"__init__": lambda self, *a, **k: None})
        sys.modules[_name] = _m

import db  # noqa: E402
import fee_capture as fcap  # noqa: E402
import fee_service as fsvc  # noqa: E402
import research_c15 as rx  # noqa: E402
import research_c15_live as live  # noqa: E402
import sim_exec as sx  # noqa: E402

UTC = timezone.utc
FIX = Path(__file__).resolve().parent / "fixtures" / "kalshi_fee_2026_10_30"
EVENT = json.loads((FIX / "event-btc15m.json").read_text())
MARKET = EVENT["markets"][0]
TICKER, EVT = MARKET["ticker"], EVENT["event"]["event_ticker"]
CLOSE = datetime(2026, 10, 30, 11, 0, tzinfo=UTC)


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


class Kalshi:
    """Serves the captured bodies with headers consistent with the clock."""

    def __init__(self, clock, *, delay_s=0.0, result=""):
        self.clock, self.delay_s, self.result = clock, delay_s, result
        self.calls = []

    async def __call__(self, url, params):
        path = url.split("/trade-api/v2", 1)[1]
        self.calls.append(path)
        if self.delay_s and path.startswith("/events/KX"):
            self.clock.t += timedelta(seconds=self.delay_s)     # metadata arrives late
        mk = dict(MARKET, result=self.result)
        bodies = {
            "/markets": json.dumps({"cursor": "", "markets": [mk]}).encode(),
            f"/markets/{TICKER}": json.dumps({"market": mk}).encode(),
            f"/events/{EVT}": (FIX / "event-btc15m.json").read_bytes(),
            "/series/KXBTC15M": (FIX / "series-btc15m.json").read_bytes(),
            "/series/fee_changes": (FIX / "series-fee-changes-historical.json").read_bytes(),
            "/events/fee_changes": (FIX / "event-fee-changes.json").read_bytes(),
        }
        body = bodies.get(path)
        hdr = {"Date": email.utils.format_datetime(self.clock(), usegmt=True),
               "Cache-Control": "public, max-age=15"}
        if body is None:
            return fcap.RawResponse(404, {**hdr, "Cache-Control": "no-store"}, b"{}")
        return fcap.RawResponse(200, hdr, body)


def candles(clock, fail=False):
    async def post(req):
        if fail:
            raise ConnectionError("hyperliquid down")
        end = int(clock().timestamp() // 60 * 60 * 1000)
        rows = [{"t": end - (40 - i) * 60_000, "c": str(77500 + (i % 7) * 3.0), "v": "1"}
                for i in range(41)]
        return 200, json.dumps(rows).encode()
    return post


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    db.init_db()
    conn = sqlite3.connect(str(db.db_path()), isolation_level=None)
    clock = Clock(CLOSE - timedelta(seconds=25))
    spot = {"reads": [("coinbase_ws", (CLOSE - timedelta(seconds=27)).timestamp(), 77650.0)]}
    yield conn, clock, spot, tmp_path
    conn.close()


def experiment(conn, clock, spot, tmp_path, *, kalshi=None, post=None, prints=None):
    cap = fcap.FeeMetadataCapturer(kalshi or Kalshi(clock), fcap.CaptureStore(tmp_path / "ev"),
                                   clock=clock)
    svc = fsvc.FeeMetadataService(cap, clock=clock)
    conn.execute("BEGIN")
    rx.setup_wallets(conn, "0" * 64) if False else None
    import fee_context as fctx
    fctx.register_account(conn, kalshi_env="paper", account_id=rx.PARAMS["account_id"],
                          precision=rx.PARAMS["precision"], precision_source="configured")
    conn.commit()
    return live.LiveExperiment(
        conn, capturer=cap, service=svc, book_dir=tmp_path / "book", clock=clock,
        spot_reads=lambda: spot["reads"],
        prints=prints or (lambda close_e, now_e: (77650.0 * 35, 35, "coinbase_proxy")),
        candle_post=post or candles(clock), local_time=lambda: clock().timestamp())


def run(c):
    return asyncio.run(c)


# ---- inputs ------------------------------------------------------------------------

def test_production_payloads_produce_verified_inputs(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    inp = run(x.make_inputs())
    assert inp.rules.ok and inp.rules.strike == 77583.44           # Decimal strike accepted
    assert (inp.market_ticker, inp.event_ticker) == (TICKER, EVT)
    assert inp.close_time == CLOSE and inp.decided_at == clock()
    assert inp.spot == 77650.0 and inp.spot_received_at <= inp.decided_at
    assert inp.sigma_1m is not None and inp.candles_received_at <= inp.decided_at
    assert inp.metadata_received_at <= inp.decided_at
    assert TICKER in x.tapes                                       # book capture started
    fees = run(x.prepare_fees(market_ticker=TICKER, decided_at=inp.decided_at,
                              arrivals={1: inp.decided_at + timedelta(seconds=1)}))
    assert fees[0].usable and fees[0].resolution.coefficient == D("0.07")
    assert fees[1][1].usable and fees[1][1].resolution.at == inp.decided_at + timedelta(seconds=1)


def test_decision_time_is_taken_after_delayed_metadata(env):
    # Codex repro: metadata arriving 5 s late used to keep the earlier timestamp.
    conn, clock, spot, tmp = env
    start = clock()
    x = experiment(conn, clock, spot, tmp, kalshi=Kalshi(clock, delay_s=5))
    inp = run(x.make_inputs())
    assert inp.decided_at >= start + timedelta(seconds=5)
    assert inp.decided_at == clock()
    assert all(t <= inp.decided_at for t in (inp.metadata_received_at, inp.spot_received_at,
                                             inp.candles_received_at))


def test_value_stamped_after_decision_is_never_used(env):
    conn, clock, spot, tmp = env
    spot["reads"] = [("coinbase_ws", (clock() + timedelta(seconds=3)).timestamp(), 99999.0)]
    inp = run(experiment(conn, clock, spot, tmp).make_inputs())
    assert inp.spot is None and inp.spot_received_at is None


def test_spot_is_not_given_invented_freshness(env):
    conn, clock, spot, tmp = env
    spot["reads"] = [("coinbase_ws", (clock() - timedelta(seconds=12)).timestamp(), 77650.0)]
    inp = run(experiment(conn, clock, spot, tmp).make_inputs())
    assert inp.spot is None                     # older than max_spot_age_s: not used at all


def test_cf_index_preferred_over_proxy(env):
    conn, clock, spot, tmp = env
    t = (clock() - timedelta(seconds=1)).timestamp()
    spot["reads"] = [("coinbase_ws", t, 77650.0), ("cf_index", t, 77640.0)]
    inp = run(experiment(conn, clock, spot, tmp).make_inputs())
    assert (inp.spot, inp.spot_source) == (77640.0, "cf_index")


def test_volatility_failing_indefinitely_goes_stale(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp, post=candles(clock, fail=True))
    inp = run(x.make_inputs())
    assert inp.sigma_1m is None
    d = rx.decide(inp, None, open_positions=0, attempted=False, quarantined=False)
    assert d.reason == "missing_volatility"
    x2 = experiment(conn, clock, spot, tmp)
    run(x2.refresh_candles())                   # one good fetch...
    x2.candle_post = candles(clock, fail=True)  # ...then the source dies for good
    x2._candles["received_at"] = clock() - timedelta(seconds=130)
    inp2 = run(x2.make_inputs())
    assert inp2.sigma_1m is not None            # the old value is still reported...
    assert rx.decide(inp2, None, open_positions=0, attempted=False,
                     quarantined=False).reason == "stale_volatility"   # ...but refused


def test_far_from_close_assembles_nothing(env):
    conn, clock, spot, tmp = env
    clock.t = CLOSE - timedelta(minutes=5)
    x = experiment(conn, clock, spot, tmp)
    assert run(x.make_inputs()) is None
    assert TICKER not in x.tapes                # book capture starts only near the close


# ---- book capture and replay -----------------------------------------------------------

def snapshot_event(clock, gen=1, bid="0.05"):
    raw = json.dumps({"type": "orderbook_snapshot", "sid": 3, "seq": 1,
                      "msg": {"market_ticker": TICKER, "yes_dollars_fp": [],
                              "no_dollars_fp": [[bid, "2.00"]]}})
    return {"t": clock().isoformat(), "kind": "msg", "gen": gen, "raw": raw}


def feed_live(x, clock, gen=1, bid="0.05"):
    x.on_book_event({"t": clock().isoformat(), "kind": "up", "gen": gen})
    clock.t += timedelta(milliseconds=100)
    x.on_book_event(snapshot_event(clock, gen, bid))
    for _ in range(4):
        clock.t += timedelta(milliseconds=500)
        x.on_book_event({"t": clock().isoformat(), "kind": "alive", "gen": gen})


def test_book_events_recorded_and_replayed_identically(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    run(x.make_inputs())
    feed_live(x, clock)
    t = clock()
    live_state = x.tapes[TICKER].state_at(t)
    assert live_state is not None and live_state.no[0][0] == D("0.05")
    clock.t = CLOSE + timedelta(seconds=20)
    run(x.maintain_tracking())                  # past close + tail: finalize to .gz
    assert TICKER not in x.tapes
    path, sha = conn.execute("SELECT path, sha256 FROM rx_book_files").fetchone()
    assert path.endswith(".jsonl.gz") and len(sha) == 64
    replay = x.tape_for(TICKER, t)
    assert replay.state_at(t) == live_state


def test_restart_resumes_window_file_and_reproduces_history(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    run(x.make_inputs())
    feed_live(x, clock)
    t_before = clock()
    before = x.tapes[TICKER].state_at(t_before)
    x.finalize_open_recorders()                 # process stops (file left open-ended)
    y = experiment(conn, clock, spot, tmp)      # new process
    run(y.make_inputs())
    assert y.tapes[TICKER].state_at(t_before) == before          # history reloaded
    clock.t += timedelta(seconds=1)
    feed_live(y, clock, gen=2, bid="0.04")
    assert y.tapes[TICKER].state_at(clock()).no[0][0] == D("0.04")


def test_tampered_book_file_is_not_used(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    run(x.make_inputs())
    feed_live(x, clock)
    clock.t = CLOSE + timedelta(seconds=20)
    run(x.maintain_tracking())
    path = conn.execute("SELECT path FROM rx_book_files").fetchone()[0]
    Path(path).write_bytes(b"tampered")
    x._replayed.clear()
    assert x.tape_for(TICKER, clock()) is None


def test_book_event_for_other_market_ignored(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    run(x.make_inputs())
    feed_live(x, clock)
    other = json.dumps({"type": "orderbook_delta", "sid": 9, "seq": 5,
                        "msg": {"market_ticker": "KXETH15M-X", "side": "no",
                                "price_dollars": "0.05", "delta_fp": "-2.00"}})
    x.on_book_event({"t": clock().isoformat(), "kind": "msg", "gen": 1, "raw": other})
    assert x.tapes[TICKER].state_at(clock()) is not None


# ---- startup ---------------------------------------------------------------------------

class FakeMD:
    def __init__(self, on_event, clock):
        self.on_event, self.clock = on_event, clock
        self.connected, self.last_error, self.markets = False, None, set()

    def set_markets(self, m):
        self.markets = set(m)

    async def run(self, stop):
        self.connected = True
        self.on_event({"t": self.clock().isoformat(), "kind": "up", "gen": 1})
        sent = False
        while not stop.is_set():
            if self.markets and not sent:
                for t in self.markets:
                    self.on_event({**snapshot_event(self.clock), "raw": json.dumps(
                        {"type": "orderbook_snapshot", "sid": 3, "seq": 1,
                         "msg": {"market_ticker": t, "yes_dollars_fp": [],
                                 "no_dollars_fp": [["0.05", "2.00"]]}})})
                sent = True
            self.on_event({"t": self.clock().isoformat(), "kind": "alive", "gen": 1})
            await asyncio.sleep(0.01)
        self.connected = False


def start_run(env, monkeypatch, *, spot_reads):
    conn, clock, spot, tmp = env
    import kalshi_auth
    monkeypatch.setattr(kalshi_auth, "is_paper", lambda env=None: True)
    clock.t = CLOSE - timedelta(seconds=70)        # tracking window, before decisions

    async def go():
        stop = asyncio.Event()
        task = asyncio.ensure_future(live.run(
            lambda: {"research_c15_enabled": True}, stop,
            make_md=lambda on_event: FakeMD(on_event, clock), send=Kalshi(clock),
            candle_post=candles(clock), clock=clock, spot_reads=spot_reads,
            prints=lambda c, n: (0.0, 0, "none"), local_time=lambda: clock().timestamp(),
            tick_s=0.02))
        await asyncio.sleep(0.4)
        stop.set()
        await asyncio.wait_for(task, 5)
    asyncio.run(go())


def test_paper_startup_launches_only_after_readiness(env, monkeypatch):
    conn, clock, spot, tmp = env
    start_run(env, monkeypatch, spot_reads=lambda: [])               # no spot: not ready
    st = conn.execute("SELECT status, launched_at FROM rx_cohorts").fetchall()
    assert st == [("armed", None)]                                   # 90 days not consumed
    fresh = lambda: [("coinbase_ws", clock().timestamp() - 1, 77650.0)]  # noqa: E731
    start_run(env, monkeypatch, spot_reads=fresh)
    st = conn.execute("SELECT status, launched_at FROM rx_cohorts").fetchall()
    assert st[0][0] == "active" and st[0][1] is not None
    # The book capture received the snapshot for the tracked market.
    path = Path(conn.execute("SELECT path FROM rx_book_files").fetchone()[0])
    events = sx.BookRecorder.load(path if path.exists() else path.with_suffix(".jsonl.gz"))
    assert any(e["kind"] == "msg" for e in events)


def test_startup_failure_releases_resources(env, monkeypatch):
    conn, clock, spot, tmp = env
    import kalshi_auth
    monkeypatch.setattr(kalshi_auth, "is_paper", lambda env=None: True)

    def boom():
        raise rx.ExperimentError("cannot freeze")
    monkeypatch.setattr(rx, "manifest_body", boom)

    async def go():
        await live.run(lambda: {"research_c15_enabled": True}, asyncio.Event(),
                       make_md=lambda on_event: FakeMD(on_event, clock), send=Kalshi(clock),
                       candle_post=candles(clock), clock=clock, spot_reads=lambda: [],
                       prints=lambda c, n: (0.0, 0, "none"))
    with pytest.raises(rx.ExperimentError):
        asyncio.run(go())
    assert conn.execute("SELECT COUNT(*) FROM rx_cohorts").fetchone()[0] == 0
