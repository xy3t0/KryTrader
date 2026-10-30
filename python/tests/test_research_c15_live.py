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
    assert TICKER in x.tracked                                     # book capture started
    fees = run(x.prepare_fees(market_ticker=TICKER, decided_at=inp.decided_at,
                              arrivals={1: inp.decided_at + timedelta(seconds=1)},
                              account_id=rx.PARAMS["account_id"], policy=rx.PARAMS["fee_policy"]))
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
    x2._candles["newest_start"] = clock() - timedelta(seconds=150)
    inp2 = run(x2.make_inputs())
    assert inp2.sigma_1m is not None            # the old value is still reported...
    assert rx.decide(inp2, None, open_positions=0, attempted=False,
                     quarantined=False).reason == "stale_volatility"   # ...but refused


def test_far_from_close_assembles_nothing(env):
    conn, clock, spot, tmp = env
    clock.t = CLOSE - timedelta(minutes=5)
    x = experiment(conn, clock, spot, tmp)
    assert run(x.make_inputs()) is None
    assert TICKER not in x.tracked              # book capture starts only near the close


# ---- book capture and replay -----------------------------------------------------------

SEQ = {"n": 0}


def snapshot_event(clock, gen=1, bid="0.05", ticker=None, sid=3):
    SEQ["n"] += 1
    raw = json.dumps({"type": "orderbook_snapshot", "sid": sid, "seq": SEQ["n"],
                      "msg": {"market_ticker": ticker or TICKER, "yes_dollars_fp": [],
                              "no_dollars_fp": [[bid, "2.00"]]}})
    return {"t": clock().isoformat(), "kind": "msg", "gen": gen, "raw": raw}


def feed_live(x, clock, gen=1, bid="0.05", up=True):
    if up:
        SEQ["n"] = 0
        x.on_book_event({"t": clock().isoformat(), "kind": "up", "gen": gen})
    clock.t += timedelta(milliseconds=100)
    x.on_book_event(snapshot_event(clock, gen, bid))
    for _ in range(4):
        clock.t += timedelta(milliseconds=500)
        x.on_book_event({"t": clock().isoformat(), "kind": "alive", "gen": gen})


def test_market_tracked_after_socket_up_becomes_usable(env):
    # Codex repro: up -> track -> snapshot -> alive used to stay unusable.
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    x.on_book_event({"t": clock().isoformat(), "kind": "up", "gen": 5})
    SEQ["n"] = 0
    clock.t += timedelta(seconds=1)
    run(x.make_inputs())                        # starts tracking on a live socket
    feed_live(x, clock, gen=5, up=False)
    assert x.stream.state_at(TICKER, clock()) is not None
    assert x.tape_for(TICKER, clock()).state_at(clock()) is not None


def test_second_window_on_same_connection_without_reconnect(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    run(x.make_inputs())
    feed_live(x, clock)
    clock.t = CLOSE + timedelta(seconds=20)
    run(x.maintain_tracking())                  # first window finalized
    nxt = "KXBTC15M-26OCT300715-15"
    x.track(nxt, CLOSE + timedelta(minutes=15))
    x.on_book_event(snapshot_event(clock, ticker=nxt))
    for _ in range(3):
        clock.t += timedelta(milliseconds=500)
        x.on_book_event({"t": clock().isoformat(), "kind": "alive", "gen": 1})
    assert x.stream.state_at(nxt, clock()) is not None
    # Its recording starts with an attach header and replays to the same book.
    events = sx.BookRecorder.load(x.recorders[nxt].path)
    assert events[0]["kind"] == "attach"
    assert sx.BookTape.from_events(nxt, events).state_at(clock()) == x.stream.state_at(nxt, clock())


def test_gap_triggers_automatic_snapshot_request(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)

    class MD:
        connected = True
        asked = []

        def set_markets(self, m):
            pass

        async def request_snapshot(self, tickers):
            MD.asked.append(list(tickers))
    x.md = MD()
    run(x.make_inputs())
    feed_live(x, clock)
    SEQ["n"] += 5                               # the stream skipped messages
    x.on_book_event(snapshot_event(clock, ticker="KXOTHER-1"))
    assert x.stream.state_at(TICKER, clock()) is None
    run(x.recover_books())
    assert MD.asked == [[TICKER]]
    run(x.recover_books())                      # rate-limited: not asked again at once
    assert MD.asked == [[TICKER]]


def test_book_events_recorded_and_replayed_identically(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    run(x.make_inputs())
    feed_live(x, clock)
    t = clock()
    live_state = x.stream.state_at(TICKER, t)
    assert live_state is not None and live_state.no[0][0] == D("0.05")
    clock.t = CLOSE + timedelta(seconds=20)
    run(x.maintain_tracking())                  # past close + tail: finalize to .gz
    assert TICKER not in x.tracked
    path, sha = conn.execute("SELECT path, sha256 FROM rx_book_files").fetchone()
    assert path.endswith(".jsonl.gz") and len(sha) == 64
    assert x.tape_for(TICKER, t).state_at(t) == live_state


def test_restart_uses_recorded_history_for_earlier_instants(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    run(x.make_inputs())
    feed_live(x, clock)
    t_before = clock()
    before = x.stream.state_at(TICKER, t_before)
    x.finalize_open_recorders()                 # process stops
    clock.t += timedelta(seconds=1)
    y = experiment(conn, clock, spot, tmp)      # new process, new connection generation
    run(y.make_inputs())
    assert y.tape_for(TICKER, t_before).state_at(t_before) == before    # from the file
    feed_live(y, clock, gen=2, bid="0.04")
    assert y.tape_for(TICKER, clock()).state_at(clock()).no[0][0] == D("0.04")
    events = sx.BookRecorder.load(y.recorders[TICKER].path)
    assert [e["kind"] for e in events].count("attach") == 2


def test_tampered_or_missing_book_file_is_not_used(env):
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
    Path(path).unlink()
    assert x.tape_for(TICKER, clock()) is None


def test_book_event_for_other_market_on_same_sid_keeps_ours(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    run(x.make_inputs())
    feed_live(x, clock)
    x.on_book_event(snapshot_event(clock, ticker="KXETH15M-X"))       # next seq, same sid
    assert x.stream.state_at(TICKER, clock()) is not None


def test_candle_series_validation():
    now = datetime(2026, 10, 30, 10, 59, 35, tzinfo=UTC)
    base = int(now.timestamp() // 60 * 60 * 1000)
    good = [{"t": base - (40 - i) * 60_000, "c": str(77500 + i)} for i in range(41)]
    closes, newest = rx.candle_series(list(reversed(good)), now, window=30)  # unordered input
    assert closes == [77500.0 + i for i in range(41)] and newest.timestamp() * 1000 == base
    dup = good + [dict(good[-1])]
    assert rx.candle_series(dup, now, window=30)[0] == closes                 # exact duplicate
    for bad in (good + [{"t": base + 86_400_000, "c": "1"}],                   # future
                good + [dict(good[-1], c="99999")],                            # conflicting dup
                good[:20] + good[21:],                                         # missing minute
                good[:-1] + [{"t": base, "c": "nan"}],                         # malformed close
                good[:-1] + [{"t": base + 1, "c": "1"}],                       # not a minute boundary
                good[:10]):                                                    # too few
        with pytest.raises(rx.CandleError):
            rx.candle_series(bad, now, window=30)


def test_invalid_candles_never_produce_a_forecast(env):
    conn, clock, spot, tmp = env

    def bad_post(c):
        async def post(req):
            end = int(c().timestamp() // 60 * 60 * 1000)
            rows = [{"t": end - (40 - i) * 60_000, "c": "77500"} for i in range(41)]
            rows.append({"t": end + 3_600_000, "c": "77500"})               # future candle
            return 200, json.dumps(rows).encode()
        return post
    x = experiment(conn, clock, spot, tmp, post=bad_post(clock))
    inp = run(x.make_inputs())
    assert inp.sigma_1m is None
    assert rx.decide(inp, None, open_positions=0, attempted=False,
                     quarantined=False).reason == "missing_volatility"


# ---- startup ---------------------------------------------------------------------------

class FakeMD:
    """One shared-seq orderbook subscription; snapshots on add or request."""

    def __init__(self, on_event, clock):
        self.on_event, self.clock = on_event, clock
        self.connected, self.last_error, self.markets = False, None, set()
        self.pending, self.seq = set(), 0

    def set_markets(self, m):
        self.pending |= set(m) - self.markets
        self.markets = set(m)

    async def request_snapshot(self, tickers):
        self.pending |= set(tickers)
        return True

    async def run(self, stop):
        self.connected = True
        self.on_event({"t": self.clock().isoformat(), "kind": "up", "gen": 1})
        while not stop.is_set():
            for t in sorted(self.pending & self.markets):
                self.seq += 1
                self.on_event({"t": self.clock().isoformat(), "kind": "msg", "gen": 1,
                               "raw": json.dumps({"type": "orderbook_snapshot", "sid": 3,
                                                  "seq": self.seq,
                                                  "msg": {"market_ticker": t, "yes_dollars_fp": [],
                                                          "no_dollars_fp": [["0.05", "2.00"]]}})})
            self.pending.clear()
            self.on_event({"t": self.clock().isoformat(), "kind": "alive", "gen": 1})
            await asyncio.sleep(0.005)
        self.connected = False


class SettlingKalshi(Kalshi):
    async def __call__(self, url, params):
        self.result = "yes" if self.clock() > CLOSE + timedelta(seconds=5) else ""
        return await super().__call__(url, params)


def start_run(env, monkeypatch, *, spot_reads, start, real_s=0.4, step=None, prints=None):
    conn, clock, spot, tmp = env
    import kalshi_auth
    monkeypatch.setattr(kalshi_auth, "is_paper", lambda env=None: True)
    clock.t = start

    async def go():
        stop = asyncio.Event()

        async def advance():
            while step and not stop.is_set():
                await asyncio.sleep(0.01)
                clock.t += step
        adv = asyncio.ensure_future(advance())
        task = asyncio.ensure_future(live.run(
            lambda: {"research_c15_enabled": True}, stop,
            make_md=lambda on_event, on_cf: FakeMD(on_event, clock), send=SettlingKalshi(clock),
            candle_post=candles(clock), clock=clock, spot_reads=spot_reads,
            prints=prints or (lambda c, n: (0.0, 0, "none")),
            local_time=lambda: clock().timestamp(), tick_s=0.005))
        await asyncio.sleep(real_s)
        stop.set()
        await asyncio.wait_for(task, 10)
        await adv
    asyncio.run(go())


def test_paper_startup_waits_for_a_usable_book_and_spot(env, monkeypatch):
    conn, clock, spot, tmp = env
    start_run(env, monkeypatch, spot_reads=lambda: [], start=CLOSE - timedelta(seconds=70))
    assert conn.execute("SELECT status, launched_at FROM rx_cohorts").fetchall() == [("armed", None)]
    # Outside the book window nothing is tracked, so readiness waits too.
    fresh = lambda: [("coinbase_ws", clock().timestamp() - 1, 77700.0)]  # noqa: E731
    start_run(env, monkeypatch, spot_reads=fresh, start=CLOSE - timedelta(minutes=10))
    assert conn.execute("SELECT status FROM rx_cohorts").fetchall() == [("armed",)]


def test_startup_to_settlement_end_to_end(env, monkeypatch):
    """Paper start -> book usable -> launch -> eligible decision -> admission
    -> simulated fills after each latency -> settlement, all from the
    production-shaped captured payloads."""
    conn, clock, spot, tmp = env
    fresh = lambda: [("coinbase_ws", clock().timestamp() - 1, 77700.0)]  # noqa: E731

    def prints(close_e, now_e):
        n = int(60 - (close_e - now_e))
        return (77700.0 * n, n, "coinbase_proxy") if n > 0 else (0.0, 0, "none")
    start_run(env, monkeypatch, spot_reads=fresh, prints=prints, start=CLOSE - timedelta(seconds=80),
              step=timedelta(seconds=0.5), real_s=3.0)
    assert conn.execute("SELECT status FROM rx_cohorts").fetchone()[0] == "active"
    eligible = conn.execute("SELECT COUNT(*) FROM rx_decisions WHERE eligible=1").fetchone()[0]
    assert eligible == 1                                       # one attempt per event
    st = dict(conn.execute("SELECT latency_s, state FROM rx_orders").fetchall())
    assert st == {1: "filled", 2: "filled", 5: "filled"}
    mid = conn.execute("SELECT manifest_id FROM rx_cohorts").fetchone()[0]
    import research_wallet as rw
    w = rw.get_wallet(conn, rx.wallet_id(mid, 1))
    assert w.cash == D(4) - D("0.9534") + 1 and w.reserved == 0          # bought at 95c, won
    assert rw.reconcile(conn, rx.wallet_id(mid, 1))["ok"]


def test_startup_failure_releases_resources(env, monkeypatch):
    conn, clock, spot, tmp = env
    import kalshi_auth
    monkeypatch.setattr(kalshi_auth, "is_paper", lambda env=None: True)

    def boom():
        raise rx.ExperimentError("cannot freeze")
    monkeypatch.setattr(rx, "manifest_body", boom)

    async def go():
        await live.run(lambda: {"research_c15_enabled": True}, asyncio.Event(),
                       make_md=lambda on_event, on_cf: FakeMD(on_event, clock), send=Kalshi(clock),
                       candle_post=candles(clock), clock=clock, spot_reads=lambda: [],
                       prints=lambda c, n: (0.0, 0, "none"))
    with pytest.raises(rx.ExperimentError):
        asyncio.run(go())
    assert conn.execute("SELECT COUNT(*) FROM rx_cohorts").fetchone()[0] == 0



# ---- fee identity bound to the cohort (review) ------------------------------------------

def test_fee_contexts_use_the_requested_account_and_its_persisted_precision(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    import fee_context as fctx
    conn.execute("BEGIN")
    fctx.register_account(conn, kalshi_env="paper", account_id="new-research-account",
                          precision="0.01", precision_source="configured")
    conn.commit()
    inp = run(x.make_inputs())
    old = run(x.prepare_fees(market_ticker=TICKER, decided_at=inp.decided_at, arrivals={},
                             account_id="research-c15", policy=fctx.CONDITIONAL_PAPER))[0]
    new = run(x.prepare_fees(market_ticker=TICKER, decided_at=inp.decided_at, arrivals={},
                             account_id="new-research-account", policy=fctx.CONDITIONAL_PAPER))[0]
    assert (old.account_id, old.precision) == ("research-c15", D("0.0001"))
    assert (new.account_id, new.precision) == ("new-research-account", D("0.01"))
    ev = fsvc.evidence_of(x._snaps[1])
    rebuilt = x.rebuild_fee(ev, inp.decided_at, account_id="research-c15",
                            policy=fctx.CONDITIONAL_PAPER)
    assert rebuilt == old


# ---- bounded memory over a long cohort (review) ---------------------------------------

def test_many_finalized_windows_keep_memory_bounded_and_replay_unchanged(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    x.on_book_event({"t": clock().isoformat(), "kind": "up", "gen": 1})
    SEQ["n"] = 0
    seen = {}
    for i in range(50):
        tk = f"KXBTC15M-W{i:02d}-00"
        close = clock() + timedelta(seconds=60)
        x.track(tk, close)
        clock.t += timedelta(milliseconds=100)
        x.on_book_event(snapshot_event(clock, ticker=tk, bid=f"0.0{1 + i % 8}"))
        for _ in range(6):
            clock.t += timedelta(milliseconds=500)
            x.on_book_event({"t": clock().isoformat(), "kind": "alive", "gen": 1})
        probe = clock() - timedelta(seconds=1)
        seen[tk] = (probe, x.stream.state_at(tk, probe, confirmed=True))
        assert seen[tk][1] is not None
        clock.t = close + timedelta(seconds=20)
        run(x.maintain_tracking())                 # untrack + finalize + trim
        x.untrack(tk)
    assert len(x.stream.markets) == 0 and not x.tracked
    # 50 windows produced 300+ liveness marks; only the last LIVE_HISTORY is kept.
    assert x.stream.history_size() < 100
    for tk, (probe, state) in seen.items():
        assert x.tape_for(tk, probe).state_at(probe, confirmed=True) == state
    assert len(x._replayed) <= live.REPLAY_CACHE


def test_old_instant_of_a_tracked_market_falls_back_to_disk_after_trim(env):
    conn, clock, spot, tmp = env
    x = experiment(conn, clock, spot, tmp)
    run(x.make_inputs())
    feed_live(x, clock)
    probe = clock() - timedelta(seconds=1)
    before = x.stream.state_at(TICKER, probe, confirmed=True)
    x.evidence_barrier(TICKER)
    x.stream.trim(clock() + timedelta(minutes=1))   # memory no longer reaches `probe`
    assert x.tape_for(TICKER, probe).state_at(probe, confirmed=True) == before


def test_switch_file_enables_without_config_key(env, monkeypatch):
    conn, clock, spot, tmp = env
    import research_preflight as pf
    (pf.data_dir() / pf.SWITCH_NAME).write_text(json.dumps({"enabled": True}))
    import kalshi_auth
    monkeypatch.setattr(kalshi_auth, "is_paper", lambda env=None: True)
    clock.t = CLOSE - timedelta(seconds=70)
    fresh = lambda: [("coinbase_ws", clock().timestamp() - 1, 77700.0)]  # noqa: E731

    async def go():
        stop = asyncio.Event()
        task = asyncio.ensure_future(live.run(
            lambda: {}, stop, make_md=lambda on_event, on_cf: FakeMD(on_event, clock),
            send=SettlingKalshi(clock), candle_post=candles(clock), clock=clock, spot_reads=fresh,
            prints=lambda c, n: (0.0, 0, "none"), local_time=lambda: clock().timestamp(),
            tick_s=0.005))
        await asyncio.sleep(0.4)
        stop.set()
        await asyncio.wait_for(task, 10)
    asyncio.run(go())
    assert conn.execute("SELECT status FROM rx_cohorts").fetchone()[0] == "active"


def test_default_spot_and_print_sources_are_bound_to_the_experiment(env, monkeypatch):
    """run() without injected spot/print readers uses the production defaults,
    bound to the experiment's own CF state (regression: the unbound defaults
    raised on every tick, so the cohort could never launch)."""
    conn, clock, spot, tmp = env
    import kalshi_auth
    import spot_ws
    monkeypatch.setattr(kalshi_auth, "is_paper", lambda env=None: True)

    class FakeClient:
        connected = True

        @property
        def prices(self):
            return {rx.PARAMS["asset"]: (clock().timestamp() - 1, 77700.0)}
    monkeypatch.setattr(spot_ws, "_client", FakeClient())
    monkeypatch.setattr(spot_ws, "is_running", lambda: True)
    monkeypatch.setattr(spot_ws, "window_partial", lambda a, c, now=None: (0.0, 0))
    clock.t = CLOSE - timedelta(seconds=80)

    async def go():
        stop = asyncio.Event()

        async def advance():
            while not stop.is_set():
                await asyncio.sleep(0.01)
                clock.t += timedelta(seconds=0.5)
        adv = asyncio.ensure_future(advance())
        task = asyncio.ensure_future(live.run(
            lambda: {"research_c15_enabled": True}, stop,
            make_md=lambda on_event, on_cf: FakeMD(on_event, clock), send=SettlingKalshi(clock),
            candle_post=candles(clock), clock=clock,
            local_time=lambda: clock().timestamp(), tick_s=0.005))
        await asyncio.sleep(1.5)
        stop.set()
        await asyncio.wait_for(task, 10)
        await adv
    asyncio.run(go())
    reasons = [r[0] for r in conn.execute("SELECT reason FROM rx_downtime")]
    assert not any("argument" in r for r in reasons), reasons
    assert conn.execute("SELECT status FROM rx_cohorts").fetchone()[0] == "active"
