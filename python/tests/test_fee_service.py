"""Tests for fee_service.py (A1 capture service). No network: a scripted
async send with a controllable clock. Stdlib-only:
    python -m pytest tests/test_fee_service.py --noconftest
"""
from __future__ import annotations

import asyncio
import email.utils
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db  # noqa: E402
import fee_capture as fcap  # noqa: E402
import fee_context as fctx  # noqa: E402
import fee_schedule as fs  # noqa: E402
import fee_service as fsvc  # noqa: E402
from fee_capture import RawResponse  # noqa: E402

UTC = timezone.utc
T0 = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
MKT, EV, SER = "KXBTC15M-26OCT301200-00", "KXBTC15M-26OCT301200", "KXBTC15M"


class Clock:
    def __init__(self):
        self.t = T0

    def __call__(self):
        return self.t


class Net:
    """Scripted Kalshi. `fail` maps a path to a list of statuses/exceptions to
    produce before succeeding; `skew` shifts the server Date."""
    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.fail = {}
        self.skew = timedelta(0)
        self.date = True
        self.cache_control = "public, max-age=15"
        self.event_override = None
        self.event_pages = None

    def body(self, path, params):
        if path == f"/markets/{MKT}":
            return {"market": {"ticker": MKT, "event_ticker": EV}}
        if path == f"/events/{EV}":
            e = {"event_ticker": EV, "series_ticker": SER}
            if self.event_override is not None:
                e["fee_multiplier_override"] = self.event_override
            return {"event": e}
        if path == f"/series/{SER}":
            return {"series": {"ticker": SER, "fee_type": "quadratic", "fee_multiplier": 1}}
        if path == "/series/fee_changes":
            return {"series_fee_change_arr": []}
        if path == "/events/fee_changes":
            if self.event_pages:
                return self.event_pages[params.get("cursor", "")]
            return {"cursor": "", "event_fee_changes": []}
        return None

    async def __call__(self, url, params):
        path = url.split("/trade-api/v2", 1)[1]
        self.calls.append(path)
        await asyncio.sleep(0)  # let concurrent callers interleave
        plan = self.fail.get(path)
        if plan:
            item = plan.pop(0)
            if isinstance(item, Exception):
                raise item
            return RawResponse(item, {"Cache-Control": "no-store", "Retry-After": "1",
                                      **self._date()}, b"{}")
        b = self.body(path, params)
        if b is None:
            return RawResponse(404, {"Cache-Control": "no-store", **self._date()}, b"{}")
        h = {"Cache-Control": self.cache_control, **self._date()}
        return RawResponse(200, h, json.dumps(b).encode())

    def _date(self):
        if not self.date:
            return {}
        return {"Date": email.utils.format_datetime(self.clock() + self.skew, usegmt=True)}


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    db.init_db()
    conn = sqlite3.connect(str(db.db_path()))
    fctx.register_account(conn, kalshi_env="paper", account_id="research-1",
                          precision="0.0001", precision_source="configured")
    clock = Clock()
    net = Net(clock)
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    cap = fcap.FeeMetadataCapturer(net, fcap.CaptureStore(tmp_path / "ev"), clock=clock)
    svc = fsvc.FeeMetadataService(cap, sleep=fake_sleep, skew_bound_s=5)
    yield conn, clock, net, svc, sleeps
    conn.close()


def run(c):
    return asyncio.run(c)


def context(conn, svc, clock, role="taker", policy=fctx.CONDITIONAL_PAPER):
    return run(svc.context_for(conn, kalshi_env="paper", account_id="research-1", policy=policy,
                               market_ticker=MKT, role=role, at=clock() + timedelta(seconds=1)))


def test_context_resolves_from_fresh_captures(env):
    conn, clock, net, svc, _ = env
    c = context(conn, svc, clock)
    assert c.usable and c.resolution.coefficient == D("0.07")
    assert len(c.evidence) == 5 and c.policy == fctx.CONDITIONAL_PAPER


def test_strict_policy_still_fails_closed(env):
    conn, clock, net, svc, _ = env
    c = context(conn, svc, clock, policy=fctx.STRICT)
    assert not c.usable and "history_unverified" in c.reason


def test_fresh_snapshots_reused_stale_refetched(env):
    conn, clock, net, svc, _ = env
    context(conn, svc, clock)
    n = len(net.calls)
    clock.t += timedelta(seconds=10)
    context(conn, svc, clock)
    assert len(net.calls) == n                       # within TTL: no new requests
    clock.t += timedelta(seconds=40)
    context(conn, svc, clock)
    assert len(net.calls) == 2 * n                   # stale: everything refetched


def test_concurrent_identical_requests_coalesce(env):
    conn, clock, net, svc, _ = env

    async def both():
        b = svc.capturer.new_batch()
        return await asyncio.gather(svc.series(SER, batch_id=b, context={}),
                                    svc.series(SER, batch_id=b, context={}))
    a, b = run(both())
    assert a is b and net.calls.count(f"/series/{SER}") == 1


def test_429_retried_with_backoff_then_succeeds(env):
    conn, clock, net, svc, sleeps = env
    net.fail[f"/series/{SER}"] = [429, 503]
    c = context(conn, svc, clock)
    assert c.usable and net.calls.count(f"/series/{SER}") == 3
    assert sleeps == [1.0, 1.0]                      # Retry-After 1 >= backoff 0.5, 1.0


def test_retries_exhausted_gives_unusable_context(env):
    conn, clock, net, svc, sleeps = env
    net.fail[f"/series/{SER}"] = [429, 429, 429]
    c = context(conn, svc, clock)
    assert not c.usable and c.reason.startswith("metadata_unavailable")
    assert c.resolution is None and len(sleeps) == 2


def test_non_retryable_status_fails_immediately(env):
    conn, clock, net, svc, sleeps = env
    net.fail[f"/events/{EV}"] = [403]
    c = context(conn, svc, clock)
    assert not c.usable and sleeps == [] and net.calls.count(f"/events/{EV}") == 1


def test_transport_error_then_reconnect(env):
    conn, clock, net, svc, _ = env
    net.fail[f"/markets/{MKT}"] = [ConnectionResetError("reset")]
    assert context(conn, svc, clock).usable


@pytest.mark.parametrize("skew", [timedelta(seconds=-20), timedelta(seconds=20)])
def test_clock_skew_beyond_bound_refused(env, skew):
    conn, clock, net, svc, _ = env
    net.skew = skew
    c = context(conn, svc, clock)
    assert not c.usable and "skew" in c.reason


def test_small_skew_accepted(env):
    conn, clock, net, svc, _ = env
    net.skew = timedelta(seconds=3)
    assert context(conn, svc, clock).usable


def test_missing_date_header_refused(env):
    conn, clock, net, svc, _ = env
    net.date = False
    c = context(conn, svc, clock)
    assert not c.usable and "Date" in c.reason


@pytest.mark.parametrize("cc", ["public, max-age=15, max-age=30", "public", None])
def test_unknown_cache_age_refused(env, cc):
    conn, clock, net, svc, _ = env
    net.cache_control = cc
    assert not context(conn, svc, clock).usable


def test_partial_pagination_refused(env):
    conn, clock, net, svc, _ = env
    net.event_pages = {"": {"cursor": "k1", "event_fee_changes": []},
                       "k1": {"cursor": "k1", "event_fee_changes": []}}  # never progresses
    c = context(conn, svc, clock)
    assert not c.usable and c.reason.startswith("metadata_unavailable")


def test_multi_page_listing_window_spans_pages(env):
    conn, clock, net, svc, _ = env
    net.event_pages = {"": {"cursor": "k1", "event_fee_changes": []},
                       "k1": {"cursor": "", "event_fee_changes": []}}
    c = context(conn, svc, clock)
    assert c.usable and len(c.evidence) == 6


def test_changed_override_seen_after_ttl(env):
    conn, clock, net, svc, _ = env
    assert context(conn, svc, clock).resolution.coefficient == D("0.07")
    net.event_override = 2
    clock.t += timedelta(seconds=5)
    assert context(conn, svc, clock).resolution.coefficient == D("0.07")   # cached, still valid
    clock.t += timedelta(seconds=60)
    assert context(conn, svc, clock).resolution.coefficient == D("0.14")   # refreshed


def test_context_reproduces_from_stored_captures(env):
    conn, clock, net, svc, _ = env
    c = context(conn, svc, clock)
    store = svc.capturer.store
    snaps = {}
    recs = {r.capture_id: r for r in store.records()}
    for key, cid in zip(("market", "event", "series", "series_changes"), c.evidence[:4]):
        rec, body = store.get(cid)
        snaps[key] = fsvc.Snapshot(rec, body, rec.as_of(clock_skew_s=svc.skew_bound_s))
    pages = tuple(fsvc.Snapshot(recs[cid], store.get(cid)[1],
                                recs[cid].as_of(clock_skew_s=svc.skew_bound_s))
                  for cid in c.evidence[4:])
    snaps["event_changes"] = fsvc.ListingSnapshot(
        pages, fs.AsOf(min(p.as_of.lo for p in pages), max(p.as_of.hi for p in pages)))
    rebuilt = fctx.build_fee_context(conn, kalshi_env="paper", account_id="research-1",
                                     policy=c.policy, role="taker", at=c.resolution.at,
                                     ttl=svc.ttl, evidence=c.evidence,
                                     **fsvc.parse_snapshots(snaps))
    assert rebuilt == c


def test_unregistered_account_unusable_even_with_good_metadata(env):
    conn, clock, net, svc, _ = env
    c = run(svc.context_for(conn, kalshi_env="paper", account_id="nobody",
                            policy=fctx.CONDITIONAL_PAPER, market_ticker=MKT, role="taker",
                            at=clock()))
    assert not c.usable and c.reason == "account_not_registered"


def test_refresh_survives_one_market_failing(env):
    conn, clock, net, svc, _ = env
    out = run(svc.refresh([MKT, "KXNOPE-1"]))
    assert out[MKT] is None and out["KXNOPE-1"] is not None


def test_monitor_loop_stops(env):
    conn, clock, net, svc, _ = env

    async def go():
        stop = asyncio.Event()
        task = asyncio.create_task(svc.run_monitor(lambda: [MKT], stop, interval_s=0.01))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, 1)
    run(go())
    assert net.calls.count(f"/markets/{MKT}") >= 1


def test_monitor_refresh_refetches_even_when_fresh(env):
    conn, clock, net, svc, _ = env
    run(svc.refresh([MKT]))
    n = len(net.calls)
    clock.t += timedelta(seconds=10)
    run(svc.refresh([MKT]))
    assert len(net.calls) == 2 * n
    assert all(r.context == {"purpose": "monitor"} for r in svc.capturer.store.records())
