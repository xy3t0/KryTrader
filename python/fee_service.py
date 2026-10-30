"""Fee metadata service (A1): keeps captured fee metadata fresh and turns it
into FeeContexts. Network failure can never yield a usable context.

  * Every fetch goes through FeeMetadataCapturer, so every input a context
    uses is a stored, hash-checked capture (contexts are reproducible).
  * A snapshot is fresh while now <= as_of.hi + ttl (default 30 s); stale
    or missing inputs are refetched before a context is built.
  * Identical in-flight requests are coalesced into one.
  * Retries are bounded: 429/5xx and transport errors back off (honouring a
    numeric Retry-After, capped); other HTTP errors fail immediately.
  * Clock skew is checked per response against the server Date + Age; a
    response without a usable Date, or skewed beyond the bound, is refused
    (unbounded clock error makes the observation window meaningless).
  * Discovery from a market ticker: market -> event -> series, plus the
    historical series change listing and the complete event change listing.
"""
from __future__ import annotations

import asyncio
import email.utils
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Awaitable, Callable, Iterable, Optional

import fee_capture as fcap
import fee_context as fctx
import fee_schedule as fs

DEFAULT_TTL = timedelta(seconds=30)
DEFAULT_SKEW_BOUND_S = 5.0
RETRYABLE = {429, 500, 502, 503, 504}
MAX_RETRY_AFTER_S = 30.0


class FeeServiceError(RuntimeError):
    """Metadata could not be obtained in a trustworthy form."""


@dataclass(frozen=True)
class Snapshot:
    record: fcap.CaptureRecord
    body: bytes
    as_of: fs.AsOf


@dataclass(frozen=True)
class ListingSnapshot:
    pages: tuple[Snapshot, ...]
    as_of: fs.AsOf          # spans the whole listing: first page lo .. last page hi


class FeeMetadataService:
    def __init__(self, capturer: fcap.FeeMetadataCapturer, *, ttl: timedelta = DEFAULT_TTL,
                 skew_bound_s: float = DEFAULT_SKEW_BOUND_S, max_attempts: int = 3,
                 backoff_s: float = 0.5,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                 clock: Optional[Callable[[], datetime]] = None):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.capturer = capturer
        self.ttl = ttl
        self.skew_bound_s = float(skew_bound_s)
        self.max_attempts = max_attempts
        self.backoff_s = backoff_s
        self.sleep = sleep
        self.clock = clock or capturer.clock
        self._cache: dict[tuple, object] = {}
        self._inflight: dict[tuple, asyncio.Future] = {}

    # ---- low level --------------------------------------------------------------

    def _fresh(self, as_of: fs.AsOf) -> bool:
        return self.clock() <= as_of.hi + self.ttl

    def _check_skew(self, rec: fcap.CaptureRecord) -> None:
        if not rec.date_header:
            raise FeeServiceError(f"capture {rec.capture_id}: no Date header; clock error unbounded")
        try:
            server = email.utils.parsedate_to_datetime(rec.date_header)
        except (TypeError, ValueError) as e:
            raise FeeServiceError(f"capture {rec.capture_id}: bad Date header") from e
        if server is None or server.tzinfo is None:
            raise FeeServiceError(f"capture {rec.capture_id}: Date header without timezone")
        age = int(rec.age_header) if (rec.age_header or "").strip().isdigit() else 0
        server_now = server + timedelta(seconds=age)
        # Date has 1 s resolution; the response arrived between sent and received.
        lo = rec.request_sent - timedelta(seconds=self.skew_bound_s + 1)
        hi = rec.response_received + timedelta(seconds=self.skew_bound_s)
        if not lo <= server_now <= hi:
            raise FeeServiceError(f"capture {rec.capture_id}: clock skew exceeds "
                                  f"{self.skew_bound_s}s bound")

    def _snapshot(self, rec: fcap.CaptureRecord, body: bytes) -> Snapshot:
        self._check_skew(rec)
        try:
            as_of = rec.as_of(clock_skew_s=self.skew_bound_s)
        except (fcap.FeeCaptureError, fs.FeeMetadataError) as e:
            raise FeeServiceError(str(e)) from e
        return Snapshot(rec, body, as_of)

    async def _with_retries(self, fn: Callable[[], Awaitable]):
        last: Optional[BaseException] = None
        for attempt in range(self.max_attempts):
            try:
                return await fn()
            except fcap.FeeCaptureError as e:
                last = e
                if e.status is not None and e.status not in RETRYABLE:
                    break
                delay = self.backoff_s * (2 ** attempt)
                if e.retry_after and e.retry_after.strip().isdigit():
                    delay = max(delay, min(float(e.retry_after.strip()), MAX_RETRY_AFTER_S))
            except (OSError, asyncio.TimeoutError, ConnectionError) as e:
                last = e
                delay = self.backoff_s * (2 ** attempt)
            except Exception as e:  # transport libraries raise their own types
                if isinstance(e, (FeeServiceError, fs.FeeMetadataError)):
                    raise
                last = e
                delay = self.backoff_s * (2 ** attempt)
            if attempt + 1 < self.max_attempts:
                await self.sleep(delay)
        raise FeeServiceError(f"fetch failed after {self.max_attempts} attempts: {last}") from last

    async def _coalesced(self, key: tuple, fn: Callable[[], Awaitable]):
        fut = self._inflight.get(key)
        if fut is not None:
            return await asyncio.shield(fut)
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._inflight[key] = fut
        try:
            result = await fn()
        except BaseException as e:
            fut.set_exception(e)
            fut.exception()  # mark retrieved; waiters re-raise it themselves
            raise
        else:
            fut.set_result(result)
            return result
        finally:
            self._inflight.pop(key, None)

    async def _get(self, key: tuple, fetch: Callable[[], Awaitable], *, force: bool = False):
        cached = self._cache.get(key)
        if cached is not None and not force and self._fresh(cached.as_of):
            return cached

        async def go():
            snap = await self._with_retries(fetch)
            self._cache[key] = snap
            return snap
        return await self._coalesced(key, go)

    # ---- typed fetches -----------------------------------------------------------

    async def _single(self, key: tuple, call, batch_id: str, context: dict, force=False) -> Snapshot:
        async def fetch():
            rec, body = await call(batch_id=batch_id, context=context)
            return self._snapshot(rec, body)
        return await self._get(key, fetch, force=force)

    async def market(self, ticker, *, batch_id, context, force=False):
        return await self._single(("market", ticker),
                                  lambda **k: self.capturer.market(ticker, **k), batch_id, context, force)

    async def event(self, event_ticker, *, batch_id, context, force=False):
        return await self._single(("event", event_ticker),
                                  lambda **k: self.capturer.event(event_ticker, **k), batch_id, context, force)

    async def series(self, series_ticker, *, batch_id, context, force=False):
        return await self._single(("series", series_ticker),
                                  lambda **k: self.capturer.series(series_ticker, **k), batch_id, context, force)

    async def series_changes(self, *, batch_id, context, force=False):
        return await self._single(
            ("series_changes",),
            lambda **k: self.capturer.series_fee_changes(show_historical=True, **k),
            batch_id, context, force)

    async def event_changes(self, event_ticker, *, batch_id, context, force=False) -> ListingSnapshot:
        async def fetch():
            pages = await self.capturer.event_fee_changes(
                event_ticker=event_ticker, batch_id=batch_id, context=context)
            snaps = tuple(self._snapshot(r, b) for r, b in pages)
            return ListingSnapshot(snaps, fs.AsOf(min(s.as_of.lo for s in snaps),
                                                  max(s.as_of.hi for s in snaps)))
        return await self._get(("event_changes", event_ticker), fetch, force=force)

    # ---- context -------------------------------------------------------------------

    async def gather(self, market_ticker: str, *, context: Optional[dict] = None,
                     force: bool = False) -> dict:
        """Fresh snapshots for one market, discovering its event and series.
        force=True refetches everything regardless of freshness."""
        ctx = dict(context or {})
        batch = self.capturer.new_batch()
        m = await self.market(market_ticker, batch_id=batch, context=ctx, force=force)
        mk = fs.loads(m.body)
        mk = mk.get("market") if isinstance(mk, dict) else None
        if not isinstance(mk, dict) or not isinstance(mk.get("event_ticker"), str):
            raise FeeServiceError("market payload has no event_ticker")
        e = await self.event(mk["event_ticker"], batch_id=batch, context=ctx, force=force)
        ev = fs.loads(e.body)
        ev = ev.get("event") if isinstance(ev, dict) else None
        if not isinstance(ev, dict) or not isinstance(ev.get("series_ticker"), str):
            raise FeeServiceError("event payload has no series_ticker")
        s, sc, ec = await asyncio.gather(
            self.series(ev["series_ticker"], batch_id=batch, context=ctx, force=force),
            self.series_changes(batch_id=batch, context=ctx, force=force),
            self.event_changes(mk["event_ticker"], batch_id=batch, context=ctx, force=force),
        )
        return {"market": m, "event": e, "series": s, "series_changes": sc, "event_changes": ec}

    async def context_for(self, conn: sqlite3.Connection, *, kalshi_env: str, account_id: str,
                          policy: str, market_ticker: str, role: str, at: datetime,
                          context: Optional[dict] = None) -> fctx.FeeContext:
        """A FeeContext for one decision. Any fetch, parse or skew failure
        yields an unusable context with the reason; never a usable one."""
        evidence: tuple[str, ...] = ()
        try:
            snaps = await self.gather(market_ticker, context=context)
            evidence = tuple(
                [snaps[k].record.capture_id for k in ("market", "event", "series", "series_changes")]
                + [p.record.capture_id for p in snaps["event_changes"].pages])
            parsed = parse_snapshots(snaps)
            return fctx.build_fee_context(
                conn, kalshi_env=kalshi_env, account_id=account_id, policy=policy,
                role=role, at=at, ttl=self.ttl, evidence=evidence, **parsed)
        except (FeeServiceError, fcap.FeeCaptureError, fs.FeeMetadataError) as e:
            return fctx.FeeContext(kalshi_env, account_id, None, Decimal(0), "", policy, None,
                                   evidence, None, False, f"metadata_unavailable:{e}")

    async def refresh(self, market_tickers: Iterable[str]) -> dict[str, Optional[str]]:
        """Force-refresh monitored markets (the 10 s loop calls this).
        Returns ticker -> error string (None on success); never raises."""
        out: dict[str, Optional[str]] = {}
        for t in market_tickers:
            try:
                await self.gather(t, context={"purpose": "monitor"}, force=True)
                out[t] = None
            except Exception as e:  # monitoring must survive one market failing
                out[t] = f"{type(e).__name__}: {e}"
        return out

    async def run_monitor(self, get_tickers: Callable[[], Iterable[str]], stop: asyncio.Event,
                          *, interval_s: float = 10.0) -> None:
        while not stop.is_set():
            await self.refresh(list(get_tickers()))
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_s)
            except asyncio.TimeoutError:
                pass


def evidence_of(snaps: dict) -> dict:
    """Capture ids behind a gathered snapshot set (JSON-serialisable)."""
    return {"market": snaps["market"].record.capture_id,
            "event": snaps["event"].record.capture_id,
            "series": snaps["series"].record.capture_id,
            "series_changes": snaps["series_changes"].record.capture_id,
            "event_changes": [p.record.capture_id for p in snaps["event_changes"].pages]}


def evidence_tuple(ev: dict) -> tuple[str, ...]:
    return (ev["market"], ev["event"], ev["series"], ev["series_changes"], *ev["event_changes"])


def snapshots_from_evidence(service: "FeeMetadataService", ev: dict) -> dict:
    """Rebuild a snapshot set from stored captures, re-running the same skew
    and cache checks. Deterministic: the same evidence always rebuilds the
    same snapshots. Raises FeeServiceError if anything is missing/corrupt."""
    store = service.capturer.store
    try:
        def one(cid):
            rec, body = store.get(cid)
            return service._snapshot(rec, body)
        pages = tuple(one(c) for c in ev["event_changes"])
        if not pages:
            raise FeeServiceError("no event change pages in evidence")
        return {"market": one(ev["market"]), "event": one(ev["event"]),
                "series": one(ev["series"]), "series_changes": one(ev["series_changes"]),
                "event_changes": ListingSnapshot(pages, fs.AsOf(min(p.as_of.lo for p in pages),
                                                                max(p.as_of.hi for p in pages)))}
    except (KeyError, TypeError) as e:
        raise FeeServiceError(f"malformed evidence: {e}") from e
    except fcap.FeeCaptureError as e:
        raise FeeServiceError(str(e)) from e


def context_from_snapshots(service: "FeeMetadataService", conn, snaps: dict, *, kalshi_env: str,
                           account_id: str, policy: str, role: str,
                           at: datetime) -> fctx.FeeContext:
    """A FeeContext from an already-gathered (or rebuilt) snapshot set."""
    try:
        return fctx.build_fee_context(conn, kalshi_env=kalshi_env, account_id=account_id,
                                      policy=policy, role=role, at=at, ttl=service.ttl,
                                      evidence=evidence_tuple(evidence_of(snaps)),
                                      **parse_snapshots(snaps))
    except fs.FeeMetadataError as e:
        return fctx.FeeContext(kalshi_env, account_id, None, Decimal(0), "", policy, None,
                               evidence_tuple(evidence_of(snaps)), None, False,
                               f"metadata_unavailable:{e}")


def parse_snapshots(snaps: dict) -> dict:
    """Parse stored snapshots into resolver inputs. Coverage starts as NONE;
    the policy in fee_context decides what coverage, if any, applies. Pure:
    the same captures always parse to the same inputs (reproducibility)."""
    m, e, s, sc, ec = (snaps[k] for k in ("market", "event", "series", "series_changes",
                                          "event_changes"))
    mk = fs.loads(m.body)
    return {
        "market": fs.parse_market(mk.get("market") if isinstance(mk, dict) else mk, m.as_of),
        "event": fs.parse_event(fs.loads(e.body), e.as_of),
        "series": fs.parse_series(fs.loads(s.body), s.as_of),
        "series_changes": fs.parse_series_fee_changes(
            fs.loads(sc.body), sc.as_of, include_historical=True, coverage=fs.Coverage.none()),
        "event_changes": fs.parse_event_fee_change_pages(
            [fs.loads(p.body) for p in ec.pages], ec.as_of, coverage=fs.Coverage.none()),
    }
