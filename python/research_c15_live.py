"""Live adapters for the frozen C15 experiment: real feeds in, paper out.

Nothing here can place an order. Inputs:
  * market discovery, metadata, settlement rules and results: captured Kalshi
    REST responses (fee_capture), so every input is stored evidence;
  * order book: research_md's read-only websocket (production key), turned
    into a BookTape and persisted per market window by BookRecorder;
  * spot: CF index frames (via research_md) when fresh, else the Coinbase
    websocket, each with its own receipt time (converted to the
    server-aligned clock);
  * settlement prints: the CF partial average when present, else the
    Coinbase one-per-second sampler (recorded as a proxy);
  * volatility: Hyperliquid 1-minute candles, stored as evidence, refreshed
    every 60 s, rejected when older than PARAMS limits.

Decision timing: all awaits (discovery, metadata, candles) happen FIRST;
then decided_at is read from the clock and every live value is read
synchronously, so nothing received after decided_at can influence it.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import fee_capture as fcap
import fee_context as fctx
import fee_schedule as fs
import fee_service as fsvc
import research_c15 as rx
import sim_exec

logger = logging.getLogger(__name__)

HYPERLIQUID_URL = "https://api.hyperliquid.xyz/info"
TRACK_LEAD = timedelta(seconds=150)       # start book capture this long before close
TRACK_TAIL = timedelta(seconds=15)        # keep it this long after close
LIVE_HISTORY = timedelta(minutes=15)      # in-memory stream history kept (older: disk replay)
REPLAY_CACHE = 4                          # finalized windows kept decoded in memory
DECISION_LEAD = timedelta(seconds=65)     # only assemble inputs this close to the close
CANDLE_REFRESH = timedelta(seconds=60)
DISCOVERY_REFRESH = timedelta(seconds=60)
CANDLE_LOOKBACK_MIN = 90


def server_now() -> datetime:
    import kalshi_auth
    return datetime.fromtimestamp(kalshi_auth.server_now(), timezone.utc)


def default_spot_reads(cf_state) -> list[tuple[str, float, float]]:
    """(source, local_epoch_received, price) for every live BTC spot. CF
    values come from the experiment's PRIVATE cf state (fed only by its own
    socket), never the app-wide cache."""
    import spot_ws
    out = []
    v = cf_state.values.get(rx.PARAMS["asset"]) if cf_state is not None else None
    if v:
        out.append(("cf_index", v[0], v[1]))
    p = spot_ws._client.prices.get(rx.PARAMS["asset"])
    if p and spot_ws._client.connected:
        out.append(("coinbase_ws", p[0], p[1]))
    return out


def default_prints(cf_state, close_epoch: float, decided_epoch: float) -> tuple[float, int, str]:
    import spot_ws
    s, n = (cf_state.settle_partial(rx.PARAMS["asset"], close_epoch) if cf_state is not None
            else (0.0, 0))
    if n:
        return float(s), int(n), "cf_index"
    s, n = spot_ws.window_partial(rx.PARAMS["asset"], close_epoch, now=decided_epoch)
    return float(s), int(n), ("coinbase_proxy" if n else "none")


class LiveExperiment:
    """Owns feeds, book tapes and the runners for one process lifetime."""

    def __init__(self, conn: sqlite3.Connection, *, capturer: fcap.FeeMetadataCapturer,
                 service: fsvc.FeeMetadataService, book_dir: Path,
                 clock: Callable[[], datetime] = server_now,
                 spot_reads: Optional[Callable[[], list]] = None,
                 prints: Optional[Callable[[float, float], tuple]] = None,
                 candle_post: Optional[Callable[[dict], Awaitable[tuple[int, bytes]]]] = None,
                 local_time: Callable[[], float] = time.time):
        import cf_ws
        self.conn, self.capturer, self.service = conn, capturer, service
        self.book_dir, self.clock = Path(book_dir), clock
        self.cf = cf_ws._State()                         # private: not the app-wide cache
        self.spot_reads = spot_reads or (lambda: default_spot_reads(self.cf))
        self.prints = prints or (lambda c, n: default_prints(self.cf, c, n))
        self.candle_post = candle_post
        self.local_time = local_time
        self.md = None                                  # research_md.MarketDataClient
        self.stream = sim_exec.BookStream()             # the whole socket stream, process-wide
        self.tracked: dict[str, datetime] = {}          # ticker -> tracking start (this process)
        self.recorders: dict[str, sim_exec.BookRecorder] = {}
        from collections import OrderedDict
        self._replayed: "OrderedDict[str, sim_exec.BookTape]" = OrderedDict()   # bounded LRU
        self._snap_asked: dict[str, datetime] = {}
        self._market: Optional[dict] = None
        self._market_at: Optional[datetime] = None
        self._candles: Optional[dict] = None
        self._snaps: Optional[tuple[str, dict]] = None

    # ---- book events ------------------------------------------------------------

    def _write_all(self, ev: dict) -> None:
        for rec in self.recorders.values():
            rec.write(ev)

    def on_book_event(self, ev: dict) -> None:
        """Every socket event goes to the process-wide stream and to every
        open window recording (the shared seq needs the whole stream)."""
        try:
            self._write_all(ev)
            self.stream.feed(ev)
        except Exception as e:
            logger.error(f"[c15] book event failed: {e}")
            bad = {"t": ev.get("t") or self.clock().isoformat(), "kind": "invalidate",
                   "reason": f"apply failed: {e}"[:200]}
            try:
                self._write_all(bad)
                self.stream.feed(bad)
            except Exception:
                pass

    def track(self, ticker: str, close: datetime) -> None:
        if ticker in self.tracked:
            return
        rec = sim_exec.BookRecorder(self.book_dir, ticker)      # drops a torn tail
        now = self.clock()
        # The recording starts mid-stream: an attach header carries the
        # stream's position so a replay checks continuity from here on.
        rec.write({"t": now.isoformat(), "kind": "attach", **self.stream.position()})
        self.recorders[ticker] = rec
        self.tracked[ticker] = now
        self.conn.execute("INSERT OR IGNORE INTO rx_book_files (market_ticker, close_time, path) "
                          "VALUES (?,?,?)", (ticker, close.isoformat(), str(rec.path)))
        self.conn.commit()
        self._replayed.pop(ticker, None)
        if self.md is not None:
            self.md.set_markets(set(self.tracked))

    def untrack(self, ticker: str) -> None:
        if ticker not in self.tracked:
            return
        gone = {"t": self.clock().isoformat(), "kind": "invalidate", "ticker": ticker,
                "reason": "untracked"}
        self.on_book_event(gone)
        self.tracked.pop(ticker)
        path, sha = self.recorders.pop(ticker).finalize()
        self.stream.forget(ticker)                  # the window file is now the record
        self.conn.execute("UPDATE rx_book_files SET path=?, sha256=?, finalized_at=? WHERE "
                          "market_ticker=?", (path, sha, self.clock().isoformat(), ticker))
        self.conn.commit()
        if self.md is not None:
            self.md.set_markets(set(self.tracked))

    def evidence_barrier(self, ticker: str) -> None:
        """Book evidence for `ticker` is fsynced before dependent records commit."""
        rec = self.recorders.get(ticker)
        if rec is not None:
            rec.sync()

    async def recover_books(self) -> None:
        """Request fresh snapshots for tracked markets without a valid book
        (new market, gap, reconnect), at most every 2 s per market."""
        if self.md is None or not self.md.connected:
            return
        now = self.clock()
        due = [t for t in self.stream.needs_snapshot(self.tracked)
               if now - self._snap_asked.get(t, now - timedelta(days=1)) >= timedelta(seconds=2)]
        if due:
            for t in due:
                self._snap_asked[t] = now
            await self.md.request_snapshot(due)

    def tape_for(self, ticker: str, at: datetime) -> Optional[sim_exec.BookTape]:
        """Live view if this process has been tracking the market since
        before `at`; otherwise the hash-checked replay of its window file."""
        start = self.tracked.get(ticker)
        kept = self.stream.retained_from
        if start is not None and start <= at and (kept is None or at - sim_exec.MAX_SILENCE >= kept):
            return self.stream.view(ticker)
        if ticker in self._replayed:
            self._replayed.move_to_end(ticker)
            return self._replayed[ticker]
        r = self.conn.execute("SELECT path, sha256 FROM rx_book_files WHERE market_ticker=?",
                              (ticker,)).fetchone()
        if r is None:
            return None
        p = Path(r[0])
        if not p.exists():
            alt = p.with_suffix(".jsonl.gz") if p.suffix == ".jsonl" else p
            if not alt.exists():
                return None
            p = alt
        if r[1]:
            import hashlib
            if hashlib.sha256(p.read_bytes()).hexdigest() != r[1]:
                logger.error(f"[c15] book file for {ticker} fails its hash; not used")
                return None
        try:
            tape = sim_exec.BookTape.from_events(ticker, sim_exec.BookRecorder.load(p))
        except Exception as e:
            logger.error(f"[c15] book file for {ticker} unreadable: {e}")
            return None
        if ticker not in self.tracked:
            self._replayed[ticker] = tape      # finalized files do not change
            while len(self._replayed) > REPLAY_CACHE:
                self._replayed.popitem(last=False)
        return tape

    def finalize_open_recorders(self) -> None:
        """At shutdown: sync and close (not gzip) open files; a restart
        appends to them after a fresh attach header."""
        for rec in self.recorders.values():
            try:
                rec.sync()
            except Exception:
                pass
            rec.close()

    # ---- discovery and metadata -----------------------------------------------------

    async def current_market(self) -> Optional[dict]:
        now = self.clock()
        stale = (self._market is None or self._market_at is None
                 or now - self._market_at > DISCOVERY_REFRESH
                 or now >= _close(self._market))
        if stale:
            rec, body = await self.capturer.open_markets(
                rx.PARAMS["series"], batch_id=self.capturer.new_batch(),
                context={"purpose": "c15-discovery"})
            markets = (fs.loads(body) or {}).get("markets") or []
            future = [m for m in markets if isinstance(m, dict) and _close(m) and _close(m) > now]
            self._market = min(future, key=_close) if future else None
            self._market_at = now
        return self._market

    async def maintain_tracking(self) -> None:
        now = self.clock()
        m = await self.current_market()
        if m is not None and _close(m) - now <= TRACK_LEAD:
            self.track(m["ticker"], _close(m))
        for t in list(self.tracked):
            r = self.conn.execute("SELECT close_time FROM rx_book_files WHERE market_ticker=?",
                                  (t,)).fetchone()
            close = datetime.fromisoformat(r[0]) if r else None
            busy = self.conn.execute("SELECT 1 FROM rx_orders WHERE market_ticker=? AND state IN "
                                     "('pending','unresolved')", (t,)).fetchone()
            if close is not None and now > close + TRACK_TAIL and not busy:
                self.untrack(t)
        await self.recover_books()
        self.stream.trim(self.clock() - LIVE_HISTORY)

    async def refresh_candles(self) -> None:
        now = self.clock()
        if self._candles is not None and now - self._candles["received_at"] < CANDLE_REFRESH:
            return
        start_ms = int((now - timedelta(minutes=CANDLE_LOOKBACK_MIN)).timestamp() * 1000)
        req = {"type": "candleSnapshot", "req": {"coin": rx.PARAMS["asset"], "interval": "1m",
                                                 "startTime": start_ms,
                                                 "endTime": int(now.timestamp() * 1000)}}
        try:
            sent = self.clock()
            status, body = await self.candle_post(req)
            received = self.clock()
            rec = self.capturer.record_external(url=HYPERLIQUID_URL, request_sent=sent,
                                                response_received=received, status=status,
                                                body=body, batch_id=self.capturer.new_batch(),
                                                context={"purpose": "c15-volatility",
                                                         "request": req})
            if status != 200:
                raise RuntimeError(f"HTTP {status}")
            import indicators
            closes, newest = rx.candle_series(json.loads(body), received,
                                              window=indicators.SIGMA_WINDOW)
            self._candles = {"sigma": indicators.sigma1m(closes), "received_at": received,
                             "newest_start": newest, "capture_id": rec.capture_id}
        except Exception as e:
            logger.warning(f"[c15] candle refresh failed: {e}")   # old candles age out

    async def make_inputs(self) -> Optional[rx.MarketInputs]:
        # --- every await first ---
        await self.maintain_tracking()
        m = self._market
        if m is None or _close(m) - self.clock() > DECISION_LEAD:
            return None
        snaps = await self.service.gather(m["ticker"], context={"purpose": "c15-decision"})
        await self.refresh_candles()
        # --- then the decision instant, and only synchronous reads ---
        decided = self.clock()
        offset = decided.timestamp() - self.local_time()
        mk = (fs.loads(snaps["market"].body) or {}).get("market") or {}
        ev = (fs.loads(snaps["event"].body) or {}).get("event") or {}
        rules = rx.validate_rules(mk, ev)
        meta_at = min(snaps["market"].record.response_received, snaps["event"].record.response_received)
        spot, spot_at, spot_src = None, None, "none"
        for src, local_ts, px in sorted(self.spot_reads(), key=lambda r: r[0] != "cf_index"):
            at = datetime.fromtimestamp(local_ts + offset, timezone.utc)
            if at <= decided and (decided - at).total_seconds() <= rx.PARAMS["max_spot_age_s"]:
                spot, spot_at, spot_src = px, at, src
                break
        close = rules.close_time or _close(mk) or _close(m)
        psum, pcount, psrc = self.prints(close.timestamp(), decided.timestamp())
        c = self._candles or {}
        self._snaps = (m["ticker"], snaps)
        return rx.MarketInputs(
            asset=rx.PARAMS["asset"], event_ticker=ev.get("event_ticker") or m.get("event_ticker"),
            market_ticker=m["ticker"], close_time=close, decided_at=decided, rules=rules,
            metadata_received_at=meta_at, spot=spot, spot_received_at=spot_at, spot_source=spot_src,
            sigma_1m=c.get("sigma"), candles_received_at=c.get("received_at"),
            newest_candle_start=c.get("newest_start"), settle_sum=psum, settle_count=pcount,
            settle_source=psrc)

    # ---- fees and results ----------------------------------------------------------

    async def prepare_fees(self, *, market_ticker: str, decided_at: datetime, arrivals: dict,
                           account_id: str, policy: str):
        """Contexts for the CALLING cohort's frozen account and policy; the
        precision comes from that account's persisted record."""
        if not self._snaps or self._snaps[0] != market_ticker:
            raise rx.ExperimentError("no metadata gathered for this decision")
        snaps = self._snaps[1]
        kw = dict(kalshi_env="paper", account_id=account_id, policy=policy, role="taker")
        ctx0 = fsvc.context_from_snapshots(self.service, self.conn, snaps, at=decided_at, **kw)
        later = {lat: fsvc.context_from_snapshots(self.service, self.conn, snaps, at=a, **kw)
                 for lat, a in arrivals.items()}
        return ctx0, later, fsvc.evidence_of(snaps)

    def rebuild_fee(self, evidence: dict, at: datetime, *, account_id: str,
                    policy: str) -> fctx.FeeContext:
        snaps = fsvc.snapshots_from_evidence(self.service, evidence)
        return fsvc.context_from_snapshots(self.service, self.conn, snaps, kalshi_env="paper",
                                           account_id=account_id, policy=policy, role="taker", at=at)

    async def result_for(self, ticker: str):
        try:
            rec, body = await self.capturer.market(ticker, batch_id=self.capturer.new_batch(),
                                                   context={"purpose": "c15-settlement"})
        except fcap.FeeCaptureError:
            return None, ()
        m = (fs.loads(body) or {}).get("market") or {}
        return (m.get("result") or None), (rec.capture_id,)

    # ---- readiness -------------------------------------------------------------

    async def readiness(self) -> tuple[bool, str]:
        """Launch gate: every feed the experiment needs is live right now."""
        if self.md is None or not self.md.connected:
            return False, f"market data not connected ({getattr(self.md, 'last_error', None)})"
        m = await self.current_market()
        if m is None:
            return False, "no open BTC 15-minute market"
        now = self.clock()
        offset = now.timestamp() - self.local_time()
        fresh = [r for r in self.spot_reads()
                 if 0 <= now.timestamp() - (r[1] + offset) <= rx.PARAMS["max_spot_age_s"]]
        if not fresh:
            return False, "no fresh spot price"
        await self.refresh_candles()
        if not self._candles or (now - self._candles["received_at"]).total_seconds() > \
                rx.PARAMS["max_vol_age_s"]:
            return False, "no fresh volatility candles"
        snaps = await self.service.gather(m["ticker"], context={"purpose": "c15-readiness"})
        ctx = fsvc.context_from_snapshots(self.service, self.conn, snaps, kalshi_env="paper",
                                          account_id=rx.PARAMS["account_id"],
                                          policy=rx.PARAMS["fee_policy"], role="taker",
                                          at=self.clock())
        if not ctx.usable:
            return False, f"fee context unusable: {ctx.reason}"
        mk = (fs.loads(snaps["market"].body) or {}).get("market") or {}
        ev = (fs.loads(snaps["event"].body) or {}).get("event") or {}
        rules = rx.validate_rules(mk, ev)
        if not rules.ok:
            return False, f"settlement rules not verified: {rules.detail}"
        if m["ticker"] not in self.tracked:
            return False, "waiting for the next market's book window"
        if self.stream.state_at(m["ticker"], self.clock()) is None:
            return False, "order book not yet usable"
        return True, "ready"


def _close(m: Optional[dict]) -> Optional[datetime]:
    if not isinstance(m, dict) or not isinstance(m.get("close_time"), str):
        return None
    try:
        dt = datetime.fromisoformat(m["close_time"].replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else None


async def run(cfg_getter: Callable[[], dict], stop: asyncio.Event, *,
              make_md: Optional[Callable[..., Any]] = None,
              send: Optional[Callable] = None,
              candle_post: Optional[Callable] = None,
              clock: Callable[[], datetime] = server_now,
              spot_reads: Optional[Callable[[], list]] = None,     # None: bound to the
              prints: Optional[Callable[[float, float], tuple]] = None,  # experiment's CF state
              local_time: Callable[[], float] = time.time,
              tick_s: float = 1.0) -> None:
    """The experiment process. Every resource is released in `finally`,
    including when initialization fails part-way."""
    import db
    import fee_ledger
    import kalshi_auth
    conn = None
    client = None
    md_stop = asyncio.Event()
    md_task = None
    live = None
    try:
        conn = sqlite3.connect(str(db.db_path()), timeout=30)
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        evdir = fee_ledger.evidence_dir("paper")
        if send is None or candle_post is None:
            import httpx
            client = httpx.AsyncClient(timeout=10.0)
            send = send or fcap.httpx_send(client)
            if candle_post is None:
                async def candle_post(body):
                    r = await client.post(HYPERLIQUID_URL, json=body)
                    return r.status_code, r.content
        capturer = fcap.FeeMetadataCapturer(send, fcap.CaptureStore(evdir), clock=clock)
        service = fsvc.FeeMetadataService(capturer)
        live = LiveExperiment(conn, capturer=capturer, service=service, book_dir=evdir / "book",
                              clock=clock, spot_reads=spot_reads, prints=prints,
                              candle_post=candle_post, local_time=local_time)
        body = rx.manifest_body()
        conn.execute("BEGIN IMMEDIATE")
        mid = rx.ensure_manifest(conn, body, clock())
        cfg = cfg_getter() or {}
        import research_preflight as _pf
        accepting_mid = rx.select_cohort(
            conn, mid, activate=cfg.get("research_c15_activate") or _pf.read_switch()["activate"],
            now=clock())
        if accepting_mid:
            rx.setup_wallets(conn, accepting_mid)
        conn.commit()
        if accepting_mid is None:
            logger.warning(f"[c15] code/config differs from the active cohort; current manifest "
                           f"{mid[:16]} needs research_c15_activate to start a new cohort")
        cohorts = rx.runner_cohorts(conn, mid, accepting_mid)

        if make_md is None:
            import research_md
            import spot_ws
            if not spot_ws.is_running():
                spot_ws.start()
            make_md = lambda on_event, on_cf: research_md.MarketDataClient(  # noqa: E731
                on_event=on_event, on_cf=on_cf)
        live.md = make_md(live.on_book_event, live.cf.handle_message)
        md_task = asyncio.ensure_future(live.md.run(md_stop))

        import research_preflight

        def enabled() -> bool:
            c = cfg_getter() or {}
            on = bool(c.get("research_c15_enabled")) or research_preflight.read_switch()["enabled"]
            return on and kalshi_auth.is_paper()

        def accepting_for(m):
            def f():
                ch = rx.cohort(conn, m)
                if not ch or ch["status"] != "active" or not enabled():
                    return False
                days = rx.cohort_params(conn, m)["duration_days"]
                return clock() < ch["launched_at"] + timedelta(days=days)
            return f

        # Every cohort with outstanding work drains with ITS frozen params.
        runners = [rx.C15Runner(conn, mid=m, accepting=accepting_for(m),
                                inputs=live.make_inputs, tape_for=live.tape_for,
                                prepare_fees=live.prepare_fees, rebuild_fee=live.rebuild_fee,
                                result_for=live.result_for, clock=clock,
                                params=rx.cohort_params(conn, m),
                                evidence_barrier=live.evidence_barrier)
                   for m in cohorts]
        last_ready_log = None
        while not stop.is_set():
            try:
                if accepting_mid and enabled():
                    ch = rx.cohort(conn, accepting_mid)
                    if ch and ch["status"] == "armed":
                        ok, why = await live.readiness()
                        if ok:
                            conn.execute("BEGIN IMMEDIATE")
                            launched = rx.launch(conn, accepting_mid, clock())
                            conn.commit()
                            logger.info(f"[c15] cohort {accepting_mid[:16]} launched {launched}")
                        elif why != last_ready_log:
                            logger.info(f"[c15] not ready: {why}")
                            last_ready_log = why
                await live.maintain_tracking()
                for r in runners:
                    await r.tick()
                if not enabled() and not any(rx.outstanding(conn, r.mid, r.p) for r in runners):
                    return
            except Exception as e:
                logger.error(f"[c15] tick failed: {e}", exc_info=True)
                try:
                    if conn.in_transaction:
                        conn.rollback()
                    conn.execute("INSERT INTO rx_downtime (manifest_id, at, reason) VALUES (?,?,?)",
                                 (accepting_mid, clock().isoformat(), str(e)[:500]))
                    conn.commit()
                except Exception:
                    pass
            try:
                await asyncio.wait_for(stop.wait(), timeout=tick_s)
            except asyncio.TimeoutError:
                pass
    finally:
        md_stop.set()
        if md_task is not None:
            try:
                await asyncio.wait_for(md_task, 5)
            except BaseException:
                md_task.cancel()
        if live is not None:
            live.finalize_open_recorders()
        if client is not None:
            await client.aclose()
        if conn is not None:
            conn.close()
