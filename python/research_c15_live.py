"""Live adapters for the frozen C15 experiment: real feeds in, paper out.

Starts only when config `research_c15_enabled` is true AND the app is in
Paper mode. It never touches order-placement code: fills are simulated and
money lives in the experiment's isolated research wallets.

Inputs per tick (server-aligned clock, kalshi_auth.server_now):
  * the current BTC 15-minute market from crypto15m.snapshot (ticker, close
    time, 1-minute volatility);
  * strike and settlement rules from the CAPTURED market/event payloads
    (fee_service), never the self-tracked window open;
  * spot from the live CF index feed, else the Coinbase websocket; REST
    fallbacks are not used (their age is unknown);
  * settlement prints: CF index partial average when present (authoritative,
    last ~15 s only), else the Coinbase proxy sampler (labelled a proxy).
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

import db
import fee_capture as fcap
import fee_context as fctx
import fee_ledger
import fee_schedule as fs
import fee_service as fsvc
import research_c15 as rx
import sim_exec

logger = logging.getLogger(__name__)

_tapes: dict[str, sim_exec.BookTape] = {}


def _server_now() -> datetime:
    import kalshi_auth
    return datetime.fromtimestamp(kalshi_auth.server_now(), timezone.utc)


def _on_book(message: dict, received: datetime) -> None:
    t = (message.get("msg") or {}).get("market_ticker")
    tape = _tapes.get(t)
    if tape is not None:
        tape.apply(message, received)


def _rules(market: dict, event: dict) -> tuple[bool, str, Optional[float]]:
    strike = market.get("floor_strike")
    if isinstance(strike, bool) or not isinstance(strike, (int, float)) or strike <= 0:
        return False, "no_floor_strike", None
    sources = {s.get("name") for s in (event.get("settlement_sources") or []) if isinstance(s, dict)}
    if rx.PARAMS["settlement_source"] not in sources:
        return False, f"settlement_source:{sorted(sources)}", None
    cadence = (event.get("product_metadata") or {}).get("cadence")
    if cadence != "fifteen_min":
        return False, f"cadence:{cadence}", None
    return True, "cf_benchmarks_fifteen_min", float(strike)


async def make_inputs(cfg: dict, service: fsvc.FeeMetadataService) -> Optional[rx.MarketInputs]:
    import cf_ws
    import crypto15m
    import kalshi_ws
    import spot_ws
    snap = await crypto15m.snapshot(cfg)
    btc = next((a for a in snap.get("assets", []) if a.get("asset") == rx.PARAMS["asset"]), None)
    if not btc or not btc.get("hasMarket") or not btc.get("ticker"):
        return None
    ticker = btc["ticker"]
    if ticker not in _tapes:
        _tapes.clear()   # one market at a time
        _tapes[ticker] = sim_exec.BookTape(ticker)
        kalshi_ws.add_orderbook_markets([ticker])
    close = datetime.fromisoformat(btc["closeTime"].replace("Z", "+00:00"))
    now = _server_now()
    base = dict(asset=rx.PARAMS["asset"], event_ticker=ticker.rsplit("-", 1)[0],
                market_ticker=ticker, close_time=close, now=now)
    if not (0 < (close - now).total_seconds() < 70):
        return rx.MarketInputs(**base, strike=None, spot=None, spot_at=None, sigma_1m=None,
                               settle_sum=0.0, settle_count=0, settle_source="none",
                               rules_ok=False, rules_detail="not_final_minute")
    snaps = await service.gather(ticker, context={"purpose": "c15-decision"})
    mk = fs.loads(snaps["market"].body).get("market") or {}
    ev = fs.loads(snaps["event"].body).get("event") or {}
    ok, detail, strike = _rules(mk, ev)
    base["event_ticker"] = ev.get("event_ticker") or base["event_ticker"]
    spot = cf_ws.fresh_spots().get(rx.PARAMS["asset"])
    spot_src = "cf_index"
    if spot is None:
        spot = spot_ws.fresh_spots().get(rx.PARAMS["asset"])
        spot_src = "coinbase_ws"
    close_epoch = close.timestamp()
    psum, pcount = cf_ws.settle_partial(rx.PARAMS["asset"], close_epoch)
    src = "cf_index"
    if not pcount:
        psum, pcount = spot_ws.window_partial(rx.PARAMS["asset"], close_epoch)
        src = "coinbase_proxy" if pcount else "none"
    # Fresh-feed spots are at most ~10 s old by those feeds' own gates.
    return rx.MarketInputs(**base, strike=strike, spot=spot,
                           spot_at=(now - timedelta(seconds=10)) if spot is not None else None,
                           sigma_1m=btc.get("sigma1m"), settle_sum=float(psum or 0.0),
                           settle_count=int(pcount or 0), settle_source=f"{src}/spot:{spot_src}",
                           rules_ok=ok, rules_detail=detail)


async def run(cfg_getter, stop: asyncio.Event) -> None:
    """The experiment loop (1 s cadence). Errors are logged as downtime and
    the loop continues; no decision is ever backfilled."""
    import kalshi_ws
    conn = sqlite3.connect(str(db.db_path()), timeout=30)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    store = fcap.CaptureStore(fee_ledger.evidence_dir("paper"))
    import httpx
    client = httpx.AsyncClient(timeout=10.0)
    capturer = fcap.FeeMetadataCapturer(fcap.httpx_send(client), store, clock=_server_now)
    service = fsvc.FeeMetadataService(capturer)
    conn.execute("BEGIN IMMEDIATE")
    body = rx.manifest_body()
    mid = rx.ensure_manifest(conn, body, _server_now())
    rx.setup_wallets(conn, mid)
    launched = rx.launch(conn, mid, _server_now())
    conn.commit()
    end = launched + timedelta(days=rx.PARAMS["duration_days"])
    logger.info(f"[c15-research] manifest {mid[:16]} launched {launched.isoformat()} ends {end.isoformat()}")

    async def ctx_for(*, market_ticker, role, at):
        return await service.context_for(conn, kalshi_env="paper", account_id=rx.PARAMS["account_id"],
                                         policy=rx.PARAMS["fee_policy"], market_ticker=market_ticker,
                                         role=role, at=at, context={"purpose": "c15-fee"})

    async def result_for(ticker):
        try:
            rec, b = await capturer.market(ticker, batch_id=capturer.new_batch(),
                                           context={"purpose": "c15-settlement"})
        except fcap.FeeCaptureError:
            return None, ()
        m = fs.loads(b).get("market") or {}
        return (m.get("result") or None), (rec.capture_id,)

    runner = rx.C15Runner(conn, mid=mid, inputs=lambda: make_inputs(cfg_getter(), service),
                          tape_for=_tapes.get, fee_context_for=ctx_for, result_for=result_for,
                          clock=_server_now)
    runner.restore_open_markets()
    kalshi_ws.add_book_listener(_on_book)
    try:
        while not stop.is_set():
            accepting = _server_now() < end
            try:
                if accepting:
                    await runner.tick()
                else:
                    await runner._settle_due()     # after day 90: only resolve what is open
                    if not runner.open_markets:
                        logger.info("[c15-research] cohort complete")
                        return
            except Exception as e:
                logger.error(f"[c15-research] tick failed: {e}", exc_info=True)
                try:
                    conn.rollback()
                    conn.execute("INSERT INTO rx_downtime (manifest_id, started_at, reason) "
                                 "VALUES (?,?,?)", (mid, _server_now().isoformat(), str(e)[:500]))
                    conn.commit()
                except Exception:
                    pass
            try:
                await asyncio.wait_for(stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
    finally:
        kalshi_ws.remove_book_listener(_on_book)
        await client.aclose()
        conn.close()
