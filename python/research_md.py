"""Read-only Kalshi market data for paper research (independent of the app's
Paper/Live order-routing mode).

Kalshi requires an authenticated websocket even for public channels, so this
client signs its handshake with the saved production key. It can only READ:
the only commands it can send are subscribe / unsubscribe / get_snapshot
(enforced in _send), and it subscribes only to orderbook_delta and the
cfbenchmarks_value index. It shares nothing with the app's trading socket.

Every book-relevant happening becomes a BookTape event (sim_exec):
  up / down        connection generation started / ended
  alive            a ping was answered (real liveness, not a timer)
  msg              raw orderbook text, untouched
Events go to `on_event`; index frames go to `on_cf` (cf_ws.handle_message).
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"
ALLOWED_CMDS = frozenset({"subscribe", "unsubscribe", "update_subscription"})
ALLOWED_CHANNELS = frozenset({"orderbook_delta", "cfbenchmarks_value"})
ALLOWED_ACTIONS = frozenset({"get_snapshot", "add_markets", "delete_markets"})


class MarketDataError(RuntimeError):
    pass


def production_headers() -> dict:
    """Signed handshake headers from the saved production key (read-only use).
    Raises FileNotFoundError when no key is saved."""
    import kalshi_auth
    key_id, pk = kalshi_auth.load_env_credentials(kalshi_auth.PRODUCTION)
    return kalshi_auth.sign_headers_with(key_id, pk, "GET", WS_PATH)


def _server_now() -> datetime:
    import kalshi_auth
    return datetime.fromtimestamp(kalshi_auth.server_now(), timezone.utc)


class MarketDataClient:
    def __init__(self, *, on_event: Callable[[dict], None],
                 on_cf: Optional[Callable[[dict], None]] = None,
                 headers: Callable[[], dict] = production_headers,
                 connect: Optional[Callable[..., Any]] = None,
                 url: str = WS_URL, clock: Callable[[], datetime] = _server_now,
                 ping_every_s: float = 1.0, pong_timeout_s: float = 2.0,
                 dead_after_s: float = 15.0, backoff_base_s: float = 1.0,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self.on_event, self.on_cf = on_event, on_cf
        self.headers, self.url, self.clock = headers, url, clock
        self._connect = connect
        self.ping_every_s, self.pong_timeout_s, self.sleep = ping_every_s, pong_timeout_s, sleep
        # A pong slower than pong_timeout_s is NOT liveness evidence (no 'alive'
        # mark), but the link is only declared dead after dead_after_s: home
        # connections have pong hiccups, and reconnecting on each one churned
        # the subscription. Evidence semantics are unchanged.
        self.dead_after_s = max(dead_after_s, pong_timeout_s)
        self.pong_stats = {"fast": 0, "slow": 0, "max_rtt_ms": 0}
        self.backoff_base_s = backoff_base_s
        # Unique across process restarts, so recorded generations never collide.
        self.generation = int(time.time() * 1000)
        self.connected = False
        self.last_error: Optional[str] = None
        self._want: set[str] = set()
        self._have: set[str] = set()       # markets currently on the subscription
        self._sid: Optional[int] = None    # the ONE orderbook subscription
        self._sub_pending: Optional[int] = None
        self._id = 0
        self._ws = None
        self._want_cf = on_cf is not None

    # ---- desired state ------------------------------------------------------

    def set_markets(self, tickers) -> None:
        self._want = {t for t in tickers if isinstance(t, str) and t}

    async def request_snapshot(self, tickers) -> bool:
        """Ask for fresh orderbook_snapshots of markets already on the subscription."""
        tickers = sorted({tickers} if isinstance(tickers, str) else set(tickers)) 
        tickers = [t for t in tickers if t in self._have]
        if self._ws is None or self._sid is None or not tickers:
            return False
        await self._send({"cmd": "update_subscription",
                          "params": {"sids": [self._sid], "market_tickers": tickers,
                                     "action": "get_snapshot"}})
        return True

    # ---- plumbing ----------------------------------------------------------------

    def _emit(self, kind: str, **kw) -> None:
        ev = {"t": self.clock().isoformat(), "kind": kind, "gen": self.generation, **kw}
        try:
            self.on_event(ev)
        except Exception as e:  # never let a consumer kill the feed silently
            logger.error(f"research_md: on_event failed: {e}")
            try:
                self.on_event({"t": self.clock().isoformat(), "kind": "invalidate",
                               "reason": f"consumer error: {e}"[:200]})
            except Exception:
                pass

    async def _send(self, obj: dict) -> None:
        cmd = obj.get("cmd")
        params = obj.get("params") or {}
        if cmd not in ALLOWED_CMDS:
            raise MarketDataError(f"command {cmd!r} not allowed on the read-only client")
        if cmd == "subscribe" and not set(params.get("channels") or []) <= ALLOWED_CHANNELS:
            raise MarketDataError(f"channel not allowed: {params.get('channels')}")
        if cmd == "update_subscription" and params.get("action") not in ALLOWED_ACTIONS:
            raise MarketDataError(f"action not allowed: {params.get('action')}")
        self._id += 1
        obj = {"id": self._id, **obj}
        if cmd == "subscribe" and params.get("channels") == ["orderbook_delta"]:
            self._sub_pending = self._id
        await self._ws.send(json.dumps(obj))

    async def _reconcile(self) -> None:
        """Keep ONE orderbook subscription whose market set equals the wanted
        set (Kalshi shares one seq across a subscription's markets)."""
        if self._sub_pending is not None:
            return                                   # wait for 'subscribed'
        add = sorted(self._want - self._have)
        remove = sorted(self._have - self._want)
        if self._sid is None:
            if add:
                await self._send({"cmd": "subscribe",
                                  "params": {"channels": ["orderbook_delta"], "market_tickers": add}})
                self._have |= set(add)
            return
        if remove:
            if self._want:
                await self._send({"cmd": "update_subscription",
                                  "params": {"sids": [self._sid], "market_tickers": remove,
                                             "action": "delete_markets"}})
            else:
                await self._send({"cmd": "unsubscribe", "params": {"sids": [self._sid]}})
                self._sid = None
            self._have -= set(remove)
            for t in remove:
                self._emit("invalidate", ticker=t, reason="removed from subscription")
        if add and self._sid is not None:
            await self._send({"cmd": "update_subscription",
                              "params": {"sids": [self._sid], "market_tickers": add,
                                         "action": "add_markets"}})
            self._have |= set(add)
            # Whether add_markets sends a snapshot is undocumented: ask for one.
            await self.request_snapshot(add)

    def _handle(self, raw: str) -> None:
        try:
            m = json.loads(raw)
        except ValueError:
            self._emit("invalidate", reason="unparseable frame")
            return
        if not isinstance(m, dict):
            return
        t = m.get("type")
        if t == "subscribed":
            sid = (m.get("msg") or {}).get("sid")
            if m.get("id") == self._sub_pending and isinstance(sid, int):
                self._sid, self._sub_pending = sid, None
        elif t in ("orderbook_snapshot", "orderbook_delta"):
            self._emit("msg", raw=raw)
        elif t == "cfbenchmarks_value" and self.on_cf is not None:
            try:
                self.on_cf(m)
            except Exception as e:
                logger.warning(f"research_md: cf handler failed: {e}")
        elif t == "error":
            logger.warning(f"research_md: server error {m.get('msg')}")
            if m.get("id") is not None and m.get("id") == self._sub_pending:
                self._sub_pending, self._have = None, set()     # retry on next reconcile

    async def _pinger(self, ws) -> None:
        while True:
            await self.sleep(self.ping_every_s)
            sent = time.monotonic()
            waiter = await ws.ping()
            await asyncio.wait_for(waiter, timeout=self.dead_after_s)
            rtt = time.monotonic() - sent
            self.pong_stats["max_rtt_ms"] = max(self.pong_stats["max_rtt_ms"], int(rtt * 1000))
            if rtt <= self.pong_timeout_s:
                self.pong_stats["fast"] += 1
                self._emit("alive")
            else:
                self.pong_stats["slow"] += 1

    async def _session(self) -> None:
        connect = self._connect
        if connect is None:
            import websockets
            import ws_ssl
            connect = lambda url, headers: websockets.connect(  # noqa: E731
                url, additional_headers=headers, ping_interval=None, close_timeout=5,
                max_size=2 ** 23, ssl=ws_ssl.client_context())
        async with connect(self.url, self.headers()) as ws:
            self._ws = ws
            self.generation += 1
            self.connected = True
            self._sid, self._sub_pending, self._have = None, None, set()
            self._emit("up")
            if self._want_cf:
                await self._send({"cmd": "subscribe",
                                  "params": {"channels": ["cfbenchmarks_value"],
                                             "index_ids": ["all"]}})
            pinger = asyncio.ensure_future(self._pinger(ws))
            receiver: Optional[asyncio.Future] = None
            try:
                while True:
                    await self._reconcile()
                    if receiver is None:
                        receiver = asyncio.ensure_future(ws.recv())
                    # Wake on a frame OR a dead link (pinger raising), whichever first.
                    done, _ = await asyncio.wait({receiver, pinger}, timeout=0.5,
                                                 return_when=asyncio.FIRST_COMPLETED)
                    if pinger in done:
                        pinger.result()          # raises: pong timeout -> reconnect
                    if receiver in done:
                        raw, receiver = receiver.result(), None
                        self._handle(raw)
            finally:
                for task in (pinger, receiver):
                    if task is not None:
                        task.cancel()
                        try:
                            await task
                        except BaseException:
                            pass

    async def run(self, stop: asyncio.Event) -> None:
        attempt = 0
        while not stop.is_set():
            try:
                session = asyncio.ensure_future(self._session())
                stopper = asyncio.ensure_future(stop.wait())
                done, _ = await asyncio.wait({session, stopper},
                                             return_when=asyncio.FIRST_COMPLETED)
                if session in done:
                    session.result()
                    attempt = 0
                else:
                    session.cancel()
                    try:
                        await session
                    except BaseException:
                        pass
                stopper.cancel()
            except FileNotFoundError:
                self.last_error = "no saved Kalshi API key"
                attempt += 1
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                logger.warning(f"research_md: disconnected ({self.last_error})")
                attempt += 1
            finally:
                if self.connected:
                    self.connected = False
                    self._ws = None
                    self._emit("down")
            if not stop.is_set():
                delay = min(30.0, self.backoff_base_s * 2.0 ** min(attempt, 5))
                try:   # backoff that a stop request interrupts immediately
                    await asyncio.wait_for(stop.wait(), timeout=delay * random.uniform(0.75, 1.25))
                except asyncio.TimeoutError:
                    pass
