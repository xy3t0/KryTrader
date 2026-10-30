"""Event-driven paper execution for taker IOC orders (plan step B2, taker part).

BookTape keeps an exact (Fraction) Kalshi order book per market from raw
websocket snapshot/delta messages, indexed by local receipt time, so the
book can be read AS OF a past instant (decision time + latency):
  * a sequence gap invalidates the book until the next snapshot;
  * a read at time t sees only messages received at or before t, so no fill
    can come from a quote that arrived after the order did.

simulate_ioc_buy() fills a taker IOC buy against the book at arrival:
buying YES at limit L lifts asks, which on Kalshi are the NO bids at
(1 - no_price), cheapest first, only up to L, and only depth not already
consumed by earlier simulated orders (LiquidityLedger, persisted). Unfilled
quantity is cancelled. Maker fills are not simulated here.

Latency is an ASSUMPTION (primary 1 s; stress 2 s and 5 s), not a measured
order-execution latency.
"""
from __future__ import annotations

import bisect
import sqlite3
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from typing import Any, Optional

import fees

SIM_VERSION = "sim-taker-ioc-v1"
PRIMARY_LATENCY = timedelta(seconds=1)
STRESS_LATENCIES = (timedelta(seconds=2), timedelta(seconds=5))
COUNT_QUANTUM = Fraction(1, 100)

LIQUIDITY_SCHEMA = """
CREATE TABLE IF NOT EXISTS sim_liquidity (
    scope TEXT NOT NULL,                 -- isolates simulations (e.g. one per cohort/latency)
    ticker TEXT NOT NULL,
    book_side TEXT NOT NULL CHECK (book_side IN ('yes', 'no')),
    price TEXT NOT NULL,                 -- the resting bid's price, dollars
    consumed TEXT NOT NULL,              -- contracts our simulated orders took
    consumed_at TEXT NOT NULL,           -- last consumption (book time)
    PRIMARY KEY (scope, ticker, book_side, price)
);
"""


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(LIQUIDITY_SCHEMA)


class SimError(RuntimeError):
    pass


def _f(v: Any, name: str) -> Fraction:
    try:
        return fees.to_fraction(v, name)
    except fees.FeeInputError as e:
        raise SimError(str(e)) from e


def _aware(dt: datetime) -> datetime:
    if not isinstance(dt, datetime) or dt.tzinfo is None:
        raise SimError("timestamps must be timezone-aware")
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class BookState:
    """Bids on each side: {price: size}, exact. valid=False after a seq gap."""
    yes: tuple[tuple[Fraction, Fraction], ...]
    no: tuple[tuple[Fraction, Fraction], ...]
    seq: int
    received_at: datetime
    valid: bool


class BookTape:
    """Exact time-indexed book for one market. Keeps `horizon` of history."""

    def __init__(self, ticker: str, *, horizon: timedelta = timedelta(minutes=3)):
        self.ticker = ticker
        self.horizon = horizon
        self._yes: dict[Fraction, Fraction] = {}
        self._no: dict[Fraction, Fraction] = {}
        self._seq: Optional[int] = None
        self._valid = False
        self._times: deque = deque()
        self._states: deque = deque()

    def _record(self, received_at: datetime) -> None:
        if self._times and received_at < self._times[-1]:
            raise SimError("book messages must arrive in receipt-time order")
        st = BookState(tuple(sorted(self._yes.items())), tuple(sorted(self._no.items())),
                       self._seq or 0, received_at, self._valid)
        self._times.append(received_at)
        self._states.append(st)
        while self._times and self._times[0] < received_at - self.horizon:
            self._times.popleft()
            self._states.popleft()

    def apply(self, message: dict, received_at: datetime) -> None:
        """Apply one raw websocket message (orderbook_snapshot/orderbook_delta)."""
        received_at = _aware(received_at)
        kind = message.get("type")
        msg = message.get("msg") or {}
        if msg.get("market_ticker") != self.ticker:
            return
        seq = message.get("seq")
        if isinstance(seq, bool) or not isinstance(seq, int):
            raise SimError("message without integer seq")
        if kind == "orderbook_snapshot":
            self._yes, self._no = {}, {}
            for side, key in ((self._yes, "yes_dollars_fp"), (self._no, "no_dollars_fp")):
                for lvl in msg.get(key) or []:
                    p, s = _f(lvl[0], "price"), _f(lvl[1], "size")
                    if s > 0:
                        side[p] = s
            self._seq, self._valid = seq, True
        elif kind == "orderbook_delta":
            if self._seq is None or seq <= self._seq:
                return  # before first snapshot, or a duplicate/stale delta
            if seq != self._seq + 1:
                self._valid = False  # gap: unusable until the next snapshot
                self._seq = seq
                self._record(received_at)
                return
            book = self._yes if msg.get("side") == "yes" else (
                self._no if msg.get("side") == "no" else None)
            if book is None:
                raise SimError(f"delta with bad side {msg.get('side')!r}")
            p = _f(msg.get("price_dollars"), "price")
            new = book.get(p, Fraction(0)) + _f(msg.get("delta_fp"), "delta")
            if new < 0:
                self._valid = False  # impossible book: distrust until a snapshot
            elif new == 0:
                book.pop(p, None)
            else:
                book[p] = new
            self._seq = seq
        else:
            return
        self._record(received_at)

    def level_absent_between(self, book_side: str, price: Fraction, t0: datetime,
                             t1: datetime) -> bool:
        """True if any recorded state in (t0, t1] lacked this level. If the
        history no longer reaches back to t0, consumption cannot be
        cleared on evidence, so this conservatively returns False."""
        if not self._times or self._times[0] > t0:
            return False
        i = bisect.bisect_right(self._times, t0)
        j = bisect.bisect_right(self._times, t1)
        for k in range(i, j):
            st = self._states[k]
            levels = dict(st.yes if book_side == "yes" else st.no)
            if price not in levels:
                return True
        return False

    def state_at(self, t: datetime) -> Optional[BookState]:
        """The book as received at or before t; None if no state or invalid."""
        t = _aware(t)
        i = bisect.bisect_right(self._times, t)
        if i == 0:
            return None
        st = self._states[i - 1]
        return st if st.valid else None


class LiquidityLedger:
    """Persisted record of displayed depth our simulated orders consumed, so
    the same visible liquidity cannot fill twice (across orders or restarts).
    Consumption at a price clears once that level leaves the book."""

    def __init__(self, conn: sqlite3.Connection, scope: str):
        # No schema work here: executescript() would COMMIT the caller's open
        # transaction. The table is created by init_schema() / db.init_db().
        self.conn, self.scope = conn, scope

    def _row(self, ticker, book_side, price):
        return self.conn.execute(
            "SELECT consumed, consumed_at FROM sim_liquidity WHERE scope=? AND ticker=? AND "
            "book_side=? AND price=?",
            (self.scope, ticker, book_side, str(fees.fraction_to_decimal(price)))).fetchone()

    def consumed(self, tape: "BookTape", book_side: str, price: Fraction,
                 now: datetime) -> Fraction:
        """Depth at this level already taken by our simulated orders. Cleared
        once the tape shows the level leaving the book after consumption."""
        row = self._row(tape.ticker, book_side, price)
        if row is None:
            return Fraction(0)
        at = datetime.fromisoformat(row[1])
        if tape.level_absent_between(book_side, price, at, now):
            self.conn.execute("DELETE FROM sim_liquidity WHERE scope=? AND ticker=? AND "
                              "book_side=? AND price=?",
                              (self.scope, tape.ticker, book_side,
                               str(fees.fraction_to_decimal(price))))
            return Fraction(0)
        return _f(row[0], "consumed")

    def add(self, tape: "BookTape", book_side: str, price: Fraction, qty: Fraction,
            now: datetime) -> None:
        row = self._row(tape.ticker, book_side, price)
        total = (Fraction(0) if row is None else _f(row[0], "consumed")) + qty
        self.conn.execute("INSERT INTO sim_liquidity VALUES (?,?,?,?,?,?) ON CONFLICT (scope, "
                          "ticker, book_side, price) DO UPDATE SET consumed=excluded.consumed, "
                          "consumed_at=excluded.consumed_at",
                          (self.scope, tape.ticker, book_side, str(fees.fraction_to_decimal(price)),
                           str(fees.fraction_to_decimal(total)), now.isoformat()))


@dataclass(frozen=True)
class SimFill:
    price: Fraction        # what we pay per contract for the side we buy
    count: Fraction


@dataclass(frozen=True)
class SimResult:
    arrival: datetime
    fills: tuple[SimFill, ...]
    unfilled: Fraction
    reason: str            # "filled" | "partial" | "no_liquidity" | "book_unavailable"
    book_seq: Optional[int] = None


def simulate_ioc_buy(tape: BookTape, ledger: LiquidityLedger, *, side: str, limit: Any,
                     count: Any, decided_at: datetime,
                     latency: timedelta = PRIMARY_LATENCY) -> SimResult:
    """Taker IOC buy of `count` contracts of `side` at limit `limit` (dollars).
    Uses ONLY the book state at arrival = decided_at + latency."""
    if side not in ("yes", "no"):
        raise SimError("side must be 'yes' or 'no'")
    lim, qty = _f(limit, "limit"), _f(count, "count")
    if not (0 < lim < 1):
        raise SimError("limit must be in (0, 1)")
    if qty <= 0 or (qty / COUNT_QUANTUM).denominator != 1:
        raise SimError("count must be a positive multiple of 0.01")
    if not isinstance(latency, timedelta) or latency < timedelta(0):
        raise SimError("latency must be a non-negative timedelta")
    arrival = _aware(decided_at) + latency
    st = tape.state_at(arrival)
    if st is None:
        return SimResult(arrival, (), qty, "book_unavailable")
    # Buying `side` lifts the opposite side's bids: ask = 1 - opposite bid.
    opp_side = "no" if side == "yes" else "yes"
    opp = st.no if side == "yes" else st.yes
    asks = sorted(((1 - p, p, s) for p, s in opp), key=lambda x: x[0])
    fills, left = [], qty
    for ask, bid_price, size in asks:
        if ask > lim or left == 0:
            break
        avail = size - ledger.consumed(tape, opp_side, bid_price, arrival)
        if avail <= 0:
            continue
        take = min(avail, left)
        take = Fraction(int(take / COUNT_QUANTUM)) * COUNT_QUANTUM  # venue granularity
        if take <= 0:
            continue
        fills.append(SimFill(ask, take))
        ledger.add(tape, opp_side, bid_price, take, arrival)
        left -= take
    reason = "filled" if left == 0 else ("partial" if fills else "no_liquidity")
    return SimResult(arrival, tuple(fills), left, reason, st.seq)
