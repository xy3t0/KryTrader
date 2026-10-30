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


MAX_SILENCE = timedelta(seconds=5)   # a liveness mark must be at least this recent


@dataclass(frozen=True)
class BookState:
    """Bids on each side, exact. Only valid states are ever returned."""
    yes: tuple[tuple[Fraction, Fraction], ...]
    no: tuple[tuple[Fraction, Fraction], ...]
    seq: int
    sid: Optional[int]
    generation: Optional[int]
    received_at: datetime
    valid: bool


def _int(v: Any) -> Optional[int]:
    return v if isinstance(v, int) and not isinstance(v, bool) else None


class BookTape:
    """Exact, time-indexed book for one market, fed by EVENTS:

      {"t": iso, "kind": "up"|"down", "gen": n}          connection state
      {"t": iso, "kind": "alive", "gen": n}               liveness mark (~1/s)
      {"t": iso, "kind": "msg", "gen": n, "raw": str}     raw websocket text
      {"t": iso, "kind": "invalidate", "reason": str}     e.g. listener failure

    The same events are what BookRecorder persists, so a tape rebuilt from
    disk (from_events) is identical to the live one.

    state_at(t) returns a book only if: the latest state at or before t is
    valid; the connection generation that produced it is still up at t; and
    a liveness mark for that generation is no older than MAX_SILENCE at t.
    Sequence gaps, sid changes, malformed or impossible messages, a new
    generation without a fresh snapshot, and explicit invalidations all make
    the book unusable until the next snapshot.
    """

    def __init__(self, ticker: str, *, horizon: Optional[timedelta] = None):
        self.ticker = ticker
        self.horizon = horizon
        self._yes: dict[Fraction, Fraction] = {}
        self._no: dict[Fraction, Fraction] = {}
        self._seq: Optional[int] = None
        self._sid: Optional[int] = None
        self._valid = False
        self._gen: Optional[int] = None          # generation currently up, else None
        self._book_gen: Optional[int] = None     # generation the book was built on
        self._last_t: Optional[datetime] = None
        self._times: list = []
        self._states: list = []
        self._conn_times: list = []
        self._conn: list = []                    # (state, gen)
        self._alive_times: list = []
        self._alive_gens: list = []

    # ---- input -----------------------------------------------------------------

    @classmethod
    def from_events(cls, ticker: str, events) -> "BookTape":
        tape = cls(ticker)
        for ev in events:
            tape.feed(ev)
        return tape

    def feed(self, ev: dict) -> None:
        t = _aware(datetime.fromisoformat(ev["t"]))
        if self._last_t is not None and t < self._last_t:
            raise SimError("book events must be fed in receipt-time order")
        self._last_t = t
        kind = ev.get("kind")
        gen = _int(ev.get("gen"))
        if kind == "up":
            self._gen = gen
            self._conn_times.append(t)
            self._conn.append(("up", gen))
            self._invalidate(t)                  # a new connection needs a fresh snapshot
        elif kind == "down":
            self._gen = None
            self._conn_times.append(t)
            self._conn.append(("down", gen))
            self._invalidate(t)
        elif kind == "alive":
            self._alive_times.append(t)
            self._alive_gens.append(gen)
        elif kind == "invalidate":
            self._invalidate(t)
        elif kind == "msg":
            self._apply_raw(ev.get("raw"), gen, t)
        else:
            raise SimError(f"unknown book event kind {kind!r}")
        self._trim(t)

    def _invalidate(self, t: datetime) -> None:
        self._valid = False
        self._record(t)

    def _apply_raw(self, raw: Any, gen: Optional[int], t: datetime) -> None:
        import json
        try:
            m = json.loads(raw, parse_float=__import__("decimal").Decimal)
        except (TypeError, ValueError):
            self._invalidate(t)
            return
        if not isinstance(m, dict):
            self._invalidate(t)
            return
        msg = m.get("msg") if isinstance(m.get("msg"), dict) else {}
        kind = m.get("type")
        if kind not in ("orderbook_snapshot", "orderbook_delta"):
            return
        if msg.get("market_ticker") != self.ticker:
            return
        seq, sid = _int(m.get("seq")), _int(m.get("sid"))
        if gen is None or gen != self._gen or seq is None:
            self._invalidate(t)                  # stale generation or malformed
            return
        try:
            if kind == "orderbook_snapshot":
                yes, no = {}, {}
                for side, key in ((yes, "yes_dollars_fp"), (no, "no_dollars_fp")):
                    levels = msg.get(key) or []
                    if not isinstance(levels, list):
                        raise SimError("levels not a list")
                    for lvl in levels:
                        if not isinstance(lvl, list) or len(lvl) < 2:
                            raise SimError("bad level")
                        p, sz = _f(lvl[0], "price"), _f(lvl[1], "size")
                        if not (0 < p < 1) or sz < 0:
                            raise SimError("impossible level")
                        if sz > 0:
                            side[p] = sz
                self._yes, self._no = yes, no
                self._seq, self._sid, self._book_gen, self._valid = seq, sid, gen, True
            else:
                if not self._valid or sid != self._sid or self._book_gen != gen:
                    self._invalidate(t)
                    return
                if seq <= self._seq:
                    return                       # duplicate / stale
                if seq != self._seq + 1:
                    self._seq = seq
                    self._invalidate(t)          # gap
                    return
                book = {"yes": self._yes, "no": self._no}.get(msg.get("side"))
                if book is None:
                    raise SimError("bad side")
                p = _f(msg.get("price_dollars"), "price")
                if not (0 < p < 1):
                    raise SimError("impossible price")
                new = book.get(p, Fraction(0)) + _f(msg.get("delta_fp"), "delta")
                self._seq = seq
                if new < 0:
                    raise SimError("negative level")
                if new == 0:
                    book.pop(p, None)
                else:
                    book[p] = new
        except (SimError, TypeError, IndexError):
            self._invalidate(t)
            return
        self._record(t)

    def _record(self, t: datetime) -> None:
        self._times.append(t)
        self._states.append(BookState(tuple(sorted(self._yes.items())),
                                      tuple(sorted(self._no.items())), self._seq or 0,
                                      self._sid, self._book_gen, t, self._valid))

    def _trim(self, now: datetime) -> None:
        if self.horizon is None:
            return
        cut = now - self.horizon
        for times, *vals in ((self._times, self._states), (self._alive_times, self._alive_gens)):
            k = bisect.bisect_left(times, cut)
            if k > 1:
                del times[:k - 1]
                for v in vals:
                    del v[:k - 1]

    # ---- queries ---------------------------------------------------------------

    def level_absent_between(self, book_side: str, price: Fraction, t0: datetime,
                             t1: datetime) -> bool:
        """True if any recorded VALID state in (t0, t1] lacked this level. If
        history does not reach back to t0, returns False (consumption kept)."""
        if not self._times or self._times[0] > t0:
            return False
        i = bisect.bisect_right(self._times, t0)
        j = bisect.bisect_right(self._times, t1)
        for k in range(i, j):
            st = self._states[k]
            if st.valid and price not in dict(st.yes if book_side == "yes" else st.no):
                return True
        return False

    def state_at(self, t: datetime) -> Optional[BookState]:
        t = _aware(t)
        i = bisect.bisect_right(self._times, t)
        if i == 0:
            return None
        st = self._states[i - 1]
        if not st.valid or st.generation is None:
            return None
        c = bisect.bisect_right(self._conn_times, t)
        if c == 0 or self._conn[c - 1] != ("up", st.generation):
            return None
        a = bisect.bisect_right(self._alive_times, t)
        while a > 0 and self._alive_gens[a - 1] != st.generation:
            a -= 1
        if a == 0 or t - self._alive_times[a - 1] > MAX_SILENCE:
            return None
        return st


class BookRecorder:
    """Persists one market window's book events as JSON lines, then gzips
    them with a SHA-256. Load replays a (possibly crash-truncated) file."""

    def __init__(self, directory, name: str):
        from pathlib import Path
        if not name or "/" in name or "\\" in name or name.startswith("."):
            raise SimError(f"bad recorder name {name!r}")
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / f"{name}.jsonl"
        if self.path.exists():
            # Drop a torn final line left by a crash so appends stay parseable.
            data = self.path.read_bytes()
            cut = data.rfind(b"\n") + 1
            if cut != len(data):
                with open(self.path, "r+b") as f:
                    f.truncate(cut)
        self._fh = open(self.path, "a", encoding="utf-8")

    def write(self, ev: dict) -> None:
        import json
        self._fh.write(json.dumps(ev, sort_keys=True, separators=(",", ":")) + "\n")
        self._fh.flush()

    def finalize(self) -> tuple[str, str]:
        """Close, gzip, fsync; returns (path, sha256 of the .gz)."""
        import gzip
        import hashlib
        import os
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        gz = self.path.with_suffix(".jsonl.gz")
        data = self.path.read_bytes()
        with open(gz, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb", mtime=0) as z:
                z.write(data)
            f.flush()
            os.fsync(f.fileno())
        self.path.unlink()
        return str(gz), hashlib.sha256(gz.read_bytes()).hexdigest()

    def close(self) -> None:
        if not self._fh.closed:
            self._fh.flush()
            self._fh.close()

    @staticmethod
    def load(path) -> list:
        """Events from a .jsonl or .jsonl.gz file. A torn final line (crash
        mid-write) is dropped; any other malformed line raises."""
        import gzip
        import json
        from pathlib import Path
        p = Path(path)
        text = gzip.decompress(p.read_bytes()).decode() if p.suffix == ".gz" else p.read_text()
        lines = text.split("\n")
        out = []
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                if i == len(lines) - 1:      # no trailing newline: torn write
                    break
                raise SimError(f"corrupt book event at line {i + 1}")
        return out


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
