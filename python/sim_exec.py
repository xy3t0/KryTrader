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


class _Market:
    __slots__ = ("yes", "no", "valid", "sid", "gen", "times", "states")

    def __init__(self):
        self.yes: dict = {}
        self.no: dict = {}
        self.valid = False
        self.sid: Optional[int] = None
        self.gen: Optional[int] = None
        self.times: list = []
        self.states: list = []


class BookStream:
    """Exact order books for every market on one websocket stream.

    Kalshi carries orderbook updates on a subscription whose `seq` is shared
    by all of its markets, so continuity is checked PER SUBSCRIPTION (sid):
    a gap invalidates every market bound to that sid until each gets a fresh
    snapshot; interleaved markets on one sid are normal. Events:

      {"kind": "attach", "gen": g, "connected": bool, "seqs": {sid: seq}}
          a recording starts mid-stream: known stream position, unknown books
      {"kind": "up"/"down", "gen": g}      connection generation
      {"kind": "alive", "gen": g}          an answered ping
      {"kind": "msg", "gen": g, "raw": s}  raw orderbook text
      {"kind": "invalidate", "ticker": t?} one market (or all) unusable

    state_at(ticker, t) returns a book only if that market's latest state is
    valid, its generation is up at t, and a liveness mark for that
    generation is no older than MAX_SILENCE.
    """

    def __init__(self):
        self._gen: Optional[int] = None
        self._seq: dict[int, int] = {}
        self.markets: dict[str, _Market] = {}
        self._last_t: Optional[datetime] = None
        self._conn_times: list = []
        self._conn: list = []
        self._alive_times: list = []
        self._alive_gens: list = []
        self.retained_from: Optional[datetime] = None   # history before this is dropped

    @classmethod
    def from_events(cls, events) -> "BookStream":
        st = cls()
        for ev in events:
            st.feed(ev)
        return st

    @property
    def generation(self) -> Optional[int]:
        return self._gen

    def position(self) -> dict:
        """What an attach header must carry to replay from here."""
        return {"gen": self._gen, "connected": self._gen is not None,
                "seqs": {str(k): v for k, v in self._seq.items()}}

    def _m(self, ticker: str) -> _Market:
        m = self.markets.get(ticker)
        if m is None:
            m = self.markets[ticker] = _Market()
        return m

    def _record(self, ticker: str, t: datetime) -> None:
        m = self._m(ticker)
        m.times.append(t)
        m.states.append(BookState(tuple(sorted(m.yes.items())), tuple(sorted(m.no.items())),
                                  self._seq.get(m.sid, 0) if m.sid is not None else 0,
                                  m.sid, m.gen, t, m.valid))

    def _invalidate(self, t: datetime, tickers=None) -> None:
        for tk in (self.markets if tickers is None else tickers):
            m = self._m(tk)
            if m.valid or not m.times:
                m.valid = False
                self._record(tk, t)

    def feed(self, ev: dict) -> None:
        t = _aware(datetime.fromisoformat(ev["t"]))
        if self._last_t is not None and t < self._last_t:
            raise SimError("book events must be fed in receipt-time order")
        self._last_t = t
        kind = ev.get("kind")
        gen = _int(ev.get("gen"))
        if kind == "attach":
            self._seq = {int(k): v for k, v in (ev.get("seqs") or {}).items()
                         if str(k).lstrip("-").isdigit() and _int(v) is not None}
            self._gen = gen if ev.get("connected") else None
            self._conn_times.append(t)
            self._conn.append(("up" if self._gen is not None else "down", gen))
            self._invalidate(t)
        elif kind in ("up", "down"):
            self._gen = gen if kind == "up" else None
            self._seq = {}
            self._conn_times.append(t)
            self._conn.append((kind, gen))
            self._invalidate(t)
        elif kind == "alive":
            self._alive_times.append(t)
            self._alive_gens.append(gen)
        elif kind == "invalidate":
            tk = ev.get("ticker")
            self._invalidate(t, None if tk is None else [tk])
        elif kind == "msg":
            self._apply_raw(ev.get("raw"), gen, t)
        else:
            raise SimError(f"unknown book event kind {kind!r}")

    def _apply_raw(self, raw: Any, gen: Optional[int], t: datetime) -> None:
        import json
        from decimal import Decimal
        try:
            m = json.loads(raw, parse_float=Decimal)
        except (TypeError, ValueError):
            self._invalidate(t)          # cannot tell which market: distrust all
            return
        if not isinstance(m, dict) or m.get("type") not in ("orderbook_snapshot", "orderbook_delta"):
            return
        msg = m.get("msg") if isinstance(m.get("msg"), dict) else {}
        ticker = msg.get("market_ticker")
        sid, seq = _int(m.get("sid")), _int(m.get("seq"))
        if not isinstance(ticker, str) or not ticker:
            self._invalidate(t)
            return
        if gen is None or gen != self._gen or sid is None or seq is None:
            self._invalidate(t, [ticker])
            return
        expected = self._seq.get(sid)
        if expected is not None and seq <= expected:
            return                                   # duplicate / replayed
        gap = expected is not None and seq != expected + 1
        self._seq[sid] = seq
        if gap:
            self._invalidate(t, [tk for tk, mk in self.markets.items() if mk.sid == sid])
        mk = self._m(ticker)
        try:
            if m["type"] == "orderbook_snapshot":
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
                mk.yes, mk.no, mk.sid, mk.gen, mk.valid = yes, no, sid, gen, True
            else:
                if gap or not mk.valid or mk.sid != sid or mk.gen != gen:
                    self._invalidate(t, [ticker])
                    return
                book = {"yes": mk.yes, "no": mk.no}.get(msg.get("side"))
                if book is None:
                    raise SimError("bad side")
                p = _f(msg.get("price_dollars"), "price")
                if not (0 < p < 1):
                    raise SimError("impossible price")
                new = book.get(p, Fraction(0)) + _f(msg.get("delta_fp"), "delta")
                if new < 0:
                    raise SimError("negative level")
                if new == 0:
                    book.pop(p, None)
                else:
                    book[p] = new
        except (SimError, TypeError, IndexError):
            mk.valid = False
            self._record(ticker, t)
            return
        self._record(ticker, t)

    # ---- memory bounds --------------------------------------------------------

    def forget(self, ticker: str) -> None:
        """Drop a finalized market entirely (its disk recording is the record)."""
        self.markets.pop(ticker, None)

    def trim(self, before: datetime) -> None:
        """Drop history older than `before`, keeping what answers state_at(t)
        for every t >= before: each market's last state before the cutoff,
        the last connection event before it, and liveness marks from
        before - MAX_SILENCE. Stream position (gen, seqs) is untouched."""
        before = _aware(before)

        def cut(times, *vals, keep_one=True, at=before):
            k = bisect.bisect_left(times, at)
            if keep_one:
                k = max(0, k - 1)
            if k:
                del times[:k]
                for v in vals:
                    del v[:k]
        for mk in self.markets.values():
            cut(mk.times, mk.states)
        cut(self._conn_times, self._conn)
        cut(self._alive_times, self._alive_gens, keep_one=False, at=before - MAX_SILENCE)
        if self.retained_from is None or before > self.retained_from:
            self.retained_from = before

    def history_size(self) -> int:
        return (sum(len(m.times) for m in self.markets.values()) + len(self._conn_times)
                + len(self._alive_times))

    # ---- queries ---------------------------------------------------------------

    def state_at(self, ticker: str, t: datetime, *, confirmed: bool = False) -> Optional[BookState]:
        """confirmed=True additionally requires a liveness mark of the same
        generation in [t, t + MAX_SILENCE]: proof the stream (and its
        recording) continued past t, so no message received by t is missing.
        Executions use it; a decision taken "now" cannot."""
        t = _aware(t)
        mk = self.markets.get(ticker)
        if mk is None:
            return None
        i = bisect.bisect_right(mk.times, t)
        if i == 0:
            return None
        st = mk.states[i - 1]
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
        if confirmed:
            b = bisect.bisect_left(self._alive_times, t)
            while b < len(self._alive_times) and self._alive_gens[b] != st.generation:
                b += 1
            if b == len(self._alive_times) or self._alive_times[b] - t > MAX_SILENCE:
                return None
            # and the generation must not have gone down in between
            c2 = bisect.bisect_right(self._conn_times, self._alive_times[b])
            if self._conn[c2 - 1] != ("up", st.generation):
                return None
        return st

    def level_absent_between(self, ticker: str, book_side: str, price: Fraction,
                             t0: datetime, t1: datetime) -> bool:
        mk = self.markets.get(ticker)
        if mk is None or not mk.times or mk.times[0] > t0:
            return False
        i = bisect.bisect_right(mk.times, t0)
        j = bisect.bisect_right(mk.times, t1)
        for k in range(i, j):
            st = mk.states[k]
            if st.valid and price not in dict(st.yes if book_side == "yes" else st.no):
                return True
        return False

    def needs_snapshot(self, tickers) -> list:
        """Tracked markets with no valid book while the stream is connected."""
        if self._gen is None:
            return []
        return [tk for tk in tickers if not (self.markets.get(tk) and self.markets[tk].valid)]

    def view(self, ticker: str) -> "BookTape":
        return BookTape(ticker, stream=self)


class BookTape:
    """One market's view of a BookStream (what the simulator consumes)."""

    def __init__(self, ticker: str, *, stream: Optional[BookStream] = None):
        self.ticker = ticker
        self.stream = stream or BookStream()

    @classmethod
    def from_events(cls, ticker: str, events) -> "BookTape":
        return cls(ticker, stream=BookStream.from_events(events))

    def feed(self, ev: dict) -> None:
        self.stream.feed(ev)

    def state_at(self, t: datetime, *, confirmed: bool = False) -> Optional[BookState]:
        return self.stream.state_at(self.ticker, t, confirmed=confirmed)

    def level_absent_between(self, book_side: str, price: Fraction, t0: datetime,
                             t1: datetime) -> bool:
        return self.stream.level_absent_between(self.ticker, book_side, price, t0, t1)


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
        self.last_written: Optional[str] = None
        self.synced: Optional[str] = None

    def write(self, ev: dict) -> None:
        import json
        self._fh.write(json.dumps(ev, sort_keys=True, separators=(",", ":")) + "\n")
        self._fh.flush()
        self.last_written = ev.get("t")

    def sync(self) -> None:
        """Make everything written so far durable (call before committing
        records that depend on this evidence)."""
        import os
        if not self._fh.closed:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self.synced = self.last_written

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
    st = tape.state_at(arrival, confirmed=True)   # evidence must extend past arrival
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
