"""Resolve which Kalshi fee schedule applies to a fill: pure, no network.

Answers: for this market, this execution role (taker/maker) and this
effective time `at`, what fee type, multiplier and quadratic coefficient
apply, and how sure are we? Fetching, caching and wiring into callers are
separate (commit 1c). Every input here is an already-parsed snapshot.

Sources, from the docs and payloads captured 2026-10-30:
  * Series:  GET /series/{t}            -> fee_type, fee_multiplier
  * Series changes: GET /series/fee_changes?show_historical=true
                                         -> series_fee_change_arr[]
  * Event:   GET /events/{t}            -> fee_type_override,
             fee_multiplier_override (each optional / nullable)
  * Event changes: GET /events/fee_changes -> event_fee_changes[], cursor;
             null on a field clears that field's override (falls back to
             the series value)
  * Market:  fee_waiver_expiration_time (optional / nullable)
  * Fee schedule PDF (effective 2026-07-07): taker 0.07*M, maker 0.0175*M;
    API enum says combo maker uses 0.5 instead of 0.25, i.e. 0.035*M.

Snapshot timing. A response reflects origin state at some unknown instant
inside an interval [lo, hi] (request time, CDN cache lifetime, Age header,
clock skew). A change scheduled inside that interval may or may not be
reflected, so it is never silently assumed either way:
  * at < lo                     -> UNAVAILABLE(before_snapshot)
  * lo <= at <= hi              -> snapshot value, unless any change is
                                   scheduled in [lo, hi] -> UNAVAILABLE
  * at > hi                     -> a change in [lo, hi] must agree with the
                                   snapshot (else UNAVAILABLE); then the
                                   latest change in (hi, at] applies
Two changes for one target at the same instant with different values ->
UNAVAILABLE(ambiguous_tie).

Outcomes are distinct statuses, and only RESOLVED and EXPLICIT_ZERO carry a
coefficient:
  RESOLVED           coefficient > 0
  EXPLICIT_ZERO      multiplier_zero | no_maker_fee_for_type
  UNSUPPORTED        fee type with no documented formula (flat, unknown)
  UNAVAILABLE        metadata missing, stale, conflicting or incomplete
  WAIVER_UNRESOLVED  a fee waiver may be in force at `at`; its scope is not
                     documented, so actual fees are unknown

Change-history coverage. Each change listing carries an explicit Coverage
claim (VERIFIED / ASSUMED with named assumptions / NONE) over an interval
and ticker scope. A result needs coverage of [lo, max(at, hi)] for every
listing it depends on, else UNAVAILABLE(history_unverified:...). Results
built on ASSUMED coverage list those assumptions; money paths must gate on
admissible(result, accepted_assumptions), never on has_coefficient.
Exchange-reported fill fees (Get Fills fee_cost) remain the accounting
evidence for fills that happened; this resolver does not replace them.

Historical resolution before a snapshot is UNAVAILABLE here. An exploratory
backtest estimate, if added, must be a separate, distinctly typed result.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from typing import Any, Callable, Iterable, Optional, Sequence

import fees

# ---- constants ------------------------------------------------------------

FEE_TYPE_QUADRATIC = "quadratic"
FEE_TYPE_MAKER = "quadratic_with_maker_fees"
FEE_TYPE_COMBO_MAKER = "quadratic_with_combo_maker_fees"
FEE_TYPE_FLAT = "flat"
KNOWN_FEE_TYPES = frozenset({FEE_TYPE_QUADRATIC, FEE_TYPE_MAKER, FEE_TYPE_COMBO_MAKER, FEE_TYPE_FLAT})

_TAKER_BASE = Fraction(7, 100)  # 0.07
_MAKER_BASE = {
    FEE_TYPE_QUADRATIC: None,                # no maker fee for this type
    FEE_TYPE_MAKER: Fraction(7, 400),        # 0.0175 = 0.07 * 0.25
    FEE_TYPE_COMBO_MAKER: Fraction(7, 200),  # 0.035  = 0.07 * 0.5
}

# Base coefficients have up to 4 decimal places; keep base*M within the
# coefficient scale fees.quadratic_model_fee accepts.
MAX_MULTIPLIER_SCALE = fees.MAX_COEFFICIENT_SCALE - 4

TAKER = "taker"
MAKER = "maker"

RESOLVED = "RESOLVED"
EXPLICIT_ZERO = "EXPLICIT_ZERO"
UNSUPPORTED = "UNSUPPORTED"
UNAVAILABLE = "UNAVAILABLE"
WAIVER_UNRESOLVED = "WAIVER_UNRESOLVED"

_UNUSABLE_TS = datetime(1, 1, 1, tzinfo=timezone.utc)


class FeeMetadataError(ValueError):
    """A payload is malformed, or inputs are inconsistent with each other."""


# ---- primitive parsing ----------------------------------------------------

def _reject_constant(name: str):
    raise FeeMetadataError(f"non-finite JSON constant {name!r}")


def loads(text: str | bytes) -> Any:
    """json.loads with every JSON number exact: floats become Decimal and
    NaN/Infinity are rejected. Use this for every payload passed to the
    parsers below; a payload parsed with plain json.loads is refused.
    Every decoding failure surfaces as FeeMetadataError (cause preserved)."""
    if not isinstance(text, (str, bytes, bytearray)):
        raise FeeMetadataError(f"payload must be str or bytes, got {type(text).__name__}")
    try:
        return json.loads(text, parse_float=Decimal, parse_constant=_reject_constant)
    except FeeMetadataError:
        raise
    except (ValueError, RecursionError) as e:  # JSONDecodeError, UnicodeDecodeError
        raise FeeMetadataError(f"payload is not valid JSON: {e}") from e


def _obj(v: Any, ctx: str) -> dict:
    if not isinstance(v, dict):
        raise FeeMetadataError(f"{ctx}: expected an object, got {type(v).__name__}")
    return v


def _require(obj: dict, key: str, ctx: str) -> Any:
    _obj(obj, ctx)
    if key not in obj:
        raise FeeMetadataError(f"{ctx}: missing required field {key!r}")
    return obj[key]


def _nonempty_str(v: Any, ctx: str) -> str:
    if not isinstance(v, str) or not v:
        raise FeeMetadataError(f"{ctx}: expected a nonempty string, got {v!r}")
    return v


def _multiplier(v: Any, ctx: str) -> Decimal:
    if isinstance(v, bool) or not isinstance(v, (int, Decimal)):
        raise FeeMetadataError(
            f"{ctx}: multiplier must be a JSON number parsed exactly (use fee_schedule.loads), "
            f"got {type(v).__name__} {v!r}"
        )
    try:
        d = fees.to_decimal(v, ctx, max_scale=MAX_MULTIPLIER_SCALE)
    except fees.FeeInputError as e:
        raise FeeMetadataError(str(e)) from e
    if d < 0:
        raise FeeMetadataError(f"{ctx}: multiplier must be non-negative, got {v!r}")
    return d


def _fee_type(v: Any, ctx: str) -> str:
    # Unknown strings are kept (the resolver reports UNSUPPORTED); non-strings
    # are malformed.
    return _nonempty_str(v, ctx)


def parse_ts(v: Any, ctx: str, *, allow_unusable: bool = False) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp to an aware UTC datetime. The zero value
    0001-01-01T00:00:00Z is not a real time: None if allow_unusable, else an
    error."""
    s = _nonempty_str(v, ctx)
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as e:
        raise FeeMetadataError(f"{ctx}: bad timestamp {s!r}") from e
    if dt.tzinfo is None:
        raise FeeMetadataError(f"{ctx}: timestamp without timezone {s!r}")
    dt = dt.astimezone(timezone.utc)
    if dt == _UNUSABLE_TS:
        if allow_unusable:
            return None
        raise FeeMetadataError(f"{ctx}: zero-value timestamp {s!r}")
    return dt


def _aware(dt: Any, ctx: str) -> datetime:
    if not isinstance(dt, datetime) or dt.tzinfo is None:
        raise FeeMetadataError(f"{ctx}: expected a timezone-aware datetime")
    return dt.astimezone(timezone.utc)


# ---- snapshot timing ------------------------------------------------------

@dataclass(frozen=True)
class AsOf:
    """The interval within which a response reflects origin state.
    `assumptions` names any unverified premise the interval rests on (e.g. an
    assumed bound on an undeclared cache); results built from it carry them."""
    lo: datetime
    hi: datetime
    assumptions: tuple = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "lo", _aware(self.lo, "AsOf.lo"))
        object.__setattr__(self, "hi", _aware(self.hi, "AsOf.hi"))
        if self.lo > self.hi:
            raise FeeMetadataError("AsOf.lo after AsOf.hi")
        object.__setattr__(self, "assumptions", tuple(sorted(set(
            _string_members(self.assumptions, "AsOf.assumptions")))))

    @staticmethod
    def span(parts: Iterable["AsOf"]) -> "AsOf":
        """Smallest interval containing every part, carrying all their assumptions."""
        parts = list(parts)
        if not parts:
            raise FeeMetadataError("AsOf.span needs at least one part")
        return AsOf(min(a.lo for a in parts), max(a.hi for a in parts),
                    tuple(x for a in parts for x in a.assumptions))

    @classmethod
    def from_http(
        cls, *, request_sent: datetime, response_received: datetime,
        cache_max_age_s: float, age_s: float, clock_skew_s: float,
        assumptions: tuple = (),
    ) -> "AsOf":
        """Conservative interval from a request's timing and cache headers.
        The body may be as old as its Age plus the cache lifetime before the
        request was sent; widened both ways by the allowed clock skew."""
        for name, v in (("cache_max_age_s", cache_max_age_s), ("age_s", age_s),
                        ("clock_skew_s", clock_skew_s)):
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not (0 <= v < 86400):
                raise FeeMetadataError(f"{name} must be a number in [0, 86400)")
        sent = _aware(request_sent, "request_sent")
        recv = _aware(response_received, "response_received")
        if recv < sent:
            raise FeeMetadataError("response_received before request_sent")
        return cls(
            lo=sent - timedelta(seconds=cache_max_age_s + age_s + clock_skew_s),
            hi=recv + timedelta(seconds=clock_skew_s),
            assumptions=assumptions,
        )


# ---- change-history coverage --------------------------------------------

VERIFIED = "VERIFIED"
ASSUMED = "ASSUMED"
NONE = "NONE"
ALL_TICKERS = "*"

# The conditions under which a pending-change listing covers future changes.
# A change being published once, early enough, establishes none of the others.
LEAD_TIME_ASSUMPTIONS = (
    "fee-change-continuously-visible-until-effective",
    "fee-change-amendments-obey-notice",
    "fee-change-pagination-complete",
)


def _string_members(value: Any, name: str) -> tuple[str, ...]:
    """Members of a collection of identifiers, each a nonempty str. A bare
    str/bytes is refused: iterating it would yield characters, letting
    unrelated ids with the same letters authorize each other."""
    if isinstance(value, (str, bytes, bytearray)):
        raise FeeMetadataError(f"{name} must be a collection of strings, not a bare string")
    try:
        members = tuple(value)
    except TypeError as e:
        raise FeeMetadataError(f"{name} must be a collection of strings") from e
    for m in members:
        if not isinstance(m, str) or not m:
            raise FeeMetadataError(f"{name} members must be nonempty strings, got {m!r}")
    return members


@dataclass(frozen=True)
class Coverage:
    """Which scheduled changes a change listing is known to contain.

    Covers changes for `scope` tickers with scheduled_ts in (start, end].
    VERIFIED: established without assumptions. ASSUMED: holds only if every
    listed assumption holds; admission must accept each one explicitly.
    NONE: no coverage claim. `evidence` references the captures it rests on.
    Pagination completeness alone is not coverage."""
    level: str
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    scope: frozenset = frozenset()
    evidence: tuple[str, ...] = ()
    assumption_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Validate collection shapes before anything is normalized.
        scope = frozenset(_string_members(self.scope, "Coverage.scope"))
        ev = _string_members(self.evidence, "Coverage.evidence")
        ids = tuple(sorted(set(_string_members(self.assumption_ids, "Coverage.assumption_ids"))))
        object.__setattr__(self, "scope", scope)
        object.__setattr__(self, "evidence", ev)
        object.__setattr__(self, "assumption_ids", ids)
        if self.level == NONE:
            if self.start is not None or self.end is not None or scope or ev or ids:
                raise FeeMetadataError("NONE coverage carries no interval, scope, evidence or assumptions")
            return
        if self.level not in (VERIFIED, ASSUMED):
            raise FeeMetadataError(f"coverage level must be VERIFIED/ASSUMED/NONE, got {self.level!r}")
        object.__setattr__(self, "start", _aware(self.start, "Coverage.start"))
        object.__setattr__(self, "end", _aware(self.end, "Coverage.end"))
        if not self.start < self.end:
            raise FeeMetadataError("Coverage.start must be before Coverage.end")
        if not scope:
            raise FeeMetadataError("Coverage.scope must name tickers (or '*')")
        if not ev:
            raise FeeMetadataError("Coverage needs at least one evidence reference")
        if self.level == VERIFIED and ids:
            raise FeeMetadataError("VERIFIED coverage cannot rest on assumptions")
        if self.level == ASSUMED and not ids:
            raise FeeMetadataError("ASSUMED coverage must name its assumptions")

    @classmethod
    def none(cls) -> "Coverage":
        return cls(NONE)

    def covers(self, ticker: str, start: datetime, end: datetime) -> bool:
        """True if every change for `ticker` scheduled in [start, end] is covered."""
        if self.level == NONE:
            return False
        if ALL_TICKERS not in self.scope and ticker not in self.scope:
            return False
        return self.start < start and end <= self.end

    def provenance(self) -> dict:
        return {"level": self.level, "start": self.start, "end": self.end,
                "scope": sorted(self.scope), "evidence": self.evidence,
                "assumption_ids": self.assumption_ids}


def lead_time_coverage(*, listing: AsOf, lead_time: timedelta, scope: Iterable[str],
                       evidence: Iterable[str]) -> Coverage:
    """ASSUMED coverage from a pending-change listing under a minimum
    announcement lead time L: changes scheduled in (listing.hi, listing.lo + L].
    L has no default and no documented source yet; the lead-time value is
    itself recorded as an assumption, alongside LEAD_TIME_ASSUMPTIONS."""
    if not isinstance(listing, AsOf):
        raise FeeMetadataError("listing must be an AsOf")
    if not isinstance(lead_time, timedelta) or lead_time <= timedelta(0):
        raise FeeMetadataError("lead_time must be a positive timedelta")
    end = listing.lo + lead_time
    if end <= listing.hi:
        raise FeeMetadataError("lead_time shorter than the listing's own uncertainty window")
    return Coverage(
        level=ASSUMED, start=listing.hi, end=end,
        scope=frozenset(_string_members(scope, "scope")),
        evidence=_string_members(evidence, "evidence"),
        assumption_ids=LEAD_TIME_ASSUMPTIONS + (lead_time_assumption_id(lead_time),),
    )


def lead_time_assumption_id(lead_time: timedelta) -> str:
    """Exact, injective id for a lead-time assumption (integer microseconds),
    so a policy accepting one duration never accepts a different one."""
    if not isinstance(lead_time, timedelta) or lead_time <= timedelta(0):
        raise FeeMetadataError("lead_time must be a positive timedelta")
    return f"fee-change-min-lead-time:{lead_time // timedelta(microseconds=1)}us"


# ---- parsed inputs --------------------------------------------------------

@dataclass(frozen=True)
class SeriesSnapshot:
    ticker: str
    fee_type: str
    multiplier: Decimal
    last_updated: Optional[datetime]  # provenance only; None when unusable
    as_of: AsOf


@dataclass(frozen=True)
class SeriesFeeChange:
    id: str
    series_ticker: str
    fee_type: str
    multiplier: Decimal
    scheduled: datetime


@dataclass(frozen=True)
class SeriesChangeList:
    changes: tuple[SeriesFeeChange, ...]
    include_historical: bool
    as_of: AsOf
    coverage: Coverage


@dataclass(frozen=True)
class EventSnapshot:
    event_ticker: str
    series_ticker: str
    type_override: Optional[str]          # None: no override (omitted or null)
    multiplier_override: Optional[Decimal]
    type_override_field: str               # "omitted" | "null" | "set"
    multiplier_override_field: str
    as_of: AsOf


@dataclass(frozen=True)
class EventFeeChange:
    id: str
    event_ticker: str
    series_ticker: str
    type_override: Optional[str]           # None clears -> series value
    multiplier_override: Optional[Decimal]
    scheduled: datetime


@dataclass(frozen=True)
class EventChangeList:
    changes: tuple[EventFeeChange, ...]
    as_of: AsOf
    coverage: Coverage


@dataclass(frozen=True)
class MarketFeeInfo:
    ticker: str
    event_ticker: str
    waiver_expiry: Optional[datetime]
    waiver_field: str                      # "omitted" | "null" | "set"
    as_of: AsOf


# ---- payload parsers ------------------------------------------------------

def parse_series(payload: dict, as_of: AsOf) -> SeriesSnapshot:
    s = _obj(_require(payload, "series", "series response"), "series")
    ctx = "series"
    last = s.get("last_updated_ts")
    return SeriesSnapshot(
        ticker=_nonempty_str(_require(s, "ticker", ctx), "series.ticker"),
        fee_type=_fee_type(_require(s, "fee_type", ctx), "series.fee_type"),
        multiplier=_multiplier(_require(s, "fee_multiplier", ctx), "series.fee_multiplier"),
        last_updated=(parse_ts(last, "series.last_updated_ts", allow_unusable=True)
                      if last not in (None, "") else None),
        as_of=as_of,
    )


def _check_coverage(coverage: Any) -> Coverage:
    if not isinstance(coverage, Coverage):
        raise FeeMetadataError("coverage must be a Coverage (use Coverage.none() for no claim)")
    return coverage


def parse_series_fee_changes(payload: dict, as_of: AsOf, *,
                             include_historical: bool,
                             coverage: Coverage) -> SeriesChangeList:
    """`include_historical` must reflect the request actually sent
    (show_historical=true). The default list holds only pending changes.
    `coverage` is the caller's explicit claim about what the listing holds;
    show_historical=true alone does not establish it."""
    if not isinstance(include_historical, bool):
        raise FeeMetadataError("include_historical must be a bool")
    _check_coverage(coverage)
    arr = _require(payload, "series_fee_change_arr", "series fee changes response")
    if not isinstance(arr, list):
        raise FeeMetadataError("series_fee_change_arr must be a list")
    out = []
    for i, c in enumerate(arr):
        ctx = f"series_fee_change_arr[{i}]"
        out.append(SeriesFeeChange(
            id=_nonempty_str(_require(c, "id", ctx), f"{ctx}.id"),
            series_ticker=_nonempty_str(_require(c, "series_ticker", ctx), f"{ctx}.series_ticker"),
            fee_type=_fee_type(_require(c, "fee_type", ctx), f"{ctx}.fee_type"),
            multiplier=_multiplier(_require(c, "fee_multiplier", ctx), f"{ctx}.fee_multiplier"),
            scheduled=parse_ts(_require(c, "scheduled_ts", ctx), f"{ctx}.scheduled_ts"),
        ))
    return SeriesChangeList(tuple(out), include_historical, as_of, coverage)


def _optional_override(obj: dict, key: str, parse: Callable[[Any, str], Any],
                       ctx: str) -> tuple[Any, str]:
    if key not in obj:
        return None, "omitted"
    if obj[key] is None:
        return None, "null"
    return parse(obj[key], f"{ctx}.{key}"), "set"


def parse_event(payload: dict, as_of: AsOf) -> EventSnapshot:
    e = _obj(_require(payload, "event", "event response"), "event")
    ctx = "event"
    t, tf = _optional_override(e, "fee_type_override", _fee_type, ctx)
    m, mf = _optional_override(e, "fee_multiplier_override", _multiplier, ctx)
    return EventSnapshot(
        event_ticker=_nonempty_str(_require(e, "event_ticker", ctx), "event.event_ticker"),
        series_ticker=_nonempty_str(_require(e, "series_ticker", ctx), "event.series_ticker"),
        type_override=t, multiplier_override=m,
        type_override_field=tf, multiplier_override_field=mf,
        as_of=as_of,
    )


def parse_event_fee_change_pages(pages: Sequence[dict], as_of: AsOf, *,
                                 coverage: Coverage) -> EventChangeList:
    """All pages of one listing, in order. The final page's cursor must be
    empty; a partial listing is refused rather than read as 'no changes'.
    A complete listing is still not coverage: `coverage` states that claim."""
    _check_coverage(coverage)
    if not isinstance(pages, (list, tuple)) or not pages:
        raise FeeMetadataError("event fee change pages: need at least one page")
    out = []
    for p_i, page in enumerate(pages):
        arr = _require(page, "event_fee_changes", f"page[{p_i}]")
        cursor = _require(page, "cursor", f"page[{p_i}]")
        if not isinstance(arr, list) or not isinstance(cursor, str):
            raise FeeMetadataError(f"page[{p_i}]: malformed")
        last = p_i == len(pages) - 1
        if last and cursor != "":
            raise FeeMetadataError("event fee changes: pagination incomplete")
        if not last and cursor == "":
            raise FeeMetadataError("event fee changes: page after an empty cursor")
        for i, c in enumerate(arr):
            ctx = f"page[{p_i}].event_fee_changes[{i}]"
            for k in ("fee_type_override", "fee_multiplier_override"):
                _require(c, k, ctx)  # required by schema even though nullable
            t = c["fee_type_override"]
            m = c["fee_multiplier_override"]
            out.append(EventFeeChange(
                id=_nonempty_str(_require(c, "id", ctx), f"{ctx}.id"),
                event_ticker=_nonempty_str(_require(c, "event_ticker", ctx), f"{ctx}.event_ticker"),
                series_ticker=_nonempty_str(_require(c, "series_ticker", ctx), f"{ctx}.series_ticker"),
                type_override=None if t is None else _fee_type(t, f"{ctx}.fee_type_override"),
                multiplier_override=None if m is None else _multiplier(m, f"{ctx}.fee_multiplier_override"),
                scheduled=parse_ts(_require(c, "scheduled_ts", ctx), f"{ctx}.scheduled_ts"),
            ))
    return EventChangeList(tuple(out), as_of, coverage)


def parse_market(market: dict, as_of: AsOf) -> MarketFeeInfo:
    """A single market object (the value of `market`, or one element of
    `markets`), with the timing of the response it came from."""
    ctx = "market"
    _obj(market, ctx)
    if not isinstance(as_of, AsOf):
        raise FeeMetadataError("market: as_of must be an AsOf")
    w, wf = _optional_override(
        market, "fee_waiver_expiration_time", lambda v, c: parse_ts(v, c), ctx,
    )
    return MarketFeeInfo(
        ticker=_nonempty_str(_require(market, "ticker", ctx), "market.ticker"),
        event_ticker=_nonempty_str(_require(market, "event_ticker", ctx), "market.event_ticker"),
        waiver_expiry=w, waiver_field=wf, as_of=as_of,
    )


# ---- resolution -----------------------------------------------------------

@dataclass(frozen=True)
class FeeResolution:
    status: str
    reason: str
    role: str
    at: datetime
    market_ticker: str = ""           # the market this resolution is for; required by consumers
    fee_type: Optional[str] = None
    multiplier: Optional[Decimal] = None
    coefficient: Optional[Decimal] = None  # only for RESOLVED / EXPLICIT_ZERO
    valid_until: Optional[datetime] = None
    applied_change_ids: tuple[str, ...] = ()
    assumptions: tuple[str, ...] = ()  # every assumption this result rests on
    provenance: dict = field(default_factory=dict)

    @property
    def has_coefficient(self) -> bool:
        return self.status in (RESOLVED, EXPLICIT_ZERO)

    @property
    def verified(self) -> bool:
        """A coefficient that rests on no assumptions."""
        return self.has_coefficient and not self.assumptions


def admissible(result: FeeResolution, accepted_assumptions: Iterable[str]) -> bool:
    """The only gate a money path may use: a coefficient exists AND the
    policy explicitly accepts every assumption it rests on. Analysis may read
    has_coefficient; admission must not."""
    if not isinstance(result, FeeResolution):
        raise FeeMetadataError("admissible() needs a FeeResolution")
    accepted = set(_string_members(accepted_assumptions, "accepted_assumptions"))
    return result.has_coefficient and set(result.assumptions) <= accepted


class _Unavailable(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _resolve_value(snapshot_value: Any, as_of: AsOf,
                   changes: Iterable[tuple[datetime, Any, str]],
                   at: datetime, label: str) -> tuple[Any, tuple[str, ...]]:
    """Value of one target at `at`, from a snapshot taken somewhere in
    [as_of.lo, as_of.hi] plus scheduled changes (ts, value, id)."""
    by_ts: dict[datetime, tuple[Any, list[str]]] = {}
    for ts, value, cid in changes:
        if ts in by_ts:
            if by_ts[ts][0] != value:
                raise _Unavailable(f"ambiguous_tie:{label}")
            by_ts[ts][1].append(cid)
        else:
            by_ts[ts] = (value, [cid])
    ordered = sorted(by_ts.items())
    window = [(ts, v, ids) for ts, (v, ids) in ordered if as_of.lo <= ts <= as_of.hi]

    if at < as_of.lo:
        raise _Unavailable(f"before_snapshot:{label}")
    if at <= as_of.hi:
        if window:
            raise _Unavailable(f"snapshot_boundary_conflict:{label}")
        return snapshot_value, ()
    if window and window[-1][1] != snapshot_value:
        raise _Unavailable(f"snapshot_boundary_conflict:{label}")
    after = [(ts, v, ids) for ts, (v, ids) in ordered if as_of.hi < ts <= at]
    if after:
        return after[-1][1], tuple(after[-1][2])
    return snapshot_value, ()


def _next_boundary(changes: Iterable[datetime], at: datetime) -> Optional[datetime]:
    later = [ts for ts in changes if ts > at]
    return min(later) if later else None


def resolve_fee_schedule(
    *,
    market: MarketFeeInfo,
    series: SeriesSnapshot,
    series_changes: SeriesChangeList,
    event: EventSnapshot,
    event_changes: EventChangeList,
    role: str,
    at: datetime,
    ttl: timedelta,
) -> FeeResolution:
    """Resolve the fee schedule for one role at one instant. `ttl` is
    caller-supplied: how long after each snapshot's hi bound it may be
    trusted. Known change boundaries and waiver expiries end validity
    regardless of ttl (see valid_until)."""
    if role not in (TAKER, MAKER):
        raise FeeMetadataError(f"role must be {TAKER!r} or {MAKER!r}, got {role!r}")
    at = _aware(at, "at")
    if not isinstance(ttl, timedelta) or ttl < timedelta(0):
        raise FeeMetadataError("ttl must be a non-negative timedelta")
    if market.event_ticker != event.event_ticker:
        raise FeeMetadataError("market and event snapshots disagree on event_ticker")
    if event.series_ticker != series.ticker:
        raise FeeMetadataError("event and series snapshots disagree on series_ticker")

    s_changes = [c for c in series_changes.changes if c.series_ticker == series.ticker]
    e_changes = [c for c in event_changes.changes if c.event_ticker == event.event_ticker]
    for c in e_changes:
        if c.series_ticker != event.series_ticker:
            raise FeeMetadataError(
                f"event fee change {c.id!r} names series {c.series_ticker!r} but event "
                f"{event.event_ticker!r} belongs to {event.series_ticker!r}"
            )
    inputs = {
        "market": market.as_of,
        "series": series.as_of, "series_changes": series_changes.as_of,
        "event": event.as_of, "event_changes": event_changes.as_of,
    }
    prov: dict = {
        "series_ticker": series.ticker,
        "event_ticker": event.event_ticker,
        "market_ticker": market.ticker,
        "series_last_updated": series.last_updated,
        "event_type_override_field": event.type_override_field,
        "event_multiplier_override_field": event.multiplier_override_field,
        "waiver_field": market.waiver_field,
        "waiver_expiry": market.waiver_expiry,
        "as_of": {k: (v.lo, v.hi) for k, v in inputs.items()},
        "ttl": ttl,
        "coverage": {"series_changes": series_changes.coverage.provenance(),
                     "event_changes": event_changes.coverage.provenance()},
    }
    # Attached to every result, including early UNAVAILABLE returns.
    assumptions = tuple(sorted(set(series_changes.coverage.assumption_ids)
                               | set(event_changes.coverage.assumption_ids)
                               | {x for a in inputs.values() for x in a.assumptions}))

    def out(status: str, reason: str, **kw) -> FeeResolution:
        return FeeResolution(status=status, reason=reason, role=role, at=at,
                             market_ticker=market.ticker,
                             assumptions=assumptions, provenance=prov, **kw)

    stale = sorted(k for k, a in inputs.items() if at > a.hi + ttl)
    if stale:
        return out(UNAVAILABLE, "stale:" + ",".join(stale))
    # Market metadata has no change feed: it is only known from its own
    # observation window onward.
    if at < market.as_of.lo:
        return out(UNAVAILABLE, "before_snapshot:market")
    if not series_changes.include_historical:
        return out(UNAVAILABLE, "change_history_required")

    # A snapshot observed anywhere in [lo, hi] may already reflect a change
    # scheduled up to hi, even when `at` is earlier: coverage must span
    # [lo, max(at, hi)] for each change listing the result depends on.
    for label, cov, ticker, snap in (
        ("series_changes", series_changes.coverage, series.ticker, series.as_of),
        ("event_changes", event_changes.coverage, event.event_ticker, event.as_of),
    ):
        if not cov.covers(ticker, snap.lo, max(at, snap.hi)):
            return out(UNAVAILABLE, f"history_unverified:{label}")
    # Validity ends at the earliest snapshot expiry, coverage expiry, or
    # known boundary after `at`.
    bounds = [a.hi + ttl for a in inputs.values()]
    bounds += [series_changes.coverage.end, event_changes.coverage.end]
    nb = _next_boundary([c.scheduled for c in s_changes] + [c.scheduled for c in e_changes], at)
    if nb is not None:
        bounds.append(nb)
    if market.waiver_expiry is not None and market.waiver_expiry > at:
        bounds.append(market.waiver_expiry)
    valid_until = min(bounds)

    if market.waiver_expiry is not None and at < market.waiver_expiry:
        return out(WAIVER_UNRESOLVED, "waiver_may_apply", valid_until=valid_until)

    try:
        (s_type, s_mult), s_ids = _resolve_value(
            (series.fee_type, series.multiplier), series.as_of,
            ((c.scheduled, (c.fee_type, c.multiplier), c.id) for c in s_changes),
            at, "series",
        )
        e_type, e_type_ids = _resolve_value(
            event.type_override, event.as_of,
            ((c.scheduled, c.type_override, c.id) for c in e_changes),
            at, "event_type",
        )
        e_mult, e_mult_ids = _resolve_value(
            event.multiplier_override, event.as_of,
            ((c.scheduled, c.multiplier_override, c.id) for c in e_changes),
            at, "event_multiplier",
        )
    except _Unavailable as u:
        return out(UNAVAILABLE, u.reason, valid_until=valid_until)

    fee_type = e_type if e_type is not None else s_type
    mult = e_mult if e_mult is not None else s_mult
    applied = tuple(dict.fromkeys(s_ids + e_type_ids + e_mult_ids))
    prov["type_source"] = "event" if e_type is not None else "series"
    prov["multiplier_source"] = "event" if e_mult is not None else "series"
    common = dict(fee_type=fee_type, multiplier=mult, valid_until=valid_until,
                  applied_change_ids=applied)

    if fee_type not in KNOWN_FEE_TYPES or fee_type == FEE_TYPE_FLAT:
        return out(UNSUPPORTED, f"fee_type:{fee_type}", **common)

    if role == TAKER:
        base = _TAKER_BASE
    else:
        base = _MAKER_BASE[fee_type]
        if base is None:
            return out(EXPLICIT_ZERO, "no_maker_fee_for_type",
                       coefficient=Decimal(0), **common)
    coeff = base * fees.to_fraction(mult, "multiplier", max_scale=MAX_MULTIPLIER_SCALE)
    if coeff == 0:
        return out(EXPLICIT_ZERO, "multiplier_zero", coefficient=Decimal(0), **common)
    return out(RESOLVED, "ok", coefficient=fees.fraction_to_decimal(coeff), **common)
