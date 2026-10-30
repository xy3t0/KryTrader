"""Exact fee ledger: persistence for model fee accounting and exchange-reported
fee evidence, kept separate so they can be reconciled. Additive only.

Nothing in the app writes here yet (1d-1). Ingestion of exchange-reported
fees is 1d-2; paper-execution accounting is 1d-3.

Principles
  * Exact values are DECIMAL STRINGS. Integer microdollar columns exist only
    where the value is always an exact microdollar multiple (trade fee,
    balance delta); everything else (rounding fee, net fee, accumulator,
    unrounded model fee) is text only.
  * Model estimates and exchange-reported amounts live in different tables.
    A model value never overwrites a reported one, and vice versa.
  * NULL means unknown. "0" means an explicit zero. Never conflated.
  * Pre-existing REAL fee columns are marked fee_source='legacy' (the
    column default, so rows written by not-yet-migrated code are legacy
    too). Legacy values are never copied into this ledger as exact values.

Identity
  account key   = (kalshi_env, account_id). kalshi_env is 'production' or
                  'paper'; account_id is a caller-chosen stable id for the
                  account (e.g. a key fingerprint, or 'paper-local').
  order         = (account key, order_id)
  model fill    = (account key, fill_id), UNIQUE: duplicate fills are refused
                  by the database, not only by code.

Transactions
  apply_model_fill() requires an open transaction on the caller's
  connection (conn.in_transaction; entering db.get_db() alone does not
  start one) and works inside a SAVEPOINT: the order-state read, the fill
  insert and the accumulator update roll back together on error, and the
  caller's transaction decides whether they commit alongside its own
  cash/position/evidence writes. Each order is bound to one market, side
  and action; maker/taker may vary by fill. The order-state update is guarded by a version number
  (compare-and-set), so two writers cannot both advance the same
  accumulator. The caller's transaction (e.g. db.get_db()) commits.
"""
from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any, Optional

import fee_schedule as fs
import fees

ENVIRONMENTS = ("production", "paper")
PRECISION_SOURCES = ("configured", "api_verified")
ROLES = ("taker", "maker")

# Existing tables whose REAL fee columns predate this ledger.
LEGACY_FEE_TABLES = ("bot_positions", "crypto15m_positions", "paper_fills", "paper_orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS fee_order_state (
    kalshi_env TEXT NOT NULL CHECK (kalshi_env IN ('production', 'paper')),
    account_id TEXT NOT NULL CHECK (length(account_id) > 0),
    order_id TEXT NOT NULL CHECK (length(order_id) > 0),
    ticker TEXT NOT NULL CHECK (length(ticker) > 0),     -- the order's market
    side TEXT NOT NULL CHECK (side IN ('yes', 'no')),
    action TEXT NOT NULL CHECK (action IN ('buy', 'sell')),
    precision TEXT NOT NULL,                   -- decimal string, e.g. '0.0001'
    precision_source TEXT NOT NULL CHECK (precision_source IN ('configured', 'api_verified')),
    accumulator TEXT,                          -- decimal string; NULL = unknown
    accumulator_status TEXT NOT NULL CHECK (accumulator_status IN ('known', 'unknown')),
    unknown_reason TEXT,
    version INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (kalshi_env, account_id, order_id),
    CHECK ((accumulator_status = 'known') = (accumulator IS NOT NULL))
);

-- One row per fill, model side only. Model columns are NULL when the fee
-- schedule could not be resolved or the order's accumulator is unknown.
CREATE TABLE IF NOT EXISTS fee_fill_model (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kalshi_env TEXT NOT NULL CHECK (kalshi_env IN ('production', 'paper')),
    account_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    fill_id TEXT NOT NULL CHECK (length(fill_id) > 0),
    ticker TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('yes', 'no')),
    action TEXT NOT NULL CHECK (action IN ('buy', 'sell')),
    role TEXT NOT NULL CHECK (role IN ('taker', 'maker')),
    price TEXT NOT NULL,                       -- dollars, decimal string
    count TEXT NOT NULL,                       -- contracts, decimal string
    revenue TEXT NOT NULL,                     -- signed, before fees
    filled_at TEXT NOT NULL,
    resolution_status TEXT NOT NULL,           -- fee_schedule status, or ORDER_UNKNOWN
    resolution_reason TEXT NOT NULL,
    coefficient TEXT,
    model_fee TEXT,                            -- unrounded
    trade_fee TEXT,
    trade_fee_micro INTEGER,
    rounding_fee TEXT,
    rebate TEXT,
    net_fee TEXT,
    balance_delta TEXT,
    balance_delta_micro INTEGER,
    accumulator_after TEXT,
    assumptions TEXT NOT NULL,                 -- JSON list
    provenance TEXT NOT NULL,                  -- JSON
    created_at TEXT NOT NULL,
    UNIQUE (kalshi_env, account_id, fill_id),
    FOREIGN KEY (kalshi_env, account_id, order_id)
        REFERENCES fee_order_state (kalshi_env, account_id, order_id),
    CHECK ((net_fee IS NULL) = (trade_fee IS NULL)),
    CHECK ((trade_fee IS NULL) = (trade_fee_micro IS NULL)),
    CHECK ((balance_delta IS NULL) = (balance_delta_micro IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_fee_fill_model_order
    ON fee_fill_model (kalshi_env, account_id, order_id);

-- Exchange-reported fee amounts, exactly as reported (1d-2 writes these).
-- report_kind separates per-fill charges from cumulative order totals and
-- position aggregates: totals are observations of a running number, stored
-- per observation and never summed. reported_value NULL with field_state
-- 'absent' means the field was missing; '0' means the exchange said zero.
CREATE TABLE IF NOT EXISTS fee_exchange_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kalshi_env TEXT NOT NULL CHECK (kalshi_env IN ('production', 'paper')),
    account_id TEXT NOT NULL CHECK (length(account_id) > 0),
    report_kind TEXT NOT NULL CHECK (report_kind IN ('fill', 'order_total', 'position_aggregate')),
    subject_id TEXT NOT NULL CHECK (length(subject_id) > 0),   -- fill / order / position id
    field_name TEXT NOT NULL,                  -- e.g. 'fee_cost', 'taker_fees_dollars'
    field_state TEXT NOT NULL CHECK (field_state IN ('reported', 'absent')),
    reported_value TEXT,                       -- exact string as received
    observed_at TEXT NOT NULL,
    evidence_ref TEXT NOT NULL,                -- capture id or equivalent
    created_at TEXT NOT NULL,
    CHECK ((field_state = 'reported') = (reported_value IS NOT NULL))
);
-- A per-fill charge is recorded once per field; re-polling cannot duplicate it.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fee_exchange_fill
    ON fee_exchange_reports (kalshi_env, account_id, subject_id, field_name)
    WHERE report_kind = 'fill';
CREATE INDEX IF NOT EXISTS idx_fee_exchange_subject
    ON fee_exchange_reports (kalshi_env, account_id, report_kind, subject_id, observed_at);

-- Which evidence (captures) a trade, forecast or evaluation depends on.
-- Pruning must skip anything referenced here.
CREATE TABLE IF NOT EXISTS fee_evidence_refs (
    evidence_ref TEXT NOT NULL CHECK (length(evidence_ref) > 0),
    kalshi_env TEXT NOT NULL CHECK (kalshi_env IN ('production', 'paper')),
    -- Account-scoped referrers (fill, order, trade) carry their account;
    -- app-scoped referrers (forecast, evaluation) use ''.
    account_id TEXT NOT NULL,
    referrer_kind TEXT NOT NULL CHECK (referrer_kind IN ('fill', 'order', 'trade', 'forecast', 'evaluation')),
    referrer_id TEXT NOT NULL CHECK (length(referrer_id) > 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (evidence_ref, kalshi_env, account_id, referrer_kind, referrer_id),
    CHECK ((referrer_kind IN ('fill', 'order', 'trade')) = (length(account_id) > 0))
);
"""

ORDER_UNKNOWN = "ORDER_UNKNOWN"


class FeeLedgerError(RuntimeError):
    """A ledger write was refused (bad input, conflict, or inconsistency)."""


class DuplicateFillError(FeeLedgerError):
    """The fill was already recorded for this account."""


class ConcurrentUpdateError(FeeLedgerError):
    """The order state changed underneath this write; retry from a fresh read."""


# ---- schema ---------------------------------------------------------------

def legacy_source_migrations() -> list[str]:
    return [f"ALTER TABLE {t} ADD COLUMN fee_source TEXT DEFAULT 'legacy'"
            for t in LEGACY_FEE_TABLES]


def init_schema(conn: sqlite3.Connection) -> None:
    """Create ledger tables and mark legacy fee columns. Idempotent."""
    conn.executescript(SCHEMA)
    for stmt in legacy_source_migrations():
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError as e:
            # Only "already exists" is expected; a missing table is not.
            if "duplicate column" not in str(e).lower():
                raise


def evidence_dir(env: str) -> Path:
    """Where fee-metadata captures for `env` live:
    <app data>/fee-evidence/<env>, using the database's data-root fallback."""
    _env(env)
    import db  # local import: db imports this module during init
    d = db._data_dir() / "fee-evidence" / env
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---- helpers ----------------------------------------------------------------

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")


def _env(env: Any) -> str:
    if env not in ENVIRONMENTS:
        raise FeeLedgerError(f"kalshi_env must be one of {ENVIRONMENTS}, got {env!r}")
    return env


def _id(v: Any, name: str) -> str:
    if not isinstance(v, str) or not _ID_RE.match(v):
        raise FeeLedgerError(f"{name} must match {_ID_RE.pattern}, got {v!r}")
    return v


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dec_str(v: Optional[Decimal]) -> Optional[str]:
    return None if v is None else str(v)


def exact_micro(v: Optional[Decimal]) -> Optional[int]:
    """Integer microdollars, only when exact; raises otherwise."""
    if v is None:
        return None
    f = fees.to_fraction(v, "amount") / Fraction(1, 10**6)
    if f.denominator != 1:
        raise FeeLedgerError(f"{v} is not an exact microdollar amount")
    return int(f)


def _json_default(o: Any) -> Any:
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, timedelta):
        return {"seconds": o // timedelta(microseconds=1) / 1e6}
    if isinstance(o, Decimal):
        return str(o)
    if isinstance(o, (set, frozenset, tuple)):
        return sorted(o) if isinstance(o, (set, frozenset)) else list(o)
    raise TypeError(f"not JSON serializable: {type(o).__name__}")


def _dumps(o: Any) -> str:
    return json.dumps(o, default=_json_default, sort_keys=True, separators=(",", ":"))


# ---- order state --------------------------------------------------------------

@dataclass(frozen=True)
class OrderStateRow:
    kalshi_env: str
    account_id: str
    order_id: str
    ticker: str
    side: str
    action: str
    precision: Decimal
    precision_source: str
    accumulator: Optional[Decimal]
    accumulator_status: str
    unknown_reason: Optional[str]
    version: int


def get_order_state(conn: sqlite3.Connection, *, kalshi_env: str, account_id: str,
                    order_id: str) -> Optional[OrderStateRow]:
    r = conn.execute(
        "SELECT precision, precision_source, accumulator, accumulator_status, "
        "unknown_reason, version, ticker, side, action FROM fee_order_state "
        "WHERE kalshi_env=? AND account_id=? AND order_id=?",
        (_env(kalshi_env), _id(account_id, "account_id"), _id(order_id, "order_id")),
    ).fetchone()
    if r is None:
        return None
    return OrderStateRow(
        kalshi_env, account_id, order_id, ticker=r[6], side=r[7], action=r[8],
        precision=fees.to_decimal(r[0], "precision"), precision_source=r[1],
        accumulator=None if r[2] is None else fees.to_decimal(r[2], "accumulator"),
        accumulator_status=r[3], unknown_reason=r[4], version=int(r[5]),
    )


@dataclass(frozen=True)
class ModelFillResult:
    fill_row_id: int
    resolution_status: str
    fee: Optional[fees.FillFeeResult]   # None when no model fee could be computed
    order_state: OrderStateRow


def apply_model_fill(
    conn: sqlite3.Connection, *,
    kalshi_env: str, account_id: str, order_id: str, fill_id: str,
    precision: Any, precision_source: str,
    ticker: str, side: str, action: str, role: str,
    price: Any, count: Any, filled_at: datetime,
    resolution: fs.FeeResolution,
) -> ModelFillResult:
    """Record one fill's MODEL fee accounting atomically.

    The fee schedule `resolution` must be for this fill's role and time. If
    it has no coefficient, or the order's accumulator is already unknown,
    the fill is still recorded with NULL model amounts and the order's
    accumulator becomes (stays) unknown: later rounding/rebates for the
    order can no longer be modelled exactly. Analysis-grade only: callers
    must not treat a model fill as exchange evidence."""
    env = _env(kalshi_env)
    acct = _id(account_id, "account_id")
    oid = _id(order_id, "order_id")
    fid = _id(fill_id, "fill_id")
    if precision_source not in PRECISION_SOURCES:
        raise FeeLedgerError(f"precision_source must be one of {PRECISION_SOURCES}")
    if role not in ROLES or side not in ("yes", "no") or action not in ("buy", "sell"):
        raise FeeLedgerError("role/side/action invalid")
    if not isinstance(ticker, str) or not ticker:
        raise FeeLedgerError("ticker required")
    if not isinstance(resolution, fs.FeeResolution) or resolution.role != role:
        raise FeeLedgerError("resolution must be a FeeResolution for this fill's role")
    if not isinstance(resolution.market_ticker, str) or not resolution.market_ticker:
        raise FeeLedgerError("resolution has no market identity")
    if resolution.market_ticker != ticker:
        raise FeeLedgerError(
            f"resolution is for market {resolution.market_ticker!r}, fill is on {ticker!r}")
    if not conn.in_transaction:
        raise FeeLedgerError(
            "apply_model_fill needs an open transaction (BEGIN first); the caller owns commit")
    if not isinstance(filled_at, datetime) or filled_at.tzinfo is None:
        raise FeeLedgerError("filled_at must be timezone-aware")
    if resolution.at != filled_at.astimezone(timezone.utc):
        raise FeeLedgerError("resolution.at must equal filled_at")
    try:
        prec = fees.validate_precision(precision)
        revenue = fees.fill_revenue(price, count, action)
        price_d = fees.to_decimal(price, "price")
        count_d = fees.to_decimal(count, "count")
    except fees.FeeInputError as e:
        raise FeeLedgerError(str(e)) from e

    now = _now()
    conn.execute("SAVEPOINT fee_apply_model_fill")
    try:
        state = get_order_state(conn, kalshi_env=env, account_id=acct, order_id=oid)
        if state is None:
            conn.execute(
                "INSERT INTO fee_order_state (kalshi_env, account_id, order_id, ticker, side, "
                "action, precision, precision_source, accumulator, accumulator_status, version, "
                "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?, 'known', 0, ?, ?)",
                (env, acct, oid, ticker, side, action, str(prec), precision_source, "0", now, now),
            )
            state = get_order_state(conn, kalshi_env=env, account_id=acct, order_id=oid)
        if state.precision != prec or state.precision_source != precision_source:
            raise FeeLedgerError("precision/precision_source differ from the order's recorded state")
        # One order, one instrument and direction; maker/taker may change per fill.
        for name, mine, theirs in (("ticker", ticker, state.ticker), ("side", side, state.side),
                                   ("action", action, state.action)):
            if mine != theirs:
                raise FeeLedgerError(f"order {oid} is {name}={theirs!r}; fill has {name}={mine!r}")

        fee: Optional[fees.FillFeeResult] = None
        new_status, new_acc, reason = state.accumulator_status, state.accumulator, state.unknown_reason
        model_fee = None
        if state.accumulator_status != "known":
            status, why = ORDER_UNKNOWN, state.unknown_reason or "accumulator unknown"
        elif not resolution.has_coefficient:
            status, why = resolution.status, resolution.reason
            new_status, new_acc = "unknown", None
            reason = f"fill {fid}: {resolution.status}:{resolution.reason}"
        else:
            status, why = resolution.status, resolution.reason
            model_fee = fees.quadratic_model_fee(price_d, count_d, resolution.coefficient)
            fee = fees.settle_fill(revenue, model_fee, prec, state.accumulator)
            new_acc = fee.accumulator

        cur = conn.execute(
            "INSERT INTO fee_fill_model (kalshi_env, account_id, order_id, fill_id, ticker, "
            "side, action, role, price, count, revenue, filled_at, resolution_status, "
            "resolution_reason, coefficient, model_fee, trade_fee, trade_fee_micro, "
            "rounding_fee, rebate, net_fee, balance_delta, balance_delta_micro, "
            "accumulator_after, assumptions, provenance, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (env, acct, oid, fid, ticker, side, action, role, str(price_d), str(count_d),
             str(revenue), filled_at.astimezone(timezone.utc).isoformat(), status, why,
             _dec_str(resolution.coefficient) if fee else None, _dec_str(model_fee) if fee else None,
             _dec_str(fee.trade_fee) if fee else None, exact_micro(fee.trade_fee) if fee else None,
             _dec_str(fee.rounding_fee) if fee else None, _dec_str(fee.rebate) if fee else None,
             _dec_str(fee.net_fee) if fee else None, _dec_str(fee.balance_delta) if fee else None,
             exact_micro(fee.balance_delta) if fee else None,
             _dec_str(fee.accumulator) if fee else None,
             _dumps(list(resolution.assumptions)), _dumps(resolution.provenance), now),
        )
        upd = conn.execute(
            "UPDATE fee_order_state SET accumulator=?, accumulator_status=?, unknown_reason=?, "
            "version=version+1, updated_at=? "
            "WHERE kalshi_env=? AND account_id=? AND order_id=? AND version=?",
            (_dec_str(new_acc), new_status, reason, now, env, acct, oid, state.version),
        )
        if upd.rowcount != 1:
            raise ConcurrentUpdateError(f"order {oid} state changed during fill {fid}")
        row_id = cur.lastrowid
        final = get_order_state(conn, kalshi_env=env, account_id=acct, order_id=oid)
    except sqlite3.IntegrityError as e:
        conn.execute("ROLLBACK TO fee_apply_model_fill")
        conn.execute("RELEASE fee_apply_model_fill")
        if "fee_fill_model" in str(e) and "UNIQUE" in str(e).upper():
            raise DuplicateFillError(f"fill {fid} already recorded for {env}/{acct}") from e
        raise FeeLedgerError(f"ledger constraint failed: {e}") from e
    except BaseException:
        conn.execute("ROLLBACK TO fee_apply_model_fill")
        conn.execute("RELEASE fee_apply_model_fill")
        raise
    conn.execute("RELEASE fee_apply_model_fill")
    return ModelFillResult(row_id, status, fee, final)


REFERRER_KINDS = ("fill", "order", "trade", "forecast", "evaluation")


ACCOUNT_SCOPED_REFERRERS = ("fill", "order", "trade")


def add_evidence_ref(conn: sqlite3.Connection, *, evidence_ref: str, kalshi_env: str,
                     referrer_kind: str, referrer_id: str,
                     account_id: Optional[str] = None) -> None:
    """Mark evidence as referenced (retention). Exact duplicates are
    idempotent; the same evidence may be referenced from different
    environments, accounts and referrers. account_id is required for
    account-scoped referrers (fill, order, trade) and refused otherwise."""
    if referrer_kind not in REFERRER_KINDS:
        raise FeeLedgerError(f"referrer_kind must be one of {REFERRER_KINDS}")
    if referrer_kind in ACCOUNT_SCOPED_REFERRERS:
        acct = _id(account_id, "account_id")
    elif account_id is not None:
        raise FeeLedgerError(f"{referrer_kind} references are not account-scoped")
    else:
        acct = ""
    conn.execute(
        "INSERT OR IGNORE INTO fee_evidence_refs (evidence_ref, kalshi_env, account_id, "
        "referrer_kind, referrer_id, created_at) VALUES (?,?,?,?,?,?)",
        (_id(evidence_ref, "evidence_ref"), _env(kalshi_env), acct, referrer_kind,
         _id(referrer_id, "referrer_id"), _now()),
    )


def is_evidence_referenced(conn: sqlite3.Connection, evidence_ref: str) -> bool:
    return conn.execute("SELECT 1 FROM fee_evidence_refs WHERE evidence_ref=? LIMIT 1",
                        (evidence_ref,)).fetchone() is not None
