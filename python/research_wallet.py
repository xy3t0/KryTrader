"""Isolated paper research wallet with exact accounting (plan step A3, core).

A research wallet is a separate imaginary bankroll for one cohort
(e.g. the frozen $4 experiment). It never touches the legacy paper book:
legacy REAL balances do not seed it, and it has no reset or top-up.

Every simulated fill goes through apply_fill(FillEvent):
  * the FeeContext must be usable, under the wallet's own policy, for the
    fill's market, role and instant;
  * fees are settled per fill through fee_ledger (persistent per-order
    accumulator), never from an average price;
  * cash, reservations, the position and the event log change in the
    caller's transaction, inside one savepoint: all or nothing.
Cash always reconciles exactly: starting_cash + sum(event cash deltas).

Quantities are decimal strings at the venue's 0.01-contract granularity;
anything finer is rejected, never rounded into a different trade.
"""
from __future__ import annotations

import math
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from fractions import Fraction
from typing import Any, Optional

import fee_context as fctx
import fee_ledger
import fees

COUNT_QUANTUM = Fraction(1, 100)       # FixedPointCount: 0.01 contracts
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS rw_wallets (
    wallet_id TEXT PRIMARY KEY CHECK (length(wallet_id) > 0),
    kalshi_env TEXT NOT NULL CHECK (kalshi_env = 'paper'),
    account_id TEXT NOT NULL,
    policy TEXT NOT NULL,
    starting_cash TEXT NOT NULL,
    cash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (kalshi_env, account_id) REFERENCES fee_accounts (kalshi_env, account_id)
);
CREATE TABLE IF NOT EXISTS rw_reservations (
    wallet_id TEXT NOT NULL REFERENCES rw_wallets (wallet_id),
    order_id TEXT NOT NULL,
    amount TEXT NOT NULL,
    PRIMARY KEY (wallet_id, order_id)
);
CREATE TABLE IF NOT EXISTS rw_positions (
    wallet_id TEXT NOT NULL REFERENCES rw_wallets (wallet_id),
    ticker TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('yes', 'no')),
    quantity TEXT NOT NULL,
    cost_basis TEXT NOT NULL,             -- cash paid incl. fees, for the open quantity
    PRIMARY KEY (wallet_id, ticker, side)
);
CREATE TABLE IF NOT EXISTS rw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_id TEXT NOT NULL REFERENCES rw_wallets (wallet_id),
    kind TEXT NOT NULL CHECK (kind IN ('fill', 'settle', 'reserve', 'release')),
    ref_id TEXT NOT NULL,                 -- fill_id / settlement key / order_id
    ticker TEXT,
    side TEXT,
    quantity TEXT,
    cash_delta TEXT NOT NULL,
    reserved_delta TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    evidence TEXT NOT NULL,               -- JSON list of refs
    UNIQUE (wallet_id, kind, ref_id)
);
"""


class WalletError(RuntimeError):
    """A wallet operation was refused; the wallet is unchanged."""


class InsufficientFunds(WalletError):
    pass


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def _id(v: Any, name: str) -> str:
    if not isinstance(v, str) or not _ID_RE.match(v):
        raise WalletError(f"{name} must match {_ID_RE.pattern}, got {v!r}")
    return v


def _frac(v: Any, name: str) -> Fraction:
    try:
        return fees.to_fraction(v, name)
    except fees.FeeInputError as e:
        raise WalletError(str(e)) from e


def _dec(f: Fraction) -> str:
    return str(fees.fraction_to_decimal(f))


def _count(v: Any) -> Fraction:
    c = _frac(v, "count")
    if c <= 0 or (c / COUNT_QUANTUM).denominator != 1:
        raise WalletError(f"count {v!r} is not a positive multiple of 0.01 contracts")
    return c


def _iso(dt: datetime) -> str:
    if not isinstance(dt, datetime) or dt.tzinfo is None:
        raise WalletError("timestamps must be timezone-aware")
    return dt.astimezone(timezone.utc).isoformat()


def _json(refs) -> str:
    import json
    refs = list(refs)
    if not all(isinstance(r, str) and r for r in refs):
        raise WalletError("evidence refs must be nonempty strings")
    return json.dumps(sorted(set(refs)))


class _Savepoint:
    def __init__(self, conn, name):
        self.conn, self.name = conn, name

    def __enter__(self):
        if not self.conn.in_transaction:
            raise WalletError("wallet writes need an open transaction (BEGIN first)")
        self.conn.execute(f"SAVEPOINT {self.name}")

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self.conn.execute(f"ROLLBACK TO {self.name}")
        self.conn.execute(f"RELEASE {self.name}")
        return False


# ---- wallet state ---------------------------------------------------------------

@dataclass(frozen=True)
class Wallet:
    wallet_id: str
    account_id: str
    policy: str
    starting_cash: Decimal
    cash: Decimal
    reserved: Decimal
    version: int

    @property
    def available(self) -> Decimal:
        # Exact: Decimal subtraction would round under the caller's context.
        return fees.fraction_to_decimal(Fraction(self.cash) - Fraction(self.reserved))


def create_wallet(conn, *, wallet_id: str, account_id: str, policy: str,
                  starting_cash: Any) -> Wallet:
    """A new isolated paper wallet. There is deliberately no reset or top-up."""
    _id(wallet_id, "wallet_id")
    if policy not in fctx.POLICIES:
        raise WalletError(f"policy must be one of {fctx.POLICIES}")
    if fctx.get_account(conn, kalshi_env="paper", account_id=account_id) is None:
        raise WalletError(f"paper account {account_id!r} is not registered")
    start = _frac(starting_cash, "starting_cash")
    if start <= 0:
        raise WalletError("starting_cash must be positive")
    with _Savepoint(conn, "rw_create"):
        try:
            conn.execute("INSERT INTO rw_wallets VALUES (?, 'paper', ?, ?, ?, ?, ?, 0)",
                         (wallet_id, account_id, policy, _dec(start), _dec(start),
                          datetime.now(timezone.utc).isoformat()))
        except sqlite3.IntegrityError as e:
            raise WalletError(f"wallet {wallet_id!r} already exists") from e
    return get_wallet(conn, wallet_id)


def get_wallet(conn, wallet_id: str) -> Wallet:
    r = conn.execute("SELECT account_id, policy, starting_cash, cash, version FROM rw_wallets "
                     "WHERE wallet_id=?", (wallet_id,)).fetchone()
    if r is None:
        raise WalletError(f"wallet {wallet_id!r} not found")
    reserved = sum((_frac(x[0], "reservation") for x in conn.execute(
        "SELECT amount FROM rw_reservations WHERE wallet_id=?", (wallet_id,))), Fraction(0))
    return Wallet(wallet_id, r[0], r[1], fees.to_decimal(r[2]), fees.to_decimal(r[3]),
                  fees.fraction_to_decimal(reserved), int(r[4]))


def get_position(conn, wallet_id: str, ticker: str, side: str) -> tuple[Decimal, Decimal]:
    r = conn.execute("SELECT quantity, cost_basis FROM rw_positions WHERE wallet_id=? AND "
                     "ticker=? AND side=?", (wallet_id, ticker, side)).fetchone()
    return (Decimal(0), Decimal(0)) if r is None else (fees.to_decimal(r[0]), fees.to_decimal(r[1]))


def _set_cash(conn, w: Wallet, new_cash: Fraction) -> None:
    upd = conn.execute("UPDATE rw_wallets SET cash=?, version=version+1 WHERE wallet_id=? AND "
                       "version=?", (_dec(new_cash), w.wallet_id, w.version))
    if upd.rowcount != 1:
        raise WalletError(f"wallet {w.wallet_id} changed concurrently")


def _event(conn, wallet_id, kind, ref_id, occurred_at, evidence, *, ticker=None, side=None,
           quantity=None, cash_delta=Fraction(0), reserved_delta=Fraction(0)) -> None:
    try:
        conn.execute("INSERT INTO rw_events (wallet_id, kind, ref_id, ticker, side, quantity, "
                     "cash_delta, reserved_delta, occurred_at, evidence) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (wallet_id, kind, ref_id, ticker, side,
                      None if quantity is None else _dec(quantity), _dec(cash_delta),
                      _dec(reserved_delta), _iso(occurred_at), _json(evidence)))
    except sqlite3.IntegrityError as e:
        raise WalletError(f"duplicate {kind} {ref_id!r} for wallet {wallet_id}") from e


# ---- reservations -------------------------------------------------------------

def reserve(conn, *, wallet_id: str, order_id: str, amount: Any, at: datetime,
            evidence=("reservation",)) -> Wallet:
    """Hold cash for an order before it is submitted. Refused if it would
    exceed available cash (no double-spending across simultaneous orders)."""
    _id(order_id, "order_id")
    amt = _frac(amount, "amount")
    if amt <= 0:
        raise WalletError("reservation must be positive")
    with _Savepoint(conn, "rw_reserve"):
        w = get_wallet(conn, wallet_id)
        if Fraction(w.cash) - Fraction(w.reserved) < amt:
            raise InsufficientFunds(f"available {w.available} < reservation {_dec(amt)}")
        try:
            conn.execute("INSERT INTO rw_reservations VALUES (?,?,?)", (wallet_id, order_id, _dec(amt)))
        except sqlite3.IntegrityError as e:
            raise WalletError(f"order {order_id} already has a reservation") from e
        _event(conn, wallet_id, "reserve", order_id, at, evidence, reserved_delta=amt)
        _set_cash(conn, w, Fraction(w.cash))   # version bump: reservations race on version
    return get_wallet(conn, wallet_id)


def release(conn, *, wallet_id: str, order_id: str, at: datetime,
            evidence=("release",)) -> Wallet:
    """Drop whatever remains of an order's reservation (e.g. IOC remainder)."""
    with _Savepoint(conn, "rw_release"):
        w = get_wallet(conn, wallet_id)
        r = conn.execute("SELECT amount FROM rw_reservations WHERE wallet_id=? AND order_id=?",
                         (wallet_id, order_id)).fetchone()
        if r is None:
            raise WalletError(f"order {order_id} has no reservation")
        conn.execute("DELETE FROM rw_reservations WHERE wallet_id=? AND order_id=?",
                     (wallet_id, order_id))
        _event(conn, wallet_id, "release", order_id, at, evidence,
               reserved_delta=-_frac(r[0], "reservation"))
        _set_cash(conn, w, Fraction(w.cash))
    return get_wallet(conn, wallet_id)


# ---- fills ---------------------------------------------------------------------

@dataclass(frozen=True)
class FillEvent:
    wallet_id: str
    order_id: str
    fill_id: str
    executed_at: datetime
    ticker: str
    side: str
    action: str
    role: str
    price: Any
    count: Any
    fee_context: fctx.FeeContext
    evidence: tuple[str, ...]


@dataclass(frozen=True)
class FillResult:
    wallet: Wallet
    fee: fees.FillFeeResult
    cash_delta: Decimal


def apply_fill(conn, ev: FillEvent) -> FillResult:
    """Account one simulated fill atomically (fee ledger + wallet)."""
    count = _count(ev.count)
    ctx = ev.fee_context
    if not isinstance(ctx, fctx.FeeContext) or not ctx.usable or ctx.resolution is None:
        raise WalletError(f"fee context not usable: {getattr(ctx, 'reason', ctx)!r}")
    with _Savepoint(conn, "rw_fill"):
        w = get_wallet(conn, ev.wallet_id)
        ctx.require_policy(w.policy)
        if ctx.kalshi_env != "paper" or ctx.account_id != w.account_id:
            raise WalletError("fee context is for a different account")
        if ev.executed_at.astimezone(timezone.utc) > ctx.valid_until:
            raise WalletError("fee context expired before the fill")
        try:
            led = fee_ledger.apply_model_fill(
                conn, kalshi_env="paper", account_id=w.account_id, order_id=ev.order_id,
                fill_id=ev.fill_id, precision=ctx.precision, precision_source=ctx.precision_source,
                ticker=ev.ticker, side=ev.side, action=ev.action, role=ev.role, price=ev.price,
                count=_dec(count), filled_at=ev.executed_at, resolution=ctx.resolution)
        except fee_ledger.FeeLedgerError as e:
            raise WalletError(f"fee ledger refused the fill: {e}") from e
        if led.fee is None:
            raise WalletError(f"fee unknown for fill ({led.resolution_status}); not accounted")
        delta = Fraction(led.fee.balance_delta)
        cash = Fraction(w.cash) + delta
        qty, basis = (Fraction(x) for x in get_position(conn, w.wallet_id, ev.ticker, ev.side))
        res_row = conn.execute("SELECT amount FROM rw_reservations WHERE wallet_id=? AND order_id=?",
                               (w.wallet_id, ev.order_id)).fetchone()
        consumed = Fraction(0)
        if ev.action == "buy":
            if res_row is not None:
                consumed = min(_frac(res_row[0], "reservation"), -delta)
            qty, basis = qty + count, basis - delta
        else:
            if qty < count:
                raise WalletError(f"sell {_dec(count)} exceeds held {_dec(qty)}")
            # Remaining basis, floored to a microdollar so it stays a finite
            # decimal (informational; cash is the accounting authority).
            basis = Fraction(math.floor(basis * (qty - count) / qty * 10**6), 10**6)
            qty = qty - count
        remaining_res = Fraction(w.reserved) - consumed
        if cash < 0 or cash < remaining_res:
            raise InsufficientFunds(f"fill needs {_dec(-delta)}, cash {w.cash}, "
                                    f"other reservations {_dec(remaining_res)}")
        if res_row is not None and consumed:
            left = _frac(res_row[0], "reservation") - consumed
            if left:
                conn.execute("UPDATE rw_reservations SET amount=? WHERE wallet_id=? AND order_id=?",
                             (_dec(left), w.wallet_id, ev.order_id))
            else:
                conn.execute("DELETE FROM rw_reservations WHERE wallet_id=? AND order_id=?",
                             (w.wallet_id, ev.order_id))
        if qty:
            conn.execute("INSERT INTO rw_positions VALUES (?,?,?,?,?) ON CONFLICT (wallet_id, "
                         "ticker, side) DO UPDATE SET quantity=excluded.quantity, "
                         "cost_basis=excluded.cost_basis",
                         (w.wallet_id, ev.ticker, ev.side, _dec(qty), _dec(basis)))
        else:
            conn.execute("DELETE FROM rw_positions WHERE wallet_id=? AND ticker=? AND side=?",
                         (w.wallet_id, ev.ticker, ev.side))
        _event(conn, w.wallet_id, "fill", ev.fill_id, ev.executed_at,
               tuple(ev.evidence) + ctx.evidence, ticker=ev.ticker, side=ev.side,
               quantity=count if ev.action == "buy" else -count, cash_delta=delta,
               reserved_delta=-consumed)
        _set_cash(conn, w, cash)
        for ref in set(ctx.evidence):
            fee_ledger.add_evidence_ref(conn, evidence_ref=ref, kalshi_env="paper",
                                        referrer_kind="fill", referrer_id=ev.fill_id,
                                        account_id=w.account_id)
    return FillResult(get_wallet(conn, ev.wallet_id), led.fee, fees.fraction_to_decimal(delta))


def settle(conn, *, wallet_id: str, ticker: str, result: str, settled_at: datetime,
           evidence: tuple[str, ...]) -> Decimal:
    """Pay out an official settlement: $1 per winning contract, $0 otherwise.
    Settlement carries no trading fee. Returns the cash credited."""
    if result not in ("yes", "no"):
        raise WalletError("result must be 'yes' or 'no' (unresolved/voided markets are not settled here)")
    with _Savepoint(conn, "rw_settle"):
        w = get_wallet(conn, wallet_id)
        payout = Fraction(0)
        for side in ("yes", "no"):
            qty, _ = get_position(conn, wallet_id, ticker, side)
            if not qty:
                continue
            pay = Fraction(qty) if side == result else Fraction(0)
            payout += pay
            _event(conn, wallet_id, "settle", f"{ticker}:{side}", settled_at, evidence,
                   ticker=ticker, side=side, quantity=-Fraction(qty), cash_delta=pay)
            conn.execute("DELETE FROM rw_positions WHERE wallet_id=? AND ticker=? AND side=?",
                         (wallet_id, ticker, side))
        _set_cash(conn, w, Fraction(w.cash) + payout)
    return fees.fraction_to_decimal(payout)


def reconcile(conn, wallet_id: str) -> dict:
    """Recompute cash and reservations from the event log; compare to state."""
    w = get_wallet(conn, wallet_id)
    cash = Fraction(w.starting_cash)
    reserved = Fraction(0)
    for cd, rd in conn.execute("SELECT cash_delta, reserved_delta FROM rw_events WHERE "
                               "wallet_id=? ORDER BY id", (wallet_id,)):
        cash += _frac(cd, "cash_delta")
        reserved += _frac(rd, "reserved_delta")
    return {"cash_from_events": fees.fraction_to_decimal(cash), "cash": w.cash,
            "reserved_from_events": fees.fraction_to_decimal(reserved), "reserved": w.reserved,
            "ok": cash == Fraction(w.cash) and reserved == Fraction(w.reserved)}
