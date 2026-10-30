"""Kalshi fee accounting core: exact Decimal arithmetic, no floats.

This module is the intended single source of truth for fee math. As of this
commit it is NOT wired into any engine; existing call sites
(backtest.kalshi_fee_per_contract, trader._taker_fee_cents,
crypto15m._fee_cents, paper_exchange._fee_usd, crypto paper fees, terminal/MCP
estimates) are migrated in a later commit.

Implements the documented fee-rounding rules
(https://docs.kalshi.com/getting_started/fee_rounding):

  * trade fee   = model fee rounded UP to $0.000001
  * the fill's revenue minus the trade fee is FLOORED to the account's
    balance precision ($0.0001 direct members, $0.01 non-direct); the
    leftover is the rounding fee, added to a per-order accumulator
  * accumulated rounding overpayment is rebated in precision-sized
    increments; a rebate may not make the current fill's net fee negative
  * the accumulator is per order and carries across all its fills, taker
    or maker

Per fill, with revenue negative for buys and positive for sells:

    t  = ceil_to(model_fee, $0.000001)
    a  = floor_to(revenue - t, precision)
    r  = revenue - t - a
    A  = old_accumulator + r
    rebate = precision * min(floor(A / precision), floor((t + r) / precision))
    new_accumulator = A - rebate
    net_fee         = t + r - rebate
    balance_delta   = a + rebate

The rebate is CAPPED, never computed in excess and then clamped, so money is
conserved: balance_delta == revenue - net_fee on every fill.

Scope notes:
  * Model fees here are MODELED. Exchange-reported fees remain authoritative
    for live reconciliation.
  * quadratic_model_fee takes an explicit coefficient. Which schedule applies
    to a market (fee type, multiplier, event overrides, maker vs taker,
    effective time) is resolved elsewhere; an unknown schedule must never
    silently fall back to this formula.
  * Admission checks (should we place this order?) must use a conservative
    estimate that assumes NO future rebate recovery. OrderFeeState is for
    accounting fills that actually happened.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Union

DecimalLike = Union[Decimal, int, str]

MICRODOLLAR = Decimal("0.000001")
DIRECT_MEMBER_PRECISION = Decimal("0.0001")
NON_DIRECT_MEMBER_PRECISION = Decimal("0.01")

_ZERO = Decimal(0)
_ONE = Decimal(1)


class FeeInputError(ValueError):
    """An input to the fee core is malformed or out of range."""


class AccountingInvariantError(RuntimeError):
    """A computed result violated a conservation invariant (a bug)."""


class DuplicateFillError(ValueError):
    """A fill id was applied to an order's fee state more than once."""


def to_decimal(value: DecimalLike, name: str = "value") -> Decimal:
    """Convert to a finite Decimal. Floats are rejected: a binary float has
    already lost the exact value, and converting it here would hide that.
    Callers at float boundaries must convert explicitly (Decimal(str(x)))."""
    if isinstance(value, bool):
        raise FeeInputError(f"{name}: bool is not a number")
    if isinstance(value, float):
        raise FeeInputError(
            f"{name}: float {value!r} rejected; pass Decimal, int or a decimal string"
        )
    if isinstance(value, Decimal):
        d = value
    elif isinstance(value, (int, str)):
        try:
            d = Decimal(value)
        except Exception as e:  # decimal.InvalidOperation
            raise FeeInputError(f"{name}: not a decimal: {value!r}") from e
    else:
        raise FeeInputError(f"{name}: unsupported type {type(value).__name__}")
    if not d.is_finite():
        raise FeeInputError(f"{name}: must be finite, got {value!r}")
    return d


def ceil_to(x: Decimal, quantum: Decimal) -> Decimal:
    """Round x up (toward +infinity) to a multiple of quantum."""
    return (x / quantum).to_integral_value(rounding=ROUND_CEILING) * quantum


def floor_to(x: Decimal, quantum: Decimal) -> Decimal:
    """Round x down (toward -infinity, NOT toward zero) to a multiple of quantum."""
    return (x / quantum).to_integral_value(rounding=ROUND_FLOOR) * quantum


def _floor_div(x: Decimal, quantum: Decimal) -> Decimal:
    return (x / quantum).to_integral_value(rounding=ROUND_FLOOR)


def validate_precision(precision: DecimalLike) -> Decimal:
    p = to_decimal(precision, "precision")
    if p <= _ZERO:
        raise FeeInputError(f"precision must be positive, got {p}")
    if (p / MICRODOLLAR) != (p / MICRODOLLAR).to_integral_value():
        raise FeeInputError(f"precision must be a multiple of $0.000001, got {p}")
    return p


def quadratic_model_fee(
    price: DecimalLike, count: DecimalLike, coefficient: DecimalLike,
) -> Decimal:
    """Unrounded quadratic model fee in dollars: coefficient * C * p * (1 - p).

    price is in dollars per contract, strictly between 0 and 1. count may be
    fractional (whether a given market accepts fractional counts is a
    separate, venue-side question). coefficient is required; there is
    deliberately no default schedule."""
    p = to_decimal(price, "price")
    c = to_decimal(count, "count")
    k = to_decimal(coefficient, "coefficient")
    if not (_ZERO < p < _ONE):
        raise FeeInputError(f"price must be in (0, 1), got {p}")
    if c <= _ZERO:
        raise FeeInputError(f"count must be positive, got {c}")
    if k < _ZERO:
        raise FeeInputError(f"coefficient must be non-negative, got {k}")
    return k * c * p * (_ONE - p)


def fill_revenue(price: DecimalLike, count: DecimalLike, action: str) -> Decimal:
    """Signed revenue of a fill before fees: negative for buys, positive for sells."""
    p = to_decimal(price, "price")
    c = to_decimal(count, "count")
    if not (_ZERO < p < _ONE):
        raise FeeInputError(f"price must be in (0, 1), got {p}")
    if c <= _ZERO:
        raise FeeInputError(f"count must be positive, got {c}")
    a = (action or "").lower()
    if a == "buy":
        return -(p * c)
    if a == "sell":
        return p * c
    raise FeeInputError(f"action must be 'buy' or 'sell', got {action!r}")


@dataclass(frozen=True)
class FillFeeResult:
    trade_fee: Decimal
    rounding_fee: Decimal
    rebate: Decimal
    net_fee: Decimal
    balance_delta: Decimal
    accumulator: Decimal


def settle_fill(
    revenue: DecimalLike,
    model_fee: DecimalLike,
    precision: DecimalLike,
    accumulator: DecimalLike = 0,
) -> FillFeeResult:
    """Account one fill. `accumulator` is the order's accumulator BEFORE this
    fill; the result carries the new value. Pure function."""
    rev = to_decimal(revenue, "revenue")
    mf = to_decimal(model_fee, "model_fee")
    prec = validate_precision(precision)
    old = to_decimal(accumulator, "accumulator")
    if mf < _ZERO:
        raise FeeInputError(f"model_fee must be non-negative, got {mf}")
    if old < _ZERO:
        raise FeeInputError(f"accumulator must be non-negative, got {old}")

    t = ceil_to(mf, MICRODOLLAR)
    a = floor_to(rev - t, prec)
    r = rev - t - a
    acc = old + r
    rebate = prec * min(_floor_div(acc, prec), _floor_div(t + r, prec))
    new_acc = acc - rebate
    net_fee = t + r - rebate
    delta = a + rebate

    if delta != rev - net_fee:
        raise AccountingInvariantError("balance_delta != revenue - net_fee")
    if net_fee < _ZERO:
        raise AccountingInvariantError("net_fee < 0")
    if new_acc < _ZERO:
        raise AccountingInvariantError("accumulator < 0")
    if new_acc != old + r - rebate:
        raise AccountingInvariantError("accumulator not conserved")
    if r < _ZERO or r >= prec:
        raise AccountingInvariantError("rounding_fee outside [0, precision)")

    return FillFeeResult(
        trade_fee=t, rounding_fee=r, rebate=rebate,
        net_fee=net_fee, balance_delta=delta, accumulator=new_acc,
    )


@dataclass
class OrderFeeState:
    """Per-order fee accumulator across fills (taker and maker alike).

    Precision is fixed for the life of the order. Fill ids are tracked so a
    re-delivered fill cannot be double-counted. Serialized values are decimal
    strings: persisting the accumulator through a float/REAL column would
    defeat the purpose."""
    order_id: str
    precision: Decimal
    accumulator: Decimal = _ZERO
    applied_fill_ids: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        if not self.order_id:
            raise FeeInputError("order_id is required")
        self.precision = validate_precision(self.precision)
        self.accumulator = to_decimal(self.accumulator, "accumulator")
        if self.accumulator < _ZERO:
            raise FeeInputError("accumulator must be non-negative")
        self.applied_fill_ids = set(self.applied_fill_ids)

    def apply_fill(
        self, fill_id: str, revenue: DecimalLike, model_fee: DecimalLike,
    ) -> FillFeeResult:
        if not fill_id:
            raise FeeInputError("fill_id is required")
        if fill_id in self.applied_fill_ids:
            raise DuplicateFillError(f"fill {fill_id!r} already applied to {self.order_id!r}")
        result = settle_fill(revenue, model_fee, self.precision, self.accumulator)
        self.accumulator = result.accumulator
        self.applied_fill_ids.add(fill_id)
        return result

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "precision": str(self.precision),
            "accumulator": str(self.accumulator),
            "applied_fill_ids": sorted(self.applied_fill_ids),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "OrderFeeState":
        if not isinstance(d, dict):
            raise FeeInputError("state must be a dict")
        for k in ("precision", "accumulator"):
            if not isinstance(d.get(k), str):
                raise FeeInputError(f"{k} must be serialized as a decimal string")
        return cls(
            order_id=d.get("order_id") or "",
            precision=Decimal(d["precision"]),
            accumulator=Decimal(d["accumulator"]),
            applied_fill_ids=set(d.get("applied_fill_ids") or []),
        )
