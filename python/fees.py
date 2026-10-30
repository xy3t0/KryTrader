"""Kalshi fee accounting core: exact arithmetic, independent of Decimal context.

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

Exactness: every input is a finite decimal within the bounds below. Inputs
are converted to fractions.Fraction (exact), all arithmetic and rounding is
done on Fractions and integers, and results are converted back to Decimal
via the exact tuple constructor. No Decimal arithmetic is performed, so the
caller's decimal context (precision, rounding, traps) cannot affect results.

Supported inputs (anything else raises FeeInputError):
  * every amount: at most MAX_SCALE digits after the decimal point and
    absolute value below 10**MAX_INTEGER_DIGITS
  * quadratic_model_fee / fill_revenue arguments: at most
    MAX_PRICE_COUNT_SCALE (price, count) or MAX_COEFFICIENT_SCALE
    (coefficient) decimal places, so their exact products stay within
    MAX_SCALE
  * precision: a positive multiple of $0.000001

Scope notes:
  * Model fees here are MODELED. Exchange-reported fees remain authoritative
    for live reconciliation.
  * quadratic_model_fee takes an explicit coefficient. Which schedule applies
    to a market (fee type, multiplier, event overrides, maker vs taker,
    effective time) is resolved elsewhere; an unknown schedule must never
    silently fall back to this formula.
  * Admission checks (should we place this order?) are a separate interface
    with their own stated assumptions; OrderFeeState is for accounting fills
    that actually happened.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from fractions import Fraction
from typing import Iterable, Union

DecimalLike = Union[Decimal, int, str]

MAX_SCALE = 40
MAX_INTEGER_DIGITS = 12
MAX_PRICE_COUNT_SCALE = 6
MAX_COEFFICIENT_SCALE = 10

MICRODOLLAR = Decimal("0.000001")
DIRECT_MEMBER_PRECISION = Decimal("0.0001")
NON_DIRECT_MEMBER_PRECISION = Decimal("0.01")

_F_ZERO = Fraction(0)
_F_ONE = Fraction(1)
_F_MICRO = Fraction(1, 10**6)
_F_LIMIT = Fraction(10**MAX_INTEGER_DIGITS)


class FeeInputError(ValueError):
    """An input to the fee core is malformed or out of range."""


class AccountingInvariantError(RuntimeError):
    """A computed result violated a conservation invariant (a bug)."""


class DuplicateFillError(ValueError):
    """A fill id was applied to an order's fee state more than once."""


# ---- exact conversion -----------------------------------------------------

def to_decimal(value: DecimalLike, name: str = "value", *,
               max_scale: int = MAX_SCALE) -> Decimal:
    """Convert to a finite, bounded Decimal without any context arithmetic.
    Floats are rejected: a binary float has already lost the exact value.
    Callers at float boundaries must convert explicitly (Decimal(str(x)))."""
    if isinstance(value, bool):
        raise FeeInputError(f"{name}: bool is not a number")
    if isinstance(value, float):
        raise FeeInputError(
            f"{name}: float {value!r} rejected; pass Decimal, int or a decimal string"
        )
    if isinstance(value, Decimal):
        d = value
    elif isinstance(value, int):
        d = Decimal(value)  # exact for ints
    elif isinstance(value, str):
        try:
            d = Decimal(value)  # string construction is exact, context-free
        except Exception as e:  # decimal.InvalidOperation
            raise FeeInputError(f"{name}: not a decimal: {value!r}") from e
    else:
        raise FeeInputError(f"{name}: unsupported type {type(value).__name__}")
    if not d.is_finite():
        raise FeeInputError(f"{name}: must be finite, got {value!r}")
    sign, digits, exponent = d.as_tuple()
    if not any(digits):
        return Decimal(0)  # any spelling of zero (0E-999999, -0.00) is exactly 0
    scale = -exponent if exponent < 0 else 0
    # Trailing zeros past the decimal point carry no value; don't count them.
    trailing = 0
    for dg in reversed(digits):
        if dg != 0 or trailing >= scale:
            break
        trailing += 1
    if scale - trailing > max_scale:
        raise FeeInputError(
            f"{name}: more than {max_scale} decimal places ({value!r})"
        )
    if digits != (0,) and len(digits) + exponent > MAX_INTEGER_DIGITS:
        raise FeeInputError(
            f"{name}: magnitude >= 10**{MAX_INTEGER_DIGITS} ({value!r})"
        )
    return d


def _to_frac(value: DecimalLike, name: str, *, max_scale: int = MAX_SCALE) -> Fraction:
    return Fraction(to_decimal(value, name, max_scale=max_scale))


def _frac_to_decimal(f: Fraction) -> Decimal:
    """Exact Fraction -> Decimal. Raises if f is not a finite decimal fraction."""
    den = f.denominator
    twos = fives = 0
    while den % 2 == 0:
        den //= 2
        twos += 1
    while den % 5 == 0:
        den //= 5
        fives += 1
    if den != 1:
        raise AccountingInvariantError(f"non-decimal intermediate {f}")
    k = max(twos, fives)
    scaled = f.numerator * 10**k // f.denominator  # exact: divisible by construction
    sign = 1 if scaled < 0 else 0
    digits = tuple(int(c) for c in str(abs(scaled)))
    return Decimal((sign, digits, -k))


def _ceil_to(x: Fraction, q: Fraction) -> Fraction:
    return math.ceil(x / q) * q


def _floor_to(x: Fraction, q: Fraction) -> Fraction:
    return math.floor(x / q) * q  # math.floor rounds toward -infinity


def _check_bound(x: Fraction, name: str) -> None:
    if abs(x) >= _F_LIMIT:
        raise FeeInputError(f"{name}: result magnitude >= 10**{MAX_INTEGER_DIGITS}")


def ceil_to(x: DecimalLike, quantum: DecimalLike) -> Decimal:
    """Round x up (toward +infinity) to a multiple of quantum, exactly."""
    q = _to_frac(quantum, "quantum")
    if q <= 0:
        raise FeeInputError("quantum must be positive")
    return _frac_to_decimal(_ceil_to(_to_frac(x, "x"), q))


def floor_to(x: DecimalLike, quantum: DecimalLike) -> Decimal:
    """Round x down (toward -infinity, NOT toward zero) to a multiple of quantum, exactly."""
    q = _to_frac(quantum, "quantum")
    if q <= 0:
        raise FeeInputError("quantum must be positive")
    return _frac_to_decimal(_floor_to(_to_frac(x, "x"), q))


def _precision_frac(precision: DecimalLike) -> Fraction:
    p = _to_frac(precision, "precision")
    if p <= 0:
        raise FeeInputError(f"precision must be positive, got {precision!r}")
    if (p / _F_MICRO).denominator != 1:
        raise FeeInputError(f"precision must be a multiple of $0.000001, got {precision!r}")
    return p


def validate_precision(precision: DecimalLike) -> Decimal:
    return _frac_to_decimal(_precision_frac(precision))


# ---- model fee and revenue ------------------------------------------------

def _price_count(price: DecimalLike, count: DecimalLike) -> tuple[Fraction, Fraction]:
    p = _to_frac(price, "price", max_scale=MAX_PRICE_COUNT_SCALE)
    c = _to_frac(count, "count", max_scale=MAX_PRICE_COUNT_SCALE)
    if not (_F_ZERO < p < _F_ONE):
        raise FeeInputError(f"price must be in (0, 1), got {price!r}")
    if c <= 0:
        raise FeeInputError(f"count must be positive, got {count!r}")
    return p, c


def quadratic_model_fee(
    price: DecimalLike, count: DecimalLike, coefficient: DecimalLike,
) -> Decimal:
    """Unrounded quadratic model fee in dollars: coefficient * C * p * (1 - p).

    price is in dollars per contract, strictly between 0 and 1. count may be
    fractional (whether a given market accepts fractional counts is a
    separate, venue-side question). coefficient is required; there is
    deliberately no default schedule."""
    p, c = _price_count(price, count)
    k = _to_frac(coefficient, "coefficient", max_scale=MAX_COEFFICIENT_SCALE)
    if k < 0:
        raise FeeInputError(f"coefficient must be non-negative, got {coefficient!r}")
    fee = k * c * p * (_F_ONE - p)
    _check_bound(fee, "model fee")
    return _frac_to_decimal(fee)


def fill_revenue(price: DecimalLike, count: DecimalLike, action: str) -> Decimal:
    """Signed revenue of a fill before fees: negative for buys, positive for sells."""
    p, c = _price_count(price, count)
    a = action.lower() if isinstance(action, str) else ""
    if a == "buy":
        return _frac_to_decimal(-(p * c))
    if a == "sell":
        return _frac_to_decimal(p * c)
    raise FeeInputError(f"action must be 'buy' or 'sell', got {action!r}")


# ---- per-fill settlement --------------------------------------------------

@dataclass(frozen=True)
class FillFeeResult:
    trade_fee: Decimal
    rounding_fee: Decimal
    rebate: Decimal
    net_fee: Decimal
    balance_delta: Decimal
    accumulator: Decimal


def _settle(rev: Fraction, mf: Fraction, prec: Fraction, old: Fraction) -> FillFeeResult:
    if mf < 0:
        raise FeeInputError(f"model_fee must be non-negative")
    if old < 0:
        raise FeeInputError(f"accumulator must be non-negative")

    t = _ceil_to(mf, _F_MICRO)
    a = _floor_to(rev - t, prec)
    r = rev - t - a
    acc = old + r
    rebate = prec * min(math.floor(acc / prec), math.floor((t + r) / prec))
    new_acc = acc - rebate
    net_fee = t + r - rebate
    delta = a + rebate

    # Exact checks on Fractions: no rounding can mask a violation.
    if delta != rev - net_fee:
        raise AccountingInvariantError("balance_delta != revenue - net_fee")
    if net_fee < 0:
        raise AccountingInvariantError("net_fee < 0")
    if new_acc < 0:
        raise AccountingInvariantError("accumulator < 0")
    if new_acc != old + r - rebate:
        raise AccountingInvariantError("accumulator not conserved")
    if r < 0 or r >= prec:
        raise AccountingInvariantError("rounding_fee outside [0, precision)")
    for v, n in ((delta, "balance_delta"), (new_acc, "accumulator")):
        _check_bound(v, n)

    return FillFeeResult(
        trade_fee=_frac_to_decimal(t),
        rounding_fee=_frac_to_decimal(r),
        rebate=_frac_to_decimal(rebate),
        net_fee=_frac_to_decimal(net_fee),
        balance_delta=_frac_to_decimal(delta),
        accumulator=_frac_to_decimal(new_acc),
    )


def settle_fill(
    revenue: DecimalLike,
    model_fee: DecimalLike,
    precision: DecimalLike,
    accumulator: DecimalLike = 0,
) -> FillFeeResult:
    """Account one fill. `accumulator` is the order's accumulator BEFORE this
    fill; the result carries the new value. Pure function."""
    return _settle(
        _to_frac(revenue, "revenue"),
        _to_frac(model_fee, "model_fee"),
        _precision_frac(precision),
        _to_frac(accumulator, "accumulator"),
    )


# ---- per-order state ------------------------------------------------------

def _require_id(value, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise FeeInputError(f"{name} must be a nonempty string, got {value!r}")
    return value


def _fill_id_set(ids, name: str = "applied_fill_ids") -> set[str]:
    if isinstance(ids, (str, bytes)) or not isinstance(ids, Iterable):
        raise FeeInputError(f"{name} must be a collection of fill ids, got {ids!r}")
    out: set[str] = set()
    for i in ids:
        out.add(_require_id(i, "fill id"))
    return out


@dataclass
class OrderFeeState:
    """Per-order fee accumulator across fills (taker and maker alike).

    Precision is fixed for the life of the order. Fill ids are tracked so a
    re-delivered fill cannot be double-counted. The constructor accepts any
    collection of string fill ids; from_dict is the strict deserializer.
    Serialized values are decimal strings: persisting the accumulator through
    a float/REAL column would defeat the purpose."""
    order_id: str
    precision: Decimal
    accumulator: Decimal = Decimal(0)
    applied_fill_ids: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        _require_id(self.order_id, "order_id")
        self.precision = validate_precision(self.precision)
        acc = _to_frac(self.accumulator, "accumulator")
        if acc < 0:
            raise FeeInputError("accumulator must be non-negative")
        self.accumulator = _frac_to_decimal(acc)
        self.applied_fill_ids = _fill_id_set(self.applied_fill_ids)

    def apply_fill(
        self, fill_id: str, revenue: DecimalLike, model_fee: DecimalLike,
    ) -> FillFeeResult:
        _require_id(fill_id, "fill_id")
        if fill_id in self.applied_fill_ids:
            raise DuplicateFillError(f"fill {fill_id!r} already applied to {self.order_id!r}")
        # Compute fully before mutating: a rejected fill leaves state unchanged.
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
        """Strict deserializer: every field required and exactly typed."""
        if not isinstance(d, dict):
            raise FeeInputError("state must be a dict")
        missing = {"order_id", "precision", "accumulator", "applied_fill_ids"} - d.keys()
        if missing:
            raise FeeInputError(f"state missing fields: {sorted(missing)}")
        for k in ("precision", "accumulator"):
            if not isinstance(d[k], str):
                raise FeeInputError(f"{k} must be serialized as a decimal string")
        ids = d["applied_fill_ids"]
        if not isinstance(ids, list):
            raise FeeInputError("applied_fill_ids must be a list of fill id strings")
        id_set = _fill_id_set(ids)
        if len(id_set) != len(ids):
            raise FeeInputError("applied_fill_ids contains duplicates")
        return cls(
            order_id=_require_id(d["order_id"], "order_id"),
            precision=to_decimal(d["precision"], "precision"),
            accumulator=to_decimal(d["accumulator"], "accumulator"),
            applied_fill_ids=id_set,
        )
