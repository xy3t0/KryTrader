"""Final admission gate for research orders (plan step B1, experiment scope).

Immediately before an order would be submitted, recompute the NET-DOLLAR
edge at the actual limit price and final quantity, under a conservative fee
bound, and reserve the cash it could spend. Anything that changes price,
quantity, side or role needs a new admission.

Conservative fee bound (no rebate recovery assumed). An IOC buy of C
contracts at limit L can fill in fragments at prices <= L. For the buyer,
expected value per contract  prob - p - fee(p)  is worst at p = L (the fee
coefficient is far below 1), so the bound evaluates price L. Fragmentation:
fills arrive in multiples of 0.01 contracts, so at most F = C / 0.01 fills;
each adds at most one microdollar of trade-fee rounding and strictly less
than one balance-precision unit of rounding fee. Hence
    fee_bound(C, L) = ceil_micro(k * C * L * (1 - L)) + F * (precision + 1e-6)
This is a bound under those stated execution constraints, not a guess at a
typical fill; it is recorded with every decision.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from fractions import Fraction
from typing import Any, Optional

import fee_context as fctx
import fees
import research_wallet as rw

COUNT_QUANTUM = Fraction(1, 100)
MICRO = Fraction(1, 10**6)
GATE_VERSION = "gate-ioc-taker-v1"


class GateError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExecutionIntent:
    wallet_id: str
    order_id: str
    ticker: str
    side: str
    action: str            # only "buy" in the experiment
    order_type: str        # only "ioc"
    role: str              # "taker" for IOC
    limit: Any             # dollars
    count: Any
    side_probability: Any  # model probability that `side` wins (float allowed; converted exactly)
    decided_at: datetime
    fee_context: fctx.FeeContext


@dataclass(frozen=True)
class Admission:
    admitted: bool
    reason: str
    fee_bound: Optional[Decimal] = None
    net_edge_per_contract: Optional[Decimal] = None
    reserve_amount: Optional[Decimal] = None
    gate_version: str = GATE_VERSION


def fee_bound(*, count: Fraction, limit: Fraction, coefficient: Fraction,
              precision: Fraction) -> Fraction:
    if count <= 0 or (count / COUNT_QUANTUM).denominator != 1:
        raise GateError("count must be a positive multiple of 0.01")
    model = coefficient * count * limit * (1 - limit)
    trade = Fraction(-((-model) // MICRO)) * MICRO          # ceil to microdollar
    fragments = count / COUNT_QUANTUM
    return trade + fragments * (precision + MICRO)


def _prob(v: Any) -> Fraction:
    if isinstance(v, float):
        if not (0.0 <= v <= 1.0):
            raise GateError("probability out of range")
        return Fraction(v)          # exact binary value of the model's float
    p = fees.to_fraction(v, "side_probability")
    if not (0 <= p <= 1):
        raise GateError("probability out of range")
    return p


def admit(conn, intent: ExecutionIntent, *, min_net_edge_per_contract: Any,
          max_limit: Any, policy: str) -> Admission:
    """Decide and, if admitted, reserve price*count + fee_bound in the wallet
    (inside the caller's transaction). Rejections never reserve anything."""
    def no(reason: str, **kw) -> Admission:
        return Admission(False, reason, **kw)

    if intent.action != "buy" or intent.order_type != "ioc" or intent.role != "taker":
        return no("unsupported_order_shape")
    ctx = intent.fee_context
    if not isinstance(ctx, fctx.FeeContext) or not ctx.usable or ctx.resolution is None:
        return no(f"fee_context_unusable:{getattr(ctx, 'reason', '?')}")
    if ctx.policy != policy:
        return no("fee_context_policy_mismatch")
    res = ctx.resolution
    if res.market_ticker != intent.ticker or res.role != intent.role:
        return no("fee_context_identity_mismatch")
    decided = intent.decided_at.astimezone(timezone.utc)
    if res.at != decided or ctx.valid_until is None or decided > ctx.valid_until:
        return no("fee_context_not_for_this_instant")
    try:
        lim = fees.to_fraction(intent.limit, "limit")
        cnt = fees.to_fraction(intent.count, "count")
        prob = _prob(intent.side_probability)
        min_edge = fees.to_fraction(min_net_edge_per_contract, "min_net_edge")
        cap = fees.to_fraction(max_limit, "max_limit")
    except (fees.FeeInputError, GateError) as e:
        return no(f"bad_input:{e}")
    if not (0 < lim < 1):
        return no("limit_out_of_range")
    if lim > cap:
        return no("limit_above_cap")
    try:
        bound = fee_bound(count=cnt, limit=lim, coefficient=Fraction(res.coefficient),
                          precision=Fraction(ctx.precision))
    except GateError as e:
        return no(f"bad_input:{e}")
    net_per = prob - lim - bound / cnt
    d = fees.fraction_to_decimal
    common = dict(fee_bound=d(bound), net_edge_per_contract=_dec_floor(net_per))
    if net_per < min_edge:
        return no("net_edge_below_minimum", **common)
    reserve = lim * cnt + bound
    try:
        rw.reserve(conn, wallet_id=intent.wallet_id, order_id=intent.order_id,
                   amount=d(reserve), at=intent.decided_at, evidence=ctx.evidence)
    except rw.InsufficientFunds:
        return no("insufficient_funds", **common)
    except rw.WalletError as e:
        return no(f"wallet_refused:{e}", **common)
    return Admission(True, "admitted", reserve_amount=d(reserve), **common)


def _dec_floor(f: Fraction) -> Decimal:
    """Exact when f is a decimal fraction; otherwise floored to 1e-12 (the
    model probability is a binary float, so its value need not be decimal)."""
    try:
        return fees.fraction_to_decimal(f)
    except Exception:
        q = Fraction(1, 10**12)
        return fees.fraction_to_decimal((f // q) * q)
