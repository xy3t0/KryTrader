"""Fee context: account identity, precision, fee resolution, evidence and
policy, combined into one object every money path consumes (plan step A1).

Account registry
  An account's ledger identity (kalshi_env, account_id) is local and
  persistent. It is never derived from an API key: credentials are bound to
  an account, and key rotation adds a new binding without changing the
  account. Bindings may record identity observed from an authenticated read
  (Kalshi order responses carry user_id / subaccount_number); a later
  observation that disagrees is refused.

Policies
  STRICT              accepts no unverified-history assumptions. Until some
                      coverage is VERIFIED, strict contexts are unavailable,
                      which is the intended fail-closed state.
  CONDITIONAL_PAPER   assumes no unreported fee changes across the required
                      snapshot/decision interval. The interval, policy id and
                      captures are recorded as ASSUMED coverage. It never
                      manufactures VERIFIED coverage or an announcement lead
                      time. Paper research only; never live release evidence.
  A context records its policy; cohorts must refuse contexts from another.
"""
from __future__ import annotations

import dataclasses
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

import fee_schedule as fs
import fees

ENVIRONMENTS = ("production", "paper")
PRECISION_SOURCES = ("configured", "api_verified")
BINDING_SOURCES = ("user_configured", "api_observed")

ACCOUNT_SCHEMA = """
CREATE TABLE IF NOT EXISTS fee_accounts (
    kalshi_env TEXT NOT NULL CHECK (kalshi_env IN ('production', 'paper')),
    account_id TEXT NOT NULL CHECK (length(account_id) > 0),
    subaccount INTEGER,                        -- NULL = primary / not applicable
    precision TEXT NOT NULL,                   -- decimal string
    precision_source TEXT NOT NULL CHECK (precision_source IN ('configured', 'api_verified')),
    created_at TEXT NOT NULL,
    PRIMARY KEY (kalshi_env, account_id)
);

-- Credentials bound to an account. Rotation adds a binding; the account
-- (and its ledger identity) is unchanged.
CREATE TABLE IF NOT EXISTS fee_account_bindings (
    kalshi_env TEXT NOT NULL,
    account_id TEXT NOT NULL,
    credential_fingerprint TEXT NOT NULL CHECK (length(credential_fingerprint) = 64),
    binding_source TEXT NOT NULL CHECK (binding_source IN ('user_configured', 'api_observed')),
    observed_user_id TEXT,
    observed_subaccount INTEGER,
    evidence_ref TEXT,
    bound_at TEXT NOT NULL,
    revoked_at TEXT,
    PRIMARY KEY (kalshi_env, account_id, credential_fingerprint),
    FOREIGN KEY (kalshi_env, account_id) REFERENCES fee_accounts (kalshi_env, account_id)
);
-- An active credential identifies exactly one account per environment.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fee_binding_active
    ON fee_account_bindings (kalshi_env, credential_fingerprint) WHERE revoked_at IS NULL;
"""

STRICT = "strict-v1"
CONDITIONAL_PAPER = "conditional-paper-v1"
POLICIES = (STRICT, CONDITIONAL_PAPER)
CONDITIONAL_ASSUMPTION = f"assume-no-unreported-fee-changes:{CONDITIONAL_PAPER}"
_ACCEPTED = {STRICT: frozenset(), CONDITIONAL_PAPER: frozenset({CONDITIONAL_ASSUMPTION})}

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_FP_RE = re.compile(r"^[0-9a-f]{64}$")


class FeeContextError(RuntimeError):
    """Account registry or context construction refused an input."""


def _env(v: Any) -> str:
    if v not in ENVIRONMENTS:
        raise FeeContextError(f"kalshi_env must be one of {ENVIRONMENTS}, got {v!r}")
    return v


def _id(v: Any, name: str) -> str:
    if not isinstance(v, str) or not _ID_RE.match(v):
        raise FeeContextError(f"{name} must match {_ID_RE.pattern}, got {v!r}")
    return v


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(ACCOUNT_SCHEMA)


# ---- accounts ------------------------------------------------------------------

@dataclass(frozen=True)
class Account:
    kalshi_env: str
    account_id: str
    subaccount: Optional[int]
    precision: Decimal
    precision_source: str


def register_account(conn: sqlite3.Connection, *, kalshi_env: str, account_id: str,
                     precision: Any, precision_source: str,
                     subaccount: Optional[int] = None) -> Account:
    """Create an account, or confirm an identical one exists. Changing an
    existing account's precision or subaccount is refused (accounting
    identity must not drift silently)."""
    env, acct = _env(kalshi_env), _id(account_id, "account_id")
    if precision_source not in PRECISION_SOURCES:
        raise FeeContextError(f"precision_source must be one of {PRECISION_SOURCES}")
    if subaccount is not None and (isinstance(subaccount, bool) or not isinstance(subaccount, int)
                                   or subaccount < 0):
        raise FeeContextError("subaccount must be a non-negative int or None")
    try:
        prec = fees.validate_precision(precision)
    except fees.FeeInputError as e:
        raise FeeContextError(str(e)) from e
    existing = get_account(conn, kalshi_env=env, account_id=acct)
    if existing is not None:
        if (existing.precision, existing.precision_source, existing.subaccount) != \
                (prec, precision_source, subaccount):
            raise FeeContextError(f"account {env}/{acct} already registered with different settings")
        return existing
    conn.execute("INSERT INTO fee_accounts VALUES (?,?,?,?,?,?)",
                 (env, acct, subaccount, str(prec), precision_source, _now()))
    return Account(env, acct, subaccount, prec, precision_source)


def get_account(conn: sqlite3.Connection, *, kalshi_env: str, account_id: str) -> Optional[Account]:
    r = conn.execute("SELECT subaccount, precision, precision_source FROM fee_accounts "
                     "WHERE kalshi_env=? AND account_id=?",
                     (_env(kalshi_env), _id(account_id, "account_id"))).fetchone()
    if r is None:
        return None
    return Account(kalshi_env, account_id, r[0], fees.to_decimal(r[1], "precision"), r[2])


def bind_credential(conn: sqlite3.Connection, *, kalshi_env: str, account_id: str,
                    credential_fingerprint: str, binding_source: str,
                    observed_user_id: Optional[str] = None,
                    observed_subaccount: Optional[int] = None,
                    evidence_ref: Optional[str] = None) -> None:
    """Bind a credential (by fingerprint, never the key) to an account.
    Refuses: an unknown account; a fingerprint actively bound to another
    account; an observed identity that disagrees with one already observed
    for this account; an api_observed binding without user_id and evidence;
    an observed subaccount that disagrees with the account's."""
    env, acct = _env(kalshi_env), _id(account_id, "account_id")
    if not isinstance(credential_fingerprint, str) or not _FP_RE.match(credential_fingerprint):
        raise FeeContextError("credential_fingerprint must be 64 lowercase hex chars")
    if binding_source not in BINDING_SOURCES:
        raise FeeContextError(f"binding_source must be one of {BINDING_SOURCES}")
    account = get_account(conn, kalshi_env=env, account_id=acct)
    if account is None:
        raise FeeContextError(f"account {env}/{acct} is not registered")
    if binding_source == "api_observed":
        _id(observed_user_id, "observed_user_id")
        _id(evidence_ref, "evidence_ref")
    if observed_subaccount is not None and observed_subaccount != account.subaccount:
        raise FeeContextError("observed subaccount disagrees with the account's subaccount")
    other = conn.execute(
        "SELECT account_id FROM fee_account_bindings WHERE kalshi_env=? AND "
        "credential_fingerprint=? AND revoked_at IS NULL", (env, credential_fingerprint)).fetchone()
    if other is not None and other[0] != acct:
        raise FeeContextError(f"credential already bound to account {other[0]!r}")
    if observed_user_id is not None:
        seen = {r[0] for r in conn.execute(
            "SELECT observed_user_id FROM fee_account_bindings WHERE kalshi_env=? AND "
            "account_id=? AND observed_user_id IS NOT NULL", (env, acct))}
        if seen and seen != {observed_user_id}:
            raise FeeContextError("observed user_id disagrees with this account's earlier observation")
    if other is not None:
        return  # already actively bound to this account
    conn.execute(
        "INSERT INTO fee_account_bindings VALUES (?,?,?,?,?,?,?,?,NULL)",
        (env, acct, credential_fingerprint, binding_source, observed_user_id,
         observed_subaccount, evidence_ref, _now()))


def revoke_credential(conn: sqlite3.Connection, *, kalshi_env: str,
                      credential_fingerprint: str) -> None:
    conn.execute("UPDATE fee_account_bindings SET revoked_at=? WHERE kalshi_env=? AND "
                 "credential_fingerprint=? AND revoked_at IS NULL",
                 (_now(), _env(kalshi_env), credential_fingerprint))


def account_for_credential(conn: sqlite3.Connection, *, kalshi_env: str,
                           credential_fingerprint: str) -> Account:
    r = conn.execute("SELECT account_id FROM fee_account_bindings WHERE kalshi_env=? AND "
                     "credential_fingerprint=? AND revoked_at IS NULL",
                     (_env(kalshi_env), credential_fingerprint)).fetchone()
    if r is None:
        raise FeeContextError("credential is not bound to any account")
    return get_account(conn, kalshi_env=kalshi_env, account_id=r[0])


# ---- policies and context ----------------------------------------------------------

def conditional_paper_coverage(*, scope_ticker: str, snapshot: fs.AsOf, at: datetime,
                               evidence: tuple[str, ...]) -> fs.Coverage:
    """ASSUMED coverage over exactly the interval the resolver requires,
    [snapshot.lo, max(at, snapshot.hi)], under the conditional-paper
    assumption. It covers nothing outside that interval."""
    if not isinstance(snapshot, fs.AsOf):
        raise FeeContextError("snapshot must be an AsOf")
    at = at.astimezone(timezone.utc)
    return fs.Coverage(
        fs.ASSUMED, start=snapshot.lo - timedelta(microseconds=1), end=max(at, snapshot.hi),
        scope=frozenset({scope_ticker}), evidence=tuple(evidence),
        assumption_ids=(CONDITIONAL_ASSUMPTION,),
    )


@dataclass(frozen=True)
class FeeContext:
    kalshi_env: str
    account_id: str
    subaccount: Optional[int]
    precision: Decimal
    precision_source: str
    policy: str
    resolution: Optional[fs.FeeResolution]
    evidence: tuple[str, ...]
    valid_until: Optional[datetime]
    usable: bool                 # admissible under this context's policy
    reason: str

    def require_policy(self, policy: str) -> "FeeContext":
        """Cohorts call this so strict and conditional contexts never mix."""
        if self.policy != policy:
            raise FeeContextError(f"context policy {self.policy!r} != cohort policy {policy!r}")
        return self


def build_fee_context(
    conn: sqlite3.Connection, *, kalshi_env: str, account_id: str, policy: str,
    market: fs.MarketFeeInfo, series: fs.SeriesSnapshot,
    series_changes: fs.SeriesChangeList, event: fs.EventSnapshot,
    event_changes: fs.EventChangeList, role: str, at: datetime, ttl: timedelta,
    evidence: tuple[str, ...],
) -> FeeContext:
    """Resolve fees for one account, market, role and instant under a policy.

    STRICT keeps the coverage the change lists already carry. CONDITIONAL
    replaces it with conditional_paper_coverage for this decision. The
    context is usable only if the resolution is admissible under exactly the
    policy's accepted assumptions."""
    if policy not in POLICIES:
        raise FeeContextError(f"policy must be one of {POLICIES}")
    ev = tuple(evidence)
    if not ev or not all(isinstance(e, str) and e for e in ev):
        raise FeeContextError("evidence must reference at least one capture")
    account = get_account(conn, kalshi_env=kalshi_env, account_id=account_id)

    def unusable(reason: str, resolution=None) -> FeeContext:
        return FeeContext(kalshi_env, account_id, account.subaccount if account else None,
                          account.precision if account else Decimal(0),
                          account.precision_source if account else "", policy,
                          resolution, ev, getattr(resolution, "valid_until", None), False, reason)

    if account is None:
        return unusable("account_not_registered")
    if policy == CONDITIONAL_PAPER:
        if kalshi_env != "paper":
            raise FeeContextError("the conditional-paper policy is paper-only")
        series_changes = dataclasses.replace(series_changes, coverage=conditional_paper_coverage(
            scope_ticker=series.ticker, snapshot=series.as_of, at=at, evidence=ev))
        event_changes = dataclasses.replace(event_changes, coverage=conditional_paper_coverage(
            scope_ticker=event.event_ticker, snapshot=event.as_of, at=at, evidence=ev))
    try:
        res = fs.resolve_fee_schedule(market=market, series=series, series_changes=series_changes,
                                      event=event, event_changes=event_changes, role=role,
                                      at=at, ttl=ttl)
    except fs.FeeMetadataError as e:
        return unusable(f"metadata_error:{e}")
    if not fs.admissible(res, _ACCEPTED[policy]):
        return unusable(f"{res.status}:{res.reason}" if not res.has_coefficient
                        else "assumptions_not_accepted", res)
    return FeeContext(kalshi_env, account_id, account.subaccount, account.precision,
                      account.precision_source, policy, res, ev, res.valid_until, True, "ok")
