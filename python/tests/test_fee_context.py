"""Tests for fee_context.py (A1: account registry, policies, FeeContext).
Stdlib-only: python -m pytest tests/test_fee_context.py --noconftest
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db  # noqa: E402
import fee_context as fc  # noqa: E402
import fee_schedule as fs  # noqa: E402

UTC = timezone.utc
T0 = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
AS_OF = fs.AsOf(T0 - timedelta(seconds=20), T0 + timedelta(seconds=2))
AT = T0 + timedelta(seconds=10)
FP1, FP2, FP3 = "a" * 64, "b" * 64, "c" * 64
EV = ("cap-1",)


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    db.init_db()
    c = sqlite3.connect(str(db.db_path()))
    c.execute("PRAGMA foreign_keys=ON")
    yield c
    c.close()


def reg(conn, env="paper", acct="research-1", prec="0.0001", sub=None):
    return fc.register_account(conn, kalshi_env=env, account_id=acct, precision=prec,
                               precision_source="configured", subaccount=sub)


# ---- account registry ---------------------------------------------------------

def test_register_is_idempotent_but_refuses_drift(conn):
    a = reg(conn)
    assert reg(conn) == a and a.precision == D("0.0001")
    with pytest.raises(fc.FeeContextError):
        reg(conn, prec="0.01")
    with pytest.raises(fc.FeeContextError):
        reg(conn, sub=1)


@pytest.mark.parametrize("kw", [dict(env="demo"), dict(acct=""), dict(acct="a b"),
                                dict(prec="0.0000001"), dict(prec=0.01), dict(sub=-1), dict(sub=True)])
def test_register_rejects_bad_input(conn, kw):
    with pytest.raises(fc.FeeContextError):
        reg(conn, **kw)


def bind(conn, fp=FP1, acct="research-1", env="paper", src="user_configured", **kw):
    fc.bind_credential(conn, kalshi_env=env, account_id=acct, credential_fingerprint=fp,
                       binding_source=src, **kw)


def test_key_rotation_keeps_account_identity(conn):
    reg(conn)
    bind(conn, FP1)
    assert fc.account_for_credential(conn, kalshi_env="paper", credential_fingerprint=FP1).account_id == "research-1"
    fc.revoke_credential(conn, kalshi_env="paper", credential_fingerprint=FP1)
    bind(conn, FP2)
    assert fc.account_for_credential(conn, kalshi_env="paper", credential_fingerprint=FP2).account_id == "research-1"
    with pytest.raises(fc.FeeContextError):
        fc.account_for_credential(conn, kalshi_env="paper", credential_fingerprint=FP1)


def test_wrong_account_binding_refused(conn):
    reg(conn, acct="acct-a")
    reg(conn, acct="acct-b")
    bind(conn, FP1, acct="acct-a")
    with pytest.raises(fc.FeeContextError):
        bind(conn, FP1, acct="acct-b")
    bind(conn, FP1, acct="acct-a")  # re-binding the same pair is idempotent


def test_binding_requires_registered_account_and_valid_fingerprint(conn):
    with pytest.raises(fc.FeeContextError):
        bind(conn, FP1, acct="nobody")
    reg(conn)
    for bad in ("A" * 64, "a" * 63, "", None):
        with pytest.raises(fc.FeeContextError):
            bind(conn, bad)


def test_api_observed_binding_needs_identity_and_evidence(conn):
    reg(conn, env="production", acct="live-1", sub=0)
    with pytest.raises(fc.FeeContextError):
        bind(conn, FP1, acct="live-1", env="production", src="api_observed")
    bind(conn, FP1, acct="live-1", env="production", src="api_observed",
         observed_user_id="user-123", observed_subaccount=0, evidence_ref="cap-9")
    with pytest.raises(fc.FeeContextError):   # rotated key reports a different user
        bind(conn, FP2, acct="live-1", env="production", src="api_observed",
             observed_user_id="user-999", evidence_ref="cap-10")
    with pytest.raises(fc.FeeContextError):   # subaccount disagreement
        bind(conn, FP3, acct="live-1", env="production", src="api_observed",
             observed_user_id="user-123", observed_subaccount=2, evidence_ref="cap-11")


def test_same_fingerprint_independent_per_environment(conn):
    reg(conn, env="paper", acct="a")
    reg(conn, env="production", acct="a")
    bind(conn, FP1, acct="a", env="paper")
    bind(conn, FP1, acct="a", env="production")


# ---- contexts -------------------------------------------------------------------

def inputs(as_of=AS_OF, fee_type="quadratic", mult="1"):
    J = fs.loads
    series = fs.parse_series(J('{"series":{"ticker":"KXBTC15M","fee_type":"' + fee_type +
                               '","fee_multiplier":' + mult + '}}'), as_of)
    sc = fs.parse_series_fee_changes(J('{"series_fee_change_arr":[]}'), as_of,
                                     include_historical=True, coverage=fs.Coverage.none())
    event = fs.parse_event(J('{"event":{"event_ticker":"E1","series_ticker":"KXBTC15M"}}'), as_of)
    ec = fs.parse_event_fee_change_pages([J('{"cursor":"","event_fee_changes":[]}')], as_of,
                                         coverage=fs.Coverage.none())
    market = fs.parse_market({"ticker": "E1-M", "event_ticker": "E1"}, as_of)
    return dict(market=market, series=series, series_changes=sc, event=event, event_changes=ec)


def ctx(conn, policy=fc.CONDITIONAL_PAPER, env="paper", acct="research-1", at=AT, role="taker", **kw):
    return fc.build_fee_context(conn, kalshi_env=env, account_id=acct, policy=policy, role=role,
                                at=at, ttl=timedelta(seconds=30), evidence=EV, **inputs(**kw))


def test_unregistered_account_is_unusable(conn):
    c = ctx(conn)
    assert not c.usable and c.reason == "account_not_registered"


def test_strict_policy_fails_closed_without_verified_coverage(conn):
    reg(conn)
    c = ctx(conn, policy=fc.STRICT)
    assert not c.usable and c.reason == "UNAVAILABLE:history_unverified:series_changes"


def test_conditional_paper_context_usable_and_labelled(conn):
    reg(conn)
    c = ctx(conn)
    assert c.usable and c.resolution.coefficient == D("0.07")
    assert c.resolution.assumptions == (fc.CONDITIONAL_ASSUMPTION,)
    assert c.policy == fc.CONDITIONAL_PAPER and c.precision == D("0.0001")
    cov = c.resolution.provenance["coverage"]["series_changes"]
    assert cov["start"] < AS_OF.lo and cov["end"] == max(AT, AS_OF.hi)
    assert cov["scope"] == ["KXBTC15M"] and cov["evidence"] == EV


def test_conditional_policy_is_paper_only(conn):
    reg(conn, env="production")
    with pytest.raises(fc.FeeContextError):
        ctx(conn, env="production")


def test_strict_and_conditional_cohorts_cannot_mix(conn):
    reg(conn)
    c = ctx(conn)
    assert c.require_policy(fc.CONDITIONAL_PAPER) is c
    with pytest.raises(fc.FeeContextError):
        c.require_policy(fc.STRICT)


def test_stale_snapshot_unusable_even_under_conditional(conn):
    reg(conn)
    c = ctx(conn, at=AS_OF.hi + timedelta(seconds=31))
    assert not c.usable and c.reason.startswith("UNAVAILABLE:stale")


def test_conditional_validity_ends_at_the_decision_instant(conn):
    # The conditional assumption covers [lo, max(at, hi)] only, so a context
    # is valid for its own decision and nothing later: a resting order or a
    # later decision needs a fresh context.
    reg(conn)
    assert ctx(conn).valid_until == AT
    at_inside = AS_OF.hi - timedelta(seconds=1)   # at inside the snapshot window
    assert ctx(conn, at=at_inside).valid_until == AS_OF.hi


def test_unsupported_fee_type_unusable(conn):
    reg(conn)
    c = ctx(conn, fee_type="flat")
    assert not c.usable and c.reason == "UNSUPPORTED:fee_type:flat"


def test_explicit_zero_is_usable_with_zero_coefficient(conn):
    reg(conn)
    c = ctx(conn, role="maker")
    assert c.usable and c.resolution.coefficient == 0


def test_evidence_required(conn):
    reg(conn)
    with pytest.raises(fc.FeeContextError):
        fc.build_fee_context(conn, kalshi_env="paper", account_id="research-1",
                             policy=fc.CONDITIONAL_PAPER, role="taker", at=AT,
                             ttl=timedelta(seconds=30), evidence=(), **inputs())


def test_unknown_policy_refused(conn):
    reg(conn)
    with pytest.raises(fc.FeeContextError):
        ctx(conn, policy="yolo")


def test_configured_binding_upgrades_to_api_observed(conn):
    reg(conn, env="production", acct="live-1", sub=0)
    bind(conn, FP1, acct="live-1", env="production")             # user_configured first
    bind(conn, FP1, acct="live-1", env="production", src="api_observed",
         observed_user_id="user-123", observed_subaccount=0, evidence_ref="cap-9")
    row = conn.execute("SELECT binding_source, observed_user_id, observed_subaccount, evidence_ref "
                       "FROM fee_account_bindings").fetchone()
    assert row == ("api_observed", "user-123", 0, "cap-9")
    with pytest.raises(fc.FeeContextError):                      # later disagreement refused
        bind(conn, FP1, acct="live-1", env="production", src="api_observed",
             observed_user_id="user-999", evidence_ref="cap-10")
