"""Tests for fee_ledger.py (1d-1: schema, persistence, transactions).

Uses a temporary app data root. Stdlib-only:
    python -m pytest tests/test_fee_ledger.py --noconftest
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
import fee_ledger as fl  # noqa: E402
import fee_schedule as fs  # noqa: E402
import fees  # noqa: E402

UTC = timezone.utc
T = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
DIRECT = "0.0001"
NONDIRECT = "0.01"


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    db.init_db()
    c = sqlite3.connect(str(db.db_path()))
    c.execute("PRAGMA foreign_keys=ON")
    yield c
    c.close()


TICKER = "KXBTC15M-X"


def res(status=fs.RESOLVED, coeff="0.07", role="taker", at=T, assumptions=("a1",), reason="ok",
        market=TICKER):
    return fs.FeeResolution(status=status, reason=reason, role=role, at=at, market_ticker=market,
                            coefficient=None if coeff is None else D(coeff),
                            assumptions=assumptions, provenance={"at": at, "ttl": timedelta(minutes=5)})


def begin(conn):
    if not conn.in_transaction:
        conn.execute("BEGIN")


def apply(conn, fill_id="f1", order_id="o1", precision=DIRECT, price="0.97", count="1",
          role="taker", action="buy", resolution=None, at=T, account="paper-local", env="paper",
          ticker=TICKER, side="yes", auto_begin=True):
    if auto_begin:
        begin(conn)
    return fl.apply_model_fill(
        conn, kalshi_env=env, account_id=account, order_id=order_id, fill_id=fill_id,
        precision=precision, precision_source="configured", ticker=ticker,
        side=side, action=action, role=role, price=price, count=count, filled_at=at,
        resolution=resolution or res(role=role, at=at),
    )


def rows(conn, sql, *args):
    return conn.execute(sql, args).fetchall()


# ---- schema -------------------------------------------------------------------

def test_init_is_idempotent_and_marks_legacy(conn):
    db.init_db()  # second run must not fail
    conn.execute("INSERT INTO paper_fills (ticker, side, kind, contracts, price_cents, "
                 "fee_usd, cash_delta_usd) VALUES ('T','yes','buy',1,50,0.02,-0.52)")
    assert rows(conn, "SELECT fee_source FROM paper_fills")[0][0] == "legacy"
    for t in fl.LEGACY_FEE_TABLES:
        cols = {r[1] for r in rows(conn, f"PRAGMA table_info({t})")}
        assert "fee_source" in cols


def test_existing_rows_become_legacy_on_upgrade(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    with db.get_db() as c:  # an old database: tables without the ledger
        c.executescript(db.SCHEMA)
        c.execute("INSERT INTO paper_orders (order_id, created_at, updated_at, ticker, side, "
                  "action, limit_cents, count, fees_usd, status) "
                  "VALUES ('p1','t','t','T','yes','buy',50,1,0.02,'executed')")
    db.init_db()
    with db.get_db() as c:
        assert c.execute("SELECT fee_source, fees_usd FROM paper_orders").fetchone()[:] == ("legacy", 0.02)
        # Legacy REAL values are NOT copied into the exact ledger.
        assert c.execute("SELECT COUNT(*) FROM fee_fill_model").fetchone()[0] == 0


def test_order_state_check_known_iff_accumulator(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO fee_order_state VALUES ('paper','a','o','T','yes','buy','0.01',"
                     "'configured',NULL,'known',NULL,0,'t','t')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO fee_order_state VALUES ('paper','a','o','T','yes','buy','0.01',"
                     "'configured','0','unknown','r',0,'t','t')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO fee_order_state VALUES ('demo','a','o','T','yes','buy','0.01',"
                     "'configured','0','known',NULL,0,'t','t')")


def _report(conn, kind="fill", subject="fill-1", state="reported", value="0.0021", field="fee_cost"):
    conn.execute("INSERT INTO fee_exchange_reports (kalshi_env, account_id, report_kind, "
                 "subject_id, field_name, field_state, reported_value, observed_at, "
                 "evidence_ref, created_at) VALUES ('production','acct',?,?,?,?,?,'t','cap','t')",
                 (kind, subject, field, state, value))


def test_exchange_reports_keep_zero_distinct_from_absent(conn):
    _report(conn, subject="f-zero", value="0")
    _report(conn, subject="f-absent", state="absent", value=None)
    got = dict(rows(conn, "SELECT subject_id, reported_value FROM fee_exchange_reports"))
    assert got == {"f-zero": "0", "f-absent": None}
    with pytest.raises(sqlite3.IntegrityError):
        _report(conn, subject="bad1", state="reported", value=None)
    with pytest.raises(sqlite3.IntegrityError):
        _report(conn, subject="bad2", state="absent", value="0")


def test_per_fill_report_unique_but_totals_are_observations(conn):
    _report(conn, subject="fill-1")
    with pytest.raises(sqlite3.IntegrityError):
        _report(conn, subject="fill-1")             # re-polling the same fill
    _report(conn, kind="order_total", subject="o-1", value="0.0021")
    _report(conn, kind="order_total", subject="o-1", value="0.0042")  # running total re-observed
    assert len(rows(conn, "SELECT 1 FROM fee_exchange_reports WHERE report_kind='order_total'")) == 2


# ---- model fills ----------------------------------------------------------------

def test_apply_model_fill_persists_exact_values(conn):
    r = apply(conn)
    expect = fees.settle_fill("-0.97", "0.002037", DIRECT)
    assert r.fee == expect and r.resolution_status == fs.RESOLVED
    row = conn.execute(
        "SELECT revenue, coefficient, model_fee, trade_fee, trade_fee_micro, rounding_fee, rebate, "
        "net_fee, balance_delta, balance_delta_micro, accumulator_after, assumptions "
        "FROM fee_fill_model").fetchone()
    assert row == ("-0.97", "0.07", "0.002037", "0.002037", 2037, "0.000063", "0",
                   "0.0021", "-0.9721", -972100, "0.000063", '["a1"]')
    st = r.order_state
    assert (st.accumulator, st.accumulator_status, st.version) == (D("0.000063"), "known", 1)


def test_accumulator_carries_and_rebate_persists_across_fills(conn):
    rr = res(coeff="0.000001", assumptions=())   # tiny coefficient: model fee ~0
    results = []
    begin(conn)
    for i in range(3):
        results.append(fl.apply_model_fill(
            conn, kalshi_env="paper", account_id="paper-local", order_id="o1",
            fill_id=f"f{i}", precision=NONDIRECT, precision_source="configured",
            ticker=TICKER, side="yes", action="buy", role="taker", price="0.05", count="1",
            filled_at=T, resolution=rr))
    # Same arithmetic as settling the three fills on one in-memory order.
    st = fees.OrderFeeState("o1", NONDIRECT)
    mf = fees.quadratic_model_fee("0.05", "1", "0.000001")
    expected = [st.apply_fill(f"f{i}", fees.fill_revenue("0.05", "1", "buy"), mf) for i in range(3)]
    assert [r.fee for r in results] == expected
    assert results[-1].order_state.accumulator == st.accumulator
    assert results[-1].order_state.version == 3


def test_duplicate_fill_refused_and_state_unchanged(conn):
    apply(conn)
    before = fl.get_order_state(conn, kalshi_env="paper", account_id="paper-local", order_id="o1")
    with pytest.raises(fl.DuplicateFillError):
        apply(conn)
    after = fl.get_order_state(conn, kalshi_env="paper", account_id="paper-local", order_id="o1")
    assert before == after
    assert rows(conn, "SELECT COUNT(*) FROM fee_fill_model")[0][0] == 1


def test_same_fill_id_allowed_in_another_account_or_env(conn):
    apply(conn)
    apply(conn, account="other-acct")
    apply(conn, env="production", account="paper-local")
    assert rows(conn, "SELECT COUNT(*) FROM fee_fill_model")[0][0] == 3


def test_unresolved_fee_makes_accumulator_unknown_and_stays_unknown(conn):
    r1 = apply(conn, fill_id="f1", resolution=res(status=fs.UNAVAILABLE, coeff=None,
                                                  reason="history_unverified:event_changes"))
    assert r1.fee is None and r1.order_state.accumulator_status == "unknown"
    assert r1.order_state.accumulator is None
    r2 = apply(conn, fill_id="f2")   # a resolvable fill on the same order
    assert r2.fee is None and r2.resolution_status == fl.ORDER_UNKNOWN
    vals = rows(conn, "SELECT trade_fee, trade_fee_micro, net_fee, balance_delta FROM fee_fill_model")
    assert vals == [(None, None, None, None), (None, None, None, None)]
    assert "history_unverified" in r2.order_state.unknown_reason


def test_explicit_zero_coefficient_is_recorded_as_zero_not_null(conn):
    r = apply(conn, role="maker", resolution=res(status=fs.EXPLICIT_ZERO, coeff="0", role="maker",
                                                 reason="no_maker_fee_for_type"))
    assert r.fee.trade_fee == 0
    row = conn.execute("SELECT trade_fee, trade_fee_micro, coefficient FROM fee_fill_model").fetchone()
    assert row == ("0", 0, "0")


def test_precision_mismatch_refused_without_writes(conn):
    apply(conn, fill_id="f1", precision=DIRECT)
    with pytest.raises(fl.FeeLedgerError):
        apply(conn, fill_id="f2", precision=NONDIRECT)
    assert rows(conn, "SELECT COUNT(*) FROM fee_fill_model")[0][0] == 1


@pytest.mark.parametrize("kw", [
    dict(resolution=res(role="maker")),                          # role mismatch
    dict(resolution=res(at=T + timedelta(seconds=1))),           # time mismatch
    dict(env="demo"), dict(account=""), dict(account="a/b"), dict(fill_id="x y"),
    dict(precision="0.0000001"), dict(price=0.97), dict(price="1"), dict(count="0"),
    dict(at=datetime(2026, 10, 30, 12, 0)),
])
def test_bad_inputs_refused_without_writes(conn, kw):
    with pytest.raises(fl.FeeLedgerError):
        apply(conn, **kw)
    assert rows(conn, "SELECT COUNT(*) FROM fee_fill_model")[0][0] == 0
    assert rows(conn, "SELECT COUNT(*) FROM fee_order_state")[0][0] == 0


def test_concurrent_version_change_rolls_back_whole_fill(conn, monkeypatch):
    apply(conn, fill_id="f0")
    real_settle = fees.settle_fill

    def racing_settle(*a, **k):
        # Another writer advances the order between our read and our update.
        conn.execute("UPDATE fee_order_state SET version=version+1")
        return real_settle(*a, **k)

    monkeypatch.setattr(fl.fees, "settle_fill", racing_settle)
    with pytest.raises(fl.ConcurrentUpdateError):
        apply(conn, fill_id="f1")
    assert rows(conn, "SELECT fill_id FROM fee_fill_model") == [("f0",)]
    # The racing bump was inside the rolled-back savepoint too.
    assert fl.get_order_state(conn, kalshi_env="paper", account_id="paper-local",
                              order_id="o1").version == 1


def test_fill_requires_existing_order_row_via_foreign_key(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO fee_fill_model (kalshi_env, account_id, order_id, fill_id, "
                     "ticker, side, action, role, price, count, revenue, filled_at, "
                     "resolution_status, resolution_reason, assumptions, provenance, created_at) "
                     "VALUES ('paper','a','missing','f','T','yes','buy','taker','0.5','1','-0.5',"
                     "'t','RESOLVED','ok','[]','{}','t')")


# ---- evidence -------------------------------------------------------------------

def test_evidence_dir_under_app_data(conn, tmp_path):
    d = fl.evidence_dir("paper")
    assert d == tmp_path / "data" / "fee-evidence" / "paper" and d.is_dir()
    with pytest.raises(fl.FeeLedgerError):
        fl.evidence_dir("demo")


def test_evidence_refs_idempotent_and_queryable(conn):
    assert not fl.is_evidence_referenced(conn, "cap1")
    for _ in range(2):
        fl.add_evidence_ref(conn, evidence_ref="cap1", kalshi_env="paper",
                            referrer_kind="fill", referrer_id="f1", account_id="paper-local")
    assert fl.is_evidence_referenced(conn, "cap1")
    assert rows(conn, "SELECT COUNT(*) FROM fee_evidence_refs")[0][0] == 1
    with pytest.raises(fl.FeeLedgerError):
        fl.add_evidence_ref(conn, evidence_ref="cap1", kalshi_env="paper",
                            referrer_kind="whim", referrer_id="x")


def test_exact_micro():
    assert fl.exact_micro(D("-0.9721")) == -972100
    assert fl.exact_micro(D("0.002037")) == 2037
    assert fl.exact_micro(None) is None
    with pytest.raises(fl.FeeLedgerError):
        fl.exact_micro(D("0.0000001"))


# ---- review fixes for f3509c5 -------------------------------------------------

def _counts(conn):
    return (rows(conn, "SELECT COUNT(*) FROM fee_fill_model")[0][0],
            rows(conn, "SELECT COUNT(*) FROM fee_order_state")[0][0])


def test_resolution_for_another_market_is_refused(conn):
    # Codex repro: a zero-fee resolution for OTHER recorded on this market.
    zero_other = res(status=fs.EXPLICIT_ZERO, coeff="0", market="OTHER", reason="multiplier_zero")
    with pytest.raises(fl.FeeLedgerError):
        apply(conn, resolution=zero_other)
    assert _counts(conn) == (0, 0)


def test_resolution_without_market_identity_is_refused(conn):
    with pytest.raises(fl.FeeLedgerError):
        apply(conn, resolution=res(market=""))
    assert _counts(conn) == (0, 0)


def test_resolver_output_carries_market_identity():
    r = fs.FeeResolution(status=fs.RESOLVED, reason="ok", role="taker", at=T)
    assert r.market_ticker == ""   # default is empty, so the ledger refuses it


@pytest.mark.parametrize("kw", [
    dict(ticker="KXETH15M-X"),       # other market
    dict(side="no"),                 # other side
    dict(action="sell"),             # other direction
])
def test_order_bound_to_market_side_and_action(conn, kw):
    apply(conn, fill_id="f1")
    before = fl.get_order_state(conn, kalshi_env="paper", account_id="paper-local", order_id="o1")
    r = res(market=kw.get("ticker", TICKER))
    with pytest.raises(fl.FeeLedgerError):
        apply(conn, fill_id="f2", resolution=r, **kw)
    assert fl.get_order_state(conn, kalshi_env="paper", account_id="paper-local",
                              order_id="o1") == before
    assert _counts(conn) == (1, 1)


def test_maker_taker_transition_on_one_order_still_allowed(conn):
    apply(conn, fill_id="f1", role="taker")
    r2 = apply(conn, fill_id="f2", role="maker",
               resolution=res(status=fs.EXPLICIT_ZERO, coeff="0", role="maker",
                              reason="no_maker_fee_for_type"))
    assert r2.order_state.version == 2 and r2.fee.trade_fee == 0


def test_requires_open_transaction(conn):
    assert not conn.in_transaction
    with pytest.raises(fl.FeeLedgerError):
        apply(conn, auto_begin=False)
    assert _counts(conn) == (0, 0)


def test_caller_rollback_after_successful_apply_discards_everything(conn):
    # Codex repro: apply then rollback used to leave the fill committed.
    begin(conn)
    apply(conn)
    conn.execute("INSERT INTO paper_fills (ticker, side, kind, contracts, price_cents, fee_usd, "
                 "cash_delta_usd) VALUES (?, 'yes', 'buy', 1, 97, 0, -0.97)", (TICKER,))
    conn.rollback()     # e.g. a later cash/position write failed
    assert _counts(conn) == (0, 0)
    assert rows(conn, "SELECT COUNT(*) FROM paper_fills")[0][0] == 0


def test_exception_after_apply_in_get_db_rolls_back(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    db.init_db()
    with pytest.raises(RuntimeError):
        with db.get_db() as c:
            c.execute("BEGIN IMMEDIATE")
            apply(c, auto_begin=False)
            raise RuntimeError("position write failed")
    with db.get_db() as c:
        assert c.execute("SELECT COUNT(*) FROM fee_fill_model").fetchone()[0] == 0
        assert c.execute("SELECT COUNT(*) FROM fee_order_state").fetchone()[0] == 0


def test_successful_caller_transaction_commits(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    db.init_db()
    with db.get_db() as c:
        c.execute("BEGIN IMMEDIATE")
        apply(c, auto_begin=False)
    with db.get_db() as c:
        assert c.execute("SELECT COUNT(*) FROM fee_fill_model").fetchone()[0] == 1


def test_evidence_refs_namespaced_by_env_and_account(conn):
    # Codex repro: paper and production refs to the same capture used to collapse.
    for env in ("paper", "production"):
        fl.add_evidence_ref(conn, evidence_ref="cap1", kalshi_env=env,
                            referrer_kind="fill", referrer_id="f1", account_id="acct-a")
    fl.add_evidence_ref(conn, evidence_ref="cap1", kalshi_env="paper",
                        referrer_kind="fill", referrer_id="f1", account_id="acct-b")
    fl.add_evidence_ref(conn, evidence_ref="cap1", kalshi_env="paper",
                        referrer_kind="forecast", referrer_id="f1")
    fl.add_evidence_ref(conn, evidence_ref="cap1", kalshi_env="paper",   # exact duplicate
                        referrer_kind="fill", referrer_id="f1", account_id="acct-a")
    got = sorted(rows(conn, "SELECT kalshi_env, account_id, referrer_kind FROM fee_evidence_refs"))
    assert got == [("paper", "", "forecast"), ("paper", "acct-a", "fill"),
                   ("paper", "acct-b", "fill"), ("production", "acct-a", "fill")]


@pytest.mark.parametrize("kind,acct", [("fill", None), ("order", ""), ("trade", "a/b"),
                                       ("forecast", "acct-a"), ("evaluation", "x")])
def test_evidence_ref_account_scope_enforced(conn, kind, acct):
    with pytest.raises(fl.FeeLedgerError):
        fl.add_evidence_ref(conn, evidence_ref="cap1", kalshi_env="paper",
                            referrer_kind=kind, referrer_id="r1", account_id=acct)


def test_evidence_ref_scope_check_constraint(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO fee_evidence_refs VALUES ('c','paper','','fill','f1','t')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO fee_evidence_refs VALUES ('c','paper','acct','forecast','f1','t')")
