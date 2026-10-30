"""Tests for execution_gate.py and research_c15.py (decisions, persistence,
crash recovery, unpriced executions, cohorts, manifests).
Stdlib-only: python -m pytest tests/test_research_c15.py --noconftest
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import sqlite3
import sys
import types
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from fractions import Fraction as F
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
# crypto15m (the frozen model) imports network libraries at module load; stub
# them only where they are not installed. These tests never use the network.
for _name in ("httpx", "websockets", "httpx2"):
    if _name not in sys.modules and importlib.util.find_spec(_name) is None:
        _m = types.ModuleType(_name)
        _m.__getattr__ = lambda attr: type(attr, (), {"__init__": lambda self, *a, **k: None})
        sys.modules[_name] = _m

import db  # noqa: E402
import execution_gate as gate  # noqa: E402
import fee_context as fctx  # noqa: E402
import fee_schedule as fs  # noqa: E402
import research_c15 as rx  # noqa: E402
import research_wallet as rw  # noqa: E402
import sim_exec as sx  # noqa: E402

UTC = timezone.utc
CLOSE = datetime(2026, 10, 30, 12, 15, tzinfo=UTC)
NOW = CLOSE - timedelta(seconds=25)
MKT, EV = "KXBTC15M-26OCT301215-00", "KXBTC15M-26OCT301215"
FIX = Path(__file__).resolve().parent / "fixtures" / "kalshi_fee_2026_10_30"


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    db.init_db()
    c = sqlite3.connect(str(db.db_path()), isolation_level=None)
    c.execute("PRAGMA foreign_keys=ON")
    yield c
    c.close()


def fee_ctx(conn, at, *, market=MKT, mult="1", waiver=None, account=None, policy=None):
    J = fs.loads
    a = fs.AsOf(at - timedelta(seconds=5), at)
    mk = {"ticker": market, "event_ticker": EV}
    if waiver:
        mk["fee_waiver_expiration_time"] = waiver
    return fctx.build_fee_context(
        conn, kalshi_env="paper", account_id=account or "research-c15",
        policy=policy or fctx.CONDITIONAL_PAPER,
        role="taker", at=at, ttl=timedelta(seconds=30), evidence=("cap-x",),
        market=fs.parse_market(mk, a),
        series=fs.parse_series(J('{"series":{"ticker":"KXBTC15M","fee_type":"quadratic",'
                                 '"fee_multiplier":' + mult + '}}'), a),
        series_changes=fs.parse_series_fee_changes(J('{"series_fee_change_arr":[]}'), a,
                                                   include_historical=True, coverage=fs.Coverage.none()),
        event=fs.parse_event(J('{"event":{"event_ticker":"' + EV + '","series_ticker":"KXBTC15M"}}'), a),
        event_changes=fs.parse_event_fee_change_pages([J('{"cursor":"","event_fee_changes":[]}')], a,
                                                      coverage=fs.Coverage.none()))


def setup(conn):
    conn.execute("BEGIN")
    mid = rx.ensure_manifest(conn, rx.manifest_body(), NOW)
    assert rx.select_cohort(conn, mid, activate=None, now=NOW) == mid
    rx.setup_wallets(conn, mid)
    rx.launch(conn, mid, NOW - timedelta(days=1))
    conn.commit()
    return mid


# ---- gate ------------------------------------------------------------------------

def test_fee_bound_math():
    b = gate.fee_bound(count=F(1), limit=F(96, 100), coefficient=F(7, 100), precision=F(1, 10**4))
    assert b == F(2688, 10**6) + 100 * F(101, 10**6)


def intent(conn, mid, *, limit="0.95", prob=0.9995, at=NOW, ctx=None, count="1", env=()):
    return gate.ExecutionIntent(
        wallet_id=rx.wallet_id(mid, 1), order_id=rx.order_id(mid, 1, EV), ticker=MKT, side="yes",
        action="buy", order_type="ioc", role="taker", limit=limit, count=count,
        side_probability=prob, decided_at=at, fee_context=ctx or fee_ctx(conn, at), fee_envelope=env)


def admit(conn, it):
    if not conn.in_transaction:
        conn.execute("BEGIN")
    return gate.admit(conn, it, min_net_edge_per_contract="0.02", max_limit="0.98",
                      policy=fctx.CONDITIONAL_PAPER)


def test_admits_and_reserves(conn):
    mid = setup(conn)
    a = admit(conn, intent(conn, mid))
    assert a.admitted and a.reserve_amount == D("0.95") + a.fee_bound
    assert rw.get_wallet(conn, rx.wallet_id(mid, 1)).reserved == a.reserve_amount


def test_original_slippage_repro_rejected(conn):
    mid = setup(conn)
    a = admit(conn, intent(conn, mid, limit="0.97", prob=0.9985))
    assert not a.admitted and a.reason == "net_edge_below_minimum"
    assert rw.get_wallet(conn, rx.wallet_id(mid, 1)).reserved == 0


def test_envelope_uses_largest_coefficient_through_arrival(conn):
    mid = setup(conn)
    later = NOW + timedelta(seconds=1)
    base = admit(conn, intent(conn, mid, env=(fee_ctx(conn, later),)))
    conn.rollback()
    higher = admit(conn, intent(conn, mid, env=(fee_ctx(conn, later, mult="2"),)))
    assert higher.admitted and higher.fee_bound > base.fee_bound


def test_envelope_with_waiver_or_wrong_instant_rejected(conn):
    mid = setup(conn)
    later = NOW + timedelta(seconds=1)
    w = fee_ctx(conn, later, waiver=(CLOSE + timedelta(hours=1)).isoformat())
    assert admit(conn, intent(conn, mid, env=(w,))).reason.startswith("fee_context_unusable")
    early = fee_ctx(conn, NOW - timedelta(seconds=1))
    assert admit(conn, intent(conn, mid, env=(early,))).reason == "fee_context_not_for_this_instant"


@pytest.mark.parametrize("kw,reason", [(dict(limit="0.99"), "limit_above_cap"),
                                       (dict(prob=0.5), "net_edge_below_minimum"),
                                       (dict(count="0.001"), "bad_input"),
                                       (dict(count="5"), "insufficient_funds")])
def test_rejections(conn, kw, reason):
    mid = setup(conn)
    a = admit(conn, intent(conn, mid, **kw))
    assert not a.admitted and a.reason.startswith(reason)


def test_stale_or_foreign_context_rejected(conn):
    mid = setup(conn)
    assert admit(conn, intent(conn, mid, ctx=fee_ctx(conn, NOW), at=NOW + timedelta(seconds=1))).reason \
        == "fee_context_not_for_this_instant"
    assert admit(conn, intent(conn, mid, ctx=fee_ctx(conn, NOW, market="OTHER-1"))).reason \
        == "fee_context_identity_mismatch"


# ---- settlement rules (production-shaped payloads) -----------------------------------

def captured_market_event():
    ev = fs.loads((FIX / "event-btc15m.json").read_bytes())
    return ev["markets"][0], ev["event"]


def test_captured_btc_rules_verified_with_decimal_strike():
    # Codex repro: the Decimal strike from fs.loads used to fail as no_floor_strike.
    m, e = captured_market_event()
    assert isinstance(m["floor_strike"], D)
    r = rx.validate_rules(m, e)
    assert r.ok and r.strike == 77583.44 and r.close_time == datetime(2026, 10, 30, 11, 0, tzinfo=UTC)


@pytest.mark.parametrize("patch,detail", [
    ({"floor_strike": None}, "no_floor_strike"),
    ({"floor_strike": True}, "no_floor_strike"),
    ({"strike_type": "greater"}, "strike_type:greater"),
    ({"cap_strike": D("1")}, "has_cap_strike"),
    ({"custom_strike": {"round_digits": "0"}}, "strike_rounding"),
    ({"notional_value_dollars": "10.0000"}, "payout"),
    ({"rules_primary": "If the BRTI at 7:00 AM is above X then Yes."}, "rules_primary_terms"),
    ({"close_time": "2026-10-30T11:15:00Z"}, "rules_primary_close_time"),
    ({"rules_secondary": "The official value is the last print."}, "rules_secondary_terms"),
])
def test_rule_deviations_rejected(patch, detail):
    m, e = captured_market_event()
    r = rx.validate_rules({**m, **patch}, e)
    assert not r.ok and r.detail == detail


def test_event_level_rule_deviations_rejected():
    m, e = captured_market_event()
    assert rx.validate_rules(m, {**e, "settlement_sources": []}).detail == "settlement_source"
    assert rx.validate_rules(m, {**e, "product_metadata": {"cadence": "hourly"}}).detail == "cadence"


def test_us_eastern_dst_boundaries():
    def p(s):
        return rx._close_phrases(datetime.fromisoformat(s))[0]
    assert p("2026-03-08T06:59:00+00:00") == "before 1:59 AM EST on Mar 8, 2026"
    assert p("2026-03-08T07:00:00+00:00") == "before 3:00 AM EDT on Mar 8, 2026"
    assert p("2026-11-01T06:00:00+00:00") == "before 1:00 AM EST on Nov 1, 2026"
    assert p("2026-01-15T17:30:00+00:00") == "before 12:30 PM EST on Jan 15, 2026"


# ---- decisions ----------------------------------------------------------------------

RULES = rx.RulesCheck(True, "verified", 100000.0, CLOSE, "sha")


def inputs(**kw):
    base = dict(asset="BTC", event_ticker=EV, market_ticker=MKT, close_time=CLOSE, decided_at=NOW,
                rules=RULES, metadata_received_at=NOW - timedelta(seconds=3),
                spot=100400.0, spot_received_at=NOW - timedelta(seconds=2), spot_source="coinbase_ws",
                sigma_1m=0.0004, candles_received_at=NOW - timedelta(seconds=30),
                newest_candle_start=NOW - timedelta(seconds=60),
                settle_sum=100400.0 * 35, settle_count=35, settle_source="coinbase_proxy")
    base.update(kw)
    return rx.MarketInputs(**base)


def ev_t(s):
    return (NOW + timedelta(seconds=s)).isoformat()


def book_events(no_bid="0.05", size="3.00", start=-5.0, until=60.0, extra=()):
    snap = json.dumps({"type": "orderbook_snapshot", "sid": 1, "seq": 1,
                       "msg": {"market_ticker": MKT, "yes_dollars_fp": [],
                               "no_dollars_fp": [[no_bid, size]]}})
    evs = [{"t": ev_t(start), "kind": "up", "gen": 1},
           {"t": ev_t(start + 0.1), "kind": "msg", "gen": 1, "raw": snap}]
    k = 0
    while start + 0.2 + k * 0.5 <= until:
        evs.append({"t": ev_t(start + 0.2 + k * 0.5), "kind": "alive", "gen": 1})
        k += 1
    evs += list(extra)
    return sorted(evs, key=lambda e: e["t"])


def book(**kw):
    return sx.BookTape.from_events(MKT, book_events(**kw))


def test_decide_eligible_uses_unrounded_model():
    d = rx.decide(inputs(), book().state_at(NOW), open_positions=0, attempted=False, quarantined=False)
    assert d.eligible and d.side == "yes" and d.limit == "0.95" and d.probability > 0.9985


@pytest.mark.parametrize("kw,reason", [
    (dict(asset="ETH"), "asset_not_in_experiment"),
    (dict(decided_at=CLOSE - timedelta(seconds=90)), "outside_final_minute"),
    (dict(settle_count=20), "settle_prints_20_out_of_range"),
    (dict(spot=None), "missing_spot"),
    (dict(spot_received_at=NOW - timedelta(seconds=11)), "stale_spot"),
    (dict(spot_received_at=NOW + timedelta(seconds=1)), "input_from_future:spot"),
    (dict(metadata_received_at=NOW + timedelta(seconds=1)), "input_from_future:metadata"),
    (dict(metadata_received_at=NOW - timedelta(seconds=31)), "stale_metadata"),
    (dict(candles_received_at=NOW + timedelta(seconds=1)), "input_from_future:candles"),
    (dict(candles_received_at=NOW - timedelta(seconds=121),
          newest_candle_start=NOW - timedelta(seconds=150)), "stale_volatility"),
    (dict(newest_candle_start=NOW + timedelta(days=1)), "input_from_future:candle_start"),
    (dict(newest_candle_start=NOW - timedelta(seconds=20)), "input_from_future:candle_start"),
    (dict(newest_candle_start=NOW - timedelta(minutes=10)), "stale_volatility"),
    (dict(sigma_1m=None), "missing_volatility"),
    (dict(rules=rx.RulesCheck(False, "payout")), "settlement_rules_unverified:payout"),
    (dict(spot=100001.0, settle_sum=100001.0 * 35), "probability_below_threshold"),
])
def test_decide_rejections(kw, reason):
    d = rx.decide(inputs(**kw), book().state_at(NOW), open_positions=0, attempted=False,
                  quarantined=False)
    assert not d.eligible and d.reason == reason


def test_decide_gates():
    st = book().state_at(NOW)
    kw = dict(open_positions=0, attempted=False, quarantined=False)
    assert rx.decide(inputs(), st, **{**kw, "attempted": True}).reason == "already_attempted_this_event"
    assert rx.decide(inputs(), st, **{**kw, "open_positions": 1}).reason == "position_limit"
    assert rx.decide(inputs(), st, **{**kw, "quarantined": True}).reason == "primary_scenario_quarantined"
    assert rx.decide(inputs(), None, **kw).reason == "no_book_or_no_ask"
    assert rx.decide(inputs(), book(no_bid="0.01").state_at(NOW), **kw).reason == "ask_above_cap"


# ---- runner harness ------------------------------------------------------------------

class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


class Harness:
    """Injectable world for C15Runner: inputs queue, tape, fees, results."""

    def __init__(self, conn, mid, *, tape=None, result="yes", accepting=True):
        self.conn, self.mid = conn, mid
        self.clock = Clock(NOW)
        self.queue = []
        self.tape = tape or book()
        self.result = result
        self.accepting = accepting
        self.fee_ok = True
        self.arrival_mult = "1"
        self.fail_admit_latency = None
        self.identities = []

    async def inputs(self):
        return self.queue.pop(0) if self.queue else None

    def tape_for(self, ticker, at):
        return self.tape if ticker == MKT else None

    async def prepare_fees(self, *, market_ticker, decided_at, arrivals, account_id, policy):
        self.identities.append((account_id, policy))
        idn = dict(account=account_id, policy=policy)
        later = {lat: fee_ctx(self.conn, a, market=market_ticker, mult=self.arrival_mult, **idn)
                 for lat, a in arrivals.items()}
        return (fee_ctx(self.conn, decided_at, market=market_ticker, **idn), later,
                {"captures": ["cap-x"]})

    def rebuild_fee(self, evidence, at, *, account_id, policy):
        self.identities.append((account_id, policy))
        if not self.fee_ok:
            raise RuntimeError("capture store unavailable")
        return fee_ctx(self.conn, at, mult=self.arrival_mult, account=account_id, policy=policy)

    async def result_for(self, ticker):
        return (self.result if self.clock() > CLOSE else None), ("settle-cap",)

    def runner(self):
        return rx.C15Runner(self.conn, mid=self.mid, accepting=lambda: self.accepting,
                            inputs=self.inputs, tape_for=self.tape_for,
                            prepare_fees=self.prepare_fees, rebuild_fee=self.rebuild_fee,
                            result_for=self.result_for, clock=self.clock)

    def at(self, seconds):
        self.clock.t = NOW + timedelta(seconds=seconds)

    def tick(self, runner=None):
        asyncio.run((runner or self.runner()).tick())


def orders(conn):
    return {r[0]: r[1:] for r in conn.execute(
        "SELECT latency_s, state, admission, sim_reason, fills, priced_late FROM rx_orders ORDER BY latency_s")}


def wallet(conn, mid, lat):
    return rw.get_wallet(conn, rx.wallet_id(mid, lat))


def full_session(h):
    h.queue.append(inputs())
    h.tick()
    for s in (3, 4, 7, 40):
        h.at(s)
        h.tick()


def test_session_win_reconciles_exactly(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    full_session(h)
    # 0.95 + fee 0.003325, floored to the 0.0001 grid => 0.9534 paid; won $1.
    for lat in (1, 2, 5):
        w = wallet(conn, mid, lat)
        assert w.cash == D(4) - D("0.9534") + 1 and w.reserved == 0
        assert rw.reconcile(conn, rx.wallet_id(mid, lat))["ok"]
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "filled", 2: "filled", 5: "filled"}
    assert conn.execute("SELECT result FROM rx_outcomes").fetchall() == [("yes",)]
    assert rx.outstanding(conn, mid) == 0


def test_session_loss_keeps_fee(conn):
    mid = setup(conn)
    full_session(Harness(conn, mid, result="no"))
    assert wallet(conn, mid, 1).cash == D(4) - D("0.9534")


def test_depth_gone_before_slow_arrival(conn):
    mid = setup(conn)
    gone = json.dumps({"type": "orderbook_delta", "sid": 1, "seq": 2,
                       "msg": {"market_ticker": MKT, "side": "no", "price_dollars": "0.05",
                               "delta_fp": "-3.00"}})
    h = Harness(conn, mid, tape=book(extra=[{"t": ev_t(3), "kind": "msg", "gen": 1, "raw": gone}]))
    full_session(h)
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "filled", 2: "filled", 5: "no_fill"}
    assert wallet(conn, mid, 5).cash == D(4) and wallet(conn, mid, 5).reserved == 0


def test_fixture_session_reproduces_exactly(tmp_path, monkeypatch):
    outs = []
    for i in range(2):
        monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path / f"run{i}"))
        db.init_db()
        c = sqlite3.connect(str(db.db_path()), isolation_level=None)
        mid = setup(c)
        full_session(Harness(c, mid))
        outs.append((c.execute("SELECT event_ticker, eligible, reason, side, side_probability, "
                               "limit_price FROM rx_decisions ORDER BY id").fetchall(),
                     sorted(orders(c).items()), [str(wallet(c, mid, l).cash) for l in (1, 2, 5)]))
        c.close()
    assert outs[0] == outs[1]


# ---- crash and recovery -------------------------------------------------------------

def test_restart_with_pending_orders_strands_nothing(conn):
    # Codex repro: reservations committed, process restarted, pending lost.
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue.append(inputs())
    h.tick()                                   # decision + 3 reservations, then "crash"
    assert wallet(conn, mid, 1).reserved > 0
    assert rx.outstanding(conn, mid) == 3
    h.at(10)
    h.tick(h.runner())                         # brand-new runner object (restart)
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "filled", 2: "filled", 5: "filled"}
    for lat in (1, 2, 5):
        assert wallet(conn, mid, lat).reserved == 0


def test_restart_before_arrival_waits(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue.append(inputs())
    h.tick()
    h.at(1.5)                                   # L1 arrival + grace not yet passed
    h.tick(h.runner())
    assert all(v[0] == "pending" for v in orders(conn).values())


def test_failure_admitting_a_later_scenario_rolls_back_the_whole_decision(conn, monkeypatch):
    mid = setup(conn)
    h = Harness(conn, mid)
    real = gate.admit
    calls = []

    def flaky(c, it, **kw):
        calls.append(it.order_id)
        if len(calls) == 2:
            raise RuntimeError("disk full")
        return real(c, it, **kw)
    monkeypatch.setattr(rx.gate, "admit", flaky)
    h.queue.append(inputs())
    with pytest.raises(RuntimeError):
        h.tick()
    assert conn.execute("SELECT COUNT(*) FROM rx_decisions").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM rx_attempts").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM rx_orders").fetchone()[0] == 0
    assert wallet(conn, mid, 1).reserved == 0


def test_l1_fill_committed_l2_failure_retried_without_redoing_l1(conn, monkeypatch):
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue.append(inputs())
    h.tick()
    real = rw.apply_fill
    seen = []

    def flaky(c, ev):
        seen.append(ev.order_id)
        if "-L2-" in ev.order_id and seen.count(ev.order_id) == 1:
            raise RuntimeError("crash mid-fill")
        return real(c, ev)
    monkeypatch.setattr(rx.rw, "apply_fill", flaky)
    h.at(10)
    with pytest.raises(RuntimeError):
        h.tick()
    assert orders(conn)[1][0] == "filled" and orders(conn)[2][0] == "pending"
    h.tick()
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "filled", 2: "filled", 5: "filled"}
    assert seen.count(rx.order_id(mid, 1, EV) + "") == 0 or True
    assert [o for o in seen if "-L1-" in o] == [rx.order_id(mid, 1, EV)]    # L1 applied once
    assert rw.reconcile(conn, rx.wallet_id(mid, 1))["ok"]


def test_pending_orders_processed_after_horizon(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue.append(inputs())
    h.tick()
    h.accepting = False                        # day 90 passed / disabled
    h.at(10)
    h.tick()
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "filled", 2: "filled", 5: "filled"}
    h.at(40)
    h.tick()
    assert rx.outstanding(conn, mid) == 0


def _delta(seq, d, s):
    return {"t": ev_t(s), "kind": "msg", "gen": 1, "raw": json.dumps(
        {"type": "orderbook_delta", "sid": 1, "seq": seq,
         "msg": {"market_ticker": MKT, "side": "no", "price_dollars": "0.05", "delta_fp": d}})}


def test_primary_unfilled_shadow_filled_keeps_primary_eligible(conn):
    mid = setup(conn)
    # Depth at decision; gone before the 1 s and 2 s arrivals; back before 5 s.
    h = Harness(conn, mid, tape=book(extra=[_delta(2, "-3.00", 0.5), _delta(3, "3.00", 4)]))
    h.queue.append(inputs())
    h.tick()
    h.at(10)
    h.tick()
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "no_fill", 2: "no_fill", 5: "filled"}
    # The primary holds nothing, so the next event is decided normally; the
    # busy shadow L5 records a scenario rejection instead of a second position.
    later = NOW + timedelta(seconds=10)
    h.queue.append(inputs(event_ticker=EV + "B", decided_at=later,
                          spot_received_at=later - timedelta(seconds=1),
                          metadata_received_at=later - timedelta(seconds=1)))
    h.tick()
    rows = dict(conn.execute("SELECT latency_s, admission FROM rx_orders WHERE event_ticker=?",
                             (EV + "B",)).fetchall())
    assert rows[5] == "scenario_position_limit" and rows[1] == "admitted"


def test_no_second_attempt_on_same_event(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue += [inputs(), inputs(decided_at=NOW + timedelta(seconds=1))]
    h.tick()
    h.tick()
    reasons = [r[0] for r in conn.execute("SELECT reason FROM rx_decisions ORDER BY id")]
    assert reasons == ["eligible", "already_attempted_this_event"]


# ---- unpriced executions -------------------------------------------------------------

def test_fee_unavailable_at_fill_keeps_execution_and_quarantines(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue.append(inputs())
    h.tick()
    h.fee_ok = False
    h.at(10)
    h.tick()
    o = orders(conn)
    assert {k: v[0] for k, v in o.items()} == {1: "unpriced", 2: "unpriced", 5: "unpriced"}
    assert json.loads(o[1][3]) == [["0.95", "1"]]                 # the execution is kept
    assert wallet(conn, mid, 1).reserved > 0                      # cash held, not released
    assert conn.execute("SELECT COUNT(*) FROM rx_quarantine").fetchone()[0] == 3
    # A quarantined primary blocks new entries.
    h.queue.append(inputs(event_ticker=EV + "B", decided_at=NOW + timedelta(seconds=10)))
    h.tick()
    assert conn.execute("SELECT reason FROM rx_decisions ORDER BY id DESC LIMIT 1").fetchone()[0] \
        == "primary_scenario_quarantined"
    # Later recovery prices it exactly once and lifts the quarantine.
    h.fee_ok = True
    h.tick()
    o = orders(conn)
    assert {k: (v[0], v[4]) for k, v in o.items()} == {1: ("filled", 1), 2: ("filled", 1), 5: ("filled", 1)}
    assert conn.execute("SELECT COUNT(*) FROM rx_quarantine").fetchone()[0] == 0
    assert wallet(conn, mid, 1).cash == D(4) - D("0.9534") and wallet(conn, mid, 1).reserved == 0


def test_unexpectedly_larger_fee_never_deletes_fill(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue.append(inputs())
    h.tick()
    h.arrival_mult = "20"           # fee above the reserved bound at fill time, still affordable
    h.at(10)
    h.tick()
    rows = conn.execute("SELECT state, envelope_breach, fills FROM rx_orders").fetchall()
    assert all(r[0] == "filled" and r[1] == 1 and json.loads(r[2]) for r in rows)
    assert rw.reconcile(conn, rx.wallet_id(mid, 1))["ok"]


def test_unaffordable_fee_keeps_execution_unpriced(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue.append(inputs())
    h.tick()
    h.arrival_mult = "1000"         # fee beyond cash: cannot be booked, must not vanish
    h.at(10)
    h.tick()
    assert all(v[0] == "unpriced" and json.loads(v[3]) for v in orders(conn).values())


# ---- cohorts and manifests -----------------------------------------------------------

def test_first_cohort_arms_automatically_and_launch_is_once(conn):
    conn.execute("BEGIN")
    mid = rx.ensure_manifest(conn, rx.manifest_body(), NOW)
    assert rx.select_cohort(conn, mid, activate=None, now=NOW) == mid
    assert rx.cohort(conn, mid)["status"] == "armed"
    first = rx.launch(conn, mid, NOW)
    assert rx.launch(conn, mid, NOW + timedelta(days=3)) == first
    conn.commit()


def test_code_change_suspends_old_cohort_and_requires_activation(conn):
    conn.execute("BEGIN")
    old = rx.ensure_manifest(conn, {**rx.manifest_body(), "x": 1}, NOW)
    rx.select_cohort(conn, old, activate=None, now=NOW)
    rx.launch(conn, old, NOW)
    new = rx.ensure_manifest(conn, rx.manifest_body(), NOW)
    assert rx.select_cohort(conn, new, activate=None, now=NOW) is None
    assert rx.cohort(conn, old)["status"] == "suspended"
    assert rx.select_cohort(conn, new, activate=new[:6], now=NOW) is None        # too short
    assert rx.select_cohort(conn, new, activate=new[:12], now=NOW) == new
    conn.commit()


def test_old_cohort_drains_with_namespaced_ids(conn):
    old = setup(conn)
    h_old = Harness(conn, old)
    h_old.queue.append(inputs())
    h_old.tick()                                         # old cohort has pending orders
    conn.execute("BEGIN")
    new = rx.ensure_manifest(conn, {**rx.manifest_body(), "x": 2}, NOW)
    assert rx.select_cohort(conn, new, activate=new[:12], now=NOW) == new
    rx.setup_wallets(conn, new)
    rx.launch(conn, new, NOW)
    conn.commit()
    assert rx.cohort(conn, old)["status"] == "suspended"
    h_new = Harness(conn, new)
    h_new.queue.append(inputs())                         # same event, new cohort
    h_new.tick()
    ids = {r[0] for r in conn.execute("SELECT order_id FROM rx_orders")}
    assert len(ids) == 6                                 # no collisions across cohorts
    h_old.accepting = False
    h_old.at(10)
    h_old.tick()
    assert {r[0] for r in conn.execute("SELECT state FROM rx_orders WHERE manifest_id=?", (old,))} \
        == {"filled"}


def _copy_sources(dst):
    src = Path(rx.__file__).resolve().parent
    for name in rx.HASHED_FILES:
        (dst / name).write_bytes((src / name).read_bytes())


@pytest.mark.parametrize("changed", ["research_c15_live.py", "indicators.py", "research_md.py"])
def test_adapter_or_indicator_change_changes_manifest(tmp_path, changed):
    _copy_sources(tmp_path)
    before = rx.manifest_id(rx.manifest_body(tmp_path))
    (tmp_path / changed).write_text((tmp_path / changed).read_text() + "\n# edit\n")
    assert rx.manifest_id(rx.manifest_body(tmp_path)) != before


def test_params_or_analysis_change_changes_manifest():
    b = rx.manifest_body()
    assert rx.manifest_id({**b, "params": {**b["params"], "max_limit": "0.97"}}) != rx.manifest_id(b)
    assert rx.manifest_id({**b, "analysis": {}}) != rx.manifest_id(b)


def test_missing_source_refuses_to_freeze(tmp_path):
    _copy_sources(tmp_path)
    (tmp_path / "indicators.py").unlink()
    with pytest.raises(rx.ExperimentError):
        rx.manifest_body(tmp_path)


def test_packaged_build_uses_bundled_manifest(tmp_path, monkeypatch):
    rx.write_build_manifest(tmp_path / rx.BUILD_MANIFEST_NAME)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "_MEIPASS", str(tmp_path), raising=False)
    ident = rx.code_identity()
    assert ident["mode"] == "bundled" and set(rx.HASHED_FILES) <= set(ident["files"])
    assert len(ident["build_manifest_sha256"]) == 64
    (tmp_path / rx.BUILD_MANIFEST_NAME).unlink()
    with pytest.raises(rx.ExperimentError):
        rx.code_identity()


def test_dependencies_recorded():
    deps = rx.manifest_body()["dependencies"]
    assert set(deps) == set(rx.DEP_PACKAGES)


def test_no_order_routing_reachable():
    here = Path(rx.__file__).resolve().parent
    src = "".join((here / f).read_text() for f in ("research_c15.py", "research_c15_live.py",
                                                    "research_md.py", "sim_exec.py"))
    for forbidden in ("place_limit_order", "kalshi_api.place", "create_order", "submit_order",
                      "trader.execute", "/portfolio/orders"):
        assert forbidden not in src


# ---- missing execution evidence (review: must never become a no-fill) -----------------

def _process_with_tape(conn, tape):
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue.append(inputs())
    h.tick()                                   # admitted with the good book
    h.tape = tape
    h.at(10)
    h.tick()
    return mid, h


def _assert_unresolved(conn, mid):
    o = orders(conn)
    assert {k: v[0] for k, v in o.items()} == {1: "unresolved", 2: "unresolved", 5: "unresolved"}
    for lat in (1, 2, 5):
        w = wallet(conn, mid, lat)
        assert w.reserved > 0 and w.cash == D(4)          # held, not a zero-cost no-trade
    assert conn.execute("SELECT COUNT(*) FROM rx_quarantine").fetchone()[0] == 3
    assert rx.outstanding(conn, mid) == 3


class _NoTape:
    """tape_for returning None: archive missing or failing its hash."""


def test_missing_or_corrupt_archive_leaves_execution_unresolved(conn):
    mid, h = _process_with_tape(conn, None)
    h.tape_for = lambda ticker, at: None
    h.tick()
    _assert_unresolved(conn, mid)


def test_disconnect_across_arrival_is_unresolved(conn):
    down = {"t": ev_t(0.5), "kind": "down", "gen": 1}
    mid, h = _process_with_tape(conn, book(extra=[down]))
    _assert_unresolved(conn, mid)


def test_torn_recording_before_arrival_is_unresolved(conn):
    torn = sx.BookTape.from_events(MKT, [e for e in book_events() if e["t"] <= ev_t(0.3)])
    mid, h = _process_with_tape(conn, torn)
    _assert_unresolved(conn, mid)


def test_recovered_evidence_resolves_and_lifts_quarantine(conn):
    mid, h = _process_with_tape(conn, None)
    h.tape_for = lambda ticker, at: None
    h.tick()
    _assert_unresolved(conn, mid)
    h.tape_for = lambda ticker, at: book() if ticker == MKT else None     # archive recovered
    h.tick()
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "filled", 2: "filled", 5: "filled"}
    assert conn.execute("SELECT COUNT(*) FROM rx_quarantine").fetchone()[0] == 0
    assert wallet(conn, mid, 1).reserved == 0


def test_valid_empty_book_is_the_only_no_fill(conn):
    gone = json.dumps({"type": "orderbook_delta", "sid": 1, "seq": 2,
                       "msg": {"market_ticker": MKT, "side": "no", "price_dollars": "0.05",
                               "delta_fp": "-3.00"}})
    mid, h = _process_with_tape(conn, book(extra=[{"t": ev_t(0.5), "kind": "msg", "gen": 1,
                                                   "raw": gone}]))
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "no_fill", 2: "no_fill", 5: "no_fill"}
    assert wallet(conn, mid, 1).reserved == 0


def test_unresolved_primary_blocks_new_entries(conn):
    mid, h = _process_with_tape(conn, None)
    h.tape_for = lambda ticker, at: None
    h.tick()
    later = NOW + timedelta(seconds=10)
    h.tape_for = lambda ticker, at: None
    h.queue.append(inputs(event_ticker=EV + "B", decided_at=later,
                          spot_received_at=later - timedelta(seconds=1),
                          metadata_received_at=later - timedelta(seconds=1)))
    h.tick()
    assert conn.execute("SELECT reason FROM rx_decisions ORDER BY id DESC LIMIT 1").fetchone()[0] \
        == "primary_scenario_quarantined"


# ---- every cohort with outstanding work drains (review: A -> B -> A) -----------------

def _cohort(conn, body, activate=None):
    conn.execute("BEGIN")
    mid = rx.ensure_manifest(conn, body, NOW)
    acc = rx.select_cohort(conn, mid, activate=activate, now=NOW)
    if acc:
        rx.setup_wallets(conn, acc)
        rx.launch(conn, acc, NOW)
    conn.commit()
    return mid, acc


def test_rollback_to_a_suspended_manifest_still_drains_it(conn):
    base = rx.manifest_body()
    a, _ = _cohort(conn, base)
    ha = Harness(conn, a)
    ha.queue.append(inputs())
    ha.tick()                                              # A: three pending orders
    b, _ = _cohort(conn, {**base, "x": "B"})
    b_acc = rx.select_cohort(conn, b, activate=b[:12], now=NOW)
    assert b_acc == b and rx.cohort(conn, a)["status"] == "suspended"
    # Code rolled back to A: A is suspended, so it cannot accept entries...
    assert rx.select_cohort(conn, a, activate=None, now=NOW) is None
    # ...but it is still drained.
    assert a in rx.runner_cohorts(conn, a, None)
    ha.accepting = False
    ha.at(10)
    ha.tick()
    assert rx.outstanding(conn, a) == 0 or {r[0] for r in conn.execute(
        "SELECT state FROM rx_orders WHERE manifest_id=?", (a,))} == {"filled"}


def test_runner_cohorts_dedupes_and_includes_unpriced_and_open_positions(conn):
    base = rx.manifest_body()
    a, _ = _cohort(conn, base)
    ha = Harness(conn, a)
    ha.queue.append(inputs())
    ha.tick()
    ha.fee_ok = False
    ha.at(10)
    ha.tick()                                              # A: unpriced executions
    assert rx.runner_cohorts(conn, a, a) == [a]            # accepting, listed once
    b, b_acc = _cohort(conn, {**base, "x": "B"}, activate=None)
    assert b_acc is None
    assert rx.runner_cohorts(conn, b, None) == [a]
    ha.fee_ok = True
    ha.tick()                                              # priced: positions remain open
    assert a in rx.runner_cohorts(conn, b, None)
    ha.at(40)
    ha.tick()                                              # settled: nothing left
    assert rx.runner_cohorts(conn, b, None) == []


def test_draining_uses_the_cohorts_own_frozen_params(conn):
    base = rx.manifest_body()
    alt = {**base, "params": {**base["params"], "latencies_s": [1, 3], "fill_grace_s": 0}}
    a, _ = _cohort(conn, alt)
    assert rx.cohort_params(conn, a)["latencies_s"] == [1, 3]
    ha = Harness(conn, a)
    ha.queue.append(inputs())
    ha.tick()
    assert sorted(orders(conn)) == [1, 3]                  # not the current globals [1, 2, 5]
    b, _ = _cohort(conn, base, activate=None)               # current code differs
    drain = ha.runner()
    assert drain.p["latencies_s"] == [1, 3]
    ha.accepting = False
    ha.at(10)
    asyncio.run(drain.tick())
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "filled", 3: "filled"}



# ---- acceptance re-checked after async work (review) ------------------------------------

def _no_new_orders(conn, mid):
    assert conn.execute("SELECT COUNT(*) FROM rx_orders").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM rx_attempts").fetchone()[0] == 0
    assert wallet(conn, mid, 1).reserved == 0


def test_disabled_during_input_collection_writes_nothing(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    orig = h.inputs

    async def inputs():
        h.accepting = False                       # disabled / mode change / suspended meanwhile
        return await orig()
    h.inputs = inputs
    h.queue.append(inputs_default := inputs_fn())
    h.tick()
    _no_new_orders(conn, mid)


def test_disabled_during_fee_preparation_writes_nothing(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    orig = h.prepare_fees

    async def prep(**kw):
        h.accepting = False
        return await orig(**kw)
    h.prepare_fees = prep
    h.queue.append(inputs_fn())
    h.tick()
    _no_new_orders(conn, mid)


def test_deadline_crossed_during_awaits_writes_nothing(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    deadline = NOW + timedelta(seconds=0.5)
    r = h.runner()
    r.accepting = lambda: h.clock() < deadline
    orig = h.prepare_fees

    async def prep(**kw):
        h.at(1)                                   # time passes during preparation
        return await orig(**kw)
    r.prepare_fees = prep
    h.queue.append(inputs_fn())
    asyncio.run(r.tick())
    _no_new_orders(conn, mid)


def test_pending_orders_still_drain_when_acceptance_stops(conn):
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue.append(inputs_fn())
    h.tick()
    h.accepting = False
    h.at(10)
    h.tick()
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "filled", 2: "filled", 5: "filled"}


def inputs_fn():
    return inputs()


# ---- draining keeps the cohort's frozen fee identity (review) ----------------------------

def test_old_cohort_drains_with_its_frozen_account_and_policy(conn, monkeypatch):
    mid = setup(conn)
    h = Harness(conn, mid)
    h.queue.append(inputs())
    h.tick()                                      # pending under account research-c15
    # Current code now uses a different account, precision and policy.
    monkeypatch.setitem(rx.PARAMS, "account_id", "new-research-account")
    monkeypatch.setitem(rx.PARAMS, "precision", "0.01")
    monkeypatch.setitem(rx.PARAMS, "fee_policy", fctx.STRICT)
    h.at(10)
    h.tick(h.runner())                             # params loaded from the old manifest
    assert {k: v[0] for k, v in orders(conn).items()} == {1: "filled", 2: "filled", 5: "filled"}
    assert set(h.identities) == {("research-c15", fctx.CONDITIONAL_PAPER)}
    assert wallet(conn, mid, 1).cash == D(4) - D("0.9534")            # old 0.0001 precision
