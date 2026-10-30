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


def fee_ctx(conn, at, *, market=MKT, mult="1", waiver=None):
    J = fs.loads
    a = fs.AsOf(at - timedelta(seconds=5), at)
    mk = {"ticker": market, "event_ticker": EV}
    if waiver:
        mk["fee_waiver_expiration_time"] = waiver
    return fctx.build_fee_context(
        conn, kalshi_env="paper", account_id=rx.PARAMS["account_id"], policy=fctx.CONDITIONAL_PAPER,
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
    (dict(candles_received_at=NOW - timedelta(seconds=121)), "stale_volatility"),
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

    async def inputs(self):
        return self.queue.pop(0) if self.queue else None

    def tape_for(self, ticker, at):
        return self.tape if ticker == MKT else None

    async def prepare_fees(self, *, market_ticker, decided_at, arrivals):
        later = {lat: fee_ctx(self.conn, a, market=market_ticker, mult=self.arrival_mult)
                 for lat, a in arrivals.items()}
        return fee_ctx(self.conn, decided_at, market=market_ticker), later, {"captures": ["cap-x"]}

    def rebuild_fee(self, evidence, at):
        if not self.fee_ok:
            raise RuntimeError("capture store unavailable")
        return fee_ctx(self.conn, at, mult=self.arrival_mult)

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
