"""Tests for execution_gate.py and research_c15.py (fixture sessions).
Stdlib-only: python -m pytest tests/test_research_c15.py --noconftest
"""
from __future__ import annotations

import asyncio
import sqlite3
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from fractions import Fraction as F
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# crypto15m (the frozen model) imports network libraries at module load.
# Where they are not installed (offline sandboxes), stub them: these tests
# never touch the network. With the real packages installed, nothing changes.
import importlib.util  # noqa: E402
import types  # noqa: E402
for _name in ("httpx", "websockets", "httpx2"):
    if importlib.util.find_spec(_name) is None and _name not in sys.modules:
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
NOW = CLOSE - timedelta(seconds=25)          # inside the final minute
MKT, EV = "KXBTC15M-26OCT301215-00", "KXBTC15M-26OCT301215"


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    db.init_db()
    c = sqlite3.connect(str(db.db_path()))
    c.execute("PRAGMA foreign_keys=ON")
    yield c
    c.close()


def fee_ctx(conn, at, *, market=MKT, mult="1"):
    J = fs.loads
    a = fs.AsOf(at - timedelta(seconds=5), at)
    return fctx.build_fee_context(
        conn, kalshi_env="paper", account_id=rx.PARAMS["account_id"], policy=fctx.CONDITIONAL_PAPER,
        role="taker", at=at, ttl=timedelta(seconds=30), evidence=("cap-x",),
        market=fs.parse_market({"ticker": market, "event_ticker": EV}, a),
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
    rx.setup_wallets(conn, mid)
    conn.commit()
    return mid


# ---- gate ------------------------------------------------------------------------

def test_fee_bound_math():
    b = gate.fee_bound(count=F(1), limit=F(96, 100), coefficient=F(7, 100), precision=F(1, 10**4))
    # ceil_micro(0.07*0.96*0.04 = 0.002688) + 100 fragments * (0.0001 + 0.000001)
    assert b == F(2688, 10**6) + 100 * F(101, 10**6)


def intent(conn, mid, *, limit="0.95", prob=0.9995, at=NOW, ctx=None, count="1"):
    return gate.ExecutionIntent(
        wallet_id=rx.wallet_id(mid, 1), order_id=f"{EV}-L1", ticker=MKT, side="yes", action="buy",
        order_type="ioc", role="taker", limit=limit, count=count, side_probability=prob,
        decided_at=at, fee_context=ctx or fee_ctx(conn, at))


def admit(conn, it):
    conn.execute("BEGIN") if not conn.in_transaction else None
    return gate.admit(conn, it, min_net_edge_per_contract="0.02", max_limit="0.98",
                      policy=fctx.CONDITIONAL_PAPER)


def test_admits_and_reserves(conn):
    mid = setup(conn)
    a = admit(conn, intent(conn, mid))
    assert a.admitted and a.reserve_amount == D("0.95") + a.fee_bound
    assert rw.get_wallet(conn, rx.wallet_id(mid, 1)).reserved == a.reserve_amount


def test_original_slippage_repro_rejected(conn):
    # A 5-cent gate passed at the signal price used to submit at a worse
    # price; now the edge is recomputed at the actual limit.
    mid = setup(conn)
    a = admit(conn, intent(conn, mid, limit="0.97", prob=0.9985))
    assert not a.admitted and a.reason == "net_edge_below_minimum"
    assert rw.get_wallet(conn, rx.wallet_id(mid, 1)).reserved == 0


@pytest.mark.parametrize("kw,reason", [
    (dict(limit="0.99"), "limit_above_cap"),
    (dict(prob=0.5), "net_edge_below_minimum"),
    (dict(count="0.001"), "bad_input"),
])
def test_rejections(conn, kw, reason):
    mid = setup(conn)
    a = admit(conn, intent(conn, mid, **kw))
    assert not a.admitted and a.reason.startswith(reason)


def test_stale_or_foreign_context_rejected(conn):
    mid = setup(conn)
    later = NOW + timedelta(seconds=1)
    assert admit(conn, intent(conn, mid, ctx=fee_ctx(conn, NOW), at=later)).reason \
        == "fee_context_not_for_this_instant"
    assert admit(conn, intent(conn, mid, ctx=fee_ctx(conn, NOW, market="OTHER-1"))).reason \
        == "fee_context_identity_mismatch"
    unusable = replace(fee_ctx(conn, NOW), usable=False, reason="UNAVAILABLE:stale")
    assert admit(conn, intent(conn, mid, ctx=unusable)).reason.startswith("fee_context_unusable")


def test_zero_fee_market_still_bounded_by_rounding(conn):
    mid = setup(conn)
    a = admit(conn, intent(conn, mid, ctx=fee_ctx(conn, NOW, mult="0")))
    assert a.admitted and a.fee_bound == D("0.0101")


def test_insufficient_funds_rejected(conn):
    mid = setup(conn)
    a = admit(conn, intent(conn, mid, count="5"))
    assert not a.admitted and a.reason == "insufficient_funds"


def test_unsupported_shapes_rejected(conn):
    mid = setup(conn)
    it = replace(intent(conn, mid), order_type="gtc")
    assert admit(conn, it).reason == "unsupported_order_shape"


# ---- decisions ----------------------------------------------------------------------

def inputs(**kw):
    base = dict(asset="BTC", event_ticker=EV, market_ticker=MKT, close_time=CLOSE, now=NOW,
                strike=100000.0, spot=100400.0, spot_at=NOW - timedelta(seconds=2), sigma_1m=0.0004,
                settle_sum=100400.0 * 35, settle_count=35, settle_source="cf_index/spot:cf_index",
                rules_ok=True, rules_detail="ok")
    base.update(kw)
    return rx.MarketInputs(**base)


def book(no_bid="0.05", size="3.00", t=NOW - timedelta(seconds=5), seq=1):
    tp = sx.BookTape(MKT)
    tp.apply({"type": "orderbook_snapshot", "seq": seq,
              "msg": {"market_ticker": MKT, "yes_dollars_fp": [], "no_dollars_fp": [[no_bid, size]]}}, t)
    return tp


def test_decide_eligible_uses_unrounded_model():
    d = rx.decide(inputs(), book().state_at(NOW), open_positions=0, attempted=False)
    assert d.eligible and d.side == "yes" and d.limit == "0.95" and d.probability > 0.9985


@pytest.mark.parametrize("kw,reason", [
    (dict(asset="ETH"), "asset_not_in_experiment"),
    (dict(now=CLOSE - timedelta(seconds=90)), "outside_final_minute"),
    (dict(settle_count=20), "settle_prints_20_out_of_range"),
    (dict(settle_count=55), "settle_prints_55_out_of_range"),
    (dict(spot=None), "missing_inputs"),
    (dict(spot_at=NOW - timedelta(seconds=30)), "stale_spot"),
    (dict(rules_ok=False, rules_detail="no_floor_strike"), "settlement_rules_unverified"),
    (dict(spot=100001.0, settle_sum=100001.0 * 35), "probability_below_threshold"),
])
def test_decide_rejections(kw, reason):
    d = rx.decide(inputs(**kw), book().state_at(NOW), open_positions=0, attempted=False)
    assert not d.eligible and d.reason.startswith(reason)


def test_decide_one_attempt_and_one_position():
    st = book().state_at(NOW)
    assert rx.decide(inputs(), st, open_positions=0, attempted=True).reason == "already_attempted_this_event"
    assert rx.decide(inputs(), st, open_positions=1, attempted=False).reason == "position_limit"
    assert rx.decide(inputs(), None, open_positions=0, attempted=False).reason == "no_book_or_no_ask"
    assert rx.decide(inputs(), book(no_bid="0.01").state_at(NOW), open_positions=0,
                     attempted=False).reason == "ask_above_cap"


# ---- full fixture session ---------------------------------------------------------

class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def session(conn, *, result="yes", tape=None):
    mid = setup(conn)
    clock = Clock(NOW)
    tp = tape or book()
    seq = iter([inputs(), None, None, None, None, None, None, None])

    async def inp():
        return next(seq)

    async def ctx_for(*, market_ticker, role, at):
        return fee_ctx(conn, at, market=market_ticker)

    async def result_for(ticker):
        return (result if clock() > CLOSE else None), ("settle-cap",)

    r = rx.C15Runner(conn, mid=mid, inputs=inp, tape_for=lambda t: tp if t == MKT else None,
                     fee_context_for=ctx_for, result_for=result_for, clock=clock)

    async def go():
        await r.tick()                         # decide + admit all three latencies
        for dt in (2, 3, 6, 40):               # fills land as each latency elapses; then settle
            clock.t = NOW + timedelta(seconds=dt)
            await r.tick()
    asyncio.run(go())
    return mid, r


def wallet(conn, mid, lat):
    return rw.get_wallet(conn, rx.wallet_id(mid, lat))


def test_session_win_reconciles_exactly(conn):
    mid, r = session(conn)
    w1 = wallet(conn, mid, 1)
    # Paid 0.95 + exact fee for 1 contract at 0.95 (direct precision), won $1.
    # fee 0.07*0.95*0.05 = 0.003325; revenue - fee = -0.953325, floored to
    # the 0.0001 grid => -0.9534 (rounding fee 0.000075).
    assert w1.cash == D(4) - D("0.9534") + 1 and w1.reserved == 0
    for lat in (1, 2, 5):
        assert rw.reconcile(conn, rx.wallet_id(mid, lat))["ok"]
    rows = conn.execute("SELECT latency_s, admission, sim_reason, filled FROM rx_scenario_results "
                        "ORDER BY latency_s").fetchall()
    assert rows == [(1, "admitted", "filled", "1"), (2, "admitted", "filled", "1"),
                    (5, "admitted", "filled", "1")]
    assert conn.execute("SELECT result FROM rx_outcomes").fetchall() == [("yes",)]
    assert not r.open_markets


def test_session_loss_keeps_fee(conn):
    mid, _ = session(conn, result="no")
    assert wallet(conn, mid, 1).cash == D(4) - D("0.9534")


def test_depth_gone_before_slow_arrival(conn):
    tp = book(size="1.00")
    tp.apply({"type": "orderbook_delta", "seq": 2,
              "msg": {"market_ticker": MKT, "side": "no", "price_dollars": "0.05",
                      "delta_fp": "-1.00"}}, NOW + timedelta(seconds=3))
    mid, _ = session(conn, tape=tp)
    res = dict(conn.execute("SELECT latency_s, sim_reason FROM rx_scenario_results").fetchall())
    assert res == {1: "filled", 2: "filled", 5: "no_liquidity"}
    assert wallet(conn, mid, 5).cash == D(4) and wallet(conn, mid, 5).reserved == 0


def test_fixture_session_reproduces_exactly(tmp_path, monkeypatch):
    outs = []
    for i in range(2):
        monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path / f"run{i}"))
        db.init_db()
        c = sqlite3.connect(str(db.db_path()))
        mid, _ = session(c)
        outs.append((mid,
                     c.execute("SELECT event_ticker, eligible, reason, side, side_probability, "
                               "limit_price FROM rx_decisions ORDER BY id").fetchall(),
                     c.execute("SELECT latency_s, admission, sim_reason, filled FROM "
                               "rx_scenario_results ORDER BY latency_s").fetchall(),
                     [str(wallet(c, mid, lat).cash) for lat in (1, 2, 5)]))
        c.close()
    assert outs[0] == outs[1]


def test_restart_restores_open_positions_and_settles(conn):
    mid = setup(conn)
    clock = Clock(NOW)
    seq = iter([inputs()] + [None] * 5)

    async def inp():
        return next(seq)

    async def ctx_for(*, market_ticker, role, at):
        return fee_ctx(conn, at, market=market_ticker)

    async def result_for(t):
        return ("yes" if clock() > CLOSE else None), ("s",)

    tp = book()
    kw = dict(mid=mid, tape_for=lambda t: tp, fee_context_for=ctx_for, result_for=result_for,
              clock=clock)
    r1 = rx.C15Runner(conn, inputs=inp, **kw)

    async def go1():
        await r1.tick()
        clock.t = NOW + timedelta(seconds=6)
        await r1.tick()
    asyncio.run(go1())
    r2 = rx.C15Runner(conn, inputs=inp, **kw)       # process restarted
    r2.restore_open_markets()
    assert r2.open_markets == {MKT}
    clock.t = CLOSE + timedelta(seconds=30)
    asyncio.run(r2.tick())
    assert not r2.open_markets and rw.reconcile(conn, rx.wallet_id(mid, 1))["ok"]


def test_no_second_attempt_on_same_event(conn):
    mid = setup(conn)
    clock = Clock(NOW)
    seq = iter([inputs(), inputs(now=NOW + timedelta(seconds=1))])

    async def inp():
        return next(seq)

    async def ctx_for(*, market_ticker, role, at):
        return fee_ctx(conn, at, market=market_ticker)

    async def result_for(t):
        return None, ()

    r = rx.C15Runner(conn, mid=mid, inputs=inp, tape_for=lambda t: book(),
                     fee_context_for=ctx_for, result_for=result_for, clock=clock)

    async def go():
        await r.tick()
        await r.tick()
    asyncio.run(go())
    reasons = [x[0] for x in conn.execute("SELECT reason FROM rx_decisions ORDER BY id")]
    assert reasons == ["eligible", "already_attempted_this_event"]
    assert conn.execute("SELECT COUNT(*) FROM rx_attempts").fetchone()[0] == 1


def test_manifest_changes_with_code_or_params(tmp_path):
    b = rx.manifest_body()
    assert rx.manifest_id(b) == rx.manifest_id(rx.manifest_body())
    b2 = {**b, "params": {**b["params"], "min_side_probability": "0.998"}}
    assert rx.manifest_id(b2) != rx.manifest_id(b)
    (tmp_path / "fees.py").write_text("x")
    with pytest.raises(rx.ExperimentError):
        c = sqlite3.connect(":memory:")
        rx.init_schema(c)
        rx.ensure_manifest(c, rx.manifest_body(tmp_path), NOW)


def test_launch_timestamp_recorded_once(conn):
    mid = setup(conn)
    first = rx.launch(conn, mid, NOW)
    assert rx.launch(conn, mid, NOW + timedelta(days=3)) == first


def test_no_production_routing_possible():
    import research_c15_live
    src = Path(research_c15_live.__file__).read_text() + Path(rx.__file__).read_text()
    for forbidden in ("place_limit_order", "kalshi_api.place", "create_order", "submit_order"):
        assert forbidden not in src
