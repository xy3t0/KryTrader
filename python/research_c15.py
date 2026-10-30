"""Frozen final-minute BTC 15-minute experiment (plan steps C2/C3 runner).

PAPER ONLY. Nothing here can place a real order: fills come from sim_exec,
money from an isolated research_wallet, fees from conditional-paper fee
contexts (which can never qualify as live-release evidence).

Frozen v1 rules (PARAMS): BTC only; one contract; one open position; at
most one entry attempt per event; $4 start, no reset/top-up; final minute
with 30..54 settlement prints; side probability >= 0.9985 from the existing
settlement-average model (crypto15m.settlement_up_prob, evaluated
UNROUNDED); net edge >= 2 cents/contract at the actual limit under the
conservative fee bound; limit <= 98 cents; taker IOC; hold to settlement.
Latency scenarios 1 s (primary), 2 s and 5 s each run their own shadow
wallet on the same decisions. Missing or stale inputs reject entry.

The manifest hashes parameters, code and dependency files. Any change is a
new manifest and a new cohort; launch records a timestamp once and the
cohort runs 90 consecutive UTC days from it.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import execution_gate as gate
import fee_context as fctx
import research_wallet as rw
import sim_exec

logger = logging.getLogger(__name__)

PARAMS = {
    "experiment": "c15-final-minute-btc",
    "version": 1,
    "asset": "BTC",
    "series": "KXBTC15M",
    "contracts_per_entry": "1",
    "max_open_positions": 1,
    "max_attempts_per_event": 1,
    "starting_cash": "4",
    "min_settle_prints": 30,
    "max_settle_prints": 54,
    "min_side_probability": "0.9985",
    "min_net_edge_per_contract": "0.02",
    "max_limit": "0.98",
    "order": "taker-ioc",
    "latencies_s": [1, 2, 5],
    "primary_latency_s": 1,
    "max_input_age_s": 15,
    "duration_days": 90,
    "fee_policy": fctx.CONDITIONAL_PAPER,
    "account_id": "research-c15",
    "precision": "0.0001",
    "precision_source": "configured",
    "settlement_source": "CF Benchmarks",
}

HASHED_FILES = ("fees.py", "fee_schedule.py", "fee_capture.py", "fee_ledger.py", "fee_context.py",
                "fee_service.py", "research_wallet.py", "execution_gate.py", "sim_exec.py",
                "research_c15.py", "crypto15m.py", "cf_ws.py", "spot_ws.py", "requirements.txt")

SCHEMA = """
CREATE TABLE IF NOT EXISTS rx_manifests (
    manifest_id TEXT PRIMARY KEY,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL,
    launched_at TEXT
);
CREATE TABLE IF NOT EXISTS rx_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id TEXT NOT NULL REFERENCES rx_manifests (manifest_id),
    event_ticker TEXT NOT NULL,
    market_ticker TEXT,
    decided_at TEXT NOT NULL,
    eligible INTEGER NOT NULL,
    reason TEXT NOT NULL,
    side TEXT,
    side_probability TEXT,
    limit_price TEXT,
    inputs TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rx_decisions_event ON rx_decisions (manifest_id, event_ticker);
CREATE TABLE IF NOT EXISTS rx_attempts (
    manifest_id TEXT NOT NULL,
    event_ticker TEXT NOT NULL,
    market_ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    decision_id INTEGER NOT NULL REFERENCES rx_decisions (id),
    PRIMARY KEY (manifest_id, event_ticker)     -- at most one attempt per event
);
CREATE TABLE IF NOT EXISTS rx_scenario_results (
    manifest_id TEXT NOT NULL,
    event_ticker TEXT NOT NULL,
    latency_s INTEGER NOT NULL,
    admission TEXT NOT NULL,
    sim_reason TEXT,
    filled TEXT,
    detail TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (manifest_id, event_ticker, latency_s)
);
CREATE TABLE IF NOT EXISTS rx_outcomes (
    manifest_id TEXT NOT NULL,
    market_ticker TEXT NOT NULL,
    result TEXT NOT NULL,
    evidence TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (manifest_id, market_ticker)
);
CREATE TABLE IF NOT EXISTS rx_downtime (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    reason TEXT NOT NULL
);
"""


class ExperimentError(RuntimeError):
    pass


def _now_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def init_schema(conn) -> None:
    conn.executescript(SCHEMA)


# ---- manifest ------------------------------------------------------------------

def manifest_body(base_dir: Optional[Path] = None) -> dict:
    base = base_dir or Path(__file__).resolve().parent
    hashes = {}
    for name in HASHED_FILES:
        p = base / name
        hashes[name] = hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else "missing"
    return {"params": PARAMS, "code_sha256": hashes, "sim_version": sim_exec.SIM_VERSION,
            "gate_version": gate.GATE_VERSION, "python": sys.version.split()[0]}


def manifest_id(body: dict) -> str:
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def ensure_manifest(conn, body: dict, now: datetime) -> str:
    mid = manifest_id(body)
    if "missing" in body["code_sha256"].values():
        raise ExperimentError("manifest refers to a missing file; refusing to freeze")
    conn.execute("INSERT OR IGNORE INTO rx_manifests (manifest_id, body, created_at) VALUES (?,?,?)",
                 (mid, json.dumps(body, sort_keys=True), _now_iso(now)))
    return mid


def launch(conn, mid: str, now: datetime) -> datetime:
    """Record the launch instant once; later calls return the original."""
    conn.execute("UPDATE rx_manifests SET launched_at=? WHERE manifest_id=? AND launched_at IS NULL",
                 (_now_iso(now), mid))
    r = conn.execute("SELECT launched_at FROM rx_manifests WHERE manifest_id=?", (mid,)).fetchone()
    if r is None:
        raise ExperimentError("unknown manifest")
    return datetime.fromisoformat(r[0])


def wallet_id(mid: str, latency_s: int) -> str:
    return f"c15-{mid[:16]}-L{latency_s}"


def setup_wallets(conn, mid: str) -> None:
    fctx.register_account(conn, kalshi_env="paper", account_id=PARAMS["account_id"],
                          precision=PARAMS["precision"], precision_source=PARAMS["precision_source"])
    for lat in PARAMS["latencies_s"]:
        wid = wallet_id(mid, lat)
        try:
            rw.get_wallet(conn, wid)
        except rw.WalletError:
            rw.create_wallet(conn, wallet_id=wid, account_id=PARAMS["account_id"],
                             policy=PARAMS["fee_policy"], starting_cash=PARAMS["starting_cash"])


# ---- decision ------------------------------------------------------------------

@dataclass(frozen=True)
class MarketInputs:
    """Everything a decision may use, each with its own receipt time."""
    asset: str
    event_ticker: str
    market_ticker: str
    close_time: datetime
    now: datetime                    # server-aligned clock at decision
    strike: Optional[float]
    spot: Optional[float]
    spot_at: Optional[datetime]
    sigma_1m: Optional[float]
    settle_sum: float
    settle_count: int
    settle_source: str               # "cf_index" | "coinbase_proxy" | "none"
    rules_ok: bool
    rules_detail: str


@dataclass(frozen=True)
class Decision:
    eligible: bool
    reason: str
    side: Optional[str] = None       # "yes" (up) | "no" (down)
    probability: Optional[float] = None
    limit: Optional[str] = None


def best_ask(state: Optional[sim_exec.BookState], side: str):
    if state is None:
        return None
    opp = state.no if side == "yes" else state.yes
    return min((1 - p for p, s in opp if s > 0), default=None)


def decide(inp: MarketInputs, book: Optional[sim_exec.BookState], *,
           open_positions: int, attempted: bool) -> Decision:
    import crypto15m  # the frozen baseline model
    p = PARAMS
    if inp.asset != p["asset"]:
        return Decision(False, "asset_not_in_experiment")
    if attempted:
        return Decision(False, "already_attempted_this_event")
    if open_positions >= p["max_open_positions"]:
        return Decision(False, "position_limit")
    if not inp.rules_ok:
        return Decision(False, f"settlement_rules_unverified:{inp.rules_detail}")
    mins_left = (inp.close_time - inp.now).total_seconds() / 60.0
    if not (0.0 < mins_left < 1.0):
        return Decision(False, "outside_final_minute")
    if not (p["min_settle_prints"] <= inp.settle_count <= p["max_settle_prints"]):
        return Decision(False, f"settle_prints_{inp.settle_count}_out_of_range")
    if None in (inp.strike, inp.spot, inp.spot_at, inp.sigma_1m):
        return Decision(False, "missing_inputs")
    if (inp.now - inp.spot_at).total_seconds() > p["max_input_age_s"]:
        return Decision(False, "stale_spot")
    up = crypto15m.settlement_up_prob(inp.spot, inp.strike, inp.sigma_1m, mins_left,
                                      partial_sum=inp.settle_sum, partial_count=inp.settle_count)
    if up is None or not math.isfinite(up):
        return Decision(False, "model_unavailable")
    side = "yes" if up >= 0.5 else "no"
    prob = up if side == "yes" else 1.0 - up
    from fractions import Fraction
    if Fraction(prob) < Fraction(p["min_side_probability"]):
        return Decision(False, "probability_below_threshold", side, prob)
    ask = best_ask(book, side)
    if ask is None:
        return Decision(False, "no_book_or_no_ask", side, prob)
    if ask > Fraction(p["max_limit"]):
        return Decision(False, "ask_above_cap", side, prob)
    import fees
    return Decision(True, "eligible", side, prob, str(fees.fraction_to_decimal(ask)))


# ---- runner -------------------------------------------------------------------

InputsProvider = Callable[[], Awaitable[Optional[MarketInputs]]]
ResultProvider = Callable[[str], Awaitable[tuple[Optional[str], tuple[str, ...]]]]


class C15Runner:
    """Drives the experiment. All I/O is injected so a fixture session can be
    replayed exactly: inputs, book tapes, fee contexts, results and clock."""

    def __init__(self, conn: sqlite3.Connection, *, mid: str, inputs: InputsProvider,
                 tape_for: Callable[[str], Optional[sim_exec.BookTape]],
                 fee_context_for: Callable[..., Awaitable[fctx.FeeContext]],
                 result_for: ResultProvider, clock: Callable[[], datetime]):
        self.conn, self.mid = conn, mid
        self.inputs, self.tape_for = inputs, tape_for
        self.fee_context_for, self.result_for, self.clock = fee_context_for, result_for, clock
        self.pending: list[dict] = []
        self.open_markets: set[str] = set()

    def _txn(self):
        if not self.conn.in_transaction:
            self.conn.execute("BEGIN IMMEDIATE")

    def _record_decision(self, inp: MarketInputs, d: Decision) -> int:
        cur = self.conn.execute(
            "INSERT INTO rx_decisions (manifest_id, event_ticker, market_ticker, decided_at, "
            "eligible, reason, side, side_probability, limit_price, inputs) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (self.mid, inp.event_ticker, inp.market_ticker, _now_iso(inp.now), int(d.eligible),
             d.reason, d.side, None if d.probability is None else repr(d.probability), d.limit,
             json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v)
                         for k, v in inp.__dict__.items()}, sort_keys=True)))
        return cur.lastrowid

    async def tick(self) -> None:
        """One evaluation: settle what closed, fill what arrived, decide anew."""
        await self._settle_due()
        await self._process_pending()
        inp = await self.inputs()
        if inp is None:
            return
        self._txn()
        try:
            attempted = self.conn.execute(
                "SELECT 1 FROM rx_attempts WHERE manifest_id=? AND event_ticker=?",
                (self.mid, inp.event_ticker)).fetchone() is not None
            tape = self.tape_for(inp.market_ticker)
            book = tape.state_at(inp.now) if tape else None
            d = decide(inp, book, open_positions=len(self.open_markets), attempted=attempted)
            if d.reason == "outside_final_minute":
                self.conn.commit()
                return
            did = self._record_decision(inp, d)
            if d.eligible:
                self.conn.execute("INSERT INTO rx_attempts VALUES (?,?,?,?,?,?)",
                                  (self.mid, inp.event_ticker, inp.market_ticker, d.side,
                                   _now_iso(inp.now), did))
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        if d.eligible:
            await self._admit_all(inp, d)

    async def _admit_all(self, inp: MarketInputs, d: Decision) -> None:
        ctx = await self.fee_context_for(market_ticker=inp.market_ticker, role="taker", at=inp.now)
        for lat in PARAMS["latencies_s"]:
            wid = wallet_id(self.mid, lat)
            order_id = f"{inp.event_ticker}-L{lat}"
            intent = gate.ExecutionIntent(
                wallet_id=wid, order_id=order_id, ticker=inp.market_ticker, side=d.side,
                action="buy", order_type="ioc", role="taker", limit=d.limit,
                count=PARAMS["contracts_per_entry"], side_probability=d.probability,
                decided_at=inp.now, fee_context=ctx)
            self._txn()
            try:
                adm = gate.admit(self.conn, intent,
                                 min_net_edge_per_contract=PARAMS["min_net_edge_per_contract"],
                                 max_limit=PARAMS["max_limit"], policy=PARAMS["fee_policy"])
                if not adm.admitted:
                    self._scenario(inp.event_ticker, lat, adm.reason, None, None,
                                   {"fee_bound": str(adm.fee_bound),
                                    "net_edge": str(adm.net_edge_per_contract)})
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise
            if adm.admitted:
                self.pending.append({"intent": intent, "latency": lat, "admission": adm})

    def _scenario(self, event, lat, admission, sim_reason, filled, detail) -> None:
        self.conn.execute("INSERT OR REPLACE INTO rx_scenario_results VALUES (?,?,?,?,?,?,?,?)",
                          (self.mid, event, lat, admission, sim_reason, filled,
                           json.dumps(detail, sort_keys=True, default=str), _now_iso(self.clock())))

    async def _process_pending(self) -> None:
        still = []
        for p in self.pending:
            it, lat = p["intent"], p["latency"]
            arrival = it.decided_at + timedelta(seconds=lat)
            if self.clock() < arrival + timedelta(milliseconds=500):
                still.append(p)
                continue
            await self._fill(it, lat, arrival, p["admission"])
        self.pending = still

    async def _fill(self, it: gate.ExecutionIntent, lat: int, arrival: datetime, adm) -> None:
        event = it.order_id.rsplit("-L", 1)[0]
        tape = self.tape_for(it.ticker)
        ctx = await self.fee_context_for(market_ticker=it.ticker, role="taker", at=arrival)
        self._txn()
        try:
            ledger = sim_exec.LiquidityLedger(self.conn, scope=f"{self.mid}:L{lat}")
            if tape is None:
                sim = sim_exec.SimResult(arrival, (), sim_exec.Fraction(1), "book_unavailable")
            else:
                sim = sim_exec.simulate_ioc_buy(tape, ledger, side=it.side, limit=it.limit,
                                                count=it.count, decided_at=it.decided_at,
                                                latency=timedelta(seconds=lat))
            filled = sum((f.count for f in sim.fills), sim_exec.Fraction(0))
            if sim.fills and not ctx.usable:
                sim = sim_exec.SimResult(arrival, (), sim.unfilled + filled,
                                         f"fee_context_unusable_at_arrival:{ctx.reason}")
                filled = sim_exec.Fraction(0)
            import fees
            for i, f in enumerate(sim.fills):
                rw.apply_fill(self.conn, rw.FillEvent(
                    wallet_id=it.wallet_id, order_id=it.order_id, fill_id=f"{it.order_id}-f{i}",
                    executed_at=arrival, ticker=it.ticker, side=it.side, action="buy",
                    role="taker", price=fees.fraction_to_decimal(f.price),
                    count=fees.fraction_to_decimal(f.count), fee_context=ctx,
                    evidence=(f"sim:{sim_exec.SIM_VERSION}:seq{sim.book_seq}",)))
            rw.release(self.conn, wallet_id=it.wallet_id, order_id=it.order_id, at=arrival)
            self._scenario(event, lat, "admitted", sim.reason,
                           str(fees.fraction_to_decimal(filled)),
                           {"fee_bound": str(adm.fee_bound), "net_edge": str(adm.net_edge_per_contract),
                            "fills": [[str(fees.fraction_to_decimal(f.price)),
                                       str(fees.fraction_to_decimal(f.count))] for f in sim.fills]})
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        if filled:
            self.open_markets.add(it.ticker)

    async def _settle_due(self) -> None:
        for ticker in list(self.open_markets):
            result, evidence = await self.result_for(ticker)
            if result not in ("yes", "no"):
                continue       # unresolved: keep waiting, never guess
            self._txn()
            try:
                for lat in PARAMS["latencies_s"]:
                    rw.settle(self.conn, wallet_id=wallet_id(self.mid, lat), ticker=ticker,
                              result=result, settled_at=self.clock(), evidence=evidence)
                self.conn.execute("INSERT OR IGNORE INTO rx_outcomes VALUES (?,?,?,?,?)",
                                  (self.mid, ticker, result, json.dumps(list(evidence)),
                                   _now_iso(self.clock())))
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise
            self.open_markets.discard(ticker)

    def restore_open_markets(self) -> None:
        """After a restart: any wallet position is an open market to settle."""
        for lat in PARAMS["latencies_s"]:
            for (t,) in self.conn.execute("SELECT ticker FROM rw_positions WHERE wallet_id=?",
                                          (wallet_id(self.mid, lat),)):
                self.open_markets.add(t)
