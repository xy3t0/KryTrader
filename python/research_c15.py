"""Frozen final-minute BTC 15-minute experiment: decisions, persistence,
recovery and cohort management (plan C2; live adapters in
research_c15_live.py).

PAPER ONLY. No order-placement code is reachable from here: fills come from
sim_exec, money from isolated research_wallet wallets, fees from
conditional-paper contexts (never live-release evidence).

Durability: every order's lifecycle is in the database, not memory.
  decision txn   decision + attempt + every scenario's admission/reservation
                 + pending order rows, all-or-nothing;
  fill txn       per scenario: simulated execution + wallet + order state;
  settle txn     per scenario wallet and market.
A crash at any point leaves either the whole step or none of it, and the
next run resumes from the rows (pending orders, unpriced executions,
positions). Execution existence depends only on the order and book
evidence: when fees cannot be established, the execution is kept as
'unpriced', its reservation held and its scenario quarantined until priced.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import logging
import math
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import execution_gate as gate
import fee_context as fctx
import fees
import research_wallet as rw
import sim_exec

logger = logging.getLogger(__name__)

PARAMS = {
    "experiment": "c15-final-minute-btc",
    "version": 2,
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
    "max_spot_age_s": 10,
    "max_vol_age_s": 120,
    "max_candle_lag_s": 180,
    "max_metadata_age_s": 30,
    "fill_grace_s": 1,
    "duration_days": 90,
    "fee_policy": fctx.CONDITIONAL_PAPER,
    "account_id": "research-c15",
    "precision": "0.0001",
    "precision_source": "configured",
    "settlement_source": "CF Benchmarks",
    "book_source": "kalshi-ws-orderbook_delta (read-only, production key)",
    "spot_source": "coinbase-ws ticker; CF index via kalshi-ws when present",
    "vol_source": "hyperliquid 1m candles -> indicators.sigma1m",
}

# Predeclared analysis (C3). Calculations are implemented later; this text
# is frozen with the cohort so they cannot be chosen after seeing results.
ANALYSIS_SPEC = {
    "primary_metric": "realized net-dollar P&L of the L1 wallet over the cohort, including idle days",
    "inference": "one-sided 95% lower confidence bound on mean daily net P&L via seeded "
                 "7-day moving-block bootstrap (seed 20261030, 10000 resamples); "
                 "inconclusive if fewer than 30 cohort days or effective sample too small",
    "stress": "report L2 and L5 wallets alongside L1; no scenario selected after the fact",
    "secondary": ["fill rate", "rejection reasons", "max drawdown", "losses", "fees",
                  "Brier score and calibration on all recorded forecasts incl. no-trades"],
    "ruin": "replay resampled 7-day blocks of recorded opportunities through the $4 wallet",
    "milestone_only": "200 trades",
    "release_eligibility": "never: conditional-paper fee policy",
}

REQUIRED_RUNTIME = {
    "app_mode": "paper",
    "config": {"research_c15_enabled": True, "crypto15m_spot_ws": True},
    "kalshi_key": "production key saved (read-only market data use)",
}

HASHED_FILES = (
    "fees.py", "fee_schedule.py", "fee_capture.py", "fee_ledger.py", "fee_context.py",
    "fee_service.py", "research_wallet.py", "execution_gate.py", "sim_exec.py",
    "research_c15.py", "research_c15_live.py", "research_md.py", "crypto15m.py",
    "indicators.py", "cf_ws.py", "spot_ws.py", "kalshi_auth.py", "ws_ssl.py",
    "requirements.txt",
)
DEP_PACKAGES = ("websockets", "httpx", "cryptography", "certifi")
BUILD_MANIFEST_NAME = "research_build_manifest.json"

SCHEMA = """
CREATE TABLE IF NOT EXISTS rx_manifests (
    manifest_id TEXT PRIMARY KEY,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rx_cohorts (
    manifest_id TEXT PRIMARY KEY REFERENCES rx_manifests (manifest_id),
    status TEXT NOT NULL CHECK (status IN ('armed', 'active', 'suspended', 'complete')),
    created_at TEXT NOT NULL,
    launched_at TEXT,
    note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS rx_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id TEXT NOT NULL REFERENCES rx_manifests (manifest_id),
    event_ticker TEXT NOT NULL,
    market_ticker TEXT,
    close_time TEXT,
    decided_at TEXT NOT NULL,
    eligible INTEGER NOT NULL,
    reason TEXT NOT NULL,
    side TEXT,
    side_probability TEXT,
    limit_price TEXT,
    book TEXT,
    inputs TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rx_decisions_event ON rx_decisions (manifest_id, event_ticker);
CREATE TABLE IF NOT EXISTS rx_attempts (
    manifest_id TEXT NOT NULL,
    event_ticker TEXT NOT NULL,
    market_ticker TEXT NOT NULL,
    decision_id INTEGER NOT NULL REFERENCES rx_decisions (id),
    PRIMARY KEY (manifest_id, event_ticker)
);
CREATE TABLE IF NOT EXISTS rx_orders (
    manifest_id TEXT NOT NULL,
    event_ticker TEXT NOT NULL,
    latency_s INTEGER NOT NULL,
    order_id TEXT NOT NULL UNIQUE,
    wallet_id TEXT NOT NULL,
    market_ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    limit_price TEXT NOT NULL,
    count TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    arrival_at TEXT NOT NULL,
    decision_id INTEGER NOT NULL REFERENCES rx_decisions (id),
    state TEXT NOT NULL CHECK (state IN ('rejected', 'pending', 'filled', 'partial',
                                         'no_fill', 'unpriced')),
    admission TEXT NOT NULL,
    fee_bound TEXT,
    net_edge TEXT,
    reserve TEXT,
    fee_evidence TEXT,
    sim_reason TEXT,
    fills TEXT,
    priced_late INTEGER NOT NULL DEFAULT 0,
    envelope_breach INTEGER NOT NULL DEFAULT 0,   -- actual debit exceeded the admitted reservation
    updated_at TEXT NOT NULL,
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
CREATE TABLE IF NOT EXISTS rx_quarantine (
    manifest_id TEXT NOT NULL,
    latency_s INTEGER NOT NULL,
    reason TEXT NOT NULL,
    since TEXT NOT NULL,
    PRIMARY KEY (manifest_id, latency_s)
);
CREATE TABLE IF NOT EXISTS rx_downtime (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id TEXT,
    at TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rx_book_files (
    market_ticker TEXT PRIMARY KEY,
    close_time TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT,
    finalized_at TEXT
);
"""


class ExperimentError(RuntimeError):
    pass


def init_schema(conn) -> None:
    conn.executescript(SCHEMA)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _dec(f: Fraction) -> str:
    return str(fees.fraction_to_decimal(f))


# ---- manifest, code identity ------------------------------------------------------

def _source_identity(base: Path) -> dict:
    files = {}
    for name in HASHED_FILES:
        p = base / name
        if not p.exists():
            raise ExperimentError(f"cannot freeze: {name} missing")
        files[name] = hashlib.sha256(p.read_bytes()).hexdigest()
    return {"mode": "source", "files": files}


def build_manifest_payload(base_dir: Optional[Path] = None) -> dict:
    """What the packaging step bundles (sources are not shipped)."""
    base = base_dir or Path(__file__).resolve().parent
    return {"files": _source_identity(base)["files"]}


def write_build_manifest(out: Path, base_dir: Optional[Path] = None) -> Path:
    out = Path(out)
    out.write_text(json.dumps(build_manifest_payload(base_dir), sort_keys=True, indent=1))
    return out


def code_identity(base_dir: Optional[Path] = None) -> dict:
    if getattr(sys, "frozen", False) and base_dir is None:
        bundle = Path(getattr(sys, "_MEIPASS", "")) / BUILD_MANIFEST_NAME
        if not bundle.exists():
            raise ExperimentError("packaged build has no research build manifest")
        raw = bundle.read_bytes()
        files = json.loads(raw).get("files") or {}
        missing = [n for n in HASHED_FILES if n not in files]
        if missing:
            raise ExperimentError(f"build manifest lacks {missing}")
        return {"mode": "bundled", "build_manifest_sha256": hashlib.sha256(raw).hexdigest(),
                "files": files}
    return _source_identity(base_dir or Path(__file__).resolve().parent)


def dependency_identity() -> dict:
    out = {}
    for pkg in DEP_PACKAGES:
        try:
            out[pkg] = importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            out[pkg] = "absent"
    return out


def manifest_body(base_dir: Optional[Path] = None) -> dict:
    return {"params": PARAMS, "analysis": ANALYSIS_SPEC, "runtime": REQUIRED_RUNTIME,
            "code": code_identity(base_dir), "dependencies": dependency_identity(),
            "python": sys.version.split()[0], "sim_version": sim_exec.SIM_VERSION,
            "gate_version": gate.GATE_VERSION}


def manifest_id(body: dict) -> str:
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def ensure_manifest(conn, body: dict, now: datetime) -> str:
    mid = manifest_id(body)
    conn.execute("INSERT OR IGNORE INTO rx_manifests VALUES (?,?,?)",
                 (mid, json.dumps(body, sort_keys=True), _iso(now)))
    return mid


# ---- cohorts -----------------------------------------------------------------------

def select_cohort(conn, current_mid: str, *, activate: Optional[str], now: datetime) -> Optional[str]:
    """Pin the cohort that may take new entries. Returns its manifest id or None.

    * A running cohort whose manifest differs from the current code is
      SUSPENDED (no new entries; it keeps draining its own orders/positions).
    * The very first cohort arms automatically. Any later cohort arms only
      when config `research_c15_activate` names its manifest id (>= 12 hex).
    """
    rows = conn.execute("SELECT manifest_id, status FROM rx_cohorts").fetchall()
    for mid, status in rows:
        if mid != current_mid and status in ("armed", "active"):
            conn.execute("UPDATE rx_cohorts SET status='suspended', note=? WHERE manifest_id=?",
                         (f"code/config changed at {_iso(now)}", mid))
    mine = {m: s for m, s in rows}.get(current_mid)
    if mine in ("armed", "active"):
        return current_mid
    if mine in ("suspended", "complete"):
        return None
    explicit = (isinstance(activate, str) and len(activate) >= 12
                and current_mid.startswith(activate.lower()))
    if not rows or explicit:
        conn.execute("INSERT INTO rx_cohorts (manifest_id, status, created_at) VALUES (?,'armed',?)",
                     (current_mid, _iso(now)))
        return current_mid
    return None


def launch(conn, mid: str, now: datetime) -> datetime:
    """armed -> active, recording the launch instant once (after readiness)."""
    conn.execute("UPDATE rx_cohorts SET status='active', launched_at=? WHERE manifest_id=? "
                 "AND status='armed'", (_iso(now), mid))
    r = conn.execute("SELECT launched_at FROM rx_cohorts WHERE manifest_id=?", (mid,)).fetchone()
    if r is None or r[0] is None:
        raise ExperimentError(f"cohort {mid[:12]} is not launchable")
    return _dt(r[0])


def cohort(conn, mid: str) -> Optional[dict]:
    r = conn.execute("SELECT status, launched_at FROM rx_cohorts WHERE manifest_id=?",
                     (mid,)).fetchone()
    return None if r is None else {"status": r[0], "launched_at": _dt(r[1]) if r[1] else None}


def wallet_id(mid: str, latency_s: int) -> str:
    return f"c15-{mid[:16]}-L{latency_s}"


def order_id(mid: str, latency_s: int, event_ticker: str) -> str:
    return f"{mid[:16]}-L{latency_s}-{event_ticker}"


def setup_wallets(conn, mid: str) -> None:
    fctx.register_account(conn, kalshi_env="paper", account_id=PARAMS["account_id"],
                          precision=PARAMS["precision"], precision_source=PARAMS["precision_source"])
    for lat in PARAMS["latencies_s"]:
        try:
            rw.get_wallet(conn, wallet_id(mid, lat))
        except rw.WalletError:
            rw.create_wallet(conn, wallet_id=wallet_id(mid, lat), account_id=PARAMS["account_id"],
                             policy=PARAMS["fee_policy"], starting_cash=PARAMS["starting_cash"])


def outstanding(conn, mid: str) -> int:
    n = conn.execute("SELECT COUNT(*) FROM rx_orders WHERE manifest_id=? AND state IN "
                     "('pending','unpriced')", (mid,)).fetchone()[0]
    for lat in PARAMS["latencies_s"]:
        n += conn.execute("SELECT COUNT(*) FROM rw_positions WHERE wallet_id=?",
                          (wallet_id(mid, lat),)).fetchone()[0]
    return n


# ---- settlement rules -------------------------------------------------------------

def _first_sunday(year: int, month: int) -> int:
    from datetime import date
    d = date(year, month, 1)
    return 1 + (6 - d.weekday()) % 7


def us_eastern(dt: datetime) -> tuple[datetime, str]:
    """US Eastern wall time and abbreviation, without a tz database (the
    packaged Windows build may lack one). DST: 2nd Sunday of March 07:00 UTC
    to 1st Sunday of November 06:00 UTC."""
    u = dt.astimezone(timezone.utc)
    start = datetime(u.year, 3, _first_sunday(u.year, 3) + 7, 7, tzinfo=timezone.utc)
    end = datetime(u.year, 11, _first_sunday(u.year, 11), 6, tzinfo=timezone.utc)
    dst = start <= u < end
    local = (u + timedelta(hours=-4 if dst else -5)).replace(tzinfo=None)
    return local, ("EDT" if dst else "EST")


_MONTHS = ("January", "February", "March", "April", "May", "June", "July", "August",
           "September", "October", "November", "December")


def _close_phrases(close: datetime) -> list[str]:
    local, tz = us_eastern(close)
    h = local.hour % 12 or 12
    ampm = "AM" if local.hour < 12 else "PM"
    t = f"{h}:{local.minute:02d} {ampm} {tz}"
    full = _MONTHS[local.month - 1]
    # Fixed English names: strftime("%b") follows the OS locale.
    names = (full[:3], full) + (("Sept",) if local.month == 9 else ())
    return [f"before {t} on {name} {local.day}, {local.year}" for name in names]


@dataclass(frozen=True)
class RulesCheck:
    ok: bool
    detail: str
    strike: Optional[float] = None
    close_time: Optional[datetime] = None
    rules_sha256: Optional[str] = None


def validate_rules(market: dict, event: dict) -> RulesCheck:
    """Accept a market only if its captured terms are exactly the settlement
    the model assumes: YES iff the simple 60-second BRTI average before close,
    rounded to 2 dp, is at least the strike; $1 binary payout."""
    if not isinstance(market, dict) or not isinstance(event, dict):
        return RulesCheck(False, "payload_not_object")
    prim, sec = market.get("rules_primary"), market.get("rules_secondary")
    if not isinstance(prim, str) or not isinstance(sec, str):
        return RulesCheck(False, "rules_text_missing")
    sha = hashlib.sha256((prim + "\n" + sec).encode()).hexdigest()

    def bad(d):
        return RulesCheck(False, d, rules_sha256=sha)
    strike = market.get("floor_strike")
    if isinstance(strike, bool) or not isinstance(strike, (int, Decimal, float)) or \
            not math.isfinite(float(strike)) or strike <= 0:
        return bad("no_floor_strike")
    if market.get("strike_type") != "greater_or_equal":
        return bad(f"strike_type:{market.get('strike_type')}")
    if market.get("cap_strike") not in (None, ""):
        return bad("has_cap_strike")
    if (market.get("custom_strike") or {}).get("round_digits") != "2":
        return bad("strike_rounding")
    if market.get("market_type") != "binary" or market.get("notional_value_dollars") != "1.0000":
        return bad("payout")
    if market.get("event_ticker") != event.get("event_ticker"):
        return bad("event_mismatch")
    if PARAMS["settlement_source"] not in {s.get("name") for s in (event.get("settlement_sources") or [])
                                           if isinstance(s, dict)}:
        return bad("settlement_source")
    if (event.get("product_metadata") or {}).get("cadence") != "fifteen_min":
        return bad("cadence")
    try:
        close = datetime.fromisoformat(str(market.get("close_time")).replace("Z", "+00:00"))
    except ValueError:
        return bad("close_time")
    if close.tzinfo is None:
        return bad("close_time")
    if "simple average of the sixty seconds of CF Benchmarks' BRTI" not in prim \
            or "is at least the simple average" not in prim \
            or "then the market resolves to Yes" not in prim:
        return bad("rules_primary_terms")
    if not any(p in prim for p in _close_phrases(close)):
        return bad("rules_primary_close_time")
    if "60 RTI prices are collected" not in sec or "average of these prices" not in sec \
            or "rounded to the nearest 2 decimal places" not in sec:
        return bad("rules_secondary_terms")
    return RulesCheck(True, "verified", float(strike), close, sha)


# ---- decision ---------------------------------------------------------------------

@dataclass(frozen=True)
class MarketInputs:
    """Everything one decision may use. Every input carries the time it was
    RECEIVED (server-aligned); none may be later than decided_at."""
    asset: str
    event_ticker: str
    market_ticker: str
    close_time: datetime
    decided_at: datetime
    rules: RulesCheck
    metadata_received_at: datetime
    spot: Optional[float]
    spot_received_at: Optional[datetime]
    spot_source: str
    sigma_1m: Optional[float]
    candles_received_at: Optional[datetime]
    newest_candle_start: Optional[datetime]
    settle_sum: float
    settle_count: int
    settle_source: str

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, sort_keys=True, default=lambda o: o.isoformat()
                          if isinstance(o, datetime) else str(o))


@dataclass(frozen=True)
class Decision:
    eligible: bool
    reason: str
    side: Optional[str] = None
    probability: Optional[float] = None
    limit: Optional[str] = None


def best_ask(state: Optional[sim_exec.BookState], side: str) -> Optional[Fraction]:
    if state is None:
        return None
    opp = state.no if side == "yes" else state.yes
    return min((1 - p for p, s in opp if s > 0), default=None)


def decide(inp: MarketInputs, book: Optional[sim_exec.BookState], *, open_positions: int,
           attempted: bool, quarantined: bool) -> Decision:
    import crypto15m  # the frozen baseline model
    p, d = PARAMS, inp.decided_at
    if inp.asset != p["asset"]:
        return Decision(False, "asset_not_in_experiment")
    if attempted:
        return Decision(False, "already_attempted_this_event")
    if quarantined:
        return Decision(False, "primary_scenario_quarantined")
    if open_positions >= p["max_open_positions"]:
        return Decision(False, "position_limit")
    mins_left = (inp.close_time - d).total_seconds() / 60.0
    if not (0.0 < mins_left < 1.0):
        return Decision(False, "outside_final_minute")
    if not inp.rules.ok:
        return Decision(False, f"settlement_rules_unverified:{inp.rules.detail}")
    for name, t in (("metadata", inp.metadata_received_at), ("spot", inp.spot_received_at),
                    ("candles", inp.candles_received_at)):
        if t is not None and t > d:
            return Decision(False, f"input_from_future:{name}")
    if (d - inp.metadata_received_at).total_seconds() > p["max_metadata_age_s"]:
        return Decision(False, "stale_metadata")
    if inp.spot is None or inp.spot_received_at is None:
        return Decision(False, "missing_spot")
    if (d - inp.spot_received_at).total_seconds() > p["max_spot_age_s"]:
        return Decision(False, "stale_spot")
    if inp.sigma_1m is None or inp.candles_received_at is None or inp.newest_candle_start is None:
        return Decision(False, "missing_volatility")
    if (d - inp.candles_received_at).total_seconds() > p["max_vol_age_s"] or \
            (inp.candles_received_at - inp.newest_candle_start).total_seconds() > p["max_candle_lag_s"]:
        return Decision(False, "stale_volatility")
    if not (p["min_settle_prints"] <= inp.settle_count <= p["max_settle_prints"]):
        return Decision(False, f"settle_prints_{inp.settle_count}_out_of_range")
    up = crypto15m.settlement_up_prob(inp.spot, inp.rules.strike, inp.sigma_1m, mins_left,
                                      partial_sum=inp.settle_sum, partial_count=inp.settle_count)
    if up is None or not math.isfinite(up):
        return Decision(False, "model_unavailable")
    side = "yes" if up >= 0.5 else "no"
    prob = up if side == "yes" else 1.0 - up
    if Fraction(prob) < Fraction(p["min_side_probability"]):
        return Decision(False, "probability_below_threshold", side, prob)
    ask = best_ask(book, side)
    if ask is None:
        return Decision(False, "no_book_or_no_ask", side, prob)
    if ask > Fraction(p["max_limit"]):
        return Decision(False, "ask_above_cap", side, prob)
    return Decision(True, "eligible", side, prob, _dec(ask))


# ---- runner ------------------------------------------------------------------------

FeesPrepared = tuple  # (ctx_at_decision, {latency: ctx_at_arrival}, evidence_dict)


class C15Runner:
    """One cohort. All I/O is injected; all state is in the database."""

    def __init__(self, conn: sqlite3.Connection, *, mid: str,
                 accepting: Callable[[], bool],
                 inputs: Callable[[], Awaitable[Optional[MarketInputs]]],
                 tape_for: Callable[[str, datetime], Optional[sim_exec.BookTape]],
                 prepare_fees: Callable[..., Awaitable[FeesPrepared]],
                 rebuild_fee: Callable[[dict, datetime], fctx.FeeContext],
                 result_for: Callable[[str], Awaitable[tuple[Optional[str], tuple]]],
                 clock: Callable[[], datetime]):
        self.conn, self.mid = conn, mid
        self.accepting, self.inputs, self.tape_for = accepting, inputs, tape_for
        self.prepare_fees, self.rebuild_fee = prepare_fees, rebuild_fee
        self.result_for, self.clock = result_for, clock

    # -- transactions
    def _begin(self):
        if self.conn.in_transaction:
            raise ExperimentError("runner found a dangling transaction")
        self.conn.execute("BEGIN IMMEDIATE")

    def _quarantined(self, lat: int) -> bool:
        return self.conn.execute("SELECT 1 FROM rx_quarantine WHERE manifest_id=? AND latency_s=?",
                                 (self.mid, lat)).fetchone() is not None

    def _scenario_busy(self, lat: int) -> int:
        n = self.conn.execute("SELECT COUNT(*) FROM rx_orders WHERE manifest_id=? AND latency_s=? "
                              "AND state IN ('pending','unpriced')", (self.mid, lat)).fetchone()[0]
        return n + self.conn.execute("SELECT COUNT(*) FROM rw_positions WHERE wallet_id=?",
                                     (wallet_id(self.mid, lat),)).fetchone()[0]

    async def tick(self) -> None:
        self.process_pending()
        self.retry_unpriced()
        await self.settle_due()
        await self.record_outcomes()
        if self.accepting():
            await self.decide_once()

    # -- decisions
    async def decide_once(self) -> None:
        inp = await self.inputs()
        if inp is None:
            return
        primary = PARAMS["primary_latency_s"]
        tape = self.tape_for(inp.market_ticker, inp.decided_at)
        book = tape.state_at(inp.decided_at) if tape else None
        attempted = self.conn.execute("SELECT 1 FROM rx_attempts WHERE manifest_id=? AND "
                                      "event_ticker=?", (self.mid, inp.event_ticker)).fetchone()
        d = decide(inp, book, open_positions=self._scenario_busy(primary),
                   attempted=attempted is not None, quarantined=self._quarantined(primary))
        if d.reason == "outside_final_minute":
            return
        prepared = None
        if d.eligible:
            arrivals = {lat: inp.decided_at + timedelta(seconds=lat) for lat in PARAMS["latencies_s"]}
            prepared = await self.prepare_fees(market_ticker=inp.market_ticker,
                                               decided_at=inp.decided_at, arrivals=arrivals)
        self._begin()
        try:
            did = self.conn.execute(
                "INSERT INTO rx_decisions (manifest_id, event_ticker, market_ticker, close_time, "
                "decided_at, eligible, reason, side, side_probability, limit_price, book, inputs) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (self.mid, inp.event_ticker, inp.market_ticker, _iso(inp.close_time),
                 _iso(inp.decided_at), int(d.eligible), d.reason, d.side,
                 None if d.probability is None else repr(d.probability), d.limit,
                 None if book is None else json.dumps({"seq": book.seq, "sid": book.sid,
                                                       "gen": book.generation,
                                                       "received_at": _iso(book.received_at)}),
                 inp.to_json())).lastrowid
            if d.eligible:
                self.conn.execute("INSERT INTO rx_attempts VALUES (?,?,?,?)",
                                  (self.mid, inp.event_ticker, inp.market_ticker, did))
                self._admit_all(inp, d, did, prepared)
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def _admit_all(self, inp: MarketInputs, d: Decision, did: int, prepared: FeesPrepared) -> None:
        ctx0, arrival_ctx, evidence = prepared
        now = _iso(self.clock())
        for lat in PARAMS["latencies_s"]:
            oid = order_id(self.mid, lat, inp.event_ticker)
            arrival = inp.decided_at + timedelta(seconds=lat)
            row = dict(manifest_id=self.mid, event_ticker=inp.event_ticker, latency_s=lat,
                       order_id=oid, wallet_id=wallet_id(self.mid, lat),
                       market_ticker=inp.market_ticker, side=d.side, limit_price=d.limit,
                       count=PARAMS["contracts_per_entry"], decided_at=_iso(inp.decided_at),
                       arrival_at=_iso(arrival), decision_id=did, fee_evidence=json.dumps(evidence),
                       updated_at=now)
            if self._quarantined(lat):
                self._insert_order(row, "rejected", "scenario_quarantined")
                continue
            if self._scenario_busy(lat) >= PARAMS["max_open_positions"]:
                self._insert_order(row, "rejected", "scenario_position_limit")
                continue
            adm = gate.admit(self.conn, gate.ExecutionIntent(
                wallet_id=row["wallet_id"], order_id=oid, ticker=inp.market_ticker, side=d.side,
                action="buy", order_type="ioc", role="taker", limit=d.limit,
                count=PARAMS["contracts_per_entry"], side_probability=d.probability,
                decided_at=inp.decided_at, fee_context=ctx0,
                fee_envelope=(arrival_ctx[lat],)),
                min_net_edge_per_contract=PARAMS["min_net_edge_per_contract"],
                max_limit=PARAMS["max_limit"], policy=PARAMS["fee_policy"])
            row.update(fee_bound=None if adm.fee_bound is None else str(adm.fee_bound),
                       net_edge=None if adm.net_edge_per_contract is None
                       else str(adm.net_edge_per_contract),
                       reserve=None if adm.reserve_amount is None else str(adm.reserve_amount))
            self._insert_order(row, "pending" if adm.admitted else "rejected", adm.reason)

    def _insert_order(self, row: dict, state: str, admission: str) -> None:
        row = {**row, "state": state, "admission": admission}
        cols = ",".join(row)
        self.conn.execute(f"INSERT INTO rx_orders ({cols}) VALUES ({','.join('?' * len(row))})",
                          tuple(row.values()))

    # -- execution
    def process_pending(self) -> None:
        grace = timedelta(seconds=PARAMS["fill_grace_s"])
        now = self.clock()
        rows = self.conn.execute(
            "SELECT event_ticker, latency_s FROM rx_orders WHERE manifest_id=? AND state='pending' "
            "ORDER BY arrival_at, latency_s", (self.mid,)).fetchall()
        for event, lat in rows:
            o = self._order(event, lat)
            if now < _dt(o["arrival_at"]) + grace:
                continue
            self._execute(o)

    def _order(self, event: str, lat: int) -> dict:
        cur = self.conn.execute("SELECT * FROM rx_orders WHERE manifest_id=? AND event_ticker=? "
                                "AND latency_s=?", (self.mid, event, lat))
        names = [c[0] for c in cur.description]
        return dict(zip(names, cur.fetchone()))

    def _simulate(self, o: dict) -> sim_exec.SimResult:
        arrival = _dt(o["arrival_at"])
        tape = self.tape_for(o["market_ticker"], arrival)
        if tape is None:
            return sim_exec.SimResult(arrival, (), Fraction(o["count"]), "book_unavailable")
        ledger = sim_exec.LiquidityLedger(self.conn, scope=f"{self.mid}:L{o['latency_s']}")
        return sim_exec.simulate_ioc_buy(
            tape, ledger, side=o["side"], limit=o["limit_price"], count=o["count"],
            decided_at=_dt(o["decided_at"]), latency=arrival - _dt(o["decided_at"]))

    @staticmethod
    def _fills_json(sim) -> str:
        return json.dumps([[_dec(f.price), _dec(f.count)] for f in sim.fills])

    def _apply_fills(self, o: dict, fills: list, ctx: fctx.FeeContext) -> Fraction:
        arrival = _dt(o["arrival_at"])
        filled = Fraction(0)
        for i, (price, count) in enumerate(fills):
            rw.apply_fill(self.conn, rw.FillEvent(
                wallet_id=o["wallet_id"], order_id=o["order_id"], fill_id=f"{o['order_id']}-f{i}",
                executed_at=arrival, ticker=o["market_ticker"], side=o["side"], action="buy",
                role="taker", price=price, count=count, fee_context=ctx,
                evidence=(f"sim:{sim_exec.SIM_VERSION}:book:{o['market_ticker']}",)))
            filled += Fraction(Decimal(count))
        # A reservation can be consumed exactly in full; release only a remainder.
        if self.conn.execute("SELECT 1 FROM rw_reservations WHERE wallet_id=? AND order_id=?",
                             (o["wallet_id"], o["order_id"])).fetchone():
            rw.release(self.conn, wallet_id=o["wallet_id"], order_id=o["order_id"], at=arrival)
        return filled

    def _breach(self, o: dict, cash_before: Decimal) -> int:
        """1 if the fills debited more than the admission reserved (the fee
        metadata moved outside the declared envelope). Recorded, not hidden."""
        spent = Fraction(cash_before) - Fraction(rw.get_wallet(self.conn, o["wallet_id"]).cash)
        return int(o["reserve"] is not None and spent > Fraction(Decimal(o["reserve"])))

    def _set_state(self, o: dict, from_state: str, **fields) -> None:
        fields["updated_at"] = _iso(self.clock())
        sets = ",".join(f"{k}=?" for k in fields)
        cur = self.conn.execute(f"UPDATE rx_orders SET {sets} WHERE order_id=? AND state=?",
                                (*fields.values(), o["order_id"], from_state))
        if cur.rowcount != 1:
            raise ExperimentError(f"order {o['order_id']} not in state {from_state}")

    def _execute(self, o: dict) -> None:
        arrival = _dt(o["arrival_at"])
        try:
            ctx = self.rebuild_fee(json.loads(o["fee_evidence"]), arrival)
        except Exception as e:
            ctx = fctx.FeeContext("paper", PARAMS["account_id"], None, Decimal(0), "",
                                  PARAMS["fee_policy"], None, (), None, False, f"rebuild_failed:{e}")
        self._begin()
        try:
            sim = self._simulate(o)
            fills = [[_dec(f.price), _dec(f.count)] for f in sim.fills]
            if not fills:
                rw.release(self.conn, wallet_id=o["wallet_id"], order_id=o["order_id"], at=arrival)
                self._set_state(o, "pending", state="no_fill", sim_reason=sim.reason, fills="[]")
            else:
                if not ctx.usable:
                    raise _Unpriced(f"fee context: {ctx.reason}")
                before = rw.get_wallet(self.conn, o["wallet_id"]).cash
                filled = self._apply_fills(o, fills, ctx)
                state = "filled" if filled == Fraction(Decimal(o["count"])) else "partial"
                self._set_state(o, "pending", state=state, sim_reason=sim.reason,
                                fills=json.dumps(fills),
                                envelope_breach=self._breach(o, before))
            self.conn.commit()
            return
        except (_Unpriced, rw.WalletError) as e:
            self.conn.rollback()
            reason = str(e)
        except BaseException:
            self.conn.rollback()
            raise
        # The execution happened on the book; only its pricing failed. Keep it.
        self._begin()
        try:
            sim = self._simulate(o)          # deterministic; re-consumes the same liquidity
            self._set_state(o, "pending", state="unpriced", sim_reason=sim.reason,
                            fills=self._fills_json(sim))
            self.conn.execute("INSERT OR IGNORE INTO rx_quarantine VALUES (?,?,?,?)",
                              (self.mid, o["latency_s"], f"unpriced {o['order_id']}: {reason}"[:300],
                               _iso(self.clock())))
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def retry_unpriced(self) -> None:
        rows = self.conn.execute("SELECT event_ticker, latency_s FROM rx_orders WHERE "
                                 "manifest_id=? AND state='unpriced'", (self.mid,)).fetchall()
        for event, lat in rows:
            o = self._order(event, lat)
            try:
                ctx = self.rebuild_fee(json.loads(o["fee_evidence"]), _dt(o["arrival_at"]))
            except Exception:
                continue
            if ctx.usable:
                try:
                    self.resolve_unpriced(event, lat, ctx)
                except rw.WalletError as e:     # still cannot be booked: stays unpriced
                    logger.warning(f"[c15] {o['order_id']} remains unpriced: {e}")

    def resolve_unpriced(self, event: str, lat: int, ctx: fctx.FeeContext) -> None:
        """Price a kept execution with a usable context for its arrival instant."""
        o = self._order(event, lat)
        if o["state"] != "unpriced":
            raise ExperimentError("order is not unpriced")
        self._begin()
        try:
            before = rw.get_wallet(self.conn, o["wallet_id"]).cash
            filled = self._apply_fills(o, json.loads(o["fills"]), ctx)
            state = "filled" if filled == Fraction(Decimal(o["count"])) else "partial"
            self._set_state(o, "unpriced", state=state, priced_late=1,
                            envelope_breach=self._breach(o, before))
            left = self.conn.execute("SELECT COUNT(*) FROM rx_orders WHERE manifest_id=? AND "
                                     "latency_s=? AND state='unpriced'", (self.mid, lat)).fetchone()[0]
            if not left:
                self.conn.execute("DELETE FROM rx_quarantine WHERE manifest_id=? AND latency_s=?",
                                  (self.mid, lat))
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    # -- settlement and outcomes
    async def settle_due(self) -> None:
        results: dict = {}
        for lat in PARAMS["latencies_s"]:
            wid = wallet_id(self.mid, lat)
            for (ticker,) in self.conn.execute("SELECT DISTINCT ticker FROM rw_positions WHERE "
                                               "wallet_id=?", (wid,)).fetchall():
                if ticker not in results:
                    results[ticker] = await self.result_for(ticker)
                result, evidence = results[ticker]
                if result not in ("yes", "no"):
                    continue                     # unresolved or voided: never guess
                self._begin()
                try:
                    rw.settle(self.conn, wallet_id=wid, ticker=ticker, result=result,
                              settled_at=self.clock(), evidence=tuple(evidence) or ("result",))
                    self.conn.execute("INSERT OR IGNORE INTO rx_outcomes VALUES (?,?,?,?,?)",
                                      (self.mid, ticker, result, json.dumps(list(evidence)),
                                       _iso(self.clock())))
                    self.conn.commit()
                except BaseException:
                    self.conn.rollback()
                    raise

    async def record_outcomes(self, limit: int = 3) -> None:
        """Official results for every recorded forecast, traded or not."""
        now = self.clock()
        rows = self.conn.execute(
            "SELECT DISTINCT d.market_ticker FROM rx_decisions d LEFT JOIN rx_outcomes o ON "
            "o.manifest_id=d.manifest_id AND o.market_ticker=d.market_ticker WHERE d.manifest_id=? "
            "AND o.market_ticker IS NULL AND d.side_probability IS NOT NULL AND d.close_time < ? "
            "LIMIT ?", (self.mid, _iso(now - timedelta(minutes=2)), limit)).fetchall()
        for (ticker,) in rows:
            result, evidence = await self.result_for(ticker)
            if result in ("yes", "no"):
                self._begin()
                try:
                    self.conn.execute("INSERT OR IGNORE INTO rx_outcomes VALUES (?,?,?,?,?)",
                                      (self.mid, ticker, result, json.dumps(list(evidence)), _iso(now)))
                    self.conn.commit()
                except BaseException:
                    self.conn.rollback()
                    raise


class _Unpriced(Exception):
    pass
