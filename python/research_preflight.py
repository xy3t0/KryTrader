"""Launch preflight for the C15 paper experiment, run on the machine that will
host it. Read-only: it opens the research market-data socket and public
price feeds; it cannot place, change or cancel orders.

    krypt-trader-backend --research-preflight      (packaged)
    python service.py --research-preflight         (from source)

Checks (all REQUIRED unless marked WARN):
  * Kalshi key saved; Kalshi clock offset measurable
  * code manifest builds (packaged: the bundled build manifest)
  * an open BTC 15-minute market is discoverable
  * authenticated read-only socket: connects, one shared subscription,
    snapshot received, book usable with liveness marks
  * get_snapshot answered with a fresh snapshot
  * forced reconnect: new generation, fresh snapshot, book usable again
  * Coinbase spot feed fresh; Hyperliquid candles valid
  * CF index frames on the research socket (WARN: proxy fallback exists)
A JSON report is written to <app data>/data/research_preflight.json, bound to
the experiment manifest it checked. `--research-enable` refuses unless a
passing report for the CURRENT manifest is between 0 and 24 h old, and the
switch it writes names that manifest. The switch is the only on/off control:
the app runs the experiment only while the switch is enabled for the running
manifest (a code change needs a new preflight). `--research-disable` always wins.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPORT_NAME = "research_preflight.json"
SWITCH_NAME = "research_c15.json"
MAX_REPORT_AGE = timedelta(hours=24)
MAX_REPORT_FUTURE = timedelta(seconds=60)   # clock jitter; anything later is refused


def default_user_data() -> Path:
    """Where the desktop app keeps its data (Electron userData for
    productName "Krypt Trader"), so a standalone run finds the saved key."""
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming")
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return base / "Krypt Trader"


def ensure_user_data() -> Path:
    if not os.environ.get("KRYPT_TRADER_USERDATA"):
        os.environ["KRYPT_TRADER_USERDATA"] = str(default_user_data())
    return Path(os.environ["KRYPT_TRADER_USERDATA"])


def data_dir() -> Path:
    import db
    return db._data_dir()


class Report:
    def __init__(self):
        self.items: list[dict] = []
        self.manifest_id: str | None = None

    def add(self, name: str, ok: bool, detail: str = "", required: bool = True) -> bool:
        self.items.append({"check": name, "ok": bool(ok), "required": required, "detail": detail})
        mark = "PASS" if ok else ("FAIL" if required else "WARN")
        print(f"[{mark}] {name}" + (f" - {detail}" if detail else ""), flush=True)
        return ok

    @property
    def passed(self) -> bool:
        return all(i["ok"] for i in self.items if i["required"])


async def _wait(cond, timeout: float, step: float = 0.25) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        await asyncio.sleep(step)
    return bool(cond())


def clock_check(kalshi_auth) -> tuple[bool, str]:
    """A fresh Kalshi clock measurement, never the cached offset that
    sync_server_time returns when its request fails."""
    try:
        asked = time.time()
        offset = kalshi_auth.sync_server_time(force=True)
    except Exception as e:
        return False, str(e)
    if kalshi_auth.last_sync_ok_at < asked:
        return False, "clock sync failed; refusing a cached offset"
    return True, f"{offset} ms (decisions use Kalshi time)"


async def run_preflight() -> Report:
    ensure_user_data()
    import cf_ws
    import httpx
    import indicators
    import kalshi_auth
    import research_c15 as rx
    import research_md
    import sim_exec
    import spot_ws

    r = Report()
    print(f"App data: {os.environ['KRYPT_TRADER_USERDATA']}", flush=True)
    have_key = kalshi_auth.credentials_present(kalshi_auth.PRODUCTION)
    if not r.add("Kalshi API key saved", have_key, "" if have_key else "add it in the app: API Keys"):
        return r
    r.add("Kalshi clock offset measured", *clock_check(kalshi_auth))
    try:
        body = rx.manifest_body()
        r.manifest_id = rx.manifest_id(body)
        r.add("Experiment manifest", True, f"{r.manifest_id[:16]} ({body['code']['mode']})")
    except Exception as e:
        r.add("Experiment manifest", False, str(e))

    def now():
        return datetime.fromtimestamp(kalshi_auth.server_now(), timezone.utc)

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get("https://api.elections.kalshi.com/trade-api/v2/markets",
                                    params={"series_ticker": rx.PARAMS["series"], "status": "open",
                                            "limit": "20"})
            markets = [m for m in resp.json().get("markets", [])
                       if datetime.fromisoformat(m["close_time"].replace("Z", "+00:00")) > now()]
            market = min(markets, key=lambda m: m["close_time"])["ticker"]
            r.add("Open BTC 15-minute market found", True, market)
        except Exception as e:
            r.add("Open BTC 15-minute market found", False, str(e))
            return r

        if not spot_ws.is_running():
            spot_ws.start()

        stream = sim_exec.BookStream()
        cf_state = cf_ws._State()
        sent: list[dict] = []
        snapshots = {"n": 0}

        def on_event(ev):
            if ev.get("kind") == "msg" and '"orderbook_snapshot"' in (ev.get("raw") or ""):
                snapshots["n"] += 1
            stream.feed(ev)
        md = research_md.MarketDataClient(on_event=on_event, on_cf=cf_state.handle_message)
        orig_send = md._send

        async def recording_send(obj):
            sent.append((md.generation, obj))
            await orig_send(obj)
        md._send = recording_send
        md.set_markets([market])
        stop = asyncio.Event()
        task = asyncio.ensure_future(md.run(stop))
        try:
            ok = await _wait(lambda: md.connected, 20)
            r.add("Authenticated read-only socket connected", ok, md.last_error or "")
            if ok:
                usable = await _wait(lambda: stream.state_at(market, now()) is not None, 25)
                r.add("Snapshot received and book usable (with liveness)", usable)
                # One subscription per CONNECTION (a reconnect re-subscribes).
                per_conn: dict = {}
                for g, s in sent:
                    if s["cmd"] == "subscribe" and s["params"].get("channels") == ["orderbook_delta"]:
                        per_conn[g] = per_conn.get(g, 0) + 1
                r.add("Exactly one orderbook subscription per connection",
                      bool(per_conn) and all(n == 1 for n in per_conn.values()),
                      f"{sum(per_conn.values())} over {len(per_conn)} connection(s)")

                got = False
                for _ in range(3):      # a drop mid-request is retried, not a failure
                    if not await _wait(lambda: md.connected and md._sid is not None
                                       and market in md._have, 20):
                        continue
                    before = snapshots["n"]
                    if await md.request_snapshot([market]) and \
                            await _wait(lambda: snapshots["n"] > before, 10):
                        got = True
                        break
                r.add("get_snapshot answered", got)

                gen = md.generation
                if md._ws is not None:
                    await md._ws.close()
                again = await _wait(lambda: md.generation > gen and md.connected
                                    and stream.state_at(market, now()) is not None, 60)
                r.add("Reconnect: new generation and usable book again", again,
                      f"generation {gen} -> {md.generation}")
                cmds = {s["cmd"] for _, s in sent}
                actions = {s["params"].get("action") for _, s in sent if s["cmd"] == "update_subscription"}
                r.add("Only read-only commands were sent",
                      cmds <= research_md.ALLOWED_CMDS and actions <= research_md.ALLOWED_ACTIONS,
                      f"commands {sorted(cmds)}")
                cf_ok = await _wait(lambda: rx.PARAMS["asset"] in cf_state.values, 10)
                r.add("CF index frames on the research socket", cf_ok,
                      "optional: Coinbase proxy is used otherwise", required=False)
                ps = md.pong_stats
                total = ps["fast"] + ps["slow"]
                r.add("Connection quality (pings answered within 2 s)",
                      total > 0 and ps["slow"] <= total // 10,
                      f"{ps['fast']}/{total} fast, slowest {ps['max_rtt_ms']} ms, "
                      f"{len(per_conn)} connection(s); slow pings only cost coverage",
                      required=False)
        finally:
            stop.set()
            try:
                await asyncio.wait_for(task, 10)
            except BaseException:
                task.cancel()

        spot_ok = await _wait(lambda: rx.PARAMS["asset"] in spot_ws.fresh_spots(), 20)
        r.add("Coinbase spot feed fresh", spot_ok)
        try:
            t = now()
            start = int((t - timedelta(minutes=90)).timestamp() * 1000)
            resp = await client.post("https://api.hyperliquid.xyz/info", json={
                "type": "candleSnapshot", "req": {"coin": rx.PARAMS["asset"], "interval": "1m",
                                                  "startTime": start, "endTime": int(t.timestamp() * 1000)}})
            closes, newest = rx.candle_series(resp.json(), now(), window=indicators.SIGMA_WINDOW)
            r.add("Volatility candles valid", indicators.sigma1m(closes) is not None,
                  f"{len(closes)} candles, newest {newest:%H:%M}Z")
        except Exception as e:
            r.add("Volatility candles valid", False, str(e))
        try:
            await spot_ws.stop()
        except Exception:
            pass
    return r


def write_report(r: Report) -> Path:
    p = data_dir() / REPORT_NAME
    p.write_text(json.dumps({"at": datetime.now(timezone.utc).isoformat(),
                             "passed": r.passed and r.manifest_id is not None,
                             "manifest_id": r.manifest_id, "items": r.items}, indent=1))
    return p


def main() -> int:
    r = asyncio.run(run_preflight())
    path = write_report(r)
    print(f"\nPREFLIGHT {'PASSED' if r.passed else 'FAILED'} - report: {path}", flush=True)
    return 0 if r.passed else 1


# ---- switch -------------------------------------------------------------------

def read_switch() -> dict:
    """{"present", "enabled", "manifest_id"}. Missing or invalid -> disabled."""
    try:
        d = json.loads((data_dir() / SWITCH_NAME).read_text())
    except FileNotFoundError:
        return {"present": False, "enabled": False, "manifest_id": None}
    except Exception:
        return {"present": True, "enabled": False, "manifest_id": None}
    if not isinstance(d, dict):           # [], null, true, 7 ... -> disabled
        return {"present": True, "enabled": False, "manifest_id": None}
    mid = d.get("manifest_id")
    ok = (d.get("enabled") is True and isinstance(mid, str) and len(mid) == 64
          and all(c in "0123456789abcdef" for c in mid))
    return {"present": True, "enabled": ok, "manifest_id": mid if ok else None}


def experiment_enabled(manifest_id: str | None = None) -> bool:
    """The single on/off rule. Enabled only by the switch file, and (when the
    running manifest is given) only for the manifest the switch names."""
    s = read_switch()
    return s["enabled"] and (manifest_id is None or s["manifest_id"] == manifest_id)


def check_report(report: dict, current_manifest: str, now: datetime) -> str | None:
    """None if the report may enable the switch, else the reason it may not."""
    try:
        at = datetime.fromisoformat(report["at"])
        if at.tzinfo is None:
            return "report time has no timezone"
    except Exception:
        return "report unreadable"
    if report.get("passed") is not True:
        return "report did not pass"
    if at - now > MAX_REPORT_FUTURE:
        return "report is dated in the future"
    if now - at >= MAX_REPORT_AGE:
        return "report is older than 24 h"
    if report.get("manifest_id") != current_manifest:
        return "report was made for a different build/manifest"
    return None


def set_switch(enabled: bool) -> int:
    ensure_user_data()
    path = data_dir() / SWITCH_NAME
    if not enabled:
        path.write_text(json.dumps({"enabled": False}))
        print("Research experiment DISABLED. Restart the app to apply.")
        return 0
    import research_c15 as rx
    try:
        mid = rx.manifest_id(rx.manifest_body())
        rep = json.loads((data_dir() / REPORT_NAME).read_text())
        why = check_report(rep, mid, datetime.now(timezone.utc))
    except Exception as e:
        why = f"cannot check report ({e})"
    if why:
        print(f"Refusing: {why}. Run --research-preflight with this build first.")
        return 1
    path.write_text(json.dumps({"enabled": True, "manifest_id": mid,
                                "at": datetime.now(timezone.utc).isoformat()}))
    print(f"Research experiment ENABLED for manifest {mid[:16]}. Restart the app to apply.")
    return 0
