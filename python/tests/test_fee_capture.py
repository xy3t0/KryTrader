"""Tests for fee_capture.py: recording, storage, pagination, timing.

No network: a scripted async `send` stands in for the HTTP client, and the
end-to-end test replays the untouched bodies captured 2026-10-30.
Stdlib-only: python -m pytest tests/test_fee_capture.py --noconftest
"""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fee_capture as fc  # noqa: E402
import fee_schedule as fs  # noqa: E402
from fee_capture import CaptureRecord, CaptureStore, FeeCaptureError, RawResponse  # noqa: E402

UTC = timezone.utc
T0 = datetime(2026, 10, 30, 10, 52, 18, tzinfo=UTC)
RAW = Path(__file__).resolve().parent / "fixtures" / "kalshi_fee_2026_10_30"
HDRS = {"Date": "Fri, 30 Oct 2026 10:52:26 GMT", "Cache-Control": "public, max-age=15"}


class Clock:
    def __init__(self, start=T0, step=timedelta(milliseconds=250)):
        self.t, self.step = start, step

    def __call__(self):
        t = self.t
        self.t += self.step
        return t


class Script:
    """send() that answers from a route table and logs every request."""
    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    async def __call__(self, url, params):
        self.calls.append((url, dict(params)))
        key = (url.split("/trade-api/v2", 1)[1], tuple(sorted(params.items())))
        r = self.routes.get(key) or self.routes.get(key[0])
        if r is None:
            return RawResponse(404, {"Cache-Control": "no-store"}, b'{"error":{"code":"not_found"}}')
        return r


def run(coro):
    return asyncio.run(coro)


def capturer(tmp_path, routes, clock=None):
    return fc.FeeMetadataCapturer(Script(routes), CaptureStore(tmp_path / "cap"),
                                  clock=clock or Clock())


# ---- recording and storage ------------------------------------------------

def test_capture_records_everything_and_persists(tmp_path):
    body = (RAW / "series-btc15m.json").read_bytes()
    c = capturer(tmp_path, {"/series/KXBTC15M": RawResponse(200, {**HDRS, "Age": "13"}, body)})
    b = c.new_batch()
    rec, got = run(c.series("KXBTC15M", batch_id=b, context={"assumptions": ["x"]}))
    assert got == body
    assert rec.url.endswith("/series/KXBTC15M") and rec.params == ()
    assert (rec.status, rec.age_header, rec.cache_control) == (200, "13", "public, max-age=15")
    assert rec.date_header == HDRS["Date"]
    assert rec.request_sent < rec.response_received
    assert rec.context == {"assumptions": ["x"]} and rec.batch_id == b
    # Round-trips through disk byte-exact.
    rec2, body2 = c.store.get(rec.capture_id)
    assert rec2 == rec and body2 == body


def test_identical_bodies_stored_once(tmp_path):
    body = b'{"series_fee_change_arr":[]}'
    c = capturer(tmp_path, {"/series/fee_changes": RawResponse(200, HDRS, body)})
    b = c.new_batch()
    r1, _ = run(c.series_fee_changes(show_historical=False, batch_id=b))
    r2, _ = run(c.series_fee_changes(show_historical=False, batch_id=b))
    assert r1.body_sha256 == r2.body_sha256 and r1.capture_id != r2.capture_id
    assert len(list((tmp_path / "cap" / "bodies").iterdir())) == 1
    assert len(list(c.store.records())) == 2


def test_show_historical_param_recorded(tmp_path):
    key = ("/series/fee_changes", (("show_historical", "true"),))
    c = capturer(tmp_path, {key: RawResponse(200, HDRS, b'{"series_fee_change_arr":[]}')})
    rec, _ = run(c.series_fee_changes(show_historical=True, batch_id="b"))
    assert rec.params == (("show_historical", "true"),)


def test_non_200_is_recorded_then_raised(tmp_path):
    c = capturer(tmp_path, {})
    with pytest.raises(FeeCaptureError):
        run(c.series("KXMVE", batch_id="b"))
    recs = list(c.store.records())
    assert len(recs) == 1 and recs[0].status == 404


def test_corrupted_body_detected(tmp_path):
    c = capturer(tmp_path, {"/series/S": RawResponse(200, HDRS, b'{"a":1}')})
    rec, _ = run(c.series("S", batch_id="b"))
    (tmp_path / "cap" / "bodies" / rec.body_sha256).write_bytes(b'{"a":2}')
    with pytest.raises(FeeCaptureError):
        c.store.get(rec.capture_id)


@pytest.mark.parametrize("bad", ["../etc", "A/B", "", "a b", "?x=1", None, 5])
def test_bad_tickers_rejected_before_any_request(tmp_path, bad):
    c = capturer(tmp_path, {})
    with pytest.raises(FeeCaptureError):
        run(c.series(bad, batch_id="b"))
    assert c.send.calls == []


def test_bad_digest_rejected(tmp_path):
    with pytest.raises(FeeCaptureError):
        CaptureStore(tmp_path).get_body("../../x")


def test_malformed_log_line_raises(tmp_path):
    s = CaptureStore(tmp_path)
    s.log.write_text("{not json\n")
    with pytest.raises(FeeCaptureError):
        list(s.records())


# ---- pagination -------------------------------------------------------------

def _page(cursor, n=0):
    items = [{"id": f"c{n}-{i}", "event_ticker": "E", "series_ticker": "S",
              "fee_type_override": None, "fee_multiplier_override": 1,
              "scheduled_ts": "2026-10-30T12:00:00Z"} for i in range(2)]
    return RawResponse(200, {**HDRS, "Cache-Control": "public, max-age=30"},
                       json.dumps({"cursor": cursor, "event_fee_changes": items}).encode())


def _key(cursor=None, ev="E"):
    p = {"limit": "1000", "event_ticker": ev}
    if cursor:
        p["cursor"] = cursor
    return ("/events/fee_changes", tuple(sorted(p.items())))


def test_pagination_follows_cursor_and_records_each_page(tmp_path):
    c = capturer(tmp_path, {_key(): _page("k1", 0), _key("k1"): _page("k2", 1), _key("k2"): _page("", 2)})
    pages = run(c.event_fee_changes(event_ticker="E", batch_id="b"))
    assert [p[0].page_index for p in pages] == [0, 1, 2]
    assert [p[0].request_cursor for p in pages] == ["", "k1", "k2"]
    ec = fs.parse_event_fee_change_pages(
        [fs.loads(b) for _, b in pages], pages[0][0].as_of(clock_skew_s=2),
        coverage=fs.Coverage.none())
    assert len(ec.changes) == 6


def test_repeated_cursor_raises(tmp_path):
    c = capturer(tmp_path, {_key(): _page("k1"), _key("k1"): _page("k1")})
    with pytest.raises(FeeCaptureError):
        run(c.event_fee_changes(event_ticker="E", batch_id="b"))


def test_max_pages_raises_instead_of_returning_partial(tmp_path):
    routes = {_key(): _page("k0")}
    for i in range(5):
        routes[_key(f"k{i}")] = _page(f"k{i + 1}")
    c = capturer(tmp_path, routes)
    with pytest.raises(FeeCaptureError):
        run(c.event_fee_changes(event_ticker="E", batch_id="b", max_pages=3))
    assert len(list(c.store.records())) == 3  # every fetched page still recorded


def test_page_without_cursor_raises(tmp_path):
    bad = RawResponse(200, HDRS, b'{"event_fee_changes":[]}')
    c = capturer(tmp_path, {_key(): bad})
    with pytest.raises(FeeCaptureError):
        run(c.event_fee_changes(event_ticker="E", batch_id="b"))


@pytest.mark.parametrize("limit", [0, 1001])
def test_limit_bounds(tmp_path, limit):
    with pytest.raises(FeeCaptureError):
        run(capturer(tmp_path, {}).event_fee_changes(event_ticker="E", batch_id="b", limit=limit))


# ---- observation windows from captured headers ----------------------------

def _rec(cc, age=None):
    return CaptureRecord(capture_id="x", batch_id="b", url="u", params=(),
                         request_sent=T0, response_received=T0 + timedelta(seconds=1),
                         status=200, date_header=None, age_header=age, cache_control=cc,
                         body_sha256="0" * 64, body_size=0)


def test_as_of_uses_max_age_and_age():
    a = _rec("public, max-age=15", "13").as_of(clock_skew_s=2)
    assert a.lo == T0 - timedelta(seconds=30) and a.hi == T0 + timedelta(seconds=3)


@pytest.mark.parametrize("cc", ["no-store", "private, no-cache"])
def test_as_of_no_store_means_no_cache_age(cc):
    assert _rec(cc).as_of(clock_skew_s=0).lo == T0


@pytest.mark.parametrize("cc,age", [("public", None), ("max-age=abc", None), ("", None),
                                    ("max-age=1, max-age=2", None), ("max-age=15", "-3"),
                                    ("max-age=15", "soon"), (None, "3")])
def test_as_of_refuses_unknown_caching(cc, age):
    with pytest.raises(FeeCaptureError):
        _rec(cc, age).as_of(clock_skew_s=0)


def test_as_of_undeclared_cache_is_an_assumed_bound():
    """Kalshi /markets/{t} and /events/{t} send neither Cache-Control nor Age."""
    import fee_capture as fcap
    a = _rec(None, None).as_of(clock_skew_s=2)
    assert a.lo == T0 - timedelta(seconds=fcap.UNDECLARED_CACHE_BOUND_S + 2)
    assert a.hi == T0 + timedelta(seconds=3)
    assert a.assumptions == (fcap.UNDECLARED_CACHE_ASSUMPTION,)
    assert _rec("public, max-age=15", "1").as_of(clock_skew_s=0).assumptions == ()


# ---- end to end: captured bodies -> store -> parse -> resolve -------------

def _manifest():
    out = {}
    for n in ("manifest.json", "manifest-extra.json"):
        for m in json.loads((RAW / n).read_text()):
            if m.get("file"):
                out[m["file"]] = m
    return out


def _replay(name, cache_control="public, max-age=15"):
    m = _manifest()[name]
    h = {"Date": m.get("response_date") or "", "Cache-Control": m.get("cache_control") or cache_control}
    if m.get("age"):
        h["Age"] = m["age"]
    return RawResponse(200, h, (RAW / name).read_bytes())


def test_end_to_end_replay_of_captured_bodies(tmp_path):
    routes = {
        "/series/KXBTC15M": _replay("series-btc15m.json"),
        ("/series/fee_changes", (("show_historical", "true"),)): _replay("series-fee-changes-historical.json"),
        "/events/KXBTC15M-26OCT300700": _replay("event-btc15m.json"),
        _key(ev="KXBTC15M-26OCT300700"): _replay("event-fee-changes.json"),
    }
    market_body = json.dumps({"market": json.loads((RAW / "markets-btc15m.json").read_bytes())["markets"][0]})
    routes["/markets/KXBTC15M-26OCT300700-00"] = RawResponse(200, HDRS, market_body.encode())
    c = capturer(tmp_path, routes)
    b = c.new_batch()
    ctx = {"assumptions": ["synthetic-test-assumption"]}

    async def go():
        return (await c.series("KXBTC15M", batch_id=b, context=ctx),
                await c.series_fee_changes(show_historical=True, batch_id=b, context=ctx),
                await c.event("KXBTC15M-26OCT300700", batch_id=b, context=ctx),
                await c.event_fee_changes(event_ticker="KXBTC15M-26OCT300700", batch_id=b, context=ctx),
                await c.market("KXBTC15M-26OCT300700-00", batch_id=b, context=ctx))
    (s_rec, s_body), (sc_rec, sc_body), (e_rec, e_body), pages, (m_rec, m_body) = run(go())

    skew = 15
    cov = fs.Coverage(fs.ASSUMED, start=datetime(2025, 1, 1, tzinfo=UTC),
                      end=T0 + timedelta(hours=1), scope=frozenset({fs.ALL_TICKERS}),
                      evidence=(sc_rec.capture_id, pages[0][0].capture_id),
                      assumption_ids=("synthetic-test-assumption",))
    series = fs.parse_series(fs.loads(s_body), s_rec.as_of(clock_skew_s=skew))
    sc = fs.parse_series_fee_changes(fs.loads(sc_body), sc_rec.as_of(clock_skew_s=skew),
                                     include_historical=True, coverage=cov)
    ev = fs.parse_event(fs.loads(e_body), e_rec.as_of(clock_skew_s=skew))
    ec = fs.parse_event_fee_change_pages([fs.loads(bd) for _, bd in pages],
                                         pages[0][0].as_of(clock_skew_s=skew), coverage=cov)
    mk = fs.parse_market(fs.loads(m_body)["market"], m_rec.as_of(clock_skew_s=skew))
    at = max(x.as_of.hi for x in (series, sc, ev, ec, mk)) + timedelta(seconds=1)
    r = fs.resolve_fee_schedule(market=mk, series=series, series_changes=sc, event=ev,
                                event_changes=ec, role=fs.TAKER, at=at, ttl=timedelta(minutes=5))
    assert (r.status, r.coefficient) == (fs.RESOLVED, D("0.07"))
    assert r.assumptions == ("synthetic-test-assumption",)
    assert not fs.admissible(r, ()) and fs.admissible(r, {"synthetic-test-assumption"})
    # Every input is traceable to a stored, hash-checked capture in one batch.
    recs = list(c.store.records())
    assert len(recs) == 5 and {x.batch_id for x in recs} == {b}
    assert all(x.context == ctx for x in recs)
    for x in recs:
        c.store.get(x.capture_id)


# ---- review fix for 8225be9: existing corrupted body -------------------------

def test_put_body_verifies_existing_file():
    import tempfile
    s = CaptureStore(tempfile.mkdtemp())
    d = s.put_body(b"good")
    (s.bodies / d).write_bytes(b"bad")
    with pytest.raises(FeeCaptureError):
        s.put_body(b"good")
    assert (s.bodies / d).read_bytes() == b"bad"   # corrupted evidence preserved


def test_recapture_onto_corrupted_body_fails_without_record(tmp_path):
    body = b'{"series":{"ticker":"S","fee_type":"quadratic","fee_multiplier":1}}'
    c = capturer(tmp_path, {"/series/S": RawResponse(200, HDRS, body)})
    rec, _ = run(c.series("S", batch_id="b"))
    path = tmp_path / "cap" / "bodies" / rec.body_sha256
    path.write_bytes(b"tampered")
    with pytest.raises(FeeCaptureError):
        run(c.series("S", batch_id="b"))
    recs = list(c.store.records())
    assert [r.capture_id for r in recs] == [rec.capture_id]   # no new record appended
    assert path.read_bytes() == b"tampered"
