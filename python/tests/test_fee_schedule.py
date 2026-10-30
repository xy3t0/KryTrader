"""Tests for fee_schedule.py (pure resolver + strict parsers).

Fixture provenance is labelled on every fixture:
  OBSERVED  field values from live unauthenticated GETs captured 2026-10-30
            (Codex capture bundle; excerpts, other fields omitted)
  SYNTHETIC constructed; no live example observed (waivers, event
            overrides, mixed nulls, boundary conflicts)

Stdlib-only: python -m pytest tests/test_fee_schedule.py --noconftest
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fee_schedule as fs  # noqa: E402
from fee_schedule import (  # noqa: E402
    EXPLICIT_ZERO, MAKER, RESOLVED, TAKER, UNAVAILABLE, UNSUPPORTED,
    WAIVER_UNRESOLVED, AsOf, FeeMetadataError,
)

UTC = timezone.utc
T0 = datetime(2026, 10, 30, 10, 50, 0, tzinfo=UTC)   # snapshot taken around here
AS_OF = AsOf(T0 - timedelta(seconds=40), T0 + timedelta(seconds=2))
TTL = timedelta(minutes=5)
AT = T0 + timedelta(seconds=30)                      # after the snapshot window


def J(text: str):
    return fs.loads(text)


# ---- OBSERVED fixtures ------------------------------------------------------

OBS_SERIES_BTC15M = '{"series":{"ticker":"KXBTC15M","fee_type":"quadratic","fee_multiplier":1}}'
OBS_SERIES_BTCY = '{"series":{"ticker":"KXBTCY","fee_type":"quadratic","fee_multiplier":0}}'
OBS_SERIES_COMBO = ('{"series":{"ticker":"KXMVECROSSCATEGORY",'
                    '"fee_type":"quadratic_with_combo_maker_fees","fee_multiplier":1}}')
OBS_SERIES_CHANGES_DEFAULT = '{"series_fee_change_arr":[]}'
OBS_SERIES_CHANGE_BTCY = ('{"series_fee_change_arr":[{"fee_multiplier":0,"fee_type":"quadratic",'
                          '"id":"80e167b5-0272-438c-b68f-4cd2c9f470b3",'
                          '"scheduled_ts":"2026-02-26T03:15:00Z","series_ticker":"KXBTCY"}]}')
OBS_EVENT_CHANGES_EMPTY = '{"cursor":"","event_fee_changes":[]}'
# Observed: BTC15M event omitted both override fields; last_updated_ts zero value.
OBS_MARKET_BTC15M = {"ticker": "KXBTC15M-26OCT300700-00", "event_ticker": "KXBTC15M-26OCT300700",
                     "status": "active", "price_level_structure": "tapered_deci_cent"}


def series(text=OBS_SERIES_BTC15M, as_of=AS_OF):
    return fs.parse_series(J(text), as_of)


def series_changes(text=OBS_SERIES_CHANGES_DEFAULT, *, hist=True, as_of=AS_OF):
    return fs.parse_series_fee_changes(J(text), as_of, include_historical=hist)


def event(event_ticker, series_ticker, extra="", as_of=AS_OF):
    body = f'"event_ticker":"{event_ticker}","series_ticker":"{series_ticker}"{extra}'
    return fs.parse_event(J('{"event":{' + body + '}}'), as_of)


def event_changes(*pages, as_of=AS_OF):
    pages = pages or (OBS_EVENT_CHANGES_EMPTY,)
    return fs.parse_event_fee_change_pages([J(p) for p in pages], as_of)


def market(ticker, event_ticker, **extra):
    return fs.parse_market({"ticker": ticker, "event_ticker": event_ticker, **extra})


def resolve(*, s=None, sc=None, e=None, ec=None, m=None, role=TAKER, at=AT, ttl=TTL):
    s = s or series()
    e = e or event("KXBTC15M-26OCT300700", s.ticker)
    m = m or market("KXBTC15M-26OCT300700-00", e.event_ticker)
    return fs.resolve_fee_schedule(
        market=m, series=s, series_changes=sc or series_changes(),
        event=e, event_changes=ec or event_changes(), role=role, at=at, ttl=ttl,
    )


# ---- observed baselines ------------------------------------------------------

def test_observed_btc15m_taker_is_007():
    r = resolve()
    assert (r.status, r.reason) == (RESOLVED, "ok")
    assert r.fee_type == "quadratic" and r.multiplier == 1
    assert r.coefficient == D("0.07")
    assert r.provenance["type_source"] == "series"
    assert r.provenance["event_type_override_field"] == "omitted"


def test_observed_btc15m_maker_has_no_fee():
    r = resolve(role=MAKER)
    assert (r.status, r.reason, r.coefficient) == (EXPLICIT_ZERO, "no_maker_fee_for_type", 0)


def test_observed_btcy_multiplier_zero():
    s = series(OBS_SERIES_BTCY)
    r = resolve(s=s, e=event("KXBTCY-26DEC31", "KXBTCY"), m=market("KXBTCY-26DEC31-B1", "KXBTCY-26DEC31"))
    assert (r.status, r.reason, r.coefficient) == (EXPLICIT_ZERO, "multiplier_zero", 0)


@pytest.mark.parametrize("role,coeff", [(TAKER, D("0.07")), (MAKER, D("0.035"))])
def test_observed_combo_coefficients(role, coeff):
    s = series(OBS_SERIES_COMBO)
    r = resolve(s=s, e=event("KXMVECROSSCATEGORY-X", s.ticker),
                m=market("KXMVECROSSCATEGORY-X-1", "KXMVECROSSCATEGORY-X"), role=role)
    assert (r.status, r.coefficient) == (RESOLVED, coeff)


def test_observed_historical_change_before_window_is_ignored():
    # Feb 2026 change long predates the snapshot: already reflected, not reapplied.
    s = series(OBS_SERIES_BTCY)
    r = resolve(s=s, sc=series_changes(OBS_SERIES_CHANGE_BTCY),
                e=event("KXBTCY-26DEC31", "KXBTCY"), m=market("KXBTCY-26DEC31-B1", "KXBTCY-26DEC31"))
    assert r.status == EXPLICIT_ZERO and r.applied_change_ids == ()


def test_observed_market_waiver_field_omitted():
    m = fs.parse_market(OBS_MARKET_BTC15M)
    assert m.waiver_expiry is None and m.waiver_field == "omitted"


def test_observed_zero_last_updated_is_unusable_not_a_baseline():
    s = fs.parse_series(J('{"series":{"ticker":"KXBTC15M","fee_type":"quadratic",'
                          '"fee_multiplier":1,"last_updated_ts":"0001-01-01T00:00:00Z"}}'), AS_OF)
    assert s.last_updated is None


def test_observed_default_change_list_alone_is_insufficient():
    r = resolve(sc=series_changes(hist=False))
    assert (r.status, r.reason) == (UNAVAILABLE, "change_history_required")


# ---- multiplier parsing --------------------------------------------------

@pytest.mark.parametrize("token,expected", [("1", D(1)), ("0", D(0)), ("0.5", D("0.5")),
                                            ("1.0", D(1)), ("2E0", D(2))])
def test_multiplier_numeric_spellings_accepted(token, expected):
    s = series('{"series":{"ticker":"S","fee_type":"quadratic","fee_multiplier":' + token + '}}')
    assert s.multiplier == expected


@pytest.mark.parametrize("token", ['"1"', "true", "null", "-1", "0.0000001", "NaN", "Infinity"])
def test_multiplier_malformed_rejected(token):
    with pytest.raises(FeeMetadataError):
        series('{"series":{"ticker":"S","fee_type":"quadratic","fee_multiplier":' + token + '}}')


def test_plain_json_float_rejected():
    import json
    payload = json.loads('{"series":{"ticker":"S","fee_type":"quadratic","fee_multiplier":0.5}}')
    with pytest.raises(FeeMetadataError):
        fs.parse_series(payload, AS_OF)


@pytest.mark.parametrize("text", [
    '{"series":{"fee_type":"quadratic","fee_multiplier":1}}',
    '{"series":{"ticker":"S","fee_multiplier":1}}',
    '{"series":{"ticker":"S","fee_type":"quadratic"}}',
    '{"series":{"ticker":"S","fee_type":7,"fee_multiplier":1}}',
    '{"nope":{}}',
])
def test_series_missing_or_malformed_fields(text):
    with pytest.raises(FeeMetadataError):
        series(text)


# ---- fee types -----------------------------------------------------------

@pytest.mark.parametrize("ftype", ["flat", "tiered_2027"])
def test_unsupported_fee_types(ftype):  # SYNTHETIC: none observed live
    s = series('{"series":{"ticker":"KXBTC15M","fee_type":"' + ftype + '","fee_multiplier":1}}')
    r = resolve(s=s)
    assert r.status == UNSUPPORTED and r.coefficient is None and not r.usable


def test_maker_fee_type_coefficients():  # SYNTHETIC multiplier 0.5 (0.5 was an observed spelling)
    s = series('{"series":{"ticker":"KXBTC15M","fee_type":"quadratic_with_maker_fees","fee_multiplier":0.5}}')
    assert resolve(s=s, role=TAKER).coefficient == D("0.035")
    assert resolve(s=s, role=MAKER).coefficient == D("0.00875")


# ---- event overrides (all SYNTHETIC) --------------------------------------

def test_event_snapshot_override_applies():
    e = event("KXBTC15M-26OCT300700", "KXBTC15M",
              ',"fee_type_override":"quadratic_with_maker_fees","fee_multiplier_override":2')
    r = resolve(e=e, role=MAKER)
    assert (r.status, r.coefficient) == (RESOLVED, D("0.035"))
    assert r.provenance["type_source"] == r.provenance["multiplier_source"] == "event"


def test_event_snapshot_mixed_null_keeps_series_type():
    e = event("KXBTC15M-26OCT300700", "KXBTC15M",
              ',"fee_type_override":null,"fee_multiplier_override":0.5')
    r = resolve(e=e)
    assert r.fee_type == "quadratic" and r.multiplier == D("0.5")
    assert r.provenance["type_source"] == "series"
    assert r.provenance["event_type_override_field"] == "null"


def _ev_change(cid, ts, t="null", m="null", ev="KXBTC15M-26OCT300700"):
    t = t if t == "null" else f'"{t}"'
    return (f'{{"id":"{cid}","event_ticker":"{ev}","series_ticker":"KXBTC15M",'
            f'"fee_type_override":{t},"fee_multiplier_override":{m},"scheduled_ts":"{ts}"}}')


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def test_scheduled_event_change_after_snapshot_applies():
    ch = _ev_change("ec1", _iso(T0 + timedelta(seconds=20)), m="3")
    r = resolve(ec=event_changes('{"cursor":"","event_fee_changes":[' + ch + ']}'))
    assert r.coefficient == D("0.21") and r.applied_change_ids == ("ec1",)


def test_event_change_null_clears_snapshot_override():
    e = event("KXBTC15M-26OCT300700", "KXBTC15M", ',"fee_multiplier_override":3')
    ch = _ev_change("ec1", _iso(T0 + timedelta(seconds=20)))  # both null: clear
    r = resolve(e=e, ec=event_changes('{"cursor":"","event_fee_changes":[' + ch + ']}'))
    assert r.multiplier == 1 and r.provenance["multiplier_source"] == "series"


def test_change_not_yet_effective_bounds_validity():
    nxt = T0 + timedelta(seconds=90)
    ch = _ev_change("ec1", _iso(nxt), m="3")
    r = resolve(ec=event_changes('{"cursor":"","event_fee_changes":[' + ch + ']}'))
    assert r.coefficient == D("0.07") and r.valid_until == nxt


def test_event_change_missing_nullable_field_is_malformed():
    bad = ('{"cursor":"","event_fee_changes":[{"id":"x","event_ticker":"E","series_ticker":"S",'
           '"fee_type_override":null,"scheduled_ts":"2026-10-30T10:50:00Z"}]}')
    with pytest.raises(FeeMetadataError):
        event_changes(bad)


def test_event_change_pagination_must_complete():
    with pytest.raises(FeeMetadataError):
        event_changes('{"cursor":"abc","event_fee_changes":[]}')
    with pytest.raises(FeeMetadataError):
        event_changes('{"cursor":"","event_fee_changes":[]}', '{"cursor":"","event_fee_changes":[]}')
    two = event_changes('{"cursor":"abc","event_fee_changes":[]}', '{"cursor":"","event_fee_changes":[]}')
    assert two.changes == ()


# ---- snapshot boundary handling (SYNTHETIC) -------------------------------

def _series_change(cid, ts, mult, ftype="quadratic", ticker="KXBTC15M"):
    return (f'{{"fee_multiplier":{mult},"fee_type":"{ftype}","id":"{cid}",'
            f'"scheduled_ts":"{ts}","series_ticker":"{ticker}"}}')


def _sc(*changes):
    return series_changes('{"series_fee_change_arr":[' + ",".join(changes) + ']}')


def test_change_inside_window_conflicting_with_snapshot_is_unavailable():
    in_window = _iso(T0 - timedelta(seconds=10))
    r = resolve(sc=_sc(_series_change("c1", in_window, 2)))
    assert (r.status, r.reason) == (UNAVAILABLE, "snapshot_boundary_conflict:series")
    assert r.coefficient is None


def test_change_inside_window_agreeing_with_snapshot_is_fine_after_window():
    in_window = _iso(T0 - timedelta(seconds=10))
    r = resolve(sc=_sc(_series_change("c1", in_window, 1)))
    assert r.status == RESOLVED and r.coefficient == D("0.07")


def test_at_inside_window_with_any_change_is_unavailable():
    in_window = _iso(T0 - timedelta(seconds=10))
    r = resolve(sc=_sc(_series_change("c1", in_window, 1)), at=T0 - timedelta(seconds=20))
    assert r.status == UNAVAILABLE


def test_at_inside_window_without_changes_uses_snapshot():
    r = resolve(at=T0 - timedelta(seconds=20))
    assert r.status == RESOLVED


def test_before_snapshot_fails_closed():
    r = resolve(at=AS_OF.lo - timedelta(seconds=1))
    assert (r.status, r.reason) == (UNAVAILABLE, "before_snapshot:series")


def test_series_change_after_snapshot_applies():
    later = _iso(T0 + timedelta(seconds=10))
    r = resolve(sc=_sc(_series_change("c2", later, D("0.5"), "quadratic_with_maker_fees")), role=MAKER)
    assert r.fee_type == "quadratic_with_maker_fees" and r.coefficient == D("0.00875")
    assert r.applied_change_ids == ("c2",)


def test_changes_for_other_series_are_ignored():
    later = _iso(T0 + timedelta(seconds=10))
    r = resolve(sc=_sc(_series_change("cx", later, 9, ticker="KXETH15M")))
    assert r.coefficient == D("0.07")


def test_conflicting_tie_is_unavailable_identical_tie_collapses():
    later = _iso(T0 + timedelta(seconds=10))
    r = resolve(sc=_sc(_series_change("a", later, 2), _series_change("b", later, 3)))
    assert (r.status, r.reason) == (UNAVAILABLE, "ambiguous_tie:series")
    r = resolve(sc=_sc(_series_change("a", later, 2), _series_change("b", later, D("2.0"))))
    assert r.coefficient == D("0.14") and set(r.applied_change_ids) == {"a", "b"}


# ---- staleness and validity ---------------------------------------------

def test_stale_snapshot_is_unavailable():
    r = resolve(at=AS_OF.hi + TTL + timedelta(seconds=1))
    assert r.status == UNAVAILABLE and r.reason.startswith("stale:")


def test_valid_until_is_earliest_snapshot_expiry_without_boundaries():
    r = resolve()
    assert r.valid_until == AS_OF.hi + TTL


# ---- waivers (SYNTHETIC) -------------------------------------------------

def test_active_waiver_is_unresolved_not_zero():
    exp = T0 + timedelta(hours=1)
    m = market("KXBTC15M-26OCT300700-00", "KXBTC15M-26OCT300700",
               fee_waiver_expiration_time=_iso(exp))
    r = resolve(m=m)
    assert r.status == WAIVER_UNRESOLVED and r.coefficient is None and not r.usable
    assert m.waiver_field == "set"


def test_expired_waiver_resolves_normally():
    m = market("KXBTC15M-26OCT300700-00", "KXBTC15M-26OCT300700",
               fee_waiver_expiration_time=_iso(T0 - timedelta(days=1)))
    assert resolve(m=m).status == RESOLVED


def test_upcoming_waiver_expiry_bounds_validity():
    exp = AT + timedelta(seconds=5)
    m = market("KXBTC15M-26OCT300700-00", "KXBTC15M-26OCT300700",
               fee_waiver_expiration_time=_iso(exp))
    assert resolve(m=m).valid_until == exp


def test_null_waiver_field_recorded():
    m = market("T", "E", fee_waiver_expiration_time=None)
    assert m.waiver_expiry is None and m.waiver_field == "null"


# ---- input validation ----------------------------------------------------

def test_inconsistent_inputs_raise():
    with pytest.raises(FeeMetadataError):
        resolve(e=event("OTHER-EVENT", "KXBTC15M"),
                m=market("KXBTC15M-26OCT300700-00", "KXBTC15M-26OCT300700"))
    with pytest.raises(FeeMetadataError):
        resolve(e=event("KXBTC15M-26OCT300700", "KXETH15M"))


@pytest.mark.parametrize("kw", [dict(role="both"), dict(at=datetime(2026, 10, 30)),
                                dict(ttl=timedelta(seconds=-1)), dict(ttl=300)])
def test_bad_resolver_arguments(kw):
    with pytest.raises(FeeMetadataError):
        resolve(**kw)


def test_asof_from_http_is_conservative():
    sent = T0
    a = AsOf.from_http(request_sent=sent, response_received=sent + timedelta(seconds=1),
                       cache_max_age_s=15, age_s=4, clock_skew_s=2)
    assert a.lo == sent - timedelta(seconds=21) and a.hi == sent + timedelta(seconds=3)
    with pytest.raises(FeeMetadataError):
        AsOf.from_http(request_sent=sent, response_received=sent - timedelta(seconds=1),
                       cache_max_age_s=15, age_s=0, clock_skew_s=0)
    with pytest.raises(FeeMetadataError):
        AsOf(T0, T0 - timedelta(seconds=1))


@pytest.mark.parametrize("ts", ["2026-10-30T10:50:00", "not-a-time", "0001-01-01T00:00:00Z"])
def test_scheduled_ts_strict(ts):
    with pytest.raises(FeeMetadataError):
        _sc(_series_change("c", ts, 1))


def test_json_constants_rejected():
    with pytest.raises(FeeMetadataError):
        fs.loads('{"a": NaN}')


# ---- OBSERVED raw payloads (untouched bodies, hashes checked) ---------------

import hashlib  # noqa: E402
import json as _json  # noqa: E402

RAW = Path(__file__).resolve().parent / "fixtures" / "kalshi_fee_2026_10_30"


def _manifest():
    out = {}
    for name in ("manifest.json", "manifest-extra.json"):
        for m in _json.loads((RAW / name).read_text()):
            if m.get("file"):
                out[m["file"]] = m
    return out


def _raw(name):
    return fs.loads((RAW / name).read_bytes())


def _raw_as_of(name, skew_s=15):
    """Conservative window from the capture's own manifest: local request
    start, cache max-age and Age, widened by clock skew (the capture's local
    clock ran ~8s behind the server Date header)."""
    m = _manifest()[name]
    sent = datetime.fromisoformat(m["request_started_at"])
    max_age = 0
    cc = m.get("cache_control") or ""
    if "max-age=" in cc:
        max_age = int(cc.split("max-age=")[1].split(",")[0])
    return AsOf.from_http(request_sent=sent, response_received=sent + timedelta(seconds=10),
                          cache_max_age_s=max_age, age_s=int(m.get("age") or 0),
                          clock_skew_s=skew_s)


@pytest.mark.parametrize("name", sorted(p.name for p in RAW.glob("*.json")
                                        if not p.name.startswith("manifest")))
def test_raw_fixture_hashes_match_capture_manifest(name):
    m = _manifest()[name]
    assert hashlib.sha256((RAW / name).read_bytes()).hexdigest() == m["sha256"]


def _raw_btc15m(role):
    ev_payload = _raw("event-btc15m.json")
    s = fs.parse_series(_raw("series-btc15m.json"), _raw_as_of("series-btc15m.json"))
    sc = fs.parse_series_fee_changes(_raw("series-fee-changes-historical.json"),
                                     _raw_as_of("series-fee-changes-historical.json"),
                                     include_historical=True)
    e = fs.parse_event(ev_payload, _raw_as_of("event-btc15m.json"))
    ec = fs.parse_event_fee_change_pages([_raw("event-fee-changes.json")],
                                         _raw_as_of("event-fee-changes.json"))
    m = fs.parse_market(_raw("markets-btc15m.json")["markets"][0])
    at = max(x.as_of.hi for x in (s, sc, e, ec)) + timedelta(seconds=1)
    return fs.resolve_fee_schedule(market=m, series=s, series_changes=sc, event=e,
                                   event_changes=ec, role=role, at=at, ttl=TTL), e, m


def test_raw_btc15m_resolves_end_to_end():
    r, e, m = _raw_btc15m(TAKER)
    assert (r.status, r.coefficient, r.applied_change_ids) == (RESOLVED, D("0.07"), ())
    assert e.type_override_field == e.multiplier_override_field == "omitted"
    assert m.waiver_field == "omitted"
    r, _, _ = _raw_btc15m(MAKER)
    assert (r.status, r.reason) == (EXPLICIT_ZERO, "no_maker_fee_for_type")


def test_raw_btc15m_event_zero_last_updated_present():
    assert _raw("event-btc15m.json")["event"]["last_updated_ts"] == "0001-01-01T00:00:00Z"


def test_raw_btcy_multiplier_zero():
    s = fs.parse_series(_raw("series-btcy.json"), _raw_as_of("series-btcy.json"))
    assert (s.fee_type, s.multiplier) == ("quadratic", 0)


def test_raw_combo_series_parse():
    for i in range(3):
        s = fs.parse_series(_raw(f"series-combo-{i}.json"), _raw_as_of(f"series-combo-{i}.json"))
        assert s.fee_type == "quadratic_with_combo_maker_fees" and s.multiplier == 1


def test_raw_historical_series_changes_all_parse():
    sc = fs.parse_series_fee_changes(_raw("series-fee-changes-historical.json"),
                                     _raw_as_of("series-fee-changes-historical.json"),
                                     include_historical=True)
    assert len(sc.changes) == 107
    assert all(c.fee_type in fs.KNOWN_FEE_TYPES for c in sc.changes)
    assert min(c.scheduled for c in sc.changes) == datetime(2025, 10, 4, 7, 0, tzinfo=UTC)


def test_raw_default_series_changes_empty():
    sc = fs.parse_series_fee_changes(_raw("series-fee-changes.json"),
                                     _raw_as_of("series-fee-changes.json"),
                                     include_historical=False)
    assert sc.changes == ()


def test_raw_404_is_not_a_series():
    with pytest.raises(FeeMetadataError):
        fs.parse_series(_raw("series-kxmve.json"), _raw_as_of("series-kxmve.json"))
