"""Tests for research_preflight.py switch/report logic (no network).
Stdlib-only: python -m pytest tests/test_research_preflight.py --noconftest
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import research_preflight as pf  # noqa: E402


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("KRYPT_TRADER_USERDATA", str(tmp_path))
    return pf.data_dir()


MID = "ab" * 32
OTHER = "cd" * 32


@pytest.fixture
def build(monkeypatch):
    import research_c15 as rx
    monkeypatch.setattr(rx, "manifest_body", lambda base_dir=None: {"fixed": True})
    monkeypatch.setattr(rx, "manifest_id", lambda body: MID)


def report(data, *, passed=True, age=timedelta(0), manifest=MID):
    (data / pf.REPORT_NAME).write_text(json.dumps(
        {"at": (datetime.now(timezone.utc) - age).isoformat(), "passed": passed,
         "manifest_id": manifest, "items": []}))


def test_switch_defaults_to_disabled(data):
    assert pf.read_switch() == {"present": False, "enabled": False, "manifest_id": None}
    assert not pf.experiment_enabled()
    for bad in ("{not json", json.dumps({"enabled": "yes", "manifest_id": MID}),
                json.dumps({"enabled": True}), json.dumps({"enabled": True, "manifest_id": "abc"}),
                "[]", "null", "true", "7", json.dumps([{"enabled": True, "manifest_id": MID}])):
        (data / pf.SWITCH_NAME).write_text(bad)
        assert pf.read_switch()["present"] and not pf.experiment_enabled()


def test_enable_requires_recent_passing_preflight_for_this_manifest(data, build):
    assert pf.set_switch(True) == 1 and not pf.experiment_enabled()       # no report
    for kw in (dict(passed=False), dict(age=timedelta(hours=25)),
               dict(age=-timedelta(minutes=5)),                          # future-dated
               dict(manifest=OTHER), dict(manifest=None)):
        report(data, **kw)
        assert pf.set_switch(True) == 1 and not pf.experiment_enabled(), kw
    report(data, age=-timedelta(seconds=10))                             # clock jitter ok
    assert pf.set_switch(True) == 0
    assert pf.read_switch() == {"present": True, "enabled": True, "manifest_id": MID}
    assert pf.experiment_enabled(MID) and not pf.experiment_enabled(OTHER)


def test_explicit_disable_always_wins(data, build):
    report(data)
    assert pf.set_switch(True) == 0 and pf.experiment_enabled(MID)
    assert pf.set_switch(False) == 0
    assert not pf.experiment_enabled() and not pf.experiment_enabled(MID)


def test_disable_needs_no_report(data):
    assert pf.set_switch(False) == 0 and pf.read_switch()["enabled"] is False


def test_report_records_manifest_and_needs_it_to_pass(data):
    r = pf.Report()
    r.add("ok", True)
    pf.write_report(r)
    rep = json.loads((data / pf.REPORT_NAME).read_text())
    assert rep["passed"] is False and rep["manifest_id"] is None          # manifest unknown
    r.manifest_id = MID
    pf.write_report(r)
    rep = json.loads((data / pf.REPORT_NAME).read_text())
    assert rep["passed"] is True and rep["manifest_id"] == MID


def test_clock_check_refuses_cached_offset():
    import time
    import types

    def fake(ok):
        ka = types.SimpleNamespace(last_sync_ok_at=0.0)

        def sync(force=False):
            if ok:
                ka.last_sync_ok_at = time.time()
            return 1234                                  # cached value either way
        ka.sync_server_time = sync
        return ka
    assert pf.clock_check(fake(True))[0] is True
    ok, why = pf.clock_check(fake(False))
    assert ok is False and "cached" in why


def test_preflight_module_is_in_the_frozen_hashes():
    import research_c15 as rx
    assert "research_preflight.py" in rx.HASHED_FILES


def test_report_pass_logic():
    r = pf.Report()
    r.add("required ok", True)
    r.add("optional failed", False, required=False)
    assert r.passed
    r.add("required failed", False)
    assert not r.passed


def test_default_user_data_matches_electron(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setenv("APPDATA", str(tmp_path))
    assert pf.default_user_data() == tmp_path / "Krypt Trader"
