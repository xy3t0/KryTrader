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


def report(data, *, passed=True, age=timedelta(0)):
    (data / pf.REPORT_NAME).write_text(json.dumps(
        {"at": (datetime.now(timezone.utc) - age).isoformat(), "passed": passed, "items": []}))


def test_switch_defaults_to_disabled(data):
    assert pf.read_switch() == {"enabled": False, "activate": None}
    (data / pf.SWITCH_NAME).write_text("{not json")
    assert pf.read_switch()["enabled"] is False
    (data / pf.SWITCH_NAME).write_text(json.dumps({"enabled": "yes"}))
    assert pf.read_switch()["enabled"] is False           # only literal true enables


def test_enable_requires_recent_passing_preflight(data):
    assert pf.set_switch(True) == 1 and not pf.read_switch()["enabled"]
    report(data, passed=False)
    assert pf.set_switch(True) == 1
    report(data, passed=True, age=timedelta(hours=25))
    assert pf.set_switch(True) == 1
    report(data, passed=True)
    assert pf.set_switch(True, "abcdef123456") == 0
    assert pf.read_switch() == {"enabled": True, "activate": "abcdef123456"}


def test_disable_needs_no_report(data):
    assert pf.set_switch(False) == 0 and pf.read_switch()["enabled"] is False


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
