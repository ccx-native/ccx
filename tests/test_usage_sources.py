"""Claude quota transport, source selection and shared process cache: no live calls."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from ccx.claude import usage as U
from ccx.claude import usage_control
from ccx.errors import InvalidRequest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    root = tmp_path / "ccx"
    root.mkdir(mode=0o700)
    monkeypatch.setattr(U, "usage_cache_path", lambda: root / "usage-cache.json")
    monkeypatch.setattr(U, "usage_source_path", lambda: root / "usage-source.json")
    return root


@pytest.fixture
def fake_claude(tmp_path, monkeypatch):
    script = tmp_path / "claude"
    script.write_text('''#!/usr/bin/env python3
import json, os, sys, time
with open(os.environ["CCX_FAKE_LAUNCH_LOG"], "a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
requests = [json.loads(sys.stdin.readline()) for _ in range(2)]
assert [r["request"]["subtype"] for r in requests] == ["initialize", "get_usage"]
assert requests[1]["request"]["skip_behaviors"] is True
scenario = os.environ.get("CCX_FAKE_SCENARIO", "success")
if scenario == "timeout":
    time.sleep(10)
    sys.exit(1)
if scenario == "garbage":
    print("this is not JSON", flush=True)
    sys.exit(0)
with open(os.environ["CCX_FAKE_FIXTURE"]) as handle:
    reply = json.load(handle)
reply["response"]["request_id"] = requests[1]["request_id"]
if scenario == "error":
    reply["response"]["subtype"] = "error"
    reply["response"].pop("response", None)
if scenario == "unexpected":
    reply["response"]["response"]["rate_limits"] = {"unknown_future_field": {"example": True}}
print(json.dumps(reply), flush=True)
sys.stdin.read()
if scenario == "nonzero":
    sys.exit(2)
''')
    script.chmod(0o700)
    launch_log = tmp_path / "launch.log"
    monkeypatch.setenv("CCX_FAKE_LAUNCH_LOG", str(launch_log))
    monkeypatch.setenv("CCX_FAKE_FIXTURE", str(FIXTURES / "usage-getusage-success.json"))
    monkeypatch.setattr(usage_control, "CLAUDE_BIN", str(script))
    return script, launch_log


def test_success_is_near_identity_and_model_catalog_deduplicates(isolated, fake_claude, monkeypatch):
    monkeypatch.setattr(U, "oauth_token", lambda: pytest.fail("default source read credentials"))
    data, origin = U.fetch_usage_for_display(force=True)
    body = json.loads((FIXTURES / "usage-getusage-success.json").read_text())["response"]["response"]["rate_limits"]
    assert {k: v for k, v in data.items() if k != "_ccx"} == body
    assert data["_ccx"]["source"] == "get_usage"
    assert data["_ccx"]["held"] is False
    assert "stale" not in data["_ccx"]
    assert "perModelState" not in data["_ccx"]
    assert origin["source"] == "live"
    assert len(U.scoped_weekly_limits(data)) == 1
    assert U.scoped_weekly_limits(data)[0]["model_id"] == "model-synthetic-001"
    assert U.cap_tripped(data, {"5h": 20}) == ("5h", 25.0, 20.0)
    assert U.cap_tripped(data, {"opus": 1}) is None
    assert U.read_usage_cached(stale_limit=120) == data
    argv = json.loads(fake_claude[1].read_text().splitlines()[0])
    assert argv == list(usage_control._PROBE_ARGS)


def test_held_keeps_last_known_stale_and_doubles_with_ceiling(isolated, fake_claude, monkeypatch):
    data = U.fetch_usage(force=True)
    assert data["five_hour"]["utilization"] == 25
    monkeypatch.setenv("CCX_FAKE_FIXTURE", str(FIXTURES / "usage-getusage-held.json"))
    prior_meta = data["_ccx"]
    intervals = []
    for count in range(4):
        with U._usage_lock(1):
            previous = U._matching_cache("get_usage")
            at = U.time.time() + count * 60
            held = U._apply_sample("get_usage", previous,
                                   usage_control.request_usage(), at)
            U._write_usage_cache(held)
        meta = held["_ccx"]
        intervals.append(round(U._parse_epoch(meta["heldUntil"]) - at))
        assert meta["fetchedAt"] == prior_meta["fetchedAt"]
        assert meta["heldUntilEstimated"] is True
        assert U.cap_tripped(held, {"5h": 1}) is None
        assert U.read_usage_cached(stale_limit=120) is None
        cached = U.fetch_usage(force=True)
        assert cached["_ccx"]["held"] is True
    assert intervals == [300, 600, 1200, 1800]
    assert len(fake_claude[1].read_text().splitlines()) == 5
    recovered = U._apply_sample("get_usage", held,
        {"kind": "success", "body": {"five_hour": {"utilization": 5}}}, U.time.time())
    assert recovered["_ccx"]["held"] is False
    next_hold = U._apply_sample("get_usage", recovered, {"kind": "held"}, U.time.time())
    assert round(U._parse_epoch(next_hold["_ccx"]["heldUntil"]) - U.time.time()) == 300


@pytest.mark.parametrize("scenario, expected", [
    ("error", "error"), ("garbage", "unparsed"),
    ("unexpected", "unparsed"), ("nonzero", "error"),
    ("timeout", "error"),
])
def test_fake_control_error_garbage_and_timeout(isolated, fake_claude, monkeypatch, scenario, expected):
    monkeypatch.setenv("CCX_FAKE_SCENARIO", scenario)
    event = usage_control.request_usage(timeout=0.5 if scenario == "timeout" else 3.0)
    assert event["kind"] == expected
    if expected == "unparsed":
        cached = U._apply_sample("get_usage", None, event, U.time.time())
        assert cached["_ccx"]["unparsed"] is True
        assert U.usage_summary(cached)["raw"] == event["raw"]
        assert U.cap_tripped(cached, {"5h": 0}) is None


def test_model_scoped_only_is_summary_fallback_not_new_limits(isolated):
    data = {"model_scoped": [{"display_name": "Opus", "utilization": 75,
                              "resets_at": "2030-01-05T00:00:00Z"}],
            "seven_day_opus": None}
    assert "limits" not in data
    quotas = U.scoped_weekly_limits(data)
    assert quotas == [{"model": "Opus", "utilization": 75.0,
                       "resets_at": "2030-01-05T00:00:00Z"}]
    assert U._metric_util(data, "opus") == 75
    assert "limits" not in data
    for bad in (None, float("nan"), True, -1, 105):
        data["model_scoped"][0]["utilization"] = bad
        assert U.scoped_weekly_limits(data) == []
        assert U._metric_util(data, "opus") is None


@pytest.mark.parametrize("platform", ["linux", "darwin"])
def test_two_sources_are_persistent_and_invalidate_cache(
    isolated, fake_claude, monkeypatch, platform,
):
    # Exercise the exact platform decision used by the product, even when
    # running these provider-free tests on a different host OS.
    monkeypatch.setattr(U.oslayer.sys, "platform", platform)
    assert U.oslayer.direct_oauth_usage_supported() is (platform == "linux")
    first = U.fetch_usage(force=True)
    assert first["five_hour"]["utilization"] == 25
    if platform != "linux":
        cached = U.usage_cache_path().read_bytes()
        with pytest.raises(InvalidRequest, match="direct OAuth usage is Linux-only; use get_usage"):
            U.set_usage_source("oauth")
        assert U._usage_source() == "get_usage"
        assert not U.usage_source_path().exists()
        assert U.usage_cache_path().read_bytes() == cached
        assert U.read_usage_cached(stale_limit=120) == first
        with pytest.raises(InvalidRequest, match="must be"):
            U.set_usage_source("garbage")
        return

    assert U.set_usage_source("oauth") == "oauth"
    assert U._usage_source() == "oauth"
    assert U.read_usage_cached(stale_limit=120) is None
    assert "five_hour" not in U._read_usage_cache()
    monkeypatch.setattr(U, "_oauth_sample", lambda timeout: {"kind": "success", "body": {
        "seven_day": {"utilization": 95}, "limits": []}})
    oauth = U.fetch_usage(force=True)
    assert oauth["_ccx"]["source"] == "oauth"
    assert "five_hour" not in oauth
    # A persisted Linux OAuth selector must also be rejected when the same
    # installation is read on a simulated macOS host.
    monkeypatch.setattr(U.oslayer.sys, "platform", "darwin")
    with pytest.raises(InvalidRequest, match="Claude OAuth usage source is Linux-only; select get_usage"):
        U._usage_source()
    monkeypatch.setattr(U.oslayer.sys, "platform", "linux")
    assert U.set_usage_source("get_usage") == "get_usage"
    assert U._usage_source() == "get_usage"
    assert U.read_usage_cached(stale_limit=120) is None
    monkeypatch.setattr(U.oslayer.sys, "platform", "darwin")
    with pytest.raises(InvalidRequest, match="direct OAuth usage is Linux-only; use get_usage"):
        U.set_usage_source("oauth")
    assert U._usage_source() == "get_usage"
    with pytest.raises(InvalidRequest, match="must be"):
        U.set_usage_source("garbage")


def test_settings_symlink_not_followed_or_used(isolated, monkeypatch):
    target = isolated / "other.json"
    target.write_text(json.dumps({"version": 1, "source": "oauth"}))
    U.usage_source_path().symlink_to(target)
    with pytest.raises(InvalidRequest, match="invalid private"):
        U._usage_source()
    assert U.fetch_usage(force=True) is None


def test_nonhold_errors_fixed_120_and_unparseable_raw(isolated):
    t = 1800000000.0
    fresh = U._apply_sample("get_usage", None, {"kind": "success", "body": {
        "five_hour": {"utilization": 50}}}, t)
    err = U._apply_sample("get_usage", fresh, {"kind": "error", "error": "offline"}, t + 60)
    assert U._parse_epoch(err["_ccx"]["nextAttemptAt"]) == t + 180
    assert err["_ccx"]["fetchedAt"] == fresh["_ccx"]["fetchedAt"]
    assert err["_ccx"]["held"] is False
    assert U.cap_tripped(err, {"5h": 1}) is None
    assert U._refresh_due(err, force=True, max_age=0, now=t + 179) is False
    raw = {"future": {"unknown": 1}}
    unparsed = U._apply_sample("get_usage", err, {"kind": "unparsed", "raw": raw}, t + 180)
    assert unparsed["_ccx"]["raw"] == raw
    assert "stale" not in unparsed["_ccx"]
    assert U.usage_summary(unparsed)["unparsed"] is True


def test_processes_singleflight_even_when_forced(isolated, fake_claude):
    """A handful of independent CCX imports cause at most one Claude launch."""
    root = isolated
    env = dict(os.environ, CCX_ROOT=str(root), CCX_USAGE_CACHE=str(U.usage_cache_path()),
               CCX_CLAUDE_BIN=str(fake_claude[0]))
    cmd = [sys.executable, "-c", "from ccx.claude.usage import fetch_usage; assert fetch_usage(force=True, max_age=0)['five_hour']['utilization'] == 25"]
    processes = [subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.PIPE) for _ in range(5)]
    for proc in processes:
        _, stderr = proc.communicate(timeout=15)
        assert proc.returncode == 0, stderr.decode(errors="replace")
    assert len(fake_claude[1].read_text().splitlines()) == 1
    assert U.fetch_usage(force=True)["five_hour"]["utilization"] == 25
    assert len(fake_claude[1].read_text().splitlines()) == 1


@pytest.mark.parametrize("source,floor,stale_at_200s", [
    ("get_usage", 60.0, True),
    ("oauth", 120.0, False),
])
def test_oauth_reads_are_spaced_and_caps_accept_the_older_sample(source, floor, stale_at_200s):
    body = {"five_hour": {"utilization": 10.0}}
    now = time.time() - 200.0
    sample = U._apply_sample(source, None, {"kind": "success", "body": body}, now)
    meta = sample["_ccx"]
    assert U._parse_epoch(meta["nextAttemptAt"]) - U._parse_epoch(meta["fetchedAt"]) == pytest.approx(floor, abs=0.01)
    assert U._is_stale(sample, U.USAGE_STALE_LIMIT_S) is stale_at_200s
    assert U._is_stale(dict(sample, _ccx=dict(meta, fetchedAt=U._utc_timestamp(now - 200.0))))
