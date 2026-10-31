'Usage-cap math and helpers (ccx.claude.usage).\n\nProvider calls are mocked: tests cover both the pure cap arithmetic and the\nprivate Anthropic reset protocol without making a real network request.\n'

from __future__ import annotations
import ccx.cli.commands.read as commands_read

import argparse
import json
from types import SimpleNamespace
import urllib.parse

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx.claude import usage as U
from ccx.errors import CcxError, InvalidRequest
from test_python_worker_api import active_api_root


# --- read_usage_cached: only source-matching, recently fetched, unheld data --

@pytest.fixture
def private_usage(tmp_path, monkeypatch):
    monkeypatch.setattr(U, "usage_cache_path", lambda: tmp_path / "usage-cache.json")
    monkeypatch.setattr(U, "usage_source_path", lambda: tmp_path / "usage-source.json")
    def write(usage=42, *, age=5, held=False, error=None):
        data = U._apply_sample("get_usage", None, {"kind": "success", "body": {
            "five_hour": {"utilization": usage}}}, U.time.time() - age)
        if held:
            data["_ccx"].update({"held": True, "heldUntil": U._utc_timestamp(U.time.time() + 300)})
        if error:
            data["_ccx"]["error"] = error
        U._write_usage_cache(data)
        return data
    return write


def test_read_usage_cached_returns_fresh_cache(private_usage):
    data = private_usage()
    assert U.read_usage_cached(stale_limit=120.0) == data


def test_read_usage_cached_no_cache_is_none(private_usage):
    assert U.read_usage_cached(stale_limit=120.0) is None


def test_read_usage_cached_stale_fails_open(private_usage):
    private_usage(99, age=999)
    assert U.read_usage_cached(stale_limit=120.0) is None


def test_read_usage_cached_missing_fetched_at_fails_open(private_usage):
    data = private_usage(99)
    data["_ccx"]["fetchedAt"] = None
    U._write_usage_cache(data)
    assert U.read_usage_cached(stale_limit=120.0) is None


def test_read_usage_cached_no_stale_limit_still_exposes_metadata(private_usage):
    data = private_usage(1, age=999)
    assert U.read_usage_cached() == data


def test_read_usage_cached_never_touches_network(private_usage, monkeypatch):
    data = private_usage(10)
    def no_io(*args, **kwargs):
        raise AssertionError("cache-only read performed network or process I/O")
    monkeypatch.setattr(U.urllib.request, "urlopen", no_io)
    monkeypatch.setattr(U, "oauth_token", no_io)
    assert U.read_usage_cached(stale_limit=120.0) == data


def test_read_usage_cached_held_or_failed_fails_open(private_usage):
    private_usage(99, held=True)
    assert U.read_usage_cached(stale_limit=120) is None
    private_usage(99, error="offline")
    assert U.read_usage_cached(stale_limit=120) is None


# --- _metric_util ------------------------------------------------------------

def test_metric_util_present_value():
    data = {"five_hour": {"utilization": 42}}
    assert U._metric_util(data, "5h") == 42.0


def test_metric_util_absent_metric_is_none():
    assert U._metric_util({}, "5h") is None


def test_metric_util_retired_null_opus_field_fails_open():
    assert U._metric_util({"seven_day_opus": None}, "opus") is None


def test_metric_util_opus_uses_dynamic_scoped_limit():
    data = {
        "limits": [{
            "kind": "weekly_scoped",
            "percent": 64,
            "scope": {"model": {"display_name": "Opus"}},
        }],
    }
    assert U._metric_util(data, "opus") == 64.0


def test_metric_util_null_utilization_is_unknown():
    assert U._metric_util({"five_hour": {"utilization": None}}, "5h") is None


def test_metric_util_unknown_key():
    assert U._metric_util({"five_hour": {"utilization": 10}}, "bogus") is None


# --- cap_tripped -------------------------------------------------------------

def test_cap_tripped_when_at_or_over_threshold():
    data = {"five_hour": {"utilization": 90}}
    assert U.cap_tripped(data, {"5h": 80}) == ("5h", 90.0, 80.0)


def test_cap_tripped_exactly_at_threshold_trips():
    data = {"five_hour": {"utilization": 80}}
    assert U.cap_tripped(data, {"5h": 80}) == ("5h", 80.0, 80.0)


def test_cap_not_tripped_below_threshold():
    data = {"five_hour": {"utilization": 50}}
    assert U.cap_tripped(data, {"5h": 80}) is None


def test_cap_tripped_skips_absent_metric_fail_open():
    # Cap set on opus but no opus data present -> fail open (no trip).
    assert U.cap_tripped({"five_hour": {"utilization": 99}}, {"opus": 10}) is None


def test_cap_tripped_none_inputs():
    assert U.cap_tripped(None, {"5h": 80}) is None
    assert U.cap_tripped({"five_hour": {"utilization": 99}}, None) is None


def test_cap_tripped_first_match_wins():
    data = {"five_hour": {"utilization": 95}, "seven_day": {"utilization": 95}}
    res = U.cap_tripped(data, {"5h": 80, "7d": 80})
    assert res[0] in ("5h", "7d")


# --- _pct (argparse type) ----------------------------------------------------

def test_pct_valid():
    assert U._pct("50") == 50.0
    assert U._pct("0") == 0.0
    assert U._pct("100") == 100.0


def test_pct_non_numeric_raises():
    with pytest.raises(argparse.ArgumentTypeError):
        U._pct("abc")


def test_pct_out_of_range_raises():
    with pytest.raises(argparse.ArgumentTypeError):
        U._pct("150")
    with pytest.raises(argparse.ArgumentTypeError):
        U._pct("-1")


# --- _caps_from_args ---------------------------------------------------------

def test_caps_from_args_collects_set_flags():
    args = SimpleNamespace(cap_5h=40, cap_7d=None, cap_opus=80)
    assert U._caps_from_args(args) == {"5h": 40, "opus": 80}


def test_caps_from_args_all_unset_is_none():
    args = SimpleNamespace(cap_5h=None, cap_7d=None, cap_opus=None)
    assert U._caps_from_args(args) is None


def test_caps_from_args_missing_attrs_is_none():
    assert U._caps_from_args(SimpleNamespace()) is None


# --- _merge_caps -------------------------------------------------------------

def test_merge_caps_strictest_wins():
    merged = U._merge_caps([{"5h": 40}, {"5h": 30, "7d": 80}])
    assert merged == {"5h": 30, "7d": 80}


def test_merge_caps_skips_empty_and_none():
    assert U._merge_caps([None, {}, {"opus": 10}]) == {"opus": 10}


def test_merge_caps_all_empty_is_none():
    assert U._merge_caps([None, {}, {}]) is None


# --- usage_summary -----------------------------------------------------------

def test_usage_summary_keeps_quota_fields():
    data = {
        "five_hour": {"utilization": 42, "resets_at": "2026-06-05T20:00:00Z"},
        "seven_day": {"utilization": 10},
        "seven_day_opus": {"utilization": 5},
        "seven_day_sonnet": {"utilization": 8},
        "other_internal_key": "should be dropped",
        "another_raw_key": 123,
    }
    result = U.usage_summary(data)
    assert result == {
        "five_hour": {"utilization": 42.0, "resets_at": "2026-06-05T20:00:00Z"},
        "seven_day": {"utilization": 10.0},
        "seven_day_opus": {"utilization": 5.0},
        "seven_day_sonnet": {"utilization": 8.0},
    }


def test_usage_summary_includes_extra_usage_when_enabled():
    xu = {"is_enabled": True, "utilization": 30, "used_credits": 5, "monthly_limit": 100, "currency": "USD"}
    data = {
        "five_hour": {"utilization": 50},
        "seven_day": {"utilization": 20},
        "seven_day_opus": None,
        "seven_day_sonnet": None,
        "extra_usage": xu,
    }
    result = U.usage_summary(data)
    assert result["extra_usage"] == {
        "enabled": True,
        "utilization": 30,
        "used_credits": 5,
        "monthly_limit": 100,
        "currency": "USD",
    }
    assert "seven_day_opus" not in result
    assert "seven_day_sonnet" not in result


def test_usage_summary_uses_dynamic_model_quotas():
    data = {
        "five_hour": {"utilization": 42},
        "seven_day": {"utilization": 30},
        "seven_day_opus": None,
        "seven_day_sonnet": None,
        "limits": [
            {
                "kind": "session",
                "percent": 42,
                "scope": None,
            },
            {
                "kind": "weekly_scoped",
                "percent": 52,
                "resets_at": "2026-07-17T18:00:00Z",
                "scope": {"model": {"id": "fable", "display_name": "Fable"}},
                "is_active": False,
            },
        ],
    }
    result = U.usage_summary(data)
    assert result["model_quotas"] == [{
        "model": "Fable",
        "model_id": "fable",
        "utilization": 52.0,
        "resets_at": "2026-07-17T18:00:00Z",
        "is_active": False,
    }]
    assert "seven_day_opus" not in result
    assert "seven_day_sonnet" not in result


def test_usage_summary_omits_extra_usage_when_disabled():
    data = {
        "five_hour": {"utilization": 50},
        "seven_day": {"utilization": 20},
        "seven_day_opus": None,
        "seven_day_sonnet": None,
        "extra_usage": {"is_enabled": False, "utilization": 0},
    }
    result = U.usage_summary(data)
    assert "extra_usage" not in result


def test_usage_summary_omits_extra_usage_when_absent():
    data = {"five_hour": {"utilization": 5}, "seven_day": {"utilization": 1}}
    result = U.usage_summary(data)
    assert "extra_usage" not in result


def test_usage_summary_partial_quota_fields():
    data = {"five_hour": {"utilization": 99}}
    result = U.usage_summary(data)
    assert list(result.keys()) == ["five_hour"]


def test_usage_summary_empty_data():
    assert U.usage_summary({}) == {}


def test_usage_summary_no_extra_raw_keys():
    data = {
        "five_hour": {"utilization": 10},
        "seven_day": {"utilization": 20},
        "seven_day_opus": {"utilization": 30},
        "seven_day_sonnet": {"utilization": 40},
        "raw_internal": "dropped",
        "another_key": {"nested": True},
    }
    result = U.usage_summary(data)
    assert set(result.keys()) == {"five_hour", "seven_day", "seven_day_opus", "seven_day_sonnet"}


def test_usage_summary_omits_empty_blocks():
    data = {"five_hour": {}, "seven_day": None, "seven_day_sonnet": {"resets_at": "2026-06-09T00:00:00Z"}}
    result = U.usage_summary(data)
    assert result == {
        "seven_day_sonnet": {"resets_at": "2026-06-09T00:00:00Z"},
    }

# --- Anthropic 5-hour session reset -----------------------------------------


def _reset_payload(**overrides):
    block = {
        "eligible": True,
        "available": True,
        "ineligible_reason": None,
        "in_experiment": True,
        "arm": "reset",
        "next_available_at": None,
        "weekly_resets_at": "2026-09-07T08:00:00Z",
        "resets_per_week": 1,
    }
    block.update(overrides)
    return {
        "five_hour": {"utilization": 100},
        "seven_day": {"utilization": 40},
        "juniper_tide": block,
    }


def test_session_reset_status_claimable_is_provider_driven():
    status = U.session_reset_status(_reset_payload())
    assert status == {
        "eligible": True,
        "available": True,
        "claimable": True,
        "ineligible_reason": None,
        "next_available_at": None,
        "weekly_resets_at": "2026-09-07T08:00:00Z",
        "resets_per_week": 1,
    }


def test_session_reset_status_does_not_use_five_hour_percentage():
    data = _reset_payload(available=False, ineligible_reason="not_at_wall")
    data["five_hour"]["utilization"] = 100
    status = U.session_reset_status(data)
    assert status is not None
    assert status["claimable"] is False


def test_session_reset_status_unknown_protocol_value_fails_closed():
    assert U.session_reset_status(
        _reset_payload(ineligible_reason="future_reason")
    ) is None
    assert U.session_reset_status(_reset_payload(arm="future_arm")) is None


def test_session_reset_status_invalid_provider_time_fails_closed():
    assert U.session_reset_status(
        _reset_payload(next_available_at="tomorrow")
    ) is None


def test_usage_payload_accepts_weekly_without_active_five_hour():
    assert U._is_usage_payload({
        "seven_day": {"utilization": 27, "resets_at": "2026-09-08T08:00:00Z"},
    })
    summary = U.usage_summary({
        "seven_day": {"utilization": 27, "resets_at": "2026-09-08T08:00:00Z"},
    })
    assert summary == {
        "seven_day": {"utilization": 27.0, "resets_at": "2026-09-08T08:00:00Z"},
    }


def test_usage_payload_accepts_scoped_limits_without_five_hour():
    assert U._is_usage_payload({
        "limits": [{
            "kind": "weekly_scoped",
            "percent": 12,
            "scope": {"model": {"display_name": "Fable"}},
        }],
    })


def test_usage_payload_rejects_unrelated_or_empty_success_objects():
    assert U._is_usage_payload({}) is False
    assert U._is_usage_payload({"message": "ok"}) is False
    assert U._is_usage_payload([]) is False


def test_request_usage_live_accepts_weekly_only_payload(monkeypatch):
    payload = {
        "seven_day": {"utilization": 27, "resets_at": "2026-09-08T08:00:00Z"},
    }
    writes = []
    monkeypatch.setattr(U, "oauth_token", lambda: "token")
    monkeypatch.setattr(U, "oauth_headers", lambda token: {"Authorization": f"Bearer {token}"})
    monkeypatch.setattr(U, "_write_usage_cache", writes.append)

    class Response:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def read(self, size=None): return json.dumps(payload).encode()

    monkeypatch.setattr(U.urllib.request, "urlopen", lambda req, timeout: Response())
    assert U._request_usage_live() == payload
    assert writes == []  # direct OAuth reads never bypass the common cache lock


def test_request_usage_live_uses_at_wall_view_without_second_request(monkeypatch):
    seen = []
    monkeypatch.setattr(U, "oauth_token", lambda: "token")
    monkeypatch.setattr(
        U,
        "oauth_headers",
        lambda token: {"Authorization": f"Bearer {token}", "x-app": "cli"},
    )
    monkeypatch.setattr(U, "_write_usage_cache", lambda data: None)

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, size=None):
            return json.dumps(_reset_payload()).encode()

    def urlopen(req, timeout):
        seen.append(req)
        return Response()

    monkeypatch.setattr(U.urllib.request, "urlopen", urlopen)
    result = U._request_usage_live()
    assert result and result["five_hour"]["utilization"] == 100
    assert len(seen) == 1
    parsed = urllib.parse.urlparse(seen[0].full_url)
    assert parsed.path == "/api/oauth/usage"
    assert urllib.parse.parse_qs(parsed.query) == {"at_wall": ["1"]}
    assert dict(seen[0].header_items())["X-app"] == "cli"


def test_reset_preflight_url_skips_spend_but_keeps_at_wall():
    parsed = urllib.parse.urlparse(
        U._usage_request_url(at_wall=True, skip_spend=True)
    )
    assert urllib.parse.parse_qs(parsed.query) == {
        "at_wall": ["1"],
        "skip_spend": ["1"],
    }


def test_skip_spend_status_read_does_not_overwrite_full_usage_cache(monkeypatch):
    writes = []
    monkeypatch.setattr(U, "oauth_token", lambda: "token")
    monkeypatch.setattr(U, "oauth_headers", lambda token: {"Authorization": f"Bearer {token}"})
    monkeypatch.setattr(U, "_write_usage_cache", writes.append)

    class Response:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def read(self, size=None): return json.dumps(_reset_payload()).encode()

    monkeypatch.setattr(U.urllib.request, "urlopen", lambda req, timeout: Response())
    assert U._request_usage_live(skip_spend=True) is not None
    assert writes == []


@pytest.fixture
def reset_opt_in(monkeypatch):
    monkeypatch.setenv("CCX_ENABLE_UNOFFICIAL_RESET", "1")
    monkeypatch.setattr(U, "_usage_source", lambda: "oauth")


@pytest.mark.parametrize("value", [None, "", "0", "false", "maybe"])
def test_reset_refuses_without_explicit_opt_in(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("CCX_ENABLE_UNOFFICIAL_RESET", raising=False)
    else:
        monkeypatch.setenv("CCX_ENABLE_UNOFFICIAL_RESET", value)
    boom = lambda *a, **k: (_ for _ in ()).throw(AssertionError("no network I/O"))
    monkeypatch.setattr(U, "_request_usage_live", boom)
    monkeypatch.setattr(U.urllib.request, "urlopen", boom)
    with pytest.raises(InvalidRequest, match="CCX_ENABLE_UNOFFICIAL_RESET=1"):
        U.reset_five_hour_limit()


def test_client_reset_refuses_without_opt_in(active_api_root, monkeypatch):
    from ccx.client import CcxClient

    monkeypatch.delenv("CCX_ENABLE_UNOFFICIAL_RESET", raising=False)
    def no_account_io(*_args, **_kwargs):
        pytest.fail("reset without explicit opt-in must not access provider account")

    monkeypatch.setattr(U, "_request_usage_live", no_account_io)
    monkeypatch.setattr(U.urllib.request, "urlopen", no_account_io)
    with pytest.raises(CcxError, match="CCX_ENABLE_UNOFFICIAL_RESET"):
        CcxClient().reset_five_hour_limit()


@pytest.mark.parametrize("value", ["1", "true", "YES", " on "])
def test_reset_runs_with_opt_in(monkeypatch, value):
    monkeypatch.setenv("CCX_ENABLE_UNOFFICIAL_RESET", value)
    monkeypatch.setattr(U, "_usage_source", lambda: "oauth")
    monkeypatch.setattr(
        U, "_request_usage_live",
        lambda *a, **k: _reset_payload(available=False, ineligible_reason="not_at_wall"),
    )
    assert U.reset_five_hour_limit()["result"] == "not_available"


def test_reset_rechecks_provider_and_skips_post_when_not_claimable(monkeypatch, reset_opt_in):
    preflight = _reset_payload(available=False, ineligible_reason="not_at_wall")
    calls = []
    monkeypatch.setattr(
        U, "_request_usage_live", lambda *a, **k: calls.append("get") or preflight
    )
    monkeypatch.setattr(
        U.urllib.request,
        "urlopen",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("POST must not be sent")),
    )
    result = U.reset_five_hour_limit()
    assert calls == ["get"]
    assert result["result"] == "not_available"
    assert result["status"]["claimable"] is False
    assert result["refreshed"] is False


def test_reset_auth_failure_does_not_claim_reconciliation(monkeypatch, reset_opt_in):
    monkeypatch.setattr(U, "_request_usage_live", lambda *a, **k: _reset_payload())
    monkeypatch.setattr(U, "oauth_token", lambda: None)
    monkeypatch.setattr(U, "oauth_organization_uuid", lambda: "org")
    monkeypatch.setattr(
        U.urllib.request,
        "urlopen",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("POST must not be sent")),
    )
    result = U.reset_five_hour_limit()
    assert result["result"] == "auth_error"
    assert result["status"]["claimable"] is True
    assert result["refreshed"] is False


def test_reset_posts_once_then_reconciles(monkeypatch, reset_opt_in):
    before = _reset_payload()
    after = _reset_payload(
        eligible=False,
        available=False,
        arm=None,
        ineligible_reason="not_at_wall",
        next_available_at="2026-09-10T08:00:00Z",
    )
    live = iter([before, after])
    reads = []
    def read_live(*args, **kwargs):
        reads.append(kwargs)
        return next(live)
    monkeypatch.setattr(U, "_request_usage_live", read_live)
    monkeypatch.setattr(U, "oauth_token", lambda: "token")
    monkeypatch.setattr(U, "oauth_organization_uuid", lambda: "org/example")
    monkeypatch.setattr(
        U,
        "oauth_headers",
        lambda token: {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    posts = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return json.dumps({
                "result": "reset",
                "next_available_at": "2026-09-10T08:00:00Z",
                "weekly_resets_at": "2026-09-07T08:00:00Z",
            }).encode()

    def urlopen(req, timeout):
        posts.append(req)
        return Response()

    monkeypatch.setattr(U.urllib.request, "urlopen", urlopen)
    result = U.reset_five_hour_limit()
    assert result["result"] == "reset"
    assert result["refreshed"] is True
    assert result["status"]["claimable"] is False
    assert [read["skip_spend"] for read in reads] == [True, False]
    assert len(posts) == 1
    req = posts[0]
    assert req.get_method() == "POST"
    assert req.full_url.endswith(
        "/api/organizations/org%2Fexample/reset_rate_limits"
    )
    assert json.loads(req.data.decode()) == {"program": "juniper_tide"}


def test_reset_never_retries_ambiguous_mutation(monkeypatch, reset_opt_in):
    live = iter([
        _reset_payload(),
        _reset_payload(available=False, ineligible_reason="not_at_wall"),
    ])
    monkeypatch.setattr(U, "_request_usage_live", lambda *a, **k: next(live))
    monkeypatch.setattr(U, "oauth_token", lambda: "token")
    monkeypatch.setattr(U, "oauth_organization_uuid", lambda: "org")
    monkeypatch.setattr(
        U, "oauth_headers", lambda token: {"Authorization": f"Bearer {token}"}
    )
    posts = 0

    def timeout_once(req, timeout):
        nonlocal posts
        posts += 1
        raise TimeoutError("ambiguous network timeout")

    monkeypatch.setattr(U.urllib.request, "urlopen", timeout_once)
    result = U.reset_five_hour_limit()
    assert posts == 1
    assert result["result"] == "error"
    assert result["refreshed"] is True


def test_reset_malformed_provider_result_fails_closed(monkeypatch, reset_opt_in):
    live = iter([_reset_payload(), _reset_payload()])
    monkeypatch.setattr(U, "_request_usage_live", lambda *a, **k: next(live))
    monkeypatch.setattr(U, "oauth_token", lambda: "token")
    monkeypatch.setattr(U, "oauth_organization_uuid", lambda: "org")
    monkeypatch.setattr(
        U, "oauth_headers", lambda token: {"Authorization": f"Bearer {token}"}
    )

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"result":"future_result"}'

    monkeypatch.setattr(U.urllib.request, "urlopen", lambda req, timeout: Response())
    assert U.reset_five_hour_limit()["result"] == "error"


# -- CLI usage presentation reflects authoritative metadata -----------------

def _usage_args(**overrides):
    values = {"engine": "claude", "cached": False, "json": False,
              "summary": False, "set_source": None}
    values.update(overrides)
    return argparse.Namespace(**values)


def _display_origin(**overrides):
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    result = {"source": "live", "age_s": 0, "provider": "get_usage",
              "stale": False, "held": False, "unparsed": False,
              "fetched_at": now, "next_attempt_at": now,
              "held_until": None, "live_error": None, "cache": "/mock/cache"}
    result.update(overrides)
    return result


def test_usage_set_source_refusal_is_a_clean_cli_error(monkeypatch, capsys):
    from ccx.cli.commands import _common as C

    def refuse(source):
        raise U.InvalidRequest("direct OAuth usage is Linux-only; use get_usage")

    monkeypatch.setattr(commands_read, "set_usage_source", refuse)
    with pytest.raises(SystemExit) as exc:
        commands_read.cmd_usage(_usage_args(set_source="oauth"))
    assert exc.value.code == U.InvalidRequest("x").exit_code
    assert "[ccx] usage: direct OAuth usage is Linux-only" in capsys.readouterr().err


def test_usage_command_labels_a_live_read(monkeypatch, capsys):
    from ccx.cli.commands import _common as C
    payload = {"five_hour": {"utilization": 12.0, "resets_at": "2026-10-16T20:00:00Z"}}
    monkeypatch.setattr(commands_read, "fetch_usage_for_display", lambda **kw: (payload, _display_origin()))
    commands_read.cmd_usage(_usage_args())
    out = capsys.readouterr().out
    assert out.startswith("usage (live):\n")
    assert "12.0%" in out


def test_usage_command_held_never_claims_live(monkeypatch, capsys):
    from ccx.cli.commands import _common as C
    cached = {"five_hour": {"utilization": 40.0}}
    monkeypatch.setattr(commands_read, "fetch_usage_for_display", lambda **kw: (cached, _display_origin(
        source="cache", stale=True, held=True, held_until="2026-10-26T09:55:00Z",
        fetched_at="2026-10-26T09:50:00Z", live_error="rate limited")))
    commands_read.cmd_usage(_usage_args())
    out = capsys.readouterr().out
    assert "last known" in out and "(held, retry ~" in out
    assert "live" not in out and "40.0%" in out


def test_usage_command_json_warns_on_stale_without_polluting_stdout(monkeypatch, capsys):
    from ccx.cli.commands import _common as C
    cached = {"five_hour": {"utilization": 40.0}}
    monkeypatch.setattr(commands_read, "fetch_usage_for_display", lambda **kw: (cached, _display_origin(
        source="cache", stale=True, live_error="offline")))
    commands_read.cmd_usage(_usage_args(json=True))
    captured = capsys.readouterr()
    assert json.loads(captured.out) == cached
    assert "offline" in captured.err and "last known" in captured.err


def test_usage_command_cached_flag_reports_a_fresh_cache(monkeypatch, capsys):
    from ccx.cli.commands import _common as C
    monkeypatch.setattr(commands_read, "fetch_usage_for_display", lambda **kw: (
        {"five_hour": {"utilization": 1.0}}, _display_origin(source="cache", age_s=4)))
    commands_read.cmd_usage(_usage_args(cached=True))
    assert capsys.readouterr().out.startswith("usage (cached, 4s old):\n")


def test_usage_command_no_signal_names_cause(monkeypatch, capsys):
    from ccx.cli.commands import _common as C
    monkeypatch.setattr(commands_read, "fetch_usage_for_display", lambda **kw: (
        None, _display_origin(source=None, live_error="probe failed")))
    with pytest.raises(SystemExit):
        commands_read.cmd_usage(_usage_args())
    assert "no signal: probe failed" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        ("http", "usage endpoint returned HTTP 429"),
        ("url", "usage endpoint unreachable: down"),
        ("json", "usage endpoint returned unreadable JSON"),
    ],
)
def test_live_usage_detail_names_the_failure(monkeypatch, raised, expected):
    import io
    import urllib.error

    monkeypatch.setattr(U, "oauth_token", lambda: "token")

    class Response(io.BytesIO):
        status = 200

    def urlopen(*a, **k):
        if raised == "http":
            raise urllib.error.HTTPError("u", 429, "Too Many", {}, None)
        if raised == "url":
            raise urllib.error.URLError("down")
        return Response(b"not json")

    monkeypatch.setattr(U.urllib.request, "urlopen", urlopen)
    assert U._request_usage_live_detail() == (None, expected)
    assert U._request_usage_live() is None
