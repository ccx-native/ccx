"""Full-UUID account-only usage adapters: synthetic provider data, never network."""
from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx import CcxClient, CodexClient, WorkerClient
from ccx.cli import main as cli
from ccx.cli.commands import _common as commands
from ccx import constants
from ccx.codex import runtime as codex_runtime
from ccx import client as claude_client
from ccx.codex import client as codex_client
from ccx import workers
from ccx.claude import usage as usage_module
from ccx.codex.types import CodexUsage
from ccx.errors import CapabilityUnsupported, EngineCliNotFound, InvalidRequest, SessionUnavailable
from ccx.rpc.local_rpc import dispatch_full_uuid
from ccx.types import FiveHourReset, Usage
from ccx.claude.usage import session_reset_status
from ccx.store.root import require_ccx_active
from test_generation_store import fixture_root
from test_v1_worker_protocol import request as v1_request


CLAUDE = {
    "five_hour": {"utilization": 42.0, "resets_at": "2026-10-22T12:00:00Z"},
    "seven_day": {"utilization": 7.5, "resets_at": "2026-10-29T12:00:00Z"},
    "limits": [{"kind": "weekly_scoped", "percent": 12.0, "scope":
                {"model": {"display_name": "Opus", "id": "opus"}}}],
}
CODEX = {
    "rateLimitsByLimitId": {
        "codex": {
            "primary": {"usedPercent": 31, "windowDurationMins": 300,
                        "resetsAt": 1800000000},
            "secondary": {"usedPercent": 8, "windowDurationMins": 10080,
                          "resetsAt": 1800100000},
        }
    }
}
TOKENS = {"summary": {"lifetimeTokens": 12345, "peakDailyTokens": 99}}


@pytest.fixture
def account_root(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)  # Sealed ACTIVE but NO worker records.
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setenv("CCX_ROOT", str(root))
    return root


def test_typed_clients_and_worker_windows_token_scope_partial_failure_budget(
    account_root, monkeypatch,
):
    calls = []
    monkeypatch.setattr(
        claude_client, "fetch_usage",
        lambda **kw: calls.append(("claude-typed", dict(kw))) or CLAUDE,
    )
    monkeypatch.setattr(
        claude_client, "reset_five_hour_limit",
        lambda: pytest.fail("read-only usage cannot perform a reset"),
    )
    direct = CcxClient()
    cached = direct.usage(cached=True)
    live = direct.usage()
    assert isinstance(cached, Usage) and isinstance(live, Usage)
    assert cached.raw == CLAUDE and cached.summary["five_hour"]["utilization"] == 42.0
    assert cached.five_hour_reset == session_reset_status(CLAUDE)
    assert cached.utilization("five_hour") == 42.0
    assert calls == [
        ("claude-typed", {"force": False}),
        ("claude-typed", {"force": True}),
    ]
    monkeypatch.setattr(claude_client, "fetch_usage", lambda **kw: None)
    with pytest.raises(InvalidRequest, match="no signal"):
        direct.usage()
    # The read-only usage path does not use the separate reset mutation.

    codex_calls = []
    monkeypatch.setattr(
        codex_client, "read_account_usage",
        lambda **kw: codex_calls.append(dict(kw)) or
        (TOKENS if kw["tokens"] else CODEX),
    )
    typed = CodexClient().usage(timeout=7)
    assert isinstance(typed, CodexUsage)
    assert typed.kind == "rate_limits" and typed.raw == CODEX
    assert typed.utilization() == 31
    assert CodexClient().usage(tokens=True, timeout=9).as_dict() == {
        "engine": "codex", "kind": "token_activity", "raw": TOKENS,
    }
    assert codex_calls == [
        {"tokens": False, "timeout": 7},
        {"tokens": True, "timeout": 9},
    ]
    with pytest.raises(InvalidRequest):
        CodexClient().usage(timeout=0)

    raw_claude = []
    monkeypatch.setattr(
        workers, "fetch_usage",
        lambda **kw: raw_claude.append(dict(kw)) or CLAUDE,
    )
    public = WorkerClient()
    both = public.usage(timeout=10)
    assert both["errors"] == {}
    assert both["skipped"] == {}
    assert set(both["usage"]) == {"claude", "codex"}
    c = both["usage"]["claude"]
    x = both["usage"]["codex"]
    assert c["raw"] == CLAUDE and x["raw"] == CODEX
    assert [w["id"] for w in c["windows"]] == [
        "five_hour", "seven_day", "model:opus",
    ]
    assert [w["id"] for w in x["windows"]] == [
        "codex:5h", "codex:7d",
    ]
    assert c["windows"][0]["used_percent"] == 42.0
    assert x["windows"][0]["used_percent"] == 31.0
    assert x["windows"][0]["resets_at"].endswith("Z")
    assert raw_claude[0]["max_age"] == 0

    with pytest.raises(CapabilityUnsupported) as denied:
        public.usage("claude", include_tokens=True)
    assert (denied.value.engine, denied.value.capability) == (
        "claude", "account_token_activity",
    )
    assert public.usage("codex", include_tokens=True)["windows"] == []
    assert public.usage("codex", include_tokens=True)["raw"] == TOKENS
    mixed_tokens = public.usage(include_tokens=True)
    assert mixed_tokens["usage"]["claude"]["kind"] == "rate_limits"
    assert mixed_tokens["usage"]["codex"]["kind"] == "token_activity"

    def offline(**_kw):
        raise SessionUnavailable("Claude account unavailable")
    monkeypatch.setattr(workers, "fetch_usage", offline)
    partial = public.usage(timeout=10)
    assert set(partial["usage"]) == {"codex"}
    assert partial["errors"]["claude"]["error"] == "SessionUnavailable"
    assert partial["errors"]["claude"]["exit_code"] == 3
    with pytest.raises(SessionUnavailable):
        public.usage("claude", timeout=10)
    with pytest.raises(InvalidRequest):
        public.usage("gemini")
    for bad in (None, 0, -1, True, "30"):
        with pytest.raises(InvalidRequest):
            public.usage(timeout=bad)

    # One original end-to-end clock: Codex only receives Claude's leftover.
    clock = [100.0]
    budget_grants = []
    monkeypatch.setattr(workers, "_deadline", lambda budget: clock[0] + budget)
    monkeypatch.setattr(
        workers, "_left", lambda deadline: max(0., deadline - clock[0]),
    )
    def claude_eats_budget(**kw):
        budget_grants.append(("claude", kw["timeout"]))
        clock[0] += 8.
        return CLAUDE
    def codex_uses_leftover(self, *, tokens=False, timeout=30):
        budget_grants.append(("codex", timeout))
        return CodexUsage(raw=CODEX)
    monkeypatch.setattr(workers, "fetch_usage", claude_eats_budget)
    monkeypatch.setattr(CodexClient, "usage", codex_uses_leftover)
    budget = public.usage(timeout=10)
    assert budget["errors"] == {}
    assert budget_grants == [("claude", 10.), ("codex", 2.)]
    budget_grants.clear()
    clock[0] = 200.
    def claude_exhausts(**kw):
        budget_grants.append(("claude", kw["timeout"]))
        clock[0] += kw["timeout"]
        return CLAUDE
    monkeypatch.setattr(workers, "fetch_usage", claude_exhausts)
    exhausted = public.usage(timeout=10)
    assert set(exhausted["usage"]) == {"claude"}
    assert exhausted["errors"]["codex"]["error"] == "SessionUnavailable"
    assert "budget exhausted" in exhausted["errors"]["codex"]["message"]
    assert budget_grants == [("claude", 10.)]
    assert require_ccx_active(account_root).list_workers(limit=None) == []


def test_codex_weekly_only_primary_duration_lookup_and_rows():
    from ccx.cli.codex import _quota_rows
    from ccx.workers import _codex_windows
    weekly = {"rateLimitsByLimitId": {"codex": {
        "primary": {"usedPercent": 62, "windowDurationMins": 10080,
                    "resetsAt": 1800100000},
    }}}
    direct = CodexUsage(raw=weekly)
    assert direct.utilization(window="5h") is None
    assert direct.utilization(window="7d") == 62
    assert _codex_windows(weekly)[0]["id"] == "codex:7d"
    assert _quota_rows(weekly)[0][:2] == ("7d", 62)
    other = {"rateLimits": {"limitId": "other",
                            "primary": {"windowDurationMins": 10080,
                                        "usedPercent": 99}}}
    assert CodexUsage(raw=other).utilization(window="7d") is None
    named_top_level = {"rateLimits": {
        "limitName": "Special", "primary": {
            "windowDurationMins": 10080, "usedPercent": 45,
        },
    }}
    assert _quota_rows(named_top_level)[0][:2] == ("7d Special", 45)
    with pytest.raises(ValueError, match="5h.*7d"):
        direct.utilization(window="primary")


def test_combined_cli_usage_json_flags_and_missing_engine(account_root, monkeypatch, capsys):
    from ccx.system import engine_binary
    from ccx.cli import codex as codex_commands

    monkeypatch.setattr(engine_binary, "resolve_engine_binary", lambda engine: f"/fake/{engine}")
    monkeypatch.setattr(engine_binary, "account_signed_out",
                        lambda *args: pytest.fail("happy path must not probe auth"))
    reads = []
    monkeypatch.setattr("ccx.cli.commands.read.fetch_usage_for_display", lambda **kw: (
        reads.append(("claude", kw)) or CLAUDE,
        {"source": "live", "cache": "test", "live_error": None, "stale": False,
         "unparsed": False, "held": False},
    ))
    monkeypatch.setattr(codex_runtime, "read_account_usage", lambda **kw:
                        reads.append(("codex", kw)) or (TOKENS if kw["tokens"] else CODEX))
    cli._main(["usage", "--json"])
    assert json.loads(capsys.readouterr().out) == {"claude": CLAUDE, "codex": CODEX}
    assert [r[0] for r in reads] == ["claude", "codex"]
    cli._main(["usage", "--cached", "--tokens"])
    text = capsys.readouterr().out
    assert text.index("Claude:") < text.index("Codex:")
    assert "42.0%" in text and "12,345" in text
    assert reads[-2:] == [("claude", {"force": False}), ("codex", {"tokens": True})]

    def no_claude(engine):
        if engine == "claude":
            raise EngineCliNotFound("Claude missing")
        return "/fake/codex"
    monkeypatch.setattr(engine_binary, "resolve_engine_binary", no_claude)
    cli._main(["usage", "--json"])
    data = json.loads(capsys.readouterr().out)
    assert data["claude"]["skipped"] == "not_installed"
    assert data["codex"] == CODEX


def test_combined_python_skip_signed_out_after_read_failure(account_root, monkeypatch):
    from ccx.system import engine_binary
    from ccx import workers as w
    monkeypatch.setattr(engine_binary, "resolve_engine_binary", lambda engine: f"/fake/{engine}")
    checks = []
    monkeypatch.setattr(engine_binary, "account_signed_out", lambda engine, binary:
                        checks.append(engine) or engine == "claude")
    monkeypatch.setattr(w, "fetch_usage",
                        lambda **kw: (_ for _ in ()).throw(SessionUnavailable("auth failed")))
    monkeypatch.setattr(CodexClient, "usage", lambda self, **kw: CodexUsage(raw=CODEX))
    result = WorkerClient().usage()
    assert result["skipped"]["claude"]["reason"] == "signed_out"
    assert set(result["usage"]) == {"codex"}
    assert result["errors"] == {}
    assert checks == ["claude"]
    with pytest.raises(SessionUnavailable, match="auth failed"):
        WorkerClient().usage("claude")
    monkeypatch.setattr(engine_binary, "account_signed_out", lambda *args: False)
    errors = WorkerClient().usage()
    assert errors["skipped"] == {}
    assert errors["errors"]["claude"]["error"] == "SessionUnavailable"


def test_combined_cli_signed_out_and_real_failure_keep_healthy_engine(
    account_root, monkeypatch, capsys,
):
    from ccx.system import engine_binary
    monkeypatch.setattr(engine_binary, "resolve_engine_binary",
                        lambda engine: f"/fake/{engine}")
    monkeypatch.setattr("ccx.cli.commands.read.fetch_usage_for_display", lambda **kw: (
        CLAUDE, {"source": "live", "cache": "test", "live_error": None,
                 "stale": False, "unparsed": False, "held": False},
    ))
    reads, probes = [], []
    def broken_codex(**kw):
        reads.append(kw)
        raise SessionUnavailable("temporary Codex account error")
    monkeypatch.setattr(codex_runtime, "read_account_usage", broken_codex)
    monkeypatch.setattr(engine_binary, "account_signed_out",
                        lambda eng, path: probes.append(eng) or True)
    cli._main(["usage", "--json"])
    first = json.loads(capsys.readouterr().out)
    assert first["claude"] == CLAUDE
    assert first["codex"]["skipped"] == "signed_out"
    assert reads == [{"tokens": False}] and probes == ["codex"]

    monkeypatch.setattr(engine_binary, "account_signed_out",
                        lambda eng, path: probes.append(eng) or False)
    cli._main(["usage", "--json"])
    second = json.loads(capsys.readouterr().out)
    assert second["claude"] == CLAUDE
    assert second["codex"]["error"]["type"] == "SystemExit"
    assert "temporary Codex account error" in second["codex"]["error"]["message"]
    assert probes == ["codex", "codex"]

    def missing(engine):
        raise EngineCliNotFound(f"{engine} missing")
    monkeypatch.setattr(engine_binary, "resolve_engine_binary", missing)
    with pytest.raises(SystemExit) as none:
        cli._main(["usage", "--json"])
    both = json.loads(capsys.readouterr().out)
    assert none.value.code != 0
    assert both["claude"]["skipped"] == "not_installed"
    assert both["codex"]["skipped"] == "not_installed"


def test_both_real_cli_parsers_keep_account_formats_and_cache_origin(
    account_root, monkeypatch, capsys,
):
    from ccx.cli.commands import read as cli_read
    read = []
    origin = {"source": "cache", "age_s": 37 * 60, "live_error": "offline",
              "stale": True, "held": False, "fetched_at": "2026-10-26T09:00:00Z",
              "next_attempt_at": "2026-10-26T09:02:00Z",
              "cache": str(account_root / "usage-cache.json")}
    monkeypatch.setattr(
        cli_read, "fetch_usage_for_display",
        lambda **kw: (read.append(kw["force"]) or CLAUDE, origin),
    )
    native = []
    monkeypatch.setattr(
        codex_runtime, "read_account_usage",
        lambda **kw: native.append(dict(kw)) or
        (TOKENS if kw["tokens"] else CODEX),
    )
    # Account reads never use the CLI's global textual worker-discovery map.
    from ccx.store import discovery as worker_discovery
    monkeypatch.setattr(
        worker_discovery, "resolve_cli_fields",
        lambda *_args, **_kw: pytest.fail("usage must not discover a worker"),
    )
    monkeypatch.setattr(
        codex_runtime, "ensure_daemon",
        lambda *_args, **_kw: pytest.fail("usage cannot start codexd"),
    )
    cli._main(["usage", "--engine", "claude", "--cached", "--json"])
    output = capsys.readouterr()
    assert json.loads(output.out) == CLAUDE
    assert "last known" in output.err and "offline" in output.err
    assert read == [False]

    cli._main(["usage", "--engine", "claude", "--summary"])
    output = capsys.readouterr()
    summary = json.loads(output.out)
    assert summary["five_hour"]["utilization"] == 42.0
    assert read == [False, True]

    cli._main(["usage", "--engine", "claude"])
    output = capsys.readouterr()
    assert output.out.startswith("usage (last known") and "(stale, retry ~" in output.out
    assert "42.0%" in output.out

    cli._main(["usage", "--engine", "codex", "--json"])
    output = capsys.readouterr()
    assert json.loads(output.out) == CODEX
    cli._main(["usage", "--engine", "codex", "--tokens", "--json"])
    assert json.loads(capsys.readouterr().out) == TOKENS
    cli._main(["usage", "--engine", "codex", "--tokens"])
    assert "12,345" in capsys.readouterr().out
    assert native == [
        {"tokens": False}, {"tokens": True}, {"tokens": True},
    ]
    assert require_ccx_active(account_root).list_workers(limit=None) == []
    assert not (account_root / "sessions").exists()


def test_v1_account_usage_structural_both_engines_and_pre_provider_validation(
    account_root, monkeypatch,
):
    ctx = require_ccx_active(account_root)
    seen = []
    monkeypatch.setattr(
        workers, "fetch_usage",
        lambda **kw: seen.append(("claude", dict(kw))) or CLAUDE,
    )
    monkeypatch.setattr(
        codex_runtime, "read_account_usage",
        lambda **kw: seen.append(("codex", dict(kw))) or CODEX,
    )

    caps = dispatch_full_uuid(v1_request("system.info"), ctx)
    assert caps["ok"] is True
    assert set(caps["result"]) == {"protocol_version", "ccx_version"}
    assert caps["result"]["protocol_version"] == 2

    for params in ({}, {"engine": ""}, {"engine": []},
                   {"engine": "claude", "target": "not-a-worker"}):
        reply = dispatch_full_uuid(
            v1_request("account.usage.read", **params), ctx,
        )
        assert reply["ok"] is False
        assert reply["error"]["code"] == "invalid_request", reply
    bad = dispatch_full_uuid(
        v1_request("account.usage.read", engine="unknown"), ctx,
    )
    assert bad["ok"] is False
    assert bad["error"]["code"] == "invalid_request"
    assert seen == []

    claude = v1_request("account.usage.read", engine="claude")
    claude["timeout_ms"] = 5000
    got = dispatch_full_uuid(claude, ctx)
    assert got["ok"] is True, got
    row = got["result"]
    assert row["provider"] == "claude" and type(row["observed_at_ms"]) is int
    assert [w["window_id"] for w in row["windows"]] == [
        "five_hour", "seven_day", "model:opus",
    ]
    assert row["windows"][0]["remaining_percent"] == 58.0

    codex = v1_request("account.usage.read", engine="codex")
    codex["timeout_ms"] = 4000
    got = dispatch_full_uuid(codex, ctx)
    assert got["ok"] is True, got
    row = got["result"]
    assert row["provider"] == "codex" and type(row["observed_at_ms"]) is int
    assert [w["window_id"] for w in row["windows"]] == [
        "codex:primary", "codex:secondary",
    ]
    assert row["windows"][0]["window_duration_seconds"] == 18_000
    assert row["windows"][0]["remaining_percent"] == 69.0
    assert row["windows"][0]["resets_at"].endswith("Z")
    assert seen[0][0] == "claude"
    assert 0 < seen[0][1]["timeout"] <= 5.
    assert seen[1][0] == "codex" and seen[1][1]["tokens"] is False
    assert 0 < seen[1][1]["timeout"] <= 4.
    assert ctx.list_workers(limit=None) == []
    assert not (account_root / "sessions").exists()


def test_public_reset_calls_helper_once_and_keeps_explicit_opt_in(
    account_root, monkeypatch,
):
    calls = []
    outcome = {
        "result": "reset", "status": {"claimable": False},
        "next_available_at": "2026-10-29T12:00:00Z",
        "weekly_resets_at": "2026-10-29T12:00:00Z", "refreshed": True,
    }
    monkeypatch.setattr(claude_client, "reset_five_hour_limit",
                        lambda: calls.append("reset") or outcome)
    result = CcxClient().reset_five_hour_limit()
    assert calls == ["reset"]
    assert result == FiveHourReset(
        result="reset", status={"claimable": False},
        next_available_at="2026-10-29T12:00:00Z",
        weekly_resets_at="2026-10-29T12:00:00Z", refreshed=True,
    )
    assert result.succeeded

    # The real helper keeps its explicit opt-in, refused before any I/O.
    monkeypatch.setattr(claude_client, "reset_five_hour_limit",
                        usage_module.reset_five_hour_limit)
    monkeypatch.delenv("CCX_ENABLE_UNOFFICIAL_RESET", raising=False)
    no_io = lambda *a, **k: pytest.fail("reset attempted I/O without opt-in")
    monkeypatch.setattr(usage_module, "_request_usage_live", no_io)
    monkeypatch.setattr(usage_module.urllib.request, "urlopen", no_io)
    with pytest.raises(InvalidRequest, match="CCX_ENABLE_UNOFFICIAL_RESET=1"):
        CcxClient().reset_five_hour_limit()
