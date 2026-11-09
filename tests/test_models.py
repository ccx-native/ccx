"""Engine-neutral model catalogs; every provider read is mocked or a fake child."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import stat

import pytest

import ccx
from ccx import constants, models as M
from ccx.claude import usage_control
from ccx.cli.commands import models as cli_models
from ccx.cli.parser import build_parser
from ccx.codex import runtime
from ccx.errors import InvalidRequest
from ccx.types import ModelCatalog, ModelInfo


CLAUDE_FIXTURE = (
    Path(__file__).parent / "fixtures" / "claude-initialize-models.json"
)


def _claude_body() -> dict:
    return json.loads(CLAUDE_FIXTURE.read_text())


def _codex_entries() -> list[dict]:
    # Hand-made model/list schema-shaped entries; no native Codex fixture/data.
    return [
        {
            "id": "internal-6", "model": "gpt-6", "displayName": "GPT-6",
            "description": "General model", "hidden": False, "isDefault": True,
            "defaultReasoningEffort": "medium",
            "supportedReasoningEfforts": [
                {"reasoningEffort": "low", "description": "Faster"},
                {"reasoningEffort": "medium", "description": "Balanced"},
                {"reasoningEffort": "high", "description": "Thorough"},
            ],
            "irrelevantField": {"must": "not leak"},
        },
        {
            "id": "gpt-5.6", "displayName": "GPT-5.6",
            "description": "Earlier model", "isDefault": False,
            "defaultReasoningEffort": "high",
            "supportedReasoningEfforts": [
                {"reasoningEffort": "medium", "description": "Balanced"},
                {"reasoningEffort": "xhigh", "description": "Deeper"},
            ],
        },
    ]


@pytest.fixture
def isolated_models(tmp_path, monkeypatch):
    root = tmp_path / "ccx"
    root.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    identity = {"version": "2.1.321", "org": "org-1"}
    monkeypatch.setattr(M, "_local_claude_version", lambda: identity["version"])
    monkeypatch.setattr(M, "oauth_organization_uuid", lambda: identity["org"])
    calls = {"claude": 0, "codex": 0, "cwd": None}

    def claude(timeout=8):
        calls["claude"] += 1
        return {"kind": "success", "body": _claude_body()}

    def codex(*, cwd, timeout=30):
        calls["codex"] += 1
        calls["cwd"] = cwd
        return _codex_entries()

    monkeypatch.setattr(M, "request_models", claude)
    monkeypatch.setattr(M, "list_model_entries", codex)
    return root, identity, calls


def _cached(root: Path, engine: str = "claude") -> dict:
    return json.loads((root / f"models-{engine}.json").read_text())


def _run_cli(*args):
    parsed = build_parser().parse_args(["models", *args])
    return parsed.func(parsed)


def test_claude_normalization_from_sanitized_fixture():
    body = _claude_body()
    assert set(body) == {"models", "claude_code_version"}
    models = M.normalize_claude_models(body["models"])
    assert len(models) == 15
    by_id = {model.id: model for model in models}
    default = by_id["default"]
    assert default.default is True
    assert default.resolved == "claude-fable-5-2"
    assert default.display_name == "Default (recommended)"
    assert default.description == "Fable 5.2"
    assert default.alias is True
    assert default.features == {"fast": True, "auto": True, "thinkingOff": False}
    assert by_id["opus"].display_name == "Opus 5.5"
    assert by_id["opus"].alias is True
    assert by_id["claude-fable-5-1"].alias is False
    assert by_id["claude-opus-4-6"].efforts == ["low", "medium", "high", "max"]
    assert "xhigh" not in by_id["claude-sonnet-4-6"].efforts
    assert by_id["claude-haiku-4-5-20251001"].efforts == []
    assert by_id["claude-haiku-4-5-20251001"].features == {
        "fast": False, "auto": False, "thinkingOff": True,
    }
    assert by_id["sonnet"].features["fast"] is False  # absent, not true
    assert default.default_effort is None
    assert len(M.normalize_claude_models(
        [None, {}, {"value": 12}, *body["models"]]
    )) == len(models)
    assert set(default.as_dict()) == {
        "id", "resolved", "displayName", "description", "alias", "default",
        "efforts", "defaultEffort", "features",
    }


def test_codex_normalization_schema_fields_and_malformed_entries():
    models = M.normalize_codex_models([
        None, {"model": 4}, {"id": ""}, {"id": "hidden", "hidden": True},
        *_codex_entries(), _codex_entries()[0],
    ])
    assert len(models) == 2
    assert models[0].id == models[0].resolved == "gpt-6"  # model, not upstream id
    assert models[0].default is True and models[1].default is False
    assert models[0].efforts == ["low", "medium", "high"]
    assert models[0].default_effort == "medium"
    assert models[0].features == {}
    assert models[0].alias is False
    assert models[1].id == "gpt-5.6"  # fallback to upstream id
    assert models[1].efforts == ["medium", "xhigh"]
    assert "irrelevantField" not in json.dumps(models[0].as_dict())


def test_cache_hit_is_private_atomic_and_contains_only_public_fields(isolated_models):
    root, _, calls = isolated_models
    first = M.read_models(engine="claude")
    assert first[0].source == "live"
    path = root / "models-claude.json"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    text = path.read_text()
    assert '"account"' not in text and "email" not in text
    assert "org-1" not in text  # digest only, not the organization UUID
    assert set(_cached(root)["catalog"]) == {
        "engine", "engineVersion", "source", "fetchedAt", "note", "models",
    }
    cached = M.read_models(engine="claude")
    assert calls["claude"] == 1
    assert cached[0].source == "cache"
    assert cached[0].fetched_at == first[0].fetched_at
    assert cached[0].models == first[0].models
    assert cached[0].note is None


def test_ttl_expiry_refetches(isolated_models):
    root, _, calls = isolated_models
    M.read_models(engine="claude")
    saved = _cached(root)
    saved["catalog"]["fetchedAt"] = (
        datetime.now(timezone.utc) - timedelta(days=2)
    ).isoformat()
    M._atomic_private_json(root / "models-claude.json", saved)
    result = M.read_models(engine="claude")
    assert result[0].source == "live"
    assert calls["claude"] == 2


@pytest.mark.parametrize("changed", ["version", "org"])
def test_claude_cache_key_change_refetches(isolated_models, changed):
    _, identity, calls = isolated_models
    M.read_models(engine="claude")
    identity[changed] = "2.1.322" if changed == "version" else "org-2"
    assert M.read_models(engine="claude")[0].source == "live"
    assert calls["claude"] == 2


def test_refresh_forces_probe_and_codex_uses_home(isolated_models):
    _, _, calls = isolated_models
    assert M.read_models(engine="codex")[0].source == "live"
    assert calls["cwd"] == str(Path.home())
    assert M.read_models(engine="codex")[0].source == "cache"
    assert M.read_models(engine="codex", refresh=True)[0].source == "live"
    assert calls["codex"] == 2


def test_stale_fallback_preserves_last_success(isolated_models, monkeypatch):
    _, _, calls = isolated_models
    first = M.read_models(engine="claude")[0]
    monkeypatch.setattr(M, "request_models", lambda **_: (_ for _ in ()).throw(OSError()))
    stale = M.read_models(engine="claude", refresh=True)[0]
    assert stale.source == "stale" and "unavailable" in stale.note
    assert stale.models == first.models
    assert stale.fetched_at == first.fetched_at
    assert calls["claude"] == 1


def test_expired_cache_is_returned_stale_when_probe_fails(isolated_models, monkeypatch):
    root, _, _ = isolated_models
    M.read_models(engine="claude")
    saved = _cached(root)
    saved["catalog"]["fetchedAt"] = (
        datetime.now(timezone.utc) - timedelta(days=2)
    ).isoformat()
    M._atomic_private_json(root / "models-claude.json", saved)
    monkeypatch.setattr(M, "request_models", lambda **_: {"kind": "error"})
    result = M.read_models(engine="claude")[0]
    assert result.source == "stale"
    assert result.fetched_at == saved["catalog"]["fetchedAt"]


def test_builtin_fallback_is_truthful_per_engine(isolated_models, monkeypatch):
    monkeypatch.setattr(M, "request_models", lambda **_: {"kind": "error", "error": "offline"})
    monkeypatch.setattr(M, "list_model_entries",
                        lambda **_: (_ for _ in ()).throw(RuntimeError("offline")))
    claude, codex = M.read_models()
    assert (claude.source, codex.source) == ("builtin", "builtin")
    assert [m.id for m in claude.models] == list(constants.MODEL_ALIASES)
    assert all(m.resolved is None and m.alias and m.efforts == [] for m in claude.models)
    assert codex.models == []  # there are no safe builtin Codex model names
    assert claude.note and codex.note


def test_unsafe_cache_is_not_read(isolated_models, monkeypatch):
    root, _, _ = isolated_models
    M.read_models(engine="claude")
    (root / "models-claude.json").chmod(0o644)
    monkeypatch.setattr(M, "request_models", lambda **_: {"kind": "error"})
    assert M.read_models(engine="claude")[0].source == "builtin"


def test_python_api_and_exports(isolated_models):
    assert ccx.ModelCatalog is ModelCatalog and ccx.ModelInfo is ModelInfo
    # These read-only methods have no dependency on client worker state.
    claude = object.__new__(ccx.CcxClient).models()
    codex = object.__new__(ccx.CodexClient).models()
    assert (claude.engine, codex.engine) == ("claude", "codex")
    assert isinstance(claude, ModelCatalog) and isinstance(claude.models[0], ModelInfo)
    workers = object.__new__(ccx.WorkerClient)
    both = workers.models()
    assert [catalog["engine"] for catalog in both] == ["claude", "codex"]
    assert both[0]["models"][0]["displayName"]
    assert [catalog["engine"] for catalog in workers.models("codex")] == ["codex"]
    with pytest.raises(InvalidRequest):
        workers.models("bad")


def test_cli_json_shape_one_and_both(isolated_models, capsys):
    assert "models" in build_parser().format_help().split("Account:")[1]
    _run_cli("--engine", "codex", "--json")
    one = json.loads(capsys.readouterr().out)
    assert len(one) == 1 and one[0]["engine"] == "codex"
    assert one[0]["models"][0]["id"] == "gpt-6"
    assert one[0]["models"][0]["defaultEffort"] == "medium"
    _run_cli("--json")
    both = json.loads(capsys.readouterr().out)
    assert [item["engine"] for item in both] == ["claude", "codex"]
    assert both[0]["models"][0]["displayName"] == "Default (recommended)"
    assert all("account" not in json.dumps(item) for item in both)


def test_cli_main_routes_models_through_the_sealed_root(isolated_models, capsys):
    from ccx.cli.main import _main
    _main(["models", "--engine", "claude", "--json"])
    rows = json.loads(capsys.readouterr().out)
    assert [row["engine"] for row in rows] == ["claude"]
    assert rows[0]["source"] == "live"


def test_cli_human_table_default_then_aliases_then_pinned(isolated_models, capsys):
    _run_cli("--engine", "claude")
    text = capsys.readouterr().out
    assert "Claude 2.1.321 models (live):" in text
    assert "--model" in text and "Display name" in text
    assert "Resolved" in text and "Efforts" in text
    assert "description" not in text
    assert text.index("default") < text.index("opus") < text.index("claude-opus-4-6")
    assert "Default (recommended)" in text and "claude-fable-5-2" in text
    assert "low, medium, high, max" in text


def test_cli_codex_human_table_shows_default_efforts(isolated_models, capsys):
    _run_cli("--engine", "codex")
    text = capsys.readouterr().out
    assert "Codex models (live):" in text
    assert text.index("gpt-6") < text.index("gpt-5.6")
    assert "GPT-6" in text and "low, medium, high" in text


def test_cli_both_engines_one_failed_still_prints_other(isolated_models, monkeypatch, capsys):
    monkeypatch.setattr(M, "request_models", lambda **_: {"kind": "error", "error": "offline"})
    _run_cli()
    output = capsys.readouterr().out
    assert "Claude models (builtin):" in output
    assert "Codex models (live):" in output
    assert "gpt-6" in output


def test_cli_explicit_refresh_failure_exits_3_without_hiding_other(
    isolated_models, monkeypatch, capsys,
):
    monkeypatch.setattr(M, "request_models", lambda **_: {"kind": "error", "error": "offline"})
    with pytest.raises(SystemExit) as stopped:
        _run_cli("--refresh", "--json")
    assert stopped.value.code == constants.EXIT_UNAVAILABLE
    printed = capsys.readouterr()
    assert [item["engine"] for item in json.loads(printed.out)] == ["claude", "codex"]
    assert "claude refresh failed" in printed.err
    assert '"source": "builtin"' in printed.out
    assert '"source": "live"' in printed.out


def test_cli_explicit_refresh_failure_with_cache_is_stale_and_exits_3(
    isolated_models, monkeypatch, capsys,
):
    M.read_models(engine="claude")
    monkeypatch.setattr(M, "request_models", lambda **_: {"kind": "error"})
    with pytest.raises(SystemExit) as stopped:
        _run_cli("--engine", "claude", "--refresh")
    assert stopped.value.code == constants.EXIT_UNAVAILABLE
    printed = capsys.readouterr()
    assert "Claude 2.1.321 models (stale):" in printed.out
    assert "Opus 5.5" in printed.out
    assert "refresh failed" in printed.err


def test_codex_raw_model_entries_preserved_across_pages_without_native_process(monkeypatch):
    calls = []

    class FakeClient:
        def request(self, method, params, timeout):
            calls.append((method, params, timeout))
            return ({"data": [*_codex_entries(), "bad-entry"], "nextCursor": "next"}
                    if not params else {"data": [{"model": "later"}], "nextCursor": None})

    @contextmanager
    def fake_app_server(**kwargs):
        assert kwargs["cwd"] == "/neutral"
        yield FakeClient()

    monkeypatch.setattr(runtime, "_short_lived_app_server", fake_app_server)
    entries = runtime.list_model_entries(cwd="/neutral", timeout=4)
    assert entries == [*_codex_entries(), {"model": "later"}]
    assert calls == [("model/list", {}, 4), ("model/list", {"cursor": "next"}, 4)]
    assert runtime.list_models(cwd="/neutral", timeout=4) == ["gpt-6", "gpt-5.6", "later"]


def test_request_models_sanitizes_initialize_account_from_fake_child(tmp_path, monkeypatch):
    executable = tmp_path / "fake-claude"
    executable.write_text('''#!/usr/bin/env python3
import json, os, sys
open(os.environ["FAKE_CWD_OUT"], "w").write(os.getcwd())
request = json.loads(sys.stdin.readline())
assert request["request"]["subtype"] == "initialize"
response = {"type": "control_response", "response": {
    "request_id": request["request_id"], "subtype": "success",
    "response": {
        "claude_code_version": "2.1.321",
        "models": [{"value": "opus", "resolvedModel": "claude-opus-5-5"}],
        "account": {"email": "private-sentinel@example.invalid"}
    }
}}
print(json.dumps(response), flush=True)
sys.stdin.read()
''')
    executable.chmod(0o700)
    monkeypatch.setattr(usage_control, "CLAUDE_BIN", str(executable))
    monkeypatch.setenv("FAKE_CWD_OUT", str(tmp_path / "cwd"))
    monkeypatch.setenv("HOME", str(tmp_path))
    reply = usage_control.request_models(timeout=3)
    # Run from home: a project's own settings must not shape the shared list.
    assert (tmp_path / "cwd").read_text() == str(tmp_path)
    assert reply == {"kind": "success", "body": {
        "claude_code_version": "2.1.321",
        "models": [{"value": "opus", "resolvedModel": "claude-opus-5-5"}],
    }}
    assert "account" not in json.dumps(reply) and "private-sentinel" not in json.dumps(reply)
