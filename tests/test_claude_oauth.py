from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

from ccx.claude import oauth as O


def _write_oauth(path, **overrides):
    oauth = {
        "accessToken": "access-old",
        "refreshToken": "refresh-old",
        "expiresAt": 2_000_000,
        "refreshTokenExpiresAt": 9_000_000,
        "scopes": ["user:profile", "user:inference"],
    }
    oauth.update(overrides)
    path.mkdir(parents=True, exist_ok=True)
    (path / ".credentials.json").write_text(json.dumps({"claudeAiOauth": oauth}))
    return oauth


def test_oauth_token_uses_unexpired_credential_without_refresh(tmp_path, monkeypatch):
    _write_oauth(tmp_path)
    monkeypatch.setattr(O, "CLAUDE_HOME", tmp_path)
    monkeypatch.setattr(O.time, "time", lambda: 1_000.0)
    monkeypatch.setattr(
        O.subprocess,
        "run",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("unexpected refresh")),
    )

    assert O.oauth_token() == "access-old"


def test_oauth_token_refreshes_expired_credential_with_native_claude(tmp_path, monkeypatch):
    old = _write_oauth(tmp_path, expiresAt=900_000)
    monkeypatch.setattr(O, "CLAUDE_HOME", tmp_path)
    monkeypatch.setattr(O, "CLAUDE_BIN", "/opt/claude")
    monkeypatch.setattr(O.time, "time", lambda: 1_000.0)

    def fake_run(argv, **kwargs):
        assert argv == ["/opt/claude", "auth", "login", "--claudeai"]
        assert kwargs["check"] is False
        assert kwargs["stdin"] is subprocess.DEVNULL
        assert kwargs["stdout"] is subprocess.DEVNULL
        assert kwargs["stderr"] is subprocess.DEVNULL
        assert kwargs["timeout"] == O._NATIVE_REFRESH_TIMEOUT_S
        assert kwargs["env"]["CLAUDE_CODE_OAUTH_REFRESH_TOKEN"] == "refresh-old"
        assert kwargs["env"]["CLAUDE_CODE_OAUTH_SCOPES"] == "user:profile user:inference"
        _write_oauth(
            tmp_path,
            accessToken="access-new",
            refreshToken="refresh-new",
            expiresAt=2_000_000,
            refreshTokenExpiresAt=old["refreshTokenExpiresAt"],
        )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(O.subprocess, "run", fake_run)
    assert O.oauth_token() == "access-new"


def test_oauth_token_refresh_failure_preserves_previous_behavior(tmp_path, monkeypatch):
    _write_oauth(tmp_path, expiresAt=900_000)
    monkeypatch.setattr(O, "CLAUDE_HOME", tmp_path)
    monkeypatch.setattr(O.time, "time", lambda: 1_000.0)
    monkeypatch.setattr(
        O.subprocess,
        "run",
        lambda *a, **k: SimpleNamespace(returncode=1),
    )

    assert O.oauth_token() == "access-old"


def test_oauth_token_refresh_timeout_preserves_previous_behavior(tmp_path, monkeypatch):
    _write_oauth(tmp_path, expiresAt=900_000)
    monkeypatch.setattr(O, "CLAUDE_HOME", tmp_path)
    monkeypatch.setattr(O.time, "time", lambda: 1_000.0)
    monkeypatch.setattr(
        O.subprocess,
        "run",
        lambda *a, **k: (_ for _ in ()).throw(subprocess.TimeoutExpired("claude", 12)),
    )

    assert O.oauth_token() == "access-old"


def test_oauth_token_does_not_refresh_expired_refresh_token(tmp_path, monkeypatch):
    _write_oauth(tmp_path, expiresAt=900_000, refreshTokenExpiresAt=950_000)
    monkeypatch.setattr(O, "CLAUDE_HOME", tmp_path)
    monkeypatch.setattr(O.time, "time", lambda: 1_000.0)
    monkeypatch.setattr(
        O.subprocess,
        "run",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("unexpected refresh")),
    )

    assert O.oauth_token() == "access-old"


def test_oauth_headers_follow_installed_claude_version(monkeypatch):
    O.claude_code_version.cache_clear()
    monkeypatch.setattr(O, "CLAUDE_BIN", "/home/u/.local/bin/claude")
    monkeypatch.setattr(
        O.os.path,
        "realpath",
        lambda p: "/home/u/.local/share/claude/versions/2.1.259",
    )
    headers = O.oauth_headers("secret")
    assert headers["User-Agent"] == "claude-cli/2.1.259 (external, cli)"
    assert headers["x-app"] == "cli"
    assert headers["anthropic-beta"] == "oauth-2025-04-20"
    assert headers["Authorization"] == "Bearer secret"
    O.claude_code_version.cache_clear()


def test_oauth_organization_uuid_reads_claude_account_dotfile(tmp_path, monkeypatch):
    path = tmp_path / ".claude.json"
    path.write_text(json.dumps({"oauthAccount": {"organizationUuid": "org-123"}}))
    monkeypatch.setattr(O, "CLAUDE_DOTFILE", path)
    assert O.oauth_organization_uuid() == "org-123"
