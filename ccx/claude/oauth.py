"""Claude Code OAuth identity shared by first-party account API calls.

These calls intentionally identify as the locally installed Claude Code build.
Anthropic's private account endpoints use that surface/version signal for feature
eligibility, so a baked-in version silently turns a valid account into a stale
client as Claude Code updates.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from functools import lru_cache
from pathlib import Path

from ccx.constants import CLAUDE_BIN, CLAUDE_DOTFILE, CLAUDE_HOME, USAGE_BETA

_VERSION_RE = re.compile(r"(?<![0-9])([0-9]+\.[0-9]+\.[0-9]+)(?![0-9])")
_NATIVE_REFRESH_TIMEOUT_S = 12.0


def _read_json(path: Path) -> dict | None:
    try:
        value = json.loads(path.read_text())
    except Exception:
        return None
    return value if isinstance(value, dict) else None


def _oauth_credentials() -> dict | None:
    data = _read_json(CLAUDE_HOME / ".credentials.json")
    value = (data or {}).get("claudeAiOauth")
    return value if isinstance(value, dict) else None


def _token_expired(credentials: dict, *, now_ms: int | None = None) -> bool:
    """Whether Claude's stored access token is conclusively expired.

    Older/partially-written credential records can omit ``expiresAt``.  In that
    case the provider remains authoritative, so keep the token rather than
    guessing that it is stale.
    """
    expires_at = credentials.get("expiresAt")
    if not isinstance(expires_at, (int, float)):
        return False
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    return float(expires_at) <= now_ms


def _native_refresh(credentials: dict) -> bool:
    """Ask the installed Claude CLI to rotate an expired OAuth credential.

    Claude Code owns the refresh-token protocol, rotation rules, and credential
    persistence. Reusing its native refresh-token login surface keeps CCX out
    of that private contract. This is best-effort: unsupported or failed
    refreshes leave the existing credential untouched and callers fall back to
    their normal no-signal/cache behavior.
    """
    refresh_token = credentials.get("refreshToken")
    scopes = credentials.get("scopes")
    if not isinstance(refresh_token, str) or not refresh_token:
        return False
    if not isinstance(scopes, list) or not scopes or not all(
        isinstance(scope, str) and scope for scope in scopes
    ):
        return False

    refresh_expires_at = credentials.get("refreshTokenExpiresAt")
    if isinstance(refresh_expires_at, (int, float)):
        if float(refresh_expires_at) <= int(time.time() * 1000):
            return False

    env = os.environ.copy()
    env["CLAUDE_CODE_OAUTH_REFRESH_TOKEN"] = refresh_token
    env["CLAUDE_CODE_OAUTH_SCOPES"] = " ".join(scopes)
    try:
        proc = subprocess.run(
            [CLAUDE_BIN, "auth", "login", "--claudeai"],
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
            timeout=_NATIVE_REFRESH_TIMEOUT_S,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0


def oauth_token() -> str | None:
    credentials = _oauth_credentials()
    if not credentials:
        return None
    value = credentials.get("accessToken")
    if not isinstance(value, str) or not value:
        return None
    if not _token_expired(credentials):
        return value

    if _native_refresh(credentials):
        refreshed = _oauth_credentials()
        refreshed_value = (refreshed or {}).get("accessToken")
        if (
            isinstance(refreshed_value, str)
            and refreshed_value
            and not _token_expired(refreshed or {})
        ):
            return refreshed_value

    # Preserve the old behavior on refresh failure.  The provider will reject
    # a truly expired token and the usage caller can still fall back to cache.
    return value


def oauth_organization_uuid() -> str | None:
    data = _read_json(CLAUDE_DOTFILE)
    value = ((data or {}).get("oauthAccount") or {}).get("organizationUuid")
    return value if isinstance(value, str) and value else None


@lru_cache(maxsize=1)
def claude_code_version() -> str | None:
    """Best-effort local Claude Code version without a persistent subprocess cost."""
    try:
        resolved = os.path.realpath(CLAUDE_BIN)
        marker = "/claude/versions/"
        if marker in resolved:
            candidate = resolved.split(marker, 1)[1].split("/", 1)[0]
            match = _VERSION_RE.fullmatch(candidate)
            if match:
                return match.group(1)
    except (OSError, TypeError):
        pass

    try:
        proc = subprocess.run(
            [CLAUDE_BIN, "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    match = _VERSION_RE.search((proc.stdout or "") + "\n" + (proc.stderr or ""))
    return match.group(1) if match else None


def oauth_headers(token: str, *, content_type: bool = True) -> dict[str, str]:
    """Headers matching Claude Code's current first-party CLI surface.

    If the installed version cannot be resolved we deliberately send an
    unversioned identity instead of pretending to be an old known release. The
    usage endpoint still degrades normally, while private feature eligibility
    fails closed provider-side.
    """
    version = claude_code_version()
    user_agent = (
        f"claude-cli/{version} (external, cli)"
        if version
        else "claude-cli (external, cli)"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "anthropic-beta": USAGE_BETA,
        "User-Agent": user_agent,
        "x-app": "cli",
    }
    if content_type:
        headers["Content-Type"] = "application/json"
    return headers
