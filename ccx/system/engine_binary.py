"""One executable lookup shared by native engine preflight and launch."""
from __future__ import annotations

import os
import shutil
import subprocess
import json

from ccx import constants
from ccx.errors import EngineCliNotFound, InvalidRequest


def resolve_engine_binary(engine: str) -> str:
    """Return a runnable absolute executable, or a typed missing-CLI failure.

    Claude's selected binary is pinned by constants at import, while Codex's
    override is read at launch time. Both accept a literal executable path or
    the same PATH lookup used when starting a native subprocess.
    """
    if engine == "claude":
        candidate = constants.CLAUDE_BIN
    elif engine == "codex":
        candidate = os.environ.get("CCX_CODEX_BIN") or (
            shutil.which("codex") or os.path.expanduser("~/.local/bin/codex")
        )
    else:
        raise InvalidRequest(f"unknown engine: {engine!r}")
    found = shutil.which(candidate)
    if found is None or not os.path.isfile(found):
        title = engine.capitalize()
        raise EngineCliNotFound(
            f"{title} CLI not found (looked for {candidate}); "
            f"install {title} or set CCX_{engine.upper()}_BIN"
        )
    return os.path.abspath(found)


def account_signed_out(engine: str, binary: str, *, timeout: float = 3) -> bool:
    """Check auth only after a usage read has failed; never initiate login."""
    args = ("auth", "status") if engine == "claude" else ("login", "status")
    try:
        result = subprocess.run(
            [binary, *args], stdin=subprocess.DEVNULL, capture_output=True,
            text=True, timeout=timeout, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    combined = (result.stdout + "\n" + result.stderr).strip()
    if engine == "claude":
        try:
            status = json.loads(result.stdout)
            if isinstance(status, dict) and status.get("loggedIn") is False:
                return True
        except (ValueError, TypeError):
            pass
    lowered = combined.lower()
    return any(phrase in lowered for phrase in (
        "not logged in", "not signed in", "not authenticated", "logged out",
    ))
