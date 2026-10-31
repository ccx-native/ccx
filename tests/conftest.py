"""Shared pytest setup for the public ccx test suite.

These tests exercise *pure* logic only — transcript cleanup, usage-cap math,
record/eligibility helpers, list filters, prompt-source validation, and CLI
parser wiring. Nothing here touches a live Claude process, the daemon, the
network, or real quota.

Two responsibilities:

1. Sandbox the home-derived path constants and the agent binaries. ``ccx.constants`` reads ``CCX_ROOT``
   and ``CLAUDE_CONFIG_DIR`` from the environment *at import time*, so we set
   them to throwaway temp dirs BEFORE any ``ccx`` module is imported. That keeps
   incidental record writes out of the developer's real ``~/.ccx`` / ``~/.claude``.

2. Make ``import ccx`` work regardless of how pytest is invoked. ``python -m
   pytest`` already puts the repo root on ``sys.path``; a bare ``pytest`` may not,
   so we insert the repo root explicitly.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import sys
import tempfile
import time

import pytest

# --- 1. sandbox the path constants (must happen before importing ccx) --------
_SANDBOX = pathlib.Path(tempfile.mkdtemp(prefix="ccx-test-"))
os.environ["CCX_ROOT"] = str(_SANDBOX / "ccx")
os.environ["CLAUDE_CONFIG_DIR"] = str(_SANDBOX / "claude")
# The usage cache defaults under CCX_ROOT; a developer override must not point
# the suite at a real (possibly shared) cache.
os.environ.pop("CCX_USAGE_CACHE", None)
# Tests may themselves run inside a Codex sandbox; the CLI guard is tested explicitly.
os.environ.pop("CODEX_SANDBOX", None)
os.environ.pop("CODEX_SANDBOX_NETWORK_DISABLED", None)
# Point the agent binaries at paths that do not exist, so a mis-targeted mock
# fails fast instead of launching a real claude/codex. The opt-in live parity
# proof (CCX_REAL_PARITY=1) needs the real binaries, so leave them alone there.
if os.environ.get("CCX_REAL_PARITY") != "1":
    os.environ["CCX_CLAUDE_BIN"] = str(_SANDBOX / "no-claude")
    os.environ["CCX_CODEX_BIN"] = str(_SANDBOX / "no-codex")

# --- 2. ensure the repo root is importable -----------------------------------
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# A suite run from inside a ccx worker inherits its shell bridge; drop it the
# way a nested worker does, so the first test doesn't consume it for the rest.
from ccx.controllers.live_vars import sanitize_inherited_shell_bridge_env  # noqa: E402
sanitize_inherited_shell_bridge_env()


@pytest.fixture
def socket_tmp_path():
    """Short throwaway pathname for real AF_UNIX binds on both OS families.

    macOS pytest defaults to deep /var/folders/... paths that can exceed its
    104-byte sun_path even before the staged /.XXXXXXXX/s suffix is appended.
    This fixture changes only test staging, never the production path budget.
    """
    from ccx.system import oslayer

    if oslayer.UNIX_SUN_PATH_BYTES == 104:
        # Mac tests may write only inside this test account's ccx-runs tree.
        parent = pathlib.Path.home() / "ccx-runs" / ".socket-tmp"
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    else:
        parent = pathlib.Path("/tmp")
    name = tempfile.mkdtemp(prefix="ct-", dir=parent)
    try:
        yield pathlib.Path(name)
    finally:
        # A stopped test host's hook children can still write here briefly.
        for _ in range(30):
            shutil.rmtree(name, ignore_errors=True)
            if not os.path.exists(name):
                break
            time.sleep(.1)


@pytest.fixture
def executable_engine_stubs(tmp_path, monkeypatch):
    """Runnable-but-inert provider stand-ins for mock-only native effect tests.

    Callers simulate the actual native effect. This fixture satisfies the
    executable preflight without ever running an installed provider binary.
    Missing-binary regression tests deliberately do not request this fixture.
    """
    from ccx import constants

    fake = tmp_path / "fake-installed-agent-cli"
    fake.write_text("#!/bin/sh\nexit 92\n")
    fake.chmod(0o700)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(fake))
    monkeypatch.setenv("CCX_CLAUDE_BIN", str(fake))
    monkeypatch.setenv("CCX_CODEX_BIN", str(fake))
    return fake
