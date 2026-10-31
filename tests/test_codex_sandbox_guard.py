"""ccx refuses once, clearly, inside a Codex command sandbox."""
import os
import subprocess
import sys
from pathlib import Path

import pytest

from ccx.cli import main as cli

REPO = Path(__file__).parent.parent


def _run(tmp_path, argv, **marker):
    root = tmp_path / "root"
    env = {**os.environ, "CCX_ROOT": str(root), "PYTHONPATH": str(REPO), **marker}
    result = subprocess.run([sys.executable, "-B", "-m", "ccx", *argv], env=env,
                            capture_output=True, text=True, timeout=25)
    return result, root


@pytest.mark.parametrize("marker", [{"CODEX_SANDBOX": "seatbelt"}])
@pytest.mark.parametrize("argv", [["list"], ["init"],
                                  ["new", "--engine", "claude", "--no-wait", "hi"]])
def test_sandboxed_commands_refuse_before_any_state(tmp_path, marker, argv):
    result, root = _run(tmp_path, argv, **marker)
    assert result.returncode == 3
    assert result.stderr.startswith("ccx unavailable: running inside the Codex sandbox (")
    assert next(iter(marker)) in result.stderr and "escalated permissions" in result.stderr
    assert "Traceback" not in result.stderr
    assert not root.exists()


def test_help_version_and_skills_still_work_in_the_sandbox(tmp_path):
    for argv in (["--version"], ["--help"], ["list", "--help"],
                 ["skills", "install", "--dir", str(tmp_path / "agent-skills")]):
        result, root = _run(tmp_path, argv, CODEX_SANDBOX="seatbelt")
        assert result.returncode == 0, (argv, result.stderr)
        assert not root.exists()


def test_internal_relays_are_exempt(monkeypatch):
    monkeypatch.setenv("CODEX_SANDBOX", "seatbelt")
    for cmd in ("_live-vars-exec", "hooks-relay", "statusline-relay", "host", "skills"):
        cli._refuse_codex_sandbox(cmd)
    with pytest.raises(SystemExit) as exc:
        cli._refuse_codex_sandbox("send")
    assert exc.value.code == 3


def test_network_disabled_alone_is_not_the_sandbox(monkeypatch):
    # Codex keeps this set on approved (escalated, unsandboxed) commands too.
    monkeypatch.delenv("CODEX_SANDBOX", raising=False)
    monkeypatch.setenv("CODEX_SANDBOX_NETWORK_DISABLED", "1")
    cli._refuse_codex_sandbox("list")


def test_escalated_command_runs(tmp_path):
    result, root = _run(tmp_path, ["list"], CODEX_SANDBOX_NETWORK_DISABLED="1")
    assert "Codex sandbox" not in result.stderr
    assert result.returncode == 0
    assert f"ccx: created store at {root}" in result.stderr
    assert root.is_dir()
