"""`ccx help`, subcommand usage on bad arguments, and local reset times."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parent.parent


def _ccx(tmp_path, *args):
    env = {"HOME": str(tmp_path), "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8",
           "PYTHONPATH": str(REPO), "CCX_ROOT": str(tmp_path / "no-store")}
    return subprocess.run([sys.executable, "-B", "-m", "ccx", *args], env=env,
                          capture_output=True, text=True, timeout=30, cwd="/")


def test_help_command_is_an_alias_for_help_flags(tmp_path):
    top = _ccx(tmp_path, "help")
    assert top.returncode == 0 and top.stdout.startswith("usage: ccx [-h]")
    send = _ccx(tmp_path, "help", "send")
    assert send.returncode == 0 and send.stdout.startswith("usage: ccx send")
    assert not (tmp_path / "no-store").exists()


def test_unknown_arguments_show_the_subcommand_usage(tmp_path):
    result = _ccx(tmp_path, "wait", "A", "B")
    assert result.returncode != 0
    assert result.stderr.startswith("usage: ccx wait")
    assert "ccx wait: error: unrecognized arguments: B" in result.stderr
    nested = _ccx(tmp_path, "controller", "status", "x", "--bogus")
    assert nested.stderr.startswith("usage: ccx controller status")


def test_reset_times_are_local_with_a_zone(monkeypatch):
    from ccx.cli.commands._common import local_reset_time
    monkeypatch.setenv("TZ", "Europe/Madrid")
    time.tzset()
    try:
        assert local_reset_time("2026-10-30T17:59:00+00:00") == "2026-10-30 18:59 CET"
        assert local_reset_time(1793383140) == "2026-10-30 18:59 CET"
        assert local_reset_time(None) == ""
        assert local_reset_time("not a time") == "not a time"
    finally:
        os.environ.pop("TZ", None)
        time.tzset()
