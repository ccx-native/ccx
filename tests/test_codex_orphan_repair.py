"""A codexd that dies mid-turn (crash, reboot) must not leave the worker busy."""
from __future__ import annotations

import os
import signal
import subprocess
import time

import test_codex_native_effects as effects
from test_codex_native_effects import _f2_fake_processes, _f2_new
from test_generation_store import B, short_root  # noqa: F401


def _resumable(factory):
    def make(*args, **kwargs):
        binary = factory(*args, **kwargs)
        # Real Codex turn ids are unique; the fake's counter restarts with it.
        binary.write_text(binary.read_text().replace(
            "turn_id='native-fake-turn-'+str(counter)",
            "turn_id='native-fake-turn-'+str(os.getpid())+'-'+str(counter)", 1))
        before = "        elif method=='thread/unsubscribe':\n"
        binary.write_text(binary.read_text().replace(before, (
            "        elif method=='thread/resume':\n"
            "            tid=params.get('threadId')\n"
            "            result={'thread':dict(threads[tid],status={'type':'idle'}),"
            "'model':threads[tid].get('model')}\n" + before), 1))
        return binary
    return make


def _lifecycle(store, state, seconds=5):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        lifecycle = store.record(B)["lifecycle"]
        if lifecycle["state"] == state:
            return lifecycle
        time.sleep(.05)
    raise AssertionError(store.record(B)["lifecycle"])


def _until_exited(pid, seconds=5):
    # `ps` works on Linux and macOS; a zombie (Z) has already exited.
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)],
                               capture_output=True, text=True).stdout.strip()
        if not state or state.startswith("Z"):
            return
        time.sleep(.02)
    raise AssertionError(f"codexd {pid} still running")


def test_killed_codexd_mid_turn_leaves_a_revivable_worker(
    short_root, tmp_path, monkeypatch,
):
    monkeypatch.setattr(effects, "_fake_codex_executable",
                        _resumable(effects._fake_codex_executable))
    gate = tmp_path / "completion-gate"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, completion_gate=gate,
        persisted_threads=tmp_path / "orphan-threads.json",
    ) as (cli, restart, _trace, store, rpc):
        created = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
        assert created.returncode == 0, (created.stdout, created.stderr)
        held = cli("send", B, "f1-race-held turn", "--no-wait", "--json")
        assert held.returncode == 0, (held.stdout, held.stderr)
        _lifecycle(store, "busy")

        # Like a reboot: codexd and its app-server die without retiring.
        pid = rpc({"op": "daemon-status"}, timeout=2)["pid"]
        os.killpg(pid, signal.SIGKILL)
        _until_exited(pid)
        restart()
        repaired = _lifecycle(store, "crashed")
        assert repaired["activeTurnId"] is None
        assert repaired["lastError"] == "codexd exited without unloading this worker"

        gate.touch()
        after = cli("send", B, "after the crash", "--wait", "--json", timeout=30)
        assert after.returncode == 0, (after.stdout, after.stderr)
        assert _lifecycle(store, "idle")["activeTurnId"] is None
