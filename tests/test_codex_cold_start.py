"""Two simultaneous first Codex users must share the original codexd boot."""
from __future__ import annotations
import ccx.store.root as store_root

from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.store.root import GenerationBusy, require_ccx_active
from test_generation_store import B, C, short_root  # noqa: F401 - fixture
from test_codex_native_effects import _fake_codex_executable


def test_spawn_lock_retries_short_storage_windows_until_caller_deadline(
    short_root, monkeypatch,
):
    """A normal 3s inode-lock window is not the native boot deadline."""
    from ccx.store import store as generation
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    require_ccx_active(short_root)
    original = store_root._flock_at
    attempts = []

    @contextmanager
    def fake_busy(fd, name, **kwargs):
        if name == "codexd.spawn.lock":
            attempts.append(kwargs["timeout"])
            if len(attempts) < 3:
                raise GenerationBusy("first singleton is still booting")
        with original(fd, name, **kwargs):
            yield

    monkeypatch.setattr(store_root, "_flock_at", fake_busy)
    with codex_runtime.daemon_spawn_lock(time.monotonic() + 4):
        assert len(attempts) == 3
    assert all(0 < timeout <= 3 for timeout in attempts)


def test_ensure_daemon_waits_for_transitional_first_boot_without_spawning(monkeypatch):
    from ccx.store.worker_state import RUNTIME_INCONCLUSIVE, RUNTIME_SERVED
    response = {"ok": True, "transport": RUNTIME_SERVED, "state": "ready",
                "rpcGeneration": constants.CODEXD_RPC_GENERATION}
    transitional = {"ok": False, "transport": RUNTIME_INCONCLUSIVE,
                    "transitional": True}
    checks = iter([transitional, transitional])

    @contextmanager
    def same_spawn_lock(_deadline):
        yield

    monkeypatch.setattr(codex_runtime, "ensure_dirs", lambda: None)
    monkeypatch.setattr(codex_runtime, "settled_daemon_status", lambda deadline: next(checks))
    monkeypatch.setattr(codex_runtime, "daemon_spawn_lock", same_spawn_lock)
    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout: response)
    monkeypatch.setattr(codex_runtime.subprocess, "Popen", lambda *a, **k: pytest.fail(
        "a transitional singleton must never launch another daemon"))
    result = codex_runtime.ensure_daemon(time.monotonic() + 4)
    assert result == response


def test_two_cold_cli_creates_wait_for_one_fake_codexd_boot(
    short_root, tmp_path, monkeypatch,
):
    """Gate the real app-server initialize past the lock window; no duplicate boot."""
    root = short_root
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    require_ccx_active(root)
    trace = tmp_path / "native-cold-start-trace"
    gate = tmp_path / "release-native-initialize"
    entered = tmp_path / "second-client-entered-spawn-lock"
    fake = _fake_codex_executable(tmp_path, trace, initialize_gate=gate)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(root), "CCX_CODEX_BIN": str(fake),
           "PYTHONPATH": str(repo), "CCX_CODEXD_LINGER": "120"}
    for key in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(key, None)

    # Observe the actual second CLI entering the existing spawn fence, not
    # merely Popen() returning: a cold process may spend time importing Python.
    instrument = (
        'import ccx.codex.runtime as codex_runtime\nfrom pathlib import Path\nfrom contextlib import contextmanager\nimport sys\noriginal=codex_runtime.daemon_spawn_lock\n@contextmanager\ndef entering(deadline):\n    Path(sys.argv[1]).touch()\n    with original(deadline): yield\ncodex_runtime.daemon_spawn_lock=entering\nfrom ccx.cli.main import main; main(sys.argv[2:])'
    )
    def args(wid):
        return ["new", "--engine", "codex", "--cwd", str(tmp_path),
                "--worker-id", wid, "--start-timeout", "14", "--json"]

    first = second = None
    try:
        first = subprocess.Popen([sys.executable, "-B", "-m", "ccx", *args(B)],
                                 env=env, cwd=repo, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        until = time.monotonic() + 9
        while (not trace.exists() or "RPC initialize" not in trace.read_text()):
            assert first.poll() is None and time.monotonic() < until
            time.sleep(.02)
        second = subprocess.Popen([sys.executable, "-B", "-c", instrument,
                                   str(entered), *args(C)], env=env, cwd=repo,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True)
        until = time.monotonic() + 8
        while not entered.exists():
            assert second.poll() is None and time.monotonic() < until
            time.sleep(.02)
        # Old code would fail here after only one three-second lock window.
        until = time.monotonic() + 3.3
        while time.monotonic() < until:
            assert second.poll() is None, second.communicate(timeout=1)
            time.sleep(.03)
        gate.touch()
        out1, err1 = first.communicate(timeout=20)
        out2, err2 = second.communicate(timeout=20)
        # Report the *typed cause* on a loaded machine: a partial create is
        # proof of neither daemon startup failure nor successful admission.
        first_result = json.loads(out1)
        second_result = json.loads(out2)
        assert first.returncode == 0, (
            first_result.get("phase"), first_result.get("native_start_error"), err1,
        )
        assert second.returncode == 0, (
            second_result.get("phase"), second_result.get("native_start_error"), err2,
        )
        assert first_result["worker"] == f"codex:{B}"
        assert second_result["worker"] == f"codex:{C}"
        observed = trace.read_text()
        assert observed.count("RPC initialize") == 1
        assert observed.count("RPC thread/start") == 2
        assert codex_runtime.daemon_status(timeout=3)["state"] == "ready"
    finally:
        gate.touch(exist_ok=True)
        for child in (first, second):
            if child is not None and child.poll() is None:
                child.terminate()
                child.wait(timeout=5)
        codex_runtime.daemon_rpc({"op": "daemon-stop", "force": True}, timeout=3)
