"""Automatic first-root initialization, using only fake native binaries/temp roots."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import time

import pytest

from ccx.store.liveness import owned_pid_alive
from ccx.store.root import require_ccx_active, root_status

REPO = Path(__file__).resolve().parent.parent
A = "aaaaaaaa-1234-4000-8000-000000000001"
B = "bbbbbbbb-1234-4000-8000-000000000002"


def _env(tmp_path, root):
    wrapper = tmp_path / "test-claude-only"
    wrapper.write_text(
        "#!/bin/sh\nexec " + shlex.quote(sys.executable) + " "
        + shlex.quote(str(REPO / "tests" / "fake_claude.py"))
        + ' "$@"\n', encoding="utf-8",
    )
    wrapper.chmod(0o700)
    env = {**os.environ,
           "CCX_ROOT": str(root),
           "CCX_CLAUDE_BIN": str(wrapper),
           "CLAUDE_CONFIG_DIR": str(tmp_path / "fake-claude-home"),
           "CCX_SPAWN_SCOPE": "0",
           "CCX_BOOT_TIMEOUT_S": "15",
           "FAKE_MODE": "",
           "FAKE_LOG": str(tmp_path / "fake-claude-turns.log"),
           "PYTHONPATH": str(REPO),
           "PYTHONDONTWRITEBYTECODE": "1"}
    for name in ("CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE", "CCX_SELF_SHORT",
                 "CODEX_SANDBOX"):
        env.pop(name, None)
    return env


def _cli(env, *args, input=None):
    return subprocess.run([sys.executable, "-B", "-m", "ccx", *args],
                          env=env, cwd=REPO, capture_output=True, text=True,
                          input=input, timeout=25)


def _stop(root):
    try:
        store = require_ccx_active(root)
    except Exception:
        return
    identified = set()
    for row in store.list_workers():
        rec = store.record(row["worker"])
        life = rec.get("lifecycle") or {}
        for pid_key, start_key in (("hostPid", "hostStart"), ("workerPid", "workerStart")):
            pid, start = life.get(pid_key), life.get(start_key)
            if owned_pid_alive(pid, start):
                identified.add((pid, start))
    for pid, start in identified:
        if owned_pid_alive(pid, start):
            try: os.kill(pid, signal.SIGTERM)
            except ProcessLookupError: pass
    end = time.monotonic() + 2
    while time.monotonic() < end and any(owned_pid_alive(pid, start)
                                       for pid, start in identified):
        time.sleep(.04)
    for pid, start in identified:
        if owned_pid_alive(pid, start):
            try: os.kill(pid, signal.SIGKILL)
            except ProcessLookupError: pass


@pytest.mark.parametrize("initial", ["missing", "empty"])
def test_new_creates_private_store_with_single_stderr_line(tmp_path, initial):
    root = tmp_path / "worker-store"
    if initial == "empty":
        root.mkdir(mode=0o700)
    env = _env(tmp_path, root)
    try:
        result = _cli(env, "new", "--engine", "claude", "--cwd", str(tmp_path),
                      "--worker-id", A, "--no-wait", "--json")
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert result.stderr.count(f"ccx: created store at {root}\n") == 1, result.stderr
        assert json.loads(result.stdout)["worker_id"] == A
        assert root_status(root).kind == "initialized"
        assert root.stat().st_mode & 0o777 == 0o700
        assert require_ccx_active(root).record(A)["native"]["sessionId"]
        read = _cli(env, "list", "--json")
        assert read.returncode == 0 and read.stderr == ""
        assert {row["worker_id"] for row in json.loads(read.stdout)} == {A}
    finally:
        _stop(root)


def test_two_concurrent_cli_creates_share_one_initialized_root(tmp_path):
    root = tmp_path / "race"
    env = _env(tmp_path, root)
    commands = [
        [sys.executable, "-B", "-m", "ccx", "new", "--engine", "claude",
         "--cwd", str(tmp_path), "--worker-id", worker,
         "--no-wait", "--json"]
        for worker in (A, B)
    ]
    processes = []
    try:
        for command in commands:
            processes.append(subprocess.Popen(
                command, env=env, cwd=REPO,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True))
        results = [proc.communicate(timeout=25) for proc in processes]
        for proc, (stdout, stderr) in zip(processes, results):
            assert proc.returncode == 0, (proc.returncode, stdout, stderr)
            assert json.loads(stdout)["worker_id"] in (A, B)
        assert sum(err.count(f"ccx: created store at {root}\n")
                   for _out, err in results) == 1
        store = require_ccx_active(root)
        assert {row["worker_id"] for row in store.list_workers()} == {A, B}
        assert len(list((root / "identity" / "generations").iterdir())) == 1
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
        _stop(root)


def test_rpc_worker_create_opens_root_without_json_contamination(tmp_path):
    root = tmp_path / "rpc-root"
    env = _env(tmp_path, root)
    envelope = {"protocol_version": 2, "request_id": "fresh-rpc-create",
                "op": "worker.create",
                "params": {"engine": "claude", "cwd": str(tmp_path), "worker_id": A},
                "timeout_ms": 10000}
    try:
        reply = _cli(env, "rpc", input=json.dumps(envelope))
        assert reply.returncode == 0, reply.stderr
        value = json.loads(reply.stdout)
        assert value["ok"] is True, value
        assert value["request_id"] == "fresh-rpc-create"
        assert reply.stderr == ""
        assert root_status(root).kind == "initialized"
        assert require_ccx_active(root).record(A)
    finally:
        _stop(root)


def test_hidden_host_daemons_and_rpc_serve_stay_separate(tmp_path):
    root = tmp_path / "not-created"
    env = _env(tmp_path, root)
    host = _cli(env, "host", A)
    assert host.returncode == 3
    assert "no CCX store" in host.stderr
    assert not root.exists()
    for code in (
        "from ccx.codex.daemon.daemon import CodexDaemon; CodexDaemon().run()",
        "from ccx.notify.daemon import run; run()",
    ):
        command = subprocess.run([sys.executable, "-B", "-c", code], env=env,
                                 cwd=REPO, capture_output=True, text=True, timeout=8)
        assert command.returncode != 0
        assert "no CCX store" in command.stderr
        assert not root.exists()


def test_missing_parent_new_and_rpc_do_not_mkdir_parents(tmp_path):
    root = tmp_path / "missing-parent" / "store"
    env = _env(tmp_path, root)
    new = _cli(env, "new", "--engine", "claude", "--cwd", str(tmp_path),
               "--worker-id", A, "--json")
    assert new.returncode == 3
    assert "parent directory" in new.stderr
    rpc = _cli(env, "rpc", input=json.dumps({
        "protocol_version": 2, "request_id": "missing-parent",
        "op": "worker.inspect", "params": {"target": A},
    }))
    assert rpc.returncode == 0
    assert json.loads(rpc.stdout)["error"]["code"] == "unavailable"
    assert "parent directory" in rpc.stdout
    assert not root.parent.exists()


def test_fork_without_source_initializes_but_does_not_launch(tmp_path):
    root = tmp_path / "new-fork-root"
    env = _env(tmp_path, root)
    result = _cli(env, "fork", A, "--json")
    assert result.returncode != 0
    assert result.stderr.count(f"ccx: created store at {root}\n") == 1
    assert root_status(root).kind == "initialized"
    assert require_ccx_active(root).list_workers() == []
    assert not (tmp_path / "fake-claude-turns.log").exists()


def test_import_exact_fake_native_session_initializes_root(tmp_path):
    from uuid import uuid4
    from test_exact_session_import import _environment, _original, _stop_only_fake

    root = tmp_path / "fresh-import-root"
    home, cwd, _argv_log, _env, call = _environment(root, tmp_path)
    sid = str(uuid4())
    _original(home, cwd, sid)
    worker_id = None
    try:
        result = call("import", sid)
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert result.stderr.count(f"ccx: created store at {root}\n") == 1
        worker = result.stdout.strip()
        assert worker.startswith("claude:") and worker != "claude:" + sid
        worker_id = worker.partition(":")[2]
        rec = require_ccx_active(root).record(worker)
        assert rec["origin"] == "ccx-import"
        assert rec["native"]["sessionId"] == sid
    finally:
        if worker_id is not None:
            _stop_only_fake(root, worker_id)
