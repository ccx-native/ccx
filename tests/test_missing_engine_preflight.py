"""Absent native CLI is a preclaim refusal on every public creation/usage path."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from ccx import constants
from ccx.ops import creation
from ccx.rpc import local_rpc
from ccx.system.engine_binary import resolve_engine_binary
from ccx.errors import SessionUnavailable
from ccx.store.root import require_ccx_active
from ccx.workers import WorkerClient
from test_fork_source import sources as sources  # noqa: F401
from test_generation_store import A, B, C, short_root as short_root  # noqa: F401

_REPO = Path(__file__).resolve().parent.parent


def _expected(engine: str, binary: Path | str) -> str:
    title = engine.capitalize()
    return (f"{title} CLI not found (looked for {binary}); "
            f"install {title} or set CCX_{engine.upper()}_BIN")


def _cli(*args, root: Path, overrides: dict | None = None):
    return subprocess.run(
        [sys.executable, "-B", "-m", "ccx", *args],
        env={**os.environ, "CCX_ROOT": str(root), "PYTHONPATH": str(_REPO),
             **(overrides or {})},
        cwd=_REPO, text=True, capture_output=True, timeout=15,
    )


@pytest.fixture(params=["claude", "codex"])
def missing_engine(request, tmp_path, monkeypatch):
    engine = request.param
    empty_path = tmp_path / "empty-path"
    empty_path.mkdir()
    missing = tmp_path / f"uninstalled-{engine}"
    monkeypatch.setenv("PATH", str(empty_path))
    monkeypatch.setenv(f"CCX_{engine.upper()}_BIN", str(missing))
    if engine == "claude":
        # Constants deliberately bind native Claude's chosen binary on import.
        monkeypatch.setattr(constants, "CLAUDE_BIN", str(missing))
    return engine, missing


def test_resolver_accepts_only_runnable_explicit_or_path_binary(missing_engine, tmp_path, monkeypatch):
    engine, missing = missing_engine
    message = _expected(engine, missing)
    with pytest.raises(SessionUnavailable, match="CLI not found") as absent:
        resolve_engine_binary(engine)
    assert str(absent.value) == message and absent.value.exit_code == 3

    missing.write_text("#!/bin/sh\nexit 0\n")
    with pytest.raises(SessionUnavailable, match="CLI not found"):
        resolve_engine_binary(engine)  # present but not executable
    missing.chmod(0o700)
    assert resolve_engine_binary(engine) == str(missing)

    native = tmp_path / "empty-path" / engine
    native.write_text("#!/bin/sh\nexit 0\n")
    native.chmod(0o700)
    if engine == "claude":
        monkeypatch.setattr(constants, "CLAUDE_BIN", engine)
    else:
        monkeypatch.delenv("CCX_CODEX_BIN")
    assert resolve_engine_binary(engine) == str(native)
    assert subprocess.run([resolve_engine_binary(engine)], check=False).returncode == 0


def test_create_preclaim_refuses_python_and_v1(missing_engine, short_root, tmp_path, monkeypatch):
    engine, missing = missing_engine
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    message = _expected(engine, missing)
    for create in (
        lambda: creation.create(engine=engine, cwd=str(tmp_path)),
        lambda: creation.create(engine=engine, cwd=str(tmp_path), worker_id=C),
        lambda: WorkerClient().create(engine=engine, cwd=str(tmp_path), worker_id=C),
    ):
        with pytest.raises(SessionUnavailable) as exc:
            create()
        assert str(exc.value) == message and exc.value.exit_code == 3
        assert store._owner(C) is None and store._claim(C) is None
        assert store.list_workers(limit=None) == []
    response = local_rpc.dispatch({
        "protocol_version": 2, "request_id": "missing-create-cli",
        "op": "worker.create", "params": {
            "engine": engine, "cwd": str(tmp_path), "worker_id": C,
        }, "timeout_ms": 3000,
    })
    assert response["ok"] is False
    assert response["error"]["code"] == "unavailable"
    assert response["error"]["message"] == message
    assert store.list_workers(limit=None) == []


def test_cli_new_and_usage_absent_leave_empty_list(missing_engine, short_root, tmp_path, monkeypatch):
    engine, missing = missing_engine
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    expected = _expected(engine, missing)
    overrides = {"CCX_CLAUDE_BIN": str(tmp_path / "no-claude"),
                 "CCX_CODEX_BIN": str(tmp_path / "no-codex")}
    overrides[f"CCX_{engine.upper()}_BIN"] = str(missing)
    for selected in ((), ("--worker-id", C)):
        result = _cli("new", "--engine", engine, "--cwd", str(tmp_path),
                      "--json", *selected, root=short_root, overrides=overrides)
        assert result.returncode == 3, (result.stdout, result.stderr)
        assert json.loads(result.stdout) == {
            "ok": False, "code": "runtime_unavailable", "message": expected,
        }
        assert store.list_workers(limit=None) == []
    for mode in (("--summary",) if engine == "claude" else ("--json",), ("--json",)):
        result = _cli("usage", "--engine", engine, *mode,
                      root=short_root, overrides=overrides)
        assert result.returncode == 3, (result.stdout, result.stderr)
        assert expected in (result.stdout + result.stderr)
        if mode == ("--json",):
            assert json.loads(result.stdout) == {
                "ok": False, "code": "runtime_unavailable", "message": expected,
            }
    with pytest.raises(SessionUnavailable, match="CLI not found") as exc:
        WorkerClient().usage(engine=engine)
    assert str(exc.value) == expected
    listed = _cli("list", "--json", root=short_root, overrides=overrides)
    assert listed.returncode == 0, listed.stderr
    assert store.list_workers(limit=None) == []
    assert C not in listed.stdout


def test_fork_refuses_before_child_reservation_or_native_source_read(missing_engine, sources,
                                                                      tmp_path):
    engine, missing = missing_engine
    store, _original, codex_source_reads = sources
    source = A if engine == "claude" else B
    target = f"{engine}:{source}"
    original = {row["worker_id"] for row in store.list_workers(limit=None)}
    message = _expected(engine, missing)

    for invoke in (
        lambda: WorkerClient().fork(target, worker_id=C),
        lambda: creation.create(engine=engine, cwd=str(tmp_path), worker_id=C),
    ):
        with pytest.raises(SessionUnavailable) as exc:
            invoke()
        assert str(exc.value) == message
        assert store._owner(C) is None and store._claim(C) is None
    response = local_rpc.dispatch({
        "protocol_version": 2, "request_id": "missing-fork-cli",
        "op": "worker.fork", "params": {"target": target, "worker_id": C},
        "timeout_ms": 3000,
    })
    assert response["error"]["code"] == "unavailable", response
    assert response["error"]["message"] == message
    overrides = {"CCX_CLAUDE_BIN": str(tmp_path / "no-claude"),
                 "CCX_CODEX_BIN": str(tmp_path / "no-codex")}
    overrides[f"CCX_{engine.upper()}_BIN"] = str(missing)
    result = _cli("fork", target, "--worker-id", C, "--json",
                  root=constants.CCX_ROOT, overrides=overrides)
    assert result.returncode == 3, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "ok": False, "code": "runtime_unavailable", "message": message,
    }
    assert {row["worker_id"] for row in store.list_workers(limit=None)} == original
    assert store._owner(C) is None and store._claim(C) is None
    assert codex_source_reads == []
    listed = _cli("list", "--json", root=constants.CCX_ROOT, overrides=overrides)
    assert listed.returncode == 0, listed.stderr
    assert C not in listed.stdout
