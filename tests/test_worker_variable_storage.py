"""Full-UUID generation variable backend and managed-shell Python; no providers."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

import pytest

from ccx import constants
from ccx.controllers import live_vars
from ccx.errors import InvalidRequest, SessionUnavailable
from ccx.store.root import NativeActionDenied, StorageIntegrityError
from ccx.store.identity import EngineMismatch, InvalidTarget, UnknownWorker
from test_generation_store import A, B, C, G1, add_worker, short_root  # noqa: F401

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def owned(short_root, monkeypatch):
    # AF_UNIX names include pid+token; the test-only private runtime must be short.
    with TemporaryDirectory(prefix="l1a-owners-", dir="/tmp") as runtime:
        monkeypatch.setattr(constants, "CCX_ROOT", short_root)
        monkeypatch.setenv("CCX_ROOT", str(short_root))
        monkeypatch.setenv("XDG_RUNTIME_DIR", runtime)
        add_worker(short_root, A, "claude")
        add_worker(short_root, B, "codex")
        add_worker(short_root, C, "claude")
        yield short_root


def _state(root: Path, state_id: str) -> Path:
    return root / "identity" / "generations" / G1 / "variables" / "states" / f"{state_id}.json"


def _python_argv(engine: str, worker_id: str, *args: str) -> list[str]:
    return [
        sys.executable, "-B", "-m", "ccx", "_live-vars-exec", "--root", str(constants.CCX_ROOT), "--engine", engine,
        "--short", worker_id, "--invoked-as", "python3", "--", *args,
    ]


def _python_env(root: Path) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if not key.startswith("CCX_SELF_")}
    # --invoked-as python3 must resolve to THIS interpreter's version, not a
    # different python3 earlier on the developer's PATH.
    env.update(CCX_ROOT=str(root), PYTHONPATH=str(REPO),
               PATH=os.pathsep.join([os.path.dirname(sys.executable), env.get("PATH", "")]))
    return env


def _exec_python(root: Path, engine: str, worker_id: str, *args: str):
    return subprocess.run(
        _python_argv(engine, worker_id, *args), env=_python_env(root),
        cwd=root, input="", text=True, capture_output=True, timeout=30,
    )


def test_full_uuid_publish_merge_share_explicit_rebind_and_selected_reader(owned):
    # Deliberately same display prefix; engine and exact full UUID own each sidecar.
    first = live_vars.prepare_durable_attachment({"count": 1, "evidence": {"ok": True}})
    live_vars.publish_attachment(first, "claude", A)
    info = live_vars.variables_info("claude", A)
    state_id = info["state_id"]
    assert info["worker"] == f"claude:{A}"
    assert info["names"] == ["count", "evidence"] and "value" not in info
    assert info["available"] and state_id and len(state_id) == 32
    assert live_vars.variables_info("claude", A, "evidence")["value"] == {"ok": True}
    assert live_vars.read_sidecar("claude", A)["workerId"] == A
    assert _state(owned, state_id).is_file()

    live_vars.publish_attachment(live_vars.prepare_durable_attachment(
        {"count": 4, "added": [1, 2]},
    ), "claude", A)
    assert live_vars.variables_info("claude", A)["state_id"] == state_id
    shared = live_vars.prepare_shared_durable_attachment("claude", A)
    assert shared.state_id == state_id and shared.names == ["added", "count", "evidence"]
    live_vars.publish_attachment(shared, "codex", B)
    assert live_vars.variables_info("codex", B)["state_id"] == state_id
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"count": 9}), "codex", B,
    )
    assert live_vars.variables_info("claude", A, "count")["value"] == 9

    live_vars.publish_attachment(live_vars.prepare_durable_attachment({"other": 50}), "claude", C)
    second_id = live_vars.variables_info("claude", C)["state_id"]
    assert second_id != state_id
    live_vars.publish_attachment(
        live_vars.prepare_shared_durable_attachment("claude", C), "codex", B,
    )
    assert live_vars.variables_info("codex", B)["state_id"] == second_id
    assert live_vars.variables_info("codex", B, "other")["value"] == 50
    assert live_vars.variables_info("claude", A, "count")["value"] == 9
    assert not (owned / "variables").exists()


def test_hidden_full_uuid_python_uses_existing_durable_runner_and_atomic_lock(owned):
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"counter": 1, "evidence": []}), "claude", A,
    )
    state_id = live_vars.variables_info("claude", A)["state_id"]
    path = _state(owned, state_id)

    ok = _exec_python(
        owned, "claude", A, "-c",
        "counter += 2; evidence.append('written'); print('runner', counter)",
    )
    assert ok.returncode == 0, (ok.stdout, ok.stderr)
    assert ok.stdout == "runner 3\n"
    assert live_vars.variables_info("claude", A, "counter")["value"] == 3
    assert live_vars.variables_info("claude", A, "evidence")["value"] == ["written"]

    before = path.read_bytes()
    bad = _exec_python(owned, "claude", A, "-c", "counter=99; evidence=object()")
    assert bad.returncode != 0
    assert "CcxStateValueUnsupported" in bad.stderr
    assert path.read_bytes() == before

    # Whole runner execution, not just the JSON write, uses the shared stateId lock.
    argv = _python_argv(
        "claude", A, "-c", "import time; n=counter; time.sleep(.12); counter=n+1",
    )
    env = _python_env(owned)
    p1 = subprocess.Popen(argv, env=env, cwd=owned, stdin=subprocess.DEVNULL,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    p2 = subprocess.Popen(argv, env=env, cwd=owned, stdin=subprocess.DEVNULL,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    for proc in (p1, p2):
        stdout, stderr = proc.communicate(timeout=30)
        assert proc.returncode == 0, (stdout, stderr)
    assert live_vars.variables_info("claude", A, "counter")["value"] == 5
    assert json.loads(path.read_text())["names"] == ["counter", "evidence"]
    assert not (owned / "variables").exists()


def _run_isolated_live_owner_test(source: str, name: str) -> bool:
    """Run a test with process-global LiveOwner stdio outside xdist's gateway.

    LiveOwner executes real Python in another thread while replacing sys.*
    streams. Tests for that integration must not run those replacements inside
    the worker process that hosts execnet's command channel.
    """
    if os.environ.get("_CCX_TEST_LIVE_OWNER_CHILD") == "1":
        return False
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("PYTEST_XDIST_", "CCX_SELF_"))
           and key not in ("PYTEST_CURRENT_TEST", "CCX_ROOT")}
    env.update(PYTHONPATH=str(REPO), _CCX_TEST_LIVE_OWNER_CHILD="1")
    result = subprocess.run([
        sys.executable, "-B", "-m", "pytest", "-q", "-p", "no:xdist",
        "-o", "faulthandler_timeout=60", f"{source}::{name}",
    ], env=env, cwd=REPO, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, (result.stdout, result.stderr)
    return True


def test_exact_owner_mode_live_rebind_and_stale_token_are_fenced(owned):
    if _run_isolated_live_owner_test(
        __file__, "test_exact_owner_mode_live_rebind_and_stale_token_are_fenced",
    ):
        return
    with pytest.raises(InvalidTarget):
        live_vars.publish_attachment(live_vars.prepare_durable_attachment({"bad": 1}),
                                     "claude", A[:8])
    with pytest.raises(EngineMismatch):
        live_vars.publish_attachment(live_vars.prepare_durable_attachment({"bad": 1}),
                                     "codex", A)
    with pytest.raises((StorageIntegrityError, SessionUnavailable, InvalidRequest)):
        live_vars.prepare_shared_durable_attachment("claude", A)

    class Box:
        def __init__(self):
            self.count = 1

    first_data = {"box": Box(), "result": None}
    first_owner = live_vars.LiveOwner()
    try:
        live_vars.publish_attachment(live_vars.PreparedAttachment(
            "live", ["box", "result"], mapping=first_data, owner=first_owner,
        ), "claude", A)
        snapshot = live_vars.variables_info("claude", A)
        assert snapshot["mode"] == "live" and not snapshot["available"]
        assert snapshot["names"] == ["box", "result"]
        with pytest.raises(NativeActionDenied):
            live_vars.variables_info("claude", A, "box")
        with pytest.raises(InvalidRequest, match="no durable variables"):
            live_vars.prepare_shared_durable_attachment("claude", A)
        with pytest.raises(InvalidRequest, match="cannot mix modes"):
            live_vars.publish_attachment(
                live_vars.prepare_durable_attachment({"box": "wrong"}), "claude", A,
            )
        reply = _exec_python(owned, "claude", A, "-c",
                             "box.count += 2; result={'done': True}; print(box.count)")
        assert reply.returncode == 0, (reply.stdout, reply.stderr)
        assert reply.stdout == "3\n"
        assert first_data["box"].count == 3 and first_data["result"] == {"done": True}

        competing_owner = live_vars.LiveOwner()
        try:
            with pytest.raises(InvalidRequest, match="reachable live variable owner"):
                live_vars.publish_attachment(live_vars.PreparedAttachment(
                    "live", ["box"], mapping={"box": Box()}, owner=competing_owner,
                ), "claude", A)
        finally:
            competing_owner.close()
    finally:
        first_owner.close()

    # Clean controller close removes only that owner's sidecar.
    assert live_vars.read_sidecar("claude", A) is None
    unavailable = _exec_python(owned, "claude", A, "-c", "print('native')")
    assert unavailable.returncode == 0
    assert unavailable.stdout == "native\n"
    assert first_data["box"].count == 3

    second_data = {"box": Box()}
    second_owner = live_vars.LiveOwner()
    try:
        live_vars.publish_attachment(live_vars.PreparedAttachment(
            "live", ["box"], mapping=second_data, owner=second_owner,
        ), "claude", A)
        sidecar = live_vars.read_sidecar("claude", A)
        assert sidecar["owner"]["token"] == second_owner.token
        # An old controller's late worker-name update cannot overwrite new names.
        first_owner._publish_live_names("claude", A, ["stale"])
        assert live_vars.variables_info("claude", A)["names"] == ["box"]
        assert live_vars.read_sidecar("claude", A)["owner"]["token"] == second_owner.token
        updated = _exec_python(owned, "claude", A, "-c", "box.count += 5")
        assert updated.returncode == 0, (updated.stdout, updated.stderr)
        assert second_data["box"].count == 6 and first_data["box"].count == 3
        assert not (owned / "variables").exists()
    finally:
        second_owner.close()


def test_live_owner_close_reaps_acceptors_and_restores_streams(owned):
    """Rapid owner replacement must not leave capture proxies or accept threads."""
    streams = (sys.stdin, sys.stdout, sys.stderr)
    for _ in range(12):
        first_owner = live_vars.LiveOwner()
        competing_owner = live_vars.LiveOwner()
        try:
            assert first_owner.socket_path != competing_owner.socket_path
        finally:
            competing_owner.close()
            first_owner.close()
        assert (sys.stdin, sys.stdout, sys.stderr) == streams
        assert not first_owner._accept_thread.is_alive()
        assert not competing_owner._accept_thread.is_alive()
        assert not first_owner._executor_thread.is_alive()
        assert not competing_owner._executor_thread.is_alive()


# Stored variables and independent shared state identity.
import multiprocessing
import stat
import time
from ccx.store.root import CreationConflict, CreationIndeterminate, GenerationBusy, GenerationUnavailable, NativeActionDenied, NativeEffectIndeterminate, StaleGeneration, StorageIntegrityError, inspect_generation, require_ccx_active
from ccx.store.store import fingerprint_v2
from test_generation_store import G2, fixture_root, privdir, put, record, spec, switch_pointer
def test_full_UUID_vars_same_prefix_cross_engine_preserves_shared_state_id(tmp_path):
    import subprocess
    import sys
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    add_worker(root, B, "codex")
    ctx = require_ccx_active(root)
    gen = root / "identity" / "generations" / G1
    for name in ("variables", "variables/states", "variables/claude", "variables/codex"):
        privdir(gen / name)
    state_id = "f" * 32
    state_path = gen / "variables" / "states" / f"{state_id}.json"
    state_lock = gen / "variables" / "states" / f"{state_id}.lock"
    put(state_path, {
        "schemaVersion": 1, "stateId": state_id,
        "names": ["counter", "message"],
        "variables": {"message": {"hello": "world"}, "counter": 17},
    })
    state_lock.touch(mode=0o600)
    state_lock.chmod(0o600)
    for worker_id, engine in ((A, "claude"), (B, "codex")):
        put(gen / "variables" / engine / f"{worker_id}.json", {
            "schemaVersion": 2, "workerId": worker_id,
            "engine": engine, "mode": "durable", "stateId": state_id,
        })
    first, second = ctx.variables_info(A), ctx.variables_info(f"codex:{B}")
    assert first["state_id"] == state_id == second["state_id"]
    assert first["worker"] == f"claude:{A}"
    assert second["worker"] == f"codex:{B}"
    assert first["short"] == second["short"] == "aaaaaaaa"
    assert first["names"] == ["counter", "message"] == second["names"]
    assert "value" not in first and "value" not in second
    assert ctx.variables_info(A, "counter")["value"] == 17
    assert ctx.variables_info(B, "message")["value"] == {"hello": "world"}
    before = sorted((str(p.relative_to(root)), p.stat().st_mtime_ns)
                    for p in root.rglob("*") if p.is_file())
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    def cli(*argv):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", "vars", *argv],
            env=env, capture_output=True, text=True, timeout=20,
        )
    plain = cli(f"claude:{A}")
    assert plain.returncode == 0, plain.stderr
    assert plain.stdout.splitlines() == ["counter", "message"]
    result = cli(f"codex:{B}", "--json")
    assert result.returncode == 0, result.stderr
    info = json.loads(result.stdout)
    assert info["worker_id"] == B and info["state_id"] == state_id
    materialized = cli(f"claude:{A}", "message", "--json")
    assert materialized.returncode == 0, materialized.stderr
    assert json.loads(materialized.stdout)["value"] == {"hello": "world"}
    counter = cli(B, "counter")
    assert counter.returncode == 0 and json.loads(counter.stdout) == 17
    assert before == sorted((str(p.relative_to(root)), p.stat().st_mtime_ns)
                            for p in root.rglob("*") if p.is_file())
    assert not (root / "variables").exists()
    assert not (root / "sessions").exists()


def test_full_UUID_vars_unbound_live_controller_forensic_names_only(tmp_path):
    import subprocess
    import sys
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    ctx = require_ccx_active(root)
    gen = root / "identity" / "generations" / G1
    privdir(gen / "variables")
    privdir(gen / "variables" / "claude")
    put(gen / "variables" / "claude" / f"{A}.json", {
        "schemaVersion": 2, "workerId": A, "engine": "claude",
        "mode": "live", "names": ["python_object"],
        "owner": {"pid": 1234, "token": "stale", "socket": "/tmp/untrusted.sock"},
    })
    snap = ctx.variables_info(A)
    assert snap["mode"] == "live"
    assert snap["names"] == ["python_object"]
    assert snap["available"] is False
    assert snap["state_id"] is None
    with pytest.raises(NativeActionDenied, match="cannot be materialized"):
        ctx.variables_info(A, "python_object")
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    bad = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "vars", A, "python_object", "--json"],
        env=env, capture_output=True, text=True, timeout=20,
    )
    assert bad.returncode == 3
    assert "cannot be materialized" in bad.stderr
    assert not (root / "run").exists()


@pytest.mark.parametrize("target", [
    "aaaaaaaa", "claude:aaaaaaaa", "same name",
    "claude-session:" + A, "codex:" + A,
])
def test_full_uuid_vars_no_short_name_native_or_cross_engine_alias(tmp_path, target):
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    ctx = require_ccx_active(root)
    with pytest.raises((InvalidTarget, EngineMismatch, UnknownWorker)):
        ctx.variables_info(target)
    assert not (root / "identity" / "generations" / G1 / "variables").exists()


def test_full_uuid_vars_missing_or_corrupt_state_fails_without_writes(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    ctx = require_ccx_active(root)
    gen = root / "identity" / "generations" / G1
    for name in ("variables", "variables/claude", "variables/states"):
        privdir(gen / name)
    state_id = "d" * 32
    sidecar = gen / "variables" / "claude" / f"{A}.json"
    put(sidecar, {"schemaVersion": 2, "engine": "claude", "workerId": A,
                  "mode": "durable", "stateId": state_id})
    before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))
    with pytest.raises((StorageIntegrityError, GenerationUnavailable)):
        ctx.variables_info(A)
    assert sorted(str(p.relative_to(root)) for p in root.rglob("*")) == before
    state = gen / "variables" / "states" / f"{state_id}.json"
    put(state, {"schemaVersion": 1, "stateId": state_id, "names": ["x"],
                "variables": {"x": 2}})
    before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))
    assert ctx.variables_info(A, "x")["value"] == 2
    assert sorted(str(p.relative_to(root)) for p in root.rglob("*")) == before
    lock = state.with_suffix(".lock")
    lock.touch(mode=0o600)
    lock.chmod(0o600)
    assert ctx.variables_info(A, "x")["value"] == 2
    state.unlink()
    state.symlink_to(gen / "owners" / f"{A}.json")
    with pytest.raises(StorageIntegrityError):
        ctx.variables_info(A)
    state.unlink()
    put(state, {"schemaVersion": 1, "stateId": "0" * 32, "names": ["x"],
                "variables": {"x": 2}})
    with pytest.raises(StorageIntegrityError):
        ctx.variables_info(A)


def test_full_UUID_vars_sidecar_foreign_worker_id_never_read(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    add_worker(root, B, "claude")
    ctx = require_ccx_active(root)
    gen = root / "identity" / "generations" / G1
    privdir(gen / "variables")
    privdir(gen / "variables" / "claude")
    put(gen / "variables" / "claude" / f"{A}.json", {
        "schemaVersion": 2, "engine": "claude", "workerId": B,
        "mode": "live", "names": ["stolen"],
    })
    with pytest.raises(StorageIntegrityError, match="owner provenance"):
        ctx.variables_info(A)



def _hold_variable_state_lock(args):
    root, state_id, acquired, release = args
    import fcntl
    import os
    from pathlib import Path
    path = (Path(root) / "identity" / "generations" / G1
            / "variables" / "states" / f"{state_id}.lock")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        acquired.set()
        release.wait(10)
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def test_full_uuid_vars_shared_state_lock_blocks_fresh_read_without_spawning(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    ctx = require_ccx_active(root)
    gen = root / "identity" / "generations" / G1
    for name in ("variables", "variables/claude", "variables/states"):
        privdir(gen / name)
    state_id = "a" * 32
    put(gen / "variables" / "claude" / f"{A}.json", {
        "schemaVersion": 2, "workerId": A, "engine": "claude",
        "mode": "durable", "stateId": state_id,
    })
    put(gen / "variables" / "states" / f"{state_id}.json", {
        "schemaVersion": 1, "stateId": state_id,
        "names": ["n"], "variables": {"n": 12},
    })
    lock = gen / "variables" / "states" / f"{state_id}.lock"
    lock.touch(mode=0o600)
    lock.chmod(0o600)
    mp = multiprocessing.get_context("fork")
    acquired, release = mp.Event(), mp.Event()
    proc = mp.Process(
        target=_hold_variable_state_lock,
        args=((str(root), state_id, acquired, release),),
    )
    proc.start()
    try:
        assert acquired.wait(5)
        assert ctx.variables_info(A, "n")["value"] == 12
        # The activation lock is released even though a different state
        # lock is contested: no model/provider turn wait can be inferred.
        import fcntl
        fd = os.open(root / "identity" / "activation.lock", os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
    finally:
        release.set()
        proc.join(5)
        if proc.is_alive():
            proc.terminate()
            proc.join(5)
    assert proc.exitcode == 0
    assert ctx.variables_info(A, "n")["value"] == 12


def test_full_uuid_vars_intermediate_symlinks_do_not_escape_active_generation(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    ctx = require_ccx_active(root)
    outside = tmp_path / "fake-provider-state"
    put(outside / f"{A}.json", {
        "schemaVersion": 2, "engine": "claude", "workerId": A,
        "mode": "live", "names": ["stolen"],
    })
    gen = root / "identity" / "generations" / G1
    (gen / "variables").symlink_to(outside, target_is_directory=True)
    with pytest.raises((StorageIntegrityError, OSError)):
        ctx.variables_info(A)
    assert (outside / f"{A}.json").is_file()


def test_worker_bridge_malformed_durable_sidecar_returns_69(owned):
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"counter": 1}), "claude", A,
    )
    sidecar = live_vars.sidecar_path("claude", A)
    value = json.loads(sidecar.read_text())
    value["stateId"] = "not-a-state-id"
    sidecar.write_text(json.dumps(value))
    result = _exec_python(owned, "claude", A, "-c", "counter += 1")
    assert result.returncode == 69
    assert "CcxStateUnavailable" in result.stderr
    assert "Traceback" not in result.stderr
