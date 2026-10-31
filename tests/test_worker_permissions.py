"""Public Claude permission configuration on a fake full-UUID owner; no provider I/O."""
from __future__ import annotations

import pytest

from ccx.cli import main as cli
from ccx import client as claude_client
from ccx import constants
from ccx.claude.host import session as host
from ccx.ops import terminate
from ccx.errors import InvalidRequest
from ccx.ops.creation_steps import prepare_worker
from ccx.store.store import GenerationStore
from ccx.store.root import require_ccx_active
from test_generation_store import A, short_root as short_root


def _cli(capsys, *args):
    try:
        cli.main(["set", f"claude:{A}", *args, "--json"])
        rc = 0
    except SystemExit as exc:
        rc = exc.code
    captured = capsys.readouterr()
    return rc, captured.out, captured.err


@pytest.fixture
def original(short_root, tmp_path, monkeypatch):
    """First owned Claude record: no original host/native process ever launched."""
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setenv("CCX_ROOT", str(short_root))
    store = require_ccx_active(short_root)
    prepare_worker(
        store, engine="claude", cwd=str(tmp_path), worker_id=A,
        model="sonnet", effort="medium", permissions="restricted",
        allowed_tools="Read", disallowed_tools="Bash",
        permission_mode="acceptEdits",
    )
    monkeypatch.setattr(
        claude_client, "_rpc",
        lambda target, msg, timeout: {"ok": False, "error": "unreachable: no fake host"},
    )
    return store


def _config_calls(monkeypatch):
    """Observe the EXISTING semantic record CAS without replacing its implementation."""
    calls = []
    original_mutate = GenerationStore.mutate_record

    def observed(self, target, *, patches=None, **kwargs):
        if patches and any(field.startswith("createArgs.") for field in patches):
            calls.append((target, dict(patches), dict(kwargs.get("expect") or {})))
        return original_mutate(self, target, patches=patches, **kwargs)

    monkeypatch.setattr(GenerationStore, "mutate_record", observed)
    return calls


def _warm_original(store, monkeypatch):
    """Fake authenticated original Claude host and child, never a process spawn."""
    native = store.record(A)["native"]["sessionId"]
    proof = {
        "sessionId": native, "hostPid": 101, "hostStart": "host-original",
        "workerPid": 102, "workerStart": "child-original",
    }
    store.mutate_record(A, patches={
        "lifecycle.state": "idle", "lifecycle.hostPid": 101,
        "lifecycle.hostStart": "host-original", "lifecycle.workerPid": 102,
        "lifecycle.workerStart": "child-original",
        "lifecycle.nativeStartProof": proof,
    })
    from ccx.store import liveness
    from ccx.system import oslayer
    real_process_info = oslayer.process_info
    def fake_original_info(pid):
        if pid == 101:
            return oslayer.ProcessInfo(pid, "alive", None, "S", "host-original")
        if pid == 102:
            return oslayer.ProcessInfo(pid, "alive", None, "S", "child-original")
        return real_process_info(pid)
    # A warm host has positively observed PID/start pairs, not just an
    # affirmative bare-PID kill(0). D1 requires both fields to agree.
    monkeypatch.setattr(oslayer, "process_info", fake_original_info)
    monkeypatch.setattr(liveness, "owned_pid_alive", lambda pid, start: (
        (pid, start) in {(101, "host-original"), (102, "child-original")}
    ))
    monkeypatch.setattr(liveness, "pid_alive", lambda pid: pid in (101, 102))
    status = {
        "ok": True, "state": "idle", "activeTurnRef": None,
        "pendingDeliveries": [], "queuedInputs": 0,
        "waitingFor": None, "compacting": False, "compactOperation": None,
        "steerJournalError": None, "compactJournalError": None,
    }
    seen = []

    def rpc(target, msg, timeout):
        seen.append((target, msg["op"]))
        if msg["op"] == "startup-status":
            return {"ok": True, "workerId": A, "engine": "claude",
                    "generation": store.generation, "nativeStartProof": proof}
        if msg["op"] == "status":
            return dict(status)
        pytest.fail(f"unexpected native fake RPC: {msg}")

    monkeypatch.setattr(claude_client, "_rpc", rpc)
    return status, proof, seen


def test_stopped_restricted_mixed_cli_single_cas_then_rename_and_refusal(
    original, monkeypatch, capsys,
):
    """Stopped posture/mixed config persists once; an uncertain probe cannot rename."""
    store = original
    saved = store.record(A)
    calls = _config_calls(monkeypatch)
    result, out, err = _cli(
        capsys, "--permissions", "restricted", "--model", "default",
        "--effort", "default", "--name", "restricted worker",
    )
    assert result == 0, err
    import json
    data = json.loads(out)
    assert data == {
        "worker": f"claude:{A}", "worker_id": A, "engine": "claude",
        "short": A[:8], "action": "set", "name": "restricted worker",
        "model": "default", "effort": None, "permissions": "restricted",
    }
    assert len(calls) == 1
    assert calls[0][0] == f"claude:{A}"
    assert calls[0][1] == {
        "createArgs.model": "default", "createArgs.effort": None,
        "createArgs.skip_permissions": False, "createArgs.permission_mode": None,
    }
    updated = store.record(A)
    args = updated["createArgs"]
    assert args["disallowed_tools"] == "Bash" and args["allowed_tools"] == "Read"
    assert host.permission_args(args) == ["--permission-mode", "default"]
    assert updated["name"] == "restricted worker"
    assert updated["native"] == saved["native"]
    assert updated["createSha256"] == saved["createSha256"]

    # Permissions-only is a configuration change, without a name mutation.
    before_name = updated["name"]
    result, out, err = _cli(capsys, "--permissions", "restricted")
    assert result == 0, err
    assert json.loads(out)["action"] == "set"
    assert json.loads(out)["permissions"] == "restricted"
    assert store.record(A)["name"] == before_name
    assert len(calls) == 2

    # A config refusal takes precedence over optional rename.
    monkeypatch.setattr(claude_client, "_rpc", lambda *_a, **_kw: {
        "ok": False, "error": "timeout",
    })
    before = store.record(A)
    result, out, err = _cli(capsys, "--permissions", "full", "--name", "must not save")
    assert result == 3 and not out and "Cannot verify" in err
    assert store.record(A) == before and len(calls) == 2
    # Invalid private semantic choice also fails before ANY host/status probe.
    def unexpected_rpc(*_a, **_kw):
        pytest.fail("invalid permission input reached native probe")
    monkeypatch.setattr(claude_client, "_rpc", unexpected_rpc)
    with pytest.raises(InvalidRequest, match="full or restricted"):
        claude_client.CcxClient()._set_spawn_config(A, {"permissions": "auto"}, restart=False)
    assert store.record(A) == before


def test_warm_idle_full_restart_one_cas_one_stop_and_saved_stop_failure(
    original, monkeypatch, capsys,
):
    """Original authenticated idle + one config CAS + at most one confirmed stop."""
    store = original
    _status, _proof, seen = _warm_original(store, monkeypatch)
    before = store.record(A)
    calls = _config_calls(monkeypatch)
    stopped = []

    def confirm(ctx, handle, *, timeout):
        stopped.append((ctx.generation, handle, timeout))
        return {"killed": True}

    monkeypatch.setattr(terminate, "confirmed_kill_uuid", confirm)
    result, out, err = _cli(
        capsys, "--permissions", "full", "--model", "opus",
        "--effort", "high", "--name", "full worker", "--restart",
    )
    assert result == 0, err
    import json
    row = json.loads(out)
    assert row["action"] == "restart" and row["permissions"] == "full"
    assert row["model"] == "opus" and row["effort"] == "high"
    assert row["name"] == "full worker"
    assert seen == [(f"claude:{A}", "startup-status"), (f"claude:{A}", "status")]
    assert len(calls) == 1 and len(stopped) == 1
    assert stopped[0][0:2] == (store.generation, f"claude:{A}")
    assert set(calls[0][1]) == {
        "createArgs.model", "createArgs.effort", "createArgs.skip_permissions",
        "createArgs.permission_mode", "createArgs.disallowed_tools",
    }
    assert all(k.startswith("lifecycle.") for k in calls[0][2])
    saved = store.record(A)
    args = saved["createArgs"]
    assert args["skip_permissions"] is True
    assert args["permission_mode"] is None and args["disallowed_tools"] is None
    assert args["allowed_tools"] == "Read"
    assert host.permission_args(args) == ["--dangerously-skip-permissions"]
    assert saved["native"] == before["native"]
    assert saved["createSha256"] == before["createSha256"]

    def unconfirmed(ctx, handle, *, timeout):
        stopped.append((ctx.generation, handle, timeout))
        return {"killed": False}

    monkeypatch.setattr(terminate, "confirmed_kill_uuid", unconfirmed)
    result, out, err = _cli(
        capsys, "--permissions", "restricted", "--name", "not renamed", "--restart",
    )
    assert result == 3 and not out and "saved" in err and "may still be running" in err
    assert len(calls) == 2 and len(stopped) == 2  # ONE additional attempt, not a retry
    failed_stop = store.record(A)
    assert failed_stop["name"] == "full worker"  # config-before-name
    assert failed_stop["createArgs"]["skip_permissions"] is False
    assert failed_stop["createArgs"]["permission_mode"] is None
    assert failed_stop["createArgs"]["disallowed_tools"] is None
    assert host.permission_args(failed_stop["createArgs"]) == ["--permission-mode", "default"]


@pytest.mark.parametrize("refusal", [
    "busy", "waiting", "queued", "compaction", "status-unknown",
    "startup-timeout", "wrong-native-proof", "live-but-unreachable",
])
def test_unsettled_or_unproven_original_refuses_config_and_name_without_stop(
    original, monkeypatch, capsys, refusal,
):
    """Unsafe observations refuse all config and rename with no native stop."""
    store = original
    status, proof, _seen = _warm_original(store, monkeypatch)
    if refusal == "busy":
        status["state"] = "busy"
    elif refusal == "waiting":
        status["waitingFor"] = {"id": "fake-permission-gate"}
    elif refusal == "queued":
        status["pendingDeliveries"] = [{"deliveryId": "queued"}]
    elif refusal == "compaction":
        status["compacting"] = True
    elif refusal == "status-unknown":
        monkeypatch.setattr(claude_client, "_rpc", lambda _target, _msg, timeout: (
            {"ok": True, "workerId": A, "engine": "claude", "generation": store.generation,
             "nativeStartProof": proof} if _msg["op"] == "startup-status" else
            {"ok": False, "error": "timeout"}
        ))
    elif refusal == "startup-timeout":
        monkeypatch.setattr(claude_client, "_rpc", lambda *_a, **_kw: {
            "ok": False, "error": "timeout",
        })
    elif refusal == "wrong-native-proof":
        monkeypatch.setattr(claude_client, "_rpc", lambda *_a, **_kw: {
            "ok": True, "workerId": A, "engine": "claude", "generation": store.generation,
            "nativeStartProof": {**proof, "sessionId": "wrong-native-session"},
        })
    elif refusal == "live-but-unreachable":
        monkeypatch.setattr(claude_client, "_rpc", lambda *_a, **_kw: {
            "ok": False, "error": "unreachable: socket refused",
        })
    calls = _config_calls(monkeypatch)
    monkeypatch.setattr(terminate, "confirmed_kill_uuid", lambda *_a, **_kw: (
        pytest.fail("native stop after refused permission config")
    ))
    before = store.record(A)
    result, out, err = _cli(
        capsys, "--permissions", "full", "--model", "opus",
        "--name", "must not rename", "--restart",
    )
    expected_exit = 5 if refusal in {"busy", "waiting", "queued", "compaction"} else 3
    assert result == expected_exit and not out and "ccx set:" in err
    assert store.record(A) == before and calls == []
