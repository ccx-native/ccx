"""N4 Claude permission parent notifications on one ORIGINAL native gate and R."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.settle as notify_settle
import ccx.cli.commands.turns as commands_turns
import ccx.ops.input as ops_input
import ccx.ops.wait as ops_wait

from argparse import Namespace
import json
import time
import uuid

import pytest

from ccx import client as client_module
from ccx.cli.commands import _common as commands
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.ops.creation_steps import activate_prepared_worker, prepare_worker
from ccx.store.store import _digest
from test_generation_store import A, C, short_root as short_root
from test_notify_claude import owners as owners
from test_worker_telemetry import (
    original_codex as original_codex, original_sources as original_sources,
)
from test_claude_native_cli import generated_hook_host as generated_hook_host
from test_notify_ext import FakeControl
from test_notify_standalone import _native_caller
from test_notify_receiptless import (
    _claude_sources, _claude_hook_events, _claude_line,
)


def _args(*, target=A, gate=None, decision="approve", reason=None):
    return Namespace(
        target=target, request_id=gate, reason=reason,
        json=True, notify_parent=True, settle=False,
        wait=False, timeout=6,
    )


def _managed_parent(store, monkeypatch, cwd):
    worker_id = C
    prepared = prepare_worker(store, engine="codex", cwd=str(cwd), worker_id=worker_id)
    thread = "original-managed-parent-thread"
    def original_ack(_budget):
        # The original native-start submission ID is minted AFTER prepare,
        # not when the fake child record is first reserved.
        native = store.native_activation(worker_id)
        return {
            "accepted": True, "workerId": worker_id, "engine": "codex",
            "source": "ccx-worker:" + worker_id,
            "submissionId": native["submissionId"],
            "nativeEvidenceSha256": "a" * 64,
            "nativeBinding": {"threadId": thread},
        }
    activate_prepared_worker(store, prepared, bounded_native_submit=original_ack)
    for key in ("CCX_SELF_SHORT", "CLAUDE_CODE_SESSION_ID"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CCX_SELF_ENGINE", "codex")
    monkeypatch.setenv("CCX_SELF_WORKER_ID", worker_id)
    monkeypatch.setenv("CODEX_THREAD_ID", thread)
    return {"kind": "ccx-codex", "workerId": worker_id,
            "threadId": thread, "generation": store.generation}


@pytest.fixture
def permission_world(owners, tmp_path, monkeypatch):
    _root, store = owners
    transcript = _claude_sources(store, monkeypatch, tmp_path)
    turn = "ccx-turn:claude:v1:original-permission-r1"
    gate = {"id": "n4-clause-original-permission-gate", "tool": "Bash",
            "input": {"command": "echo original"}}
    _claude_hook_events(store, {"type": "turn_start", "turn_ref": turn,
                                "turn": 1, "report_start_offset": 0})
    store.mutate_record("claude:" + A, patches={
        "waitingFor": gate, "lifecycle.state": "waiting",
        "lifecycle.activeTurnRef": turn,
    })
    _native_caller(monkeypatch)
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_: True)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    protocol_ops._CLIENT.value = None
    calls = []
    state = {"turn": turn, "gate": gate, "native": "accepted"}
    def original_host(wid, msg, timeout=10):
        assert wid == A
        if msg["op"] == "status":
            return {"ok": True, "state": "waiting",
                    "waitingFor": state["gate"], "activeTurnRef": state["turn"]}
        assert msg["op"] == "permission-decision"
        assert msg["request_id"] == gate["id"]
        token = msg["response_id"]
        intent = store.read_receipt("claude:" + A, "answer", token)
        assert intent["state"] == "submitted-unknown"
        event_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL, "ccx-notify:" +
            f"full-uuid-claude-input:{store.generation}:{A}:{gate['id']}",
        ))
        row = parent_notify.read_registration(event_id)
        assert row is not None and row["state"] == "armed"
        assert row["operation"]["selectedTurnRef"] == turn
        assert row["operation"]["nativeSourceId"] == store.record(A)["native"]["sessionId"]
        assert row["operation"]["intentSha256"] == intent["intentSha256"]
        assert json.loads(parent_notify.registration_path(event_id).read_text()) == row
        # One pure layout assertion against the accepted original native SHA.
        legacy_sha = _digest({
            "workerId": A, "engine": "claude", "generation": store.generation,
            "nativeId": row["operation"]["nativeSourceId"],
            "nativeSource": "claude-session:" + row["operation"]["nativeSourceId"],
            "kind": "decide", "requestId": gate["id"],
            "responseId": token, "payloadSha256": _digest({
                "decision": "approve" if msg["decision"] == "allow" else "reject",
                "reason": msg.get("reason"),
            }), "action": "answer",
        })
        assert intent["intentSha256"] == legacy_sha
        calls.append((dict(msg), event_id))
        if state["native"] == "ambiguous":
            raise TimeoutError("original native permission reply lost after receipt submitted")
        if state["native"] == "definitive":
            return {"ok": False, "state": "waiting",
                    "request_id": gate["id"], "response_id": token,
                    "error": "no pending permission"}
        return {"ok": True, "request_id": gate["id"], "response_id": token,
                "decision": msg["decision"]}
    monkeypatch.setattr(client_module, "_rpc", original_host)
    return store, transcript, turn, gate, state, calls


@pytest.mark.parametrize("parent_kind", ["interactive", "managed"])
def test_claude_permission_original_cli_binds_gate_and_fsync_before_native(
    permission_world, tmp_path, monkeypatch, capsys, parent_kind,
):
    store, _transcript, turn, gate, _state, calls = permission_world
    if parent_kind == "managed":
        parent = _managed_parent(store, monkeypatch, tmp_path)
    else:
        parent = _native_caller(monkeypatch)
    # No original permission: wrong explicit request is an exact pre-effect
    # refusal, no parent event or input intent.
    with pytest.raises(SystemExit) as wrong:
        commands_turns.cmd_approve(_args(gate="not-current"))
    assert wrong.value.code in {1, 4, 5}
    capsys.readouterr()
    assert not calls and not parent_notify.iter_registrations()
    # Both native decisions are accepted input: deny is not a rejected effect.
    command = commands_turns.cmd_reject if parent_kind == "managed" else commands_turns.cmd_approve
    command(_args(gate=None))
    public = json.loads(capsys.readouterr().out)
    assert public["status"] == "accepted" and public["original_gate_answered"]
    assert public["worker_id"] == A and public["request_id"] == gate["id"]
    assert public["response_id"] == str(uuid.uuid5(uuid.NAMESPACE_URL,
        f"ccx-input-decision:{store.generation}:{A}:{gate['id']}"))
    assert public["decision"] == ("deny" if parent_kind == "managed" else "allow")
    assert public["notify_parent"]["parent"] == (
        "codex:" + C if parent_kind == "managed" else parent)
    assert public["notify_parent"]["source"] == "input"
    assert public["notify_parent"]["selected_turn_ref"] == turn
    assert public["notify_parent"]["settle"] is False
    assert len(calls) == 1
    row = parent_notify.read_registration(public["notify_parent"]["event_id"])
    assert row["operation"]["reason"] is None
    assert row["operation"]["selectedTurnRef"] == turn
    assert row["worker"]["workerId"] == A
    assert row["worker"]["engine"] == "claude"


def test_claude_permission_reason_replay_unknown_cancel_and_no_late_arm(
    permission_world, monkeypatch, capsys,
):
    store, _transcript, _turn, gate, state, calls = permission_world
    state["native"] = "ambiguous"
    with pytest.raises(SystemExit) as lost:
        commands_turns.cmd_reject(_args(gate=gate["id"], reason="no thanks"))
    assert lost.value.code == 6
    first = json.loads(capsys.readouterr().out)
    token, event_id = first["response_id"], first["notify_parent"]["event_id"]
    assert first["status"] in {"submitted", "unknown"}
    assert store.read_receipt(A, "answer", token)["state"] == "submitted-unknown"
    assert parent_notify.read_registration(event_id)["state"] == "armed"
    assert len(calls) == 1
    state["native"] = "accepted"
    # Prior original receipt short-circuits before ANY current status/fence.
    state["gate"] = {"id": "some-new-gate", "tool": "Bash"}
    with pytest.raises(SystemExit) as again:
        commands_turns.cmd_reject(_args(gate=gate["id"], reason="no thanks"))
    assert again.value.code == 6
    same = json.loads(capsys.readouterr().out)
    assert same["notify_parent"]["event_id"] == event_id and len(calls) == 1
    with pytest.raises(SystemExit) as conflict:
        commands_turns.cmd_approve(_args(gate=gate["id"], reason="no thanks"))
    assert conflict.value.code == 5
    capsys.readouterr()
    with pytest.raises(SystemExit):
        commands_turns.cmd_reject(_args(gate=gate["id"], reason="changed reason"))
    capsys.readouterr()
    assert len(calls) == 1
    assert [r["eventId"] for r in parent_notify.iter_registrations()] == [event_id]

    # The same original gate as a NEW response after prior effect is not a
    # second acceptance; a definite no-effect must cancel ONLY its event.
    state["gate"] = gate
    state["native"] = "definitive"
    # Keep this as a distinct original gate/response in another fixture
    # test rather than modifying the already-fenced native intent.
    assert parent_notify.read_registration(event_id)["state"] == "armed"


def test_claude_permission_original_gate_waits_new_same_r_then_exact_stop(
    permission_world, monkeypatch,
):
    store, transcript, turn, gate, state, calls = permission_world
    from ccx.ops import common as ops
    result = ops_input.decide_input("claude:" + A, request_id=gate["id"],
                              response_id="original-decision-specific",
                              decision="approve", notify_parent=True)
    assert result["status"] == "accepted" and len(calls) == 1
    reg = parent_notify.read_registration(result["notification"]["event_id"])
    assert notify_settle.settlement_for_registration(reg) is None
    successor = {"id": "genuinely-new-permission-gate", "tool": "Bash",
                 "input": {"command": "echo new"}}
    state["gate"] = successor
    store.mutate_record("claude:" + A, patches={"waitingFor": successor})
    def native_wait(*_a, **_kw):
        return {"worker": "claude:" + A, "state": "waiting",
                "active_turn_ref": turn, "waiting_for": successor}
    monkeypatch.setattr(ops_wait, "wait_worker", native_wait)
    waiting = notify_settle.settlement_for_registration(reg)
    assert waiting["status"] == "waiting" and waiting["report"] is None
    assert waiting["waitingFor"]["id"] == successor["id"]
    # The selected original turn completes BEFORE later R2.
    _claude_line(transcript, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "EXACT original R1 permission result"}]}})
    offset = transcript.stat().st_size
    _claude_hook_events(store, {"type": "turn_complete", "turn_ref": turn,
                                "turn": 1},
                        {"type": "turn_start", "turn_ref": "ccx-turn:claude:v1:later-R2",
                         "turn": 2, "report_start_offset": offset})
    _claude_line(transcript, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "WRONG latest R2"}]}})
    done = notify_settle.settlement_for_registration(reg)
    assert done["status"] == "completed" and done["turnRef"] == turn
    assert done["report"] == "EXACT original R1 permission result"
    assert "WRONG" not in str(done)


def test_claude_permission_definitive_noeffect_cancels_same_event_only(
    permission_world, monkeypatch, capsys,
):
    store, _transcript, _turn, gate, state, calls = permission_world
    from ccx.errors import DeliveryRejected
    from ccx.client import CcxClient
    protocol_ops._CLIENT.value = CcxClient()
    native = []
    def original_refusal(target, **kw):
        native.append((target, kw["request_id"], kw["response_id"]))
        expected = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"ccx-notify:full-uuid-claude-input:{store.generation}:{A}:{gate['id']}",
        ))
        reg = parent_notify.read_registration(expected)
        assert reg is not None and reg["state"] == "armed"
        assert store.read_receipt(A, "answer", kw["response_id"])["state"] == "submitted-unknown"
        raise DeliveryRejected("original native gate positively refused",
                               reason="request id mismatch")
    # Patch the actual client, not the lazy proxy: restoring a proxy attr
    # would leave a stale bound approve method for the next physical-host test.
    monkeypatch.setattr(protocol_ops._CLIENT.value, "approve", original_refusal)
    with pytest.raises(SystemExit) as failed:
        commands_turns.cmd_approve(_args(gate=gate["id"]))
    assert failed.value.code == 5
    first = json.loads(capsys.readouterr().out)
    assert first["status"] == "rejected"
    assert first["notify_parent"]["state"] == "cancelled"
    assert len(native) == 1
    event = first["notify_parent"]["event_id"]
    assert parent_notify.read_registration(event)["state"] == "cancelled"
    assert store.read_receipt(A, "answer", first["response_id"])["state"] == "rejected"
    # Neither reappearance of a gate nor native recovery can rearm a cancelled
    # original decision or resend its original effect.
    state["gate"] = {"id": "different-current-input", "tool": "Bash"}
    with pytest.raises(SystemExit) as repeat:
        commands_turns.cmd_approve(_args(gate=gate["id"]))
    assert repeat.value.code == 5
    same = json.loads(capsys.readouterr().out)
    assert same["notify_parent"]["event_id"] == event
    assert same["notify_parent"]["state"] == "cancelled"
    assert len(native) == 1


def test_shared_digest_retains_accepted_original_codex_sha(
    original_codex, monkeypatch,
):
    """C2: the extracted pure helper MUST equal a real original Codex receipt."""
    from types import SimpleNamespace
    from ccx.codex import runtime as codex_runtime
    store, _cwd, wid, native_id = original_codex
    turn = "n4-digest-original-codex-R"
    gate = "n4-digest-original-codex-gate"
    response = "n4-digest-original-response"
    store.mutate_record(wid, patches={"pendingApprovals": [{
        "gateId": gate, "turnId": turn, "requestId": 0,
        "runtimeId": "original-fake-daemon", "kind": "commandExecution",
    }]})
    monkeypatch.setattr(codex_runtime, "daemon_rpc",
                        lambda *_a, **_kw: {"ok": True, "transport": "served", "loaded": True})
    protocol_ops._CODEX_CLIENT.value = SimpleNamespace(
        _decision=lambda *_a, **_kw: None,
    )
    result = ops_input.decide_input("codex:" + wid, request_id=gate,
                                       response_id=response, decision="approve")
    assert result["status"] == "accepted"
    receipt = store.read_receipt("codex:" + wid, "answer", response)
    original_sha = _digest({
        "workerId": wid, "engine": "codex", "generation": store.generation,
        "nativeId": native_id, "nativeSource": f"ccx-worker:{wid}",
        "kind": "decide", "requestId": gate, "responseId": response,
        "payloadSha256": _digest({"decision": "approve", "reason": None}),
        "action": "answer",
    })
    assert receipt["intentSha256"] == original_sha
    assert receipt["intentSha256"] == ops_input._input_intent_digest(
        worker_id=wid, engine="codex", generation=store.generation,
        native_id=native_id, native_source=f"ccx-worker:{wid}",
        kind="decide", request_id=gate, response_id=response,
        payload={"decision": "approve", "reason": None},
    )


@pytest.mark.parametrize("generated_hook_host", ["restricted"], indirect=True)
def test_generated_restricted_claude_permission_original_r_and_one_parent_wake(
    generated_hook_host, monkeypatch, capsys, request,
):
    """Native fake Claude/PTY, generated PermissionRequest hook and original Stop."""
    from pathlib import Path
    from ccx.codex import control as codex_control
    from ccx.store import paths
    from ccx.notify import daemon as notifyd
    from test_claude_native_cli import _generated_hook_results
    store, call, env, hook_log = generated_hook_host
    from test_notify_claude_question import _isolate_disposable_notifyd_for_fake_native
    _isolate_disposable_notifyd_for_fake_native(store, request)
    before = call("send", A, "request generated permission", "--no-wait",
                  "--delivery-id", "n4-claude-real-permission-1", "--json")
    assert before.returncode == 0, (before.stdout, before.stderr)
    until = time.monotonic() + 14
    waiting = None
    while time.monotonic() < until:
        waiting = store.record(A).get("waitingFor")
        if isinstance(waiting, dict) and waiting.get("id"):
            break
        time.sleep(.06)
    assert waiting and waiting.get("tool") == "Bash", store.record(A)
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", env["CLAUDE_CONFIG_DIR"])
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    protocol_ops._CLIENT.value = None
    parent = _native_caller(monkeypatch)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    args = _args(gate=None, reason="")
    args.timeout = 15  # Physical fake-host startup can consume the 6s synthetic deadline.
    try:
        commands_turns.cmd_approve(args)
    except SystemExit as exc:
        receipt_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"ccx-input-decision:{store.generation}:{A}:{waiting['id']}",
        ))
        rec = store.record(A)
        host_log = store.worker_path(A, "record.json").parent / "host.log"
        pytest.fail(
            f"native permission CLI returned {exc.code}; "
            f"original_input_receipt={store.read_receipt(A, 'answer', receipt_id)!r}; "
            f"current_waiting={rec.get('waitingFor')!r}; "
            f"hostlog_tail={(host_log.read_text(errors='replace')[-2000:] if host_log.exists() else 'absent')!r}; "
            f"hook_tail={_generated_hook_results(hook_log)!r}; "
            f"stdout={capsys.readouterr().out!r}"
        )
    first = json.loads(capsys.readouterr().out)
    assert first["status"] == "accepted" and first["request_id"] == waiting["id"]
    assert first["notify_parent"]["parent"] == parent
    assert first["response_id"] == str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"ccx-input-decision:{store.generation}:{A}:{waiting['id']}",
    ))
    row = parent_notify.read_registration(first["notify_parent"]["event_id"])
    r1 = row["operation"]["selectedTurnRef"]
    assert r1.startswith("ccx-turn:claude:v1:")
    assert row["operation"]["reason"] is None
    until = time.monotonic() + 14
    completed = None
    while time.monotonic() < until:
        completed = notify_settle.settlement_for_registration(row)
        # The exact original turn's completed event can arrive before the
        # transcript report is readable. Observe BOTH, as in the generated
        # native question test, without rerunning any accepted native effect.
        if (completed and completed.get("status") == "completed"
                and isinstance(completed.get("report"), str)):
            break
        time.sleep(.06)
    assert (completed is not None and completed.get("status") == "completed"
            and isinstance(completed.get("report"), str)), completed
    assert completed["turnRef"] == r1 and "Fake Claude completed" in completed["report"]
    result = [r for r in _generated_hook_results(hook_log)
              if r["event"] == "PermissionRequest"]
    assert len(result) == 1 and result[0]["rc"] == 0
    assert json.loads(result[0]["stdout"])["hookSpecificOutput"]["decision"]["behavior"] == "allow"
    native = FakeControl()
    monkeypatch.setattr(codex_control, "CodexControlClient", lambda: native)
    from test_notify_claude_question import _drive_fake_native_notify_once
    _drive_fake_native_notify_once(row, monkeypatch)
    delivered = parent_notify.read_registration(row["eventId"])
    assert delivered["state"] == "delivered"
    assert delivered["payload"]["turnRef"] == r1
    assert delivered["payload"]["report"] == completed["report"]
    assert len(native.methods("turn/start")) == 1
    notifyd.process_registration(delivered)
    assert len(native.methods("turn/start")) == 1
