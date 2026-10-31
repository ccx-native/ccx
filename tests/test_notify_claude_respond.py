"""Original Claude MCP elicitation parent notifications on full CCX UUID + native R."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.settle as notify_settle
import ccx.cli.commands.turns as commands_turns
import ccx.ops.input as ops_input
import ccx.ops.wait as ops_wait

from argparse import Namespace
import json

import pytest

from ccx import client as client_module
from ccx.codex import control as codex_control
from ccx.cli.commands import _common as commands
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.client import CcxClient
from ccx.errors import DeliveryRejected, OperationConflict
from ccx.store.store import _digest
from test_generation_store import A, C, short_root as short_root  # noqa: F401
from test_notify_claude import owners as owners  # noqa: F401
from test_notify_claude_permission import _managed_parent
from test_notify_claude_question import _ids
from test_notify_ext import FakeControl
from test_notify_standalone import _native_caller
from test_notify_receiptless import (
    _claude_sources, _claude_hook_events, _claude_line,
)
from test_claude_native_cli import generated_hook_host as generated_hook_host  # noqa: F401


FORM_SCHEMA = {
    "type": "object",
    "properties": {"consent": {"type": "boolean"},
                   "count": {"type": "integer", "minimum": 1},
                   "title": {"type": "string"}},
    "required": ["consent", "count"],
}


def _args(gate=None, *, action="accept", content=None, fields=None, json_mode=True):
    return Namespace(
        target=A, request_id=gate,
        accept=action == "accept", decline=action == "decline",
        cancel=action == "cancel", content=content, field=fields,
        notify_parent=True, wait=False, no_wait=False, timeout=6,
        json=json_mode,
    )


@pytest.fixture
def mcp_world(owners, monkeypatch, tmp_path):
    _root, store = owners
    transcript = _claude_sources(store, monkeypatch, tmp_path)
    r1 = "ccx-turn:claude:v1:original-elicitation-r1"
    gate = {"id": "original-mcp-elicitation", "type": "elicitation",
            "server": "fake-mcp", "mode": "form", "schema": FORM_SCHEMA}
    _claude_hook_events(store, {"type": "turn_start", "turn_ref": r1,
                                "turn": 1, "report_start_offset": 0})
    store.mutate_record("claude:" + A, patches={
        "waitingFor": gate, "lifecycle.state": "waiting",
        "lifecycle.activeTurnRef": r1,
    })
    _native_caller(monkeypatch)
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_a: True)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    protocol_ops._CLIENT.value = None
    state = {"gate": gate, "turn": r1, "native": "accepted"}
    calls = []

    def original_host(wid, msg, timeout=10):
        assert wid == A
        if msg["op"] == "status":
            return {"ok": True, "state": "waiting", "waitingFor": state["gate"],
                    "activeTurnRef": state["turn"]}
        assert msg["op"] == "elicitation-response"  # NEVER a new user SEND
        token = msg["response_id"]
        payload = {"action": msg["action"], "content": msg.get("content")}
        receipt = store.read_receipt("claude:" + A, "answer", token)
        assert receipt["state"] == "submitted-unknown"
        row = parent_notify.read_registration(_ids(store, msg["request_id"])[1])
        assert row["state"] == "armed" and row["source"] == "input"
        assert row["operation"]["kind"] == "claude-input-respond"
        assert row["operation"]["selectedTurnRef"] == r1
        assert row["operation"]["payloadSha256"] == _digest(payload)
        assert row["operation"]["intentSha256"] == receipt["intentSha256"]
        assert json.loads(parent_notify.registration_path(row["eventId"]).read_text()) == row
        sid = store.record(A)["native"]["sessionId"]
        assert sid != A
        # Independent original layout literal: not a copy of production helper.
        expected = _digest({
            "workerId": A, "engine": "claude", "generation": store.generation,
            "nativeId": sid, "nativeSource": "claude-session:" + sid,
            "kind": "respond", "requestId": msg["request_id"],
            "responseId": token, "payloadSha256": _digest(payload),
            "action": "answer",
        })
        assert receipt["intentSha256"] == expected
        assert expected == ops_input._input_intent_digest(
            worker_id=A, engine="claude", generation=store.generation,
            native_id=sid, native_source="claude-session:" + sid,
            kind="respond", request_id=msg["request_id"], response_id=token,
            payload_sha256=row["operation"]["payloadSha256"])
        calls.append(dict(msg))
        if state["native"] == "ambiguous":
            raise TimeoutError("native MCP response ACK lost after write")
        return {"ok": True, "request_id": msg["request_id"],
                "response_id": token, "action": msg["action"]}

    monkeypatch.setattr(client_module, "_rpc", original_host)
    return store, transcript, r1, gate, state, calls


@pytest.mark.parametrize("parent_kind,raw_content,fields", [
    ("interactive", None, ["consent=no", "count=3", "title=Read"]),
    ("managed", '{"title":"Read"}', ["consent=no", "count=3"]),
])
def test_mcp_form_source_mapping_original_parent_and_stale_field_refusal(
    mcp_world, tmp_path, monkeypatch, capsys, parent_kind, raw_content, fields,
):
    store, _transcript, r1, gate, state, calls = mcp_world
    parent = (_managed_parent(store, monkeypatch, tmp_path)
              if parent_kind == "managed" else _native_caller(monkeypatch))
    request_id = gate["id"]
    response_id, event_id = _ids(store, request_id)
    args = _args(request_id, content=raw_content, fields=fields)
    # The selected schema cannot come from a later gate, even if the field name matches.
    with pytest.raises(SystemExit) as stale:
        commands_turns.cmd_respond(_args("old-mcp", fields=fields))
    assert stale.value.code == 3
    err = capsys.readouterr().err
    assert "response_id=" in err and "notification_event_id=" in err
    assert not calls and not parent_notify.iter_registrations()

    commands_turns.cmd_respond(args)
    public = json.loads(capsys.readouterr().out)
    assert public["ok"] and public["status"] == "accepted"
    assert public["action"] == "accept" and public["original_gate_answered"]
    assert public["input_mode"] == "native-input"
    assert public["response_id"] == response_id and public["worker_id"] == A
    assert public["notify_parent"]["event_id"] == event_id
    assert public["notify_parent"]["source"] == "input"
    assert public["notify_parent"]["selected_turn_ref"] == r1
    assert public["notify_parent"]["parent"] == (
        "codex:" + C if parent_kind == "managed" else parent)
    assert len(calls) == 1 and calls[0]["action"] == "accept"
    assert calls[0]["content"] == {"consent": False, "count": 3, "title": "Read"}
    row = parent_notify.read_registration(event_id)
    assert set(row["operation"]) == {
        "kind", "gateId", "responseId", "nativeSourceId",
        "intentSha256", "selectedTurnRef", "payloadSha256"}

    # Original receipt replays without selecting the new current gate.
    successor = {"id": "next-mcp", "type": "elicitation", "mode": "form",
                 "schema": {"type": "object", "properties": {"title": {"type": "string"}}}}
    state["gate"] = successor
    store.mutate_record("claude:" + A, patches={"waitingFor": successor})
    replay = ops_input.respond_input(
        "claude:" + A, request_id=request_id, response_id=response_id,
        action="accept", content={"consent": False, "count": 3, "title": "Read"},
        notify_parent=True,
    )
    assert replay["status"] == "accepted" and len(calls) == 1
    # A self-contained typed --content JSON still permits read-only receipt
    # replay after the broker preview disappears; it is not --field remapping.
    commands_turns.cmd_respond(_args(
        request_id, content='{"consent":false,"count":3,"title":"Read"}',
    ))
    replay_cli = json.loads(capsys.readouterr().out)
    assert replay_cli["status"] == "accepted"
    assert replay_cli["response_id"] == response_id and len(calls) == 1
    with pytest.raises(OperationConflict):
        ops_input.respond_input(
            "claude:" + A, request_id=request_id, response_id=response_id,
            action="accept", content={"consent": True, "count": 3, "title": "Read"},
            notify_parent=True)
    # --field is source-schema-dependent: no post-gate reinterpretation.
    with pytest.raises(SystemExit) as after:
        commands_turns.cmd_respond(args)
    assert after.value.code == 3
    assert response_id in capsys.readouterr().err and len(calls) == 1


@pytest.mark.parametrize("action,mode,content", [
    ("decline", "form", None), ("cancel", "form", None),
    ("accept", "url", None),
])
def test_mcp_action_and_url_accept_native_positive_effect(
    mcp_world, action, mode, content, capsys,
):
    store, _transcript, _r1, gate, state, calls = mcp_world
    if mode == "url":
        gate = {**gate, "mode": "url", "schema": None,
                "url": "https://example.test/auth"}
        state["gate"] = gate
        store.mutate_record("claude:" + A, patches={"waitingFor": gate})
    commands_turns.cmd_respond(_args(None, action=action, content=content, json_mode=False))
    plain = capsys.readouterr().out
    assert f"{action} MCP input accepted" in plain
    assert "notify: armed " in plain  # actual state, not a claim of completed R
    assert len(calls) == 1 and calls[0]["action"] == action
    assert "content" not in calls[0]  # decline/cancel/url accept carry NO form
    row = parent_notify.read_registration(_ids(store, gate["id"])[1])
    assert row["operation"]["payloadSha256"] == _digest(
        {"action": action, "content": None})
    assert store.read_receipt(A, "answer", _ids(store, gate["id"])[0])["state"] == "accepted"


def test_mcp_wrong_kind_unbrokered_invalid_schema_or_dead_host_no_effect(
    mcp_world, monkeypatch, capsys,
):
    store, _transcript, _r1, gate, state, calls = mcp_world
    token, event = _ids(store, gate["id"])
    # Form validation is strict even when this is a notified asynchronous response.
    with pytest.raises(SystemExit) as invalid:
        commands_turns.cmd_respond(_args(gate["id"], content='{"consent":"no","count":3}'))
    assert invalid.value.code == 1
    assert not calls and not parent_notify.read_registration(event)
    capsys.readouterr()
    with pytest.raises(SystemExit) as forbidden:
        commands_turns.cmd_respond(_args(gate["id"], action="decline", content="{}"))
    assert forbidden.value.code == 1
    capsys.readouterr()
    for different in ({"id": gate["id"], "tool": "AskUserQuestion"},
                      {**gate, "id": None}):
        store.mutate_record("claude:" + A, patches={"waitingFor": different})
        state["gate"] = different
        with pytest.raises(SystemExit):
            commands_turns.cmd_respond(_args(gate["id"], fields=["consent=no"]))
        assert not calls and not parent_notify.read_registration(event)
        capsys.readouterr()
    store.mutate_record("claude:" + A, patches={"waitingFor": gate})
    state["gate"] = gate
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_a: False)
    with pytest.raises(SystemExit) as gone:
        commands_turns.cmd_respond(_args(gate["id"], action="decline"))
    assert gone.value.code == 3
    assert not calls and not parent_notify.read_registration(event)
    assert store.read_receipt(A, "answer", token) is None


def test_mcp_lost_ack_late_action_evidence_replays_original_no_resend(
    mcp_world, monkeypatch, capsys,
):
    store, _transcript, _r1, gate, state, calls = mcp_world
    token, event_id = _ids(store, gate["id"])
    state["native"] = "ambiguous"
    args = _args(gate["id"], action="decline")
    with pytest.raises(SystemExit) as pending:
        commands_turns.cmd_respond(args)
    assert pending.value.code == 6
    printed = json.loads(capsys.readouterr().out)
    assert printed["status"] in {"submitted", "unknown"}
    assert not printed["original_gate_answered"]
    assert parent_notify.read_registration(event_id)["state"] == "armed"
    assert len(calls) == 1
    state["gate"] = {"id": "successor", "tool": "Bash"}
    store.mutate_record("claude:" + A, patches={"waitingFor": state["gate"]})
    # Action-only with original supplied request id has a self-contained payload:
    # CLI may replay existing receipt without current schema/another native effect.
    with pytest.raises(SystemExit) as retry:
        commands_turns.cmd_respond(args)
    assert retry.value.code == 6
    assert json.loads(capsys.readouterr().out)["response_id"] == token
    assert len(calls) == 1

    from ccx.ops import worker_events
    event = {"type": "elicitation_resolved", "request_id": gate["id"],
             "response_id": token, "action": "cancel", "cursor": "wrong-action",
             "epoch": "old-host", "seq": 7}
    monkeypatch.setattr(worker_events, "read_full_uuid_event_page",
                        lambda *_a, **_kw: {"events": [dict(event)]})
    inconclusive = ops_input.respond_input(
        "claude:" + A, request_id=gate["id"], response_id=token,
        action="decline", notify_parent=True)
    assert inconclusive["status"] == "unknown" and len(calls) == 1
    event.update(action="decline", cursor="exact-original-action")
    accepted = ops_input.respond_input(
        "claude:" + A, request_id=gate["id"], response_id=token,
        action="decline", notify_parent=True)
    assert accepted["status"] == "accepted"
    assert accepted["evidence"] == "host-event:old-host:7"
    assert accepted["notification"]["event_id"] == event_id and len(calls) == 1
    with pytest.raises(OperationConflict):
        ops_input.respond_input(
            "claude:" + A, request_id=gate["id"], response_id=token,
            action="cancel", notify_parent=True)


def test_mcp_definite_noeffect_cancels_exact_event_with_truthful_plain_status(
    mcp_world, monkeypatch, capsys,
):
    store, _transcript, _r1, gate, _state, calls = mcp_world
    token, event_id = _ids(store, gate["id"])
    protocol_ops._CLIENT.value = CcxClient()

    def reject_original(_worker, _action, _content, **kw):
        assert kw["wait"] is False and kw["response_id"] == token
        assert parent_notify.read_registration(event_id)["state"] == "armed"
        assert store.read_receipt(A, "answer", token)["state"] == "submitted-unknown"
        calls.append({"definitive": True})
        raise DeliveryRejected("current gate positively differs",
                               reason="request id mismatch")
    monkeypatch.setattr(protocol_ops._CLIENT.value, "respond_elicitation", reject_original)
    with pytest.raises(SystemExit) as rejection:
        commands_turns.cmd_respond(_args(gate["id"], action="cancel", json_mode=False))
    assert rejection.value.code == 5
    plain = capsys.readouterr().out
    assert "notify: cancelled " in plain and "notify: armed " not in plain
    reg = parent_notify.read_registration(event_id)
    assert reg["state"] == "cancelled"
    again = ops_input.respond_input(
        "claude:" + A, request_id=gate["id"], response_id=token,
        action="cancel", notify_parent=True)
    assert again["status"] == "rejected"
    assert again["notification"]["event_id"] == event_id
    assert parent_notify.read_registration(event_id)["state"] == "cancelled"
    assert len(calls) == 1


def test_mcp_same_gate_new_gate_same_r_original_stop_only_one_wake(
    mcp_world, monkeypatch,
):
    store, transcript, r1, gate, state, calls = mcp_world
    accepted = ops_input.respond_input(
        "claude:" + A, request_id=gate["id"], response_id="mcp-original-R-response",
        action="decline", notify_parent=True)
    assert accepted["status"] == "accepted" and len(calls) == 1
    reg = parent_notify.read_registration(accepted["notification"]["event_id"])
    assert notify_settle.settlement_for_registration(reg) is None
    successor = {"id": "new-mcp", "type": "elicitation", "mode": "url",
                 "url": "https://example.test/new", "schema": None}
    store.mutate_record("claude:" + A, patches={"waitingFor": successor})
    state["gate"] = successor
    monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_kw: {
        "worker": "claude:" + A, "state": "waiting",
        "active_turn_ref": r1, "waiting_for": successor,
    })
    waiting = notify_settle.settlement_for_registration(reg)
    assert waiting["status"] == "waiting" and waiting["turnStatus"] == "waiting"
    assert waiting["report"] is None and waiting["waitingFor"]["id"] == successor["id"]
    _claude_line(transcript, {"type": "assistant", "message": {"content": [
        {"type": "text", "text": "EXACT elicitation original R"}]}})
    offset = transcript.stat().st_size
    _claude_hook_events(
        store,
        {"type": "turn_complete", "turn_ref": r1, "turn": 1},
        {"type": "turn_start", "turn_ref": "ccx-turn:claude:v1:later-mcp-R2",
         "turn": 2, "report_start_offset": offset},
    )
    _claude_line(transcript, {"type": "assistant", "message": {"content": [
        {"type": "text", "text": "WRONG later R2"}]}})
    completed = notify_settle.settlement_for_registration(reg)
    assert completed["status"] == "completed" and completed["turnRef"] == r1
    assert completed["report"] == "EXACT elicitation original R"
    native = FakeControl()
    monkeypatch.setattr(codex_control, "CodexControlClient", lambda: native)
    notifyd.process_registration(reg)
    done = parent_notify.read_registration(reg["eventId"])
    assert done["state"] == "delivered"
    assert done["payload"]["turnRef"] == r1 and done["payload"]["report"] == completed["report"]
    notifyd.process_registration(done)
    assert len(native.methods("turn/start")) == 1


@pytest.mark.parametrize("generated_hook_host", ["restricted"], indirect=True)
def test_generated_native_mcp_elicitation_notify_original_and_single_parent_wake(
    generated_hook_host, monkeypatch, capsys, request,
):
    """One complete disposable fake Claude Elicitation hook, real IPC and Codex W1."""
    from pathlib import Path
    import time
    from ccx.store import paths
    from test_claude_native_cli import _generated_hook_results

    store, call, env, hook_log = generated_hook_host
    from test_notify_claude_question import _isolate_disposable_notifyd_for_fake_native
    _isolate_disposable_notifyd_for_fake_native(store, request)
    submitted = call("send", A, "request generated elicitation", "--no-wait",
                     "--delivery-id", "mcp-notify-generated-native-1", "--json")
    assert submitted.returncode == 0, (submitted.stdout, submitted.stderr)
    deadline = time.monotonic() + 15
    waiting = None
    while time.monotonic() < deadline:
        waiting = store.record(A).get("waitingFor")
        if isinstance(waiting, dict) and waiting.get("id") and waiting.get("type") == "elicitation":
            break
        time.sleep(.06)
    assert waiting and waiting["server"] == "fake-mcp", waiting
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", env["CLAUDE_CONFIG_DIR"])
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    protocol_ops._CLIENT.value = None
    parent = _native_caller(monkeypatch)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    args = _args(waiting["id"], fields=["title=Text"])
    args.timeout = 15
    commands_turns.cmd_respond(args)
    public = json.loads(capsys.readouterr().out)
    assert public["status"] == "accepted" and public["original_gate_answered"]
    assert public["response_id"] == _ids(store, waiting["id"])[0]
    assert public["notify_parent"]["parent"] == parent
    reg = parent_notify.read_registration(public["notify_parent"]["event_id"])
    assert reg["operation"]["kind"] == "claude-input-respond"
    r1 = reg["operation"]["selectedTurnRef"]
    deadline = time.monotonic() + 15
    settled = None
    while time.monotonic() < deadline:
        settled = notify_settle.settlement_for_registration(reg)
        # The completed event can precede a readable transcript report.
        if (settled is not None and settled.get("status") == "completed"
                and isinstance(settled.get("report"), str)):
            break
        time.sleep(.06)
    assert (settled and settled["status"] == "completed"
            and isinstance(settled.get("report"), str)), settled
    assert settled["turnRef"] == r1 and "Fake Claude completed" in settled["report"]
    elicitations = [x for x in _generated_hook_results(hook_log)
                    if x["event"] == "Elicitation"]
    assert len(elicitations) == 1 and elicitations[0]["rc"] == 0
    hook = json.loads(elicitations[0]["stdout"])["hookSpecificOutput"]
    assert hook["hookEventName"] == "Elicitation"
    assert hook["action"] == "accept" and hook["content"] == {"title": "Text"}
    native = FakeControl()
    monkeypatch.setattr(codex_control, "CodexControlClient", lambda: native)
    from test_notify_claude_question import _drive_fake_native_notify_once
    _drive_fake_native_notify_once(reg, monkeypatch)
    finished = parent_notify.read_registration(reg["eventId"])
    assert finished["state"] == "delivered"
    assert finished["payload"]["turnRef"] == r1
    notifyd.process_registration(finished)
    assert len(native.methods("turn/start")) == 1
