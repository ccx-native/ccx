"""Required F: only a positively dead original QUESTION becomes a notified NEW SEND."""
from __future__ import annotations
import ccx.notify.settle as notify_settle
import ccx.cli.commands.turns as commands_turns
import ccx.ops.send as ops_send

import json
from uuid import NAMESPACE_URL, uuid5

import pytest

from ccx.claude import delivery as claude_delivery
from ccx.codex import control as codex_control
from ccx.cli.commands import _common as commands
from ccx.store import liveness
from ccx.notify import daemon as notifyd
from ccx.system import oslayer
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.store.store import GenerationStore
from test_generation_store import A
from test_notify_claude import _fake_original_response, owners as owners
from test_notify_claude_question import (
    _args, _ids, question_world as question_world,
)
from test_notify_ext import FakeControl
from test_notify_standalone import _native_caller


def _fallback_ids(store, worker: str, gate: str) -> tuple[str, str]:
    token = str(uuid5(
        NAMESPACE_URL,
        f"ccx-question-deadhost-new-send:{store.generation}:{worker}:{gate}",
    ))
    event = str(uuid5(
        NAMESPACE_URL,
        f"ccx-notify:full-uuid-claude-send:{store.generation}:{worker}:{token}",
    ))
    return token, event


def _dead_question(store, monkeypatch):
    store.mutate_record("claude:" + A, patches={
        "lifecycle.hostPid": 987654321,
        "lifecycle.hostStart": "1234",
    })
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_a: False)
    _observed_question_host(monkeypatch, "dead")


def _observed_question_host(monkeypatch, presence):
    # Positive death/ownership is a matching recorded PID/start and an OS
    # observation. A missing token must still refuse the question fallback.
    reader = oslayer.process_info
    def observed(pid):
        if pid == 987654321:
            return oslayer.ProcessInfo(
                pid, presence, None,
                "S" if presence == "alive" else None,
                "1234" if presence == "alive" else None,
            )
        return reader(pid)
    monkeypatch.setattr(oslayer, "process_info", observed)


def test_f_only_positive_owned_preinput_death_and_exact_question(
    question_world, monkeypatch, capsys,
):
    store, _transcript, _r1, gate, _state, original_input = question_world
    gate_id = gate["id"]
    token, event = _fallback_ids(store, A, gate_id)
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_a: False)
    monkeypatch.setattr(
        ops_send, "send_turn",
        lambda *_a, **_kw: pytest.fail("F has not passed death/source proof"),
    )

    # Missing socket alone and unknown host PID cannot authorize F.
    with pytest.raises(SystemExit) as refused:
        commands_turns.cmd_answer(_args(gate_id))
    assert refused.value.code == 3
    assert "Cannot confirm Claude stopped" in capsys.readouterr().err

    store.mutate_record("claude:" + A, patches={
        "lifecycle.hostPid": 987654321,
        "lifecycle.hostStart": "1234",
    })
    _observed_question_host(monkeypatch, "alive")
    with pytest.raises(SystemExit) as alive:
        commands_turns.cmd_answer(_args(gate_id))
    assert alive.value.code == 3
    assert "Cannot confirm Claude stopped" in capsys.readouterr().err

    _observed_question_host(monkeypatch, "dead")
    original_activation = GenerationStore.native_activation
    with monkeypatch.context() as patch:
        def incorrect_source(self, owner):
            value = original_activation(self, owner)
            return {**value, "nativeBinding": {"sessionId": "wrong-SID"}}
        patch.setattr(GenerationStore, "native_activation", incorrect_source)
        with pytest.raises(SystemExit) as wrong_source:
            commands_turns.cmd_answer(_args(gate_id))
        assert wrong_source.value.code == 3
        assert "Cannot verify this question" in capsys.readouterr().err

    # Another right-kind gate may not supply original answer labels.
    wrong = _args("different-original-question")
    with pytest.raises(SystemExit) as stale:
        commands_turns.cmd_answer(wrong)
    assert stale.value.code == 3
    assert "response_id=" in capsys.readouterr().err

    # Permission/MCP gates never convert to a question fallback.
    store.mutate_record("claude:" + A, patches={"waitingFor": {
        "id": gate_id, "tool": "Bash", "input": {"command": "true"},
    }})
    with pytest.raises(SystemExit) as wrong_kind:
        commands_turns.cmd_answer(_args(gate_id))
    assert wrong_kind.value.code == 3
    assert not original_input
    assert claude_delivery.read_receipt("claude:" + A, token) is None
    assert parent_notify.read_registration(event) is None


def test_f_own_send_receipt_and_event_precede_one_effect_then_exact_new_r_wake(
    question_world, monkeypatch, capsys,
):
    store, _transcript, original_r, gate, _state, original_input = question_world
    _dead_question(store, monkeypatch)
    gate_id = gate["id"]
    token, event_id = _fallback_ids(store, A, gate_id)
    qtoken, qevent = _ids(store, gate_id)
    assert token != qtoken and event_id != qevent
    parent = _native_caller(monkeypatch)
    native = FakeControl()
    monkeypatch.setattr(codex_control, "CodexControlClient", lambda: native)

    message = ("User provided answer to the previous question:\n"
               "Question: Proceed?\nAnswer: Yes")
    submitted = []
    preceding = []
    original_begin = GenerationStore.begin_native_intent

    def before_effect(self, target, *, action, family, operation_id, intent_sha256):
        if family == "claude-delivery" and operation_id == token:
            semantic = claude_delivery.read_receipt("claude:" + A, token)
            event = parent_notify.read_registration(event_id)
            assert semantic["state"] == "prepared"
            assert semantic["promptText"] == message
            assert semantic["requestedMode"] == "new_turn_only"
            assert semantic["cap"] is None
            assert event["source"] == "send"
            assert event["state"] == "armed"
            assert event["parent"] == parent
            assert event["operation"]["deliveryId"] == token
            assert event["operation"]["kind"] == "claude-turn"
            assert event["operation"]["promptSha256"] == semantic["promptSha256"]
            assert json.loads(parent_notify.registration_path(event_id).read_text()) == event
            assert not submitted and not original_input
            preceding.append(event_id)
        return original_begin(self, target, action=action, family=family,
                              operation_id=operation_id, intent_sha256=intent_sha256)

    monkeypatch.setattr(GenerationStore, "begin_native_intent", before_effect)
    original_send = ops_send.send_turn

    def native_send(handle, sid, text, delivery_id, _budget):
        assert handle == "claude:" + A and delivery_id == token
        assert sid == store.record(A)["native"]["sessionId"]
        assert text == message and preceding == [event_id]
        submitted.append(delivery_id)
        return _fake_original_response(store, delivery_id, text)

    new_r = "ccx-turn:claude:v1:original-" + token

    def native_stop(handle, turn_ref):
        assert (handle, turn_ref) == ("claude:" + A, new_r)
        return {
            "workerId": A,
            "nativeSource": "claude-session:" + store.record(A)["native"]["sessionId"],
            "deliveryId": token, "turnRef": new_r, "status": "completed",
            "text": "EXACT new SEND model reply", "nativeEvidenceSha256": "b" * 64,
        }

    def forwarded(*args, **kwargs):
        assert kwargs["mode"] == "new_turn_only"
        assert kwargs["notify_parent"] is True and kwargs["settle"] is False
        assert kwargs["cap"] is None
        assert kwargs["expected_generation"] == store.generation
        return original_send(*args, **kwargs, _native_submit=native_send,
                             _observe_settlement=native_stop)

    monkeypatch.setattr(ops_send, "send_turn", forwarded)
    commands_turns.cmd_answer(_args(gate_id))
    result = json.loads(capsys.readouterr().out)
    assert result["input_mode"] == "new-message-fallback"
    assert result["original_gate"] == result["request_id"] == gate_id
    assert result["original_gate_answered"] is False
    assert result["delivery_id"] == token
    assert result["status"] == "accepted" and result["owned"]
    assert result["settled"] and result["completed_model_turn"]
    assert result["turn_ref"] == new_r and new_r != original_r
    assert result["notify_parent"]["event_id"] == event_id
    assert result["notify_parent"]["source"] == "send"
    assert not original_input and submitted == [token]
    assert store.read_receipt("claude:" + A, "answer", qtoken) is None
    assert parent_notify.read_registration(qevent) is None
    assert store.record(A)["waitingFor"]["id"] == gate_id

    # Notification is about this NEW model R, not the original blocked Q
    # or a later/latest unrelated report. Parent W1 is machine placement only.
    store.mutate_record("claude:" + A, patches={
        "lastReport": {"turnRef": "ccx-turn:claude:v1:WRONG-R2", "text": "WRONG R2"},
    })
    registration = parent_notify.read_registration(event_id)
    settlement = notify_settle.settlement_for_registration(registration)
    assert settlement["status"] == "completed"
    assert settlement["turnRef"] == new_r
    assert settlement["report"] == "EXACT new SEND model reply"
    notifyd.process_registration(registration)
    notified = parent_notify.read_registration(event_id)
    assert notified["state"] == "delivered"
    assert notified["payload"]["report"] == "EXACT new SEND model reply"
    notifyd.process_registration(notified)
    assert len(native.methods("turn/start")) == 1


def test_f_same_preview_uses_core_same_id_then_successor_read_only(
    question_world, monkeypatch, capsys,
):
    store, _transcript, _r1, gate, _state, original_input = question_world
    _dead_question(store, monkeypatch)
    token, event = _fallback_ids(store, A, gate["id"])
    submitted = []
    original_send = ops_send.send_turn

    def send_fake(*args, **kwargs):
        def once(_handle, _sid, text, native_id, _budget):
            submitted.append((text, native_id))
            return _fake_original_response(store, native_id, text)
        return original_send(*args, **kwargs, _native_submit=once)

    monkeypatch.setattr(ops_send, "send_turn", send_fake)
    commands_turns.cmd_answer(_args(gate["id"]))
    first = json.loads(capsys.readouterr().out)
    assert first["status"] == "accepted" and not first["settled"]
    assert len(submitted) == 1
    # The original question remains previewable, but host now appears live.
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_a: True)
    with monkeypatch.context() as patch:
        patch.setattr(liveness, "owned_process",
                      lambda *_a: pytest.fail("death must not be rechecked for F receipt"))
        commands_turns.cmd_answer(_args(gate["id"]))
    same = json.loads(capsys.readouterr().out)
    assert same["delivery_id"] == first["delivery_id"] == token
    assert same["notify_parent"]["event_id"] == event
    assert len(submitted) == 1 and not original_input
    # A same-ID DIFFERENT answer is a SEND-core immutable payload conflict.
    with pytest.raises(SystemExit) as conflict:
        commands_turns.cmd_answer(_args(gate["id"], text="No"))
    assert conflict.value.code == 5
    assert token in capsys.readouterr().err
    assert len(submitted) == 1

    # Original preview lost: never remap formatted '--option', inspect only.
    successor = {"id": "another-question", "tool": "AskUserQuestion",
                 "input": {"questions": [{"question": "WRONG later question"}]}}
    store.mutate_record("claude:" + A, patches={"waitingFor": successor})
    monkeypatch.setattr(ops_send, "send_turn",
                        lambda *_a, **_kw: pytest.fail("B2 is observation only"))
    commands_turns.cmd_answer(_args(gate["id"], text="conflicting late input"))
    observed = json.loads(capsys.readouterr().out)
    assert observed["status"] == "accepted" and observed["original_gate_answered"] is False
    assert observed["delivery_id"] == token and observed["notify_parent"]["source"] == "send"
    assert observed["inspection_guidance"].find(token) >= 0
    assert observed["turn_ref"] == first["turn_ref"]
    assert not observed["settled"] and not observed["completed_model_turn"]
    assert len(submitted) == 1
    plain = _args(gate["id"], text="late different text")
    plain.json = False
    commands_turns.cmd_answer(plain)
    output = capsys.readouterr().out
    assert "Claude question" in output and "NOT answered" in output
    assert "separate NEW user message" in output
    assert "parent machine placement" in output
    assert token in output and not original_input

    # Parent changed on a preview-gone retry: typed conflict with both F IDs.
    monkeypatch.setenv("CODEX_THREAD_ID", "different-native-parent-thread")
    with pytest.raises(SystemExit) as conflict:
        commands_turns.cmd_answer(_args(gate["id"]))
    assert conflict.value.code == 5
    guidance = capsys.readouterr().err
    assert token in guidance and event in guidance
    assert len(submitted) == 1


def test_f_preview_gone_prepared_is_guided_incomplete_not_retry(
    question_world, monkeypatch, capsys,
):
    store, _transcript, _r1, gate, _state, original_input = question_world
    token, event = _fallback_ids(store, A, gate["id"])
    from ccx.ops.semantics import question_answer_message
    payload = question_answer_message({"Proceed?": "Yes"})
    claude_delivery.prepare("claude:" + A, token, payload, mode="new_turn_only")
    parent_notify.create_registration(
        parent=_native_caller(monkeypatch), worker_engine="claude", worker_id=A,
        worker_name=None, source="send",
        operation={"kind": "claude-turn", "deliveryId": token},
    )
    store.mutate_record("claude:" + A, patches={"waitingFor": None})
    monkeypatch.setattr(ops_send, "send_turn",
                        lambda *_a, **_kw: pytest.fail("no payload replay when preview gone"))
    with pytest.raises(SystemExit) as incomplete:
        commands_turns.cmd_answer(_args(gate["id"], text="new ambiguous text"))
    assert incomplete.value.code == 6
    result = json.loads(capsys.readouterr().out)
    assert not result["ok"] and not result["original_gate_answered"]
    assert result["delivery_id"] == token
    assert result["pending"] and not result["owned"] and not result["settled"]
    assert result["recommended_action"] == "inspect"
    assert event in result["inspection_guidance"]
    assert not original_input


def test_f_original_q_receipt_second_replays_even_when_original_host_dead(
    question_world, monkeypatch, capsys,
):
    store, _transcript, _r1, gate, _state, original_input = question_world
    token, _event = _fallback_ids(store, A, gate["id"])
    commands_turns.cmd_answer(_args(gate["id"]))
    first = json.loads(capsys.readouterr().out)
    assert first["status"] == "accepted" and first["input_mode"] == "native-input"
    assert len(original_input) == 1

    _dead_question(store, monkeypatch)
    monkeypatch.setattr(ops_send, "send_turn",
                        lambda *_a, **_kw: pytest.fail("Q receipt must replay before F"))
    commands_turns.cmd_answer(_args(gate["id"]))
    replay = json.loads(capsys.readouterr().out)
    assert replay["status"] == "accepted" and replay["input_mode"] == "native-input"
    assert replay["response_id"] == first["response_id"]
    assert replay["notify_parent"]["event_id"] == first["notify_parent"]["event_id"]
    assert replay["original_gate_answered"] is True
    assert len(original_input) == 1
    assert claude_delivery.read_receipt("claude:" + A, token) is None
