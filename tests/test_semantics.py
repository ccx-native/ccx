"""Pure helpers and real-client/CLI semantic use, without external providers."""

from __future__ import annotations
import ccx.cli.commands.turns as commands_turns

import json

import pytest

import ccx.client as client_mod
import ccx.cli.commands._common as C
import ccx.ops.semantics as SEM
from ccx import CcxClient, PendingInput
from ccx import errors as E
from ccx import constants
from ccx.constants import S, SEND_TEXT_MAX_BYTES
from ccx.store.root import require_ccx_active
from test_generation_store import A, add_worker, fixture_root
from ccx.ops.semantics import (
    REQUEST_ID_MISMATCH,
    answer_illegality,
    answer_request_fence,
    filter_events_after,
    payload_guidance,
    pending_input_kind,
    question_answer_map,
    question_answer_message,
    send_payload_violation,
)
from ccx.types import Event

AUQ_WF = {
    "type": "permission", "id": "req-1", "tool": "AskUserQuestion",
    "input": {"questions": [{"question": "Which db?",
                             "options": [{"label": "pg"}, {"label": "lite"}],
                             "multiSelect": False}]},
    "requested_at": 1, "message": None,
}
PERM_WF = {
    "type": "permission", "id": "req-2", "tool": "Bash",
    "input": {"command": "rm -rf /"}, "requested_at": 1, "message": None,
}


# --- 1. pure helper behavior ---------------------------------------------------

def test_pending_input_kind():
    assert pending_input_kind(AUQ_WF) == "question"
    assert pending_input_kind(PERM_WF) == "permission"
    assert pending_input_kind(None) is None
    assert pending_input_kind({}) is None
    assert pending_input_kind("nonsense") is None


def test_answer_illegality_matrix():
    assert answer_illegality(AUQ_WF, "abc") is None
    assert "no pending question" in answer_illegality(None, "abc")
    msg = answer_illegality(PERM_WF, "abc")
    assert "permission prompt" in msg and "Bash" in msg and "approve/reject" in msg


def test_answer_request_fence():
    assert answer_request_fence(AUQ_WF, None) is None
    assert answer_request_fence(AUQ_WF, "req-1") is None
    assert answer_request_fence(AUQ_WF, "other") == REQUEST_ID_MISMATCH
    # No pending input at all cannot satisfy a fence.
    assert answer_request_fence(None, "req-1") == REQUEST_ID_MISMATCH


def test_question_answer_map_single_text():
    qs = AUQ_WF["input"]["questions"]
    answers, err = question_answer_map(qs, text="pg")
    assert err is None and answers == {"Which db?": "pg"}


def test_question_answer_map_empty_text_is_caller_error():
    answers, err = question_answer_map(AUQ_WF["input"]["questions"], text="  ")
    assert answers == {} and err is None


def test_question_answer_map_multi_requires_structured_or_json():
    qs = [{"question": "A?"}, {"question": "B?"}]
    _, err = question_answer_map(qs, text="just one answer")
    assert "JSON object" in err
    answers, err = question_answer_map(qs, text=json.dumps({"A?": "1", "B?": "2"}))
    assert err is None and answers == {"A?": "1", "B?": "2"}
    _, err = question_answer_map(qs, text=json.dumps({"A?": "1"}))
    assert "missing question" in err


def test_question_answer_map_structured():
    qs = [{"question": "A?", "options": [{"label": "x"}]}, {"question": "B?"}]
    answers, err = question_answer_map(
        qs, options=[(1, "x")], text_answers=[(2, "free")])
    assert err is None and answers == {"A?": "x", "B?": "free"}
    _, err = question_answer_map(qs, options=[(1, "x")])
    assert "missing answer" in err
    _, err = question_answer_map(qs, options=[(3, "x")], text_answers=[(2, "t")])
    assert "out of range" in err
    _, err = question_answer_map(
        qs, options=[(1, "x")], text_answers=[(1, "t"), (2, "u")])
    assert "both --option and --text-answer" in err
    _, err = question_answer_map(qs, text="conflict", options=[(1, "x"), (2, "y")])
    assert "do not combine" in err


def test_send_payload_violation_defaults_and_limits():
    assert send_payload_violation("x" * 10, limit=100) is None
    assert send_payload_violation("x" * 101, limit=100) == (101, 100)
    assert send_payload_violation("é" * 6, limit=10) == (12, 10)  # UTF-8 bytes
    assert send_payload_violation("x" * 10 ** 6, limit=0) is None
    # None → live process default from ccx.constants, unlimited by default.
    assert SEND_TEXT_MAX_BYTES == 0
    assert send_payload_violation("x" * 10 ** 6) is None


def _ev(seq, epoch, type="state"):
    return Event.from_dict({"type": type, "seq": seq, "epoch": epoch, "ts": seq})


def test_filter_events_after_cursor_semantics():
    events = [_ev(1, "e1"), _ev(2, "e1"), _ev(1, "e2"), _ev(2, "e2")]
    assert filter_events_after(events, None, None) == events
    assert filter_events_after(events, None, 1) == [events[1], events[3]]  # legacy
    assert filter_events_after(events, "e1", 1) == events[1:]
    assert filter_events_after(events, "e2", 1) == [events[3]]
    # Unknown/rotated-out epoch → full replay (at-least-once, never drop).
    assert filter_events_after(events, "gone", 5) == events


# --- 2. the real surfaces use these exact helpers --------------------------------

def test_client_event_filter_is_single_sourced():
    assert client_mod.filter_events_after is SEM.filter_events_after


def test_client_answer_helpers_are_single_sourced():
    assert client_mod.answer_illegality is SEM.answer_illegality
    assert client_mod.answer_request_fence is SEM.answer_request_fence
    assert client_mod.question_answer_map is SEM.question_answer_map
    assert client_mod.question_answer_message is SEM.question_answer_message


def test_commands_adapters_delegate_to_semantics(monkeypatch):
    # payload: the CLI adapter resolves the limit from its patchable module
    # global, then delegates to the pure core.
    monkeypatch.setattr(commands_turns, "SEND_TEXT_MAX_BYTES", 10)
    assert commands_turns._send_payload_violation("x" * 11) == SEM.send_payload_violation(
        "x" * 11, limit=10) == (11, 10)
    assert C._payload_guidance is SEM.payload_guidance

    # answer map: args-namespace adapter equals the pure core.
    from types import SimpleNamespace
    q_payload = {"questions": [{"question": "A?"}, {"question": "B?"}]}
    args = SimpleNamespace(text=None, option=[(1, "x")], text_answer=[(2, "y")])
    assert C._question_answer_map(q_payload, args) == SEM.question_answer_map(
        q_payload["questions"], options=[(1, "x")], text_answers=[(2, "y")])


def test_pending_input_type_uses_kind_helper():
    assert PendingInput.from_waiting_for(AUQ_WF).kind == pending_input_kind(AUQ_WF)
    assert PendingInput.from_waiting_for(PERM_WF).kind == pending_input_kind(PERM_WF)


# --- 3. real-client request fencing against a sealed fixture root ----------

@pytest.fixture
def active_claude(tmp_path, monkeypatch):
    """Lane-local accepted sealed generation; no host or provider is started."""
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    return require_ccx_active(root)


def _waiting_real(store, waiting_for=None, *, state=S.WAITING):
    store.mutate_record(A, patches={
        "waitingFor": waiting_for, "lifecycle.state": state,
    })


def test_real_answer_fence_mismatch(active_claude, monkeypatch):
    _waiting_real(active_claude, AUQ_WF)
    calls = []
    monkeypatch.setattr(client_mod, "_rpc", lambda *a, **kw: calls.append(a))
    with pytest.raises(E.DeliveryRejected) as caught:
        CcxClient().answer(A, "pg", request_id="stale-id")
    assert caught.value.reason == REQUEST_ID_MISMATCH
    assert caught.value.short == A
    assert caught.value.state == S.WAITING
    assert calls == []


def test_real_answer_fence_match_reaches_transport_gate(active_claude, monkeypatch):
    _waiting_real(active_claude, AUQ_WF)
    calls = []
    monkeypatch.setattr(client_mod.host_rpc, "host_socket_live", lambda *_a, **_kw: True)

    def ack(worker, message, **_kw):
        calls.append((worker, message))
        return {"ok": True, "request_id": "req-1", "response_id": "response-one"}

    monkeypatch.setattr(client_mod, "_rpc", ack)
    result = CcxClient().answer(A, "pg", request_id="req-1", response_id="response-one")
    assert result.as_dict()["worker"] == f"claude:{A}"
    assert result.queued and result.state == S.BUSY
    assert calls == [(A, {
        "op": "answer-permission", "answers": {"Which db?": "pg"},
        "request_id": "req-1", "response_id": "response-one",
    })]


def test_real_answer_illegality_no_pending(active_claude):
    _waiting_real(active_claude, None, state=S.IDLE)
    with pytest.raises(E.DeliveryRejected) as caught:
        CcxClient().answer(A, "pg", request_id="req-1")
    assert caught.value.exit_code == 5 and caught.value.short == A


def test_real_answer_illegality_permission_kind(active_claude):
    _waiting_real(active_claude, PERM_WF)
    with pytest.raises(E.SessionUnavailable) as caught:
        CcxClient().answer(A, "yes", request_id="req-1")
    assert caught.value.short == A


def test_real_answer_fence_precedes_host_liveness_gate(active_claude, monkeypatch):
    _waiting_real(active_claude, AUQ_WF)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("invalid request reached native transport")

    monkeypatch.setattr(client_mod.host_rpc, "host_socket_live", forbidden)
    monkeypatch.setattr(client_mod, "_rpc", forbidden)
    with pytest.raises(E.DeliveryRejected) as caught:
        CcxClient().answer(A, "pg", request_id="stale")
    assert caught.value.reason == REQUEST_ID_MISMATCH
    with pytest.raises(E.DeliveryRejected) as fallback:
        CcxClient().answer(A, "pg", request_id="stale", fallback_to_send=True)
    assert fallback.value.reason == REQUEST_ID_MISMATCH


def test_real_answer_omitted_request_id_binds_current_gate(active_claude, monkeypatch):
    _waiting_real(active_claude, AUQ_WF)
    calls = []
    monkeypatch.setattr(client_mod.host_rpc, "host_socket_live", lambda *_a, **_kw: True)

    def ack(worker, message, **_kw):
        calls.append((worker, message))
        return {"ok": True, "request_id": "req-1", "response_id": "response-one"}

    monkeypatch.setattr(client_mod, "_rpc", ack)
    result = CcxClient().answer(A, "pg", response_id="response-one")
    assert result.as_dict()["worker"] == f"claude:{A}"
    assert calls == [(A, {
        "op": "answer-permission", "answers": {"Which db?": "pg"},
        "request_id": "req-1", "response_id": "response-one",
    })]
