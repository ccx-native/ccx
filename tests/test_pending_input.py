"""W4: typed pending-input surface and payload-size handling.

`PendingInput`/`Question` give orchestrators attribute access to what a WAITING
session is blocked on — without changing the JSON projection (`as_dict()`
returns the raw host preview verbatim, byte-compatible with the CLI `--json`
surfaces). Payload enforcement is opt-in: when an operator configures a
managed-send bound, excess text raises `PayloadTooLarge` before delivery.
CCX imposes no prompt byte limit by default.
"""

from __future__ import annotations
import ccx.cli.commands.turns as commands_turns

import json

import pytest

import ccx.cli.commands._common as C
from ccx import PendingInput, Question
from ccx import errors as E
from ccx import types as T
from ccx.constants import S

from tests.test_client_api import active_input_client  # noqa: F401  (fixture)

AUQ_WF = {
    "type": "permission",
    "id": "req-123",
    "tool": "AskUserQuestion",
    "input": {"questions": [
        {"question": "Which db?", "header": "DB",
         "options": [{"label": "postgres", "description": "pg"},
                     {"label": "sqlite"}],
         "multiSelect": False},
        {"question": "Region?", "options": [{"label": "eu"}, {"label": "us"}],
         "multiSelect": True},
    ]},
    "requested_at": 1750000000000,
    "message": None,
}

PERM_WF = {
    "type": "permission",
    "id": "req-456",
    "tool": "Bash",
    "input": {"command": "rm -rf build"},
    "requested_at": 1750000000001,
    "message": "wants to run a command",
}


# --- PendingInput / Question parsing --------------------------------------------

def test_pending_input_from_question_wf():
    p = PendingInput.from_waiting_for(AUQ_WF)
    assert p.kind == "question"
    assert p.tool == "AskUserQuestion"
    assert p.request_id == "req-123"
    assert len(p.questions) == 2
    q1, q2 = p.questions
    assert q1.text == "Which db?"
    assert q1.header == "DB"
    assert q1.labels == ["postgres", "sqlite"]
    assert q1.multi_select is False
    assert q2.multi_select is True


def test_pending_input_from_permission_wf():
    p = PendingInput.from_waiting_for(PERM_WF)
    assert p.kind == "permission"
    assert p.tool == "Bash"
    assert p.questions == []
    assert p.message == "wants to run a command"


def test_pending_input_as_dict_is_raw_passthrough():
    p = PendingInput.from_waiting_for(AUQ_WF)
    assert p.as_dict() == AUQ_WF  # byte-compatible with the CLI JSON
    assert json.loads(json.dumps(p.as_dict())) == AUQ_WF


def test_pending_input_none_and_garbage_safe():
    assert PendingInput.from_waiting_for(None) is None
    assert PendingInput.from_waiting_for("not-a-dict") is None
    p = PendingInput.from_waiting_for(AUQ_WF)
    assert PendingInput.from_waiting_for(p) is p  # idempotent


def test_question_from_dict_round_trips_raw():
    q = Question.from_dict(AUQ_WF["input"]["questions"][0])
    assert q.as_dict() == AUQ_WF["input"]["questions"][0]


# --- typed surface on peek / wait / SessionWaiting ---------------------------------

def test_peek_waiting_for_is_typed_and_json_stable(active_input_client):
    client, store, worker_id = active_input_client
    store.mutate_record(worker_id, patches={
        "waitingFor": AUQ_WF, "lifecycle.state": S.WAITING,
    })
    before = store.record(worker_id)
    st = client.peek(worker_id)
    assert st.worker_id == worker_id and st.engine == "claude"
    assert st.short == worker_id[:8]
    assert st.host_status == "unobserved"
    assert isinstance(st.waiting_for, PendingInput) and st.is_waiting
    assert st.waiting_for.request_id == "req-123"
    assert st.waiting_for.questions[0].labels == ["postgres", "sqlite"]
    assert st.waiting_for.questions[1].labels == ["eu", "us"]
    # The public projection retains the exact raw multi-question preview.
    assert st.as_dict()["waiting_for"] == AUQ_WF
    assert st.as_dict()["worker"] == f"claude:{worker_id}"
    assert store.record(worker_id) == before


def test_wait_result_as_dict_omits_absent_waiting_for():
    v = T.WaitResult(status="idle")
    assert "waiting_for" not in v.as_dict()


def test_session_waiting_pending_input_property():
    e = E.SessionWaiting("blocked", short="x", waiting_for=AUQ_WF)
    p = e.pending_input
    assert isinstance(p, PendingInput)
    assert p.kind == "question"
    assert E.SessionWaiting("blocked").pending_input is None


# --- payload bound ------------------------------------------------------------------

def test_send_payload_check_counts_utf8_bytes(monkeypatch):
    monkeypatch.setattr(commands_turns, "SEND_TEXT_MAX_BYTES", 10)
    v = commands_turns._send_payload_violation("é" * 6)  # 12 UTF-8 bytes, 6 chars
    assert v == (12, 10)


def test_send_payload_check_disabled_by_nonpositive_limit(monkeypatch):
    monkeypatch.setattr(commands_turns, "SEND_TEXT_MAX_BYTES", 0)
    assert commands_turns._send_payload_violation("x" * 1_000_000) is None
