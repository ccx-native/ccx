"""Public Python client API (`ccx.client.CcxClient`).

Covers the library surface: the documented import paths and exports, the typed
`ccx.errors` hierarchy and its CLI exit-code mapping, `ccx.types` projections,
typed refusals for unknown or short worker IDs, original input-gate decisions
against a sealed full-UUID worker with the host RPC mocked, and typed `usage()`
with the provider fetch mocked. No real host, native provider or network.
"""

from __future__ import annotations

import pytest

import ccx
from ccx import CcxClient
from ccx.client import CcxClient as CcxClientDirect
from ccx import errors as E
from ccx import types as T
from ccx.constants import (
    BUDGET_EXIT,
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_REJECTED,
    EXIT_TURN_INCOMPLETE,
    EXIT_UNAVAILABLE,
    EXIT_WAITING,
    S,
)
from ccx import constants
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, add_worker, fixture_root


# --- fixtures ----------------------------------------------------------------

@pytest.fixture
def client(tmp_path, monkeypatch):
    """A sealed ACTIVE root with no worker, for calls that need none."""
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    return CcxClient()


@pytest.fixture
def active_input_client(tmp_path, monkeypatch):
    """One sealed, disposable Claude worker for real Python input preflight."""
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    return CcxClient(), require_ccx_active(root), A


# --- importability -----------------------------------------------------------

def test_client_importable_from_both_paths():
    assert CcxClient is CcxClientDirect
    assert ccx.CcxClient is CcxClient


def test_public_symbols_exported():
    for name in ("CcxClient", "SessionInfo", "SessionStatus", "ClaudeTask", "Report",
                 "SendResult", "CompactResult", "CompactOperationResult",
                 "WaitResult", "Usage", "CcxError",
                 "SessionNotFound", "SessionUnavailable", "SessionWaiting",
                 "DeliveryRejected", "TurnIncomplete", "OperationTimeout",
                 "ProviderProtocolError", "CompactIndeterminate", "BudgetStopped"):
        assert hasattr(ccx, name), name


# --- exception hierarchy / exit-code mapping ---------------------------------

def test_all_errors_subclass_ccx_error():
    for cls in (E.InvalidRequest, E.CursorInvalidated, E.ProviderProtocolError,
                E.OperationTimeout, E.SessionNotFound, E.SessionUnavailable,
                E.SessionWaiting, E.DeliveryRejected, E.TurnIncomplete,
                E.CompactIndeterminate, E.BudgetStopped):
        assert issubclass(cls, E.CcxError)


def test_exit_code_mapping_matches_constants():
    assert E.InvalidRequest.exit_code == EXIT_ERROR
    assert E.SessionNotFound.exit_code == EXIT_NOT_FOUND
    assert E.SessionUnavailable.exit_code == EXIT_UNAVAILABLE
    assert E.SessionWaiting.exit_code == EXIT_WAITING
    assert E.DeliveryRejected.exit_code == EXIT_REJECTED
    assert E.TurnIncomplete.exit_code == EXIT_TURN_INCOMPLETE
    assert E.OperationTimeout.exit_code == EXIT_TURN_INCOMPLETE
    assert E.ProviderProtocolError.exit_code == EXIT_ERROR
    assert E.BudgetStopped.exit_code == BUDGET_EXIT


def test_exit_code_to_error_table_is_consistent():
    for code, cls in E.EXIT_CODE_TO_ERROR.items():
        assert cls.exit_code == code


# --- types: as_dict / projections --------------------------------------------

def test_sessionstatus_as_dict_drops_absent_optional_keys():
    st = T.SessionStatus(short="abcd1234", state="idle", turn=2)
    d = st.as_dict()
    assert d["short"] == "abcd1234"
    assert "mode" not in d and "needs" not in d and "error" not in d


# --- peek: typed miss for an unowned full UUID -------------------------------

def test_peek_unknown_full_uuid_raises_not_found(active_input_client):
    client, _, _ = active_input_client
    with pytest.raises(E.SessionNotFound):
        client.peek(B)  # valid full UUID, unowned, same eight-char prefix as A


# --- approve / reject: exact UUID owner, native preflight and typed errors ---

@pytest.mark.parametrize("verb", ("answer", "approve", "reject"))
@pytest.mark.parametrize(
    "target,error",
    ((B, E.SessionNotFound), (A[:8], E.InvalidRequest)),
    ids=("unowned-full-uuid", "invalid-short-alias"),
)
def test_input_decision_missing_worker_or_short_alias(
    active_input_client, monkeypatch, verb, target, error,
):
    client, store, worker_id = active_input_client
    before = store.record(worker_id)
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live",
                        lambda *_a, **_k: pytest.fail("must not inspect host"))
    monkeypatch.setattr("ccx.client._rpc",
                        lambda *_a, **_k: pytest.fail("must not send native input"))
    with pytest.raises(error) as exc:
        if verb == "answer":
            client.answer(target, "yes", request_id="r")
        else:
            getattr(client, verb)(target, request_id="r")
    assert exc.value.exit_code == error.exit_code
    assert store.record(worker_id) == before


def test_approve_host_not_alive_raises_unavailable(active_input_client, monkeypatch):
    client, store, worker_id = active_input_client
    store.mutate_record(worker_id, patches={
        "waitingFor": {
            "id": "permission-dead-1", "type": "permission", "tool": "Bash",
            "input": {"command": "echo test"},
        },
        "lifecycle.state": S.WAITING,
    })
    before = store.record(worker_id)
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_a, **_k: False)
    monkeypatch.setattr("ccx.client._rpc",
                        lambda *_a, **_k: pytest.fail("dead host must not receive RPC"))
    with pytest.raises(E.SessionUnavailable, match="not alive") as error:
        client.approve(worker_id, request_id="permission-dead-1")
    assert error.value.short == worker_id
    assert store.record(worker_id) == before


@pytest.mark.parametrize("decision_method", ("approve", "reject"))
def test_decision_on_question_prompt_raises_unavailable(
    active_input_client, monkeypatch, decision_method,
):
    client, store, worker_id = active_input_client
    store.mutate_record(worker_id, patches={
        "waitingFor": {
            "id": "question-1", "type": "permission", "tool": "AskUserQuestion",
            "input": {"questions": [{"question": "Choose?", "options": []}]},
        },
        "lifecycle.state": S.WAITING,
    })
    before = store.record(worker_id)
    # Permission decisions check liveness before the recorded gate kind.
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_a, **_k: True)
    monkeypatch.setattr("ccx.client._rpc",
                        lambda *_a, **_k: pytest.fail("wrong verb must not reach RPC"))
    with pytest.raises(E.SessionUnavailable, match="answer") as error:
        getattr(client, decision_method)(worker_id, request_id="question-1")
    assert error.value.short == worker_id
    assert store.record(worker_id) == before


# --- usage (monkeypatched, no network) ---------------------------------------

def test_usage_returns_typed(client, monkeypatch):
    payload = {"five_hour": {"utilization": 42.0, "resets_at": "2026-06-07T12:00:00Z"}}
    monkeypatch.setattr("ccx.client.fetch_usage", lambda **k: payload)
    u = client.usage()
    assert isinstance(u, T.Usage)
    assert u.utilization("five_hour") == 42.0
    assert "five_hour" in u.as_dict()["summary"]


def test_usage_no_signal_raises(client, monkeypatch):
    monkeypatch.setattr("ccx.client.fetch_usage", lambda **k: None)
    with pytest.raises(E.InvalidRequest):
        client.usage()


# --- answer: exact request/response fence ------------------------------------

def test_answer_forwards_response_id_and_exact_request_fence(
    active_input_client, monkeypatch,
):
    client, store, worker_id = active_input_client
    store.mutate_record(worker_id, patches={
        "waitingFor": {
            "id": "request-live", "tool": "AskUserQuestion", "type": "permission",
            "input": {"questions": [
                {"question": "Choose?", "options": [{"label": "A"}]},
            ]},
        },
        "lifecycle.state": S.WAITING,
    })
    before = store.record(worker_id)
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_a, **_k: True)
    seen = []
    reply = {"ok": True, "request_id": "request-live",
             "response_id": "response-live"}

    def fake_rpc(target, message, **_kwargs):
        seen.append((target, message))
        return reply

    monkeypatch.setattr("ccx.client._rpc", fake_rpc)
    args = {"text_answers": [(1, "A")], "request_id": "request-live",
            "response_id": "response-live"}
    result = client.answer(worker_id, **args)
    expected = {
        "op": "answer-permission", "answers": {"Choose?": "A"},
        "request_id": "request-live", "response_id": "response-live",
    }
    assert seen == [(worker_id, expected)]
    assert isinstance(result, T.SendResult)
    assert (result.worker_id, result.engine, result.queued, result.state) == (
        worker_id, "claude", True, S.BUSY,
    )
    assert result.as_dict()["worker"] == f"claude:{worker_id}"
    assert store.record(worker_id) == before

    # Positive ACK with a different response identity cannot prove success.
    reply = {"ok": True, "request_id": "request-live",
             "response_id": "another-response"}
    with pytest.raises(E.SendIndeterminate) as error:
        client.answer(worker_id, **args)
    assert error.value.exit_code == E.SendIndeterminate.exit_code
    assert error.value.short == worker_id
    assert seen == [(worker_id, expected), (worker_id, expected)]
    assert store.record(worker_id) == before
