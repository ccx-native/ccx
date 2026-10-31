'Import identity and pure projection of the public Codex client types.\n\n`CodexClient` is one object whether imported from `ccx` or\n`ccx.codex.client`, every public Codex result type serializes with\n`engine="codex"`, and a wait result takes its active-turn error from the wait\npayload over a stale record. No worker, record store or native thread is used.\n'
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait

import ccx
import ccx.codex.client as client_mod
import pytest
from types import SimpleNamespace
from ccx import (
    CodexApprovalResult,
    CodexClient,
    CodexCompactResult,
    CodexContext,
    CodexControlResult,
    CodexReport,
    CodexSessionInfo,
    CodexTelemetryPage,
    CodexTurnResult,
    CodexUsage,
    CodexWaitResult,
)


def test_codex_client_imports_and_bounded_surface():
    assert ccx.CodexClient is CodexClient
    assert client_mod.CodexClient is CodexClient


def test_codex_client_wait_reports_budget_stop_and_forwards_caps(monkeypatch):
    from ccx.ops import common as protocol_ops
    from ccx.codex import settle as codex_settle
    from ccx.errors import BudgetStopped
    from test_generation_store import B

    client = object.__new__(CodexClient)
    client._uuid_store = SimpleNamespace(revalidate=lambda: None)
    monkeypatch.setattr(CodexClient, "_owned_codex",
                        lambda self, target: (f"codex:{B}", {}))
    captured = []
    monkeypatch.setattr(ops_send, "send_turn", lambda *a, **kw:
                        captured.append(kw) or {"status": "accepted", "settled": False})
    monkeypatch.setattr(ops_wait, "reconcile_turn", lambda *a, **kw: {
        "status": "accepted", "delivery": {"turn_id": "turn-capped"},
    })
    monkeypatch.setattr(codex_settle, "send_settle_failure", lambda *a: {
        "status": "budget-stopped", "error": "usage cap 7d reached",
    })
    with pytest.raises(BudgetStopped, match="7d") as stopped:
        client.send(f"codex:{B}", "hello", wait=True, caps={"7d": 60})
    assert stopped.value.phase == "turn"
    assert captured[0]["cap"] == {"7d": 60}


def test_codex_settle_only_marks_original_interruption_as_budget(monkeypatch):
    from ccx.ops import common as protocol_ops
    from ccx.codex import records as codex_records
    from ccx.codex.settle import send_settle_failure
    from test_generation_store import B
    handle = f"codex:{B}"
    monkeypatch.setattr(ops_wait, "wait_worker", lambda *a, **kw: {
        "status": "idle",
        "lastTurn": {"turnId": "turn-capped", "status": "interrupted"},
    })
    monkeypatch.setattr(codex_records, "read_record", lambda *a: {
        "quotaStop": {"turnId": "turn-capped", "reason": "usage cap 7d reached"},
    })
    observation = {"status": "accepted",
                   "delivery": {"turn_ref": "ccx-turn:codex:v1:turn-capped"}}
    assert send_settle_failure(handle, observation) == {
        "status": "budget-stopped", "error": "usage cap 7d reached",
    }
    other = {"status": "accepted",
             "delivery": {"turn_ref": "ccx-turn:codex:v1:unrelated"}}
    assert send_settle_failure(handle, other) is None


def test_codex_public_types_are_exported_and_json_friendly():
    values = [
        CodexSessionInfo(short="s"),
        CodexTelemetryPage(short="s"),
        CodexTurnResult(short="s"),
        CodexControlResult(short="s"),
        CodexWaitResult(short="s"),
        CodexReport(short="s"),
        CodexApprovalResult(short="s"),
        CodexCompactResult(short="s"),
        CodexContext(short="s"),
        CodexUsage(),
    ]
    assert all(value.as_dict()["engine"] == "codex" for value in values)


def test_codex_wait_result_exposes_active_turn_error_separately():
    record = {
        "short": "s",
        "lifecycle": {"state": "busy", "activeTurnId": "turn-1"},
        "activeTurnError": {
            "turnId": "turn-1", "error": {"message": "retrying"},
            "willRetry": True, "observedAtMs": 123,
        },
    }
    result = CodexWaitResult.from_result(record, {
        "ok": True,
        "state": "busy",
        "activeTurnId": "turn-1",
        "activeTurnError": None,
    })

    assert result.active_turn_error is None
    assert result.last_turn is None
