"""Engine-neutral original full-UUID permission decisions and waited turns."""
from __future__ import annotations

import time
import pytest

from ccx import CodexApprovalResult, WorkerClient
from ccx.errors import (
    CapabilityUnsupported, DeliveryRejected, InvalidRequest,
    SessionUnavailable, SessionWaiting, TurnIncomplete,
)
from test_generation_store import A, C, short_root
from test_worker_telemetry import original_codex, original_sources
from test_claude_native_cli import generated_hook_host


def codex_gate(store, wid=C, *, gate_id="original-gate-one",
               turn_id="original-native-turn"):
    store.mutate_record(wid, patches={"pendingApprovals": [{
        "gateId": gate_id, "requestId": 1, "turnId": turn_id,
        "runtimeId": "fake-original-daemon", "kind": "commandExecution",
    }]})
    return gate_id, turn_id


def settled(turn_id, *, outcome="completed", state="idle"):
    return {
        "state": state, "native_state": state, "status": state,
        "timed_out": state == "timeout",
        "last_turn": {"turnId": turn_id, "status": outcome, "error": None},
        "last_settled_turn": {
            "turn_ref": f"ccx-turn:codex:v1:{turn_id}",
            "turn_id": turn_id, "outcome": outcome,
        },
    }


@pytest.mark.parametrize("wait", (False, True))
def test_claude_delegates_original_permission_id_and_reason(
    original_sources, monkeypatch, wait,
):
    store, _cwd = original_sources
    store.mutate_record(A, patches={"waitingFor": {
        "id": "one-claude-gate", "type": "permission", "tool": "Bash",
        "tool_name": "Bash",
    }})
    worker = WorkerClient()
    calls = []
    def decision(target, *, request_id, reason, wait, timeout):
        calls.append((target, request_id, reason, wait, timeout))
        return type("Result", (), {"reply": "original reply" if wait else None})()
    monkeypatch.setattr(worker._claude, "reject", decision)
    result = worker.reject(A, reason="original rationale", wait=wait, timeout=8)
    assert result["worker"] == f"claude:{A}"
    assert result["request_id"] == "one-claude-gate"
    assert result["reply"] == ("original reply" if wait else None)
    assert len(calls) == 1 and calls[0][:4] == (
        f"claude:{A}", "one-claude-gate", "original rationale", wait,
    )
    assert 0 < calls[0][4] <= 8
    with pytest.raises(DeliveryRejected):
        worker.reject(A, request_id="a-successor", wait=wait)
    assert len(calls) == 1


def test_codex_one_native_decision_shared_deadline_exact_report(
    original_codex, monkeypatch,
):
    store, _cwd, wid, tid = original_codex
    gate, turn_id = codex_gate(store, wid)
    worker = WorkerClient()
    calls = []
    def native(target, operation, request_id, timeout=None, *,
               response_id=None, expected_turn_id=None):
        calls.append(("native", target, operation, request_id,
                      timeout, expected_turn_id))
        assert target == f"codex:{wid}" and expected_turn_id == turn_id
        time.sleep(.02)
        return CodexApprovalResult.from_record(
            store.record(wid), request_id=request_id, decision="accept",
        )
    def wait(target, *, timeout=None, **kwargs):
        calls.append(("wait", target, timeout, kwargs))
        return settled(turn_id)
    def report(target, *, turn_ref=None, timeout=None, **kwargs):
        calls.append(("report", target, turn_ref, timeout))
        return {"turn_ref": f"ccx-turn:codex:v1:{turn_id}",
                "turn_id": turn_id, "status": "completed",
                "text": "owned original reply"}
    monkeypatch.setattr(worker._codex, "_decision", native)
    monkeypatch.setattr(worker, "wait", wait)
    monkeypatch.setattr(worker, "report", report)
    result = worker.approve(wid, wait=True, timeout=3)
    assert result == {
        "worker": f"codex:{wid}", "worker_id": wid, "short": wid[:8],
        "engine": "codex", "decision": "allow",
        "request_id": gate, "reply": "owned original reply",
    }
    assert [row[0] for row in calls] == ["native", "wait", "report"]
    assert calls[0][5] == turn_id and calls[0][3] == gate
    assert calls[1][3] == {}  # Codex plain wait: no unsupported next-turn selector.
    assert 0 < calls[2][3] < calls[1][2] < calls[0][4] <= 3
    assert calls[2][2] == f"ccx-turn:codex:v1:{turn_id}"
    assert store.record(wid)["native"]["threadId"] == tid

    public = []
    monkeypatch.setattr(
        worker._codex, "approve",
        lambda target, *, request_id, timeout:
        public.append((target, request_id, timeout))
        or CodexApprovalResult.from_record(
            store.record(wid), request_id=request_id, decision="accept",
        ),
    )
    immediate = worker.approve(wid, request_id=gate, wait=False, timeout=3)
    assert immediate["reply"] is None and immediate["request_id"] == gate
    assert len(public) == 1
    assert len([row for row in calls if row[0] == "native"]) == 1


@pytest.mark.parametrize("case", (
    "stale", "failed", "interrupted", "timeout", "new-gate", "missing-report",
))
def test_codex_waited_approval_refuses_false_completion(
    original_codex, monkeypatch, case,
):
    store, _cwd, wid, _tid = original_codex
    gate, turn_id = codex_gate(store, wid)
    worker = WorkerClient()
    calls = []
    def native(target, operation, request_id, timeout=None, *,
               response_id=None, expected_turn_id=None):
        calls.append(("native", target, request_id, expected_turn_id))
        return CodexApprovalResult.from_record(
            store.record(wid), request_id=request_id, decision="accept",
        )
    def observe(target, *, timeout=None, **kwargs):
        calls.append(("wait", target))
        if case == "stale":
            return settled("later-native-turn")
        if case in ("failed", "interrupted"):
            return settled(turn_id, outcome=case)
        if case == "timeout":
            return {"native_state": "busy", "status": "timeout", "timed_out": True}
        if case == "new-gate":
            return {"native_state": "waiting", "status": "waiting",
                    "pending_input": {"native": {"pendingApprovals": [{
                        "gateId": "new-gate", "turnId": "new-turn",
                    }]}}}
        return settled(turn_id)
    def report(target, *, turn_ref, timeout):
        calls.append(("report", turn_ref))
        raise SessionUnavailable("original delivery receipt missing")
    monkeypatch.setattr(worker._codex, "_decision", native)
    monkeypatch.setattr(worker, "wait", observe)
    monkeypatch.setattr(worker, "report", report)
    expected = SessionWaiting if case == "new-gate" else TurnIncomplete
    with pytest.raises(expected) as failure:
        worker.approve(wid, request_id=gate, wait=True, timeout=3)
    assert failure.value.input_accepted is True
    assert failure.value.request_id == gate
    assert failure.value.turn_ref == f"ccx-turn:codex:v1:{turn_id}"
    assert "never retry" in str(failure.value)
    if expected is TurnIncomplete:
        assert failure.value.turn_id == turn_id
    assert [row[0] for row in calls] == (
        ["native", "wait", "report"] if case == "missing-report"
        else ["native", "wait"]
    )


def test_codex_unique_optional_gate_and_reason_refuse_pre_effect(
    original_codex, monkeypatch,
):
    store, _cwd, wid, _tid = original_codex
    gate, turn_id = codex_gate(store, wid)
    worker = WorkerClient()
    calls = []
    def reject(target, *, request_id, timeout):
        calls.append((target, request_id))
        return CodexApprovalResult.from_record(
            store.record(wid), request_id=request_id, decision="decline",
        )
    monkeypatch.setattr(worker._codex, "reject", reject)
    assert worker.reject(wid, wait=False)["request_id"] == gate
    assert calls == [(f"codex:{wid}", gate)]
    with pytest.raises(CapabilityUnsupported) as denied:
        worker.reject(wid, request_id=gate, reason="cannot transmit reason")
    assert denied.value.capability == "decision_reason"
    with pytest.raises(DeliveryRejected):
        worker.reject(wid, request_id="1")  # recyclable provider requestId
    assert len(calls) == 1
    store.mutate_record(wid, patches={"pendingApprovals": [
        {"gateId": gate, "requestId": 1, "turnId": turn_id},
        {"gateId": "second-unique-gate", "requestId": 1, "turnId": "other-turn"},
    ]})
    with pytest.raises(InvalidRequest, match="multiple original gates"):
        worker.reject(wid)
    with pytest.raises(DeliveryRejected):
        worker.reject(wid, request_id="1")
    assert len(calls) == 1


@pytest.mark.parametrize("generated_hook_host", ["restricted"], indirect=True)
def test_worker_waited_claude_permission_through_real_fake_host(
    generated_hook_host,
):
    """D2 calls accepted D1 exactly once on a genuine brokered native gate."""
    store, call, _env, _log = generated_hook_host
    sent = call(
        "send", A, "request generated permission", "--no-wait",
        "--delivery-id", "d2-real-original-permission", "--json",
    )
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    until = time.monotonic() + 12
    gate = None
    while time.monotonic() < until:
        gate = store.record(A).get("waitingFor")
        if isinstance(gate, dict) and gate.get("id") and gate.get("tool") == "Bash":
            break
        time.sleep(.04)
    assert gate and gate["id"], store.record(A)
    worker = WorkerClient()
    result = worker.approve(A, wait=True, timeout=12)
    assert result["engine"] == "claude"
    assert result["worker"] == f"claude:{A}"
    assert result["request_id"] == gate["id"]
    assert isinstance(result["reply"], str) and result["reply"].strip()
    assert store.record(A).get("lastSettledTurn", {}).get("outcome") == "completed"


def test_codex_ambiguous_ack_never_enters_wait_or_reissues_decision(
    original_codex, monkeypatch,
):
    from ccx.errors import SendIndeterminate
    store, _cwd, wid, _tid = original_codex
    gate, turn = codex_gate(store, wid)
    worker = WorkerClient()
    seen = []

    def lost_reply(target, operation, request_id, timeout=None, *,
                   response_id=None, expected_turn_id=None):
        seen.append((target, request_id, expected_turn_id))
        raise SendIndeterminate("original native decision ACK lost after write")
    monkeypatch.setattr(worker._codex, "_decision", lost_reply)
    monkeypatch.setattr(
        worker, "wait",
        lambda *_a, **_kw: pytest.fail("uncertain input cannot start waited success"),
    )
    with pytest.raises(SendIndeterminate, match="ACK lost"):
        worker.approve(wid, request_id=gate, wait=True, timeout=3)
    assert seen == [(f"codex:{wid}", gate, turn)]
