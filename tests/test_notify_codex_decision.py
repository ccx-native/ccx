"""N4 original Codex approval notification: one gate/effect, one source-frozen wake."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.settle as notify_settle
import ccx.ops.wait as ops_wait

from argparse import Namespace
import json
import uuid

import pytest

from ccx.codex import client as codex_client
from ccx.cli import codex as codex_commands
from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.errors import OperationConflict, SessionUnavailable
from ccx.cli.parser import build_parser
from ccx.store.store import GenerationStore
from test_generation_store import A, B, C, short_root
from test_notify_registration import machine_originals, _n1c_managed_parent
from test_notify_ext import native_caller


def _args(gate, *, target="codex:" + B, notified=True, settle=True):
    return Namespace(target=target, request_id=gate, notify_parent=notified,
                     settle=settle, no_wait=False, json=True)


def _parent(store, worker=A):
    return {"kind": "ccx-codex", "workerId": worker,
            "threadId": store.record(worker)["native"]["threadId"],
            "generation": store.generation}


def _gate(store, gate_id, turn="original-native-decision-R1"):
    store.mutate_record("codex:" + B, patches={
        "pendingApprovals": [{
            "gateId": gate_id, "turnId": turn, "requestId": 0,
            "runtimeId": "original-runtime", "kind": "commandExecution",
        }],
        "lifecycle.state": "waiting", "lifecycle.activeTurnId": turn,
    })
    return turn


def _served(monkeypatch):
    # Existing V1/native fake tests reset this lazy client per disposable
    # ACTIVE generation; never reuse its original store across tests.
    protocol_ops._CODEX_CLIENT.value = None
    monkeypatch.setattr(codex_runtime, "daemon_rpc",
                        lambda *_a, **_kw: {
                            "ok": True, "transport": "served", "loaded": True,
                        })
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon",
                        lambda **_kw: None)


def _ack(store, called, *, positive=True):
    def native(worker_id, msg, timeout=10):
        assert worker_id == B
        assert msg["gateId"] and msg["responseId"]
        assert msg["op"] in ("approve", "reject")
        key = f"full-uuid-codex-input:{store.generation}:{B}:{msg['gateId']}"
        event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "ccx-notify:" + key))
        reg = parent_notify.read_registration(event_id)
        assert reg is not None and reg["state"] == "armed"
        assert reg["source"] == "input" and reg["worker"]["workerId"] == B
        assert reg["operation"]["responseId"] == msg["responseId"]
        assert reg["operation"]["decision"] == msg["op"]
        assert reg["operation"]["turnId"] == store.record(B)["lifecycle"]["activeTurnId"]
        assert reg["operation"]["nativeSourceId"] == store.record(B)["native"]["threadId"]
        assert json.loads(parent_notify.registration_path(event_id).read_text()) == reg
        original = store.read_receipt("codex:" + B, "answer", msg["responseId"])
        assert original["state"] == "submitted-unknown"
        assert original["intentSha256"] == reg["operation"]["intentSha256"]
        called.append((dict(msg), event_id))
        if not positive:
            return {"ok": False, "transport": "served", "definitive": True,
                    "loaded": True, "error": "native gate definitively gone"}
        return {"ok": True, "gateId": msg["gateId"], "workerId": B,
                "threadId": store.record(B)["native"]["threadId"],
                "turnId": reg["operation"]["turnId"],
                "decision": "accept" if msg["op"] == "approve" else "decline"}
    return native


@pytest.mark.parametrize("parent_kind,decision", [
    ("managed", "approve"), ("interactive", "reject"),
])
def test_decision_one_gate_and_durable_event_precedes_one_native_effect(
    machine_originals, monkeypatch, capsys, parent_kind, decision,
):
    _client, store, trace, _daemon = machine_originals
    gate = "n4-original-gate-" + parent_kind
    _gate(store, gate)
    parent = (_parent(store) if parent_kind == "managed" else
              native_caller(monkeypatch))
    if parent_kind == "managed":
        _n1c_managed_parent(monkeypatch, store)
    _served(monkeypatch)
    native_calls = []
    monkeypatch.setattr(codex_client, "rpc", _ack(store, native_calls))
    baseline = trace.read_text().count("RPC turn/start")
    # Invalid gate and unbound caller are pre-effect and create no row.
    invalid = _args("different-native-gate")
    with pytest.raises(SystemExit):
        codex_commands.cmd_approve(invalid)
    capsys.readouterr()
    assert parent_notify.iter_registrations() == []
    assert not native_calls
    assert trace.read_text().count("RPC turn/start") == baseline

    command = codex_commands.cmd_approve if decision == "approve" else codex_commands.cmd_reject
    # Parse the real optional CLI flag for notified/recursive settle.
    if parent_kind == "managed":
        parsed = build_parser().parse_args([
            decision, f"codex:{B}", "--notify-parent", "--settle", "--json",
        ])
        assert parsed.request_id is None
        command(parsed)
    else:
        command(_args(gate))
    public = json.loads(capsys.readouterr().out)
    assert public["worker"] == "codex:" + B
    assert public["request_id"] == gate
    assert public["status"] == "accepted" and public["decision_sent"] is True
    assert public["decision"] == ("accept" if decision == "approve" else "decline")
    assert public["notify_parent"]["source"] == "input"
    assert public["notify_parent"]["settle"] is True
    assert len(native_calls) == 1
    assert public["notify_parent"]["event_id"] == native_calls[0][1]
    assert parent_notify.read_registration(native_calls[0][1])["parent"] == parent
    assert trace.read_text().count("RPC turn/start") == baseline
    assert store.read_receipt("codex:" + B, "answer", public["response_id"])["state"] == "accepted"


@pytest.mark.parametrize("operation,no_wait", [("approve", False), ("reject", True)])
def test_omitted_codex_cli_immediate_one_gate_and_ambiguous_pre_effect_refusal(
    machine_originals, monkeypatch, capsys, operation, no_wait,
):
    _client, store, trace, _daemon = machine_originals
    _served(monkeypatch)
    gate = "cli-current-only-gate"
    original_turn = _gate(store, gate)
    native_calls = []
    def native(worker_id, msg, timeout=10):
        native_calls.append((worker_id, dict(msg)))
        assert msg == {"op": operation, "gateId": gate}
        return {"ok": True, "gateId": gate, "workerId": B,
                "threadId": store.record(B)["native"]["threadId"],
                "turnId": original_turn,
                "decision": "accept" if operation == "approve" else "decline"}
    monkeypatch.setattr(codex_client, "rpc", native)
    args = build_parser().parse_args([
        operation, f"codex:{B}",
        *(("--no-wait",) if no_wait else ()), "--json",
    ])
    assert args.request_id is None
    command = codex_commands.cmd_approve if operation == "approve" else codex_commands.cmd_reject
    before = trace.read_text().count("RPC turn/start")
    command(args)
    output = json.loads(capsys.readouterr().out)
    assert output["request_id"] == gate and output["decision_sent"] is True
    assert output["decision"] == ("accept" if operation == "approve" else "decline")
    assert "notify_parent" not in output
    assert native_calls == [(B, {"op": operation, "gateId": gate})]
    assert not parent_notify.iter_registrations()
    assert trace.read_text().count("RPC turn/start") == before

    # No new or guessed original: even a native requestId cannot stand in for gateId.
    for approvals in ([], [{"requestId": "recycled-native"}], [
        {"gateId": gate, "turnId": original_turn},
        {"gateId": "second-unique-gate", "turnId": original_turn},
    ]):
        store.mutate_record("codex:" + B, patches={"pendingApprovals": approvals})
        with pytest.raises(SystemExit) as invalid:
            command(args)
        # No actionable current gate is a state refusal (5); selecting among
        # multiple current gates still requires an explicit ID (bad input, 1).
        assert invalid.value.code == (1 if len(approvals) > 1 else 5)
        assert not parent_notify.iter_registrations()
        assert len(native_calls) == 1
        capsys.readouterr()
    # Invalid settle remains an error even when there is no pending gate.
    invalid_settle = _args(None, notified=False, settle=True)
    with pytest.raises(SystemExit) as invalid:
        command(invalid_settle)
    assert invalid.value.code == 1
    assert "--settle requires --notify-parent" in capsys.readouterr().err
    assert len(native_calls) == 1


def test_omitted_codex_notify_zero_or_multi_gate_has_no_parent_or_native_effect(
    machine_originals, monkeypatch, capsys,
):
    _client, store, _trace, _daemon = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    _served(monkeypatch)
    monkeypatch.setattr(codex_commands, "_prepare_parent_notification",
                        lambda *_a: pytest.fail("parent prepared before gate bound"))
    monkeypatch.setattr(codex_client, "rpc",
                        lambda *_a, **_kw: pytest.fail("native input before gate bound"))
    for approvals in ([], [
        {"gateId": "a", "turnId": "R1"}, {"gateId": "b", "turnId": "R1"},
    ], [{"requestId": "recycled-native", "turnId": "R1"}]):
        store.mutate_record("codex:" + B, patches={"pendingApprovals": approvals})
        with pytest.raises(SystemExit) as invalid:
            codex_commands.cmd_reject(_args(None))
        assert invalid.value.code == (1 if len(approvals) > 1 else 5)
        assert not parent_notify.iter_registrations()
        capsys.readouterr()


def test_decision_keyed_replay_unknown_refusal_and_sticky_cancel(
    machine_originals, monkeypatch, capsys,
):
    _client, store, trace, _daemon = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    _served(monkeypatch)
    gate = "n4-unknown-gate"
    _gate(store, gate)
    start = trace.read_text().count("RPC turn/start")
    calls = []
    def lost_ack(worker, msg, timeout=10):
        calls.append(dict(msg))
        raise TimeoutError("original Codex serverRequest ACK lost AFTER submitted")
    monkeypatch.setattr(codex_client, "rpc", lost_ack)
    with pytest.raises(SystemExit) as uncertain:
        codex_commands.cmd_approve(_args(None))  # initial omitted call freezes original gate
    args = _args(gate)  # later lost-ACK replay names the captured original explicitly
    assert uncertain.value.code == 6
    unknown = json.loads(capsys.readouterr().out)
    assert unknown["status"] in {"submitted", "unknown"}
    token = unknown["response_id"]
    assert store.read_receipt("codex:" + B, "answer", token)["state"] == "submitted-unknown"
    event_id = unknown["notify_parent"]["event_id"]
    assert parent_notify.read_registration(event_id)["state"] == "armed"
    assert len(calls) == 1

    # The existing original native receipt fences all same-ID retries,
    # even after the socket is restored. Opposite decision shares the key.
    monkeypatch.setattr(codex_client, "rpc", _ack(store, []))
    with pytest.raises(SystemExit):
        codex_commands.cmd_approve(args)
    again = json.loads(capsys.readouterr().out)
    assert again["response_id"] == token
    assert again["notify_parent"]["event_id"] == event_id
    assert len(calls) == 1
    with pytest.raises(SystemExit) as changed:
        codex_commands.cmd_reject(args)
    assert changed.value.code == 5
    capsys.readouterr()
    with pytest.raises(SystemExit) as mode:
        codex_commands.cmd_approve(_args(gate, settle=False))
    assert mode.value.code == 5
    capsys.readouterr()
    native_caller(monkeypatch)
    with pytest.raises(SystemExit):
        codex_commands.cmd_approve(args)
    capsys.readouterr()
    assert len(calls) == 1
    assert trace.read_text().count("RPC turn/start") == start
    assert [r["eventId"] for r in parent_notify.iter_registrations()] == [event_id]

    # A different original gate has its own one receipt. A definite native
    # no-effect rejection cancels its original event, never the child turn.
    _n1c_managed_parent(monkeypatch, store)
    other_gate = "n4-definite-refusal"
    _gate(store, other_gate)
    rejected_calls = []
    monkeypatch.setattr(codex_client, "rpc",
                        _ack(store, rejected_calls, positive=False))
    with pytest.raises(SystemExit) as refused:
        codex_commands.cmd_approve(_args(other_gate))
    assert refused.value.code == 5
    refusal = json.loads(capsys.readouterr().out)
    assert refusal["status"] == "rejected"
    assert refusal["notify_parent"]["state"] == "cancelled"
    cancelled = parent_notify.read_registration(refusal["notify_parent"]["event_id"])
    assert cancelled["state"] == "cancelled"
    assert store.read_receipt("codex:" + B, "answer", refusal["response_id"])["state"] == "rejected"
    assert len(rejected_calls) == 1


def test_decision_original_gate_suppresses_wait_then_same_r_waits_and_exact_r_settles(
    machine_originals, monkeypatch, capsys,
):
    _client, store, trace, _daemon = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    _served(monkeypatch)
    native_calls = []
    monkeypatch.setattr(codex_client, "rpc", _ack(store, native_calls))
    turn = _gate(store, "n4-original-still-pending")
    from ccx.codex import evidence as codex_evidence
    journal = codex_evidence.EvidenceJournal(B)
    tid = store.record(B)["native"]["threadId"]
    journal.record("turn/started", {"turn": {"id": turn}}, thread_id=tid)
    journal.close()
    codex_commands.cmd_approve(_args("n4-original-still-pending", settle=False))
    outcome = json.loads(capsys.readouterr().out)
    reg = parent_notify.read_registration(outcome["notify_parent"]["event_id"])
    assert notify_settle.settlement_for_registration(reg) is None
    assert len(native_calls) == 1

    # A new approval arrived on precisely the same original native R.
    newer = "n4-new-same-R-actionable"
    _gate(store, newer, turn=turn)
    from ccx.ops import common as ops
    def waiting(worker, *, timeout=None, expected_generation=None):
        assert worker == "codex:" + B
        return {"worker": worker, "state": "waiting",
                "active_turn_ref": f"ccx-turn:codex:v1:{turn}",
                "pendingApprovals": store.record(B)["pendingApprovals"]}
    with monkeypatch.context() as patched:
        patched.setattr(ops_wait, "wait_worker", waiting)
        observed = notify_settle.settlement_for_registration(reg)
    assert observed["status"] == "waiting" and observed["turnStatus"] == "waiting"
    assert observed["report"] is None
    assert observed["waitingFor"][0]["gateId"] == newer

    # Native R1 later completes. Its positive source ID/turn excludes R2.
    journal = codex_evidence.EvidenceJournal(B)
    journal.record("turn/completed",
                   {"turn": {"id": turn, "status": "completed"}}, thread_id=tid)
    journal.close()
    from ccx.codex import runtime as codex_runtime
    def exact_original_history(req, *, timeout=None, short=None):
        assert req["op"] == "thread-history"
        assert req["turnId"] == turn
        assert req["threadId"] == tid and req["workerId"] == B
        return {"ok": True, "transport": "served",
                "workerId": B, "threadId": tid,
                "thread": {"id": tid, "threadSource": f"ccx-worker:{B}",
                           "turns": [
                               {"id": turn, "status": "completed", "items": [
                                   {"type": "agentMessage", "phase": "final_answer",
                                    "text": "EXACT R1 after original approval"}]},
                               {"id": "unrelated-R2", "status": "completed", "items": [
                                   {"type": "agentMessage", "phase": "final_answer",
                                    "text": "WRONG later R2"}]},
                           ]}}
    monkeypatch.setattr(codex_runtime, "daemon_call", exact_original_history)
    settled = notify_settle.settlement_for_registration(reg)
    assert settled["status"] == "completed" and settled["turnId"] == turn
    assert settled["report"] == "EXACT R1 after original approval"
    assert "WRONG" not in str(settled)
    assert trace.read_text().count("RPC turn/start") == 1



def test_decision_recursive_r1_frozen_while_descendant_blocks_single_parent_wake(
    machine_originals, monkeypatch, capsys,
):
    client, store, trace, original_daemon_rpc = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    _served(monkeypatch)
    monkeypatch.setattr(codex_client, "rpc", _ack(store, []))
    turn = _gate(store, "n4-recursive-gate", turn="native-turn-1")
    codex_commands.cmd_approve(_args("n4-recursive-gate", settle=True))
    public = json.loads(capsys.readouterr().out)
    original = parent_notify.read_registration(public["notify_parent"]["event_id"])
    assert original["settlementMode"] == "orchestration"

    from ccx.codex import delivery as codex_delivery
    from test_worker_telemetry import _journal_turn
    _journal_turn(B, store.record(B)["native"]["threadId"])
    child = "cccccccc-1234-4000-8000-000000000099"
    # Only the original fake daemon can authorize an actual child thread.
    # The light-weight approval STATUS stub must not impersonate it.
    with monkeypatch.context() as creating:
        creating.setattr(codex_runtime, "daemon_rpc", original_daemon_rpc)
        assert client.create(
            cwd=str(store.record(B)["cwd"]), worker_id=child,
        ).worker_id == child
    token = "n4-recursive-descendant-pending"
    codex_delivery.prepare("codex:" + child, token, "descendant operation",
                           mode="new_turn_only")
    descendant = parent_notify.create_registration(
        parent=_parent(store, B), worker_engine="codex", worker_id=child,
        worker_name=None, operation={"kind": "codex-turn", "deliveryId": token},
        source="send",
    )
    native_id = store.record(B)["native"]["threadId"]
    def exact_original_history(req, *, timeout=None, short=None):
        assert req["op"] == "thread-history"
        assert req["turnId"] == turn
        assert req["workerId"] == B and req["threadId"] == native_id
        return {"ok": True, "transport": "served", "workerId": B,
                "threadId": native_id, "thread": {
                    "id": native_id, "threadSource": f"ccx-worker:{B}",
                    "turns": [
                        {"id": turn, "status": "completed", "items": [
                            {"type": "agentMessage", "phase": "final_answer",
                             "text": "N4 only original R1 final"}]},
                        {"id": "native-later-R2", "status": "completed", "items": [
                            {"type": "agentMessage", "phase": "final_answer",
                             "text": "WRONG later R2"}]},
                    ],
                }}
    monkeypatch.setattr(codex_runtime, "daemon_call", exact_original_history)
    store.mutate_record("codex:" + B, patches={
        "pendingApprovals": [], "lifecycle.state": "idle",
        "lifecycle.activeTurnId": None,
    })
    # A2 requires positive native idle/worker-source evidence, not a local
    # record's idle claim. Restore the original fake served daemon rather
    # than using the deliberately minimal pre-input status stub.
    monkeypatch.setattr(codex_runtime, "daemon_rpc", original_daemon_rpc)
    assert notify_settle.settlement_for_registration(original) is None
    frozen = parent_notify.read_registration(original["eventId"])["operationSettlement"]
    assert frozen["status"] == "completed" and frozen["turnId"] == turn
    assert frozen["report"] == "N4 only original R1 final"
    parent_notify.cancel_registration(descendant["eventId"], "resolved descendant")
    finished = notify_settle.settlement_for_registration(original)
    assert finished["settlementVerdict"] == "settled"
    assert finished["turnId"] == turn and finished["report"] == frozen["report"]
    assert "WRONG" not in str(finished)

    # The real fake original parent runtime, not the input-status stub, owns
    # the one machine wake. No second child approval or model SEND.
    monkeypatch.setattr(codex_runtime, "daemon_rpc", original_daemon_rpc)
    before = trace.read_text().count("RPC turn/start")
    notifyd.process_registration(parent_notify.read_registration(original["eventId"]))
    delivered = parent_notify.read_registration(original["eventId"])
    assert delivered["state"] == "delivered"
    assert delivered["payload"]["deliveryId"] == original["eventId"] + ":settled"
    assert delivered["payload"]["report"] == frozen["report"]
    assert trace.read_text().count("RPC turn/start") == before + 1
    notifyd.process_registration(delivered)
    assert trace.read_text().count("RPC turn/start") == before + 1
