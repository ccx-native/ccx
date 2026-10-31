"""N3 A1: one full-UUID read-only Codex settlement and strict v3 inventory."""
from __future__ import annotations
import ccx.ops.wait as ops_wait

from argparse import Namespace
import json

import pytest

from ccx.cli import codex as codex_commands
from ccx.codex import settle as codex_settle
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.errors import SessionUnavailable
from test_generation_store import A, B, short_root
from test_notify_registration import machine_originals, prepare


def _wait_args(*, timeout=0, wake_grace=0, json_output=True):
    return Namespace(target="codex:" + A, settle=True, timeout=timeout,
                     wake_grace=wake_grace, json=json_output)


def test_a1_real_fake_codex_idle_python_and_cli_stability(machine_originals, capsys):
    client, store, trace, _daemon_rpc = machine_originals
    before = trace.read_text()
    from ccx import CodexClient
    result = CodexClient().wait("codex:" + A, settle=True, timeout=2,
                                wake_grace=0)
    assert result.settle and result.settlement_verdict == "settled"
    assert result.settled is True and result.outstanding_count == 0
    assert result.pending_wake_count == 0
    # A plain stability wait ignores settle-specific wake grace.
    normal = CodexClient().wait("codex:" + A, settle=False,
                                wake_grace=4, timeout=2)
    assert normal.settle is False and normal.status == "idle"
    codex_commands.cmd_wait(_wait_args())
    output = json.loads(capsys.readouterr().out)
    assert output["worker"] == "codex:" + A
    assert output["settlement_verdict"] == "settled" and output["settled"]
    assert output["outstanding_count"] == 0 and output["pending_wake_count"] == 0
    args = _wait_args(json_output=False)
    codex_commands.cmd_wait(args)
    assert capsys.readouterr().out.strip() == "codex:" + A + "  settled"
    assert trace.read_text() == before  # pure waiter: no native start/load


@pytest.mark.parametrize("status,exit_code", [
    ("waiting", 4), ("dead", 3), ("crashed", 3), ("timeout", 6),
])
def test_a1_distinct_wait_status_and_exit(machine_originals, monkeypatch, capsys,
                                           status, exit_code):
    _client, store, _trace, _rpc = machine_originals
    if status == "waiting":
        monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_k: {
            "worker": "codex:" + A, "state": "waiting", "status": "waiting",
            "pendingApprovals": [{"requestId": "approved-original-gate"}]})
    elif status in {"dead", "crashed"}:
        store.patch_lifecycle(A, changes={"state": status})
    else:
        monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_k: {
            "worker": "codex:" + A, "state": "busy", "status": "timeout",
            "timed_out": True})
    args = _wait_args(timeout=0)
    with pytest.raises(SystemExit) as exc:
        codex_commands.cmd_wait(args)
    assert exc.value.code == exit_code
    output = json.loads(capsys.readouterr().out)
    assert output["settlement_verdict"] == (
        "approval_waiting" if status == "waiting" else status)
    assert not output["settled"]


def test_a1_valid_child_live_cancelled_and_undeliverable(machine_originals):
    _client, store, trace, _rpc = machine_originals
    reg, _parent = prepare(store, token="a1-original-child")
    assert parent_notify.iter_registrations(strict=True)[0]["eventId"] == reg["eventId"]
    original = codex_settle._record("codex:" + A, store)
    assert codex_settle._parent_registrations(original, [reg]) == [reg]
    before = trace.read_text()
    result = codex_settle.wait_for_settlement("codex:" + A, timeout=0)
    assert result["settlementVerdict"] == "timeout"
    assert result["outstandingCount"] == 1 and result["outstanding"][0]["worker"] == "codex:" + B
    assert result["registrationCount"] == 1
    parent_notify.cancel_registration(reg["eventId"], "cancelled child operation")
    done = codex_settle.wait_for_settlement("codex:" + A, timeout=1)
    assert done["settlementVerdict"] == "settled" and done["outstandingCount"] == 0
    # A cancelled descendant is terminal, but an undeliverable one is not a
    # successful graph. This is a registration outcome, not model completion.
    parent_notify.mutate_registration(reg["eventId"], lambda row: row.update(
        state="undeliverable"))
    bad = codex_settle.wait_for_settlement("codex:" + A, timeout=1)
    assert bad["settlementVerdict"] == "undeliverable_child"
    assert reg["eventId"] in bad["error"]
    assert trace.read_text() == before


@pytest.mark.parametrize("wake", ["exact-unsettled-native-wake", None])
def test_a1_delivered_parent_exact_wake_or_gap_never_time_borrow(machine_originals, wake):
    _client, store, trace, _rpc = machine_originals
    reg, _parent = prepare(store, token="a1-original-wake")
    parent_notify.mutate_registration(reg["eventId"], lambda row: row.update(
        state="delivered", deliveredAtMs=1,
        delivery={"turnId": wake}))
    before = trace.read_text()
    snapshot = codex_settle.settlement_progress("codex:" + A)
    assert snapshot["settlementVerdict"] == "pending"
    assert snapshot["pendingWakeCount"] == 1
    missing = codex_settle.wait_for_settlement("codex:" + A,
                                               timeout=1,
                                               wake_grace_s=0)
    assert missing["settlementVerdict"] == "delivery_gap" and missing["pendingWakeCount"] == 1
    # A later, DIFFERENT native turn cannot close this exact original wake.
    store.mutate_record("codex:" + A, patches={
        "lastTurn": {"turnId": "newer-other-turn", "status": "completed"},
        "lastReport": {"turnId": "newer-other-turn", "text": "WRONG latest reply"}})
    still = codex_settle.settlement_progress("codex:" + A)
    assert still["pendingWakeCount"] == 1 and still["settlementVerdict"] == "pending"
    parent_notify.mutate_registration(reg["eventId"], lambda row: row["delivery"].update(
        turnId="newer-other-turn"))
    matched = codex_settle.wait_for_settlement("codex:" + A, timeout=1)
    assert matched["settlementVerdict"] == "settled"
    # Informational lastMessage is exact to lastTurn, never the child reply.
    assert matched["lastMessage"] == "WRONG latest reply"
    assert trace.read_text() == before


def test_a1_running_exact_wake_is_pending_not_delivery_gap(machine_originals, monkeypatch):
    _client, store, trace, _rpc = machine_originals
    reg, _parent = prepare(store, token="a1-running-wake")
    wake = "running-exact-wake-turn"
    parent_notify.mutate_registration(reg["eventId"], lambda row: row.update(
        state="delivered", deliveredAtMs=1, delivery={"turnId": wake}))
    store.patch_lifecycle(A, changes={"state": "busy", "activeTurnId": wake})
    monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_k: {
        "worker": "codex:" + A, "state": "busy", "status": "timeout",
        "active_turn_ref": "ccx-turn:codex:v1:" + wake, "timed_out": True})
    before = trace.read_text()
    # A visibly running wake suspends the missing-wake grace (as main does):
    # zero grace must not turn a long legitimate parent turn into a gap.
    running = codex_settle.wait_for_settlement("codex:" + A, timeout=.5, wake_grace_s=0)
    assert running["settlementVerdict"] == "timeout", running
    assert running["pendingWakeCount"] == 1 and running["settled"] is False
    assert trace.read_text() == before


def test_a1_external_parent_is_return_address_not_managed_descendant(machine_originals):
    _client, store, trace, _rpc = machine_originals
    from ccx.codex import delivery as codex_delivery
    token = "a1-external-return"
    codex_delivery.prepare("codex:" + B, token, "ordinary child", mode="new_turn_only")
    reg = parent_notify.create_registration(
        parent={"kind": "codex-native-thread", "threadId": "external-return-thread"},
        worker_engine="codex", worker_id=B, worker_name=None,
        operation={"kind": "codex-turn", "deliveryId": token}, source="send")
    before = trace.read_text()
    assert parent_notify.iter_registrations(strict=True)[0]["eventId"] == reg["eventId"]
    observed = codex_settle.settlement_progress("codex:" + A)
    assert observed["settlementVerdict"] == "settled"
    assert observed["registrationCount"] == 0  # native return address is no worker
    assert trace.read_text() == before


@pytest.mark.parametrize("bad", ["unreadable", "nondict", "current-invalid",
                                 "malformed-generation", "old-parseable",
                                 "other-generation"])
def test_a1_i1_strict_inventory_and_old_inert(machine_originals, bad):
    _client, store, trace, _rpc = machine_originals
    path = parent_notify.registrations_dir() / "a1-broken-row.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if bad == "unreadable":
        path.write_text("{not valid JSON")
    elif bad == "nondict":
        path.write_text("[]")
    elif bad == "current-invalid":
        path.write_text(json.dumps({"version": 3, "generation": store.generation,
                                    "worker": {"engine": "codex", "workerId": A}}))
    elif bad == "malformed-generation":
        path.write_text(json.dumps({"version": 3, "generation": [store.generation]}))
    elif bad == "old-parseable":
        path.write_text(json.dumps({"version": 2, "worker": {"short": "old-short"}}))
    else:
        path.write_text(json.dumps({"version": 3, "generation": "g-previous-generation",
                                    "worker": {"engine": "codex", "workerId": A}}))
    before = trace.read_text()
    assert parent_notify.iter_registrations() == []  # existing permissive callers
    verdict = codex_settle.wait_for_settlement("codex:" + A, timeout=1)
    if bad in {"old-parseable", "other-generation"}:
        assert verdict["settlementVerdict"] == "settled"
        assert parent_notify.iter_registrations(strict=True) == []
    else:
        assert verdict["settlementVerdict"] == "inventory_incomplete"
        assert "a1-broken-row.json" in verdict["error"]
        assert verdict["ok"] is False
        with pytest.raises(SessionUnavailable, match="inventory incomplete"):
            parent_notify.iter_registrations(strict=True)
    assert trace.read_text() == before


def test_a1_pinned_journal_exact_old_wake_after_newer_turn(machine_originals):
    client, store, trace, _rpc = machine_originals
    from ccx.codex import delivery as codex_delivery
    token = "a1-original-journal-proof"
    codex_delivery.prepare("codex:" + B, token, "original child", mode="new_turn_only")
    reg = parent_notify.create_registration(
        parent={"kind": "ccx-codex", "workerId": A,
                "threadId": store.record(A)["native"]["threadId"],
                "generation": store.generation},
        worker_engine="codex", worker_id=B, worker_name=None,
        operation={"kind": "codex-turn", "deliveryId": token}, source="send")
    first = client.send("codex:" + A, "original wake", delivery_id="a1-wake-first",
                        wait=True, timeout=15)
    second = client.send("codex:" + A, "later unrelated turn",
                         delivery_id="a1-wake-later", wait=True, timeout=15)
    assert first.turn_id != second.turn_id
    parent_notify.mutate_registration(reg["eventId"], lambda row: row.update(
        state="delivered", delivery={"turnId": first.turn_id}, deliveredAtMs=1))
    before = trace.read_text().count("RPC turn/start")
    actual = codex_settle.wait_for_settlement("codex:" + A, timeout=1)
    assert actual["settlementVerdict"] == "settled"
    assert actual["pendingWakeCount"] == 0
    assert trace.read_text().count("RPC turn/start") == before
    # Sticky journal degradation cannot be rescued by a different latest R.
    store.mutate_record("codex:" + A, patches={"evidence": {
        "status": "degraded", "degradedReason": "lost native evidence"}})
    unavailable = codex_settle.settlement_progress("codex:" + A)
    assert unavailable["pendingWakeCount"] == 1
    assert unavailable["settlementVerdict"] == "pending"


@pytest.mark.parametrize("status,reason", [
    ("timeout", None), ("unavailable", "daemon liveness uncertain"),
])
def test_a1_recorded_idle_without_positive_runtime_proof_is_not_settled(
    machine_originals, monkeypatch, status, reason,
):
    _client, _store, trace, _rpc = machine_originals
    before = trace.read_text()
    monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_k: {
        "worker": "codex:" + A, "state": "idle", "status": status,
        "reason": reason, "active_turn_ref": None})
    result = codex_settle.wait_for_settlement("codex:" + A, timeout=0)
    assert result["settlementVerdict"] == "timeout"
    assert result["settled"] is False
    assert trace.read_text() == before
