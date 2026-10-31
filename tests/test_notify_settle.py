"""N3 A2: original v3 Codex final notifications and recursive graph."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.select as notify_select
import ccx.notify.settle as notify_settle

from argparse import Namespace
import json

import pytest

from ccx.cli import codex as codex_commands
from ccx.codex import control as codex_control
from ccx.codex import delivery as codex_delivery
from ccx import constants
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.errors import InvalidRequest
from test_generation_store import A, B, C, short_root
from test_notify_registration import machine_originals
from test_notify_standalone import _managed_caller, _native_caller
from test_notify_ext import FakeControl


def _parent(store, wid):
    return {"kind": "ccx-codex", "workerId": wid,
            "threadId": store.record(wid)["native"]["threadId"],
            "generation": store.generation}


def _stable():
    return {"kind": "codex-stable", "selectionMode": "status-only",
            "selectedTurnRef": None, "stableState": "idle"}


def _descendant(client, store, tmp_path):
    assert client.create(cwd=str(tmp_path), worker_id=C).worker_id == C
    token = "a2-prepared-descendant"
    codex_delivery.prepare("codex:" + C, token, "child original", mode="new_turn_only")
    return parent_notify.create_registration(
        parent=_parent(store, B), worker_engine="codex", worker_id=C,
        worker_name=None, operation={"kind": "codex-turn", "deliveryId": token},
        source="send")


def _arm(monkeypatch, store, *, parent_id=A, parent_override=None, operation=None):
    parent = parent_override or _parent(store, parent_id)
    return parent_notify.create_registration(
        parent=parent, worker_engine="codex", worker_id=B, worker_name=None,
        operation=operation or _stable(), source="notify", settle=True)



def test_a2_cli_independent_standalone_and_internal_send_mode_sticky(
    machine_originals, monkeypatch, capsys,
):
    client, store, trace, _daemon = machine_originals
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_k: None)
    assert client.send("codex:" + B, "A2 original idle", delivery_id="a2-cli-original",
                       wait=True, timeout=15).turn_id
    _managed_caller(monkeypatch, store)
    before = trace.read_text().count("RPC turn/start")
    codex_commands.cmd_notify(Namespace(target="codex:" + B, settle=True, json=True))
    public = json.loads(capsys.readouterr().out)
    assert public["armed"] and public["settle"] is True
    assert public["source"] == "notify" and public["parent"] == "codex:" + A
    reg = parent_notify.read_registration(public["event_id"])
    assert reg["settlementMode"] == "orchestration"
    assert reg["registrationKeySha256"] is None
    assert public["operation_kind"] == "codex-observed-turn"
    _native_caller(monkeypatch)
    codex_commands.cmd_notify(Namespace(target="codex:" + B, settle=True, json=True))
    other = json.loads(capsys.readouterr().out)
    assert other["event_id"] != public["event_id"]
    assert other["parent_kind"] == "codex-native-thread"
    assert trace.read_text().count("RPC turn/start") == before

    # One prepared SEND semantic ID always resolves to ONE UUIDv5 event,
    # whether retry asks for the same mode or an incompatible mode.
    token = "a2-owned-send-id"
    codex_delivery.prepare("codex:" + B, token, "same child input", mode="new_turn_only")
    op = {"kind": "codex-turn", "deliveryId": token}
    send_reg = parent_notify.create_registration(
        parent=_parent(store, A), worker_engine="codex", worker_id=B,
        worker_name=None, operation=op, source="send", settle=True)
    assert send_reg["settlementMode"] == "orchestration"
    repeated = parent_notify.create_registration(
        parent=_parent(store, A), worker_engine="codex", worker_id=B,
        worker_name=None, operation=op, source="send", settle=True)
    assert repeated["eventId"] == send_reg["eventId"]
    with pytest.raises(InvalidRequest, match="different parent or input"):
        parent_notify.create_registration(
            parent=_parent(store, A), worker_engine="codex", worker_id=B,
            worker_name=None, operation=op, source="send", settle=False)
    with pytest.raises(InvalidRequest, match="notification type is unsupported"):
        parent_notify.create_registration(
            parent=_parent(store, A), worker_engine="claude", worker_id=B,
            worker_name=None, operation={"kind": "claude-stable"},
            source="notify", settle=True)
    with pytest.raises(InvalidRequest, match="generation changed"):
        parent_notify.create_registration(
            parent={**_parent(store, A), "generation": "g-foreign"},
            worker_engine="codex", worker_id=B, worker_name=None,
            operation=_stable(), source="notify", settle=True)
    assert trace.read_text().count("RPC turn/start") == before


def test_a2_cycle_lock_self_and_opposing_managed_edges_pre_event(
    machine_originals,
):
    _client, store, trace, _daemon = machine_originals
    before = trace.read_text()
    with pytest.raises((InvalidRequest, parent_notify.SessionUnavailable)):
        parent_notify.create_registration(
            parent=_parent(store, B), worker_engine="codex", worker_id=B,
            worker_name=None, operation=_stable(), source="notify", settle=True)
    assert parent_notify.iter_registrations() == []
    # First B→A is allowed; attempting A→B closes the same native graph cycle.
    first = parent_notify.create_registration(
        parent=_parent(store, B), worker_engine="codex", worker_id=A,
        worker_name=None, operation=_stable(), source="notify", settle=True)
    with pytest.raises(InvalidRequest, match="cycle"):
        _arm(None, store, parent_id=A)
    assert [r["eventId"] for r in parent_notify.iter_registrations()] == [first["eventId"]]
    assert trace.read_text() == before


@pytest.mark.parametrize("original", ["status-only", "receipt"])
def test_a2_original_operation_frozen_while_descendants_settle(
    machine_originals, tmp_path, monkeypatch, original,
):
    client, store, trace, _daemon = machine_originals
    assert client.send("codex:" + B,
                       "R1 native assistant" if original == "receipt" else "A2 idle warmup",
                       delivery_id="a2-original-r1" if original == "receipt" else "a2-warm-idle",
                       wait=True, timeout=15).turn_id
    if original == "receipt":
        selection = notify_select.select_current_turn("codex:" + B)
        assert selection["operation"]["selectionMode"] == "last-settled"
        op = selection["operation"]
    else:
        op = _stable()
    child = _descendant(client, store, tmp_path)
    reg = _arm(monkeypatch, store, operation=op)
    first = notify_settle.settlement_for_registration(reg)
    assert first is None  # original R1 is ready, but C's notification is live
    frozen = parent_notify.read_registration(reg["eventId"])["operationSettlement"]
    assert frozen["status"] == ("completed" if original == "receipt" else "idle")
    expected_report = frozen["report"]
    if original == "receipt":
        assert expected_report == "Fake Codex completed: R1 native assistant"
        assert frozen["turnId"] == op["turnId"]
    else:
        assert expected_report is None
    store.mutate_record("codex:" + B, patches={"lastReport": {
        "turnId": "later-R2", "text": "WRONG R2 later reply"}})
    assert notify_settle.settlement_for_registration(reg) is None
    parent_notify.cancel_registration(child["eventId"], "cancelled descendant")
    final = notify_settle.settlement_for_registration(reg)
    assert final["settlementVerdict"] == "settled"
    assert final["status"] == frozen["status"]
    assert final["report"] == expected_report
    assert parent_notify.read_registration(reg["eventId"])["operationSettlement"] == frozen
    assert "WRONG R2" not in str(final)


def test_a2_recursive_original_wake_gap_and_undeliverable(
    machine_originals, tmp_path,
):
    client, store, _trace, _daemon = machine_originals
    assert client.send("codex:" + B, "A2 stable for graph", delivery_id="a2-graph-warm",
                       wait=True, timeout=15).turn_id
    child = _descendant(client, store, tmp_path)
    reg = _arm(None, store)
    assert notify_settle.settlement_for_registration(reg) is None
    parent_notify.mutate_registration(child["eventId"], lambda row: row.update(
        state="undeliverable", lastError="native parent absent"))
    bad = notify_settle.settlement_for_registration(reg)
    assert bad["status"] == bad["settlementVerdict"] == "undeliverable_child"
    assert bad["report"] is None


@pytest.mark.parametrize("parent_kind", ["external", "managed"])
def test_a2_recursive_final_only_quiet_until_descendants_resolve(
    machine_originals, tmp_path, monkeypatch, parent_kind,
):
    client, store, trace, _daemon_rpc = machine_originals
    assert client.send("codex:" + B, "A2 original settled child",
                       delivery_id="a2-final-only-warm", wait=True, timeout=15).turn_id
    descendant = _descendant(client, store, tmp_path)
    control = FakeControl() if parent_kind == "external" else None
    if control is not None:
        monkeypatch.setattr(codex_control, "CodexControlClient", lambda: control)
    parent = (_native_caller(monkeypatch) if control is not None
              else _managed_caller(monkeypatch, store, A))
    reg = _arm(monkeypatch, store, parent_override=parent)
    assert notify_settle.settlement_for_registration(reg) is None
    frozen = parent_notify.read_registration(reg["eventId"])["operationSettlement"]
    assert frozen["status"] == "idle"
    before = trace.read_text().count("RPC turn/start")
    notifyd.process_registration(parent_notify.read_registration(reg["eventId"]))
    pending = parent_notify.read_registration(reg["eventId"])
    assert pending["state"] == "armed" and pending.get("payload") is None
    assert pending.get("deliveryAttemptedAtMs") is None
    assert (len(control.methods("turn/start")) if control is not None
            else trace.read_text().count("RPC turn/start") - before) == 0

    parent_notify.cancel_registration(descendant["eventId"], "descendant resolved")
    notifyd.process_registration(pending)
    final = parent_notify.read_registration(reg["eventId"])
    assert final["state"] == "delivered"
    assert final["payload"]["phase"] == "settled"
    assert final["payload"]["settlementVerdict"] == "settled"
    assert final["payload"]["deliveryId"] == reg["eventId"] + ":settled"
    assert final["payload"]["report"] is None
    notifyd.process_registration(final)
    if control is not None:
        assert len(control.methods("turn/start")) == 1
        assert [payload["phase"] for _, payload in control.placed] == ["settled"]
    else:
        assert trace.read_text().count("RPC turn/start") == before + 1


def test_a2_recursion_wake_grace_persisted_and_active_parent_resets_clock(
    machine_originals,
):
    client, store, _trace, _daemon = machine_originals
    assert client.send("codex:" + B, "native settled before graph",
                       delivery_id="a2-pending-wake-warm", wait=True, timeout=15).turn_id
    reg = _arm(None, store)
    token = "a2-wake-target-child"
    codex_delivery.prepare("codex:" + A, token, "original child", mode="new_turn_only")
    descendant = parent_notify.create_registration(
        parent=_parent(store, B), worker_engine="codex", worker_id=A,
        worker_name=None, operation={"kind": "codex-turn", "deliveryId": token},
        source="send")
    parent_notify.mutate_registration(descendant["eventId"], lambda row: row.update(
        state="delivered", deliveredAtMs=1, delivery={"turnId": "unobserved-B-wake"}))
    assert notify_settle.settlement_for_registration(reg) is None
    original_marker = (parent_notify.read_registration(reg["eventId"])
                       .get("settlementState") or {})
    assert type(original_marker.get("pendingWakeSinceMs")) is int
    def age(row):
        row["settlementState"]["pendingWakeSinceMs"] = parent_notify.now_ms() - 121000
    parent_notify.mutate_registration(reg["eventId"], age)
    store.patch_lifecycle(B, changes={"state": "busy", "activeTurnId": "running-B-wake"})
    assert notify_settle.settlement_for_registration(reg) is None
    refreshed = parent_notify.read_registration(reg["eventId"])
    assert "pendingWakeSinceMs" not in (refreshed.get("settlementState") or {})
    store.patch_lifecycle(B, changes={"state": "idle", "activeTurnId": None})
    assert notify_settle.settlement_for_registration(reg) is None
    parent_notify.mutate_registration(reg["eventId"], age)
    gap = notify_settle.settlement_for_registration(reg)
    assert gap["status"] == gap["settlementVerdict"] == "delivery_gap"
    assert gap["report"] is None


def test_a2_receiptless_codex_native_r_freezes_one_actual_original_turn(
    machine_originals, tmp_path, monkeypatch,
):
    client, store, trace, _daemon = machine_originals
    assert client.send("codex:" + B, "warm original parent for native wake",
                       delivery_id="a2-native-wake-parent-warm", wait=True, timeout=15).turn_id
    assert client.create(cwd=str(tmp_path), worker_id=C).worker_id == C
    assert client.send("codex:" + C, "native child final",
                       delivery_id="a2-native-child-r", wait=True, timeout=15).turn_id
    original = notify_select.select_current_turn("codex:" + C)["operation"]
    lower = parent_notify.create_registration(
        parent=_parent(store, B), worker_engine="codex", worker_id=C,
        worker_name=None, operation=original, source="notify")
    previous = (store.record(B).get("lastTurn") or {}).get("turnId")
    notifyd.process_registration(lower)
    assert parent_notify.read_registration(lower["eventId"])["state"] == "delivered"
    exact = notify_select.select_current_turn("codex:" + B)["operation"]
    native_r = exact["turnId"]
    assert exact["nativeProof"] == "original-native-turn" and native_r != previous
    assert codex_delivery.receipt_for_turn("codex:" + B, native_r) is None
    outer = _arm(monkeypatch, store, operation=exact)
    value = notify_settle.settlement_for_registration(outer)
    assert value["settlementVerdict"] == "settled"
    assert value["turnId"] == native_r and value["report"] is None
    assert parent_notify.read_registration(outer["eventId"])["operationSettlement"]["turnId"] == native_r
    before = trace.read_text().count("RPC turn/start")
    assert notify_settle.settlement_for_registration(outer)["turnId"] == native_r
    assert trace.read_text().count("RPC turn/start") == before


def test_a2_recursive_strict_inventory_failure_does_not_claim_settled(
    machine_originals, tmp_path,
):
    client, store, trace, _daemon = machine_originals
    assert client.send("codex:" + B, "positive original idle",
                       delivery_id="a2-inventory-idle", wait=True, timeout=15).turn_id
    outer = _arm(None, store)
    directory = parent_notify.registrations_dir()
    damaged = {directory / "a2-unreadable-current.json": b"{broken-registration",
               directory / "a2-nonobject-current.json": b"[1, 2]"}
    for path, data in damaged.items():
        path.write_bytes(data)
    before = trace.read_text()
    incomplete = notify_settle.settlement_for_registration(outer)
    assert incomplete["settlementVerdict"] == incomplete["status"] == "inventory_incomplete"
    for path in damaged:
        assert path.is_absolute() and str(path) in incomplete["error"]
    assert incomplete["error"].endswith(
        f"Move these files out of {directory} to unblock settle.")
    assert {path: path.read_bytes() for path in damaged} == damaged  # untouched
    assert trace.read_text() == before
    assert parent_notify.read_registration(outer["eventId"])["operationSettlement"]["status"] == "idle"
    for path in damaged:  # The operator's move-aside, outside the registrations dir.
        path.rename(tmp_path / path.name)
    final = notify_settle.settlement_for_registration(outer)
    assert final["settlementVerdict"] == "settled" and final["status"] == "idle"
