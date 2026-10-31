"""N3 B: attach the ONE original Codex SEND to existing recursive notification."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.deliver as notify_deliver
import ccx.notify.settle as notify_settle
import ccx.ops.wait as ops_wait

import json
import time

import pytest

from ccx.cli import codex as codex_commands
from ccx.codex import delivery as codex_delivery
from ccx import constants
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.errors import InvalidRequest
from ccx.store.store import GenerationStore
from ccx.store.root import NativeEffectIndeterminate
from test_generation_store import A, B, C, short_root
from test_notify_registration import (
    machine_originals, _n1c_cli_send_args, _n1c_managed_parent,
)
from test_notify_ext import native_caller, FakeControl


def _args(token, prompt="N3 original child prompt", *, notify=True, settle=True):
    args = _n1c_cli_send_args(token, prompt=prompt, notify=notify)
    args.settle = settle
    return args


def _parent(store, wid):
    return {
        "kind": "ccx-codex", "workerId": wid,
        "threadId": store.record(wid)["native"]["threadId"],
        "generation": store.generation,
    }


@pytest.mark.parametrize("parent_kind", ["managed", "interactive"])
def test_send_settle_public_cli_fsyncs_original_event_before_native(
    machine_originals, monkeypatch, capsys, parent_kind,
):
    client, store, trace, _daemon = machine_originals
    parent = (_n1c_managed_parent(monkeypatch, store) if parent_kind == "managed"
              else native_caller(monkeypatch))
    if parent_kind == "managed":
        parent = _parent(store, A)
    start_count = trace.read_text().count("RPC turn/start")
    assert parent_notify.iter_registrations() == []
    # Original settle/no-parent refusal precedes any variable publication.
    with pytest.raises(SystemExit) as missing_parent:
        codex_commands.cmd_send(_args("n3-no-parent", notify=False))
    assert missing_parent.value.code == 1
    assert "--settle requires --notify-parent" in capsys.readouterr().err
    assert codex_delivery.read_receipt("codex:" + B, "n3-no-parent") is None
    args = _args("n3-vars-refusal")
    args.var = ["x=y"]
    with pytest.raises(SystemExit) as malformed_json:
        codex_commands.cmd_send(args)
    assert malformed_json.value.code == InvalidRequest.exit_code
    assert "valid JSON" in capsys.readouterr().err
    assert codex_delivery.read_receipt("codex:" + B, "n3-vars-refusal") is None
    assert parent_notify.iter_registrations() == []
    token = "n3-notified-" + parent_kind
    called = []
    begin = GenerationStore.begin_native_intent
    def original_fence(self, target, *, family, operation_id, **kwargs):
        if family == "codex-send" and operation_id == token:
            rows = parent_notify.iter_registrations()
            assert len(rows) == 1
            row = rows[0]
            assert row["version"] == 3 and row["state"] == "armed"
            assert row["worker"]["workerId"] == B and row["parent"] == parent
            assert row["settlementMode"] == "orchestration"
            op = row["operation"]
            assert op["kind"] == "codex-turn" and op["deliveryId"] == token
            assert op["requestedMode"] == "new_turn_only"
            assert op["callerMetadata"] is None
            assert json.loads(
                parent_notify.registration_path(row["eventId"]).read_text()
            ) == row
            original = codex_delivery.read_receipt("codex:" + B, token)
            assert original["state"] == "prepared"
            assert original["requestedMode"] == "new_turn_only"
            assert op["promptSha256"] == original["promptSha256"]
            assert op["clientUserMessageId"] == original["clientUserMessageId"]
            assert trace.read_text().count("RPC turn/start") == start_count
            called.append(row["eventId"])
        return begin(self, target, family=family, operation_id=operation_id, **kwargs)
    monkeypatch.setattr(GenerationStore, "begin_native_intent", original_fence)
    daemon_called = []
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon",
                        lambda **_kw: daemon_called.append(True))
    codex_commands.cmd_send(_args(token))
    result = json.loads(capsys.readouterr().out)
    assert daemon_called, "registration did not invoke the patched notify supervisor"
    assert len(called) == 1
    assert result["worker"] == "codex:" + B
    assert result["delivery_id"] == token and result["status"] == "accepted"
    assert result["notify_parent"]["event_id"] == called[0]
    assert result["notify_parent"]["settle"] is True
    # Child-turn settlement can precede the acceptance response. The parent
    # registration remains independently armed for recursive settlement.
    if result["settled"]:
        assert result["delivery"]["settlement"]["deliveryId"] == token
    assert parent_notify.read_registration(called[0])["settlementMode"] == "orchestration"
    assert "report" not in result
    assert trace.read_text().count("RPC turn/start") == start_count + 1


def test_send_settle_with_generated_child_id_preserves_recursive_mode(
    machine_originals, monkeypatch, capsys,
):
    from uuid import UUID

    _client, store, trace, _daemon = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    monkeypatch.setattr(notify_daemon, 'ensure_notify_daemon',
                        lambda **_kw: None)
    before = trace.read_text().count('RPC turn/start')
    codex_commands.cmd_send(_args(None, settle=True))
    output = json.loads(capsys.readouterr().out)
    token = output['delivery_id']
    assert UUID(token).version == 4
    assert trace.read_text().count('RPC turn/start') == before + 1
    reg = parent_notify.read_registration(output['notify_parent']['event_id'])
    assert reg['operation']['deliveryId'] == token
    assert reg['settlementMode'] == 'orchestration'
    assert output['notify_parent']['settle'] is True
    payload = notify_deliver.payload_for_registration(
        reg, {'status': 'completed', 'settlementVerdict': 'settled',
              'settlement': {'status': 'settled'}})
    assert payload['selectedOriginalDeliveryId'] == token
    assert payload['deliveryId'] == reg['eventId'] + ':settled'
    assert payload['settle'] is True


def test_send_settle_replay_conflicts_cycle_and_original_unknown(
    machine_originals, monkeypatch, capsys,
):
    client, store, trace, _daemon = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    prompt, token = "N3 immutable child R1", "n3-one-original-id"
    args = _args(token, prompt)
    start = trace.read_text().count("RPC turn/start")
    codex_commands.cmd_send(args)
    first = json.loads(capsys.readouterr().out)
    eid = first["notify_parent"]["event_id"]
    assert first["status"] == "accepted"
    assert parent_notify.read_registration(eid)["settlementMode"] == "orchestration"
    assert trace.read_text().count("RPC turn/start") == start + 1
    # Same id/parent/mode uses the old UUIDv5 row and never starts another turn.
    codex_commands.cmd_send(args)
    same = json.loads(capsys.readouterr().out)
    assert same["notify_parent"]["event_id"] == eid
    assert trace.read_text().count("RPC turn/start") == start + 1
    with pytest.raises(SystemExit) as changed_mode:
        codex_commands.cmd_send(_args(token, prompt, settle=False))
    assert changed_mode.value.code == 5
    capsys.readouterr()
    assert trace.read_text().count("RPC turn/start") == start + 1
    # Another positive original managed parent cannot take over this row.
    assert client.create(cwd=str(store.record(A)["cwd"]), worker_id=C).worker_id == C
    _n1c_managed_parent(monkeypatch, store, C)
    with pytest.raises(SystemExit) as foreign:
        codex_commands.cmd_send(args)
    assert foreign.value.code == 5
    capsys.readouterr()
    assert trace.read_text().count("RPC turn/start") == start + 1
    _n1c_managed_parent(monkeypatch, store, A)
    # A->B already armed; B->A would close a graph cycle. Registration
    # refusal may leave a PREPARED semantic receipt but no event/native SEND.
    _n1c_managed_parent(monkeypatch, store, B)
    before = trace.read_text().count("RPC turn/start")
    rows = {r["eventId"] for r in parent_notify.iter_registrations()}
    cycle = _args("n3-cycle-closing", "must not issue a turn")
    cycle.target = "codex:" + A
    with pytest.raises(SystemExit) as refused:
        codex_commands.cmd_send(cycle)
    assert refused.value.code == 1
    assert "cycle" in capsys.readouterr().err
    assert trace.read_text().count("RPC turn/start") == before
    assert {r["eventId"] for r in parent_notify.iter_registrations()} == rows
    pending = codex_delivery.read_receipt("codex:" + A, "n3-cycle-closing")
    assert pending is None or pending["state"] == "prepared"

    _n1c_managed_parent(monkeypatch, store, A)
    original_dispatch = GenerationStore.dispatch_native_intent_once
    def lost_ack_after_submit(self, prepared, *, bounded_submit, **kwargs):
        # Preserve the REAL fsynced submitted-unknown native intent; lose
        # exactly its original bounded native ACK before any second send.
        def lost(_budget):
            raise TimeoutError("original submitted-unknown ACK lost")
        return original_dispatch(
            self, prepared, bounded_submit=lost, **kwargs,
        )
    with monkeypatch.context() as injected:
        injected.setattr(
            GenerationStore, "dispatch_native_intent_once",
            lost_ack_after_submit,
        )
        uncertain = _args("n3-original-lost-ack")
        with pytest.raises(SystemExit) as lost:
            codex_commands.cmd_send(uncertain)
        assert lost.value.code == 6
        outcome = json.loads(capsys.readouterr().out)
        assert outcome["delivery_id"] == uncertain.delivery_id
        assert outcome["reconcile_required"] is True
        assert store.read_receipt(
            "codex:" + B, "codex-send", uncertain.delivery_id,
        )["state"] == "submitted-unknown"
        assert codex_delivery.read_receipt(
            "codex:" + B, uncertain.delivery_id,
        )["state"] == "submitted"
    native_before = trace.read_text().count("RPC turn/start")
    # Existing global intent guard must reject a replay, not dispatch again.
    with pytest.raises(SystemExit) as once:
        codex_commands.cmd_send(uncertain)
    assert once.value.code == 6
    again = json.loads(capsys.readouterr().out)
    assert again["delivery_id"] == uncertain.delivery_id
    assert trace.read_text().count("RPC turn/start") == native_before
    unknown_rows = [r for r in parent_notify.iter_registrations()
                    if r["operation"].get("deliveryId") == uncertain.delivery_id]
    assert len(unknown_rows) == 1 and unknown_rows[0]["settlementMode"] == "orchestration"
    # Original N1A policy: cancellation is sticky, even when an accepted
    # child turn is later observed again under that SAME native SEND ID.
    parent_notify.cancel_registration(eid, "cancel original wake, not child turn")
    codex_commands.cmd_send(args)
    cancelled = json.loads(capsys.readouterr().out)
    assert cancelled["notify_parent"]["event_id"] == eid
    assert cancelled["notify_parent"]["state"] == "cancelled"
    assert trace.read_text().count("RPC turn/start") == native_before


def test_send_settle_nested_descendant_keeps_frozen_original_r1(
    machine_originals, tmp_path, monkeypatch, capsys,
):
    client, store, trace, _daemon = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    token = "n3-nested-original-r1"
    start = trace.read_text().count("RPC turn/start")
    codex_commands.cmd_send(_args(token, "N3 child R1 must freeze"))
    sent = json.loads(capsys.readouterr().out)
    event_id = sent["notify_parent"]["event_id"]
    assert trace.read_text().count("RPC turn/start") == start + 1
    assert client.create(cwd=str(tmp_path), worker_id=C).worker_id == C
    descendant_token = "n3-descendant-original"
    codex_delivery.prepare(
        "codex:" + C, descendant_token, "descendant awaits cancellation",
        mode="new_turn_only",
    )
    descendant = parent_notify.create_registration(
        parent=_parent(store, B), worker_engine="codex", worker_id=C,
        worker_name=None,
        operation={"kind": "codex-turn", "deliveryId": descendant_token},
        source="send",
    )
    until = time.monotonic() + 8
    observed = None
    while time.monotonic() < until:
        observed = ops_wait.reconcile_turn("codex:" + B, delivery_id=token)
        if observed["status"] == "settled":
            break
        time.sleep(.035)
    assert observed["status"] == "settled", observed
    assert notify_settle.settlement_for_registration(
        parent_notify.read_registration(event_id)
    ) is None  # R1 completed, C is still in the descendant graph.
    frozen_row = parent_notify.read_registration(event_id)
    frozen = frozen_row["operationSettlement"]
    assert frozen["status"] == "completed"
    assert frozen["turnId"] == observed["delivery"]["turn_id"]
    assert frozen["report"] == "Fake Codex completed: N3 child R1 must freeze"
    assert frozen_row["settlementMode"] == "orchestration"
    store.mutate_record("codex:" + B, patches={
        "lastReport": {"turnId": "WRONG-LATER-R2", "text": "WRONG latest report"},
    })
    assert notify_settle.settlement_for_registration(
        parent_notify.read_registration(event_id)
    ) is None
    armed = parent_notify.read_registration(event_id)
    notifyd.process_registration(armed)
    assert parent_notify.read_registration(event_id)["state"] == "armed"
    assert trace.read_text().count("RPC turn/start") == start + 1
    parent_notify.cancel_registration(descendant["eventId"], "descendant completed")
    final = notify_settle.settlement_for_registration(
        parent_notify.read_registration(event_id)
    )
    assert final["settlementVerdict"] == "settled"
    assert final["report"] == frozen["report"]
    assert final["turnId"] == frozen["turnId"]
    assert "WRONG" not in str(final)
    notifyd.process_registration(parent_notify.read_registration(event_id))
    delivered = parent_notify.read_registration(event_id)
    assert delivered["state"] == "delivered"
    assert delivered["payload"]["report"] == frozen["report"]
    assert delivered["payload"]["deliveryId"] == event_id + ":settled"
    assert trace.read_text().count("RPC turn/start") == start + 2
    notifyd.process_registration(delivered)
    assert trace.read_text().count("RPC turn/start") == start + 2
