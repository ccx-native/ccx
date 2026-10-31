"""N3C public Codex new: one original created worker and durable parent wake."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.settle as notify_settle
import ccx.cli.commands.create as commands_create

import json
import uuid

import pytest

from ccx import CodexClient
from ccx.cli.commands import _common as commands
from ccx.codex import delivery as codex_delivery
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.cli import parser
from ccx.store.store import GenerationStore
from test_generation_store import A, B, C, short_root
from test_notify_registration import machine_originals, _n1c_managed_parent
from test_notify_ext import native_caller


def _new(cwd, worker=C, *, prompt=None, settle=False, notified=True, json_output=True):
    args = ["new", "--engine", "codex", "--cwd", str(cwd), "--worker-id", worker,
            "--start-timeout", "8"]
    if prompt is not None:
        args.append(prompt)
    if notified:
        args.append("--notify-parent")
    if settle:
        args.append("--settle")
    if json_output:
        args.append("--json")
    return parser.build_parser().parse_args(args)


def _parent(store, worker=A):
    return {"kind": "ccx-codex", "workerId": worker,
            "threadId": store.record(worker)["native"]["threadId"],
            "generation": store.generation}


@pytest.mark.parametrize("kind", ["managed", "interactive"])
def test_codex_new_prompted_one_original_keyed_wake_before_send(
    machine_originals, tmp_path, monkeypatch, capsys, kind,
):
    _client, store, trace, _daemon = machine_originals
    parent = (_parent(store) if kind == "managed"
              else {"kind": "codex-native-thread", "threadId": "n3c-interactive-return"})
    if kind == "managed":
        _n1c_managed_parent(monkeypatch, store)
    else:
        assert native_caller(monkeypatch, parent["threadId"]) == parent
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    before = trace.read_text()
    start_threads = before.count("RPC thread/start")
    start_turns = before.count("RPC turn/start")
    with pytest.raises(SystemExit) as solo:
        commands_create.cmd_new(_new(tmp_path, prompt="invalid", settle=True, notified=False))
    assert solo.value.code == 1
    assert "--settle requires --notify-parent" in capsys.readouterr().err
    with monkeypatch.context() as bad:
        for field in ("CCX_SELF_ENGINE", "CCX_SELF_WORKER_ID", "CODEX_THREAD_ID"):
            bad.delenv(field, raising=False)
        with pytest.raises(SystemExit):
            commands_create.cmd_new(_new(tmp_path, prompt="invalid"))
    capsys.readouterr()
    assert trace.read_text() == before
    assert len(store.list_workers(engine="codex", limit=None)) == 2

    begin = GenerationStore.begin_native_intent
    seen = []
    def source_fence(self, target, *, family, operation_id, **kwargs):
        if family == "codex-send" and operation_id == "create:initial":
            matches = [r for r in parent_notify.iter_registrations()
                       if r["worker"]["workerId"] == C]
            assert len(matches) == 1
            row = matches[0]
            assert row["source"] == "send" and row["parent"] == parent
            assert row["operation"]["kind"] == "codex-turn"
            assert row["operation"]["deliveryId"] == "create:initial"
            assert row["settlementMode"] == "orchestration"
            assert codex_delivery.read_receipt("codex:" + C, "create:initial")["state"] == "prepared"
            assert trace.read_text().count("RPC turn/start") == start_turns
            seen.append(row["eventId"])
        return begin(self, target, family=family, operation_id=operation_id, **kwargs)
    monkeypatch.setattr(GenerationStore, "begin_native_intent", source_fence)
    prompt = "N3C first true R1"
    commands_create.cmd_new(_new(tmp_path, prompt=prompt, settle=True))
    public = json.loads(capsys.readouterr().out)
    assert public["ok"] is True and public["worker"] == "codex:" + C
    assert public["delivery_id"] == "create:initial" and public["delivery_status"] == "accepted"
    assert public["notify_parent"]["source"] == "send"
    assert public["notify_parent"]["settle"] is True
    assert public["notify_parent"]["state"] == "armed"
    assert len(seen) == 1 and public["notify_parent"]["event_id"] == seen[0]
    assert len([r for r in parent_notify.iter_registrations()
                if r["worker"]["workerId"] == C]) == 1
    assert store.record(C)["native"]["threadId"] != C
    assert trace.read_text().count("RPC thread/start") == start_threads + 1
    assert trace.read_text().count("RPC turn/start") == start_turns + 1
    commands_create.cmd_new(_new(tmp_path, prompt=prompt, settle=True, json_output=False))
    plain = capsys.readouterr().out
    assert plain.splitlines()[0] == "codex:" + C
    assert "notify: armed " + seen[0] in plain
    assert trace.read_text().count("RPC thread/start") == start_threads + 1
    assert trace.read_text().count("RPC turn/start") == start_turns + 1


@pytest.mark.parametrize("settle", [False, True])
def test_codex_new_promptless_stable_sticky_replay_after_newer_turn(
    machine_originals, tmp_path, monkeypatch, capsys, settle,
):
    client, store, trace, _daemon = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    threads = trace.read_text().count("RPC thread/start")
    turns = trace.read_text().count("RPC turn/start")
    args = _new(tmp_path, settle=settle)
    commands_create.cmd_new(args)
    public = json.loads(capsys.readouterr().out)
    note = public["notify_parent"]
    assert public["worker"] == "codex:" + C and "delivery_id" not in public
    assert note["source"] == "new" and note["selection_mode"] == "status-only"
    assert note["delivery_id"] is None and note["turn_id"] is None
    eid = note["event_id"]
    row = parent_notify.read_registration(eid)
    assert row["parent"] == _parent(store)
    assert row["settlementMode"] == ("orchestration" if settle else None)
    assert row["operation"] == {"kind": "codex-stable", "expectTurn": False,
                                "selectionMode": "status-only", "stableState": "idle"}
    assert eid == str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        "ccx-notify:" + f"full-uuid-codex-new:{store.generation}:{C}:promptless",
    ))
    assert trace.read_text().count("RPC thread/start") == threads + 1
    assert trace.read_text().count("RPC turn/start") == turns
    assert codex_delivery.read_receipt("codex:" + C, "create:initial") is None
    # A numeric zero is not the immutable boolean expectTurn=False.
    from ccx.errors import InvalidRequest
    with pytest.raises(InvalidRequest):
        parent_notify.create_registration(
            parent=_parent(store), worker_engine="codex", worker_id=C,
            worker_name=None, source="new", settle=True,
            operation={"kind": "codex-stable", "expectTurn": 0,
                       "selectionMode": "status-only", "stableState": "idle"},
        )
    frozen = notify_settle.settlement_for_registration(row)
    if frozen is None:
        pytest.fail(
            f"source/new stable recursive wait: "
            f"row={parent_notify.read_registration(eid)!r}; "
            f"child={store.record(C).get('lifecycle')!r}"
        )
    assert frozen["status"] == "idle"
    assert frozen.get("settlementVerdict") == ("settled" if settle else None)
    assert frozen["report"] is None and frozen["turnStatus"] is None
    assert client.send("codex:" + C, "WRONG later R2",
                       delivery_id="n3c-later", wait=True, timeout=15).turn_id
    assert notify_settle.settlement_for_registration(row)["report"] is None
    assert parent_notify.read_registration(eid)["operation"] == row["operation"]
    parent_notify.cancel_registration(eid, "cancel parent wake")
    commands_create.cmd_new(args)
    again = json.loads(capsys.readouterr().out)
    assert again["notify_parent"]["event_id"] == eid
    assert again["notify_parent"]["state"] == "cancelled"
    assert trace.read_text().count("RPC thread/start") == threads + 1
    assert trace.read_text().count("RPC turn/start") == turns + 1
    with pytest.raises(SystemExit):
        commands_create.cmd_new(_new(tmp_path, settle=not settle))
    capsys.readouterr()
    _n1c_managed_parent(monkeypatch, store, B)
    with pytest.raises(SystemExit):
        commands_create.cmd_new(args)
    capsys.readouterr()
    assert len([r for r in parent_notify.iter_registrations(include_terminal=True)
                if r["worker"]["workerId"] == C]) == 1


def test_codex_new_original_r1_frozen_under_descendants_one_final_wake(
    machine_originals, tmp_path, monkeypatch, capsys,
):
    client, store, trace, _daemon = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    commands_create.cmd_new(_new(tmp_path, prompt="N3C original R1", settle=True))
    public = json.loads(capsys.readouterr().out)
    eid = public["notify_parent"]["event_id"]
    row = parent_notify.read_registration(eid)
    turn = codex_delivery.read_receipt("codex:" + C, "create:initial")["turnId"]
    assert turn and row["source"] == "send"
    descendant_id = "dddddddd-4444-4444-8444-444444444444"
    assert client.create(cwd=str(tmp_path), worker_id=descendant_id).worker_id == descendant_id
    token = "n3c-descendant-pending"
    codex_delivery.prepare("codex:" + descendant_id, token,
                           "descendant", mode="new_turn_only")
    descendant = parent_notify.create_registration(
        parent=_parent(store, C), worker_engine="codex",
        worker_id=descendant_id, worker_name=None,
        operation={"kind": "codex-turn", "deliveryId": token}, source="send",
    )
    from ccx.ops.wait import reconcile_turn
    assert reconcile_turn("codex:" + C, delivery_id="create:initial")["status"] == "settled"
    assert notify_settle.settlement_for_registration(row) is None
    frozen = parent_notify.read_registration(eid)["operationSettlement"]
    assert frozen["status"] == "completed" and frozen["turnId"] == turn
    assert frozen["report"] == "Fake Codex completed: N3C original R1"
    notifyd.process_registration(parent_notify.read_registration(eid))
    assert parent_notify.read_registration(eid)["state"] == "armed"
    assert client.send("codex:" + C, "WRONG cached R2",
                       delivery_id="n3c-second", wait=True, timeout=15).turn_id
    assert notify_settle.settlement_for_registration(row) is None
    parent_notify.cancel_registration(descendant["eventId"], "descendant resolved")
    final = notify_settle.settlement_for_registration(row)
    assert final["settlementVerdict"] == "settled"
    assert final["turnId"] == frozen["turnId"] and final["report"] == frozen["report"]
    count = trace.read_text().count("RPC turn/start")
    notifyd.process_registration(parent_notify.read_registration(eid))
    delivered = parent_notify.read_registration(eid)
    assert delivered["state"] == "delivered"
    assert delivered["payload"]["deliveryId"] == eid + ":settled"
    assert delivered["payload"]["report"] == frozen["report"]
    assert trace.read_text().count("RPC turn/start") == count + 1
    notifyd.process_registration(delivered)
    assert trace.read_text().count("RPC turn/start") == count + 1


def test_codex_new_initial_lost_native_ack_keeps_one_child_and_event(
    machine_originals, tmp_path, monkeypatch, capsys,
):
    _client, store, trace, _daemon = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    threads = trace.read_text().count("RPC thread/start")
    turns = trace.read_text().count("RPC turn/start")
    args = _new(tmp_path, prompt="one maybe-submitted N3C first message", settle=True)
    original = GenerationStore.dispatch_native_intent_once
    def drop_ack_after_fence(self, prepared, *, bounded_submit, **kwargs):
        if prepared.family == "codex-send" and prepared.operation_id == "create:initial":
            def lost(_budget):
                raise TimeoutError("original first SEND ACK lost AFTER durable submit")
            return original(self, prepared, bounded_submit=lost, **kwargs)
        return original(self, prepared, bounded_submit=bounded_submit, **kwargs)
    with monkeypatch.context() as patched:
        patched.setattr(GenerationStore, "dispatch_native_intent_once", drop_ack_after_fence)
        with pytest.raises(SystemExit):
            commands_create.cmd_new(args)
    failed = json.loads(capsys.readouterr().out)
    assert failed["phase"] == "initial-delivery"
    assert failed["worker_id"] == C and failed["delivery_id"] == "create:initial"
    assert store.read_receipt("codex:" + C, "codex-send", "create:initial")[
        "state"] == "submitted-unknown"
    assert codex_delivery.read_receipt("codex:" + C, "create:initial")[
        "state"] == "submitted"
    rows = [r for r in parent_notify.iter_registrations()
            if r["worker"]["workerId"] == C]
    assert len(rows) == 1 and rows[0]["source"] == "send"
    eid = rows[0]["eventId"]
    assert trace.read_text().count("RPC thread/start") == threads + 1
    assert trace.read_text().count("RPC turn/start") == turns

    # A second process with the exact caller-known full child id/first
    # message cannot re-dispatch after the original submitted-unknown.
    with pytest.raises(SystemExit):
        commands_create.cmd_new(args)
    replay = json.loads(capsys.readouterr().out)
    assert replay["worker_id"] == C and replay["delivery_id"] == "create:initial"
    assert replay["phase"] == "initial-delivery"
    assert trace.read_text().count("RPC thread/start") == threads + 1
    assert trace.read_text().count("RPC turn/start") == turns
    assert [r["eventId"] for r in parent_notify.iter_registrations()
            if r["worker"]["workerId"] == C] == [eid]
