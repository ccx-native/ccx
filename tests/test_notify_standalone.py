"""Read-only N2 standalone notify: one original full-UUID selected turn per arm."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.cli.commands.turns as commands_turns
import ccx.notify.select as notify_select
import ccx.ops.send as ops_send
import ccx.notify.settle as notify_settle
import ccx.ops.wait as ops_wait

import json
from argparse import Namespace

import pytest

from ccx.claude import delivery as claude_delivery
from ccx.codex import control as codex_control
from ccx.codex import delivery as codex_delivery
from ccx import constants
from ccx.notify import daemon as notifyd
from ccx.controllers import orchestrator
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.errors import InvalidRequest, SessionUnavailable, OperationConflict
from ccx.notify.client import ParentNotificationClient
from test_generation_store import A, B, short_root
from test_notify_registration import machine_originals, originals
from test_notify_ext import FakeControl, RETURN_THREAD
from test_notify_claude import owners, _fake_original_response


def _managed_caller(monkeypatch, store, wid=A):
    for key in ("CCX_SELF_SHORT", "CLAUDE_CODE_SESSION_ID"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CCX_SELF_ENGINE", "codex")
    monkeypatch.setenv("CCX_SELF_WORKER_ID", wid)
    monkeypatch.setenv("CODEX_THREAD_ID", store.record(wid)["native"]["threadId"])
    return orchestrator.require_notify_parent()


def _native_caller(monkeypatch):
    for key in ("CCX_SELF_SHORT", "CCX_SELF_ENGINE", "CCX_SELF_WORKER_ID",
                "CLAUDE_CODE_SESSION_ID"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CODEX_THREAD_ID", RETURN_THREAD)
    monkeypatch.setattr(orchestrator, '_external_codex_unavailable', lambda _thread: None)
    return orchestrator.require_notify_parent()


def _cli_args(target, *, settle=False, next_turn=False):
    return Namespace(target=target, json=True, settle=settle, next_turn=next_turn)


def test_independent_codex_arms_two_parents_one_original_turn_wake_each(
    machine_originals, monkeypatch, capsys,
):
    from ccx.cli import main as cli
    from ccx.cli import codex as codex_commands

    client, store, trace, _daemon_rpc = machine_originals
    token = "n2-one-original-codex-child"
    assert client.send("codex:"+B, "N2 current selected original Codex reply",
                       delivery_id=token, wait=True, timeout=15).turn_id
    native = FakeControl(lost_ack=True)
    monkeypatch.setattr(codex_control, "CodexControlClient", lambda: native)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    before=trace.read_text().count("RPC turn/start")
    _managed_caller(monkeypatch, store)
    args=_cli_args("codex:"+B)
    cli.main(["notify","codex:"+B,"--json"])
    one=json.loads(capsys.readouterr().out)
    assert one["source"]=="notify" and one["armed"]
    assert one["selection_mode"]=="last-settled"
    assert one["selected_original_delivery_id"]==token
    assert one["parent"]=="codex:"+A
    _native_caller(monkeypatch)
    codex_commands.cmd_notify(args)
    two=json.loads(capsys.readouterr().out)
    assert one["event_id"]!=two["event_id"]
    assert two["parent"]=={"kind":"codex-native-thread","threadId":RETURN_THREAD}
    assert len(parent_notify.iter_registrations())==2
    assert trace.read_text().count("RPC turn/start")==before
    # A newer cached report is not the original selected turn's native result.
    store.mutate_record("codex:"+B, patches={
        "lastReport":{"turnId":"later-native-turn","text":"WRONG newer report"}})
    for event in (one["event_id"], two["event_id"]):
        record=parent_notify.read_registration(event)
        assert record["operation"]["selectedTurnRef"]==one["selected_turn_ref"]
        assert record["registrationKeySha256"] is None
        assert json.loads(parent_notify.registration_path(event).read_text())==record
        notifyd.process_registration(record)
        final=parent_notify.read_registration(event)
        if event == two["event_id"]:
            # Native first placement lands, but its ACK is lost. W1 keeps the
            # saved attempted marker and reconciles exact event from history.
            assert final["state"]=="ready" and final["deliveryAttemptedAtMs"]
            parent_notify.mutate_registration(event,lambda v:v.update(nextAttemptAtMs=0))
            notifyd.process_registration(parent_notify.read_registration(event))
            final=parent_notify.read_registration(event)
            assert final["delivery"]["reconciled"] is True
        assert final["state"]=="delivered",final
        assert final["payload"]["report"]=="Fake Codex completed: N2 current selected original Codex reply"
        assert final["payload"]["source"]=="notify"
        assert final["payload"]["selectionMode"]=="last-settled"
        notifyd.process_registration(final)
    assert trace.read_text().count("RPC turn/start")==before+1  # managed A wake ONLY
    assert len(native.methods("turn/start"))==1  # independent external return
    assert ParentNotificationClient().list(source="notify",include_terminal=True)["count"]==2


@pytest.mark.parametrize("mode",["new_turn_only","auto"])
def test_claude_current_ref_gate_negative_and_later_report_never_borrowed(
    owners,monkeypatch,mode,
):
    from ccx.claude import managed_turns
    root,store=owners
    monkeypatch.setenv("CCX_ROOT",str(root))
    _native_caller(monkeypatch)
    token="n2-original-claude-turn"
    text="N2 one selected Claude turn"
    first=ops_send.send_turn("claude:"+A,text,delivery_id=token,mode=mode,
        _native_submit=lambda *args:{**_fake_original_response(store,token,text),
                                     "route":"new_turn"})
    ref=first["delivery"]["turn_ref"]
    assert ref and first["status"]=="accepted"
    assert claude_delivery.read_receipt("claude:"+A,token)["requestedMode"]==mode
    monkeypatch.setattr(ops_wait,"wait_worker",lambda *a,**kw:{
        "worker":"claude:"+A,"state":"busy","active_turn_ref":ref,
        "_served":True, "_pending":False, "last_settled_turn":None})
    from ccx.rpc import rpc
    from ccx.ops import compact_ops
    monkeypatch.setattr(rpc,"_rpc",lambda *a,**kw:{
        "ok":True,"activeTurnRef":ref,"compacting":False})
    monkeypatch.setattr(compact_ops,"active_operation",lambda *a:None)
    picked=notify_select.select_current_turn("claude:"+A)
    assert picked["operation"]["selectedTurnRef"]==ref
    assert picked["operation"]["selectionMode"]=="active"
    reg=parent_notify.create_registration(
        parent=orchestrator.require_notify_parent(),worker_engine="claude",
        worker_id=A,worker_name="Selected Claude",operation=picked["operation"],
        source="notify")
    monkeypatch.setattr(managed_turns,"settlement_for_turn_ref",
                        lambda wid,ref:(None,True))
    monkeypatch.setattr(ops_wait,"wait_worker",lambda *a,**kw:{
        "worker":"claude:"+A,"state":"waiting",
        "active_turn_ref":"ccx-turn:claude:v1:another-turn",
        "waiting_for":{"id":"later-gate"}})
    assert notify_settle.settlement_for_registration(reg) is None
    monkeypatch.setattr(ops_wait,"wait_worker",lambda *a,**kw:{
        "worker":"claude:"+A,"state":"waiting",
        "active_turn_ref":ref,"waiting_for":{"id":"same-turn-gate"}})
    waiting=notify_settle.settlement_for_registration(reg)
    assert waiting["status"]=="waiting" and waiting["report"] is None
    assert waiting["waitingFor"]["id"]=="same-turn-gate"
    monkeypatch.setattr(managed_turns,"settlement_for_turn_ref",
                        lambda wid,turn:({"turnRef":turn,"outcome":"unknown"},True))
    negative=notify_settle.settlement_for_registration(reg)
    assert negative["status"]=="unknown" and negative["report"] is None
    # A later changed report must not become this selected turn's evidence.
    store.mutate_record("claude:"+A, patches={
        "lastReport":{"turnRef":"ccx-turn:claude:v1:newer","text":"WRONG"}})
    assert notify_settle.settlement_for_registration(reg)["report"] is None


def test_positive_idle_unmatched_last_ref_is_status_only_no_native_send(
    owners, monkeypatch,
):
    root,store=owners
    monkeypatch.setenv("CCX_ROOT",str(root))
    parent=_native_caller(monkeypatch)
    monkeypatch.setattr(ops_wait,"wait_worker",lambda *a,**kw:{
        "worker":"claude:"+A,"state":"idle","active_turn_ref":None,
        "last_settled_turn":{"turn_ref":"ccx-turn:claude:v1:UNMATCHED-OLD"},
        "_served":True,"_pending":False,"status":"idle",
        "reason":"worker is idle"})
    from ccx.ops import compact_ops
    from ccx.rpc import rpc
    monkeypatch.setattr(compact_ops,"active_operation",lambda *a:None)
    monkeypatch.setattr(rpc,"_rpc",lambda *a,**kw:{
        "ok":True,"activeTurnRef":None,"compacting":False})
    selection=notify_select.select_current_turn("claude:"+A)
    assert selection["operation"]=={
        "kind":"claude-stable","selectionMode":"status-only",
        "selectedTurnRef":None,"stableState":"idle"}
    reg=parent_notify.create_registration(parent=parent,worker_engine="claude",
        worker_id=A,worker_name=None,operation=selection["operation"],source="notify")
    view=ParentNotificationClient().get(reg["eventId"])
    assert view["selected_turn_ref"] is None and view["source"]=="notify"
    assert view["delivery_id"] is None
    ready=notify_settle.settlement_for_registration(reg)
    assert ready["status"]=="idle" and ready["report"] is None
    assert ready.get("turnRef") is None and ready.get("turnId") is None


def test_active_compact_future_flag_and_unknown_opener_refuse_pre_event(
    owners,monkeypatch,capsys,
):
    from ccx.cli.commands import _common as commands
    from ccx.ops import compact_ops
    from ccx.rpc import rpc
    root,store=owners
    monkeypatch.setenv("CCX_ROOT",str(root))
    _native_caller(monkeypatch)
    # A journal-shaped active operation without its original authenticated
    # global effect remains unadoptable, with no model-turn fallback.
    monkeypatch.setattr(compact_ops,"active_operation",lambda *a:{"operation_id":"ongoing"})
    with pytest.raises(OperationConflict,match="no recorded submission"):
        notify_select.select_current_turn("claude:"+A)
    assert parent_notify.iter_registrations()==[]
    monkeypatch.setattr(compact_ops,"active_operation",lambda *a:None)
    monkeypatch.setattr(ops_wait,"wait_worker",lambda *a,**kw:{
        "worker":"claude:"+A,"state":"busy","active_turn_ref":"ccx-turn:claude:v1:UNKNOWN",
        "_served":True,"_pending":False})
    monkeypatch.setattr(rpc,"_rpc",lambda *a,**kw:{
        "ok":True,"activeTurnRef":"ccx-turn:claude:v1:UNKNOWN","compacting":False})
    with pytest.raises(SessionUnavailable,match="eligible opening receipt"):
        notify_select.select_current_turn("claude:"+A)
    assert parent_notify.iter_registrations()==[]
    with pytest.raises(SystemExit) as refused:
        commands_turns.cmd_notify(_cli_args("claude:"+A,next_turn=True))
    assert refused.value.code==3
    assert parent_notify.iter_registrations()==[]


def test_explicitly_killed_original_child_is_status_only_without_native_revival(
    originals,monkeypatch,capsys,
):
    from ccx.cli import codex as codex_commands
    from ccx import WorkerClient
    store,trace,_=originals
    assert WorkerClient().kill("codex:"+B)["killed"] is True
    assert store.record("codex:"+B)["lifecycle"]["state"]=="dead"
    _managed_caller(monkeypatch,store)
    monkeypatch.setattr(notify_daemon,"ensure_notify_daemon",lambda **_kw:None)
    before=trace.read_text()
    codex_commands.cmd_notify(_cli_args("codex:"+B))
    result=json.loads(capsys.readouterr().out)
    assert result["selection_mode"]=="status-only"
    assert result["selected_turn_ref"] is None
    assert result["selected_state"]=="dead"
    reg=parent_notify.read_registration(result["event_id"])
    verdict=notify_settle.settlement_for_registration(reg)
    assert verdict["status"]=="dead" and verdict["report"] is None
    assert "RPC turn/start" not in trace.read_text()[len(before):]
    assert "RPC thread/resume" not in trace.read_text()[len(before):]


def test_codex_steer_fallback_and_active_compact_cannot_be_adopted(
    originals,monkeypatch,
):
    from ccx.cli import codex as codex_commands
    from ccx.store.store import GenerationStore
    store,trace,_=originals
    _managed_caller(monkeypatch,store)
    before=trace.read_text()
    ref="ccx-turn:codex:v1:raw-or-steered-turn"
    monkeypatch.setattr(ops_wait,"wait_worker",lambda *a,**kw:{
        "worker":"codex:"+B,"state":"busy","status":"timeout","active_turn_ref":ref})
    monkeypatch.setattr(codex_delivery,"receipt_for_turn",lambda *a:{
        "state":"accepted","deliveryId":"n2-steer-not-opener",
        "turnId":"raw-or-steered-turn","route":"steer",
        "placement":"current_turn","requestedMode":"steer_only"})
    with pytest.raises(SessionUnavailable,match="eligible opening receipt"):
        notify_select.select_current_turn("codex:"+B)
    prior=GenerationStore.record
    def compact_source(self,target):
        result=prior(self,target)
        if target in ("codex:"+B,B):
            return {**result,"activeCompact":{"operationId":"c3-actual-compaction"}}
        return result
    monkeypatch.setattr(GenerationStore,"record",compact_source)
    # A marker alone is not an authenticated original compact effect.
    with pytest.raises(OperationConflict,match="no recorded submission"):
        notify_select.select_current_turn("codex:"+B)
    with pytest.raises(SystemExit) as refused:
        codex_commands.cmd_notify(_cli_args("codex:"+B,settle=True))
    assert refused.value.code==5
    assert parent_notify.iter_registrations()==[]
    assert trace.read_text()==before


def test_selected_claude_original_stop_frozen_after_later_report(
    owners,monkeypatch,
):
    root,store=owners
    monkeypatch.setenv("CCX_ROOT",str(root))
    parent=_native_caller(monkeypatch)
    token="n2-selected-original-claude-stop"
    text="original Claude selected source"
    turn="ccx-turn:claude:v1:original-"+token
    sid=store.record(A)["native"]["sessionId"]
    sent=ops_send.send_turn("claude:"+A,text,delivery_id=token,
        _native_submit=lambda *a:_fake_original_response(store,token,text),
        _observe_settlement=lambda *a:{
            "workerId":A,"nativeSource":"claude-session:"+sid,
            "deliveryId":token,"turnRef":turn,"status":"completed",
            "text":"ORIGINAL exact Claude Stop, not latest",
            "nativeEvidenceSha256":"b"*64})
    assert sent["status"]=="accepted" and sent["settled"]
    from ccx.ops import compact_ops
    from ccx.rpc import rpc
    monkeypatch.setattr(compact_ops,"active_operation",lambda *a:None)
    monkeypatch.setattr(ops_wait,"wait_worker",lambda *a,**kw:{
        "worker":"claude:"+A,"state":"idle","status":"idle",
        "reason":"worker is idle","active_turn_ref":None,
        "last_settled_turn":{"turn_ref":turn}})
    monkeypatch.setattr(rpc,"_rpc",lambda *a,**kw:{
        "ok":True,"activeTurnRef":None,"compacting":False})
    selected=notify_select.select_current_turn("claude:"+A)
    assert selected["operation"]["selectionMode"]=="last-settled"
    assert selected["operation"]["selectedTurnRef"]==turn
    reg=parent_notify.create_registration(parent=parent,worker_engine="claude",
        worker_id=A,worker_name=None,operation=selected["operation"],source="notify")
    store.mutate_record("claude:"+A,patches={
        "lastReport":{"turnRef":"ccx-turn:claude:v1:newer","text":"WRONG later turn"}})
    observed=notify_settle.settlement_for_registration(reg)
    assert observed["status"]=="completed"
    assert observed["turnRef"]==turn
    assert observed["report"]=="ORIGINAL exact Claude Stop, not latest"
    fake=FakeControl()
    monkeypatch.setattr(codex_control,"CodexControlClient",lambda:fake)
    notifyd.process_registration(reg)
    final=parent_notify.read_registration(reg["eventId"])
    assert final["state"]=="delivered"
    assert final["payload"]["report"]==observed["report"]
    assert final["payload"]["selectionMode"]=="last-settled"
    assert len(fake.methods("turn/start"))==1


def test_codex_notify_bootstrap_failure_keeps_one_durable_arm(originals,monkeypatch,capsys):
    from ccx.cli import codex as codex_commands
    store,trace,_=originals
    _native_caller(monkeypatch)
    before=trace.read_text()
    monkeypatch.setattr(notify_select,"select_current_turn",lambda child:{
        "worker":child,"worker_id":B,"engine":"codex","name":None,"state":"idle",
        "operation":{"kind":"codex-stable","selectionMode":"status-only",
                     "selectedTurnRef":None,"stableState":"idle"}})
    def down():
        raise SessionUnavailable("notifier bootstrap failed")
    monkeypatch.setattr(notify_daemon,"ensure_notify_daemon",down)
    codex_commands.cmd_notify(_cli_args("codex:"+B))
    out=capsys.readouterr()
    result=json.loads(out.out)
    assert result["armed"] is True and result["notifier_available"] is False
    assert "event durably armed" in out.err
    events=parent_notify.iter_registrations()
    assert [e["eventId"] for e in events]==[result["event_id"]]
    assert events[0]["state"]=="armed" and events[0]["source"]=="notify"
    assert trace.read_text()==before  # no child native effect


@pytest.mark.parametrize("engine,case,code",[
    ("claude","unknown",2),("claude","malformed",1),("claude","binding",3),
    ("codex","unknown",2),("codex","malformed",1),
])
def test_notify_cli_error_boundary_maps_exit_without_event(
    owners,monkeypatch,capsys,engine,case,code,
):
    from ccx.cli import codex as codex_commands
    from ccx.cli.commands import _common as commands
    from ccx.store.root import NativeActionDenied
    _native_caller(monkeypatch)
    target={"unknown":engine+":00000000-0000-4000-8000-000000000000",
            "malformed":engine+":not-a-full-uuid",
            "binding":"claude:"+A}[case]
    if case=="binding":
        def denied(child):
            raise NativeActionDenied("terminal-only or pending native binding")
        monkeypatch.setattr(notify_select,"select_current_turn",denied)
    handler=commands_turns.cmd_notify if engine=="claude" else codex_commands.cmd_notify
    with pytest.raises(SystemExit) as ex:
        handler(_cli_args(target))
    assert ex.value.code==code
    message = capsys.readouterr().err
    assert "ccx notify" in message or "notification" in message
    assert parent_notify.iter_registrations()==[]
