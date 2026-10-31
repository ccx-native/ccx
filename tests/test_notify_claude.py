"""Claude child original full-UUID notify; Codex parent wake is already accepted."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.cli.commands.turns as commands_turns
import ccx.notify.deliver as notify_deliver
import ccx.ops.send as ops_send
import ccx.notify.settle as notify_settle
import ccx.ops.wait as ops_wait

import json
import time

import pytest

from ccx.claude import delivery as claude_delivery
from ccx.codex import control as codex_control
from ccx import constants
from ccx.notify import daemon as notifyd
from ccx.controllers import orchestrator
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.errors import InvalidRequest, OperationConflict, SessionUnavailable
from ccx.notify.client import ParentNotificationClient
from ccx.store.store import GenerationStore
from ccx.store.root import NativeEffectIndeterminate
from ccx.ops.creation_steps import prepare_worker
from test_generation_store import A, B, C, short_root, fixture_root
from test_delivery_flow import _activate_fake_claude
from test_notify_registration import machine_originals
from test_notify_ext import FakeControl, RETURN_THREAD


def _return_address(monkeypatch, thread=RETURN_THREAD):
    for key in ('CCX_SELF_SHORT','CCX_SELF_WORKER_ID','CCX_SELF_ENGINE','CLAUDE_CODE_SESSION_ID'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv('CODEX_THREAD_ID', thread)
    monkeypatch.setattr(orchestrator, '_external_codex_unavailable', lambda _thread: None)
    return orchestrator.require_notify_parent()


@pytest.fixture
def owners(tmp_path,monkeypatch):
    """Original globally claimed Claude natives; disposable injected activation only."""
    from ccx.store.root import require_ccx_active
    root=fixture_root(tmp_path)
    store=require_ccx_active(root)
    for wid in (A,B):
        prepared=prepare_worker(store,engine='claude',cwd=str(tmp_path),
                                worker_id=wid,name='claude-original')
        _activate_fake_claude(store,prepared)
    monkeypatch.setattr(constants,'CCX_ROOT',root)
    monkeypatch.setenv('CCX_ROOT', str(root))
    return root,store


def _activated_child(owners):
    return owners


def _fake_original_response(store, token, text, *, worker=A):
    sid = store.record(worker)['native']['sessionId']
    turn_ref = 'ccx-turn:claude:v1:original-'+token
    return {
        'accepted': True, 'workerId': worker, 'engine': 'claude',
        'deliveryId': token, 'nativeSource':'claude-session:'+sid,
        'sessionId':sid, 'turnRef':turn_ref,
        'nativeEvidenceSha256':'a'*64,
    }


@pytest.mark.parametrize("json_mode,explicit", [
    (False, False), (True, False), (True, True),
])
def test_claude_cli_notify_parent_optional_delivery_id_and_stable_wake(
    owners, monkeypatch, capsys, json_mode, explicit,
):
    """One CLI-generated child ID survives registration and the separate wake."""
    from argparse import Namespace
    from uuid import UUID
    from ccx.cli.commands import _common as commands

    _root, store = _activated_child(owners)
    _return_address(monkeypatch)
    monkeypatch.setattr(notify_daemon, 'ensure_notify_daemon',
                        lambda **_kwargs: None)
    submitted = []
    original_send = ops_send.send_turn
    def native_submit(handle, sid, prompt, token, budget):
        submitted.append(token)
        return _fake_original_response(store, token, prompt)
    def intercepted_send(handle, prompt, **kwargs):
        return original_send(handle, prompt, **kwargs,
                             _native_submit=native_submit)
    monkeypatch.setattr(ops_send, 'send_turn', intercepted_send)
    chosen = "caller-explicit-claude-child" if explicit else None
    args = Namespace(
        target='claude:' + A, text='Generated child Claude message',
        stdin=False, file=None, timeout=4, delivery_id=chosen,
        notify_parent=True, var=None, vars_from=None,
        json=json_mode, no_wait=False, wait=False,
        cap_5h=None, cap_7d=None, cap_opus=None,
    )
    commands_turns.cmd_send(args)
    output = capsys.readouterr().out
    token = (json.loads(output)['delivery_id'] if json_mode else
             output.split('delivery_id: ', 1)[1].splitlines()[0])
    if explicit:
        assert token == chosen
    else:
        assert UUID(token).version == 4
    assert submitted == [token]
    rows = parent_notify.iter_registrations()
    assert len(rows) == 1
    reg = rows[0]
    assert reg['operation']['deliveryId'] == token
    assert reg['source'] == 'send'
    assert token in output
    payload = notify_deliver.payload_for_registration(
        reg, {'status': 'completed', 'workerState': 'idle',
              'report': 'mock child complete'})
    assert payload['selectedOriginalDeliveryId'] == token
    assert payload['deliveryId'] == reg['eventId'] + ':settled'
    assert payload['deliveryId'] != token
    assert notify_deliver.payload_for_registration(
        reg, {'status': 'completed'})['deliveryId'] == payload['deliveryId']
    assert 'selectionMode' not in payload  # send does not borrow notify selectors

    if explicit:
        # An explicit same-ID repeat reuses original semantics; no second native send.
        commands_turns.cmd_send(args)
        replay = json.loads(capsys.readouterr().out)
        assert replay['delivery_id'] == chosen
        assert replay['notify_parent']['event_id'] == reg['eventId']
        assert submitted == [chosen]
        assert len(parent_notify.iter_registrations()) == 1


def test_original_claude_semantic_receipt_registration_precedes_one_native_input_and_parent_wake(
    owners, monkeypatch,
):
    root, store = _activated_child(owners)
    parent = _return_address(monkeypatch)
    native = FakeControl()
    monkeypatch.setattr(codex_control,'CodexControlClient',lambda:native)
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',lambda **_kw:None)
    token = 'claude-notify-original-id'
    text = 'Original Claude child answer required'
    native_writes=[]
    starts=[]
    original_begin=GenerationStore.begin_native_intent

    def beginning(self, target, *, action, family, operation_id, intent_sha256):
        if family == 'claude-delivery' and operation_id == token:
            rows=parent_notify.iter_registrations()
            assert len(rows)==1 and rows[0]['state']=='armed'
            row=rows[0]
            assert row['parent']==parent and row['worker']['engine']=='claude'
            assert row['worker']['workerId']==A
            assert row['operation']['deliveryId']==token
            assert row['operation']['requestedMode']=='new_turn_only'
            assert row['operation']['cap']=={'5h':50}
            assert json.loads(parent_notify.registration_path(row['eventId']).read_text())==row
            assert claude_delivery.read_receipt('claude:'+A,token)['state']=='prepared'
            assert not native_writes
            starts.append(row['eventId'])
        return original_begin(self,target,action=action,family=family,
                              operation_id=operation_id,intent_sha256=intent_sha256)
    monkeypatch.setattr(GenerationStore,'begin_native_intent',beginning)

    def send_original(handle, sid, prompt, original_id, budget):
        assert (handle,prompt,original_id)==('claude:'+A,text,token)
        assert sid==store.record(A)['native']['sessionId']
        assert len(starts)==1
        native_writes.append(original_id)
        return _fake_original_response(store,token,text)

    def settle_original(handle, turn_ref):
        assert (handle,turn_ref)==('claude:'+A,'ccx-turn:claude:v1:original-'+token)
        return {'workerId':A,'nativeSource':'claude-session:'+store.record(A)['native']['sessionId'],
                'deliveryId':token,'turnRef':turn_ref,'status':'completed',
                'text':'ORIGINAL Claude Stop text','nativeEvidenceSha256':'b'*64}

    first=ops_send.send_turn('claude:'+A,text,delivery_id=token,
        mode='new_turn_only', cap={'5h':50}, notify_parent=True,
        caller_metadata={'marker':'original'},_native_submit=send_original,
        _observe_settlement=settle_original)
    assert first['status']=='accepted' and first['settled']
    assert first['notification']['event_id']==starts[0]
    assert first['notification']['parent']==parent
    assert native_writes==[token]
    row=parent_notify.read_registration(starts[0])
    assert notify_settle.settlement_for_registration(row)['report']=='ORIGINAL Claude Stop text'
    store.mutate_record('claude:'+A,patches={
        'lastReport':{'turnRef':'a-newer-unrelated-turn','text':'WRONG later report'}})
    notifyd.process_registration(row)
    final=parent_notify.read_registration(starts[0])
    assert final['state']=='delivered' and final['payload']['worker']=='claude:'+A
    assert final['payload']['turnRef']=='ccx-turn:claude:v1:original-'+token
    assert final['payload']['report']=='ORIGINAL Claude Stop text'
    assert final['delivery']['threadId']==RETURN_THREAD
    assert len(native.methods('turn/start'))==1
    again=ops_send.send_turn('claude:'+A,text,delivery_id=token,
        mode='new_turn_only',cap={'5h':50},notify_parent=True,
        caller_metadata={'marker':'original'},_native_submit=send_original)
    assert again['notification']['state']=='delivered'
    assert native_writes==[token] and len(native.methods('turn/start'))==1
    for change in ({'text':'wrong'}, {'cap':{'7d':50}},
                   {'caller_metadata':{'marker':'OTHER'}}):
        with pytest.raises(OperationConflict):
            ops_send.send_turn('claude:'+A,change.get('text',text),
                delivery_id=token,mode='new_turn_only',notify_parent=True,
                cap=change.get('cap',{'5h':50}),
                caller_metadata=change.get('caller_metadata',{'marker':'original'}),
                _native_submit=send_original)
    _return_address(monkeypatch,'another-native-parent-address')
    with pytest.raises(OperationConflict):
        ops_send.send_turn('claude:'+A,text,delivery_id=token,
            mode='new_turn_only',cap={'5h':50},notify_parent=True,
            caller_metadata={'marker':'original'},_native_submit=send_original)
    assert native_writes==[token]


def test_claude_r1_submitted_unknown_prepared_refusal_is_never_replayed(owners,monkeypatch,capsys):
    from ccx.cli.commands import _common as commands
    from argparse import Namespace
    root,store=_activated_child(owners)
    _return_address(monkeypatch)
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',lambda **_kw:None)
    token='claude-host-postpreflight-refusal'
    writes=[]

    def host_refused(handle,sid,prompt,original_id,budget):
        writes.append(original_id)
        claude_delivery.mark_retryable_refusal(handle,original_id,
            code='busy',message='host became busy after preflight',state='busy')
        # Actual host ownership-only reply cannot certify acceptance.
        return {'ok':True,'delivery':{'delivery_id':original_id,'status':'pending',
                                      'owned':False}}

    with pytest.raises(NativeEffectIndeterminate):
        ops_send.send_turn('claude:'+A,'postpreflight busy child',
            delivery_id=token,notify_parent=True,_native_submit=host_refused)
    assert writes==[token]
    receipt=claude_delivery.read_receipt('claude:'+A,token)
    assert receipt['state']=='prepared' and receipt['lastRefusalCode']=='busy'
    assert store.read_receipt('claude:'+A,'claude-delivery',token)['state']=='submitted-unknown'
    events=parent_notify.iter_registrations()
    assert len(events)==1 and events[0]['state']=='armed'
    assert notify_settle.settlement_for_registration(events[0]) is None
    with pytest.raises(NativeEffectIndeterminate):
        ops_send.send_turn('claude:'+A,'postpreflight busy child',
            delivery_id=token,notify_parent=True,_native_submit=host_refused)
    assert writes==[token] and len(parent_notify.iter_registrations())==1
    assert parent_notify.read_registration(events[0]['eventId'])['state']=='armed'

    # CLI maps the same ordinary AND notified uncertainty to original-token exit 6.
    from ccx.store.root import NativeEffectIndeterminate as Unknown
    def uncertain(*args, **kwargs):
        raise Unknown('original host native effect could have landed')
    monkeypatch.setattr(ops_send,'send_turn',uncertain)
    for notify in (False,True):
        args=Namespace(target='claude:'+A,text='R1',stdin=False,file=None,
            timeout=3,delivery_id=token,notify_parent=notify,var=None,
            vars_from=None,json=True,no_wait=False,wait=False)
        with pytest.raises(SystemExit) as ex:
            commands_turns.cmd_send(args)
        assert ex.value.code==6
        assert token in capsys.readouterr().err


@pytest.mark.parametrize('error,code',[
    (SessionUnavailable('host busy; original prompt not sent'),3),
    (OperationConflict('original Claude delivery conflicts'),OperationConflict.exit_code),
])
def test_claude_cli_send_preserves_unavailable_and_conflict_exits(owners,monkeypatch,capsys,error,code):
    from ccx.cli.commands import _common as commands
    from argparse import Namespace
    def refused(*args,**kwargs):
        raise error
    monkeypatch.setattr(ops_send,'send_turn',refused)
    args=Namespace(target='claude:'+A,text='busy',stdin=False,file=None,
        timeout=3,delivery_id='claude-cli-exit',notify_parent=False,var=None,
        vars_from=None,json=True,no_wait=False,wait=False)
    with pytest.raises(SystemExit) as ex:
        commands_turns.cmd_send(args)
    assert ex.value.code==code
    assert str(error) in capsys.readouterr().err


def test_claude_exact_turn_input_gate_and_negative_settlement_never_borrow_later_turn(
    owners,monkeypatch,
):
    from ccx.claude import managed_turns
    root,store=_activated_child(owners)
    parent=_return_address(monkeypatch)
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',lambda **_kw:None)
    token='claude-exact-gate'
    text='Claude waiting gate'
    first=ops_send.send_turn('claude:'+A,text,delivery_id=token,
        notify_parent=True,_native_submit=lambda *args:_fake_original_response(store,token,text))
    reg=parent_notify.read_registration(first['notification']['event_id'])
    original_ref=first['delivery']['turn_ref']
    assert original_ref
    monkeypatch.setattr(managed_turns,'settlement_for_turn_ref',
                        lambda wid,ref: (None,True))
    # parent_notify imported binding at call time; stub public source observer.
    gate={'id':'native-gate-this-turn','kind':'question'}
    monkeypatch.setattr(ops_wait,'wait_worker',lambda *args,**kwargs:{
        'worker':'claude:'+A, 'state':'waiting',
        'active_turn_ref':'ccx-turn:claude:v1:unrelated-successor',
        'waiting_for':gate})
    assert notify_settle.settlement_for_registration(reg) is None
    monkeypatch.setattr(ops_wait,'wait_worker',lambda *args,**kwargs:{
        'worker':'claude:'+A,'state':'waiting',
        'active_turn_ref':original_ref,'waiting_for':gate})
    current=notify_settle.settlement_for_registration(reg)
    assert current['status']=='waiting' and current['report'] is None
    assert current['turnRef']==original_ref and current['waitingFor']==gate
    monkeypatch.setattr(managed_turns,'settlement_for_turn_ref',
                        lambda wid,ref: ({'turnRef':ref,'outcome':'interrupted',
                                          'evidence':'original-native-interrupt'},True))
    outcome=notify_settle.settlement_for_registration(reg)
    assert outcome['status']=='interrupted' and outcome['report'] is None
    assert outcome['turnRef']==original_ref
    monkeypatch.setattr(managed_turns,'settlement_for_turn_ref',
                        lambda wid,ref: ({'turnRef':ref,'outcome':'unknown',
                                          'evidence':'native-session-inactive'},True))
    unknown=notify_settle.settlement_for_registration(reg)
    assert unknown['status']=='unknown' and unknown['report'] is None
    assert unknown['turnRef']==original_ref


def test_claude_cancelled_wake_does_not_cancel_child_send_or_late_arm(owners,monkeypatch):
    root,store=_activated_child(owners)
    parent=_return_address(monkeypatch)
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',lambda **_kw:None)
    token='claude-cancelled-notification'
    text='Original Claude child unaffected by cancelled wake'
    claude_delivery.prepare('claude:'+A,token,text,mode='new_turn_only')
    reg=parent_notify.create_registration(parent=parent,worker_engine='claude',
        worker_id=A,worker_name=None,operation={'kind':'claude-turn','deliveryId':token},
        source='send')
    parent_notify.cancel_registration(reg['eventId'],'cancel just parent wake')
    writes=[]
    def send(*args):
        writes.append(token)
        return _fake_original_response(store,token,text)
    sent=ops_send.send_turn('claude:'+A,text,delivery_id=token,
        notify_parent=True,_native_submit=send)
    assert sent['status']=='accepted' and sent['notification']['state']=='cancelled'
    assert writes==[token]
    assert parent_notify.read_registration(reg['eventId'])['state']=='cancelled'
    # Ordinary accepted original child send cannot be retroactively adopted.
    old='claude-ordinary-already-accepted'
    ops_send.send_turn('claude:'+B,'old unrelated text',delivery_id=old,
        _native_submit=lambda *a: _fake_original_response(store,old,'old',worker=B))
    with pytest.raises(InvalidRequest,match='retroactively arm'):
        ops_send.send_turn('claude:'+B,'old unrelated text',delivery_id=old,
            notify_parent=True,_native_submit=lambda *a: pytest.fail('second native call'))


def test_claude_real_generated_hooks_one_child_input_and_one_frozen_parent_wake(
    short_root,tmp_path,monkeypatch,capsys,
):
    """Disposable physical Claude host/hook; parent control is simulated only."""
    from argparse import Namespace
    from ccx.cli.commands import _common as commands
    from ccx.store.root import require_ccx_active
    from test_create_cross_surface import (
        _deadline_fake_claude, _first_wait_original_claude_ready,
    )
    from test_claude_native_cli import _stop_disposable_original

    _deadline_fake_claude(short_root,tmp_path,monkeypatch)
    pty_log=tmp_path/'claude-child-native-pty.log'
    hook_log=tmp_path/'claude-child-generated-hooks.jsonl'
    monkeypatch.setenv('FAKE_HOOKS_VIA_SETTINGS','1')
    monkeypatch.setenv('FAKE_LOG',str(pty_log))
    monkeypatch.setenv('FAKE_HOOK_RESULT_LOG',str(hook_log))
    store=require_ccx_active(short_root)
    token='claude-real-hook-notify'
    text='Claude original generated Stop response'
    fake=FakeControl()
    monkeypatch.setattr(codex_control,'CodexControlClient',lambda:fake)
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',lambda **_kw:None)
    began=[]
    original_begin=GenerationStore.begin_native_intent
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id==A
        _return_address(monkeypatch)
        def check_before_effect(self,target,*,action,family,operation_id,intent_sha256):
            if family=='claude-delivery' and operation_id==token:
                rows=parent_notify.iter_registrations()
                assert len(rows)==1
                event=rows[0]
                assert event['worker']['engine']=='claude'
                assert event['worker']['workerId']==A
                assert event['parent']['kind']=='codex-native-thread'
                assert event['state']=='armed'
                assert json.loads(parent_notify.registration_path(event['eventId']).read_text())==event
                assert claude_delivery.read_receipt('claude:'+A,token)['state']=='prepared'
                assert not pty_log.exists() or text not in pty_log.read_text()
                began.append(event['eventId'])
            return original_begin(self,target,action=action,family=family,
                                  operation_id=operation_id,intent_sha256=intent_sha256)
        monkeypatch.setattr(GenerationStore,'begin_native_intent',check_before_effect)
        args=Namespace(target='claude:'+A,text=text,stdin=False,file=None,
            timeout=9,delivery_id=token,notify_parent=True,
            var=None,vars_from=None,json=True,no_wait=False,wait=False,
            cap_5h=None,cap_7d=None,cap_opus=None)
        commands_turns.cmd_send(args)
        emitted=json.loads(capsys.readouterr().out)
        assert emitted['delivery_id']==token
        assert emitted['notify_parent']['event_id']==began[0]
        assert emitted['notify_parent']['state']=='armed'
        assert 'report' not in emitted
        end=time.monotonic()+9
        actual=None
        while time.monotonic()<end:
            actual=ops_wait.reconcile_turn('claude:'+A,delivery_id=token)
            if actual['status']=='settled':
                break
            time.sleep(.06)
        assert actual['status']=='settled',actual
        original=claude_delivery.read_receipt('claude:'+A,token)
        assert original['settlement']['text']=='Fake Claude completed: '+text
        assert original['turnRef']==actual['delivery']['turn_ref']
        notifyd.process_registration(parent_notify.read_registration(began[0]))
        after=parent_notify.read_registration(began[0])
        assert after['state']=='delivered',after
        assert after['payload']['worker']=='claude:'+A
        assert after['payload']['report']=='Fake Claude completed: '+text
        assert after['payload']['turnRef']==original['turnRef']
        assert after['delivery']['threadId']==RETURN_THREAD
        assert len(fake.methods('turn/start'))==1
        assert pty_log.read_bytes().count(text.encode())==1
        end=time.monotonic()+3
        rows=[]
        while time.monotonic()<end:
            if hook_log.exists():
                rows=[json.loads(line) for line in hook_log.read_text().splitlines()]
            if {'SessionStart','UserPromptSubmit','Stop'} <= {r.get('event') for r in rows}:
                break
            time.sleep(.03)
        assert {'SessionStart','UserPromptSubmit','Stop'} <= {r.get('event') for r in rows},rows
        assert all(r['rc']==0 for r in rows),rows
    finally:
        _stop_disposable_original(short_root)


def test_claude_real_busy_original_preflight_has_no_new_receipt_or_event(
    short_root,tmp_path,monkeypatch,
):
    from ccx import CcxClient
    from ccx.errors import DeliveryRejected
    from ccx.store.root import require_ccx_active
    from test_create_cross_surface import (
        _deadline_fake_claude, _first_wait_original_claude_ready,
    )
    from test_cancel_core import _wait_ref
    from test_claude_native_cli import _stop_disposable_original

    _deadline_fake_claude(short_root,tmp_path,monkeypatch)
    monkeypatch.setenv('FAKE_HOOKS_VIA_SETTINGS','1')
    monkeypatch.setenv('FAKE_MODE','nostop')
    monkeypatch.setenv('FAKE_LOG',str(tmp_path/'busy-claude-native-pty.log'))
    store=require_ccx_active(short_root)
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id==A
        ongoing=CcxClient().new('busy original Claude turn',cwd=str(tmp_path),
                                worker_id=A,wait=False,start_timeout=4)
        assert ongoing.worker_id==A
        assert _wait_ref('claude:'+A)
        _return_address(monkeypatch)
        with pytest.raises(DeliveryRejected) as caught:
            ops_send.send_turn('claude:'+A,'do not send another native input',
                delivery_id='claude-notify-fresh-busy',notify_parent=True)
        assert caught.value.refusal=='busy'
        assert claude_delivery.read_receipt('claude:'+A,'claude-notify-fresh-busy') is None
        assert ParentNotificationClient().list()['count']==0
    finally:
        _stop_disposable_original(short_root)


def test_claude_child_wakes_exact_original_managed_codex_parent(
    machine_originals, tmp_path, monkeypatch,
):
    """Genuine disposable Codex parent machine, Claude child original fake receipt."""
    client, store, trace, _rpc = machine_originals
    prepared=prepare_worker(store,engine='claude',cwd=str(tmp_path),
                            worker_id=C,name='same-prefix Claude child')
    _activate_fake_claude(store,prepared)
    for key in ('CCX_SELF_SHORT','CLAUDE_CODE_SESSION_ID'):
        monkeypatch.delenv(key,raising=False)
    monkeypatch.setenv('CCX_SELF_ENGINE','codex')
    monkeypatch.setenv('CCX_SELF_WORKER_ID',A)
    monkeypatch.setenv('CODEX_THREAD_ID',store.record(A)['native']['threadId'])
    parent=orchestrator.require_notify_parent()
    assert parent['kind']=='ccx-codex' and parent['workerId']==A
    token='cross-engine-one-claude-child'
    text='original Claude child for managed Codex'
    before=trace.read_text().count('RPC turn/start')
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',lambda **_kw:None)
    original_ref='ccx-turn:claude:v1:cross-engine-original'
    sid=store.record(C)['native']['sessionId']
    sent=ops_send.send_turn('claude:'+C,text,delivery_id=token,
        notify_parent=True,
        _native_submit=lambda *args: {
            'accepted':True,'workerId':C,'engine':'claude','deliveryId':token,
            'nativeSource':'claude-session:'+sid,'sessionId':sid,
            'turnRef':original_ref,'nativeEvidenceSha256':'a'*64},
        _observe_settlement=lambda *args:{
            'workerId':C,'nativeSource':'claude-session:'+sid,
            'deliveryId':token,'turnRef':original_ref,
            'status':'completed','text':'EXACT cross-engine original Claude report',
            'nativeEvidenceSha256':'b'*64})
    row=parent_notify.read_registration(sent['notification']['event_id'])
    assert row['worker']['workerId']==C and row['parent']['workerId']==A
    notifyd.process_registration(row)
    done=parent_notify.read_registration(row['eventId'])
    assert done['state']=='delivered',done
    assert done['payload']['worker']=='claude:'+C
    assert done['payload']['report']=='EXACT cross-engine original Claude report'
    assert done['delivery']['workerId']==A
    assert done['delivery']['threadId']==store.record(A)['native']['threadId']
    assert trace.read_text().count('RPC turn/start')==before+1
