"""N1-ext: caller-captured native RETURN thread; original child UUID still owns send."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.deliver as notify_deliver
import ccx.notify.settle as notify_settle
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait

import json
import time

import pytest

from ccx.codex import control as codex_control
from ccx.codex import delivery as codex_delivery
from ccx import constants
from ccx.notify import daemon as notifyd
from ccx.controllers import orchestrator
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.errors import InvalidRequest, OperationConflict, SessionUnavailable
from ccx.notify.client import ParentNotificationClient
from ccx.store.store import GenerationStore
from ccx.store.root import StorageIntegrityError
from test_generation_store import A, B, short_root
from test_notify_registration import originals


RETURN_THREAD = 'interactive-native-return-thread-unique'


def native_caller(monkeypatch, thread_id=RETURN_THREAD):
    for key in ('CCX_SELF_SHORT', 'CCX_SELF_WORKER_ID', 'CCX_SELF_ENGINE',
                'CLAUDE_CODE_SESSION_ID'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv('CODEX_THREAD_ID', thread_id)
    # These original delivery tests fake the owning app-server explicitly.
    monkeypatch.setattr(orchestrator, '_external_codex_unavailable', lambda _thread: None)
    return orchestrator.require_notify_parent()


class FakeControl:
    """Codex app-server-shaped response authority, NOT a real provider/socket."""
    def __init__(self, *, missing_first=False, archived=False, lost_ack=False,
                 history_unloaded=False, history_absent=False):
        self.calls = []
        self.placed = []
        self.history = []
        self.missing_first = missing_first
        self.archived = archived
        self.lost_ack = lost_ack
        self.history_unloaded = history_unloaded
        self.history_absent = history_absent
        self.resumed = False

    def initialize(self):
        self.calls.append(('initialize', None))

    def close(self):
        self.calls.append(('close', None))

    def request(self, method, params, *, timeout=30):
        self.calls.append((method, dict(params)))
        if method == 'thread/items/list':
            if self.history_unloaded and not self.resumed:
                raise codex_control.CodexControlError(method, {
                    'code':-32600,'message':'thread not found (unloaded)'})
            if self.history_absent:
                return {'data': [], 'nextCursor': None}
            return {'data':list(reversed(self.history)), 'nextCursor':None}
        if method == 'thread/resume':
            if self.archived:
                raise codex_control.CodexControlError(method, {
                    'code':-32600,'message':'archived native thread; resume refused'})
            self.resumed=True
            return {'thread':{'id':params['threadId'],'status':{'type':'idle'}}}
        if method == 'turn/start':
            if self.missing_first and not self.resumed:
                raise codex_control.CodexControlError(method, {
                    'code':-32600,'message':'thread not found (unloaded)'})
            assert params['input']==[]
            assert params['turnTrigger']=='ccx-worker-notification'
            output=params['toolOutput']
            assert output['name']=='worker_notification' and output['namespace']=='ccx'
            payload=json.loads(output['output'])
            assert params['threadId']==RETURN_THREAD
            turn_id='machine-wake-'+str(len(self.placed)+1)
            self.placed.append((params['threadId'],payload))
            self.history.append({'turnId':turn_id,'item':{
                'type':'functionCallOutput','name':'worker_notification',
                'output':json.dumps(payload),
            }})
            if self.lost_ack:
                self.lost_ack=False
                raise TimeoutError('fake lost native machine turn reply AFTER placement')
            return {'turn':{'id':turn_id}}
        raise AssertionError('unsupported fake native control method: '+method)

    def methods(self, method):
        return [params for m,params in self.calls if m==method]


def _frozen_child(store, *, token, parent, answer='ORIGINAL exact child answer'):
    child='codex:'+B
    codex_delivery.prepare(child,token,'child prompt '+token,mode='new_turn_only')
    reg=parent_notify.create_registration(
        parent=parent,worker_engine='codex',worker_id=B,worker_name='Child',
        operation={'kind':'codex-turn','deliveryId':token},source='send')
    codex_delivery.mark_submitted(child,token)
    turn='native-child-turn-'+token
    codex_delivery.mark_accepted(child,token,turn_id=turn)
    codex_delivery.mark_settled(child,token,turn_id=turn,
        original_positive_completion={
            'workerId':B,'engine':'codex','deliveryId':token,'turnId':turn,
            'threadId':store.record(B)['native']['threadId'],
            'nativeSource':'ccx-worker:'+B,'status':'completed','text':answer,
            'nativeEvidenceSha256':'a'*64,
        })
    return reg


def _advance(reg):
    parent_notify.mutate_registration(reg['eventId'],
        lambda v: v.update(nextAttemptAtMs=0))
    notifyd.process_registration(parent_notify.read_registration(reg['eventId']))
    return parent_notify.read_registration(reg['eventId'])


def test_native_return_address_once_and_e1_inventory_including_stopped(originals, monkeypatch):
    from ccx import WorkerClient
    store,trace,_=originals
    before=trace.read_text()
    parent=native_caller(monkeypatch)
    assert parent=={'kind':'codex-native-thread','threadId':RETURN_THREAD}
    assert orchestrator.current_caller().supports_parent_notify
    token='native-return-identity'
    codex_delivery.prepare('codex:'+B,token,'child prompt',mode='new_turn_only')
    first=parent_notify.create_registration(
        parent=parent,worker_engine='codex',worker_id=B,worker_name=None,
        operation={'kind':'codex-turn','deliveryId':token},source='send')
    assert first['version']==3 and first['generation']==store.generation
    assert first['parent']==parent and first['worker']['workerId']==B
    assert json.loads(parent_notify.registration_path(first['eventId']).read_text())==first
    projected=ParentNotificationClient().get(first['eventId'])
    assert projected['parent']==parent and projected['parent_kind']=='codex-native-thread'
    assert projected['worker']=='codex:'+B
    assert ParentNotificationClient().list()['count']==1
    assert ParentNotificationClient().list(parent='codex:'+A)['count']==0
    # Changing caller environment cannot change the durable first address.
    native_caller(monkeypatch,'second-interactive-return-thread')
    with pytest.raises(InvalidRequest,match='different parent'):
        parent_notify.create_registration(
            parent=orchestrator.require_notify_parent(),worker_engine='codex',
            worker_id=B,worker_name=None,
            operation={'kind':'codex-turn','deliveryId':token},source='send')
    assert parent_notify.read_registration(first['eventId'])['parent']==parent
    assert trace.read_text()==before
    # E1 excludes ANY managed native thread, including the exact CHILD and
    # a separately stopped original worker. No ID is resolved as a target.
    for wid in (A,B):
        native_id=store.record(wid)['native']['threadId']
        with pytest.raises(InvalidRequest,match='already a CCX worker'):
            native_caller(monkeypatch,native_id)
    assert WorkerClient().kill('codex:'+A)['killed'] is True
    with pytest.raises(InvalidRequest,match='already a CCX worker'):
        native_caller(monkeypatch,store.record(A)['native']['threadId'])
    assert parent_notify.read_registration(first['eventId'])['parent']==parent
    cancelled=ParentNotificationClient().cancel(first['eventId'],reason='return wake cancelled')
    assert cancelled['state']=='cancelled' and cancelled['parent']==parent
    assert cancelled['delivery_available'] is False
    assert ParentNotificationClient().list()['count']==0
    assert ParentNotificationClient().list(include_terminal=True)['count']==1


def test_e1_inventory_fail_closed_before_receipts(originals,monkeypatch):
    store,trace,_=originals
    before=trace.read_text()
    native_caller(monkeypatch)
    original=GenerationStore.list_workers
    def inaccessible(self, **kwargs):
        assert kwargs['engine']=='codex' and kwargs['limit'] is None
        raise StorageIntegrityError('owned record inventory unreadable')
    monkeypatch.setattr(GenerationStore,'list_workers',inaccessible)
    with pytest.raises(SessionUnavailable,match='Cannot list Codex workers'):
        orchestrator.require_notify_parent()
    assert trace.read_text()==before
    assert ParentNotificationClient().list()['count']==0
    monkeypatch.setattr(GenerationStore,'list_workers',original)
    assert orchestrator.require_notify_parent()=={
        'kind':'codex-native-thread','threadId':RETURN_THREAD}


def test_native_parent_frozen_child_report_and_one_public_real_child_send(originals,monkeypatch):
    store,trace,_=originals
    parent=native_caller(monkeypatch)
    fake=FakeControl()
    monkeypatch.setattr(codex_control,'CodexControlClient',lambda:fake)
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',lambda **_kw:None)
    before=trace.read_text().count('RPC turn/start')
    sent=ops_send.send_turn('codex:'+B,'native interactive child prompt',
        delivery_id='native-child-real-delivery',notify_parent=True)
    assert sent['status']=='accepted' and sent['notification']['parent']==parent
    event_id=sent['notification']['event_id']
    assert trace.read_text().count('RPC turn/start')==before+1
    deadline=time.monotonic()+6
    while time.monotonic()<deadline:
        observed=ops_wait.reconcile_turn('codex:'+B,
            delivery_id='native-child-real-delivery')
        if observed['status']=='settled':
            break
        time.sleep(.03)
    assert observed['status']=='settled',observed
    # A later unrelated cached report must never contaminate the frozen event.
    store.mutate_record('codex:'+B,patches={
        'lastReport':{'turnId':'newer-turn','text':'WRONG latest child reply'}})
    before_native=trace.read_text()
    notifyd.process_registration(parent_notify.read_registration(event_id))
    final=parent_notify.read_registration(event_id)
    assert final['state']=='delivered' and final['delivery']['reconciled'] is False,final
    assert final['payload']['report']=='Fake Codex completed: native interactive child prompt'
    assert final['payload']['parent']==parent
    assert fake.methods('turn/resume')==[]
    assert len(fake.methods('turn/start'))==1
    assert fake.methods('turn/start')[0]['threadId']==RETURN_THREAD
    assert len(fake.placed)==1 and fake.placed[0][1]['deliveryId']==event_id+':settled'
    assert trace.read_text()==before_native  # no second CCX child native turn
    assert ParentNotificationClient().get(event_id)['parent']==parent
    assert ParentNotificationClient().get(event_id)['settlement']['report']==final['payload']['report']
    assert 'parent model completion is unproven' in ParentNotificationClient().get(event_id)['instruction']
    notifyd.process_registration(final)
    assert len(fake.methods('turn/start'))==1


def test_unloaded_external_parent_is_never_resumed_and_defers_without_effect(originals,monkeypatch):
    # The user's own Codex UI closed: waking it headless would hide the turn.
    store,trace,_=originals
    parent=native_caller(monkeypatch)
    reg=_frozen_child(store,token='native-unloaded',parent=parent)
    fake=FakeControl(missing_first=True)
    monkeypatch.setattr(codex_control,'CodexControlClient',lambda:fake)
    notifyd.process_registration(reg)
    final=parent_notify.read_registration(reg['eventId'])
    assert final['state']=='ready' and final.get('deliveredAtMs') is None
    assert final['payload']['report']=='ORIGINAL exact child answer'
    assert final.get('deliveryAttemptedAtMs') is None  # definite no write
    assert 'thread not found' in final['lastError']
    assert len(fake.methods('turn/start'))==1 and not fake.placed
    # Once its UI has the thread loaded again, the same event is delivered.
    fake.missing_first=False
    final=_advance(reg)
    assert final['state']=='delivered' and len(fake.placed)==1
    assert fake.methods('thread/resume')==[]


def test_any_external_deferral_expires_after_the_window(originals,monkeypatch):
    store,trace,_=originals
    parent=native_caller(monkeypatch)
    reg=_frozen_child(store,token='native-expire',parent=parent)
    fake=FakeControl(missing_first=True)
    monkeypatch.setattr(codex_control,'CodexControlClient',lambda:fake)
    notifyd.process_registration(reg)
    saved=parent_notify.read_registration(reg['eventId'])
    assert saved['state']=='ready'
    monkeypatch.setattr(notifyd,'now_ms',
        lambda:int(saved['readyAtMs'])+notifyd.EXTERNAL_PARENT_EXPIRY_MS)
    final=_advance(reg)
    assert final['state']=='undeliverable' and final['nextAttemptAtMs']==0
    assert not fake.placed and fake.methods('thread/resume')==[]


def test_w1_native_lost_ack_reconciles_same_history_and_never_resends(originals,monkeypatch):
    store,trace,_=originals
    parent=native_caller(monkeypatch)
    reg=_frozen_child(store,token='native-lost-ack',parent=parent)
    fake=FakeControl(lost_ack=True)
    monkeypatch.setattr(codex_control,'CodexControlClient',lambda:fake)
    notifyd.process_registration(reg)
    saved=parent_notify.read_registration(reg['eventId'])
    assert saved['state']=='ready' and saved['deliveryAttemptedAtMs']
    assert len(fake.placed)==1 and len(fake.methods('turn/start'))==1
    result=_advance(reg)
    assert result['state']=='delivered' and result['delivery']['reconciled'] is True
    assert result['delivery']['threadId']==RETURN_THREAD
    assert result['delivery']['deliveryId']==reg['eventId']+':settled'
    assert len(fake.placed)==1 and len(fake.methods('turn/start'))==1
    assert fake.methods('thread/items/list')


def test_w1_native_prior_attempt_unloaded_history_is_deferred_without_resume(originals,monkeypatch):
    store,trace,_=originals
    parent=native_caller(monkeypatch)
    reg=_frozen_child(store,token='native-history-missing',parent=parent)
    # Simulate a durable attempted marker after unknown control reply.
    parent_notify.mutate_registration(reg['eventId'],lambda v: v.update(
        state='ready',settlement=notify_settle.settlement_for_registration(reg),
        payload=notify_deliver.payload_for_registration(
            reg,notify_settle.settlement_for_registration(reg)),
        deliveryAttemptedAtMs=parent_notify.now_ms(),nextAttemptAtMs=0))
    fake=FakeControl(history_unloaded=True,history_absent=True)
    monkeypatch.setattr(codex_control,'CodexControlClient',lambda:fake)
    before=trace.read_text().count('RPC turn/start')
    result=_advance(reg)
    assert result['state']=='ready' and result['deliveryAttemptedAtMs']
    assert result['payload']['report']=='ORIGINAL exact child answer'
    assert len(fake.methods('thread/items/list'))==1
    assert not fake.methods('turn/start') and not fake.placed
    assert trace.read_text().count('RPC turn/start')==before
    # A second poll stays history-only and still never resumes or resends.
    next_result=_advance(reg)
    assert next_result['state']=='ready' and next_result['deliveryAttemptedAtMs']
    assert fake.methods('thread/resume')==[]
    assert not fake.methods('turn/start')
