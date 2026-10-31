"""N1A original full-UUID Codex notification: durable data, NO native wake."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.deliver as notify_deliver
import ccx.notify.settle as notify_settle
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait

import json
from pathlib import Path

import pytest

from ccx import CodexClient, constants
from ccx.codex import delivery as codex_delivery
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.notify.paths import daemon_generation_path
from ccx.ops import common as protocol_ops
from ccx.controllers import orchestrator
from ccx.errors import InvalidRequest, SessionUnavailable
from ccx.notify.client import ParentNotificationClient
from test_generation_store import A, B, C, short_root, switch_pointer
from test_codex_native_effects import _f2_fake_processes


@pytest.fixture
def originals(short_root, tmp_path, monkeypatch):
    """Actual fake app-server creation claims, never native parent delivery."""
    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        # notify_paths and constants intentionally consume different root
        # adapters; both are confined to the SAME disposable sealed root.
        monkeypatch.setenv('CCX_ROOT', str(short_root))
        monkeypatch.setattr(constants, 'CCX_ROOT', short_root)
        client = CodexClient()
        assert client.create(cwd=str(tmp_path), worker_id=A).worker_id == A
        assert client.create(cwd=str(tmp_path), worker_id=B).worker_id == B
        assert A[:8] == B[:8]
        assert trace.read_text().count('RPC thread/start') == 2
        assert 'RPC turn/start' not in trace.read_text()
        yield store, trace, tmp_path


def test_invalid_notification_delivery_id_wording_and_rpc_requires_id():
    from ccx.rpc.local_rpc import _op_turn_send

    with pytest.raises(InvalidRequest, match='valid delivery_id'):
        parent_notify.create_registration(
            parent={}, worker_engine='codex', worker_id=A,
            worker_name=None, source='send',
            operation={'kind': 'codex-turn', 'deliveryId': ''},
        )
    # The CLI-only convenience must not change RPC's persistent-token contract.
    with pytest.raises(InvalidRequest, match='delivery_id'):
        _op_turn_send({'target': 'codex:' + B, 'text': 'no id'}, timeout=1)


def test_managed_codex_parent_ref_uses_active_original_thread_only(originals,monkeypatch):
    """A real sealed worker's original native thread is identity evidence."""
    store,trace,_ = originals
    original=store.record(A)['native']
    before=trace.read_text()
    with monkeypatch.context() as env:
        for key in ('CCX_SELF_SHORT','CLAUDE_CODE_SESSION_ID'):
            env.delenv(key,raising=False)
        env.setenv('CCX_SELF_ENGINE','codex')
        env.setenv('CCX_SELF_WORKER_ID',A)
        env.setenv('CODEX_THREAD_ID',original['threadId'])
        caller=orchestrator.current_caller()
        assert caller.managed and caller.supports_parent_notify
        expected={'kind':'ccx-codex','workerId':A,
                  'threadId':original['threadId'],'generation':store.generation}
        assert caller.parent_ref()==expected
        assert orchestrator.require_notify_parent()==expected
        # Native ID is optional when the current managed stamp resolves to the
        # already bound original worker; an unowned native thread never is.
        env.delenv('CODEX_THREAD_ID')
        assert orchestrator.require_notify_parent()==expected
    assert trace.read_text()==before
    assert ParentNotificationClient().list()['count']==0


def test_invalid_codex_parent_origins_refuse_before_any_native_effect(originals,monkeypatch):
    store,trace,_ = originals
    native=store.record(A)['native']['threadId']
    other=store.record(B)['native']['threadId']
    unrelated='bbbbbbbb-1111-4000-8000-000000000077'
    bad_origins=(
        {'CODEX_THREAD_ID':native},  # N1-ext will add a typed return-address branch later
        {'CLAUDE_CODE_SESSION_ID':'native-claude-only'},
        {'CODEX_THREAD_ID':native,'CLAUDE_CODE_SESSION_ID':'ambiguous-native-pair'},
        {'CCX_SELF_SHORT':A[:8],'CODEX_THREAD_ID':native},
        {'CCX_SELF_ENGINE':'codex','CCX_SELF_WORKER_ID':A,'CODEX_THREAD_ID':other},
        {'CCX_SELF_ENGINE':'codex','CCX_SELF_WORKER_ID':unrelated,
         'CODEX_THREAD_ID':native},
        {'CCX_SELF_ENGINE':'claude','CCX_SELF_WORKER_ID':A,
         'CLAUDE_CODE_SESSION_ID':'fake-claude-native'},
    )
    before=trace.read_text()
    for fields in bad_origins:
        with monkeypatch.context() as env:
            for key in ('CCX_SELF_SHORT','CCX_SELF_ENGINE','CCX_SELF_WORKER_ID',
                        'CODEX_THREAD_ID','CLAUDE_CODE_SESSION_ID'):
                env.delenv(key,raising=False)
            for key,value in fields.items():
                env.setenv(key,value)
            with pytest.raises((InvalidRequest,SessionUnavailable)):
                orchestrator.require_notify_parent()
        assert trace.read_text()==before
        assert ParentNotificationClient().list()['count']==0


def test_managed_caller_context_cannot_substitute_another_native_thread(originals):
    store,trace,_ = originals
    before=trace.read_text()
    # Even a managed-looking context object cannot impersonate the original
    # thread of a second colliding-prefix worker. Immutable native fields
    # cannot be rewritten through the worker record mutation API.
    forged=orchestrator.CallerContext(
        'codex', native_id=store.record(B)['native']['threadId'],
        worker_id=A, managed=True,
    )
    assert forged.supports_parent_notify
    with pytest.raises(SessionUnavailable,match='caller does not match'):
        forged.parent_ref()
    assert trace.read_text()==before
    assert ParentNotificationClient().list()['count']==0


def prepare(store, token='n1a-semantic-1', text='one accepted child turn'):
    child = 'codex:'+B
    codex_delivery.prepare(child, token, text, mode='new_turn_only')
    parent = {'kind': 'ccx-codex', 'workerId': A,
              'threadId': store.record(A)['native']['threadId'],
              'generation': store.generation}
    reg = parent_notify.create_registration(
        parent=parent, worker_engine='codex', worker_id=B,
        worker_name='Codex child', operation={'kind': 'codex-turn',
                    'deliveryId': token}, source='send')
    return reg, parent


def accept(token='n1a-semantic-1', turn='n1a-native-turn-1'):
    child='codex:'+B
    codex_delivery.mark_submitted(child, token)
    return codex_delivery.mark_accepted(child, token, turn_id=turn)


def complete(store, token='n1a-semantic-1', turn='n1a-native-turn-1',
             text='EXACT settled native assistant message'):
    accept(token, turn)
    return codex_delivery.mark_settled('codex:'+B, token, turn_id=turn,
        original_positive_completion={
            'workerId': B, 'engine': 'codex', 'deliveryId': token,
            'turnId': turn, 'threadId': store.record(B)['native']['threadId'],
            'nativeSource': 'ccx-worker:'+B, 'status':'completed',
            'text':text, 'nativeEvidenceSha256':'a'*64})


def test_registration_exact_full_ids_fsynced_reuses_progress_and_conflicts(originals):
    store,trace,_ = originals
    reg,parent=prepare(store)
    path=parent_notify.registration_path(reg['eventId'])
    assert path.is_file() and json.loads(path.read_text())['eventId']==reg['eventId']
    assert reg['worker']['workerId']==B and reg['parent']['workerId']==A
    assert reg['worker'].get('short') is None
    assert reg['generation']==store.generation and reg['operation']['deliveryId']=='n1a-semantic-1'
    assert not daemon_generation_path().exists()  # no N1A supervisor launch
    assert 'RPC turn/start' not in trace.read_text()
    parent_notify.mutate_registration(reg['eventId'],
       lambda row: row.update(state='ready',settlement={'status':'rejected'},
                              readyAtMs=1234))
    # Display renaming is not a change of the semantic owner or receipt.
    renamed=parent_notify.create_registration(parent=parent,worker_engine='codex',
        worker_id=B,worker_name='renamed display only',
        operation={'kind':'codex-turn','deliveryId':'n1a-semantic-1'},source='send')
    same,_=prepare(store)
    assert renamed['eventId']==same['eventId']==reg['eventId'] and same['state']=='ready'
    assert same['readyAtMs']==1234 and same['settlement']=={'status':'rejected'}
    forged={**parent,'workerId': B, 'threadId': store.record(B)['native']['threadId']}
    with pytest.raises((InvalidRequest,SessionUnavailable)):
        parent_notify.create_registration(parent=forged,worker_engine='codex',
            worker_id=B,worker_name='Codex child',operation={'kind':'codex-turn',
                'deliveryId':'n1a-semantic-1'},source='send')
    # A second *real* original parent cannot arm the same child delivery.
    assert CodexClient().create(cwd=str(originals[2]),worker_id=C).worker_id==C
    from_other_parent={**parent,'workerId':C,
        'threadId':store.record(C)['native']['threadId']}
    with pytest.raises(InvalidRequest,match='different parent'):
        parent_notify.create_registration(parent=from_other_parent,
            worker_engine='codex',worker_id=B,worker_name='Codex child',
            operation={'kind':'codex-turn','deliveryId':'n1a-semantic-1'},source='send')
    with pytest.raises(InvalidRequest,match='conflicts'):
        parent_notify.create_registration(parent=parent,worker_engine='codex',
            worker_id=B,worker_name='Codex child',operation={'kind':'codex-turn',
                'deliveryId':'n1a-semantic-1','promptSha256':'0'*64},source='send')
    assert 'RPC turn/start' not in trace.read_text()


def test_only_original_frozen_receipt_can_supply_completed_report(originals, monkeypatch):
    store,trace,_=originals
    reg,_=prepare(store)
    complete(store)
    parent_notify.set_codex_turn_id(reg['eventId'], 'n1a-native-turn-1')
    with pytest.raises(InvalidRequest, match='differs from original'):
        parent_notify.set_codex_turn_id(reg['eventId'], 'another-turn')
    # Last-turn/report can already belong to a different later turn.
    store.mutate_record('codex:'+B,patches={'lastTurn':{
        'turnId':'newer-turn','status':'completed'},
        'lastReport':{'turnId':'newer-turn','text':'WRONG LATEST'}})
    assert notify_settle.settlement_for_registration(reg)['report']==\
           'EXACT settled native assistant message'
    def no_parent_delivery(*_args,**_kwargs):
        # This original parent has no subscribed Worker; refusal is positively
        # before any native mutation, so the event remains eligible for later.
        error=notify_deliver.DeliveryDeferred('original parent is not loaded')
        error.no_effect=True
        raise error
    monkeypatch.setattr(notifyd,'deliver_to_parent',no_parent_delivery)
    notifyd.process_registration(reg)
    final=parent_notify.read_registration(reg['eventId'])
    assert final['state']=='ready' and final['settlement']['status']=='completed'
    assert final['payload']['report']=='EXACT settled native assistant message'
    assert final['payload']['worker']=='codex:'+B
    assert final['payload']['parent']=='codex:'+A
    assert ParentNotificationClient().get(reg['eventId'])['turn_id']=='n1a-native-turn-1'
    assert final.get('deliveryAttemptedAtMs') is None
    assert final['state']=='ready' and final['lastError']
    notifyd.process_registration(final)
    assert parent_notify.read_registration(reg['eventId'])['payload']==final['payload']
    assert trace.read_text().count('RPC thread/start')==2
    assert 'RPC turn/start' not in trace.read_text()


@pytest.mark.parametrize('outcome',['failed','interrupted','cancelled'])
def test_exact_noncompleted_native_outcome_has_null_report(originals,monkeypatch,outcome):
    store,trace,_=originals
    reg,_=prepare(store)
    accept()
    def exact_reconcile(worker,*,delivery_id,**kwargs):
        assert worker=='codex:'+B and delivery_id=='n1a-semantic-1'
        return {'worker':{'worker_id':B},'delivery':{
            'delivery_id':delivery_id,'turn_id':'n1a-native-turn-1'},
            'native_status':outcome,'native_error':{'message':'original '+outcome}}
    monkeypatch.setattr(ops_wait,'reconcile_turn',exact_reconcile)
    settled=notify_settle.settlement_for_registration(reg)
    assert settled['status']==outcome and settled['turnId']=='n1a-native-turn-1'
    assert settled['report'] is None and settled['turnStatus']==outcome
    assert 'RPC turn/start' not in trace.read_text()


def test_submitted_unknown_and_foreign_turn_never_complete(originals,monkeypatch):
    store,trace,_=originals
    reg,_=prepare(store)
    codex_delivery.mark_submitted('codex:'+B,'n1a-semantic-1')
    monkeypatch.setattr(ops_wait,'reconcile_turn',lambda *_a,**_k:
        (_ for _ in ()).throw(AssertionError('submitted-unknown cannot observe as terminal')))
    assert notify_settle.settlement_for_registration(reg) is None
    codex_delivery.mark_accepted('codex:'+B,'n1a-semantic-1',turn_id='n1a-native-turn-1')
    monkeypatch.setattr(ops_wait,'reconcile_turn',lambda *_a,**_k: {
        'worker':{'worker_id':B},'delivery':{
            'delivery_id':'n1a-semantic-1','turn_id':'FOREIGN-TURN'},
        'native_status':'interrupted'})
    monkeypatch.setattr(ops_wait,'wait_worker',lambda *_a,**_k: {
        'worker':'codex:'+B,'state':'waiting',
        'active_turn_ref':'ccx-turn:codex:v1:FOREIGN-TURN',
        'pendingApprovals':[{'requestId':'obsolete-gate'}]})
    assert notify_settle.settlement_for_registration(reg) is None
    assert 'RPC turn/start' not in trace.read_text()


def test_current_exact_gate_freezes_waiting_but_old_gate_cannot(originals,monkeypatch):
    store,_,_=originals
    reg,_=prepare(store)
    accept()
    monkeypatch.setattr(ops_wait,'reconcile_turn',lambda *_a,**_k: {
        'worker':{'worker_id':B},'delivery':{
            'delivery_id':'n1a-semantic-1','turn_id':'n1a-native-turn-1'},
        'status':'accepted'})
    monkeypatch.setattr(ops_wait,'wait_worker',lambda *_a,**_k: {
        'worker':'codex:'+B, 'state':'waiting',
        'active_turn_ref':'ccx-turn:codex:v1:n1a-native-turn-1',
        'pendingApprovals':[{'requestId':'current-gate'}]})
    waiting=notify_settle.settlement_for_registration(reg)
    assert waiting['status']=='waiting' and waiting['waitingFor'][0]['requestId']=='current-gate'
    assert waiting['report'] is None


def test_stale_short_and_foreign_generation_never_native_or_public(originals,monkeypatch):
    store,trace,_=originals
    reg,_=prepare(store)
    from ccx.notify.paths import registration_path
    old_event='00000000-0000-4000-8000-000000000042'
    old={'eventId':old_event, 'version':2,'state':'ready',
         'worker':{'engine':'codex','short':'aaaaaaaa'},
         'parent':{'kind':'codex','threadId':'external-native'},
         'payload':{'eventId':'legacy-unsafe'},'operation':{}}
    registration_path(old_event).write_text(json.dumps(old))
    observed=[]
    monkeypatch.setattr(notifyd,'deliver_to_parent',lambda *_a,**_k:
       observed.append('native-wake'))
    notifyd.process_registration(old)
    assert not observed
    assert [v['eventId'] for v in parent_notify.iter_registrations()]==[reg['eventId']]
    with pytest.raises(SessionUnavailable):
        ParentNotificationClient().get(old_event)
    switch_pointer(store.root)
    assert parent_notify.iter_registrations()==[]
    with pytest.raises(SessionUnavailable):
        ParentNotificationClient().get(reg['eventId'])
    notifyd.process_registration(reg)
    assert not observed
    assert 'RPC turn/start' not in trace.read_text()


def test_public_list_get_cancel_original_full_uuid_only(originals):
    store,trace,_=originals
    reg,_=prepare(store)
    public=ParentNotificationClient()
    one=public.get(reg['eventId'])
    assert one['worker']=='codex:'+B and one['parent']=='codex:'+A
    assert one['resume_automatically'] is False and one['delivery_available'] is True
    assert 'CCX will try to notify the calling Codex worker' in one['instruction']
    assert 'awaits N1B' not in one['instruction']
    assert one['delivery_id']=='n1a-semantic-1'
    assert public.list(worker='codex:'+B)['count']==1
    assert public.list(worker='codex:'+A)['count']==0
    with pytest.raises(InvalidRequest,match='FULL_UUID'):
        public.list(worker='codex:aaaaaaaa')
    with pytest.raises(InvalidRequest,match='canonical UUID'):
        public.get('../unowned-file')
    first=public.cancel(reg['eventId'],reason='N1A stop')
    second=public.cancel(reg['eventId'],reason='another')
    assert first['state']==second['state']=='cancelled'
    assert first['delivery_available'] is False
    assert 'child delivery is unaffected' in first['instruction']
    assert parent_notify.read_registration(reg['eventId'])['cancelReason']=='N1A stop'
    assert public.list()['count']==0
    assert public.list(include_terminal=True)['count']==1
    assert 'RPC turn/start' not in trace.read_text()


def test_stopped_child_still_readable_only_and_never_native_wake(originals,monkeypatch):
    from ccx import WorkerClient
    store,trace,_=originals
    reg,_=prepare(store)
    stopped=WorkerClient().kill('codex:'+B)
    assert stopped['killed'] is True
    before=trace.read_text()
    assert ParentNotificationClient().get(reg['eventId'])['worker']=='codex:'+B
    assert notify_settle.settlement_for_registration(reg) is None
    monkeypatch.setattr(notifyd,'deliver_to_parent',lambda *_args,**_kwargs:
        (_ for _ in ()).throw(AssertionError('must not call native parent wake')))
    notifyd.process_registration(reg)
    assert parent_notify.read_registration(reg['eventId'])['state']=='armed'
    _assert_no_new_native_effects_after_stop(before, trace.read_text())  # no wake


def test_bad_row_settlement_failure_does_not_stop_later_rows(originals,monkeypatch):
    from ccx.store.root import GenerationError
    store,trace,_=originals
    bad,_=prepare(store,token='n1a-bad-row')
    accept('n1a-bad-row','n1a-bad-turn')
    good,_=prepare(store,token='n1a-good-row')
    complete(store,'n1a-good-row','n1a-good-turn',text='good exact report')
    def reconcile(worker,*,delivery_id,**_kwargs):
        assert delivery_id=='n1a-bad-row'
        raise GenerationError('row-local generation read failure')
    monkeypatch.setattr(ops_wait,'reconcile_turn',reconcile)
    snapshot=parent_notify.iter_registrations(include_terminal=True)
    # Same per-row sequence as notifyd.run(): the bad row goes first.
    for row in [parent_notify.read_registration(bad['eventId']),
                parent_notify.read_registration(good['eventId'])]:
        notifyd.process_registration(row,registration_snapshot=snapshot)
    after_bad=parent_notify.read_registration(bad['eventId'])
    assert after_bad['state']=='armed' and after_bad.get('payload') is None
    assert after_bad.get('settlement') is None
    after_good=parent_notify.read_registration(good['eventId'])
    # A promptless original Codex parent is now legitimately IDLE, not
    # unready: the healthy good row must finish parent notification.
    assert after_good['state']=='delivered'
    assert after_good['payload']['report']=='good exact report'
    import ast
    native_starts = [
        ast.literal_eval(line.removeprefix('RPC turn/start '))
        for line in trace.read_text().split('\\n')
        if line.startswith('RPC turn/start ')
    ]
    assert len(native_starts) == 1
    wake = native_starts[0]
    assert wake['threadId'] == store.record(A)['native']['threadId']
    assert wake['turnTrigger'] == 'ccx-worker-notification'
    assert wake['toolOutput']['name'] == 'worker_notification'
    details = json.loads(wake['toolOutput']['output'])
    assert details['eventId'] == good['eventId']
    assert details['report'] == 'good exact report'
    assert bad['eventId'] not in trace.read_text()


def _machine_provider(monkeypatch):
    """Test-local app-server support for original paged machine output."""
    import test_codex_native_effects as fake
    original = fake._fake_codex_executable

    def create(*args, **kwargs):
        binary = original(*args, **kwargs)
        text = binary.read_text()
        substitutions = (
            ("counter=len(threads)\n", "counter=len(threads)\nsubscribed=set()\n"),
            ("            threads[tid]={'id':tid,'threadSource':params.get('threadSource'),\n"
             "                          'model':params.get('model') or 'gpt-fake',\n"
             "                          'turns':[]}\n",
             "            threads[tid]={'id':tid,'threadSource':params.get('threadSource'),\n"
             "                          'model':params.get('model') or 'gpt-fake',\n"
             "                          'turns':[]}\n"
             "            subscribed.add(tid)\n"),
            ("        elif method=='thread/read':\n",
             "        elif method=='thread/resume':\n"
             "            tid=params.get('threadId')\n"
             "            if tid not in threads: raise ValueError('unknown original thread')\n"
             "            if not threads[tid]['turns']: raise ValueError('no rollout found')\n"
             "            subscribed.add(tid)\n"
             "            result={'thread':dict(threads[tid],status={'type':'idle'})}\n"
             "        elif method=='thread/read':\n"),
            ("        elif method=='thread/unsubscribe':\n            result={}\n",
             "        elif method=='thread/unsubscribe':\n"
             "            subscribed.discard(params.get('threadId'))\n"
             "            result={}\n"),
            ("            if tid not in threads: raise ValueError('unknown turn thread')\n",
             "            if tid not in threads: raise ValueError('unknown turn thread')\n"
             "            if tid not in subscribed: raise ValueError('thread is not subscribed')\n"),
            ("        elif method=='turn/start':\n",
             "        elif method=='thread/items/list':\n"
             "            tid=params.get('threadId')\n"
             "            if tid not in threads: raise ValueError('unknown history thread')\n"
             "            rows=[{'turnId':turn['id'],'item':item}\n"
             "                  for turn in threads[tid]['turns']\n"
             "                  if not params.get('turnId') or turn['id']==params['turnId']\n"
             "                  for item in (turn.get('items') or [])]\n"
             "            if params.get('sortDirection')=='desc': rows.reverse()\n"
             "            result={'data':rows,'nextCursor':None}\n"
             "        elif method=='turn/start':\n"),
            ("            items=[] if text.startswith('initial-wait-empty') else [\n"
             "                {'type':'agentMessage','text':answer}]\n",
             "            machine=params.get('toolOutput')\n"
             "            items=([{'type':'functionCallOutput',\n"
             "                       'name':machine.get('name'),\n"
             "                       'output':machine.get('output')}]\n"
             "                   if isinstance(machine,dict) else\n"
             "                   [] if text.startswith('initial-wait-empty') else\n"
             "                   [{'type':'agentMessage','text':answer}])\n"),
            ("        if method=='turn/start':\n",
             "        if method=='turn/start' and not text.startswith('busy-hold-'):\n"),
        )
        for before,after in substitutions:
            assert text.count(before)==1, before
            text=text.replace(before,after,1)
        binary.write_text(text)
        return binary
    monkeypatch.setattr(fake, '_fake_codex_executable', create)


@pytest.fixture
def machine_originals(short_root,tmp_path,monkeypatch):
    from test_codex_native_effects import _f2_fake_processes
    _machine_provider(monkeypatch)
    with _f2_fake_processes(short_root,tmp_path,monkeypatch) as (
        cli,restart,trace,store,daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN',str(tmp_path/'fake-codex'))
        monkeypatch.setenv('CCX_ROOT',str(short_root))
        monkeypatch.setattr(constants,'CCX_ROOT',short_root)
        client=CodexClient()
        assert client.create(cwd=str(tmp_path),worker_id=A).worker_id==A
        assert client.send('codex:'+A,'historic original parent',
            delivery_id='n1b-parent-prior',wait=True,timeout=15).turn_id
        assert client.create(cwd=str(tmp_path),worker_id=B).worker_id==B
        yield client,store,trace,daemon_rpc


def _settled_ready(client,store,token='n1a-semantic-1'):
    reg,_=prepare(store,token=token)
    assert client.send('codex:'+B,'one accepted child turn' if token=='n1a-semantic-1'
                       else 'one accepted child turn',
                       delivery_id=token,wait=True,timeout=15).turn_id
    return reg


def test_original_parent_machine_output_once_and_history_same_native_id(machine_originals):
    client,store,trace,daemon_rpc=machine_originals
    reg=_settled_ready(client,store)
    parent_thread=store.record(A)['native']['threadId']
    child_thread=store.record(B)['native']['threadId']
    assert parent_thread != child_thread and A[:8]==B[:8]
    before=trace.read_text().count('RPC turn/start')
    assert before==2
    notifyd.process_registration(reg)
    after=parent_notify.read_registration(reg['eventId'])
    assert after['state']=='delivered',after
    projected=ParentNotificationClient().get(reg['eventId'])
    assert projected['delivery_available'] is True
    assert projected['resume_automatically'] is False
    assert 'parent model completion is unproven' in projected['instruction']
    assert projected['settlement']['report']=='Fake Codex completed: one accepted child turn'
    assert after['delivery']['ok'] and after['delivery']['workerId']==A
    assert after['delivery']['threadId']==parent_thread
    assert after['delivery']['deliveryId']==reg['eventId']+':settled'
    assert after['payload']['worker']=='codex:'+B
    assert after['payload']['report']=='Fake Codex completed: one accepted child turn'
    assert trace.read_text().count('RPC turn/start')==before+1
    assert 'RPC thread/start' in trace.read_text()
    content=trace.read_text()
    # The disposable fake logs literal escaped separators, not LF-delimited
    # RPC lines. Bound the exact request by the next RPC marker.
    prefix="RPC turn/start {'threadId': '"+parent_thread+"', 'input': []"
    assert content.count(prefix)==1,content
    machine=content.split(prefix,1)[1].split('RPC ',1)[0]
    assert "'name': 'worker_notification'" in machine
    assert child_thread not in machine
    assert "'turnTrigger': 'ccx-worker-notification'" in machine
    assert 'RPC thread/resume' not in content
    # The sibling/child original provider thread was never addressed for wake.
    assert store.record(B)['native']['threadId']==child_thread


def test_busy_parent_machine_start_or_steer_not_deferred(machine_originals):
    import time
    client,store,trace,daemon_rpc=machine_originals
    reg=_settled_ready(client,store)
    started=client.send('codex:'+A,'busy-hold-parent',
         delivery_id='n1b-busy-parent',wait=False,timeout=15)
    assert started.turn_id
    end=time.monotonic()+3
    while time.monotonic()<end:
        reply=daemon_rpc({'op':'status','short':A,
             'rpcGeneration':constants.CODEXD_RPC_GENERATION},timeout=1)
        if reply.get('state')=='busy':
            break
        time.sleep(.03)
    assert reply['state']=='busy',reply
    before=trace.read_text().count('RPC turn/start')
    notifyd.process_registration(reg)
    final=parent_notify.read_registration(reg['eventId'])
    assert final['state']=='delivered',final
    assert trace.read_text().count('RPC turn/start')==before+1


def test_lost_ack_reconciles_exact_parent_machine_history_and_never_restarts(machine_originals):
    client,store,trace,daemon_rpc=machine_originals
    reg=_settled_ready(client,store)
    notifyd.process_registration(reg)
    delivered=parent_notify.read_registration(reg['eventId'])
    assert delivered['state']=='delivered'
    before=trace.read_text().count('RPC turn/start')
    # Crash after the exact native placement but before the durable delivered
    # marker: keep only the N1A persisted attempted marker and frozen payload.
    def simulate_lost_ack(row):
        row['state']='ready'
        row.pop('delivery',None)
        row.pop('deliveredAtMs',None)
        row['deliveryAttemptedAtMs']=parent_notify.now_ms()
        row['nextAttemptAtMs']=0
    parent_notify.mutate_registration(reg['eventId'],simulate_lost_ack)
    notifyd.process_registration(parent_notify.read_registration(reg['eventId']))
    final=parent_notify.read_registration(reg['eventId'])
    assert final['state']=='delivered' and final['delivery']['reconciled'] is True,final
    assert trace.read_text().count('RPC turn/start')==before
    assert "'threadId': '"+store.record(A)['native']['threadId']+"'" in trace.read_text()
    assert trace.read_text().count('RPC thread/items/list')>=1


def test_prior_attempt_with_no_positive_history_never_resends(machine_originals):
    client,store,trace,daemon_rpc=machine_originals
    reg=_settled_ready(client,store)
    snapshot=notifyd._mark_ready(reg['eventId'],
       notify_settle.settlement_for_registration(reg))
    assert snapshot['state']=='ready'
    parent_notify.mutate_registration(reg['eventId'],lambda row: row.update(
        deliveryAttemptedAtMs=parent_notify.now_ms(),nextAttemptAtMs=0))
    before=trace.read_text().count('RPC turn/start')
    notifyd.process_registration(parent_notify.read_registration(reg['eventId']))
    after=parent_notify.read_registration(reg['eventId'])
    assert after['state']=='ready' and after['lastError']
    assert after['deliveryAttemptedAtMs']
    assert trace.read_text().count('RPC turn/start')==before
    assert 'RPC thread/items/list' in trace.read_text()


def test_unloaded_parent_and_wrong_parent_native_dispatch_have_no_effect(machine_originals):
    from ccx import WorkerClient
    client,store,trace,daemon_rpc=machine_originals
    reg=_settled_ready(client,store)
    frozen=notifyd._mark_ready(reg['eventId'],
       notify_settle.settlement_for_registration(reg))
    old=trace.read_text().count('RPC turn/start')
    wrong=daemon_rpc({'op':'parent-notify','short':B,
        'rpcGeneration':constants.CODEXD_RPC_GENERATION,
        'eventId':reg['eventId'],'deliveryId':frozen['payload']['deliveryId'],
        'payload':frozen['payload'],'reconcile':False},timeout=3)
    assert wrong['ok'] is False
    forged={**frozen['payload'],'report':'FOREIGN-FROZEN-TEXT'}
    changed=daemon_rpc({'op':'parent-notify','short':A,
        'rpcGeneration':constants.CODEXD_RPC_GENERATION,
        'eventId':reg['eventId'],'deliveryId':forged['deliveryId'],
        'payload':forged,'reconcile':False},timeout=3)
    assert changed['ok'] is False
    assert trace.read_text().count('RPC turn/start')==old
    assert WorkerClient().kill('codex:'+A)['killed']
    notifyd.process_registration(frozen)
    saved=parent_notify.read_registration(reg['eventId'])
    assert saved['state']=='ready' and saved.get('deliveredAtMs') is None
    assert saved.get('deliveryAttemptedAtMs') is None  # positive no-native refusal
    assert trace.read_text().count('RPC turn/start')==old
    assert 'RPC thread/resume' not in trace.read_text()


def test_refused_reconcile_only_retry_keeps_attempted_marker(machine_originals,monkeypatch):
    from ccx import WorkerClient
    from ccx.codex import runtime as codex_runtime
    client,store,trace,_daemon_rpc=machine_originals
    reg=_settled_ready(client,store)
    notifyd.process_registration(reg)
    prefix=("RPC turn/start {'threadId': '"+store.record(A)['native']['threadId']
            +"', 'input': []")
    assert trace.read_text().count(prefix)==1
    def lost_ack(row):
        row['state']='ready'
        row.pop('delivery',None)
        row.pop('deliveredAtMs',None)
        row['deliveryAttemptedAtMs']=parent_notify.now_ms()
        row['nextAttemptAtMs']=0
    def retry():
        notifyd.process_registration(parent_notify.read_registration(reg['eventId']))
        return parent_notify.read_registration(reg['eventId'])
    parent_notify.mutate_registration(reg['eventId'],lost_ack)
    real=codex_runtime.daemon_rpc
    monkeypatch.setattr(codex_runtime,'daemon_rpc',
                        lambda *_a,**_k: {'transport':'absent','ok':False})
    absent=retry()
    assert absent['state']=='ready' and absent['deliveryAttemptedAtMs']
    monkeypatch.setattr(codex_runtime,'daemon_rpc',real)
    parent_notify.mutate_registration(reg['eventId'],lambda row: row.update(nextAttemptAtMs=0))
    healed=retry()  # still reconcile-only: history proves the first wake
    assert healed['state']=='delivered' and healed['delivery']['reconciled'] is True
    parent_notify.mutate_registration(reg['eventId'],lost_ack)
    assert WorkerClient().kill('codex:'+A)['killed']
    killed=retry()
    assert killed['state']=='ready' and killed['deliveryAttemptedAtMs']
    assert trace.read_text().count(prefix)==1  # never a second machine wake


def test_idle_unloaded_original_parent_reloads_same_native_thread_then_one_wake(
    short_root,tmp_path,monkeypatch,
):
    """The real codexd janitor unsubscribes an idle original, not a kill."""
    import time
    from test_codex_native_effects import _f2_fake_processes
    _machine_provider(monkeypatch)
    with _f2_fake_processes(short_root,tmp_path,monkeypatch) as (
        _cli,_restart,trace,store,_daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN',str(tmp_path/'fake-codex'))
        monkeypatch.setenv('CCX_ROOT',str(short_root))
        monkeypatch.setattr(constants,'CCX_ROOT',short_root)
        client=CodexClient()
        assert client.create(cwd=str(tmp_path),worker_id=A,idle_timeout=1).worker_id==A
        assert client.send('codex:'+A,'original parent historical answer',
            delivery_id='n1b-idle-parent-prior',wait=True,timeout=15).turn_id
        original_id=store.record(A)['native']['threadId']
        assert client.create(cwd=str(tmp_path),worker_id=B).worker_id==B
        reg=_settled_ready(client,store)
        # Wait for the *actual* janitor unload, never edit lifecycle records
        # and never reuse explicit kill as a fake idle-unload oracle.
        limit=time.monotonic()+9
        while time.monotonic()<limit:
            if store.record(A)['lifecycle']['state']=='idle-killed':
                break
            time.sleep(.08)
        assert store.record(A)['lifecycle']['state']=='idle-killed'
        # The durable idle-killed state is written before the async provider
        # unsubscribe. Wait for the actual native request, not a fixed delay.
        unsubscribe = "RPC thread/unsubscribe {'threadId': '"+original_id+"'"
        limit = time.monotonic()+10
        while unsubscribe not in trace.read_text() and time.monotonic()<limit:
            time.sleep(.05)
        assert unsubscribe in trace.read_text()
        before=trace.read_text().count('RPC turn/start')
        initial_starts=trace.read_text().count('RPC thread/start')
        notifyd.process_registration(reg)
        final=parent_notify.read_registration(reg['eventId'])
        assert final['state']=='delivered',final
        assert final['delivery']['threadId']==original_id
        assert final['delivery']['workerId']==A
        assert trace.read_text().count('RPC thread/resume')==1
        assert "'threadId': '"+original_id+"'" in trace.read_text()
        assert trace.read_text().count('RPC thread/start')==initial_starts
        assert trace.read_text().count('RPC turn/start')==before+1
        assert store.record(A)['native']['threadId']==original_id
        assert final['payload']['report']=='Fake Codex completed: one accepted child turn'
        assert store.record(B)['native']['threadId']!=original_id


def test_explicit_kill_keeps_wake_pending_until_manual_same_thread_resume(
    machine_originals,
):
    from ccx import WorkerClient
    client,store,trace,_daemon_rpc=machine_originals
    reg=_settled_ready(client,store)
    parent_thread=store.record(A)['native']['threadId']
    before=trace.read_text().count('RPC turn/start')
    killed=WorkerClient().kill('codex:'+A)
    assert killed['killed'] is True and store.record(A)['lifecycle']['state']=='dead'
    notifyd.process_registration(reg)
    held=parent_notify.read_registration(reg['eventId'])
    assert held['state']=='ready' and held['payload']['report']==        'Fake Codex completed: one accepted child turn'
    assert held.get('deliveryAttemptedAtMs') is None
    assert held.get('deliveredAtMs') is None
    assert trace.read_text().count('RPC thread/resume')==0
    assert trace.read_text().count('RPC turn/start')==before
    # Only an independent public resume may make the previously killed parent
    # subscribed again. Notifyd does NOT initiate this transition itself.
    manual=client.resume('codex:'+A,timeout=12)
    assert manual.worker_id==A and manual.thread_id==parent_thread
    assert manual.resumed is True and manual.state=='idle'
    assert trace.read_text().count('RPC thread/resume')==1
    parent_notify.mutate_registration(reg['eventId'],
        lambda row: row.update(nextAttemptAtMs=0))  # existing later poll
    notifyd.process_registration(parent_notify.read_registration(reg['eventId']))
    final=parent_notify.read_registration(reg['eventId'])
    assert final['state']=='delivered',final
    assert final['eventId']==reg['eventId']
    assert final['payload']['deliveryId']==reg['eventId']+':settled'
    assert final['delivery']['threadId']==parent_thread
    assert trace.read_text().count('RPC thread/resume')==1
    assert trace.read_text().count('RPC turn/start')==before+1
    notifyd.process_registration(final)
    assert trace.read_text().count('RPC turn/start')==before+1


def _idle_parent_world(short_root,tmp_path,monkeypatch,stack,idle_timeout=1):
    from test_codex_native_effects import _f2_fake_processes
    _machine_provider(monkeypatch)
    _cli,_restart,trace,store,daemon_rpc=stack.enter_context(
        _f2_fake_processes(short_root,tmp_path,monkeypatch))
    monkeypatch.setenv('CCX_CODEX_BIN',str(tmp_path/'fake-codex'))
    monkeypatch.setenv('CCX_ROOT',str(short_root))
    monkeypatch.setattr(constants,'CCX_ROOT',short_root)
    client=CodexClient()
    assert client.create(cwd=str(tmp_path),worker_id=A,
                         idle_timeout=idle_timeout).worker_id==A
    assert client.send('codex:'+A,'original parent historical answer',
        delivery_id='n1b-load-parent-prior',wait=True,timeout=15).turn_id
    assert client.create(cwd=str(tmp_path),worker_id=B).worker_id==B
    return client,store,trace,daemon_rpc


def _until_state(store,wanted,seconds=9):
    import time
    limit=time.monotonic()+seconds
    while store.record(A)['lifecycle']['state']!=wanted and time.monotonic()<limit:
        time.sleep(.05)
    assert store.record(A)['lifecycle']['state']==wanted


def test_daemon_stop_keeps_idle_parent_reloadable_but_absent_kill_is_dead(
    short_root,tmp_path,monkeypatch,
):
    import contextlib
    from ccx import WorkerClient
    with contextlib.ExitStack() as stack:
        client,store,trace,daemon_rpc=_idle_parent_world(
            short_root,tmp_path,monkeypatch,stack,idle_timeout=3600)
        reg=_settled_ready(client,store)
        assert store.record(A)['lifecycle']['state']=='idle'  # loaded, no janitor
        assert daemon_rpc({'op':'daemon-stop'},timeout=5)['ok']  # served, no kill
        _until_state(store,'idle-killed')  # an exit unloads idle; it is not `dead`
        import time
        from ccx.codex import runtime as codex_runtime
        limit=time.monotonic()+5
        while (codex_runtime.daemon_rpc({'op':'status','short':A,
                   'rpcGeneration':constants.CODEXD_RPC_GENERATION},timeout=1
               ).get('transport')!='absent' and time.monotonic()<limit):
            time.sleep(.05)
        # The singleton is now absent: an explicit kill must still record `dead`.
        first=WorkerClient().kill('codex:'+A)
        assert first['killed'] is True and first['changed'] is True
        assert store.record(A)['lifecycle']['state']=='dead'
        again=WorkerClient().kill('codex:'+A)
        assert again['changed'] is False and store.record(A)['lifecycle']['state']=='dead'
        resumes=trace.read_text().count('RPC thread/resume')
        notifyd.process_registration(reg)
        held=parent_notify.read_registration(reg['eventId'])
        assert held['state']=='ready' and 'dead' in held['lastError']
        assert trace.read_text().count('RPC thread/resume')==resumes  # no autoload


def test_explicit_kill_after_janitor_unload_stays_pending_until_live(
    short_root,tmp_path,monkeypatch,
):
    import contextlib
    from ccx import WorkerClient
    with contextlib.ExitStack() as stack:
        client,store,trace,_daemon_rpc=_idle_parent_world(
            short_root,tmp_path,monkeypatch,stack)
        reg=_settled_ready(client,store)
        parent_thread=store.record(A)['native']['threadId']
        _until_state(store,'idle-killed')  # the real janitor unload
        killed=WorkerClient().kill('codex:'+A)  # served daemon, worker not loaded
        assert killed['killed'] is True and killed['changed'] is True
        assert store.record(A)['lifecycle']['state']=='dead'
        assert WorkerClient().kill('codex:'+A)['changed'] is False
        before=trace.read_text().count('RPC turn/start')
        notifyd.process_registration(reg)
        held=parent_notify.read_registration(reg['eventId'])
        assert held['state']=='ready' and held.get('deliveryAttemptedAtMs') is None
        assert trace.read_text().count('RPC thread/resume')==0
        assert trace.read_text().count('RPC turn/start')==before
        manual=client.resume('codex:'+A,timeout=12)  # independent explicit live
        assert manual.thread_id==parent_thread and manual.state=='idle'
        parent_notify.mutate_registration(reg['eventId'],
            lambda row: row.update(nextAttemptAtMs=0))
        notifyd.process_registration(parent_notify.read_registration(reg['eventId']))
        final=parent_notify.read_registration(reg['eventId'])
        assert final['state']=='delivered' and final['delivery']['threadId']==parent_thread
        assert final['payload']['deliveryId']==reg['eventId']+':settled'
        assert trace.read_text().count('RPC turn/start')==before+1


def _n1c_managed_parent(monkeypatch, store, worker_id=A):
    """Only original generated thread environment stamps, not native targeting."""
    for key in ('CCX_SELF_SHORT', 'CLAUDE_CODE_SESSION_ID'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv('CCX_SELF_ENGINE', 'codex')
    monkeypatch.setenv('CCX_SELF_WORKER_ID', worker_id)
    monkeypatch.setenv('CODEX_THREAD_ID', store.record(worker_id)['native']['threadId'])


def _n1c_cli_send_args(token, *, prompt='N1C child original prompt', notify=True):
    from argparse import Namespace
    return Namespace(target='codex:'+B, text=prompt, prompt=None,
                     stdin=False, file=None, delivery_id=token,
                     notify_parent=notify, no_wait=False, wait=False,
                     timeout=12, json=True, settle=False,
                     var=None, vars_from=None)


@pytest.mark.parametrize('json_mode', [False, True])
def test_codex_cli_notify_parent_generates_child_id_and_keeps_wake_distinct(
    machine_originals, monkeypatch, capsys, json_mode,
):
    """Mock Codex parent, fake provider: both CLI output forms expose one UUID."""
    from uuid import UUID
    from ccx.cli import codex as codex_commands

    _client, store, trace, _daemon = machine_originals
    _n1c_managed_parent(monkeypatch, store)
    monkeypatch.setattr(notify_daemon, 'ensure_notify_daemon',
                        lambda **_kw: None)
    original = trace.read_text().count('RPC turn/start')
    args = _n1c_cli_send_args(None)
    args.json = json_mode
    codex_commands.cmd_send(args)
    printed = capsys.readouterr().out
    token = (json.loads(printed)['delivery_id'] if json_mode else
             printed.split('delivery_id: ', 1)[1].splitlines()[0])
    assert UUID(token).version == 4
    assert token in printed
    rows = parent_notify.iter_registrations()
    assert len(rows) == 1 and rows[0]['source'] == 'send'
    reg = rows[0]
    assert reg['operation']['deliveryId'] == token
    assert trace.read_text().count('RPC turn/start') == original + 1
    payload = notify_deliver.payload_for_registration(
        reg, {'status': 'completed', 'workerState': 'idle',
              'report': 'fake child reply'})
    assert payload['selectedOriginalDeliveryId'] == token
    assert payload['deliveryId'] == reg['eventId'] + ':settled'
    assert payload['deliveryId'] != token
    assert notify_deliver.payload_for_registration(
        reg, {'status': 'completed'})['deliveryId'] == payload['deliveryId']
    assert 'selectionMode' not in payload


def test_n1c_public_cli_prepares_and_fsyncs_before_native_and_wakes_parent(
    machine_originals, monkeypatch, capsys,
):
    from ccx.cli import codex as codex_commands
    from ccx.store.store import GenerationStore
    import time

    client,store,trace,_ = machine_originals
    _n1c_managed_parent(monkeypatch,store)
    token='n1c-cli-known-token'
    began=[]
    prior=GenerationStore.begin_native_intent
    before=trace.read_text().count('RPC turn/start')
    def original_intent(self, target, *, family, operation_id, **kwargs):
        if family=='codex-send' and operation_id==token:
            regs=parent_notify.iter_registrations()
            assert len(regs)==1
            armed=regs[0]
            assert armed['state']=='armed' and armed['worker']['workerId']==B
            assert armed['parent']['workerId']==A
            assert armed['operation']['deliveryId']==token
            assert json.loads(parent_notify.registration_path(armed['eventId']).read_text())==armed
            native_receipt=codex_delivery.read_receipt('codex:'+B,token)
            assert native_receipt['state']=='prepared'
            assert trace.read_text().count('RPC turn/start')==before
            began.append(armed['eventId'])
        return prior(self,target,family=family,operation_id=operation_id,**kwargs)
    monkeypatch.setattr(GenerationStore,'begin_native_intent',original_intent)
    # Leave singleton start owned by the accepted implementation; test only
    # substitutes daemon bootstrap so the final poll is deterministic.
    started=[]
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',
                        lambda **_kwargs: started.append('running'))
    args=_n1c_cli_send_args(token)
    codex_commands.cmd_send(args)
    public=json.loads(capsys.readouterr().out)
    assert len(began)==1 and len(started)==1
    assert public['worker']=='codex:'+B and public['delivery_id']==token
    assert public['status']=='accepted' and public['notify_parent']['event_id']==began[0]
    assert public['notify_parent']['parent']=='codex:'+A
    assert public['notify_parent']['state']=='armed'
    assert 'report' not in public  # child admission != parent completion
    assert trace.read_text().count('RPC turn/start')==before+1
    # Real fake provider completes in its own daemon; the ordinary original
    # receipt observer freezes only that token's actual answer.
    deadline=time.monotonic()+6
    while time.monotonic()<deadline:
        outcome=ops_wait.reconcile_turn('codex:'+B,delivery_id=token)
        if outcome['status']=='settled':
            break
        time.sleep(.04)
    assert outcome['status']=='settled',outcome
    notifyd.process_registration(parent_notify.read_registration(began[0]))
    final=parent_notify.read_registration(began[0])
    assert final['state']=='delivered',final
    assert final['payload']['report']=='Fake Codex completed: N1C child original prompt'
    assert final['delivery']['workerId']==A
    assert final['delivery']['threadId']==store.record(A)['native']['threadId']
    assert final['payload']['deliveryId']==began[0]+':settled'
    assert trace.read_text().count('RPC turn/start')==before+2
    # Replay of the accepted semantic token observes the progressed event,
    # never submits a second native child turn or second parent wake.
    codex_commands.cmd_send(args)
    retried=json.loads(capsys.readouterr().out)
    assert retried['notify_parent']['event_id']==began[0]
    assert retried['notify_parent']['state']=='delivered'
    assert trace.read_text().count('RPC turn/start')==before+2


def test_n1c_v1_reuses_same_event_and_conflicts_before_any_new_native_send(
    machine_originals,monkeypatch,
):
    from ccx.rpc.local_rpc import dispatch
    client,store,trace,_=machine_originals
    _n1c_managed_parent(monkeypatch,store)
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',lambda **_kwargs: None)
    token='n1c-v1-known'
    request={'protocol_version':2,'request_id':'n1c-v1-1','op':'turn.send',
             'params':{'target':'codex:'+B,'text':'v1 child once',
                       'delivery_id':token,'notify_parent':True,
                       'caller_metadata':{'call_id':'only-one','stage':1}}}
    before=trace.read_text().count('RPC turn/start')
    first=dispatch(request)
    assert first['ok'] is True,first
    assert first['result']['notification']['parent']=='codex:'+A
    event=first['result']['notification']['event_id']
    assert first['result']['delivery']['caller_metadata']=={'call_id':'only-one','stage':1}
    assert trace.read_text().count('RPC turn/start')==before+1
    second=dispatch(request)
    assert second['ok'] is True,second
    assert second['result']['notification']['event_id']==event
    assert trace.read_text().count('RPC turn/start')==before+1
    tampered={**request,'request_id':'n1c-v1-input-conflict',
              'params':{**request['params'],'caller_metadata':{'call_id':'DIFFERENT'}}}
    changed=dispatch(tampered)
    assert changed['ok'] is False and changed['error']['code']=='refused',changed
    different_prompt={**request,'request_id':'n1c-v1-prompt-conflict',
                      'params':{**request['params'],'text':'another native prompt'}}
    changed2=dispatch(different_prompt)
    assert changed2['ok'] is False and changed2['error']['code']=='refused',changed2
    assert trace.read_text().count('RPC turn/start')==before+1
    # Second positive ORIGINAL parent has another native thread; the same
    # semantic id belongs to A and cannot be reclaimed by C.
    assert client.create(cwd=str(store.record(A)['cwd']),worker_id=C).worker_id==C
    prior=trace.read_text().count('RPC turn/start')
    _n1c_managed_parent(monkeypatch,store,worker_id=C)
    foreign=dispatch(request)
    assert foreign['ok'] is False and foreign['error']['code']=='refused',foreign
    assert trace.read_text().count('RPC turn/start')==prior
    assert parent_notify.read_registration(event)['parent']['workerId']==A


def test_n1c_busy_fresh_send_is_pre_effect_refusal(
    machine_originals,monkeypatch,capsys,
):
    from ccx.cli import codex as codex_commands
    client,store,trace,_=machine_originals
    _n1c_managed_parent(monkeypatch,store)
    assert client.send('codex:'+B,'busy-hold-n1c-child',
                       delivery_id='n1c-busy-own',wait=False,timeout=15).turn_id
    busy_count=trace.read_text().count('RPC turn/start')
    with pytest.raises(SystemExit) as refused:
        codex_commands.cmd_send(_n1c_cli_send_args('n1c-new-busy'))
    assert refused.value.code==5
    capsys.readouterr()
    assert trace.read_text().count('RPC turn/start')==busy_count
    assert codex_delivery.read_receipt('codex:'+B,'n1c-new-busy') is None
    assert ParentNotificationClient().list()['count']==0


def test_n1c_native_indeterminate_is_exit6_for_ordinary_and_notify_sends(
    machine_originals,monkeypatch,capsys,
):
    from ccx.cli import codex as codex_commands
    from ccx.store.store import GenerationStore
    from ccx.store.root import NativeEffectIndeterminate
    from ccx.rpc.local_rpc import dispatch
    client,store,trace,_=machine_originals
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',lambda **_kwargs: None)
    def lost_ack(self, *_args, **_kwargs):
        raise NativeEffectIndeterminate('fake submitted-unknown native turn reply')
    monkeypatch.setattr(GenerationStore,'dispatch_native_intent_once',lost_ack)
    _n1c_managed_parent(monkeypatch,store)
    before=trace.read_text().count('RPC turn/start')
    for notified in (False,True):
        token='n1c-indeterminate-'+str(notified).lower()
        with pytest.raises(SystemExit) as uncertain:
            codex_commands.cmd_send(_n1c_cli_send_args(token,notify=notified))
        assert uncertain.value.code==6
        payload=json.loads(capsys.readouterr().out)
        assert payload['reconcile_required'] is True
        assert payload['delivery_id']==token
        assert payload['delivery_status']=='submitted'
        if notified:
            rows=parent_notify.iter_registrations()
            assert len(rows)==1 and rows[0]['operation']['deliveryId']==token
            assert rows[0]['state']=='armed'
        assert trace.read_text().count('RPC turn/start')==before
    # V1 maps the same typed original-token uncertainty to reconcile_required.
    v1={'protocol_version':2,'request_id':'n1c-native-unknown','op':'turn.send',
        'params':{'target':'codex:'+B,'text':'last v1 unknown',
                  'delivery_id':'n1c-v1-unknown','notify_parent':True}}
    uncertain_v1=dispatch(v1)
    assert uncertain_v1['ok'] is False
    assert uncertain_v1['error']['code']=='incomplete'
    assert uncertain_v1['error']['details']['delivery_id']=='n1c-v1-unknown'
    assert trace.read_text().count('RPC turn/start')==before


def test_n1c_cancelled_wake_keeps_child_send_and_late_arm_is_refused(
    machine_originals,monkeypatch,
):
    client,store,trace,_=machine_originals
    _n1c_managed_parent(monkeypatch,store)
    monkeypatch.setattr(notify_daemon,'ensure_notify_daemon',lambda **_kwargs: None)
    text='N1C cancelled wake still sends child'
    token='n1c-cancelled-before-send'
    codex_delivery.prepare('codex:'+B,token,text,mode='new_turn_only')
    parent={'kind':'ccx-codex','workerId':A,
            'threadId':store.record(A)['native']['threadId'],
            'generation':store.generation}
    reg=parent_notify.create_registration(
        parent=parent,worker_engine='codex',worker_id=B,
        worker_name=None,operation={'kind':'codex-turn','deliveryId':token},
        source='send')
    parent_notify.cancel_registration(reg['eventId'],'operator cancelled wake only')
    before=trace.read_text().count('RPC turn/start')
    sent=ops_send.send_turn('codex:'+B,text,delivery_id=token,notify_parent=True)
    assert sent['status']=='accepted' and sent['notification']['state']=='cancelled'
    assert sent['notification']['delivery_available'] is False
    assert trace.read_text().count('RPC turn/start')==before+1
    notifyd.process_registration(parent_notify.read_registration(reg['eventId']))
    assert parent_notify.read_registration(reg['eventId'])['state']=='cancelled'
    assert trace.read_text().count('RPC turn/start')==before+1
    # A once-accepted non-notify original delivery cannot be retroactively
    # adopted under a notification ID after its native effect has occurred.
    plain=client.send('codex:'+B,'N1C prior ordinary send',
                      delivery_id='n1c-plain-already-accepted',wait=False,timeout=15)
    assert plain.turn_id
    settled=trace.read_text().count('RPC turn/start')
    with pytest.raises(InvalidRequest,match='retroactively arm'):
        ops_send.send_turn('codex:'+B,'N1C prior ordinary send',
             delivery_id='n1c-plain-already-accepted',notify_parent=True)
    assert trace.read_text().count('RPC turn/start')==settled


def _assert_no_new_native_effects_after_stop(before: str, after: str) -> None:
    """Exact prior trace plus harmless async Codex unsubscribe ONLY.

    A stopped original's physical unsub callback can outlive the kill RPC.
    It is not a new model turn or parent notification; any other appended
    provider operation is still a safety violation.
    """
    # The physical fake app-server appends each original RPC record without
    # an interprocess log-line lock. In this fake, its output terminator is
    # literally backslash+n; synthetic tests may use a real newline. A
    # snapshot can cut a preexisting record in HALF. Compare exact bytes,
    # not two independently split final-line snapshots.
    assert after.startswith(before)

    def first_delimiter(tail):
        markers = [(tail.find(mark), mark) for mark in ("\\n", "\n")
                   if tail.find(mark) >= 0]
        return min(markers, key=lambda item: item[0]) if markers else None

    newly_appended = after[len(before):]
    if not before.endswith(("\\n", "\n")):
        # Until the first record terminator, bytes only COMPLETE an event
        # already started before the stopped-child snapshot. They cannot be a
        # NEW post-kill native effect, whatever that historical event was.
        first = first_delimiter(newly_appended)
        if first is None:
            return
        offset, marker = first
        newly_appended = newly_appended[offset+len(marker):]
    # Every newly STARTED physical provider action must be a harmless exact
    # unsubscribe. Reject any new thread/start, resume, turn/start or write.
    while newly_appended:
        end = first_delimiter(newly_appended)
        if end is None:
            assert "RPC thread/unsubscribe ".startswith(newly_appended) or (
                newly_appended.startswith("RPC thread/unsubscribe "))
            return
        offset, marker = end
        line = newly_appended[:offset]
        assert line.startswith("RPC thread/unsubscribe "), line
        newly_appended = newly_appended[offset+len(marker):]


def test_stopped_child_trace_tolerates_late_original_unsubscribe_not_effects():
    """A janitor unsubscribe may finish after stop; it cannot replay turns."""
    before = "RPC thread/read {'threadId': 'native-original'}\n"
    after = before + "RPC thread/unsubscribe {'threadId': 'native-original'}\n"
    _assert_no_new_native_effects_after_stop(before, after)
    with pytest.raises(AssertionError):
        _assert_no_new_native_effects_after_stop(
            before, after + "RPC turn/start {'threadId': 'native-original'}\n")
    # A physical log read may end mid pre-existing janitor line. Finishing
    # that exact line later is NOT a new effect and must pass byte-prefix proof.
    interrupted = before + "RPC thread/unsub"
    completed = interrupted + "scribe {'threadId': 'native-original'}\n"
    _assert_no_new_native_effects_after_stop(interrupted, completed)
    with pytest.raises(AssertionError):
        _assert_no_new_native_effects_after_stop(
            interrupted, completed + "RPC turn/start {'threadId': 'foreign'}\n")
    # The physical fake records are terminated by literal backslash+n bytes,
    # not necessarily LF. Preserve strict no-native-wake under either shape.
    raw_before = "RPC thread/read {'threadId': 'native-original'}\\nRPC thread/unsub"
    raw_after = raw_before + "scribe {'threadId':'native-original'}\\n"
    _assert_no_new_native_effects_after_stop(raw_before, raw_after)
    with pytest.raises(AssertionError):
        _assert_no_new_native_effects_after_stop(
            raw_before, raw_after + "RPC turn/start {'threadId':'foreign'}\\n")
