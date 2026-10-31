"""Regressions from the live tests of the pre-open-source main."""
import ccx.notify.select as notify_select
import ccx.ops.report as ops_report
import ccx.ops.wait as ops_wait
import pytest

from ccx.ops import common as protocol_ops
from ccx.errors import SessionUnavailable, TurnIncomplete
from test_oss_c_regressions import fake_store
from test_generation_store import A
from test_notify_claude import owners  # noqa: F401  (fixture)

REF = 'ccx-turn:claude:v1:' + A


def test_claude_send_wait_stops_on_interrupted_turn(monkeypatch):
    fake_store(monkeypatch, {'workerId': A, 'lifecycle': {'state': 'idle'},
                             'lastSettledTurn': {'turnRef': REF, 'outcome': 'interrupted'}})
    owned = {'status': 'owned', 'delivery': {'delivery_id': 'd1', 'turn_ref': REF}}
    monkeypatch.setattr(ops_wait, 'reconcile_turn', lambda *a, **kw: owned)
    monkeypatch.setattr(ops_wait.time, 'sleep',
                        lambda _: pytest.fail('wait ignored the interrupted turn'))
    with pytest.raises(TurnIncomplete) as raised:
        ops_wait.wait_delivery('claude:' + A, delivery_id='d1', timeout=60)
    assert raised.value.turn_status == 'interrupted'
    assert raised.value.delivery_id == 'd1'


def test_facade_wait_uses_normalized_state(monkeypatch):
    from types import SimpleNamespace
    from ccx import workers
    client = object.__new__(workers.WorkerClient)
    client._uuid_store = SimpleNamespace(generation='g')
    monkeypatch.setattr(client, '_uuid_owner', lambda w: SimpleNamespace(worker='claude:' + A))
    monkeypatch.setattr(workers, '_background_task_block_for_worker', lambda *a: {})
    called = []
    def fake_wait(*a, **kw):
        called.append((a, kw))
        return {'state': 'dead', 'native_state': 'dead', 'status': 'dead'}
    monkeypatch.setattr(ops_wait, 'wait_worker', fake_wait)
    result = client.wait('claude:' + A, timeout=2)
    assert called
    assert result['state'] == 'stopped'
    assert result['native_state'] == 'dead'


def test_notify_selection_waits_for_first_prompt_to_open(owners, monkeypatch):
    from ccx.ops import compact_ops
    from ccx.notify import registry as parent_notify
    from ccx.rpc import rpc
    from test_notify_standalone import _native_caller
    root, _store = owners
    monkeypatch.setenv('CCX_ROOT', str(root))
    _native_caller(monkeypatch)
    snapshots = iter([
        {'state': 'idle', 'active_turn_ref': None, 'status': 'timeout',
         'reason': 'wait deadline elapsed'},
        {'state': 'idle', 'active_turn_ref': None, 'status': 'idle',
         'reason': 'worker is idle', 'last_settled_turn': None},
    ])
    monkeypatch.setattr(ops_wait, 'wait_worker', lambda *a, **kw: next(snapshots))
    monkeypatch.setattr(compact_ops, 'active_operation', lambda *a: None)
    monkeypatch.setattr(rpc, '_rpc', lambda *a, **kw: {
        'ok': True, 'activeTurnRef': None, 'compacting': False})
    selection = notify_select.select_current_turn('claude:' + A)
    assert selection['operation']['selectionMode'] == 'status-only'


def test_notify_selection_waits_for_a_queued_send_to_open(owners, monkeypatch):
    from ccx.claude import delivery as claude_delivery
    from ccx.ops import compact_ops
    from ccx.notify import registry as parent_notify
    from ccx.rpc import rpc
    from test_notify_standalone import _native_caller
    root, _store = owners
    monkeypatch.setenv('CCX_ROOT', str(root))
    _native_caller(monkeypatch)
    idle = {'state': 'idle', 'active_turn_ref': None, 'status': 'idle',
            'reason': 'worker is idle', 'last_settled_turn': None}
    snapshots = iter([idle, idle, {**idle, 'state': 'busy', 'status': 'timeout',
                                   'active_turn_ref': 'ccx-turn:claude:v1:new'}])
    receipts = iter([[{'state': 'owned'}], [{'state': 'owned'}], []])
    monkeypatch.setattr(ops_wait, 'wait_worker', lambda *a, **kw: next(snapshots))
    monkeypatch.setattr(claude_delivery, 'iter_receipts', lambda child: next(receipts))
    monkeypatch.setattr(compact_ops, 'active_operation', lambda *a: None)
    monkeypatch.setattr(rpc, '_rpc', lambda *a, **kw: {
        'ok': True, 'activeTurnRef': 'ccx-turn:claude:v1:new', 'compacting': False})
    # The fake new turn has no receipt or native proof, so selection refuses
    # it; what matters is that it waited for that turn, not the previous one.
    with pytest.raises(SessionUnavailable, match='opening receipt or native proof'):
        notify_select.select_current_turn('claude:' + A)
    assert next(snapshots, None) is None


def test_notify_refuses_while_a_send_is_still_being_delivered(owners, monkeypatch):
    from types import SimpleNamespace
    from ccx.claude import delivery as claude_delivery
    from ccx.notify import registry as parent_notify
    from test_notify_receiptless import _claude_wait
    ref = 'ccx-turn:claude:v1:old'
    _claude_wait(monkeypatch, None, last=ref)
    old = {'state': 'accepted', 'route': 'new_turn', 'placement': 'new_turn',
           'turnRef': ref, 'deliveryId': 'old'}
    monkeypatch.setattr(claude_delivery, 'iter_receipts',
                        lambda _: iter([old, {'state': 'owned', 'deliveryId': 'new'}]))
    monkeypatch.setattr(claude_delivery, 'read_receipt', lambda *_: old)
    ticks = iter(range(0, 10000, 20))
    monkeypatch.setattr(parent_notify, 'time', SimpleNamespace(
        monotonic=lambda: next(ticks), sleep=lambda _: None))
    # After the bounded wait the new prompt is still opening: never fall
    # back to the previous turn.
    with pytest.raises(SessionUnavailable, match='still being delivered'):
        notify_select.select_current_turn('claude:' + A)


def test_report_marks_an_open_turn_of_a_crashed_worker_failed(owners, tmp_path, monkeypatch):
    from test_notify_receiptless import _claude_line, _claude_sources
    _root, store = owners
    path = _claude_sources(store, monkeypatch, tmp_path)
    _claude_line(path, {'type': 'user', 'message': {'content': 'new request'}})
    _claude_line(path, {'type': 'assistant', 'message': {
        'stop_reason': 'tool_use', 'content': [
            {'type': 'text', 'text': 'Let me inspect that.'},
            {'type': 'tool_use', 'id': 't1', 'name': 'Bash', 'input': {'command': 'sleep 600'}}]}})
    store.mutate_record('claude:' + A, patches={
        'lifecycle.state': 'crashed', 'lifecycle.activeTurnRef': 'ccx-turn:claude:v1:unfinished'})
    result = ops_report.report_worker('claude:' + A)
    assert result['text'] == 'Let me inspect that.'
    assert result['status'] == 'failed' and result['has_final_message'] is False
    assert result['note'] == 'the worker failed'
