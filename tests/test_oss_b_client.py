"""Regression coverage with inert workers and mocked provider boundaries."""
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ccx.codex import client as codex_client
from ccx.codex import types as codex_types
from ccx.ops import common as protocol_ops
from ccx.errors import SessionWaiting, TurnIncomplete
from test_generation_store import A


@pytest.mark.parametrize('active', [{'activeTurnId': None}, {'active_turn_ref': None},
                                   {'active_turn_ref': 'ccx-turn:codex:v1:new'}])
def test_wait_does_not_resurrect_cleared_state(active):
    record = {'workerId': A, 'lifecycle': {'state': 'busy', 'activeTurnId': 'old'},
              'pendingApprovals': [{'requestId': 'old-gate'}]}
    result = codex_types.CodexWaitResult.from_result(record, {
        'ok': True, 'state': 'idle', 'pendingApprovals': None, **active})
    assert result.pending_approvals == []
    assert result.active_turn_id == ('new' if active.get('active_turn_ref') else None)


def test_rename_preserves_busy_configuration():
    record = {'workerId': A, 'name': 'renamed',
              'lifecycle': {'state': 'busy', 'activeTurnId': 'live'},
              'createArgs': {'approvalPolicy': 'never', 'idleTimeout': 123}}
    client = object.__new__(codex_client.CodexClient)
    client._uuid_store = SimpleNamespace(
        resolve=lambda _: SimpleNamespace(engine='codex', worker=f'codex:{A}', worker_id=A, short=A[:8]),
        rename_worker=lambda *a, **k: record)
    assert client.set_name(A, 'renamed') == codex_types.CodexSessionInfo.from_record(record)


@pytest.mark.parametrize('outcome', ['failed', 'interrupted', 'cancelled', 'waiting'])
def test_waited_send_stops_on_proven_outcome(monkeypatch, outcome):
    client = object.__new__(codex_client.CodexClient)
    client._uuid_store = SimpleNamespace(revalidate=lambda: None)
    client._owned_codex = lambda _: (f'codex:{A}', {})
    receipt = {'turn_id': 'turn', 'turn_ref': 'ccx-turn:codex:v1:turn'}
    error = {'message': 'exact terminal error'}
    preview = {'kind': 'permission', 'request_id': 'gate-1', 'tool': 'command',
               'message': 'allow command', 'native': {'pendingApprovals': [{'gateId': 'gate-1'}]}}
    send = Mock(return_value={'status': 'accepted', 'delivery': receipt})
    reconcile = Mock(side_effect=[{'status': 'accepted', 'delivery': receipt,
                                  'native_status': outcome, 'native_error': error},
                                 AssertionError('polled after proven outcome')])
    monkeypatch.setattr(ops_send, 'send_turn', send)
    monkeypatch.setattr(ops_wait, 'reconcile_turn', reconcile)
    monkeypatch.setattr(ops_wait, 'wait_worker', lambda *a, **k: {
        'status': 'waiting' if outcome == 'waiting' else 'idle',
        'lastTurn': {'turnId': 'turn', 'status': outcome, 'error': error},
        'pending_input': preview if outcome == 'waiting' else None})
    with pytest.raises(SessionWaiting if outcome == 'waiting' else TurnIncomplete) as caught:
        client.send(A, 'prompt', delivery_id='original', wait=True)
    assert send.call_count == reconcile.call_count == 1
    assert receipt['turn_id'] == 'turn'
    if outcome == 'waiting':
        assert caught.value.pending_input.request_id == 'gate-1'
        assert caught.value.pending_input.message == 'allow command'
        assert caught.value.waiting_for['native'] == preview['native']
    else:
        assert caught.value.turn_id == 'turn'
        assert caught.value.turn_status == outcome
        assert caught.value.turn_error == error


@pytest.mark.parametrize('outcome', ['failed', 'interrupted', 'cancelled'])
def test_waited_send_retains_original_failure_after_later_completion(monkeypatch, tmp_path, outcome):
    from ccx.codex import evidence as codex_evidence
    from ccx.codex import records as codex_records
    client = object.__new__(codex_client.CodexClient)
    client._uuid_store = SimpleNamespace(revalidate=lambda: None)
    record = {'native': {'threadId': 'thread'},
              'lastTurn': {'turnId': 'R2', 'status': 'completed'}}
    client._owned_codex = lambda _: (f'codex:{A}', record)
    monkeypatch.setattr(codex_records, 'read_record', lambda _: record)
    error = {'message': 'R1 terminal error'}
    events = [
        {'method': method, 'turnId': turn, 'threadId': 'thread',
         'sequence': index, 'params': {'turn': {'id': turn, 'status': status, 'error': detail}}}
        for index, (method, turn, status, detail) in enumerate([
            ('turn/started', 'R1', 'inProgress', None),
            ('turn/completed', 'R1', outcome, error),
            ('turn/started', 'R2', 'inProgress', None),
            ('turn/completed', 'R2', 'completed', None),
        ], 1)
    ]
    journal = codex_evidence.JournalRead(A, tmp_path / 'journal', exists=True,
        events=events, last_sequence=4, partial_tail=False, malformed_lines=[])
    monkeypatch.setattr(codex_evidence, 'read_journal', lambda _: journal)
    receipt = {'turn_id': 'R1', 'turn_ref': 'ccx-turn:codex:v1:R1'}
    observation = {'status': 'accepted', 'delivery': receipt, 'native_status': outcome}
    send = Mock(return_value=observation)
    reconcile = Mock(side_effect=[observation, AssertionError('polled after retained R1 failure')])
    monkeypatch.setattr(ops_send, 'send_turn', send)
    monkeypatch.setattr(ops_wait, 'reconcile_turn', reconcile)
    monkeypatch.setattr(ops_wait, 'wait_worker', lambda *a, **k: {
        'status': 'idle', 'lastTurn': record['lastTurn'],
        'last_settled_turn': {'turn_ref': 'ccx-turn:codex:v1:R2', 'outcome': 'completed'}})
    with pytest.raises(TurnIncomplete) as caught:
        client.send(A, 'prompt', delivery_id='original', wait=True)
    assert caught.value.turn_id == 'R1'
    assert caught.value.turn_status == outcome
    assert caught.value.turn_error == error
    assert send.call_count == reconcile.call_count == 1
    assert receipt == {'turn_id': 'R1', 'turn_ref': 'ccx-turn:codex:v1:R1'}
