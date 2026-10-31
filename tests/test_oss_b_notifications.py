"""Regression coverage with inert workers and mocked provider boundaries."""
import ccx.notify.deliver as notify_deliver
import ccx.notify.settle as notify_settle
import ccx.store.root as store_root
from types import SimpleNamespace

import pytest

from ccx.notify import client as notifications
from ccx.notify import registry as parent_notify
from test_generation_store import A, B


@pytest.mark.parametrize('kind', ['codex', 'claude', 'controller'])
@pytest.mark.parametrize('bare', [False, True])
def test_notification_filters_real_child_kind(monkeypatch, kind, bare):
    row = {'parent': f'codex:{A}', 'worker': f'{kind}:{B}', 'state': 'armed'}
    monkeypatch.setattr(parent_notify, 'iter_registrations', lambda **kw: [row])
    monkeypatch.setattr(parent_notify, 'public_registration', lambda value: value)
    found = notifications.ParentNotificationClient().list(
        parent=A, worker=B if bare else f'{kind}:{B}')
    assert found['count'] == 1


def test_standalone_payload_keeps_report_truncation(monkeypatch):
    monkeypatch.setattr(notify_deliver, '_original_notification_registration',
                        lambda _: (None, f'codex:{A}', f'codex:{B}'))
    reg = {'eventId': A, 'source': 'notify', 'worker': {'engine': 'codex'},
           'operation': {'selectionMode': 'current'}}
    text, truncated = notify_settle._truncate_report('x' * 12001)
    payload = notify_deliver.payload_for_registration(reg, {
        'status': 'completed', 'report': text, 'reportTruncated': truncated})
    assert len(payload['report']) == 12000
    assert payload['reportTruncated'] is True



@pytest.mark.parametrize('engine', ['codex', 'claude'])
def test_receiptless_settlement_carries_truncation_flag(monkeypatch, engine):
    from ccx.store import store as generation
    from ccx.claude import managed_turns
    from ccx.ops import turn_reports
    store = SimpleNamespace(record=lambda _: {'lifecycle': {'state': 'idle'}})
    monkeypatch.setattr(store_root, 'require_ccx_active', lambda _: store)
    monkeypatch.setattr(notify_settle, 'NOTIFY_REPORT_MAX_CHARS', 12000)
    monkeypatch.setattr(notify_settle, '_codex_native_turn_evidence',
                        lambda *a: {'status': 'completed', 'error': None})
    monkeypatch.setattr(notify_settle, '_receiptless_codex_report', lambda *a: 'x' * 12001)
    ref = f'ccx-turn:{engine}:v1:turn'
    monkeypatch.setattr(managed_turns, 'report_window_for_turn_ref',
                        lambda *a: ({'startOffset': 0, 'endOffsetKnown': True}, True))
    monkeypatch.setattr(managed_turns, 'settlement_for_turn_ref',
                        lambda *a: ({'turnRef': ref, 'eventType': 'turn_complete'}, True))
    monkeypatch.setattr(turn_reports, 'claude_source_report', lambda *a: {'text': 'x' * 12001})
    reg = {'worker': {'engine': engine, 'workerId': B},
           'operation': {'selectedTurnRef': ref, 'turnId': 'turn', 'nativeSourceId': 'thread'}}
    settlement = notify_settle._receiptless_settlement(reg)
    assert len(settlement['report']) == 12000
    assert settlement['reportTruncated'] is True
