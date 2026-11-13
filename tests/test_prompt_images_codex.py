"""Exercise Codex's real native send path with an in-memory app-server."""
from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from ccx import constants
from ccx.codex import delivery as D
from ccx.codex.daemon.native import _original_full_uuid_native
from ccx.store.store import _digest
from test_generation_store import B, add_worker, short_root  # noqa: F401


@pytest.mark.parametrize('route', ['new_turn', 'steer'])
@pytest.mark.parametrize('text_kind', ['mixed', 'image-only', 'plain', 'missing'])
def test_native_input_items_and_confirmation_id(short_root, tmp_path, monkeypatch, route, text_kind):
    monkeypatch.setattr(constants, 'CCX_ROOT', short_root)
    store = add_worker(short_root, B, 'codex', confirmed=True)
    thread_id = store.record(B)['native']['threadId']
    paths = [tmp_path / 'first.PNG', tmp_path / 'second.jpeg']
    for path in paths:
        path.write_bytes(b'fixture image')
    image_items = [{'type': 'localImage', 'path': str(path)} for path in paths]
    if text_kind == 'mixed':
        text = '\n'.join([str(paths[0]), str(paths[1]), 'Review'])
        expected = image_items + [{'type': 'text', 'text': 'Review'}]
    elif text_kind == 'image-only':
        text = '\n'.join(str(path) for path in paths)
        expected = image_items
    else:
        text = 'plain text' if text_kind == 'plain' else str(tmp_path / 'missing.png')
        expected = [{'type': 'text', 'text': text}]
    mode = 'new_turn_only' if route == 'new_turn' else 'steer_only'
    token = 'fixture-' + route + '-' + text_kind
    receipt, _ = D.prepare(B, token, text, mode=mode)
    intent = store.begin_native_intent(
        B, family='codex-send', action='send', operation_id=token,
        intent_sha256=_digest(D.intent_spec(receipt)))
    if route == 'new_turn':
        receipt = D.mark_submitted(B, token)
    native_calls = []

    def request(method, params, **kwargs):
        native_calls.append((method, params))
        return {'turn': {'id': 'fixture-new-turn'}}

    worker = SimpleNamespace(
        killed=threading.Event(), kill_intent=threading.Event(),
        lock=threading.RLock(), operation_lock=threading.RLock(),
        loaded=True, teardown_claimed=False, launch=None,
        record={'lifecycle': {'state': 'idle' if route == 'new_turn' else 'busy',
                              'activeTurnId': None if route == 'new_turn' else 'fixture-running-turn'}},
        _save=lambda: None, _native_turn_start_marker=lambda **kw: kw,
    )
    ready = threading.Event()
    ready.set()
    daemon = SimpleNamespace(
        _uuid_store=store, _adopt_original_native_worker=lambda _: worker,
        client=SimpleNamespace(request=request), ready=ready,
        _still_registered=lambda _: True, instance_id='fixture-daemon')
    message = {
        'op': 'native-send', 'workerId': B, 'threadId': thread_id,
        'deliveryId': token, 'text': text,
        'clientUserMessageId': receipt.get('clientUserMessageId'),
    }
    answer = store.dispatch_native_intent_once(
        intent, bounded_submit=lambda _: _original_full_uuid_native(daemon, message))
    assert answer['accepted']
    assert len(native_calls) == 1
    method, params = native_calls[0]
    assert method == ('turn/start' if route == 'new_turn' else 'turn/steer')
    assert params['input'] == expected
    assert params['threadId'] == thread_id
    assert params['clientUserMessageId'] == D.read_receipt(B, token)['clientUserMessageId']
    assert answer['clientUserMessageId'] == params['clientUserMessageId']
    if route == 'steer':
        assert params['expectedTurnId'] == 'fixture-running-turn'
        assert answer['turnId'] == 'fixture-running-turn'
    else:
        assert answer['turnId'] == 'fixture-new-turn'


# These fixtures write sanitized receipts/journals only, with no daemon running.
from test_codex_delivery_recovery_reconcile import bound, journal, lost  # noqa: E402, F401


def test_image_only_reconciliation_uses_client_id(bound):
    from ccx.codex.delivery_recovery import recover
    store, thread, calls = bound
    receipt = lost(store, 'image-recovery', text='/fixture/image.png')
    item = {'type': 'userMessage', 'id': 'fixture-image-message',
            'clientId': receipt['clientUserMessageId'],
            'content': [{'type': 'localImage', 'path': '/fixture/image.png'}]}
    journal(('item/completed', {'threadId': thread, 'turnId': 'fixture-turn', 'item': item}))
    recovered = recover(store, B, receipt['deliveryId'])
    assert recovered['state'] == 'accepted'
    assert recovered['turnId'] == 'fixture-turn'
    assert not calls
