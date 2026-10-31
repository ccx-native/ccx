"""Regression coverage with inert workers and mocked provider boundaries."""
import ccx.codex.daemon.native as daemon_native
import ccx.store.root as store_root
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from test_generation_store import A, short_root


def test_retire_unresolved_start_keeps_durable_field(short_root, tmp_path, monkeypatch):
    from ccx import constants
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.root import require_ccx_active
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    monkeypatch.setattr(constants, 'CCX_ROOT', short_root)
    store = require_ccx_active(short_root)
    prepared = prepare_worker(store, engine='codex', cwd=str(tmp_path), worker_id=A)
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, prepared.record)
    worker.record['nativeTurnStart'] = {'state': 'unknown'}
    worker.record['lifecycle']['state'] = 'busy'
    worker._save()
    daemon.workers[A] = worker
    daemon._retire_workers()
    record = store.record(A)
    assert record['nativeTurnStart'] is None
    assert record['lifecycle']['state'] == 'crashed'


@pytest.mark.parametrize('answered', [True, False])
def test_turn_start_refusal_restores_state_only_when_answered(tmp_path, monkeypatch, answered):
    import hashlib
    import os
    import threading
    from contextlib import nullcontext
    from ccx.codex import delivery as codex_delivery
    from ccx.codex import provenance as codex_provenance
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    from ccx.codex.protocol import ProtocolError, ProtocolTransportError
    from ccx.store.store import _digest
    from ccx.store.identity import WorkerOwner
    prompt = 'original prompt'
    receipt = {'state': 'submitted', 'mode': 'new_turn_only', 'route': 'new_turn',
               'clientUserMessageId': 'client-id', 'promptSha256': hashlib.sha256(prompt.encode()).hexdigest()}
    monkeypatch.setattr(codex_delivery, 'read_receipt', lambda *a: receipt)
    monkeypatch.setattr(codex_delivery, 'requested_mode', lambda _: 'new_turn_only')
    monkeypatch.setattr(codex_delivery, 'intent_spec', lambda _: {})
    monkeypatch.setattr(codex_provenance, 'parse_client_message_id',
                        lambda _: {'workerId': A, 'kind': 'start'})
    from ccx.codex.daemon import daemon as codexd
    monkeypatch.setattr(daemon_native, 'send_receipt_route', lambda _: 'new_turn')
    record = {'native': {'threadId': 'thread', 'threadSource': 'source'},
              'lifecycle': {'state': 'idle', 'since': 123, 'activeTurnId': None}}
    daemon = CodexDaemon()
    directory = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    store = SimpleNamespace(
        generation='generation', revalidate=lambda: None,
        resolve=lambda _: WorkerOwner(A, 'codex'), record=lambda _: record,
        codex_source=lambda _: 'source', authorize_native_action=lambda *a: None,
        read_receipt=lambda *a: {'state': 'submitted-unknown', 'action': 'send',
            'workerId': A, 'generation': 'generation', 'intentSha256': _digest({})},
        lease=lambda: nullcontext(), _gen=lambda *a, **k: nullcontext(directory))
    worker = SimpleNamespace(
        record=record, loaded=True, killed=threading.Event(), kill_intent=threading.Event(),
        teardown_claimed=False, launch=None, lock=threading.RLock(),
        operation_lock=threading.RLock(), _save=Mock(),
        _native_turn_start_marker=lambda **k: {'state': 'pending'})
    worker._mark_native_turn_start_unknown_locked = lambda err: Worker._mark_native_turn_start_unknown_locked(worker, err)
    daemon._uuid_store = store
    daemon._adopt_original_native_worker = lambda _: worker
    daemon._still_registered = lambda _: True
    daemon.ready.set()
    error = (ProtocolError('invalid input', request_id=7, code=-32602) if answered
             else ProtocolTransportError('lost reply', request_id=7))
    daemon.client = SimpleNamespace(request=Mock(side_effect=error))
    message = {'op': 'native-send', 'workerId': A, 'threadId': 'thread',
               'deliveryId': 'original', 'clientUserMessageId': 'client-id', 'text': prompt}
    try:
        if answered:
            result = daemon._original_full_uuid_native(message)
            assert result['accepted'] is False
            assert result['rejectionCode'] == 'native-rejected'
            assert record['lifecycle'] == {'state': 'idle', 'since': 123, 'activeTurnId': None}
            assert record['nativeTurnStart'] is None
        else:
            with pytest.raises(ProtocolTransportError):
                daemon._original_full_uuid_native(message)
            assert record['lifecycle']['state'] == 'busy'
            assert record['nativeTurnStart']['state'] == 'unknown'
        assert worker.launch is None
        assert daemon.client.request.call_count == 1
    finally:
        os.close(directory)


@pytest.mark.parametrize("route", ["new_turn", "steer"])
def test_native_send_cold_weekly_cap_refuses_before_any_turn_start(monkeypatch, route):
    """An over-cap turn or steer never reaches the app-server's turn/start or turn/steer."""
    import hashlib
    import threading
    from ccx.codex import delivery as codex_delivery
    from ccx.codex.daemon import daemon as codexd
    from ccx.store.store import _digest
    from ccx.store.identity import WorkerOwner

    prompt = "short turn must be refused"
    caps = {"7d": 20}
    steer = route == "steer"
    receipt = {
        "state": "prepared" if steer else "submitted", "route": route, "cap": caps,
        "requestedMode": "steer_only" if steer else "new_turn_only",
        "clientUserMessageId": "client-id",
        "promptSha256": hashlib.sha256(prompt.encode()).hexdigest(),
    }
    monkeypatch.setattr(codex_delivery, "read_receipt", lambda *a: receipt)
    monkeypatch.setattr(codex_delivery, "intent_spec", lambda _: {})
    monkeypatch.setattr(daemon_native, "send_receipt_route", lambda _: route)
    lifecycle = ({"state": "busy", "activeTurnId": "turn-1"} if steer
                 else {"state": "idle", "activeTurnId": None})
    record = {
        "native": {"threadId": "thread", "threadSource": "source"},
        "lifecycle": dict(lifecycle),
    }
    daemon = codexd.CodexDaemon()
    daemon._uuid_store = SimpleNamespace(
        generation="gen", revalidate=lambda: None,
        resolve=lambda _: WorkerOwner(A, "codex"), record=lambda _: record,
        codex_source=lambda _: "source", authorize_native_action=lambda *a: None,
        read_receipt=lambda *a: {
            "state": "submitted-unknown", "action": "send",
            "workerId": A, "generation": "gen", "intentSha256": _digest({}),
        },
    )
    worker = SimpleNamespace(
        record=record, loaded=True, killed=threading.Event(),
        kill_intent=threading.Event(), teardown_claimed=False, launch=None,
        lock=threading.RLock(), operation_lock=threading.RLock(),
    )
    daemon._adopt_original_native_worker = lambda _: worker
    daemon._still_registered = lambda _: True
    daemon.ready.set()
    provider = Mock(return_value={"rateLimits": {
        "primary": {"usedPercent": 29, "windowDurationMins": 10080,
                    "resetsAt": 4102444800},
    }})
    daemon.client = SimpleNamespace(request=provider)
    result = daemon._original_full_uuid_native({
        "op": "native-send", "workerId": A, "threadId": "thread",
        "deliveryId": "original", "clientUserMessageId": "client-id",
        "text": prompt, "cap": caps,
    })
    assert result["accepted"] is False
    assert result["rejectionCode"] == "budget-stopped"
    assert f"no {'message' if steer else 'turn'} delivered" in result["rejectionMessage"]
    assert provider.call_args_list == [
        call("account/rateLimits/read", timeout=codexd.CAP_ADMISSION_READ_TIMEOUT_S),
    ]
    assert record["lifecycle"] == lifecycle


def test_shutdown_cleans_provider_and_sockets_after_retirement_failure(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from ccx.codex.daemon import daemon as codexd
    from ccx.store import store as generation
    from ccx.rpc import transport
    daemon = codexd.CodexDaemon()
    path = tmp_path / 'codexd.sock'
    provider_path = tmp_path / 'provider.sock'
    provider_path.touch()
    inode = provider_path.stat()
    daemon.app_server_path = provider_path
    daemon.app_server_inode = (inode.st_dev, inode.st_ino)
    daemon.client = Mock()
    store = SimpleNamespace(lease=lambda: nullcontext())
    monkeypatch.setattr(store_root, 'require_ccx_active', lambda _: store)
    monkeypatch.setattr(transport, 'plan_transport', lambda *a, **k: SimpleNamespace(final=path))
    monkeypatch.setattr(transport, 'ensure_transport_parent', lambda *a: None)
    monkeypatch.setattr(transport, 'daemon_process_lease', lambda *a: nullcontext())
    monkeypatch.setattr(codexd.threading, 'Thread', Mock())
    monkeypatch.setattr(daemon, '_start_client', lambda: None)
    monkeypatch.setattr(daemon, '_reconcile_orphan_records', lambda: None)
    monkeypatch.setattr(daemon, '_retire_workers', Mock(side_effect=RuntimeError('save failed')))
    daemon.stop.set()
    with pytest.raises(RuntimeError, match='save failed'):
        daemon.run()
    daemon.client.close.assert_called_once()
    assert not path.exists()
    assert not provider_path.exists()
