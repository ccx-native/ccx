"""Original full-UUID turn.cancel core, through real disposable host/codexd."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from test_generation_store import A, B, short_root
from test_create_cross_surface import (
    _deadline_fake_claude, _first_wait_original_claude_ready,
)
from test_claude_native_cli import _stop_disposable_original
import test_codex_native_effects as native_codex
from ccx import constants
from ccx.rpc.local_rpc import dispatch_full_uuid
from ccx.ops.cancel import cancel_turn, stop_worker
from ccx.ops.wait import reconcile_turn
from ccx.store.root import require_ccx_active
from ccx.errors import DeliveryRejected, OperationConflict


def _provider_native_identity(kernel_start):
    """Claude-shaped (procStart, pidDomain) for a fake native session file.

    Claude's real Darwin session record contains a one-second, C-locale UTC
    lstart string, not the kernel's microsecond token. Retain the exact kernel
    second and provider pidDomain; the production matcher must reject
    incorrect originals.
    """
    if kernel_start.startswith('darwin:'):
        from datetime import datetime, timezone
        started_sec = int(kernel_start.split(':')[1])
        provider_start = datetime.fromtimestamp(
            started_sec, timezone.utc).strftime('%a %b %d %H:%M:%S %Y')
        return provider_start, {'pidDomain': 'darwin'}
    return kernel_start, {
        'pidDomain': 'linux:9c1c63c55c164e25b6cab9ecf2d91080:pid:[4026532223]'
    }


def _native_escape_count(path):
    """Exclude Claude's normal bracketed-paste framing ESC[200~/ESC[201~."""
    raw = path.read_bytes()
    return (raw.count(b'\x1b') - raw.count(b'\x1b[200~')
            - raw.count(b'\x1b[201~'))


def _request(store, worker, token, ref, timeout_ms=350):
    return dispatch_full_uuid({
        'protocol_version': 2,
        'request_id': 'cancel-test-' + token,
        'op': 'turn.cancel',
        'params': {'target': worker, 'cancellation_id': token, 'turn_ref': ref},
        'timeout_ms': timeout_ms,
    }, store)


def _wait_ref(worker, token='create:initial', timeout=14):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = reconcile_turn(worker, delivery_id=token)
        if result.get('turn_ref'):
            return result['turn_ref']
        time.sleep(.04)
    from ccx.rpc.rpc import _rpc
    info = _rpc(worker, {"op": "status"}, timeout=1) if worker.startswith("claude:") else {}
    hooklog = Path(os.environ.get('FAKE_HOOK_RESULT_LOG', '/dev/null'))
    inputlog = Path(os.environ.get('FAKE_LOG', '/dev/null'))
    raise AssertionError(f'no original ref {result.get("status")} / host={info.get("state")} '
                         f'input={inputlog.read_bytes()[-300:]!r} '
                         f'hooks={hooklog.read_text()[-1200:] if hooklog.exists() else "none"}')


def test_cancel_claude_generated_hooks_same_id_no_reescape_and_fresh_stop(
    short_root, tmp_path, monkeypatch,
):
    """The generated hooks route runs; cancellation operates on the same host."""
    from ccx import CcxClient
    starts = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    input_log = tmp_path / 'cancel-pty.log'
    hook_log = tmp_path / 'generated-cancel-hooks.jsonl'
    monkeypatch.setenv('FAKE_HOOKS_VIA_SETTINGS', '1')
    monkeypatch.setenv('FAKE_HOOK_RESULT_LOG', str(hook_log))
    monkeypatch.setenv('FAKE_MODE', 'nostop')
    monkeypatch.setenv('FAKE_LOG', str(input_log))
    store = require_ccx_active(short_root)
    prompt = 'Claude cancellation stays on this exact first turn'
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        first = CcxClient().new(prompt, cwd=str(tmp_path), worker_id=A,
                                start_timeout=4, wait=False)
        assert first.worker_id == A
        target = f'claude:{A}'
        ref = _wait_ref(target)
        cancelled = _request(store, target, 'original-v1-cancel', ref, timeout_ms=350)
        assert cancelled['ok'], cancelled
        result = cancelled['result']['cancellation']
        assert result['cancellation_id'] == 'original-v1-cancel'
        assert result['turn_ref'] == ref
        assert result['status'] in {'submitted', 'unknown', 'confirmed'}
        # First native Escape reached exactly the original host PTY.
        end = time.monotonic() + 4
        while (time.monotonic() < end
               and (not input_log.exists() or _native_escape_count(input_log) < 1)):
            time.sleep(.04)
        assert _native_escape_count(input_log) == 1
        again = _request(store, target, 'original-v1-cancel', ref, timeout_ms=150)
        assert again['ok'], again
        assert _native_escape_count(input_log) == 1
        # Explicit NEW stop invocations must be allowed to ask the *same*
        # still-active turn to stop again. No token derivation by turn ref.
        repeated = stop_worker(target, timeout=.2)
        assert repeated['cancellation_id'] != 'original-v1-cancel'
        assert repeated['turn_ref'] == ref
        end = time.monotonic() + 4
        while time.monotonic() < end and _native_escape_count(input_log) < 2:
            time.sleep(.04)
        assert _native_escape_count(input_log) == 2
        assert starts.read_text().splitlines() == ['provider-start']
        assert (store.record(A)['native']['sessionId'] ==
                store.native_activation(A)['nativeBinding']['sessionId'])
        end = time.monotonic() + 2
        results = []
        while time.monotonic() < end:
            if hook_log.exists():
                results = [json.loads(line) for line in hook_log.read_text().splitlines()]
            if {'SessionStart', 'UserPromptSubmit'} <= {r['event'] for r in results}:
                break
            time.sleep(.04)
        assert {'SessionStart', 'UserPromptSubmit'} <= {r['event'] for r in results}
        assert all(r['rc'] == 0 for r in results), results
    finally:
        _stop_disposable_original(short_root)


def test_cancel_claude_stale_and_idle_refuse_without_escape(
    short_root, tmp_path, monkeypatch,
):
    from ccx import CcxClient
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    native_input = tmp_path / 'cancel-stale-pty.log'
    monkeypatch.setenv('FAKE_LOG', str(native_input))
    store = require_ccx_active(short_root)
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        prompted = CcxClient().new('turn that completes normally', cwd=str(tmp_path),
                                    worker_id=A, wait=True, timeout=8)
        assert prompted.settled
        ref = prompted.turn_ref
        baseline = native_input.read_bytes().count(b'\x1b')
        stale = _request(store, f'claude:{A}', 'after-idle-refused', ref)
        assert stale['ok'] and stale['result']['cancellation']['status'] == 'rejected'
        with pytest.raises(DeliveryRejected):
            stop_worker(f'claude:{A}', timeout=.2)
        from ccx.errors import InvalidRequest
        with pytest.raises(InvalidRequest):
            cancel_turn(f'claude:{A}', cancellation_id='no-implicit-target',
                        turn_ref=None, timeout=.2)
        # Preflight rejected before claiming an interrupt intent. An invented
        # alternate ref remains refused; no semantic receipt is fabricated.
        with pytest.raises(DeliveryRejected):
            cancel_turn(f'claude:{A}', cancellation_id='after-idle-refused',
                        turn_ref='ccx-turn:claude:v1:a-different-start', timeout=.2)
        assert native_input.read_bytes().count(b'\x1b') == baseline
    finally:
        _stop_disposable_original(short_root)


def _cancel_enabled_codex_binary(monkeypatch):
    """Extend disposable fake app-server only; never edit the shared test fake."""
    original = native_codex._fake_codex_executable

    def wrapper(*args, **kwargs):
        binary = original(*args, **kwargs)
        script = binary.read_text()
        previous = "status=('failed' if text.startswith('initial-wait-failed')"
        assert script.count(previous) == 1
        script = script.replace(previous, "status=('inProgress' if text.startswith('cancel-hold-')\n"
                                                "                    else 'failed' if text.startswith('initial-wait-failed')", 1)
        previous = "        elif method=='thread/unsubscribe':\n            result={}"
        assert script.count(previous) == 1
        replacement = """        elif method=='turn/interrupt':
            tid=params.get('threadId')
            turn_id=params.get('turnId')
            matches=[t for t in threads[tid]['turns'] if t.get('id')==turn_id]
            if len(matches)!=1: raise ValueError('no exact native turn')
            turn=matches[0]
            if turn.get('status')!='inProgress': raise ValueError('turn already settled')
            turn['status']='interrupted'
            turn['error']={'message':'native fake interrupted original turn'}
            result={}
            if PERSIST:
                with open(PERSIST,'w',encoding='utf8') as f: json.dump(threads,f)
        elif method=='thread/unsubscribe':
            result={}"""
        script = script.replace(previous, replacement, 1)
        previous = "            print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',\n                'params':{'threadId':tid,'turn':turn}}),flush=True)"
        assert script.count(previous) == 1
        script = script.replace(previous, "            if not text.startswith('cancel-hold-'):\n"
                                          "    " + previous, 1)
        marker = "        if method=='turn/start':\n            print(json.dumps({'jsonrpc':'2.0','method':'turn/started',"
        assert script.count(marker) == 1
        script = script.replace(marker, """        if method=='turn/interrupt':
            print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',
                'params':{'threadId':tid,'turn':turn}}),flush=True)
""" + marker, 1)
        binary.write_text(script)
        return binary

    monkeypatch.setattr(native_codex, '_fake_codex_executable', wrapper)


def test_cancel_codex_exact_interrupt_same_prefix_sibling_and_same_id_no_resend(
    short_root, tmp_path, monkeypatch,
):
    from ccx import CodexClient
    _cancel_enabled_codex_binary(monkeypatch)
    sibling = 'bbbbbbbb-1234-4000-8000-000000000099'
    with native_codex._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path/'fake-codex'))
        for wid in (B, sibling):
            proc = cli('new', '--engine', 'codex', '--cwd', str(tmp_path),
                       '--worker-id', wid, '--json')
            assert proc.returncode == 0, (proc.stdout, proc.stderr)
        for wid in (B, sibling):
            created = CodexClient().create('cancel-hold-' + wid, cwd=str(tmp_path),
                                           worker_id=wid)
            assert created.worker_id == wid
        ref = _wait_ref(f'codex:{B}')
        sibling_ref = _wait_ref(f'codex:{sibling}')
        first = _request(store, f'codex:{B}', 'codex-cancel-1', ref, timeout_ms=900)
        assert first['ok'], first
        result = first['result']['cancellation']
        assert result['status'] == 'confirmed' and result['outcome'] == 'interrupted', result
        repeats = _request(store, f'codex:{B}', 'codex-cancel-1', ref, timeout_ms=100)
        assert repeats['ok'] and repeats['result']['cancellation']['outcome'] == 'interrupted'
        with pytest.raises(OperationConflict):
            cancel_turn(f'codex:{B}', cancellation_id='codex-cancel-1',
                        turn_ref=sibling_ref, timeout=.1)
        text = trace.read_text()
        assert text.count('RPC turn/interrupt') == 1
        assert text.count('RPC thread/start') == 2
        assert text.count('RPC turn/start') == 2
        # This fake app-server emits a literal escaped newline into TRACE;
        # inspect the one native interrupt operation, not the whole log line.
        assert ("RPC turn/interrupt {'threadId': '" +
                store.record(B)['native']['threadId'] + "'") in text
        assert ("RPC turn/interrupt {'threadId': '" +
                store.record(sibling)['native']['threadId'] + "'") not in text
        sibling_status=daemon_rpc({"op":"status", "short":sibling,
                                   "rpcGeneration":constants.CODEXD_RPC_GENERATION},timeout=1)
        assert sibling_status['ok'] is True and sibling_status['activeTurnId'] == (
            sibling_ref.split(':')[-1]), sibling_status
        assert store.record(sibling)['lifecycle']['state'] == 'busy'
        assert store.record(B)['native']['threadId'] != store.record(sibling)['native']['threadId']


def test_cancel_preclaim_wrong_addresses_and_capabilities(short_root, monkeypatch):
    from ccx.errors import InvalidRequest
    monkeypatch.setattr(constants, 'CCX_ROOT', short_root)
    store = require_ccx_active(short_root)
    caps = dispatch_full_uuid({'protocol_version':2, 'request_id':'capabilities',
                               'op':'system.info','params':{}}, store)
    assert caps['result']["protocol_version"] == 2
    assert set(caps['result']) == {"protocol_version", "ccx_version"}
    for bad in ('claude:aaaaaaaa', 'codex:bbbbbbbb', 'display-name'):
        with pytest.raises(InvalidRequest):
            cancel_turn(bad, cancellation_id='invalid',
                        turn_ref='ccx-turn:claude:v1:some-id', timeout=.1)
    assert not (store.pointer.generation_root/'receipts'/'cancel').exists()


def test_cancel_codex_lost_native_reply_same_v1_id_never_reinterrupt(
    short_root, tmp_path, monkeypatch,
):
    """Physical native interrupt landed, caller lost ACK; same token observes."""
    from ccx import CodexClient
    from ccx.codex import runtime as codex_runtime
    _cancel_enabled_codex_binary(monkeypatch)
    with native_codex._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path/'fake-codex'))
        created=CodexClient().create('cancel-hold-lost-ack',
                                     cwd=str(tmp_path),worker_id=B)
        assert created.worker_id==B
        target=f'codex:{B}'
        ref=_wait_ref(target)
        native_rpc=codex_runtime.daemon_rpc
        calls=[]

        def lose_native_ack(message, *args, **kwargs):
            result=native_rpc(message, *args, **kwargs)
            if message.get('op')=='interrupt':
                assert result['ok'] is True, result
                calls.append(result)
                return {'ok':False,'error':'simulated lost native reply',
                        'transport':'inconclusive'}
            return result

        with monkeypatch.context() as patch:
            patch.setattr(codex_runtime,'daemon_rpc',lose_native_ack)
            first=_request(store,target,'codex-lost-ack-1',ref,timeout_ms=300)
        assert first['ok'],first
        assert first['result']['cancellation']['status']=='submitted'
        durable=store.read_receipt(target,'cancel','codex-lost-ack-1')
        assert durable['state']=='submitted-unknown'
        assert len(calls)==1
        again=_request(store,target,'codex-lost-ack-1',ref,timeout_ms=1200)
        assert again['ok'],again
        assert again['result']['cancellation']['status']=='confirmed'
        assert again['result']['cancellation']['outcome']=='interrupted'
        assert trace.read_text().count('RPC turn/interrupt')==1
        assert trace.read_text().count('RPC turn/start')==1
        assert store.record(B)['native']['threadId']


def test_cancel_host_prewritten_stale_turn_ref_is_definitive_without_escape(
    short_root, tmp_path, monkeypatch,
):
    """Exercise the real host's native expected-turn fence, not a simulated reply."""
    from ccx import CcxClient
    from ccx.rpc.rpc import _rpc

    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    pty = tmp_path / 'stale-host-escape.log'
    monkeypatch.setenv('FAKE_LOG', str(pty))
    monkeypatch.setenv('FAKE_MODE', 'nostop')
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        CcxClient().new('host raw stale turn fence', cwd=str(tmp_path),
                        worker_id=A, wait=False, start_timeout=4)
        target = f'claude:{A}'
        ref = _wait_ref(target)
        refused = _rpc(target, {'op': 'interrupt',
                                'expected_turn_ref': 'ccx-turn:claude:v1:other-turn'}, timeout=2)
        assert refused['ok'] is False and refused['definitive'] is True
        assert refused['error'] == 'active turn changed'
        assert refused['activeTurnRef'] == ref
        assert _rpc(target, {'op': 'status'}, timeout=2)['activeTurnRef'] == ref
        assert _native_escape_count(pty) == 0
    finally:
        _stop_disposable_original(short_root)


def test_cancel_claude_real_prior_escape_already_observing_is_accepted(
    short_root, tmp_path, monkeypatch,
):
    """A real host reply about an exact prior Escape is not a completion verdict."""
    from ccx import CcxClient
    from ccx.rpc import rpc
    from ccx.store.liveness import read_managed_claude_native_state

    provider_home = tmp_path / 'provider-home'
    provider_home.mkdir(mode=0o700)
    monkeypatch.setenv('CLAUDE_CONFIG_DIR', str(provider_home))
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    pty = tmp_path / 'already-observing-escape.log'
    monkeypatch.setenv('FAKE_LOG', str(pty))
    monkeypatch.setenv('FAKE_MODE', 'nostop')
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        CcxClient().new('same turn prior Escape proof', cwd=str(tmp_path),
                        worker_id=A, wait=False, start_timeout=4)
        target = f'claude:{A}'
        ref = _wait_ref(target)
        store = require_ccx_active(short_root)
        record = store.record(A)
        lifecycle = record['lifecycle']
        pid, session = lifecycle['workerPid'], record['native']['sessionId']
        session_dir = provider_home / 'sessions'
        session_dir.mkdir(mode=0o700)
        native_file = session_dir / f'{pid}.json'
        stamp = int(time.time() * 1000)
        provider_start, provider_domain = _provider_native_identity(lifecycle['workerStart'])
        busy = {'pid': pid, 'sessionId': session, 'kind': 'interactive',
                'procStart': provider_start, 'status': 'busy',
                'statusUpdatedAt': stamp + 3000, **provider_domain}
        native_file.write_text(json.dumps(busy))
        native = read_managed_claude_native_state(pid, session, claude_home=provider_home)
        assert native is not None and native.status == 'busy'

        original_rpc = rpc._rpc
        first = original_rpc(target, {'op': 'interrupt', 'expected_turn_ref': ref}, timeout=2)
        assert first['ok'] is True and first['sent'] is True and first['turnRef'] == ref
        native_file.write_text(json.dumps({
            **busy, 'status': 'idle', 'statusUpdatedAt': stamp + 4000,
        }))
        # The real host enters its bounded semantic-evidence observation grace.
        time.sleep(.22)
        replies = []

        def trace_native(short, message, timeout=5):
            reply = original_rpc(short, message, timeout=timeout)
            if message.get('op') == 'interrupt':
                replies.append(reply)
            return reply

        with monkeypatch.context() as patch:
            patch.setattr(rpc, '_rpc', trace_native)
            result = cancel_turn(target, cancellation_id='same-turn-observing',
                                 turn_ref=ref, timeout=.12)
        assert len(replies) == 1, replies
        assert replies[0]['ok'] is True and replies[0]['sent'] is False
        assert replies[0]['already_observing'] is True and replies[0]['turnRef'] == ref
        assert result['accepted'] is True and result['confirmed'] is False
        assert result['status'] == 'unknown' and result['outcome'] is None
        receipt = store.read_receipt(target, 'cancel', 'same-turn-observing')
        assert receipt['state'] == 'accepted' and receipt['nativeEvidenceSha256']
        assert _native_escape_count(pty) == 1
        again = cancel_turn(target, cancellation_id='same-turn-observing',
                            turn_ref=ref, timeout=.1)
        assert again['accepted'] is True
        assert _native_escape_count(pty) == 1
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize('reply, status, durable', [
    ({'ok': False, 'definitive': True, 'error': 'a new definite refusal'},
     'rejected', 'rejected'),
    ({'ok': False, 'error': 'active turn changed'},
     'submitted', 'submitted-unknown'),
    ({'ok': True, 'sent': False}, 'submitted', 'submitted-unknown'),
    ({'ok': True, 'sent': False, 'already_observing': True,
      'turnRef': 'ccx-turn:claude:v1:wrong'}, 'submitted', 'submitted-unknown'),
    ({'ok': False, 'error': 'unreachable: BrokenPipeError'},
     'submitted', 'submitted-unknown'),
], ids=['structured-definite', 'no-prose-fallback', 'bare-unsent',
        'wrong-observation-ref', 'transport-uncertain'])
def test_cancel_native_reply_mapping_requires_structured_proof(
    short_root, tmp_path, monkeypatch, reply, status, durable,
):
    """Consumer shim only: physical reachability is tested at the producers."""
    from ccx import CcxClient
    from ccx.rpc import rpc
    from ccx.errors import TurnIncomplete

    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    pty = tmp_path / 'mapping-pty.log'
    monkeypatch.setenv('FAKE_LOG', str(pty))
    monkeypatch.setenv('FAKE_MODE', 'nostop')
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        CcxClient().new('hold turn for native reply mapping', cwd=str(tmp_path),
                        worker_id=A, wait=False, start_timeout=4)
        target = f'claude:{A}'
        ref = _wait_ref(target)
        store = require_ccx_active(short_root)
        real_rpc = rpc._rpc
        submitted = []

        def native_reply_only(short, message, timeout=5):
            if message.get('op') == 'interrupt':
                submitted.append(message)
                return dict(reply)
            return real_rpc(short, message, timeout=timeout)

        with monkeypatch.context() as patch:
            patch.setattr(rpc, '_rpc', native_reply_only)
            first = _request(store, target, 'reply-mapping', ref, timeout_ms=120)
            assert first['ok'], first
            assert first['result']['cancellation']['status'] == status, first
            receipt = store.read_receipt(target, 'cancel', 'reply-mapping')
            assert receipt['state'] == durable
            assert bool(receipt['nativeEvidenceSha256']) == (durable == 'rejected')
            replay = _request(store, target, 'reply-mapping', ref, timeout_ms=120)
            assert replay['ok'] and len(submitted) == 1
            assert store.read_receipt(target, 'cancel', 'reply-mapping') == receipt
            if status == 'rejected':
                with pytest.raises(DeliveryRejected) as error:
                    stop_worker(target, timeout=.12)
                assert error.value.exit_code == 5
            elif reply.get('error') == 'active turn changed':
                with pytest.raises(TurnIncomplete) as error:
                    stop_worker(target, timeout=.12)
                assert error.value.exit_code == 6
        assert _native_escape_count(pty) == 0
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize('state, turn_id, expected_id, kill_pending, refusal', [
    ('idle', None, None, False, 'no active turn'),
    ('busy', None, None, False, 'active turn id is unavailable'),
    ('busy', 'turn-one', 'turn-two', False, 'active turn changed'),
    ('busy', 'turn-one', 'turn-one', True, 'is being killed'),
])
def test_cancel_codex_prewritten_daemon_refusals(
    state, turn_id, expected_id, kill_pending, refusal,
):
    """The actual Worker/daemon guards refuse before calling the native client."""
    import threading
    from types import SimpleNamespace
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker

    native_calls = []

    class Native:
        def request(self, method, params):
            native_calls.append((method, params))
            raise AssertionError('a definitive refusal must not call native')

    worker = object.__new__(Worker)
    worker.daemon = SimpleNamespace(client=Native())
    worker.short = B
    worker.record = {'lifecycle': {'state': state, 'activeTurnId': turn_id}}
    worker.lock = threading.RLock()
    worker.killed = threading.Event()
    worker.kill_intent = threading.Event()
    if kill_pending:
        worker.kill_intent.set()
    request = {'op': 'interrupt', 'expectedTurnId': expected_id}
    direct = worker.handle_operation(request)
    assert direct['ok'] is False and direct['definitive'] is True
    assert refusal in direct['error']
    assert native_calls == []

    if kill_pending:
        daemon = object.__new__(CodexDaemon)
        daemon.lock = threading.RLock()
        daemon.workers = {B: worker}
        daemon._uuid_store = SimpleNamespace(resolve=lambda _id: SimpleNamespace(
            engine='codex', worker_id=B))
        # Check the earlier shared gate, including its pre-effect approval refusal.
        for op in ('interrupt', 'approve'):
            admitted = daemon.handle({'op': op, 'short': B,
                                      'expectedTurnId': expected_id})
            assert admitted['ok'] is False and admitted['definitive'] is True
            assert 'is being killed' in admitted['error']
        assert native_calls == []


def test_cancel_codex_idle_daemon_native_producer_has_no_interrupt(
    short_root, tmp_path, monkeypatch,
):
    """An actual disposable codexd and fake app-server refuse the idle turn."""
    from ccx import CodexClient
    from ccx.codex.runtime import daemon_rpc

    with native_codex._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, _store, _rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        created = CodexClient().create('finish naturally for idle interrupt',
                                       cwd=str(tmp_path), worker_id=B)
        assert created.worker_id == B
        reply = daemon_rpc({'op': 'interrupt', 'short': B,
                            'rpcGeneration': constants.CODEXD_RPC_GENERATION}, timeout=1)
        assert reply['ok'] is False and reply['definitive'] is True
        assert reply['error'] == 'no active turn'
        assert 'RPC turn/interrupt' not in trace.read_text()


@pytest.mark.parametrize('receipts', ['none', 'damaged'])
def test_cancel_codex_turn_without_an_opening_receipt_uses_the_live_turn(
    short_root, tmp_path, monkeypatch, receipts,
):
    """A parent notification opens a Codex turn with no CCX delivery receipt.

    Stop must still reach it through the live exact active turn, and a damaged
    unrelated receipt must not block it; an invented ref stays refused.
    """
    from ccx import CodexClient
    from ccx.codex import delivery as codex_delivery
    _cancel_enabled_codex_binary(monkeypatch)
    with native_codex._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path/'fake-codex'))
        proc = cli('new', '--engine', 'codex', '--cwd', str(tmp_path),
                   '--worker-id', B, '--json')
        assert proc.returncode == 0, (proc.stdout, proc.stderr)
        CodexClient().create('cancel-hold-receiptless', cwd=str(tmp_path), worker_id=B)
        ref = _wait_ref(f'codex:{B}')

        def no_opening(_short, _turn_id):
            if receipts == 'damaged':
                raise codex_delivery.DeliveryReceiptError('unrelated damaged receipt')
            return None

        monkeypatch.setattr(codex_delivery, 'receipt_for_turn', no_opening)
        with pytest.raises(DeliveryRejected):
            cancel_turn(f'codex:{B}', cancellation_id='invented',
                        turn_ref='ccx-turn:codex:v1:not-the-live-turn', timeout=.5)
        assert trace.read_text().count('RPC turn/interrupt') == 0

        result = stop_worker(f'codex:{B}', timeout=3)
        assert result['turn_ref'] == ref
        assert result['accepted'] is True and result['status'] == 'confirmed', result
        assert trace.read_text().count('RPC turn/interrupt') == 1


def test_codex_interrupt_targets_the_turn_it_checked_not_a_successor():
    """The reader may publish successor U right after T is checked; U is never hit."""
    import threading
    from ccx.codex.daemon.worker import Worker

    class _Lifecycle(dict):
        def get(self, key, default=None):
            value = super().get(key, default)
            if key == 'activeTurnId' and value == 'turn-T':
                self['activeTurnId'] = 'turn-U'  # successor published next
            return value

    sent = []
    worker = Worker.__new__(Worker)
    worker.lock = threading.RLock()
    worker.killed, worker.kill_intent = threading.Event(), threading.Event()
    worker.short = 'w'
    worker.record = {'lifecycle': _Lifecycle(state='busy', activeTurnId='turn-T'),
                     'native': {'threadId': 'thread-1'}, 'threadId': 'thread-1'}
    from types import SimpleNamespace
    worker.daemon = SimpleNamespace(client=SimpleNamespace(
        request=lambda method, params: sent.append((method, params))))

    reply = worker.handle_operation({'op': 'interrupt', 'expectedTurnId': 'turn-T'})

    assert reply == {'ok': True, 'state': 'busy', 'turnId': 'turn-T'}
    assert sent == [('turn/interrupt', {'threadId': 'thread-1', 'turnId': 'turn-T'})]
