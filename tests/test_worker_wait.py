"""Full-UUID general wait: genuine fake providers plus labeled snapshot-shim cases."""
from __future__ import annotations
import ccx.cli.commands.turns as commands_turns
import ccx.ops.wait as ops_wait

from argparse import Namespace
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from test_generation_store import A, B, add_worker, short_root
from test_create_cross_surface import (
    _deadline_fake_claude, _first_wait_original_claude_ready,
)
from test_claude_native_cli import _stop_disposable_original
from test_codex_native_effects import _f2_fake_processes
from ccx import CcxClient, CodexClient, WorkerClient, constants
from ccx.errors import CapabilityUnsupported, InvalidRequest, SessionUnavailable
from ccx.rpc.local_rpc import dispatch_full_uuid
from ccx.ops.wait import wait_worker
from ccx.store.root import require_ccx_active


@pytest.fixture
def owned_claude(short_root, tmp_path, monkeypatch):
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    pty = tmp_path / 'wait-no-native-input.log'
    monkeypatch.setenv('FAKE_LOG', str(pty))
    monkeypatch.setenv('FAKE_MODE', 'nostop')
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        yield f'claude:{A}', require_ccx_active(short_root), pty
    finally:
        _stop_disposable_original(short_root)


def v1(store, handle, *, params=None, timeout_ms=300):
    return dispatch_full_uuid({
        'protocol_version': 2, 'request_id': 'full-uuid-wait-test',
        'op': 'worker.wait', 'params': {'target': handle, **(params or {})},
        'timeout_ms': timeout_ms,
    }, store)


def cli_wait(*args):
    return subprocess.run(
        [sys.executable, '-B', '-m', 'ccx', *args],
        capture_output=True, text=True, timeout=12,
    )


def test_claude_real_host_plain_wait_python_cli_v1(owned_claude):
    """Real authenticated disposable host; wait never sends any native input."""
    handle, store, pty = owned_claude
    py = CcxClient().wait(A, timeout=.5)
    assert py.status == 'idle' and py.state == 'idle'
    neutral = WorkerClient().wait(handle, timeout=.5)
    assert (neutral['worker'], neutral['worker_id'], neutral['engine'],
            neutral['status'], neutral['state']) == (
                handle, A, 'claude', py.status, py.state)
    assert neutral['pending_input'] is None and neutral['api_error'] is None
    caps = WorkerClient().capabilities(handle)
    assert caps['wait_next_turn'] is caps['wait_known_input'] is True
    assert WorkerClient().capabilities('codex')['wait_after_turn'] is False
    proc = cli_wait('wait', A, '--timeout', '.5', '--json')
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert json.loads(proc.stdout)['worker'] == handle
    rpc = v1(store, handle)
    assert rpc['ok'] and rpc['result']['status'] == 'idle', rpc
    assert rpc['result']['worker'] == handle
    assert rpc['result']['worker_view']['state_source'] == 'recorded'
    assert not pty.exists() or not pty.read_bytes()
    for invalid in ('aaaaaaaa', 'claude:aaaaaaaa'):
        with pytest.raises(InvalidRequest):
            wait_worker(invalid, timeout=0)


def test_claude_plain_timeout_and_slow_status_budget(owned_claude, monkeypatch):
    """Snapshot shim: a busy host and slow reply, not a fabricated terminal event."""
    from ccx.ops import common as protocol_ops
    handle, _store, _pty = owned_claude
    seen = []

    def busy_status(_worker, _request, timeout=5):
        seen.append(timeout)
        time.sleep(min(timeout, .04))
        return {'ok': True, 'state': 'busy', 'activeTurnRef':
                'ccx-turn:claude:v1:active'}

    monkeypatch.setattr(ops_wait, 'claude_host_rpc', busy_status)
    started = time.monotonic()
    result = wait_worker(handle, timeout=.035)
    elapsed = time.monotonic() - started
    assert result['status'] == 'timeout' and result['state'] == 'busy'
    assert .0 <= elapsed < .25 and seen and max(seen) <= .05
    assert wait_worker(handle, timeout=0)['status'] == 'timeout'


def test_claude_slow_healthy_status_still_reaches_idle(owned_claude, monkeypatch):
    """Real host reply behind a delay longer than one poll: the probe gets the
    remaining budget (finite) or the 5s RPC bound (unbounded), not a short cap."""
    from ccx.ops import common as protocol_ops
    handle, _store, _pty = owned_claude
    real = ops_wait.claude_host_rpc
    delay = .6

    def slow_status(worker, request, timeout=5):
        if timeout < delay:
            time.sleep(timeout)
            return {'ok': False, 'error': 'timeout'}
        time.sleep(delay)
        return real(worker, request, timeout=timeout - delay)

    monkeypatch.setattr(ops_wait, 'claude_host_rpc', slow_status)
    for budget in (2.0, None):
        started = time.monotonic()
        result = wait_worker(handle, timeout=budget)
        assert result['status'] == 'idle', (budget, result)
        assert time.monotonic() - started < 2.0


def test_claude_next_turn_new_gate_and_known_input(owned_claude, monkeypatch):
    """Snapshot shim: newly published and previously known exact request IDs."""
    from ccx.ops import common as protocol_ops
    handle, _store, _pty = owned_claude
    calls = []

    def later_gate(_worker, _request, timeout=5):
        calls.append(1)
        if len(calls) == 1:
            return {'ok': True, 'state': 'busy', 'activeTurnRef': 'ccx-turn:claude:v1:r'}
        return {'ok': True, 'state': 'waiting', 'activeTurnRef': 'ccx-turn:claude:v1:r',
                'waitingFor': {'id': 'gate-new', 'tool': 'AskUserQuestion',
                               'input': {'questions': [{'question': 'pick'}]}}}

    monkeypatch.setattr(ops_wait, 'claude_host_rpc', later_gate)
    result = wait_worker(handle, next_turn=True, timeout=.8)
    assert result['status'] == 'waiting' and result['next_turn_observed'] is True
    assert result['waiting_for']['id'] == 'gate-new'
    assert result['wait_kind'] == 'question'
    assert result['last_settled_turn'] is None

    def old_gate(_worker, _request, timeout=5):
        return {'ok': True, 'state': 'waiting',
                'waitingFor': {'id': 'gate-new', 'tool': 'AskUserQuestion'}}

    monkeypatch.setattr(ops_wait, 'claude_host_rpc', old_gate)
    held = wait_worker(handle, next_turn=True, timeout=.15)
    assert held['status'] == 'timeout' and held['next_turn_observed'] is False
    held_known = WorkerClient().wait(handle, next_turn=True, known_input='gate-new',
                                     timeout=.15)
    assert held_known['status'] == 'timeout' and held_known['next_turn_observed'] is False
    with pytest.raises(SessionUnavailable, match='latest exact settlement is unknown'):
        wait_worker(handle, next_turn=True, after_turn='ccx-turn:claude:v1:checkpoint',
                    known_input='gate-new', timeout=.1)

    seen = []
    def resolved(_worker, _request, timeout=5):
        seen.append(1)
        if len(seen) == 1:
            return old_gate(_worker, _request)
        return {'ok': True, 'state': 'busy', 'activeTurnRef': 'ccx-turn:claude:v1:r'}

    monkeypatch.setattr(ops_wait, 'claude_host_rpc', resolved)
    after = wait_worker(handle, next_turn=True, known_input='gate-new', timeout=.8)
    assert after['status'] == 'busy' and after['next_turn_observed'] is True

    # A state-only transition still represents a new gate, without rich input.
    observations = []
    def no_preview(_worker, _request, timeout=5):
        observations.append(1)
        return ({'ok': True, 'state': 'busy'} if len(observations) == 1
                else {'ok': True, 'state': 'waiting', 'waitingFor': None})
    monkeypatch.setattr(ops_wait, 'claude_host_rpc', no_preview)
    no_details = wait_worker(handle, next_turn=True, timeout=.8)
    assert no_details['status'] == 'waiting' and no_details['next_turn_observed']
    assert no_details['waiting_for'] is None


def test_claude_checkpoint_busy_successor_and_unknown(owned_claude, monkeypatch):
    """Full-UUID lifecycle CAS for exact settlement, with a live-status shim."""
    from ccx.ops import common as protocol_ops
    handle, store, _pty = owned_claude
    x = 'ccx-turn:claude:v1:exact-X'
    y = 'ccx-turn:claude:v1:exact-Y'
    now = int(time.time() * 1000)
    store.mutate_record(handle, patches={
        'lastSettledAt': now,
        'lastSettledTurn': {'turnRef': x, 'turn': 1, 'outcome': 'completed',
                            'settledAtMs': now},
    })
    reads = []

    def settle_then_busy(_worker, _request, timeout=5):
        reads.append(1)
        if len(reads) == 2:
            store.mutate_record(handle, patches={
                'lastSettledAt': now + 10,
                'lastSettledTurn': {'turnRef': y, 'turn': 2,
                                    'outcome': 'unknown', 'settledAtMs': now + 10},
            })
        return {'ok': True, 'state': 'busy', 'activeTurnRef': 'ccx-turn:claude:v1:successor-Z'}

    monkeypatch.setattr(ops_wait, 'claude_host_rpc', settle_then_busy)
    result = wait_worker(handle, next_turn=True, after_turn=x, timeout=.8)
    assert result['status'] == 'busy' and result['next_turn_observed']
    assert result['last_settled_turn']['turn_ref'] == y
    assert result['last_settled_turn']['outcome'] == 'unknown'
    assert result['active_turn_ref'].endswith('successor-Z')
    repeated = WorkerClient().wait(handle, next_turn=True, after_turn=x, timeout=.1)
    assert repeated['next_turn_observed'] and repeated['worker_id'] == A
    assert not repeated['last_settled_turn']['outcome'] == 'completed'
    # Another worker's shape-valid ref is an equality checkpoint, not membership.
    foreign = wait_worker(handle, next_turn=True,
                          after_turn='ccx-turn:claude:v1:other', timeout=.1)
    assert foreign['next_turn_observed'] and foreign['last_settled_turn']['turn_ref'] == y
    with pytest.raises(SessionUnavailable, match='latest exact settlement is unknown'):
        # An unknown latest never proves the caller's checkpoint advanced.
        store.mutate_record(handle, patches={'lastSettledAt': now + 100})
        wait_worker(handle, next_turn=True, after_turn=x, timeout=.1)


@pytest.mark.parametrize('api_error,expected_exit', [
    ({'status': 429, 'kind': 'rate_limit', 'rate_limited': True,
      'transient': True}, 8),
    ({'status': 529, 'kind': 'server_error', 'rate_limited': False,
      'transient': True}, 9),
])
def test_claude_settlement_precedes_later_crash(owned_claude, monkeypatch,
                                                capsys, api_error, expected_exit):
    """Snapshot/CAS shim: exact Y boundary then death, retaining API classification."""
    from ccx.ops import common as protocol_ops
    from ccx.cli.commands import _common as commands
    handle, store, _pty = owned_claude
    calls = []
    now = int(time.time() * 1000)

    def before_crash(_worker, _request, timeout=5):
        calls.append(1)
        if len(calls) == 2:
            store.mutate_record(handle, patches={
                'lastSettledAt': now,
                'lastSettledTurn': {'turnRef': 'ccx-turn:claude:v1:terminal-Y',
                                    'outcome': 'unknown', 'turn': 1,
                                    'settledAtMs': now},
                'lifecycle.state': 'crashed',
                'lifecycle.apiError': api_error,
            })
        return {'ok': True, 'state': 'busy'}

    monkeypatch.setattr(ops_wait, 'claude_host_rpc', before_crash)
    result = wait_worker(handle, next_turn=True, timeout=.8)
    assert result['status'] == 'crashed' and result['next_turn_observed']
    assert result['apiError'] == api_error
    worker = WorkerClient().wait(handle, timeout=.1)
    assert worker['status'] == 'crashed'
    assert worker['api_error'] == api_error
    assert not ({"apiError", "lastTurn", "pendingApprovals"} & worker.keys())
    assert {"api_error", "last_turn", "pending_approvals"} <= worker.keys()
    with pytest.raises(SystemExit) as error:
        commands_turns._emit_full_uuid_wait(Namespace(json=True), result)
    assert error.value.code == expected_exit
    assert json.loads(capsys.readouterr().out)['next_turn_observed'] is True


def test_claude_hostless_stable_record_not_false_idle(owned_claude, monkeypatch):
    """Status loss alone cannot make a recorded idle a proven live idle."""
    from ccx.ops import common as protocol_ops
    handle, store, _pty = owned_claude
    monkeypatch.setattr(ops_wait, 'claude_host_rpc',
                        lambda *_args, **_kw: {'ok': False, 'error': 'timeout'})
    # A still-alive authenticated original host prevents a false host-dead verdict.
    verdict = wait_worker(handle, timeout=.1)
    assert verdict['status'] == 'timeout'
    assert store.record(handle)['lifecycle']['state'] == 'idle'


def test_codex_real_daemon_python_cli_v1_and_no_implicit_send(short_root, tmp_path, monkeypatch):
    from ccx.errors import CapabilityUnsupported
    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        created = CodexClient().create(cwd=str(tmp_path), worker_id=B)
        assert created.worker_id == B
        handle = f'codex:{B}'
        py = CodexClient().wait(B, timeout=.4)
        assert py.status == 'stopped' and not py.timed_out
        assert py.worker_id == B
        neutral = WorkerClient().wait(handle, timeout=.4)
        assert (neutral['worker'], neutral['worker_id'], neutral['engine'],
                neutral['status']) == (handle, B, 'codex', py.status)
        assert neutral['last_settled_turn'] is None
        native = v1(store, handle)
        assert native['ok'] and native['result']['status'] == 'stopped', native
        out = cli('wait', B, '--timeout', '.4', '--json')
        assert out.returncode == 0, (out.stdout, out.stderr)
        assert json.loads(out.stdout)['worker'] == handle
        assert 'RPC turn/start' not in trace.read_text()
        # A failed older native turn is visible but cannot poison idle worker wait.
        CodexClient().send(B, 'initial-wait-failed older-result', wait=False,
                           delivery_id='wait-failed-old-turn')
        end = time.monotonic() + 6
        while True:
            settled = CodexClient().wait(B, timeout=.25)
            if settled.status == 'idle':
                break
            assert time.monotonic() < end, settled.as_dict()
        assert settled.turn_status == 'failed'
        assert settled.report is None  # no preserved earlier cached answer
        neutral = WorkerClient().wait(handle, timeout=.3)
        assert neutral['status'] == 'idle'
        assert neutral['last_turn']['status'] == 'failed'
        assert neutral['last_settled_turn']['outcome'] == 'failed'
        result_with_report = v1(store, handle, params={'include_report': True},
                                timeout_ms=900)
        assert result_with_report['ok'], result_with_report
        assert result_with_report['result']['report'] is None
        assert result_with_report['result']['report_error']
        assert trace.read_text().count('RPC turn/start') == 1
        with pytest.raises(CapabilityUnsupported):
            wait_worker(handle, next_turn=True, timeout=.2)
        with pytest.raises(CapabilityUnsupported):
            WorkerClient().wait(handle, next_turn=True, timeout=.2)
        with pytest.raises(CapabilityUnsupported):
            WorkerClient().wait(handle, next_turn=True,
                                after_turn='ccx-turn:codex:v1:unsupported', timeout=.2)
        rejected = v1(store, handle, params={'include_inspect': True})
        assert rejected['ok'] is False and rejected['error']['code'] in {
            'invalid_request',
        }, rejected


def test_codex_real_approval_gate_and_old_failure_does_not_poison_idle(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(short_root, tmp_path, monkeypatch,
                            approval_mode=True) as (
        _cli, _restart, trace, store, _rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        CodexClient().create(cwd=str(tmp_path), worker_id=B)
        CodexClient().send(B, 'codex-approve-full-uuid-wait', wait=False,
                           delivery_id='gate-wait-1')
        until = time.monotonic() + 8
        while time.monotonic() < until:
            gates = store.record(B).get('pendingApprovals') or []
            if gates and gates[0].get('gateId'):
                break
            time.sleep(.04)
        assert gates and gates[0]['gateId']
        result = WorkerClient().wait(f'codex:{B}', timeout=.5)
        assert result['status'] == 'waiting'
        assert result['pending_input']['request_id'] == gates[0]['gateId']
        assert result['wait_kind'] == 'permission'
        assert trace.read_text().count('RPC turn/start') == 1


def test_claude_wait_idle_rechecks_settled_ref_committed_during_status_rpc(
    owned_claude, monkeypatch,
):
    """RED deterministic snapshot race: record CAS during host status RPC.

    The first record snapshot has no lastSettledTurn. The host publishes an
    original durable completion before answering status=idle. A successful
    stable-state wait must not return a stale null checkpoint if that durable
    settlement already exists before the host says idle. No model request.
    """
    from ccx.ops import common as protocol_ops

    handle, store, _pty = owned_claude
    assert store.record(handle).get("lastSettledTurn") is None
    ref = "ccx-turn:claude:v1:published-during-status"
    now = int(time.time() * 1000)
    invoked = []

    def commit_then_idle(worker, request, timeout=5):
        assert worker == handle and request == {"op": "status"}
        invoked.append(1)
        store.mutate_record(handle, patches={
            "lastSettledAt": now,
            "lastSettledTurn": {"turnRef": ref, "turn": 1,
                                "outcome": "completed", "settledAtMs": now},
            "lifecycle.state": "idle",
        })
        return {"ok": True, "state": "idle", "activeTurnRef": None,
                "pendingDeliveries": []}

    monkeypatch.setattr(ops_wait, "claude_host_rpc", commit_then_idle)
    got = ops_wait.wait_worker(handle, timeout=.5)
    assert invoked and got["status"] == "idle"
    # Proven by rereading the original generation, not a synthetic stdout.
    assert store.record(handle)["lastSettledTurn"]["turnRef"] == ref
    assert got["last_settled_turn"] is not None, (
        "wait emitted idle with a stale null turn reference despite durable "
        "commit occurring before the host status reply", got
    )
    assert got["last_settled_turn"]["turn_ref"] == ref


def test_codex_wait_idle_rechecks_original_settlement_committed_during_status_rpc(
    short_root, monkeypatch,
):
    """RED status-snapshot race: durable Codex turn publishes during RPC.

    A loaded daemon can answer its earlier status snapshot while the original
    generation commits a completed turn during the RPC. An idle wait with no
    old turn identity must re-read that exact record once before returning.
    All native transport is fake; no Codex daemon or model is launched.
    """
    from ccx.codex import runtime as codex_runtime
    from ccx.ops import common as protocol_ops

    store = add_worker(short_root, B, engine="codex", confirmed=True)
    handle = f"codex:{B}"
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    assert store.record(handle).get("lastTurn") is None
    now = int(time.time() * 1000)
    calls = []

    def commit_then_idle(payload, timeout=5):
        assert payload["op"] == "status" and payload["short"] == B
        calls.append(1)
        store.mutate_record(handle, patches={
            "lastTurn": {"turnId": "native-completed-during-status",
                         "status": "completed", "completedAtMs": now},
            "lastTurnAt": now, "lifecycle.state": "idle",
        })
        # An original RPC result may be a snapshot captured before the CAS.
        return {"ok": True, "transport": "served", "loaded": True,
                "rpcGeneration": constants.CODEXD_RPC_GENERATION,
                "state": "idle", "activeTurnId": None,
                "lastTurn": None, "pendingApprovals": []}

    monkeypatch.setattr(codex_runtime, "daemon_rpc", commit_then_idle)
    got = ops_wait.wait_worker(handle, timeout=.5)
    assert calls and got["status"] == "idle"
    assert store.record(handle)["lastTurn"]["turnId"] == "native-completed-during-status"
    assert got["last_settled_turn"] is not None, (
        "idle Codex wait omitted exact newly committed original turn after "
        "status RPC", got
    )
    assert got["last_settled_turn"]["turn_ref"] == (
        "ccx-turn:codex:v1:native-completed-during-status"
    )


def test_claude_idle_without_original_settlement_stays_unknown(
    owned_claude, monkeypatch,
):
    """An IDLE status is not evidence that any exact turn has settled."""
    from ccx.ops import common as protocol_ops

    handle, store, _pty = owned_claude
    assert store.record(handle).get("lastSettledTurn") is None
    calls = []

    def idle_status(worker, request, timeout=5):
        assert request == {"op": "status"} and worker == handle
        calls.append(1)
        return {"ok": True, "state": "idle", "activeTurnRef": None,
                "pendingDeliveries": []}

    monkeypatch.setattr(ops_wait, "claude_host_rpc", idle_status)
    got = ops_wait.wait_worker(handle, timeout=.5)
    assert got["status"] == "idle" and got["last_settled_turn"] is None
    assert calls == [1]  # no extra status RPC


def test_codex_idle_without_original_settlement_stays_unknown(
    short_root, monkeypatch,
):
    """Codex also must not synthesize an exact turn from mere IDLE."""
    from ccx.codex import runtime as codex_runtime
    from ccx.ops import common as protocol_ops

    store = add_worker(short_root, B, engine="codex", confirmed=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    assert store.record(f"codex:{B}").get("lastTurn") is None
    calls = []

    def idle_status(payload, timeout=5):
        assert payload["op"] == "status" and payload["short"] == B
        calls.append(1)
        return {"ok": True, "transport": "served", "loaded": True,
                "rpcGeneration": constants.CODEXD_RPC_GENERATION,
                "state": "idle", "activeTurnId": None,
                "lastTurn": None, "pendingApprovals": []}

    monkeypatch.setattr(codex_runtime, "daemon_rpc", idle_status)
    got = ops_wait.wait_worker(f"codex:{B}", timeout=.5)
    assert got["status"] == "idle" and got["last_settled_turn"] is None
    assert calls == [1]
