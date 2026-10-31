"""Physical CLI/Python stop adapters over the single full-UUID cancellation core."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from ccx import CcxClient, CodexClient, WorkerClient
from ccx.errors import DeliveryRejected
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, short_root
from test_cancel_core import (
    _cancel_enabled_codex_binary,
    _native_escape_count,
    _wait_ref,
)
from test_create_cross_surface import (
    _deadline_fake_claude,
    _first_wait_original_claude_ready,
)
from test_claude_native_cli import _stop_disposable_original
import test_codex_native_effects as codex_fake


def _real_escape_count(path: Path, count: int):
    limit = time.monotonic() + 4
    while time.monotonic() < limit:
        if path.exists() and _native_escape_count(path) >= count:
            break
        time.sleep(.04)
    assert path.exists() and _native_escape_count(path) == count


def test_claude_cli_and_python_stop_only_original_active_turn_and_generated_hooks(
    short_root, tmp_path, monkeypatch,
):
    starts = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    text_log = tmp_path / 'public-stop-claude-pty.log'
    hooks_log = tmp_path / 'public-stop-claude-generated-hooks.jsonl'
    monkeypatch.setenv('FAKE_LOG', str(text_log))
    monkeypatch.setenv('FAKE_MODE', 'nostop')
    monkeypatch.setenv('FAKE_HOOKS_VIA_SETTINGS', '1')
    monkeypatch.setenv('FAKE_HOOK_RESULT_LOG', str(hooks_log))
    root = require_ccx_active(short_root)
    handle = f'claude:{A}'
    repo = Path(__file__).resolve().parent.parent

    def cli(*args):
        return subprocess.run([sys.executable, '-B', '-m', 'ccx', *args],
                              cwd=repo, env={**os.environ},
                              capture_output=True, text=True, timeout=18)

    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        created = CcxClient().new('physical public Claude stop turn',
                                  cwd=str(tmp_path), worker_id=A, wait=False,
                                  start_timeout=4)
        assert created.worker_id == A
        original = _wait_ref(handle)
        original_host = root.record(A)['lifecycle']['hostPid']
        assert original_host
        first = cli('stop', handle, '--json')
        assert first.returncode == 0, (first.stdout, first.stderr)
        item = json.loads(first.stdout)
        assert item['worker']['worker'] == handle
        assert item['requested'] is True and item['accepted'] is True
        assert item['turn_ref'] == original and item['cancellation_id']
        assert item['confirmed'] is False and item['cancelled'] is False
        assert item['status'] in {'submitted', 'unknown'}
        _real_escape_count(text_log, 1)

        # Existing exact-ref Python option is honored, not silently discarded.
        client = CcxClient()
        second = client.interrupt(handle, expected_turn_ref=original)
        assert second['ok'] is True and second['sent'] is True
        assert second['accepted'] is True and not second['cancelled']
        assert second['cancellation_id'] != item['cancellation_id']
        assert second['turn_ref'] == original
        _real_escape_count(text_log, 2)

        normalized = WorkerClient().stop(handle)
        assert normalized['worker_id'] == A and normalized['requested'] is True
        assert normalized['worker'] == handle
        assert normalized['accepted'] is True and not normalized['cancelled']
        assert normalized['cancellation_id'] not in {
            item['cancellation_id'], second['cancellation_id']}
        _real_escape_count(text_log, 3)
        assert not hasattr(WorkerClient, "interrupt")

        with pytest.raises(DeliveryRejected):
            client.interrupt(handle, expected_turn_ref='ccx-turn:claude:v1:stale-ref')
        for invalid in (
            ('stop', handle, '--wait'),
            ('stop', handle, '--notify-parent'),
            ('stop', A[:7]),  # below the 8-hex prefix minimum: invalid query
        ):
            refused = cli(*invalid)
            assert refused.returncode != 0, (invalid, refused.stdout, refused.stderr)
        _real_escape_count(text_log, 3)
        assert root.record(A)['lifecycle']['hostPid'] == original_host
        assert root.record(A)['native']['sessionId'] == root.native_activation(A)['nativeBinding']['sessionId']
        assert starts.read_text().splitlines() == ['provider-start']
        finish = time.monotonic() + 3
        events = []
        while time.monotonic() < finish:
            if hooks_log.exists():
                events = [json.loads(line) for line in hooks_log.read_text().splitlines()]
            if {'SessionStart', 'UserPromptSubmit'} <= {e['event'] for e in events}:
                break
            time.sleep(.04)
        assert {'SessionStart', 'UserPromptSubmit'} <= {e['event'] for e in events}
        assert all(e['rc'] == 0 for e in events)
    finally:
        _stop_disposable_original(short_root)


def test_claude_completed_turn_cli_and_python_idle_stop_refused(
    short_root, tmp_path, monkeypatch,
):
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    log = tmp_path / 'idle-stop-pty.log'
    monkeypatch.setenv('FAKE_LOG', str(log))
    monkeypatch.setenv('FAKE_HOOKS_VIA_SETTINGS', '1')
    repo = Path(__file__).resolve().parent.parent
    handle = f'claude:{A}'
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        first = CcxClient().new('completed before stop requested',
                                cwd=str(tmp_path), worker_id=A,
                                wait=True, timeout=10, start_timeout=4)
        assert first.settled is True
        refused = subprocess.run(
            [sys.executable, '-B', '-m', 'ccx', 'stop', handle, '--json'],
            cwd=repo, env={**os.environ}, capture_output=True,
            text=True, timeout=16,
        )
        assert refused.returncode == 5, (refused.stdout, refused.stderr)
        assert json.loads(refused.stdout)['status'] == 'rejected'
        with pytest.raises(DeliveryRejected):
            CcxClient().interrupt(handle)
        with pytest.raises(DeliveryRejected):
            WorkerClient().stop(handle)
        assert _native_escape_count(log) == 0
    finally:
        _stop_disposable_original(short_root)



def test_codex_cli_python_and_worker_stop_exact_thread_siblings_untouched(
    short_root, tmp_path, monkeypatch,
):
    _cancel_enabled_codex_binary(monkeypatch)
    siblings = (B, 'aaaaaaaa-9876-4000-8000-000000000099',
                'aaaaaaaa-4321-4000-8000-000000000098')
    assert all(id[:8] == 'aaaaaaaa' for id in siblings)
    with codex_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        for wid in siblings:
            original = client.create('cancel-hold-public-' + wid, cwd=str(tmp_path),
                                     worker_id=wid, wait=False)
            assert original.worker_id == wid
        refs = {wid: _wait_ref(f'codex:{wid}') for wid in siblings}
        original_native = {wid: store.record(wid)['native']['threadId'] for wid in siblings}
        prior_daemon = daemon_rpc({'op':'daemon-status'}, timeout=1)
        assert prior_daemon['ok'] is True

        target, other, last = siblings
        bad = cli('stop', target[:8], '--json')
        assert bad.returncode != 0, (bad.stdout, bad.stderr)
        assert trace.read_text().count('RPC turn/interrupt') == 0

        first = cli('stop', target, '--json')
        assert first.returncode == 0, (first.stdout, first.stderr)
        payload = json.loads(first.stdout)
        assert payload['worker']['worker'] == f'codex:{target}'
        assert payload['requested'] is True and payload['accepted'] is True
        assert payload['confirmed'] is True and payload['outcome'] == 'interrupted'
        assert payload['cancelled'] is True
        assert payload['turn_ref'] == refs[target]
        assert payload['cancellation_id']
        native_trace = trace.read_text()
        assert native_trace.count('RPC turn/interrupt') == 1
        assert "RPC turn/interrupt {'threadId': '" + original_native[target] + "'" in native_trace
        assert "RPC turn/interrupt {'threadId': '" + original_native[other] + "'" not in native_trace
        assert store.record(other)['lifecycle']['state'] == 'busy'

        # Existing Codex Python signature accepts the *native* expected ID,
        # but never an unfenced interrupt or an unrelated worker's turn.
        with pytest.raises(DeliveryRejected):
            client.interrupt(other, expected_turn_id=refs[target].rsplit(':', 1)[-1])
        assert trace.read_text().count('RPC turn/interrupt') == 1
        typed = client.interrupt(other, expected_turn_id=refs[other].rsplit(':', 1)[-1])
        assert typed.accepted is True and typed.action == 'interrupt'
        assert typed.worker_id == other
        assert typed.turn_id == refs[other].rsplit(':', 1)[-1]
        assert typed.thread_id == original_native[other]

        facade = WorkerClient().stop(f'codex:{last}')
        assert facade['worker_id'] == last and facade['requested'] is True
        assert facade['accepted'] is True and facade['confirmed'] is True
        assert facade['outcome'] == 'interrupted'
        assert facade['turn_id'] == refs[last].rsplit(':', 1)[-1]
        with pytest.raises(DeliveryRejected):
            client.interrupt(target)  # already terminal, not a new native effect
        after_idle = cli('stop', target, '--json')
        assert after_idle.returncode == 5, (after_idle.stdout, after_idle.stderr)
        assert json.loads(after_idle.stdout)['status'] == 'rejected'
        assert trace.read_text().count('RPC turn/interrupt') == 3
        assert trace.read_text().count('RPC turn/start') == 3
        assert trace.read_text().count('RPC thread/start') == 3
        assert {wid: store.record(wid)['native']['threadId'] for wid in siblings} == original_native
        after_daemon = daemon_rpc({'op':'daemon-status'}, timeout=1)
        assert after_daemon['ok'] is True
        assert after_daemon['pid'] == prior_daemon['pid']
        assert after_daemon['appServerPid'] == prior_daemon['appServerPid']
