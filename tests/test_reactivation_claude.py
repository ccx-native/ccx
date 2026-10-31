"""Original managed Claude UUID runtime revival using real disposable host/PTY."""
from __future__ import annotations
import ccx.ops.wait as ops_wait

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from ccx import CcxClient, WorkerClient
from ccx.errors import DeliveryRejected
from ccx.store.root import require_ccx_active
from test_generation_store import A, short_root
from test_create_cross_surface import _deadline_fake_claude, _first_wait_original_claude_ready
from test_cancel_core import _wait_ref
from test_claude_native_cli import _stop_disposable_original


def _setup(short_root, tmp_path, monkeypatch):
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv('FAKE_HOOKS_VIA_SETTINGS', '1')
    monkeypatch.setenv('FAKE_LOG', str(tmp_path / 'r2-claude-pty.log'))
    monkeypatch.setenv('FAKE_HOOK_RESULT_LOG', str(tmp_path / 'r2-hooks.jsonl'))
    return launches, require_ccx_active(short_root)


def test_existing_live_busy_send_refusal_is_typed_before_revive(
    short_root, tmp_path, monkeypatch,
):
    _launches, store = _setup(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv('FAKE_MODE','nostop')
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        created = CcxClient().new('R2 first ongoing turn', cwd=str(tmp_path),
                                   worker_id=A, wait=False, start_timeout=4)
        assert created.worker_id == A
        assert _wait_ref('claude:'+A)
        with pytest.raises(DeliveryRejected) as caught:
            CcxClient().send('claude:'+A, 'R2 forbidden busy successor',
                             delivery_id='r2-live-busy', wait=False)
        print('BASELINE_BUSY_TYPED:',repr(vars(caught.value)),str(caught.value),flush=True)
        assert caught.value.refusal == 'busy'
        assert store.record(A)['lifecycle']['state'] == 'busy'
    finally:
        _stop_disposable_original(short_root)


def _record_fake_native_argv(tmp_path):
    """Observe the real host's provider argv; only test-local fake wrapper."""
    import shlex
    wrapper = tmp_path / 'deadline-claude-fake'
    source = wrapper.read_text()
    needle = '\nexec '
    assert source.count(needle) == 1
    argv_log = tmp_path / 'R2-original-native-argv.txt'
    source = source.replace(
        needle,
        '\nprintf \'%s\\n\' "$*" >> ' + shlex.quote(str(argv_log)) + needle,
    )
    wrapper.write_text(source)
    return argv_log


def test_explicit_claude_resume_same_original_session_and_current_hook_proof(
    short_root, tmp_path, monkeypatch,
):
    from ccx.store.liveness import owned_pid_alive
    launches, store = _setup(short_root, tmp_path, monkeypatch)
    argv_log = _record_fake_native_argv(tmp_path)
    handle = 'claude:'+A
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id == A
        created = CcxClient().new('R2 actual original historical turn', cwd=str(tmp_path),
                                  worker_id=A, wait=True, timeout=15,start_timeout=4)
        assert created.settled, created
        from ccx.store.paths import settings_path
        worker_settings = Path(settings_path(A))
        assert json.loads(worker_settings.read_text())["tui"] == "default"
        initial = store.record(A)
        session_id = initial['native']['sessionId']
        old_proof = initial['lifecycle']['nativeStartProof']
        original_transcript = Path(initial['transcript'])
        assert original_transcript.is_file() and original_transcript.stat().st_size > 0
        original_bytes = original_transcript.read_bytes()
        assert owned_pid_alive(old_proof['hostPid'],old_proof['hostStart'])
        assert owned_pid_alive(old_proof['workerPid'],old_proof['workerStart'])
        assert len(argv_log.read_text().splitlines()) == 1
        assert '--session-id '+session_id in argv_log.read_text()
        stopped = WorkerClient().kill(handle, timeout=12)
        assert stopped['killed'] and stopped['resume_available'] is True, stopped
        assert stopped['resumable'] is True
        assert not owned_pid_alive(old_proof['hostPid'],old_proof['hostStart'])
        assert not owned_pid_alive(old_proof['workerPid'],old_proof['workerStart'])
        resumed = WorkerClient().resume(handle,timeout=16)
        assert resumed['worker']==handle and resumed['session_id']==session_id
        assert resumed['resumed'] is True and resumed['state']=='idle',resumed
        # Revival invokes the same managed host setup, rewriting only the
        # worker-owned sidecar with the classic renderer selection.
        assert json.loads(worker_settings.read_text())["tui"] == "default"
        current=store.record(A)
        new_proof=current['lifecycle']['nativeStartProof']
        assert new_proof['sessionId'] == session_id
        assert new_proof['submissionId'] == old_proof['submissionId']
        assert new_proof['hostPid'] != old_proof['hostPid']
        assert new_proof['workerPid'] != old_proof['workerPid']
        assert new_proof['nativeEvidenceSha256'] != old_proof['nativeEvidenceSha256']
        assert owned_pid_alive(new_proof['hostPid'],new_proof['hostStart'])
        assert owned_pid_alive(new_proof['workerPid'],new_proof['workerStart'])
        assert store.native_activation(A)['nativeBinding']['sessionId']==session_id
        assert current['stats']['turns'] == initial['stats']['turns']
        assert original_transcript.read_bytes() == original_bytes
        lines=argv_log.read_text().splitlines()
        assert len(lines)==2,lines
        assert '--resume '+session_id in lines[1] and '--session-id' not in lines[1]
        assert '--fork-session' not in lines[1]
        assert launches.read_text().splitlines()==['provider-start','provider-start']
        # A same-user client is NOT a descendant of this native child: a
        # forged SessionStart cannot rebind even when its host RPC authenticates.
        from ccx.rpc.rpc import _rpc
        forged = _rpc(handle, {
            'op':'hook-event','event':'SessionStart',
            'data':{'session_id':'00000000-0000-4000-8000-000000000009'},
        },timeout=2)
        assert forged.get('ok') is False, forged
        assert store.record(A)['lifecycle']['nativeStartProof'] == new_proof
        again=WorkerClient().resume(handle,timeout=5)
        assert again['resumed'] is False and again['session_id']==session_id
        assert len(argv_log.read_text().splitlines())==2
    finally:
        _stop_disposable_original(short_root)


SECOND = 'aaaaaaaa-9876-4000-8000-000000000099'


def _cli(*args, timeout=24):
    repo = Path(__file__).resolve().parent.parent
    return subprocess.run([sys.executable,'-B','-m','ccx',*args],
                          env={**os.environ}, cwd=repo,
                          capture_output=True, text=True, timeout=timeout)


def test_claude_direct_fresh_send_after_kill_with_sibling_and_original_token_replay(
    short_root, tmp_path, monkeypatch,
):
    from ccx.store.liveness import owned_pid_alive
    from ccx.errors import OperationConflict
    launches,store = _setup(short_root,tmp_path,monkeypatch)
    argv_log = _record_fake_native_argv(tmp_path)
    original = 'claude:'+A
    sibling = 'claude:'+SECOND
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id == A
        initial_prompt = 'R2 original history for same native session'
        first = CcxClient().new(initial_prompt, cwd=str(tmp_path), worker_id=A,
                                wait=True, timeout=15,start_timeout=4)
        assert first.settled
        first_record=store.record(A)
        original_session=first_record['native']['sessionId']
        source_history=Path(first_record['transcript']).read_bytes()
        # Independent globally owned same-prefix UUID remains fully live.
        neighbor=CcxClient().new(cwd=str(tmp_path),worker_id=SECOND,start_timeout=12)
        assert neighbor.worker_id==SECOND and A[:8]==SECOND[:8]
        sib=store.record(SECOND)
        sib_host=(sib['lifecycle']['hostPid'],sib['lifecycle']['hostStart'])
        assert owned_pid_alive(*sib_host)
        assert WorkerClient().kill(original)['resume_available'] is True
        before=launches.read_text().splitlines()
        # Same semantic ID is read-only even when the original host is dead.
        replay=CcxClient().send(original,initial_prompt,delivery_id='create:initial',wait=False)
        assert replay.delivery_id=='create:initial'
        with pytest.raises(OperationConflict):
            CcxClient().send(original,'changed replay text',
                             delivery_id='create:initial',wait=False)
        assert launches.read_text().splitlines()==before
        assert Path(first_record['transcript']).read_bytes()==source_history
        nxt=CcxClient().send(original,'R2 one fresh requested turn after kill',
                             delivery_id='r2-direct-next',wait=True,timeout=18)
        assert nxt.settled and nxt.delivery_id=='r2-direct-next',nxt
        assert store.record(A)['native']['sessionId']==original_session
        assert store.record(SECOND)['native']['sessionId']==sib['native']['sessionId']
        assert owned_pid_alive(*sib_host)
        starts=launches.read_text().splitlines()
        assert len(starts)==len(before)+1 and len(starts)==3,starts
        argv=argv_log.read_text().splitlines()
        assert sum('--resume '+original_session in line for line in argv)==1,argv
        assert '--session-id' not in argv[-1]
        assert source_history in Path(first_record['transcript']).read_bytes()
        # The same unchanged protocol V1 send route, no new resume V1 op.
        from ccx.rpc.local_rpc import dispatch_full_uuid
        parked=WorkerClient().kill(original)
        assert parked['killed'] and parked['resume_available']
        frame=dispatch_full_uuid({
            'protocol_version':2,'request_id':'R2-V1-fresh',
            'op':'turn.send','params':{
                'target':original,'text':'R2 explicit V1 next native turn',
                'delivery_id':'r2-v1-next','mode':'new_turn_only',
            },'timeout_ms':16000,
        },store)
        assert frame['ok'] is True,frame
        assert frame['result']['worker']=='claude:'+A
        assert len(launches.read_text().splitlines())==len(starts)+1
    finally:
        _stop_disposable_original(short_root)
        try:
            from ccx.rpc.rpc import _rpc
            _rpc(sibling,{'op':'kill'},timeout=2)
        except Exception:
            pass


def test_claude_zero_turn_original_park_refuses_before_second_provider_start(
    short_root, tmp_path, monkeypatch,
):
    from ccx.errors import SessionUnavailable
    starts,store = _setup(short_root,tmp_path,monkeypatch)
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id == A
        created=CcxClient().new(cwd=str(tmp_path),worker_id=A,start_timeout=4)
        assert created.worker_id==A
        rec=store.record(A)
        original_native=rec['native']['sessionId']
        original_claim=store.native_activation(A)
        path=Path(rec['transcript'])
        assert not path.exists() or path.stat().st_size==0
        stopped=WorkerClient().kill('claude:'+A)
        assert stopped['killed'] is True
        assert stopped['resume_available'] is False and stopped['resumable'] is False
        before=starts.read_text()
        with pytest.raises(SessionUnavailable,match='empty transcript|history|resume'):
            WorkerClient().resume('claude:'+A,timeout=3)
        assert starts.read_text()==before
        assert store.record(A)['native']['sessionId']==original_native
        assert store.native_activation(A)==original_claim
    finally:
        _stop_disposable_original(short_root)



def test_same_uuid_simultaneous_claude_resume_one_new_host_and_one_winner(
    short_root, tmp_path, monkeypatch,
):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    launches, store = _setup(short_root,tmp_path,monkeypatch)
    argv_log = _record_fake_native_argv(tmp_path)
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id == A
        first=CcxClient().new('R2 one historical turn for concurrent revive',
                              cwd=str(tmp_path),worker_id=A,
                              wait=True,timeout=15,start_timeout=4)
        assert first.settled
        native=store.record(A)['native']['sessionId']
        assert WorkerClient().kill('claude:'+A)['killed']
        both=Barrier(2)
        def resume_once():
            both.wait(timeout=8)
            return WorkerClient().resume('claude:'+A,timeout=22)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(resume_once) for _ in range(2)]
            results=[f.result(timeout=26) for f in futures]
        assert [r['resumed'] for r in results].count(True)==1,results
        assert all(r['worker']=='claude:'+A and r['session_id']==native
                   and r['state']=='idle' for r in results)
        assert launches.read_text().splitlines()==['provider-start','provider-start']
        assert len(argv_log.read_text().splitlines())==2
    finally:
        _stop_disposable_original(short_root)


def test_missing_original_cwd_refuses_before_claude_resume_or_native_launch(
    short_root, tmp_path, monkeypatch,
):
    import shutil
    from ccx.errors import SessionUnavailable
    launches,store=_setup(short_root,tmp_path,monkeypatch)
    project=tmp_path/'disposable-project'
    project.mkdir(mode=0o700)
    try:
        assert _first_wait_original_claude_ready(project,A).worker_id == A
        first=CcxClient().new('R2 historical turn, then workspace vanishes',
                              cwd=str(project),worker_id=A,wait=True,
                              timeout=15,start_timeout=4)
        assert first.settled
        original=store.record(A)['native']['sessionId']
        assert WorkerClient().kill('claude:'+A)['killed']
        before=launches.read_text()
        shutil.rmtree(project)
        with pytest.raises(SessionUnavailable,match='working directory'):
            WorkerClient().resume('claude:'+A,timeout=2)
        assert launches.read_text()==before
        assert store.record(A)['native']['sessionId']==original
    finally:
        _stop_disposable_original(short_root)


def test_claude_revive_lock_timeout_keeps_typed_launch_verdict(tmp_path,monkeypatch):
    """An exhausted existing respawn lock refuses, never leaks tuple unpack."""
    from ccx.rpc import rpc
    monkeypatch.setattr(rpc, 'respawn_lock_path', lambda _target: tmp_path/'lock')
    monkeypatch.setattr(rpc, '_acquire_respawn_lock', lambda _file, _deadline: False)
    assert rpc._respawn('claude:aaaaaaaa-1234-4000-8000-000000000001',
                        deadline=time.time()+.1,report_launch=True) == (1,False)
    assert rpc._respawn('claude:aaaaaaaa-1234-4000-8000-000000000001',
                        deadline=time.time()+.1) == 1


def test_failed_probes_never_tear_down_owned_live_busy_or_waiting_host(
    short_root, tmp_path, monkeypatch,
):
    """Missed status RPCs are not evidence the owned live host is gone."""
    from ccx.ops import common as protocol_ops
    from ccx.rpc import rpc
    from ccx.errors import SessionUnavailable
    from ccx.store.liveness import owned_pid_alive
    _launches, store = _setup(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv('FAKE_MODE','nostop')
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        assert CcxClient().new('R2 guard ongoing turn', cwd=str(tmp_path),
                               worker_id=A, wait=False, start_timeout=4).worker_id == A
        assert _wait_ref('claude:'+A)
        end = time.monotonic() + 5
        # The host can publish busy before the fake writes the transcript.
        while not (store.record(A)['lifecycle'].get('state') == 'busy'
                   and os.path.isfile(store.record(A)['transcript'])
                   and os.path.getsize(store.record(A)['transcript']) > 0) and time.monotonic() < end:
            time.sleep(.04)
        rec = store.record(A)
        lc = rec['lifecycle']
        assert lc['state'] == 'busy' and os.path.getsize(rec['transcript']) > 0
        assert owned_pid_alive(lc['hostPid'], lc['hostStart'])
        effects, verdicts = [], []
        # Deterministic stall: every startup-status/status probe times out.
        monkeypatch.setattr(rpc, '_rpc', lambda *_a, **_k: {'ok': False, 'error': 'timeout'})
        monkeypatch.setattr(ops_wait, 'park_worker',
                            lambda *a, **k: effects.append(('park', a)))
        monkeypatch.setattr(rpc, 'spawn_host_detached',
                            lambda *a, **k: effects.append(('spawn', a)))
        locked = rpc._respawn_locked
        monkeypatch.setattr(rpc, '_respawn_locked',
                            lambda *a, **k: verdicts.append(locked(*a, **k)) or verdicts[-1])
        for state in ('busy', 'waiting'):
            if state == 'waiting':
                store.patch_lifecycle('claude:'+A, changes={'state': 'waiting'})
            with pytest.raises(SessionUnavailable):
                WorkerClient().resume('claude:'+A, timeout=3)
            assert effects == [] and verdicts[-1] == 1
            assert store.record(A)['lifecycle']['state'] == state
            assert owned_pid_alive(lc['hostPid'], lc['hostStart'])
            assert owned_pid_alive(lc['workerPid'], lc['workerStart'])
        assert len(verdicts) == 2
    finally:
        monkeypatch.undo()
        _stop_disposable_original(short_root)


def test_background_task_native_resume_idle_is_process_scoped(
    short_root, tmp_path, monkeypatch,
):
    """Old transcript-open Bash never protects a new native host; new Bash does."""
    from ccx.errors import CreatePartialFailure
    from ccx.claude.transcript import extract_background_tasks
    from test_transcript import _bg_launch_events

    launches, store = _setup(short_root, tmp_path, monkeypatch)
    handle = 'claude:' + A

    def state_after(timeout):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            record = store.record(A)
            if record['lifecycle']['state'] == 'idle-killed':
                return 'idle-killed'
            time.sleep(.05)
        return store.record(A)['lifecycle']['state']

    def events():
        path = store.worker_path(A).parent / 'events.ndjson'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def append_open_bash(path, task_id, tool_use_id):
        with path.open('a', encoding='utf-8') as stream:
            for row in _bg_launch_events(task_id=task_id, tool_use_id=tool_use_id):
                stream.write(json.dumps(row) + '\n')

    try:
        until = time.monotonic() + 20
        while True:
            try:
                first = CcxClient().new(
                    'First native fake turn settles', cwd=str(tmp_path), worker_id=A,
                    idle_timeout=2, wait=True, timeout=13, start_timeout=5,
                )
                assert first.settled, first
                break
            except CreatePartialFailure as exc:
                assert exc.phase == 'native-activation' and exc.worker_id == A
                assert time.monotonic() < until, exc.info
                time.sleep(.05)
        first_record = store.record(A)
        transcript = Path(first_record['transcript'])
        assert transcript.is_file()
        assert state_after(10) == 'idle-killed', 'first fake host did not idle-kill'

        append_open_bash(transcript, 'prior-bash', 'toolu_prior')
        earlier_size = transcript.stat().st_size
        second = WorkerClient().resume(handle, timeout=16)
        assert second['state'] == 'idle', second

        # First failing assertion on the untouched base: a resumed real host
        # mistakenly sees this old open Bash and emits idle_grace_bg indefinitely.
        actual = state_after(10)
        assert actual == 'idle-killed', (
            f'prior-native Bash blocked actual host idle kill: state={actual}, '
            f'events={[e.get("type") for e in events()[-12:]]}'
        )

        # New metadata assertions deliberately follow the baseline behavior proof.
        second_record = store.record(A)
        assert second_record['lifecycle']['nativeTranscriptStart'] == earlier_size
        prior = next(t for t in extract_background_tasks(transcript)
                     if t['task_id'] == 'prior-bash')
        assert prior['last_open_offset'] < earlier_size

        third = WorkerClient().resume(handle, timeout=16)
        assert third['state'] == 'idle', third
        third_record = store.record(A)
        assert third_record['lifecycle']['nativeTranscriptStart'] == transcript.stat().st_size
        before = len(events())
        append_open_bash(transcript, 'current-bash', 'toolu_current')

        until = time.monotonic() + 8
        while time.monotonic() < until:
            if any(e.get('type') == 'idle_grace_bg' for e in events()[before:]):
                break
            time.sleep(.05)
        assert any(e.get('type') == 'idle_grace_bg' for e in events()[before:]), (
            'current task did not protect fake host idle grace'
        )
        time.sleep(4)  # twice the configured timeout: current work still protects it
        assert store.record(A)['lifecycle']['state'] == 'idle'

        facts = CcxClient()._uuid_background_task_facts(A)
        rows = {t['task_id']: t for t in CcxClient().background_tasks(A)}
        assert set(rows) == {'prior-bash', 'current-bash'}
        assert rows['prior-bash']['status'] == 'running'
        assert rows['prior-bash']['liveness'] == 'stale'
        assert rows['prior-bash']['active'] is False
        assert rows['current-bash']['status'] == 'running'
        assert rows['current-bash']['active'] is True
        assert all('last_open_offset' not in row for row in rows.values())
        assert facts['background_task_count'] == 2
        assert facts['running_background_task_count'] == 2
        assert facts['open_background_task_count'] == 2
        assert facts['active_background_task_count'] == 1
        assert facts['stale_background_task_count'] == 1
        assert facts['background_task_hint'].startswith('1 background task(s) active')
        assert launches.read_text().splitlines() == [
            'provider-start', 'provider-start', 'provider-start',
        ]
    finally:
        _stop_disposable_original(short_root)


def test_offline_converted_claude_first_resume_records_start_proof(
    short_root, tmp_path, monkeypatch,
):
    """A cutover-converted worker (confirmed, no submissionId) revives."""
    from test_converter_runtime_binding import _converted
    _launches, store = _setup(short_root, tmp_path, monkeypatch)
    handle = 'claude:'+A
    record, _ = _converted(store, tmp_path, A, 'claude')
    session_id = record['native']['sessionId']
    transcript = Path(record['transcript'])
    transcript.parent.mkdir(parents=True, exist_ok=True)
    transcript.write_text('{"type":"user"}\n')
    assert store.native_activation(A)['submissionId'] is None
    try:
        resumed = WorkerClient().resume(handle, timeout=16)
        assert resumed['resumed'] is True and resumed['state'] == 'idle', resumed
        assert resumed['session_id'] == session_id
        proof = store.record(A)['lifecycle']['nativeStartProof']
        assert proof['sessionId'] == session_id and proof['submissionId'] is None
        assert store.native_activation(A)['submissionId'] is None
        # A later revive (e.g. after idle-kill) must authenticate the
        # first lifetime's proof and admit the new one.
        assert WorkerClient().kill(handle, timeout=12)['killed']
        again = WorkerClient().resume(handle, timeout=16)
        assert again['resumed'] is True and again['state'] == 'idle', again
        second = store.record(A)['lifecycle']['nativeStartProof']
        assert second['sessionId'] == session_id and second['submissionId'] is None
        assert second['hostPid'] != proof['hostPid']
    finally:
        _stop_disposable_original(short_root)
