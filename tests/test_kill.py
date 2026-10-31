"""Full-UUID kill: physical fake host + shared Codex daemon, no actual providers."""
from __future__ import annotations
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import pytest
from ccx import CcxClient, CodexClient, WorkerClient, constants
from ccx.errors import SessionUnavailable
from ccx.store.liveness import owned_pid_alive
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, short_root
from test_create_cross_surface import _deadline_fake_claude, _first_wait_original_claude_ready
from test_claude_native_cli import _stop_disposable_original
from test_cancel_core import _cancel_enabled_codex_binary, _wait_ref
import test_codex_native_effects as codex_fake


def _v1_park(store, target, timeout_ms=12000):
    from ccx.rpc.local_rpc import dispatch_full_uuid
    return dispatch_full_uuid({"protocol_version":2,"request_id":"K-park-1",
        "op":"worker.kill","params":{"target":target},"timeout_ms":timeout_ms},store)


def _until_trace(trace, needle, seconds=5):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        if trace.exists() and needle in trace.read_text():
            return
        time.sleep(.05)
    raise AssertionError(f"missing original provider call {needle!r}: {trace.read_text() if trace.exists() else 'absent'}")


def test_k_claude_active_kill_and_idempotent_worker_sibling(
    short_root,tmp_path,monkeypatch,
):
    _deadline_fake_claude(short_root,tmp_path,monkeypatch)
    monkeypatch.setenv('FAKE_HOOKS_VIA_SETTINGS','1')
    monkeypatch.setenv('FAKE_MODE','nostop')
    store=require_ccx_active(short_root)
    sibling='aaaaaaaa-1234-4000-8000-000000000099'
    repo=Path(__file__).resolve().parent.parent
    first=f'claude:{A}'
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id==A
        active=CcxClient().new('k native running first turn',cwd=str(tmp_path),worker_id=A,
                                wait=False,start_timeout=4)
        assert active.worker_id==A
        _wait_ref(first)
        sibling_created=CcxClient().new(cwd=str(tmp_path),worker_id=sibling,start_timeout=12)
        assert sibling_created.worker_id==sibling
        rec=store.record(first); native=rec['native']['sessionId']
        host_pid=rec['lifecycle']['hostPid'];child_pid=rec['lifecycle']['workerPid']
        host_start=rec['lifecycle']['hostStart'];child_start=rec['lifecycle']['workerStart']
        assert owned_pid_alive(host_pid,host_start) and owned_pid_alive(child_pid,child_start)
        before_sib=store.record(sibling)
        cli=subprocess.run([sys.executable,'-B','-m','ccx','kill',first,'--json'],
            cwd=repo,env={**os.environ},text=True,capture_output=True,timeout=25)
        assert cli.returncode==0,(cli.stdout,cli.stderr)
        result=json.loads(cli.stdout)
        assert result['worker']==first and result['killed'] is True
        # A history-bearing original Claude worker is now an eligible resume.
        assert result['history_retained'] is True and result['resume_available'] is True
        assert result['resumable'] is True
        assert result['changed'] is True and result['state']=='stopped'
        assert store.record(first)['native']['sessionId']==native
        assert store.record(first)['lifecycle']['state']=='dead'
        assert not owned_pid_alive(host_pid,host_start)
        assert not owned_pid_alive(child_pid,child_start)
        assert owned_pid_alive(before_sib['lifecycle']['hostPid'],
                               before_sib['lifecycle']['hostStart'])
        again=WorkerClient().kill(first)
        assert again['worker']==first and again['killed'] is True
        assert again['changed'] is False and again['resume_available'] is True
        assert again['resumable'] is True
        assert store.record(sibling)['native']['sessionId']==before_sib['native']['sessionId']
    finally:
        _stop_disposable_original(short_root)
        # dispose sibling from the test root, never from global CCX.
        try:
            from ccx.rpc.rpc import _rpc
            _rpc(f'claude:{sibling}',{'op':'kill'},timeout=2)
        except Exception:
            pass


def test_k_claude_missing_start_token_refuses_before_signal(
    short_root,tmp_path,monkeypatch,
):
    _deadline_fake_claude(short_root,tmp_path,monkeypatch)
    store=require_ccx_active(short_root)
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id==A
        created=CcxClient().new(cwd=str(tmp_path),worker_id=A,start_timeout=4)
        assert created.worker_id==A
        rec=store.record(A)
        pid=rec['lifecycle']['hostPid'];start=rec['lifecycle']['hostStart']
        assert owned_pid_alive(pid,start)
        store.patch_lifecycle(A,changes={'hostStart':None})
        with pytest.raises(SessionUnavailable,match='process could not be verified'):
            WorkerClient().kill(f'claude:{A}',timeout=2)
        assert owned_pid_alive(pid,start)  # no signal reached original host
    finally:
        _stop_disposable_original(short_root)


def test_k_claude_original_host_sigkill_orphan_scoped_cleanup(
    short_root,tmp_path,monkeypatch,
):
    """An unclean original host exit cannot leave its recorded child behind."""
    _deadline_fake_claude(short_root,tmp_path,monkeypatch)
    monkeypatch.setenv('FAKE_HOOKS_VIA_SETTINGS','1')
    store=require_ccx_active(short_root)
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id==A
        assert CcxClient().new(cwd=str(tmp_path),worker_id=A,start_timeout=4).worker_id==A
        rec=store.record(A)
        host_pid=rec['lifecycle']['hostPid'];child_pid=rec['lifecycle']['workerPid']
        host_start=rec['lifecycle']['hostStart'];child_start=rec['lifecycle']['workerStart']
        original_session=rec['native']['sessionId']
        assert owned_pid_alive(host_pid,host_start) and owned_pid_alive(child_pid,child_start)
        os.kill(host_pid,signal.SIGKILL)
        end=time.monotonic()+3
        while time.monotonic()<end and owned_pid_alive(host_pid,host_start):
            time.sleep(.04)
        result=WorkerClient().kill(f'claude:{A}',timeout=8)
        assert result['killed'] is True and result['worker']==f'claude:{A}'
        assert not owned_pid_alive(host_pid,host_start)
        assert not owned_pid_alive(child_pid,child_start)
        assert store.record(A)['native']['sessionId']==original_session
        assert store.record(A)['lifecycle']['state']=='dead'
    finally:
        _stop_disposable_original(short_root)



def test_k_codex_promptless_park_exact_unsubscribe_no_native_recreate(
    short_root,tmp_path,monkeypatch,
):
    with codex_fake._f2_fake_processes(short_root,tmp_path,monkeypatch) as (
        cli,_restart,trace,store,daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN',str(tmp_path/'fake-codex'))
        created=CodexClient().create(cwd=str(tmp_path),worker_id=B)
        assert created.worker_id==B
        native=store.record(B)['native']['threadId']
        prior=daemon_rpc({'op':'status','short':B,'rpcGeneration':constants.CODEXD_RPC_GENERATION},timeout=1)
        assert prior['loaded'] is False
        reply=_v1_park(store,f'codex:{B}')
        assert reply['ok'] is True,reply
        result=reply['result']
        assert result['worker']==f'codex:{B}' and result['killed'] is True
        assert result['resumable'] is False
        assert result['changed'] is True and result['state']=='stopped'
        assert store.record(B)['native']['threadId']==native
        assert store.record(B)['lifecycle']['state']=='dead'
        _until_trace(trace,"RPC thread/unsubscribe {'threadId': '"+native+"'")
        text=trace.read_text()
        assert text.count('RPC thread/unsubscribe')==1
        assert text.count('RPC thread/start')==1
        assert 'RPC thread/resume' not in text
        assert 'RPC turn/start' not in text
        again=WorkerClient().kill(f'codex:{B}')
        assert again['changed'] is False and again['worker']==f'codex:{B}'
        assert trace.read_text().count('RPC thread/unsubscribe')==1


def test_k_codex_busy_sibling_exact_interrupt_and_shared_daemon_alive(
    short_root,tmp_path,monkeypatch,
):
    _cancel_enabled_codex_binary(monkeypatch)
    other='aaaaaaaa-5678-4000-8000-000000000099'
    with codex_fake._f2_fake_processes(short_root,tmp_path,monkeypatch) as (
        cli,_restart,trace,store,daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN',str(tmp_path/'fake-codex'))
        client=CodexClient()
        for wid in (B,other):
            assert client.create('cancel-hold-k-'+wid,cwd=str(tmp_path),worker_id=wid).worker_id==wid
        refs={wid:_wait_ref(f'codex:{wid}') for wid in (B,other)}
        original={wid:store.record(wid)['native']['threadId'] for wid in (B,other)}
        before=daemon_rpc({'op':'daemon-status'},timeout=1)
        killed=cli('kill',B,'--json')
        assert killed.returncode==0,(killed.stdout,killed.stderr)
        result=json.loads(killed.stdout)
        assert result['worker']==f'codex:{B}' and result['killed'] is True
        assert result['history_retained'] is True and not result['resume_available']
        _until_trace(trace,"RPC thread/unsubscribe {'threadId': '"+original[B]+"'")
        text=trace.read_text()
        assert "RPC turn/interrupt {'threadId': '"+original[B]+"'" in text
        assert "RPC thread/unsubscribe {'threadId': '"+original[other]+"'" not in text
        assert text.count('RPC thread/unsubscribe')==1
        assert store.record(other)['lifecycle']['state']=='busy'
        after=daemon_rpc({'op':'daemon-status'},timeout=1)
        assert after['pid']==before['pid'] and after['appServerPid']==before['appServerPid']
        assert store.record(B)['native']['threadId']==original[B]
        assert WorkerClient().kill(f'codex:{B}')['changed'] is False
        assert trace.read_text().count('RPC thread/unsubscribe')==1


def test_k_codex_native_turn_start_held_vs_kill_never_confirms_early(
    short_root,tmp_path,monkeypatch,
):
    """One original native start fences kill; no second native request."""
    from concurrent.futures import ThreadPoolExecutor
    from ccx.errors import SendIndeterminate
    _cancel_enabled_codex_binary(monkeypatch)
    original=codex_fake._fake_codex_executable
    gate=tmp_path/'release-k-race'

    def gated_fake(*args,**kwargs):
        binary=original(*args,**kwargs)
        source=binary.read_text()
        marker="            result={'turn':{'id':turn_id,'status':'inProgress'}}"
        assert source.count(marker)==1
        source=source.replace(marker,
            """            if text.startswith('cancel-hold-k-race-'):
                import time
                until=time.monotonic()+8
                while not os.path.exists(__K_RELEASE__) and time.monotonic()<until:
                    time.sleep(.01)
""".replace('__K_RELEASE__',repr(str(gate)))+marker,1)
        binary.write_text(source)
        return binary

    monkeypatch.setattr(codex_fake,'_fake_codex_executable',gated_fake)
    with codex_fake._f2_fake_processes(short_root,tmp_path,monkeypatch) as (
        cli,_restart,trace,store,daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN',str(tmp_path/'fake-codex'))
        assert CodexClient().create(cwd=str(tmp_path),worker_id=B).worker_id==B
        target=f'codex:{B}'
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending=pool.submit(lambda:CodexClient().send(
                target,'cancel-hold-k-race-start',delivery_id='k-race-original',wait=False))
            _until_trace(trace,'RPC turn/start')
            try:
                with pytest.raises(SessionUnavailable):
                    WorkerClient().kill(target,timeout=.3)
                assert store.record(B)['lifecycle']['state']!='dead'
                assert trace.read_text().count('RPC turn/start')==1
            finally:
                gate.touch()
            try:
                pending.result(timeout=8)
            except (SendIndeterminate, SessionUnavailable,RuntimeError):
                pass
        # The short operation-lock timeout did not accept a background kill.
        # Once the original start settles, explicitly retry the same worker.
        assert WorkerClient().kill(target)['changed'] is True
        assert store.record(B)['lifecycle']['state']=='dead'
        assert WorkerClient().kill(target)['changed'] is False
        assert trace.read_text().count('RPC turn/start')==1
        assert trace.read_text().count('RPC turn/interrupt')==1
        assert trace.read_text().count('RPC thread/unsubscribe')==1
