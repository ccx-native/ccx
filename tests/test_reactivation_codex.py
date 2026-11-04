"""Actual singleton Codex reactivation: original thread subscription and history."""
from __future__ import annotations

import json
import os
from pathlib import Path
import time

import pytest

from ccx import CodexClient, WorkerClient, constants
from ccx.errors import DeliveryRejected, SessionUnavailable, OperationConflict
from test_generation_store import B, short_root
import test_codex_native_effects as native_fake


SECOND = "aaaaaaaa-9876-4000-8000-000000000099"


def _subscription_fenced_provider(
    monkeypatch, *, hold_resume=None, hold_unsubscribe=None,
    hold_turns=False, resume_mode=None, fail_with_system_error=False,
    recovery_turn_gate=None, slow_parked_read_s=None,
):
    """Extend only one generated disposable executable, not the shared fixture."""
    factory = native_fake._fake_codex_executable

    def build(*args, **kwargs):
        binary = factory(*args, **kwargs)
        text = binary.read_text()
        changes = (
            ("counter=len(threads)\n", "counter=len(threads)\nsubscribed=set()\n"),
            ("            if PERSIST:\n", "            subscribed.add(tid)\n            if PERSIST:\n"),
            ("            if tid not in threads: raise ValueError('unknown turn thread')\n",
             "            if tid not in threads: raise ValueError('unknown turn thread')\n"
             "            if tid not in subscribed: raise ValueError('thread not subscribed')\n"),
            ("        elif method=='thread/unsubscribe':\n            result={}\n",
             "        elif method=='thread/resume':\n"
             "            tid=params.get('threadId')\n"
             "            if tid not in threads: raise ValueError('unknown native thread')\n"
             "            if not threads[tid]['turns']: raise ValueError('no rollout found')\n"
             "            subscribed.add(tid)\n"
             "            result={'thread':dict(threads[tid],status={'type':'idle'}),'model':threads[tid].get('model')}\n"
             "        elif method=='thread/unsubscribe':\n"
             "            subscribed.discard(params.get('threadId'))\n"
             "            result={}\n"),
        )
        for before, after in changes:
            assert text.count(before) == 1, before
            text = text.replace(before, after, 1)
        if slow_parked_read_s is not None:
            marker = "        elif method=='thread/read':\n            tid=params.get('threadId')\n"
            assert text.count(marker) == 1
            text = text.replace(marker, marker + (
                "            if tid in threads and tid not in subscribed and threads[tid]['turns']:\n"
                "                with open(TRACE,'a',encoding='utf8') as trace:\n"
                "                    trace.write('SLOW_PARKED_THREAD_READ '+repr(tid)+chr(10))\n"
                f"                time.sleep({slow_parked_read_s!r})\n"
            ), 1)
        if hold_resume is not None:
            marker = "            result={'thread':dict(threads[tid],status={'type':'idle'}),'model':threads[tid].get('model')}\n"
            assert text.count(marker) == 1
            text = text.replace(marker,
                "            until=time.monotonic()+7\n"
                f"            while not os.path.exists({str(hold_resume)!r}) and time.monotonic()<until:\n"
                "                time.sleep(.01)\n" + marker, 1)
        if hold_unsubscribe is not None:
            # Delay only this reply/effect: stdin must keep reading, so a
            # premature thread/resume is actually visible in the trace.
            marker = ("        elif method=='thread/unsubscribe':\n"
                      "            subscribed.discard(params.get('threadId'))\n"
                      "            result={}\n")
            assert text.count(marker) == 1
            text = text.replace("import sys,json,os,time\n",
                                "import sys,json,os,time,threading\n", 1)
            gate = repr(str(hold_unsubscribe))
            delayed = f"""        elif method=='thread/unsubscribe':
            # Bound now: the fake's loop is module level, so a held second
            # unsubscribe must not overwrite the first one's reply id.
            def settle_unsubscribe(release_id=msg['id'], release_thread=params.get('threadId')):
                until=time.monotonic()+20
                while not os.path.exists({gate}) and time.monotonic()<until:
                    time.sleep(.01)
                if not os.path.exists({gate}):
                    return
                subscribed.discard(release_thread)
                print(json.dumps({{'jsonrpc':'2.0','id':release_id,'result':{{}}}}),flush=True)
                with open(TRACE,'a',encoding='utf8') as trace:
                    trace.write('UNSUBSCRIBE_REPLIED '+repr(release_thread)+'\\n')
            threading.Thread(target=settle_unsubscribe,daemon=True).start()
            continue
"""
            text = text.replace(marker, delayed, 1)
        if hold_turns:
            # A 'busy-hold-' turn stays inProgress: the worker is live busy.
            for before, after in (
                ("            status=('failed' if text.startswith('initial-wait-failed')",
                 "            status=('inProgress' if text.startswith('busy-hold-')\n"
                 "                    else 'failed' if text.startswith('initial-wait-failed')"),
                ("            print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',\n"
                 "                'params':{'threadId':tid,'turn':turn}}),flush=True)\n"
                 "            if GATE and text.startswith('f1-race-'):",
                 "            if not text.startswith('busy-hold-'):\n"
                 "                print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',\n"
                 "                    'params':{'threadId':tid,'turn':turn}}),flush=True)\n"
                 "            if GATE and text.startswith('f1-race-'):"),
            ):
                assert text.count(before) == 1, before
                text = text.replace(before, after, 1)
        if fail_with_system_error:
            changes = (
                ("            status=('failed' if text.startswith('initial-wait-failed')",
                 "            status=('failed' if text.startswith('resume-429-')\n"
                 "                    else 'failed' if text.startswith('initial-wait-failed')"),
                ("            if status!='completed':turn['error']={'message':'fake '+status+' original turn'}\n",
                 "            if status!='completed':turn['error']={'message':'fake '+status+' original turn'}\n"
                 "            if text.startswith('resume-429-'):\n"
                 "                turn['error']={'message':'exceeded retry limit, last status: 429 Too Many Requests'}\n"),
                ("            print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',\n"
                 "                'params':{'threadId':tid,'turn':turn}}),flush=True)\n"
                 "            if GATE and text.startswith('f1-race-'):\n",
                 "            print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',\n"
                 "                'params':{'threadId':tid,'turn':turn}}),flush=True)\n"
                 "            if text.startswith('resume-429-'):\n"
                 "                threads[tid]['status']={'type':'systemError'}\n"
                 "                print(json.dumps({'jsonrpc':'2.0','method':'thread/status/changed',\n"
                 "                    'params':{'threadId':tid,'status':{'type':'systemError'}}}),flush=True)\n"
                 "            if GATE and text.startswith('f1-race-'):\n"),
            )
            for before, after in changes:
                assert text.count(before) == 1, before
                text = text.replace(before, after, 1)
        if recovery_turn_gate is not None:
            # Codex can re-emit the persisted systemError on the first turn
            # before its fresh turn/started (or active status) is delivered.
            marker = ("        if method=='turn/start':\n"
                      "            print(json.dumps({'jsonrpc':'2.0','method':'turn/started',\n")
            assert text.count(marker) == 1
            text = text.replace(marker, f"""        if method=='turn/start':
            if text.startswith('post-resume-first-'):
                if threads[tid].get('status')!={{'type':'systemError'}}:
                    raise ValueError('first resumed turn lost persisted systemError')
                print(json.dumps({{'jsonrpc':'2.0','method':'thread/status/changed',
                    'params':{{'threadId':tid,'status':{{'type':'systemError'}}}}}}),flush=True)
                time.sleep(.15)  # allow daemon notification reduction before the gate
                with open(TRACE,'a',encoding='utf8') as trace:
                    trace.write('STALE_SYSTEMERROR_BEFORE_TURN\\n')
                until=time.monotonic()+8
                while not os.path.exists({str(recovery_turn_gate)!r}) and time.monotonic()<until:
                    time.sleep(.01)
                if not os.path.exists({str(recovery_turn_gate)!r}):
                    raise ValueError('first resumed turn never released')
                threads[tid]['status']={{'type':'active','activeFlags':[]}}
            print(json.dumps({{'jsonrpc':'2.0','method':'turn/started',
""", 1)
            marker = ("            print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',\n"
                      "                'params':{'threadId':tid,'turn':turn}}),flush=True)\n"
                      "            if text.startswith('resume-429-'):\n")
            assert text.count(marker) == 1
            text = text.replace(marker, marker.replace(
                "            if text.startswith('resume-429-'):\n",
                "            if text.startswith('post-resume-first-'):\n"
                "                threads[tid]['status']={'type':'idle'}\n"
                "            if text.startswith('resume-429-'):\n",
            ), 1)
        if resume_mode is not None:
            marker = "            result={'thread':dict(threads[tid],status={'type':'idle'}),'model':threads[tid].get('model')}\n"
            assert text.count(marker) == 1
            text = text.replace(marker, f"""            mode=open({str(resume_mode)!r},encoding='utf8').read().strip()
            if mode=='lost':
                continue  # subscribed, but no native reply ever arrives
            if mode=='error':
                print(json.dumps({{'jsonrpc':'2.0','id':msg['id'],
                    'error':{{'code':-32603,'message':'resume refused by native server'}}}}),flush=True)
                continue
            observed=dict(threads[tid],status={{'type':'idle'}})
            if mode=='systemError': observed['status']={{'type':'systemError'}}
            if mode=='persisted': observed['status']=threads[tid].get('status',{{'type':'idle'}})
            if mode=='active': observed['status']={{'type':'active','activeFlags':[]}}
            if mode=='notLoaded': observed['status']={{'type':'notLoaded'}}
            if mode=='wrong-id': observed['id']='wrong-original-thread'
            if mode=='wrong-source': observed['threadSource']='ccx-worker:wrong-owner'
            if mode=='malformed': observed['status']='invalid'
            result={{'thread':observed,'model':threads[tid].get('model')}}
""", 1)
        binary.write_text(text)
        return binary

    monkeypatch.setattr(native_fake, '_fake_codex_executable', build)


def _trace_count(trace, part):
    return trace.read_text().count(part) if trace.exists() else 0


def _until(trace, part, timeout=4):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if trace.exists() and part in trace.read_text():
            return
        time.sleep(.04)
    raise AssertionError(f"missing {part!r}: {trace.read_text() if trace.exists() else 'no trace'}")


def test_direct_fresh_send_after_park_resubscribes_same_original_thread(
    short_root, tmp_path, monkeypatch,
):
    _subscription_fenced_provider(monkeypatch)
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        created = client.create(cwd=str(tmp_path),worker_id=B)
        first = client.send('codex:'+B, 'historical first turn', delivery_id='history-first',wait=True,timeout=15)
        sibling = client.create('sibling historical turn', cwd=str(tmp_path),
                                worker_id=SECOND, wait=True, timeout=15)
        assert first.worker_id == B and sibling.worker_id == SECOND
        assert B[:8] == SECOND[:8]
        identity = store.record(B)['native']['threadId']
        sibling_id = store.record(SECOND)['native']['threadId']
        assert identity != sibling_id
        before = daemon_rpc({'op': 'daemon-status'}, timeout=1)
        first_count = _trace_count(trace, 'RPC turn/start')
        assert first_count == 2
        parked = WorkerClient().kill('codex:' + B)
        assert parked['killed'] and parked['changed']
        _until(trace, "RPC thread/unsubscribe {'threadId': '" + identity + "'")
        assert "RPC thread/unsubscribe {'threadId': '" + sibling_id + "'" not in trace.read_text()
        continued = client.send('codex:' + B, 'the same conversation continued',
                                delivery_id='R1-next-message', wait=True, timeout=15)
        assert continued.worker_id == B and continued.thread_id == identity
        assert continued.reply and 'the same conversation continued' in continued.reply
        assert _trace_count(trace, 'RPC thread/resume') == 1
        assert _trace_count(trace, 'RPC turn/start') == first_count + 1
        assert _trace_count(trace, 'RPC thread/start') == 2
        assert _trace_count(trace, 'RPC thread/fork') == 0
        # Resume's params include original cwd/settings before threadId;
        # the one native RPC was already counted above.
        assert "'threadId': '" + identity + "'" in trace.read_text()
        assert "RPC thread/unsubscribe {'threadId': '" + sibling_id + "'" not in trace.read_text()
        after = daemon_rpc({'op': 'daemon-status'}, timeout=1)
        assert after['pid'] == before['pid'] and after['appServerPid'] == before['appServerPid']
        assert store.record(B)['native']['threadId'] == identity
        assert store.record(SECOND)['native']['threadId'] == sibling_id
        assert store.record(SECOND)['lifecycle']['state'] == 'idle'

        # V1's existing closed send route is the very same fresh readiness
        # path, not a new protocol resume verb or a replacement native thread.
        from ccx.rpc.local_rpc import dispatch_full_uuid
        assert WorkerClient().kill('codex:' + B)['killed'] is True
        answer = dispatch_full_uuid({
            'protocol_version': 2, 'request_id': 'R1-v1-next',
            'op': 'turn.send',
            'params': {'target': 'codex:' + B, 'text': 'V1 next original turn',
                       'delivery_id': 'R1-v1-only', 'mode': 'new_turn_only'},
            'timeout_ms': 15000,
        }, store)
        assert answer['ok'] is True, answer
        assert answer['result']['worker'] == 'codex:' + B
        assert _trace_count(trace, 'RPC thread/resume') == 2
        assert _trace_count(trace, 'RPC turn/start') == first_count + 2
        assert _trace_count(trace, 'RPC thread/start') == 2


def test_idle_killed_worker_slow_native_read_uses_resume_budget(
    short_root, tmp_path, monkeypatch,
):
    # A large rollout can take longer than one second to load. Delay only
    # thread/read after unsubscribe, not the initial native activation.
    _subscription_fenced_provider(monkeypatch, slow_parked_read_s=1.5)
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        client.create(cwd=str(tmp_path), worker_id=B, idle_timeout=1)
        original = client.send(B, 'history before idle kill',
                               delivery_id='slow-read-first', wait=True, timeout=15)
        deadline = time.monotonic() + 8
        while (store.record(B)['lifecycle']['state'] != 'idle-killed'
               and time.monotonic() < deadline):
            time.sleep(.04)
        assert store.record(B)['lifecycle']['state'] == 'idle-killed'
        _until(trace, 'RPC thread/unsubscribe')

        resumed = client.resume(B, timeout=12)
        assert resumed.resumed is True and resumed.state == 'idle'
        assert resumed.thread_id == original.thread_id
        assert _trace_count(trace, 'SLOW_PARKED_THREAD_READ') == 1
        assert _trace_count(trace, 'RPC thread/resume') == 1
        continued = client.send(B, 'continue after slow adoption',
                                delivery_id='slow-read-next', wait=True, timeout=15)
        assert continued.thread_id == original.thread_id
        assert continued.turn_id != original.turn_id
        assert _trace_count(trace, 'RPC turn/start') == 2
        assert _trace_count(trace, 'RPC thread/start') == 1


def test_original_resume_waits_for_unsubscribe_answer_before_resubscribing(
    short_root, tmp_path, monkeypatch,
):
    from concurrent.futures import ThreadPoolExecutor

    release = tmp_path / 'RC1-unsubscribe-release'
    _subscription_fenced_provider(monkeypatch, hold_unsubscribe=release)
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        try:
            assert client.create(cwd=str(tmp_path), worker_id=B).worker_id == B
            original = client.send(B, 'RC1 historical turn', delivery_id='RC1-first',
                                   wait=True, timeout=15)
            thread = original.thread_id
            assert thread == store.record(B)['native']['threadId']
            assert WorkerClient().kill('codex:' + B)['killed']
            _until(trace, "RPC thread/unsubscribe {'threadId': '" + thread + "'")
            reads = _trace_count(trace, 'RPC thread/read')
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(lambda: CodexClient().resume(B, timeout=12))
                # Native thread/read after the held unsubscribe proves the
                # fake is still reading stdin, so an early resume is visible.
                until = time.monotonic() + 4
                while _trace_count(trace, 'RPC thread/read') <= reads and time.monotonic() < until:
                    time.sleep(.03)
                assert _trace_count(trace, 'RPC thread/read') > reads
                # Either release-pending fence may hold the call: it waits, or
                # refuses. Neither may submit a native resume meanwhile.
                time.sleep(.5)
                assert _trace_count(trace, 'RPC thread/resume') == 0
                release.touch()
                _until(trace, 'UNSUBSCRIBE_REPLIED ' + repr(thread))
                try:
                    resumed = pending.result(timeout=10)
                except SessionUnavailable as exc:
                    assert 'has not yet confirmed releasing thread' in str(exc)
                    # The refused load keeps its unsubscribe obligation: retry
                    # once only after every unsubscribe sent has been answered.
                    until = time.monotonic() + 4
                    while (_trace_count(trace, 'UNSUBSCRIBE_REPLIED')
                           < _trace_count(trace, 'RPC thread/unsubscribe')
                           and time.monotonic() < until):
                        time.sleep(.04)
                    assert (_trace_count(trace, 'UNSUBSCRIBE_REPLIED')
                            == _trace_count(trace, 'RPC thread/unsubscribe'))
                    resumed = CodexClient().resume(B, timeout=12)
            assert resumed.thread_id == thread and resumed.resumed is True
            continued = client.send(B, 'RC1 continued once', delivery_id='RC1-next',
                                    wait=True, timeout=15)
            assert continued.thread_id == thread
            assert continued.reply and 'RC1 continued once' in continued.reply
            assert _trace_count(trace, 'RPC thread/resume') == 1
            assert _trace_count(trace, 'RPC thread/start') == 1
            assert _trace_count(trace, 'RPC thread/fork') == 0
            assert _trace_count(trace, 'RPC turn/start') == 2
        finally:
            release.touch()


def test_explicit_resume_then_send_and_original_token_replay_without_revival(
    short_root, tmp_path, monkeypatch,
):
    _subscription_fenced_provider(monkeypatch)
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        assert client.create(cwd=str(tmp_path), worker_id=B).worker_id == B
        first = client.send(B, 'the original first Codex turn',
                            delivery_id='original-R1-token', wait=True, timeout=15)
        assert first.turn_id and first.thread_id
        original_thread = first.thread_id
        before = _trace_count(trace, 'RPC turn/start')
        parked = WorkerClient().kill('codex:' + B)
        assert parked['killed'] and parked['resume_available'] is True
        assert parked['resumable'] is True and parked['history_retained'] is True
        _until(trace, 'RPC thread/unsubscribe')

        # Same ID and wrong-content conflict must be read-only even though
        # the target runtime is terminal, and must not invoke thread/resume.
        retry = client.send(B, 'the original first Codex turn',
                            delivery_id='original-R1-token', wait=False)
        assert retry.turn_id == first.turn_id and retry.thread_id == original_thread
        with pytest.raises(OperationConflict):
            client.send(B, 'different prompt', delivery_id='original-R1-token', wait=False)
        assert _trace_count(trace, 'RPC turn/start') == before
        assert _trace_count(trace, 'RPC thread/resume') == 0

        resumed = client.resume(B)
        assert resumed.worker_id == B and resumed.thread_id == original_thread
        assert resumed.resumed is True and resumed.state == 'idle'
        assert _trace_count(trace, 'RPC thread/resume') == 1
        assert _trace_count(trace, 'RPC turn/start') == before
        # The original resume re-applies the same full-UUID caller stamp as
        # the first thread/start (Codex applies it per start/resume request).
        # The fake proves the request only, not real command env inheritance.
        text = trace.read_text()
        start_request = text.split('RPC thread/start ', 1)[1].split('RPC ', 1)[0]
        resume_request = text.split('RPC thread/resume ', 1)[1].split('RPC ', 1)[0]
        expected_env = str(
            store.pointer.generation_root / 'variables' / 'env' / 'codex' / (B + '.sh')
        )
        expected_zdotdir = str(
            store.pointer.generation_root / 'variables' / 'env' / 'codex' / 'zsh' / B
        )
        for actual_request in (start_request, resume_request):
            assert "'shell_environment_policy': {'set': " in actual_request
            assert "'CCX_SELF_WORKER_ID': '" + B + "'" in actual_request
            assert "'CCX_SELF_ENGINE': 'codex'" in actual_request
            assert "'BASH_ENV': '" + expected_env + "'" in actual_request
            assert "'ZDOTDIR': '" + expected_zdotdir + "'" in actual_request
        assert expected_env not in str(store.pointer.generation_root / 'variables' / 'env' / 'codex' / (SECOND + '.sh'))
        assert Path(expected_env).is_file()
        assert (Path(expected_zdotdir) / '.zshenv').is_file()
        assert '--engine codex --short ' + B in Path(expected_env).read_text()
        assert "'threadId': '" + original_thread + "'" in resume_request
        assert _trace_count(trace, 'RPC thread/start') == 1
        facade = WorkerClient().resume('codex:' + B)
        assert facade['worker'] == 'codex:' + B
        assert facade['thread_id'] == original_thread and facade['resumed'] is False
        command = cli('resume', B, '--json')
        assert command.returncode == 0, (command.stdout, command.stderr)
        projected = json.loads(command.stdout)
        assert projected['worker'] == 'codex:' + B
        assert projected['thread_id'] == original_thread
        assert projected['resumed'] is False
        assert _trace_count(trace, 'RPC thread/resume') == 1
        assert _trace_count(trace, 'RPC turn/start') == before

        # A fresh facade send is a second user turn, not part of resume.
        sent = WorkerClient().send('codex:' + B, 'only the explicitly requested next turn',
                                   delivery_id='next-R1-token', wait=True, timeout=15)
        assert sent['worker'] == 'codex:' + B
        assert sent['turn_id'] != first.turn_id
        assert _trace_count(trace, 'RPC turn/start') == before + 1
        assert _trace_count(trace, 'RPC thread/start') == 1
        assert _trace_count(trace, 'RPC thread/resume') == 1


def test_rate_limited_turn_system_error_resume_then_next_turn(
    short_root, tmp_path, monkeypatch,
):
    mode = tmp_path / 'R1-incident-resume-mode'
    mode.write_text('persisted')
    start_gate = tmp_path / 'R1-first-resumed-turn-release'
    _subscription_fenced_provider(
        monkeypatch, resume_mode=mode, fail_with_system_error=True,
        recovery_turn_gate=start_gate,
    )
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        client.create(cwd=str(tmp_path), worker_id=B)
        native_id = store.record(B)['native']['threadId']
        client.send(B, 'resume-429-rate-limited-turn',
                    delivery_id='R1-incident-failed', wait=False, timeout=15)
        deadline = time.monotonic() + 6
        while store.record(B)['lifecycle']['state'] != 'crashed' and time.monotonic() < deadline:
            time.sleep(.04)
        failed = store.record(B)
        assert failed['lifecycle']['state'] == 'crashed'
        assert failed['lifecycle']['lastError'] == (
            'Codex thread system error: '
            'exceeded retry limit, last status: 429 Too Many Requests'
        )
        assert failed['lastTurn']['status'] == 'failed'
        assert failed['lastTurn']['error']['message'] == (
            'exceeded retry limit, last status: 429 Too Many Requests'
        )
        _until(trace, 'RPC thread/unsubscribe')  # janitor unloads the crashed worker
        # The native thread's exact id, source, AND systemError survive park.
        native_read = _daemon_rpc({
            'op': 'thread-read', 'workerId': B, 'threadId': native_id,
            'includeTurns': False, 'rpcGeneration': constants.CODEXD_RPC_GENERATION,
        }, timeout=3)
        assert native_read['ok'], native_read
        assert native_read['thread']['threadSource'] == f'ccx-worker:{B}'
        assert native_read['thread']['status'] == {'type': 'systemError'}

        resumed = client.resume(B, timeout=12)
        assert resumed.state == 'idle' and resumed.resumed is True
        restored = store.record(B)
        assert restored['native']['threadId'] == native_id
        assert restored['lifecycle']['state'] == 'idle'
        assert restored['lifecycle']['lastError'] is None
        assert restored.get('activeTurnError') is None
        assert restored['lifecycle']['activeTurnId'] is None
        assert restored['lastTurn'] == failed['lastTurn']  # no new turn from resume
        native_after_resume = _daemon_rpc({
            'op': 'thread-read', 'workerId': B, 'threadId': native_id,
            'includeTurns': False, 'rpcGeneration': constants.CODEXD_RPC_GENERATION,
        }, timeout=3)
        assert native_after_resume['ok'], native_after_resume
        assert native_after_resume['thread']['status'] == {'type': 'systemError'}

        try:
            # A replayed stale status edge lands BEFORE any fresh turn event.
            # It must not mark the accepted resumed worker crashed.
            following = client.send(B, 'post-resume-first-success',
                                    delivery_id='R1-incident-next', wait=False, timeout=15)
            assert following.thread_id == native_id and following.turn_id
            _until(trace, 'STALE_SYSTEMERROR_BEFORE_TURN')
            during = store.record(B)
            assert during['lifecycle']['state'] == 'busy', during['lifecycle']
            assert during['lifecycle']['lastError'] is None
            assert during['lastTurn'] == failed['lastTurn']
        finally:
            start_gate.touch()
        deadline = time.monotonic() + 6
        while store.record(B)['lastTurn']['status'] != 'completed' and time.monotonic() < deadline:
            time.sleep(.04)
        assert store.record(B)['lastTurn']['status'] == 'completed'
        assert store.record(B)['lifecycle']['state'] == 'idle'
        assert store.record(B)['lifecycle']['lastError'] is None

        # The guard is only for the prior native error. A new failed turn
        # with its own turn/started and systemError edge must still crash.
        client.send(B, 'resume-429-another-failed-turn',
                    delivery_id='R1-next-429', wait=False, timeout=15)
        deadline = time.monotonic() + 6
        while (store.record(B)['lifecycle']['state'] != 'crashed'
               and time.monotonic() < deadline):
            time.sleep(.04)
        second_failure = store.record(B)
        assert second_failure['lastTurn']['status'] == 'failed'
        assert second_failure['lifecycle']['state'] == 'crashed'
        assert second_failure['lifecycle']['lastError'].startswith(
            'Codex thread system error: exceeded retry limit')
        assert _trace_count(trace, 'RPC thread/start') == 1
        assert _trace_count(trace, 'RPC thread/resume') == 1
        assert _trace_count(trace, 'RPC turn/start') == 3


@pytest.mark.parametrize('mode, message', [
    ('wrong-id', 'Codex resumed the wrong thread'),
    ('wrong-source', 'Codex thread belongs to another worker'),
    ('active', "not ready: status 'active'"),
    ('notLoaded', "not ready: status 'notLoaded'"),
    ('malformed', 'missing thread status'),
    ('error', 'thread/resume failed: resume refused by native server'),
])
def test_answered_unusable_resume_crashes_and_explicit_retry_succeeds(
    short_root, tmp_path, monkeypatch, mode, message,
):
    reply_mode = tmp_path / 'R1-answer-mode'
    reply_mode.write_text(mode)
    _subscription_fenced_provider(monkeypatch, resume_mode=reply_mode)
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, store, daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        client.create(cwd=str(tmp_path), worker_id=B)
        first = client.send(B, 'history for bad resume reply',
                            delivery_id='R1-bad-first', wait=True, timeout=15)
        assert WorkerClient().kill('codex:' + B)['killed']
        with pytest.raises(SessionUnavailable, match=message):
            client.resume(B, timeout=12)
        refused = store.record(B)
        assert refused['lifecycle']['state'] == 'crashed'
        assert message in refused['lifecycle']['lastError']
        assert refused['native']['threadId'] == first.thread_id
        assert _trace_count(trace, 'RPC thread/resume') == 1

        # Avoid racing a janitor that has already dropped the thread binding
        # but not yet removed its crashed Worker object from the registry.
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            status = daemon_rpc({'op': 'daemon-status'}, timeout=1)
            if B not in status['workers']:
                break
            time.sleep(.04)
        assert B not in status['workers']
        reply_mode.write_text('idle')
        retry = client.resume(B, timeout=12)
        assert retry.resumed and retry.state == 'idle'
        assert store.record(B)['lifecycle']['lastError'] is None
        assert _trace_count(trace, 'RPC thread/resume') == 2


def test_lost_resume_reply_stays_resuming_until_a_retry_succeeds(
    short_root, tmp_path, monkeypatch,
):
    reply_mode = tmp_path / 'R1-lost-mode'
    reply_mode.write_text('lost')
    _subscription_fenced_provider(monkeypatch, resume_mode=reply_mode)
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        client.create(cwd=str(tmp_path), worker_id=B)
        client.send(B, 'history for lost native reply',
                    delivery_id='R1-lost-first', wait=True, timeout=15)
        assert WorkerClient().kill('codex:' + B)['killed']
        with pytest.raises(SessionUnavailable):
            client.resume(B, timeout=2)
        _until(trace, 'RPC thread/resume')
        assert store.record(B)['lifecycle']['state'] == 'resuming'
        # Codex finished loading meanwhile: the next resume simply retries.
        reply_mode.write_text('idle')
        retry = client.resume(B, timeout=12)
        assert retry.resumed and retry.state == 'idle'
        assert _trace_count(trace, 'RPC thread/resume') == 2
        resumes = [line for line in trace.read_text().split('\\n')
                   if 'RPC thread/resume' in line]
        assert resumes and all("'excludeTurns': True" in line for line in resumes), resumes


def test_promptless_current_epoch_no_native_resume_and_zero_rollout_after_park_refuses(
    short_root, tmp_path, monkeypatch,
):
    _subscription_fenced_provider(monkeypatch)
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        created = client.create(cwd=str(tmp_path), worker_id=B)
        assert created.worker_id == B
        original = store.record(B)['native']['threadId']
        first = client.resume(B)
        assert first.resumed is False and first.state == 'idle'
        assert first.thread_id == original
        assert _trace_count(trace, 'RPC thread/resume') == 0
        assert _trace_count(trace, 'RPC turn/start') == 0
        parked = WorkerClient().kill('codex:' + B)
        assert parked['killed'] and parked['resume_available'] is False
        assert parked['resumable'] is False
        with pytest.raises(SessionUnavailable, match='no persisted rollout'):
            client.resume(B)
        assert store.record(B)['native']['threadId'] == original
        assert _trace_count(trace, 'RPC thread/start') == 1
        assert _trace_count(trace, 'RPC thread/resume') == 1
        assert _trace_count(trace, 'RPC turn/start') == 0


def test_wrong_native_source_after_kill_never_resumes_or_remints(
    short_root, tmp_path, monkeypatch,
):
    read_mode = tmp_path / 'R1-source-mode'
    read_mode.write_text('normal')
    _subscription_fenced_provider(monkeypatch)
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch,
                                        read_mode=read_mode) as (
        cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        assert client.create(cwd=str(tmp_path), worker_id=B).worker_id == B
        original = store.record(B)['native']['threadId']
        assert client.send(B, 'one real historical message',
                           delivery_id='source-first', wait=True, timeout=15).turn_id
        assert WorkerClient().kill('codex:' + B)['killed']
        read_mode.write_text('foreign')
        with pytest.raises(SessionUnavailable, match='source absent or foreign'):
            client.resume(B)
        sent = cli('send', 'codex:' + B, 'must not reach a foreign thread',
                   '--delivery-id', 'source-foreign-cli', '--no-wait')
        assert sent.returncode == SessionUnavailable.exit_code == 3, (sent.stdout, sent.stderr)
        assert store.record(B)['native']['threadId'] == original
        assert _trace_count(trace, 'RPC thread/start') == 1
        assert _trace_count(trace, 'RPC thread/resume') == 0


def test_native_resume_in_flight_kill_never_falsely_claims_ready(
    short_root, tmp_path, monkeypatch,
):
    from concurrent.futures import ThreadPoolExecutor
    hold = tmp_path / 'R1-release-resume'
    _subscription_fenced_provider(monkeypatch, hold_resume=hold)
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        assert client.create(cwd=str(tmp_path), worker_id=B).worker_id == B
        assert client.send(B, 'historic turn before R1 race',
                           delivery_id='R1-race-first', wait=True, timeout=15).turn_id
        assert WorkerClient().kill('codex:' + B)['killed']
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(lambda: CodexClient().resume(B))
            _until(trace, 'RPC thread/resume')
            try:
                killed = WorkerClient().kill('codex:' + B, timeout=2)
                assert killed['killed'] is True
                assert store.record(B)['lifecycle']['state'] == 'dead'
            finally:
                hold.touch()
            with pytest.raises(SessionUnavailable):
                pending.result(timeout=10)
        assert store.record(B)['lifecycle']['state'] == 'dead'
        assert _trace_count(trace, 'RPC thread/resume') == 1
        assert _trace_count(trace, 'RPC thread/start') == 1
        # The late response's native subscription is released without a
        # second provider thread; late Worker load cannot publish IDLE.
        assert _trace_count(trace, 'RPC thread/unsubscribe') >= 2


def test_historical_unbound_owner_is_never_bound_or_started_by_resume(
    tmp_path, monkeypatch,
):
    from test_generation_store import add_worker, fixture_root
    root = fixture_root(tmp_path / 'forensic-only')
    add_worker(root, B, 'codex', native_state='unbound-unknown', terminal=True)
    monkeypatch.setattr(constants, 'CCX_ROOT', root)
    from ccx.store.root import require_ccx_active
    store = require_ccx_active(root)
    before = store.record(B)
    with pytest.raises(SessionUnavailable, match='cannot resume'):
        CodexClient().resume(B)
    with pytest.raises(SessionUnavailable, match='cannot resume'):
        WorkerClient().resume('codex:' + B)
    assert store.record(B) == before
    assert not (root / 'run').exists()


def test_fresh_send_to_live_busy_original_is_typed_busy_refusal_without_effect(
    short_root, tmp_path, monkeypatch,
):
    from ccx.codex import delivery as codex_delivery
    _subscription_fenced_provider(monkeypatch, hold_turns=True)
    with native_fake._f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, _daemon_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path / 'fake-codex'))
        client = CodexClient()
        assert client.create(cwd=str(tmp_path), worker_id=B).worker_id == B
        sibling = client.create('sibling idle turn', cwd=str(tmp_path),
                                worker_id=SECOND, wait=True, timeout=15)
        assert sibling.worker_id == SECOND and B[:8] == SECOND[:8]
        client.send('codex:' + B, 'busy-hold-original', delivery_id='R1-busy-hold', wait=False)
        end = time.monotonic() + 5
        while store.record(B)['lifecycle'].get('state') != 'busy' and time.monotonic() < end:
            time.sleep(.04)
        assert store.record(B)['lifecycle']['state'] == 'busy'
        turns = _trace_count(trace, 'RPC turn/start')
        with pytest.raises(DeliveryRejected) as refused:
            client.send('codex:' + B, 'fresh while busy', delivery_id='R1-busy-fresh')
        error = refused.value
        assert error.exit_code == 5 and error.state == 'busy'
        assert error.refusal == 'worker-state' and error.reason == 'worker is busy'
        assert error.delivery_id == 'R1-busy-fresh'
        command = cli('send', 'codex:' + B, 'fresh cli while busy',
                      '--delivery-id', 'R1-busy-cli')
        assert command.returncode == 5, (command.stdout, command.stderr)
        assert 'worker is busy' in command.stderr
        handle = 'codex:' + B
        for token in ('R1-busy-fresh', 'R1-busy-cli'):
            assert codex_delivery.read_receipt(handle, token) is None
            assert store.read_receipt(handle, 'codex-send', token) is None
        assert _trace_count(trace, 'RPC turn/start') == turns
        assert _trace_count(trace, 'RPC thread/resume') == 0
        assert 'RPC thread/unsubscribe' not in trace.read_text()
        assert store.record(B)['lifecycle']['state'] == 'busy'
        assert store.record(SECOND)['lifecycle']['state'] == 'idle'
