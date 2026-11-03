"""Real codexd singleton socket and disposable fake app-server process."""
from __future__ import annotations
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest
from test_generation_store import short_root, A, B
from ccx import constants
from ccx.store.root import require_ccx_active
from ccx.store.worker_state import normalize_native_state


_WS_FAKE_BRIDGE = r"""
# Socket transport for the same disposable JSONL fake. Cold reads stay stdio.
if '--listen' in sys.argv:
    import base64, hashlib, socket, struct
    endpoint = sys.argv[sys.argv.index('--listen') + 1]
    assert endpoint.startswith('unix://')
    endpoint = endpoint[len('unix://'):]
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(endpoint)
    server.listen(1)
    channel, _ = server.accept()
    header = b''
    while b'\r\n\r\n' not in header:
        chunk = channel.recv(4096)
        if not chunk:
            raise EOFError('fake WebSocket upgrade missing')
        header += chunk
    key = header.split(b'Sec-WebSocket-Key: ')[1].split(b'\r\n')[0].decode()
    value = base64.b64encode(hashlib.sha1(
        (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
    channel.sendall(('HTTP/1.1 101 Switching Protocols\r\n'
        'Upgrade: websocket\r\nConnection: Upgrade\r\n'
        'Sec-WebSocket-Accept: '+value+'\r\n\r\n').encode())
    def exact(size):
        out=b''
        while len(out)<size:
            chunk=channel.recv(size-len(out))
            if not chunk:
                raise EOFError('fake WebSocket disconnected')
            out+=chunk
        return out
    def next_frame():
        while True:
            b1,b2=exact(2)
            size=b2&127
            if size==126: size=struct.unpack('!H',exact(2))[0]
            elif size==127: size=struct.unpack('!Q',exact(8))[0]
            mask=exact(4) if b2&128 else None
            raw=exact(size)
            if mask: raw=bytes(byte^mask[i%4] for i,byte in enumerate(raw))
            opcode=b1&15
            if opcode==8: raise EOFError()
            if opcode==1: return raw.decode()
    def send_frame(text):
        raw=text.encode()
        n=len(raw)
        if n<126: header=bytes([0x81,n])
        elif n<65536: header=bytes([0x81,126])+struct.pack('!H',n)
        else: header=bytes([0x81,127])+struct.pack('!Q',n)
        channel.sendall(header+raw)
    class NativeInput:
        def __iter__(self): return self
        def __next__(self):
            try: return next_frame()+'\n'
            except EOFError: raise StopIteration
    class NativeOutput:
        def __init__(self): self.buffer=''
        def write(self,text):
            self.buffer+=text
            while '\n' in self.buffer:
                line,self.buffer=self.buffer.split('\n',1)
                send_frame(line)
            return len(text)
        def flush(self): pass
    sys.stdin=NativeInput()
    sys.stdout=NativeOutput()
"""


def _fake_codex_executable(
    path: Path, trace: Path, *, completion_gate: Path | None = None,
    start_delay: float = 0.0, no_start_reply: bool = False,
    persisted_threads: Path | None = None, read_mode: Path | None = None,
    read_delay_path: Path | None = None, read_padding_path: Path | None = None,
    omit_completion: bool = False, phase_messages: bool = False,
    start_error_kind: str = "none", start_error_code: int = -32603,
    start_error_message: str = "Fake Codex original thread/start refused",
    approval_mode: bool = False,
    initialize_gate: Path | None = None,
    read_turns_unsupported: bool = False,
) -> Path:
    """Physical fake JSON-RPC app-server process, never a native Codex binary."""
    binary = path / "fake-codex"
    script = r"""
import sys,json,os,time
if '--version' in sys.argv:
    print('codex 0.fake'); sys.exit(0)
TRACE=__TRACE__
GATE=__GATE__
START_DELAY=__START_DELAY__
NO_START_REPLY=__NO_START_REPLY__
PERSIST=__PERSIST__
READ_MODE=__READ_MODE__
READ_DELAY_PATH=__READ_DELAY_PATH__
READ_PADDING_PATH=__READ_PADDING_PATH__
OMIT_COMPLETION=__OMIT_COMPLETION__
PHASE_MESSAGES=__PHASE_MESSAGES__
START_ERROR_KIND=__START_ERROR_KIND__
START_ERROR_CODE=__START_ERROR_CODE__
START_ERROR_MESSAGE=__START_ERROR_MESSAGE__
APPROVAL_MODE=__APPROVAL_MODE__
INIT_GATE=__INIT_GATE__
READ_TURNS_UNSUPPORTED=__READ_TURNS_UNSUPPORTED__
HELD_APPROVAL_TURNS={}
threads=json.loads(open(PERSIST,encoding='utf8').read()) if PERSIST and os.path.exists(PERSIST) else {}
counter=len(threads)
with open(TRACE,'a',encoding='utf8') as f: f.write('EXEC '+repr(sys.argv)+'\\n')
for line in sys.stdin:
    msg={}
    try:
        msg=json.loads(line)
        if 'method' not in msg and 'id' in msg:
            with open(TRACE,'a',encoding='utf8') as f:
                f.write('APPROVAL_REPLY '+repr(msg)+'\\n')
            if APPROVAL_MODE and msg.get('id')=='reused-native-request':
                hold=HELD_APPROVAL_TURNS.pop('reused-native-request',None)
                if hold is not None:
                    tid,turn=hold
                    print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',
                        'params':{'threadId':tid,'turn':turn}}),flush=True)
            continue
        if 'id' not in msg:
            continue
        method=msg.get('method')
        params=msg.get('params') or {}
        with open(TRACE,'a',encoding='utf8') as f:
            f.write('RPC '+method+' '+repr(params)+'\\n')
        if method=='initialize':
            if INIT_GATE:
                until=time.monotonic()+16
                while not os.path.exists(INIT_GATE) and time.monotonic()<until:
                    time.sleep(.01)
                if not os.path.exists(INIT_GATE):
                    raise RuntimeError('fake initialize gate was not released')
            result={'userAgent':'fake-codex'}
        elif method=='model/list':
            result={'data':[{'model':model} for model in (
                'gpt-fake','model-src','source-model','child-model',
                'full-source-model','gpt-m1-wire','different-model','different',
                'gpt-config-new','gpt-config-cli','gpt-no-busy',
                'gpt-offline','gpt-one','gpt-forbidden-waiting',
                'gpt-5.6-luna','gpt-5.6-sol','o4-fake')], 'nextCursor':None}
        elif method=='thread/start':
            if START_ERROR_KIND!='none':
                if START_DELAY:
                    time.sleep(START_DELAY)
                if START_ERROR_KIND=='reset':
                    os._exit(0)  # Local transport failure, not provider refusal
                native_error={'message':START_ERROR_MESSAGE}
                if START_ERROR_KIND=='coded':
                    native_error['code']=START_ERROR_CODE
                print(json.dumps({'jsonrpc':'2.0', 'id':msg['id'],
                    'error':native_error}),flush=True)
                continue
            counter+=1
            tid='native-fake-thread-'+str(counter)
            threads[tid]={'id':tid,'threadSource':params.get('threadSource'),
                          'model':params.get('model') or 'gpt-fake',
                          'turns':[]}
            if PERSIST:
                with open(PERSIST,'w',encoding='utf8') as f:
                    json.dump(threads,f)
                    f.flush();os.fsync(f.fileno())
            if START_DELAY:
                time.sleep(START_DELAY)
            if NO_START_REPLY:
                # The native effect happened, but the JSON-RPC response never
                # reached CCX. Provider source discovery is NOT adoption proof.
                continue
            result={'thread':{'id':tid}, 'model':'gpt-fake'}
        elif method=='thread/read':
            tid=params.get('threadId')
            if tid not in threads: raise ValueError('unknown native thread')
            if READ_TURNS_UNSUPPORTED and params.get('includeTurns', True):
                raise ValueError('list_turns is not supported yet')
            if READ_DELAY_PATH and os.path.exists(READ_DELAY_PATH):
                time.sleep(float(open(READ_DELAY_PATH,encoding='utf8').read()))
            mode=open(READ_MODE,encoding='utf8').read().strip() if READ_MODE and os.path.exists(READ_MODE) else 'normal'
            if mode=='fail': raise ValueError('fake independent thread/read unavailable')
            observed=dict(threads[tid])
            if READ_PADDING_PATH and os.path.exists(READ_PADDING_PATH):
                observed['largeRollout']=('x' * int(open(READ_PADDING_PATH,encoding='utf8').read()))
            if mode=='null':observed['threadSource']=None
            if mode=='foreign':observed['threadSource']='ccx-worker:foreign'
            if mode=='wrong-id':observed['id']='provider-foreign-thread'
            if not params.get('includeTurns'):
                observed['turns'] = []
            result={'thread':observed}
        elif method=='thread/turns/list':
            tid=params.get('threadId')
            if tid not in threads: raise ValueError('unknown native thread')
            if READ_TURNS_UNSUPPORTED:
                raise ValueError('list_turns is not supported yet')
            native=list(threads[tid]['turns'])
            if params.get('sortDirection')=='desc': native.reverse()
            offset=int(params.get('cursor') or '0')
            limit=int(params.get('limit',100))
            view=params.get('itemsView') or 'summary'
            page=[{**turn, 'items':(turn.get('items',[]) if view=='full' else []),
                   'itemsView':view} for turn in native[offset:offset+limit]]
            result={'data':page,
                    'nextCursor':str(offset+limit) if offset+limit<len(native) else None}
        elif method=='turn/start':
            tid=params.get('threadId')
            if tid not in threads: raise ValueError('unknown turn thread')
            counter+=1
            turn_id='native-fake-turn-'+str(counter)
            inputs=params.get('input') or []
            text=inputs[0].get('text') if inputs else ''
            answer='Fake Codex completed: '+text
            status=('failed' if text.startswith('initial-wait-failed')
                    else 'interrupted' if text.startswith('initial-wait-interrupted')
                    else 'completed')
            items=[] if text.startswith('initial-wait-empty') else [
                {'type':'agentMessage','text':answer}]
            if PHASE_MESSAGES and items:
                items=[
                    {'type':'agentMessage','phase':'commentary','text':'NOT THE REPORT'},
                    {'type':'agentMessage','phase':'final_answer','text':answer},
                    {'type':'agentMessage','text':'NOT THE FINAL'},
                ]
            turn={'id':turn_id,'status':status,'items':items}
            if status!='completed':turn['error']={'message':'fake '+status+' original turn'}
            threads[tid]['turns'].append(turn)
            result={'turn':{'id':turn_id,'status':'inProgress'}}
        elif method=='thread/items/list':
            tid=params.get('threadId')
            if tid not in threads: raise ValueError('unknown native thread')
            native=[{'turnId':turn['id'],'item':item}
                    for turn in threads[tid]['turns']
                    if not params.get('turnId') or turn['id']==params['turnId']
                    for item in turn.get('items',[])]
            if params.get('sortDirection')=='desc': native.reverse()
            offset=int(params.get('cursor') or '0')
            limit=int(params.get('limit',100))
            result={'data':native[offset:offset+limit],
                    'nextCursor':str(offset+limit) if offset+limit<len(native) else None}
        elif method=='thread/list':
            result={'data':list(threads.values()),'nextCursor':None}
        elif method=='thread/unsubscribe':
            result={}
        else:
            result={}
        print(json.dumps({'jsonrpc':'2.0','id':msg['id'],'result':result}),flush=True)
        if method=='turn/start':
            print(json.dumps({'jsonrpc':'2.0','method':'turn/started',
                'params':{'threadId':tid,'turn':{'id':turn_id}}}),flush=True)
            if OMIT_COMPLETION:continue
            if APPROVAL_MODE and text.startswith('codex-approve-'):
                HELD_APPROVAL_TURNS['reused-native-request']=(tid,turn)
                print(json.dumps({'jsonrpc':'2.0','id':'reused-native-request',
                    'method':'item/commandExecution/requestApproval',
                    'params':{'threadId':tid,'turnId':turn_id,
                              'command':'echo pending native approval'}}),flush=True)
                continue
            if GATE and text.startswith(('f1-race-', 'f1-approval-')):
                until=time.monotonic()+12
                while not os.path.exists(GATE) and time.monotonic()<until:
                    time.sleep(.01)
                if not os.path.exists(GATE):
                    raise RuntimeError('F1 completion gate never released')
            if GATE and text.startswith('f1-approval-'):
                print(json.dumps({'jsonrpc':'2.0','id':'f1-approval-request',
                    'method':'item/commandExecution/requestApproval',
                    'params':{'threadId':tid,'turnId':turn_id,
                              'command':'echo must-be-rejected'}}),flush=True)
            print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',
                'params':{'threadId':tid,'turn':turn}}),flush=True)
            if GATE and text.startswith('f1-race-'):
                # Duplicate delivery has the SAME native turn id. No second
                # stats increment, resurrection, or native action is allowed.
                print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',
                    'params':{'threadId':tid,'turn':turn}}),flush=True)
    except Exception as e:
        print(json.dumps({'jsonrpc':'2.0','id':msg.get('id'),
            'error':{'code':-32000,'message':str(e)}}),flush=True)
"""
    script = script.replace("for line in sys.stdin:", _WS_FAKE_BRIDGE + "\nfor line in sys.stdin:", 1)
    binary.write_text(
        "#!" + sys.executable + "\n"
        + script.replace("__TRACE__", repr(str(trace))).replace(
            "__GATE__", repr(str(completion_gate)) if completion_gate else "None"
        ).replace("__START_DELAY__", repr(start_delay)).replace(
            "__NO_START_REPLY__", repr(no_start_reply)
        ).replace("__PERSIST__", repr(str(persisted_threads)) if persisted_threads else "None"
        ).replace("__READ_MODE__", repr(str(read_mode)) if read_mode else "None"
        ).replace("__READ_DELAY_PATH__", repr(str(read_delay_path)) if read_delay_path else "None"
        ).replace("__READ_PADDING_PATH__", repr(str(read_padding_path)) if read_padding_path else "None"
        ).replace("__OMIT_COMPLETION__", repr(omit_completion)
        ).replace("__PHASE_MESSAGES__", repr(phase_messages)
        ).replace("__START_ERROR_KIND__", repr(start_error_kind)
        ).replace("__START_ERROR_CODE__", repr(start_error_code)
        ).replace("__START_ERROR_MESSAGE__", repr(start_error_message)
        ).replace("__APPROVAL_MODE__", repr(approval_mode)).replace(
            "__INIT_GATE__", repr(str(initialize_gate)) if initialize_gate else "None"
        ).replace("__READ_TURNS_UNSUPPORTED__", repr(read_turns_unsupported)),
        encoding="utf-8",
    )
    binary.chmod(0o700)
    return binary


def test_actual_codexd_socket_singleton_and_real_fake_appserver(
    short_root, tmp_path, monkeypatch,
):
    from ccx.codex.runtime import daemon_rpc
    from ccx.store.liveness import proc_start_token
    root = short_root
    fake_trace = tmp_path / "app-server-trace"
    executable = _fake_codex_executable(tmp_path, fake_trace)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(root),
           "CCX_CODEX_BIN": str(executable),
           "PYTHONPATH": str(repo), "CCX_CODEXD_LINGER": "120"}
    for var in ("CCX_SELF_SHORT","CCX_SELF_WORKER_ID","CCX_SELF_ENGINE"):
        env.pop(var, None)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    stderr = tmp_path / "real-codexd.log"
    with stderr.open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, "-B", "-m", 'ccx.codex.daemon'],
            env=env, cwd=str(repo), stdout=log, stderr=log,
            start_new_session=True,
        )
        try:
            deadline = time.monotonic() + 10
            result = None
            while time.monotonic() < deadline:
                result = daemon_rpc({"op":"daemon-status"}, timeout=.5)
                if result.get("ok") and result.get("state")=="ready":
                    break
                if process.poll() is not None:
                    break
                time.sleep(.05)
            assert result is not None and result.get("ok") and result.get("state")=="ready", (
                "real codexd failed to boot: "+repr(result)+" log=" +
                stderr.read_text(errors="replace")[-3500:]
            )
            assert result["pid"] == process.pid
            assert proc_start_token(process.pid)
            store = require_ccx_active(root)
            gen = store.pointer.generation_root
            assert (root / "run" / "codexd.sock").is_socket()
            assert (gen/"runtime-metadata"/"pid"/"codexd.json").exists()
            assert not (root/"codex"/"codexd.sock").exists()
            assert not (root/"codex").exists()
            assert not (root/"sessions").exists()
            assert "RPC initialize" in fake_trace.read_text()
            # A second process serving the same generation must fail the
            # retained kernel-exclusive daemon role lock, never overwrite it.
            second = subprocess.run(
                [sys.executable,"-B","-m",'ccx.codex.daemon'],
                env=env,cwd=str(repo),capture_output=True,text=True,timeout=3,
            )
            assert second.returncode in (0,1)
            assert "runtime" in second.stderr or "lease" in second.stderr
            assert daemon_rpc({"op":"daemon-status"}, timeout=.5)["pid"] == process.pid
            stopped = daemon_rpc({"op":"daemon-stop"},timeout=1)
            assert stopped["ok"] is True
            process.wait(timeout=5)
            assert not (root/"run"/"codexd.sock").exists()
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try: process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=3)



def test_actual_codex_native_thread_and_turn_against_fake_appserver(
    short_root, tmp_path, monkeypatch,
):
    from ccx.ops.creation_steps import (
        prepare_worker, dispatch_prepared_codex_native, reconcile_submitted_native,
    )
    from ccx.codex.runtime import daemon_rpc
    from ccx.ops.send import ensure_ready, send_turn
    from ccx.ops.wait import reconcile_turn
    from ccx.ops.report import report_worker
    from ccx.codex import delivery as codex_delivery
    from ccx.constants import CODEXD_RPC_GENERATION
    root = short_root
    store = require_ccx_active(root)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    prior = prepare_worker(store, engine="claude",
                           cwd=str(tmp_path), worker_id=A)
    created = prepare_worker(store, engine="codex",
                             cwd=str(tmp_path), worker_id=B)
    assert A[:8] == B[:8]
    fake_trace = tmp_path / "fake-native-rpcs"
    executable = _fake_codex_executable(tmp_path, fake_trace)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ,"CCX_ROOT":str(root),
           "CCX_CODEX_BIN":str(executable),"PYTHONPATH":str(repo)}
    log_file=tmp_path/"native-daemon.log"
    with log_file.open("wb") as output:
        process=subprocess.Popen(
            [sys.executable,"-B","-m",'ccx.codex.daemon'],
            env=env,cwd=str(repo),stdout=output,stderr=output,
            start_new_session=True,
        )
        try:
            until=time.monotonic()+8
            while time.monotonic()<until:
                status=daemon_rpc({"op":"daemon-status"},timeout=.5)
                if status.get("state")=="ready":break
                if process.poll() is not None:break
                time.sleep(.04)
            assert status.get("state")=="ready", (
                repr(status)+log_file.read_text(errors="replace")[-2500:]
            )
            launches=[]
            def start_native(budget):
                claim=store.native_activation(B)
                assert claim["state"]=="submitted-unknown"
                launches.append(claim["submissionId"])
                return daemon_rpc({
                    "op":"native-activate","workerId":B,
                    "submissionId":claim["submissionId"],
                    "rpcGeneration":CODEXD_RPC_GENERATION,
                },timeout=budget)
            queued=dispatch_prepared_codex_native(
                store,created,bounded_native_submit=start_native,
                dispatch_budget_s=2.0,
            )
            assert queued.activation_state=="submitted-unknown"
            until=time.monotonic()+5
            evidence=None
            while time.monotonic()<until:
                evidence=daemon_rpc({
                    "op":"native-activation-status","workerId":B,
                    "submissionId":store.native_activation(B)["submissionId"],
                    "rpcGeneration":CODEXD_RPC_GENERATION,
                },timeout=3)
                if evidence.get("accepted") is True:break
                assert evidence.get("ok") is True,evidence
                time.sleep(.04)
            assert evidence is not None and evidence.get("accepted") is True,evidence
            original=reconcile_submitted_native(
                store,created,original_receipt=evidence,
            )
            assert original.activation_state=="confirmed"
            tid=original.record["native"]["threadId"]
            assert tid.startswith("native-fake-thread-")
            assert original.record["native"]["threadSource"]==f"ccx-worker:{B}"
            assert len(launches)==1
            assert store.authorize_native_action(B,"send").worker_id==B

            token="actual-codex-original-delivery"
            dispatched=[]
            def send_native(handle,thread,text,delivery,budget):
                receipt=codex_delivery.read_receipt(handle,delivery)
                dispatched.append((thread,delivery))
                return daemon_rpc({
                    "op":"native-send","workerId":B,
                    "threadId":thread,"text":text,"deliveryId":delivery,
                    "clientUserMessageId":receipt["clientUserMessageId"],
                    "rpcGeneration":CODEXD_RPC_GENERATION,
                },timeout=budget)
            def observe_native(handle,turn_ref):
                receipt=codex_delivery.read_receipt(handle,token)
                tid_turn=receipt.get("turnId")
                assert turn_ref==f"ccx-turn:codex:v1:{tid_turn}"
                return daemon_rpc({
                    "op":"native-observe","workerId":B,
                    "turnId":tid_turn,"deliveryId":token,
                    "rpcGeneration":CODEXD_RPC_GENERATION,
                },timeout=2)
            # The injected transport bypasses send_turn's real readiness
            # preflight, so run that same original preflight explicitly:
            # native-send requires a loaded, subscribed, idle original Worker.
            ready=ensure_ready(B,expected_generation=store.generation)
            assert ready["loaded"] and ready["state"]=="idle"
            assert ready["thread_id"]==tid
            sent=send_turn(B,"hello actual Codex",delivery_id=token,
                           _native_submit=send_native,
                           _observe_settlement=observe_native)
            assert sent["status"]=="accepted" and sent["settled"]
            assert reconcile_turn(B,delivery_id=token)["status"]=="settled"
            actual_report=report_worker(B)
            assert actual_report["text"]=="Fake Codex completed: hello actual Codex"
            assert actual_report["delivery_id"]==token
            # Original daemon Worker registry must now route true app-server
            # turn/completed into fresh global UUID record lifecycle, not
            # merely settle a second independent semantic receipt.
            deadline=time.monotonic()+3
            current=store.record(B)
            while time.monotonic()<deadline and not current.get("lastTurn"):
                time.sleep(.03)
                current=store.record(B)
            assert (current.get("lastTurn") or {}).get("turnId")==(
                actual_report["turn_id"]
            ),("native event did not reach original Worker",current)
            assert current["stats"]["turns"]>=1
            # Original native registry must be keyed by exact GLOBAL UUID,
            # not the shared eight-hex display prefix or provider thread ID.
            daemon_status=daemon_rpc({"op":"daemon-status"},timeout=1)
            assert daemon_status["workers"].get(B) == "idle"
            assert A[:8] not in daemon_status["workers"]
            native_view=daemon_rpc({
                "op":"status","short":B,
                "rpcGeneration":CODEXD_RPC_GENERATION,
            },timeout=1)
            assert native_view["ok"] and native_view["loaded"]
            assert native_view["threadId"]==tid
            # Provider-native ids cannot be used to read arbitrary threads:
            # only the globally owned full worker UUID selects this source.
            scoped=daemon_rpc({
                "op":"thread-read","workerId":B,
                "threadId":tid,
                "rpcGeneration":CODEXD_RPC_GENERATION,
            },timeout=1)
            assert scoped["ok"] and scoped["workerId"]==B
            assert scoped["thread"]["threadSource"]==f"ccx-worker:{B}"
            previous_native_reads=fake_trace.read_text().count("RPC thread/read")
            raw_alias=daemon_rpc({
                "op":"thread-read","threadId":tid,
                "rpcGeneration":CODEXD_RPC_GENERATION,
            },timeout=1)
            assert raw_alias["ok"] is False
            wrong_owner=daemon_rpc({
                "op":"thread-read","workerId":A,"threadId":tid,
                "rpcGeneration":CODEXD_RPC_GENERATION,
            },timeout=1)
            assert wrong_owner["ok"] is False
            native_list=daemon_rpc({
                "op":"thread-list","params":{},
                "rpcGeneration":CODEXD_RPC_GENERATION,
            },timeout=1)
            assert native_list["ok"] is False
            assert fake_trace.read_text().count("RPC thread/read")==previous_native_reads
            assert "RPC thread/list" not in fake_trace.read_text()
            denied=daemon_rpc({
                "op":"status","short":B[:8],
                "rpcGeneration":CODEXD_RPC_GENERATION,
            },timeout=1)
            assert denied["ok"] is False
            # Explicit load ADOPTS the proven original thread, never
            # performs thread/start/resume for a v2 worker.
            adopted=daemon_rpc({
                "op":"load","short":B,
                "rpcGeneration":CODEXD_RPC_GENERATION,
            },timeout=1)
            assert adopted["ok"] and adopted["threadId"]==tid
            # Independent CLI/daemon config + analytics must survive the
            # original Worker receiving its next native completion.
            renamed=daemon_rpc({
                "op":"set-name","short":B,"name":"independent Codex rename",
                "rpcGeneration":CODEXD_RPC_GENERATION,
            },timeout=1)
            assert renamed["ok"] and renamed["name"]=="independent Codex rename"
            configured=daemon_rpc({
                "op":"set-config","short":B,
                "updates":{"model":"o4-fake","effort":"high"},
                "rpcGeneration":CODEXD_RPC_GENERATION,
            },timeout=1)
            assert configured["ok"] and configured["model"]=="o4-fake"
            store.mutate_record(
                B,patches={"stats.externalProof":37},
            )
            next_original="second-real-native-after-cas"
            def second_submit(handle,thread,text,delivery,budget):
                receipt=codex_delivery.read_receipt(handle,delivery)
                return daemon_rpc({
                    "op":"native-send","workerId":B,
                    "threadId":thread,"text":text,"deliveryId":delivery,
                    "clientUserMessageId":receipt["clientUserMessageId"],
                    "rpcGeneration":CODEXD_RPC_GENERATION,
                },timeout=budget)
            def second_observe(handle,turn_ref):
                receipt=codex_delivery.read_receipt(handle,next_original)
                return daemon_rpc({
                    "op":"native-observe","workerId":B,
                    "turnId":receipt["turnId"],
                    "deliveryId":next_original,
                    "rpcGeneration":CODEXD_RPC_GENERATION,
                },timeout=1)
            second=send_turn(
                B,"Codex independent CAS survives",
                delivery_id=next_original,
                _native_submit=second_submit,
                _observe_settlement=second_observe,
            )
            assert second["settled"]
            deadline=time.monotonic()+3
            latest=store.record(B)
            while time.monotonic()<deadline and latest["stats"]["turns"]<2:
                time.sleep(.03)
                latest=store.record(B)
            assert latest["stats"]["turns"]==2
            assert latest["name"]=="independent Codex rename"
            assert latest["createArgs"]["model"]=="o4-fake"
            assert latest["createArgs"]["effort"]=="high"
            assert latest["stats"]["externalProof"]==37
            assert latest["native"]["threadId"]==tid
            assert latest["native"]["threadSource"]==f"ccx-worker:{B}"
            assert A[:8]==B[:8] and store.record(A)["engine"]=="claude"
            def never_dispatch(*a):
                raise AssertionError("duplicate native turn attempted")
            again=send_turn(B,"hello actual Codex",delivery_id=token,
                            _native_submit=never_dispatch,
                            _observe_settlement=observe_native)
            assert again==sent and len(dispatched)==1
            trace=fake_trace.read_text()
            assert trace.count("RPC thread/start")==1
            assert trace.count("RPC turn/start")==2
            assert "RPC thread/read" in trace
            assert store.record(A)["native"]["sessionId"]==prior.record["native"]["sessionId"]
            assert not (root/"codex").exists()
        finally:
            if process.poll() is None:
                try:daemon_rpc({"op":"daemon-stop"},timeout=.5)
                except Exception:pass
                try:process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGTERM)
                    try:process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid,signal.SIGKILL)
                        process.wait(timeout=3)



def test_normal_codex_cli_new_send_report_retry_over_real_daemon(
    short_root, tmp_path, monkeypatch,
):
    """Real CLI launches codexd and fake app-server, performs native thread/turn.

    Cleanup uses only private-root daemon metadata and positive /proc PID/start
    proof; this never contacts a real Codex install/provider.
    """
    from ccx.codex.runtime import daemon_rpc
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.liveness import owned_pid_alive, proc_start_token
    root=short_root
    store=require_ccx_active(root)
    monkeypatch.setattr(constants,"CCX_ROOT",root)
    source=prepare_worker(store,engine="claude",
                          cwd=str(tmp_path),worker_id=A)
    trace=tmp_path/"real-native-codex-cli"
    fake=_fake_codex_executable(tmp_path,trace)
    repo=Path(__file__).resolve().parent.parent
    env={**os.environ,"CCX_ROOT":str(root),"CCX_CODEX_BIN":str(fake),
         "PYTHONPATH":str(repo),"CCX_CODEXD_LINGER":"120"}
    for variable in ("CCX_SELF_SHORT","CCX_SELF_WORKER_ID","CCX_SELF_ENGINE"):
        env.pop(variable,None)
    def cmd(*args):
        return subprocess.run(
            [sys.executable,"-B","-m","ccx",*args],cwd=str(repo),env=env,
            capture_output=True,text=True,timeout=15,
        )
    def status():
        return daemon_rpc({"op":"daemon-status"},timeout=1)
    # The daemon is created by a subprocess CLI, not this pytest process.
    # Capture its actual kernel PID/start identity before asserting that a
    # later acknowledged daemon-stop has finished its cleanup. A stop RPC
    # ACK alone only signals the stop event; the process may still be writing
    # into this test's temporary root as the fixture begins its own teardown.
    original_processes = []
    try:
        made=cmd("new", "--engine", "codex","--cwd",str(tmp_path),
                 "--worker-id",B,"--json")
        created=json.loads(made.stdout)
        assert made.returncode==0,(created.get("native_start_error"),created,made.stderr)
        assert created["worker"]==f"codex:{B}"
        assert created["native_ready"] is True
        assert store.record(B)["native"]["threadSource"]==f"ccx-worker:{B}"
        original_thread=store.record(B)["native"]["threadId"]
        first_daemon=status()
        assert first_daemon["state"]=="ready"
        daemon_start=proc_start_token(first_daemon["pid"])
        assert daemon_start and owned_pid_alive(first_daemon["pid"],daemon_start)
        original_processes.append((first_daemon["pid"], daemon_start))
        appserver_pid = first_daemon["appServerPid"]
        appserver_start = proc_start_token(appserver_pid)
        assert appserver_start and owned_pid_alive(appserver_pid, appserver_start)
        original_processes.append((appserver_pid, appserver_start))
        assert A[:8]==B[:8]
        assert store.record(A)["native"]["sessionId"]==(
            source.record["native"]["sessionId"]
        )
        replay=cmd("new", "--engine", "codex","--cwd",str(tmp_path),
                   "--worker-id",B,"--json")
        assert replay.returncode==0,(replay.stdout,replay.stderr)
        assert json.loads(replay.stdout)["worker_id"]==B
        assert store.record(B)["native"]["threadId"]==original_thread
        assert status()["pid"]==first_daemon["pid"]

        token="real-cli-native-codex-token"
        args=("send",B,"hello true Codex",
              "--no-wait","--delivery-id",token,"--json")
        sent=cmd(*args)
        assert sent.returncode==0,(sent.stdout,sent.stderr)
        outcome=json.loads(sent.stdout)
        assert outcome["worker_id"]==B
        assert outcome["delivery_id"]==token
        deadline=time.monotonic()+8
        report=None
        while time.monotonic()<deadline:
            observed=cmd("report",B,"--json")
            if observed.returncode==0:
                report=json.loads(observed.stdout)
                break
            time.sleep(.1)
        assert report is not None,(observed.stdout,observed.stderr)
        assert report["text"]=="Fake Codex completed: hello true Codex"
        assert report["delivery_id"]==token
        again=cmd(*args)
        assert again.returncode==0,(again.stdout,again.stderr)
        assert json.loads(again.stdout)["settled"] is True
        wrong=cmd("send",B,"DIFFERENT",
                  "--no-wait","--delivery-id",token,"--json")
        assert wrong.returncode!=0
        short=cmd("send",B[:8],"not owned",
                  "--no-wait","--json")
        assert short.returncode!=0
        native_calls=trace.read_text()
        assert native_calls.count("RPC thread/start")==1
        assert native_calls.count("RPC turn/start")==1
        assert status()["pid"]==first_daemon["pid"]

        # Same genuine daemon/app-server process: actual public Python clients
        # make additional native turns directly, with typed full UUID results.
        from ccx.codex.client import CodexClient
        from ccx.workers import WorkerClient
        typed_client=CodexClient()
        worker_client=WorkerClient()
        assert worker_client.capabilities(B)["engine"] == "codex"
        typed_token="direct-python-native-codex"
        typed=typed_client.send(
            B,"Codex typed Python turn",wait=False,delivery_id=typed_token,
        )
        assert typed.worker_id==B and typed.short==B[:8]
        assert typed.thread_id==original_thread
        assert typed.delivery_id==typed_token
        assert typed.as_dict()["worker"]==f"codex:{B}"
        state=typed_client.reconcile(B,delivery_id=typed_token)
        assert state.worker_id==B and state.delivery_id==typed_token
        assert state.status=="settled"
        native_report=typed_client.report(B)
        assert native_report.text==(
            "Fake Codex completed: Codex typed Python turn"
        )
        assert native_report.thread_id==original_thread
        repeated=typed_client.send(
            f"codex:{B}","Codex typed Python turn",
            wait=False,delivery_id=typed_token,
        )
        assert repeated.turn_id==typed.turn_id
        assert repeated.delivery_id==typed_token
        assert repeated.reply==(
            "Fake Codex completed: Codex typed Python turn"
        )
        facade_token="facade-real-native-codex"
        normalized=worker_client.send(
            B,"Codex WorkerClient native turn",
            wait=True,timeout=10,delivery_id=facade_token,
        )
        assert normalized["worker"]==f"codex:{B}"
        assert normalized["delivery_id"]==facade_token
        assert normalized["settled"] is True
        assert normalized["reply"]==(
            "Fake Codex completed: Codex WorkerClient native turn"
        )
        exact=worker_client.report(
            B,turn_ref=normalized["turn_ref"],
        )
        assert exact["worker_id"]==B
        assert exact["text"]==normalized["reply"]
        prior_turns=trace.read_text().count("RPC turn/start")
        assert prior_turns==3
        facade_retry=worker_client.send(
            B,"Codex WorkerClient native turn",
            wait=False,delivery_id=facade_token,
        )
        assert facade_retry["turn_id"]==normalized["turn_id"]
        assert facade_retry["settled"] is True
        assert facade_retry["reply"]==normalized["reply"]
        assert trace.read_text().count("RPC turn/start")==prior_turns
        assert status()["pid"]==first_daemon["pid"]

        # The ordinary one-shot V1 native operation dispatch uses the SAME
        # process/socket/original receipt, never a parallel app-server client.
        def v1(op,**params):
            original={"protocol_version":2,
                      "request_id":"real-codex-v1-original",
                      "op":op,"params":params}
            started=subprocess.run(
                [sys.executable,"-B","-m","ccx","rpc"],
                input=json.dumps(original),env=env,cwd=str(repo),
                capture_output=True,text=True,timeout=15,
            )
            assert started.returncode==0,(started.stdout,started.stderr)
            return json.loads(started.stdout)
        v1_token="v1-codex-real-native"
        launched=v1("turn.send",target=B,text="from V1 real Codex",
                    delivery_id=v1_token)
        assert launched["ok"] is True,launched
        assert launched["result"]["worker"]==f"codex:{B}"
        deadline=time.monotonic()+8
        confirmed=None
        while time.monotonic()<deadline:
            observed=v1("turn.reconcile",target=B,delivery_id=v1_token)
            assert observed["ok"] is True,observed
            if observed["result"]["delivery"]["status"]=="settled":
                confirmed=observed["result"]
                break
            time.sleep(.05)
        assert confirmed is not None
        final=v1("worker.report",target=B,turn_ref=confirmed["delivery"]["turn_ref"])
        assert final["ok"] is True,final
        assert final["result"]["text"]==(
            "Fake Codex completed: from V1 real Codex"
        )
        v1_again=v1("turn.send",target=B,text="from V1 real Codex",
                    delivery_id=v1_token)
        assert v1_again["ok"] is True
        assert isinstance(v1_again["result"]["delivery"]["settlement"], dict)
        assert trace.read_text().count("RPC turn/start")==prior_turns+1
        mismatch=v1("turn.send",target=B,text="different native prompt",
                    delivery_id=v1_token)
        assert mismatch["ok"] is False
        assert mismatch["error"]["code"]=="refused"
        assert trace.read_text().count("RPC turn/start")==prior_turns+1
        assert status()["pid"]==first_daemon["pid"]

        # Deliberately drop the acknowledged Codex native-turn reply AFTER
        # real app-server thread/turn bytes have crossed the singleton RPC.
        # A second request with that same id must not ever reach turn/start.
        from ccx.ops.send import send_turn as internal_send
        # Public send_turn types an uncertain native effect as SendIndeterminate
        # (exit 6) carrying the original token, never as a raw GenerationError.
        from ccx.errors import SendIndeterminate
        from ccx.codex.delivery import read_receipt as original_semantic
        from ccx.constants import CODEXD_RPC_GENERATION
        lost_token="lost-reply-original-codex"
        native_lost=[]
        def native_reply_lost(handle, native_thread, text, token, budget):
            original=original_semantic(handle,token)
            response=daemon_rpc({
                "op":"native-send", "workerId":B,
                "threadId":native_thread, "text":text,
                "deliveryId":token,
                "clientUserMessageId":original["clientUserMessageId"],
                "rpcGeneration":CODEXD_RPC_GENERATION,
            },timeout=budget)
            assert response.get("accepted") is True,response
            native_lost.append(response["turnId"])
            raise TimeoutError("caller lost positive native app-server reply")
        with pytest.raises(SendIndeterminate) as lost:
            internal_send(B,"lost reply native once",delivery_id=lost_token,
                          _native_submit=native_reply_lost)
        assert lost.value.delivery_id==lost_token
        assert len(native_lost)==1
        with pytest.raises(SendIndeterminate) as replay:
            internal_send(B,"lost reply native once",delivery_id=lost_token)
        assert replay.value.delivery_id==lost_token
        assert trace.read_text().count("RPC turn/start")==5
        assert store.read_receipt(B,"codex-send",lost_token)["state"]=="submitted-unknown"
        assert original_semantic(B,lost_token)["state"]=="submitted"
        assert not (root/"codex").exists()
        assert not (root/"sessions").exists()
    finally:
        # Read-only status check on THIS disposable private root and peer
        # authentication; daemon-stop refuses any foreign/unowned socket.
        # It acknowledges the stop *request*, not process completion. Wait for
        # the exact original daemon AND its exact original fake app-server to
        # leave before socket_tmp_path deletes their sealed private root.
        if original_processes:
            # This is a disposable fake daemon: the test deliberately left a
            # *submitted-unknown* native turn to prove no duplicate replay.
            # Normal daemon-stop may legitimately refuse to discard that
            # evidence. At test teardown ONLY, use the explicit force policy
            # after proving the scoped daemon's original kernel PID/start.
            # Never address a daemon discovered by process name or another
            # root, nor force-stop while any test assertion still needs it.
            try:
                observed=status()
            except Exception:
                observed=None  # Already stopped/disconnected; prove exit below.
            if isinstance(observed, dict) and observed.get("transport")=="served":
                assert observed["pid"]==original_processes[0][0]
                assert proc_start_token(observed["pid"])==original_processes[0][1]
                stopped=daemon_rpc({"op":"daemon-stop", "force":True},timeout=3)
                assert stopped.get("ok") is True and stopped.get("stopped") is True, stopped
        for pid, start in original_processes:
            deadline = time.monotonic() + 8
            while owned_pid_alive(pid, start) and time.monotonic() < deadline:
                time.sleep(.02)
            assert not owned_pid_alive(pid, start), (
                "original disposable Codex daemon/app-server still owns the test root", pid,
            )


def test_f1_real_appserver_terminal_kill_race_isolated_from_sibling(
    short_root, tmp_path, monkeypatch,
):
    """A native completion racing the original UUID terminal CAS cannot kill codexd.

    A separate Python writer terminalizes the original busy turn before
    native completion, held by the fake app-server gate. This test does
    not dispatch a CLI kill.
    """
    from ccx.codex.runtime import daemon_rpc
    root = short_root
    store = require_ccx_active(root)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    target = B
    sibling = "aaaaaaaa-9876-4000-8000-000000000099"
    assert target[:8] == sibling[:8]
    gate = tmp_path / "release-native-completion"
    trace = tmp_path / "f1-fake-native-trace"
    fake = _fake_codex_executable(tmp_path, trace, completion_gate=gate)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(root), "CCX_CODEX_BIN": str(fake),
           "PYTHONPATH": str(repo), "CCX_CODEXD_LINGER": "120"}
    for variable in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(variable, None)

    def cli(*args):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", *args],
            cwd=str(repo), env=env, capture_output=True, text=True, timeout=15,
        )

    daemon_log = tmp_path / "f1-real-daemon.log"
    with daemon_log.open("wb") as output:
        process = subprocess.Popen(
            [sys.executable, "-B", "-m", 'ccx.codex.daemon'], env=env,
            cwd=str(repo), stdout=output, stderr=output,
            start_new_session=True,
        )
        try:
            until = time.monotonic() + 9
            while time.monotonic() < until:
                status = daemon_rpc({"op": "daemon-status"}, timeout=.5)
                if status.get("state") == "ready":
                    break
                if process.poll() is not None:
                    break
                time.sleep(.03)
            assert status.get("state") == "ready", daemon_log.read_text(errors="replace")
            original_daemon = status["pid"]
            for wid in (target, sibling):
                result = cli("new", "--engine", "codex", "--cwd", str(tmp_path),
                             "--worker-id", wid, "--json")
                assert result.returncode == 0, (result.stdout, result.stderr)
                assert json.loads(result.stdout)["worker_id"] == wid
            assert daemon_rpc({"op": "daemon-status"})["pid"] == original_daemon

            token = "f1-race-delivery"
            args = ("send", target, "f1-race-kill-completion",
                    "--no-wait", "--delivery-id", token, "--json")
            first = cli(*args)
            assert first.returncode == 0, (first.stdout, first.stderr)
            from ccx.codex.delivery import read_receipt
            native_receipt = read_receipt(f"codex:{target}", token)
            turn = native_receipt["turnId"]
            until = time.monotonic() + 6
            while time.monotonic() < until:
                row = store.record(target)
                if (row["lifecycle"].get("activeTurnId") == turn
                        and row["lifecycle"]["state"] == "busy"):
                    break
                time.sleep(.02)
            assert row["lifecycle"]["activeTurnId"] == turn, row
            assert not gate.exists(), "native completion gate opened too early"

            # Separate process commits only the original busy/active-turn CAS.
            terminated = subprocess.run(
                [sys.executable, "-B", "-c",
                 "import sys; from ccx.constants import CCX_ROOT; from ccx.store.root import require_ccx_active; from ccx.codex.records import now_ms; store = require_ccx_active(CCX_ROOT); updated = store.mutate_record(sys.argv[1], patches={'lifecycle.state': 'dead', 'lifecycle.activeTurnId': None, 'lifecycle.since': now_ms()}, expect={'lifecycle.state': 'busy', 'lifecycle.activeTurnId': sys.argv[2]}); print(updated['lifecycle']['state'])", target, turn],
                env=env, cwd=str(repo), capture_output=True, text=True, timeout=5,
            )
            assert terminated.returncode == 0, (terminated.stdout, terminated.stderr)
            assert terminated.stdout.strip() == "dead"
            store.rename_worker(target, engine="codex", name="outside CLI rename")
            store.mutate_record(target, patches={
                "createArgs.model": "independent-model",
                "createArgs.effort": "high",
            })
            assert store.record(target)["lifecycle"]["activeTurnId"] is None

            # Release real native notifications. The daemon's cached baseline
            # was busy, but a different process has written dead+no active id.
            gate.write_text("release", encoding="utf-8")
            until = time.monotonic() + 7
            row = store.record(target)
            while time.monotonic() < until:
                row = store.record(target)
                if (row.get("lastTurn") or {}).get("turnId") == turn:
                    break
                time.sleep(.03)
            assert (row.get("lastTurn") or {}).get("turnId") == turn, (
                row, daemon_log.read_text(errors="replace")[-3000:]
            )
            assert row["lifecycle"]["state"] == "dead"
            assert row["lifecycle"]["activeTurnId"] is None
            assert row["pendingApprovals"] == []
            assert row["stats"]["turns"] == 1
            assert row["name"] == "outside CLI rename"
            assert row["createArgs"]["model"] == "independent-model"
            assert row["createArgs"]["effort"] == "high"
            assert daemon_rpc({"op": "daemon-status"})["pid"] == original_daemon
            observed = daemon_rpc({
                "op": "status", "short": target,
                "rpcGeneration": constants.CODEXD_RPC_GENERATION,
            })
            # The separate terminal CAS is durable before the daemon's
            # completion arrives. The janitor may unload that already-dead
            # worker before this status RPC executes (particularly under
            # heavy CPU load). Both a still-loaded `dead` response and the
            # daemon's exact `not loaded` response are valid; in either case
            # the original durable record above must remain dead/unchanged,
            # and the sibling must remain served by the same daemon.
            if observed["ok"]:
                assert observed["state"] == "dead", observed
                assert observed["persistenceError"] is None, observed
            else:
                assert observed.get("loaded") is False, observed
                assert observed.get("error") == f"worker {target} is not loaded", observed
                after_unload = daemon_rpc({"op": "daemon-status"})
                assert after_unload["pid"] == original_daemon
                assert target not in after_unload["workers"], after_unload
                # The sibling was created WITHOUT an initial prompt. The
                # original source-owned native thread is durable, but it need
                # not have a daemon Worker until its FIRST semantic send
                # (codexd.py:5354–5355). A live daemon registry is only a
                # loaded-worker cache; missing membership is not sibling loss.
                sibling_rec = store.record(sibling)
                assert sibling_rec["native"]["threadId"]
                assert sibling_rec["native"]["threadSource"] == f"ccx-worker:{sibling}"
                assert sibling_rec["lifecycle"]["state"] in ("idle", "idle-killed")
                assert store.record(target)["lifecycle"]["state"] == "dead"

            # The promptless sibling owns an independent original native
            # thread. Its FIRST semantic send must load/adopt that same UUID,
            # complete the turn and leave the daemon/sibling healthy; this is
            # the real cross-worker isolation proof, never cache membership.
            sibling_token = "f1-sibling-still-works"
            sibling_send = cli(
                "send", sibling, "sibling survived F1 conflict",
                "--no-wait", "--delivery-id", sibling_token, "--json",
            )
            assert sibling_send.returncode == 0, (
                sibling_send.stdout, sibling_send.stderr,
                daemon_log.read_text(errors="replace")[-3000:],
            )
            until = time.monotonic() + 7
            answer = None
            while time.monotonic() < until:
                result = cli("report", sibling, "--json")
                if result.returncode == 0:
                    answer = json.loads(result.stdout)
                    break
                time.sleep(.04)
            assert answer and answer["text"] == (
                "Fake Codex completed: sibling survived F1 conflict"
            )
            # Repeated caller intent and duplicate native completion do not
            # start a second turn or increment the killed worker's stats again.
            again = cli(*args)
            assert again.returncode in (0, 3, 5, 6), (again.stdout, again.stderr)
            assert store.record(target)["stats"]["turns"] == 1
            native_calls = trace.read_text()
            assert native_calls.count("RPC thread/start") == 2
            assert native_calls.count("RPC turn/start") == 2
            assert native_calls.count("RPC thread/resume") == 0
            assert daemon_rpc({"op": "daemon-status"})["pid"] == original_daemon
        finally:
            gate.touch(exist_ok=True)
            if process.poll() is None:
                try:
                    daemon_rpc({"op": "daemon-stop"}, timeout=1)
                except Exception:
                    pass
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=3)


def test_f1_real_appserver_approval_after_terminal_refused_without_sibling_loss(
    short_root, tmp_path, monkeypatch,
):
    """A native server request cannot acquire a phantom approval after kill."""
    from ccx.codex.runtime import daemon_rpc
    root = short_root
    store = require_ccx_active(root)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    target = B
    sibling = "aaaaaaaa-9876-4000-8000-000000000099"
    gate = tmp_path / "release-native-approval"
    trace = tmp_path / "f1-approval-trace"
    fake = _fake_codex_executable(tmp_path, trace, completion_gate=gate)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(root), "CCX_CODEX_BIN": str(fake),
           "PYTHONPATH": str(repo), "CCX_CODEXD_LINGER": "120"}
    for variable in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(variable, None)

    def cli(*args):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", *args],
            cwd=str(repo), env=env, capture_output=True, text=True, timeout=15,
        )

    log = tmp_path / "f1-approval-daemon.log"
    with log.open("wb") as output:
        process = subprocess.Popen(
            [sys.executable, "-B", "-m", 'ccx.codex.daemon'], cwd=str(repo),
            env=env, stdout=output, stderr=output, start_new_session=True,
        )
        try:
            until = time.monotonic() + 8
            while time.monotonic() < until:
                status = daemon_rpc({"op": "daemon-status"}, timeout=.5)
                if status.get("state") == "ready":
                    break
                if process.poll() is not None:
                    break
                time.sleep(.03)
            assert status.get("state") == "ready", log.read_text(errors="replace")
            original_pid = status["pid"]
            for wid in (target, sibling):
                created = cli("new", "--engine", "codex", "--cwd", str(tmp_path),
                              "--worker-id", wid, "--json")
                assert created.returncode == 0, (created.stdout, created.stderr)
            sent = cli("send", target, "f1-approval-race",
                       "--no-wait", "--delivery-id", "f1-approval-race-id", "--json")
            assert sent.returncode == 0, (sent.stdout, sent.stderr)
            until = time.monotonic() + 5
            while time.monotonic() < until:
                row = store.record(target)
                if row["lifecycle"].get("activeTurnId"):
                    break
                time.sleep(.02)
            assert row["lifecycle"]["activeTurnId"], row
            assert not gate.exists()
            # Separate original UUID terminal CAS races the app-server's
            # permission request; it must never acquire a post-terminal gate.
            writer = subprocess.run(
                [sys.executable, "-B", "-c",
                 "import sys; from ccx.constants import CCX_ROOT; from ccx.store.root import require_ccx_active; from ccx.codex.records import now_ms; store = require_ccx_active(CCX_ROOT); updated = store.mutate_record(sys.argv[1], patches={'lifecycle.state': 'dead', 'lifecycle.activeTurnId': None, 'lifecycle.since': now_ms()}, expect={'lifecycle.state': 'busy', 'lifecycle.activeTurnId': sys.argv[2]}); assert updated['lifecycle']['state'] == 'dead'",
                 target, row["lifecycle"]["activeTurnId"]],
                env=env, cwd=str(repo), capture_output=True, text=True, timeout=5,
            )
            assert writer.returncode == 0, (writer.stdout, writer.stderr)
            gate.write_text("release", encoding="utf-8")
            until = time.monotonic() + 6
            observed = ""
            while time.monotonic() < until:
                observed = trace.read_text()
                if "APPROVAL_REPLY" in observed:
                    break
                time.sleep(.03)
            assert "APPROVAL_REPLY" in observed, log.read_text(errors="replace")[-3000:]
            assert "could not durably admit this approval" in observed
            assert "'error':" in observed
            row = store.record(target)
            assert row["lifecycle"]["state"] == "dead"
            assert row["lifecycle"]["activeTurnId"] is None
            assert row["pendingApprovals"] == []
            status = daemon_rpc({
                "op": "status", "short": target,
                "rpcGeneration": constants.CODEXD_RPC_GENERATION,
            })
            assert status["ok"] and status["pendingApprovals"] == [], status
            assert status["persistenceError"]["kind"] == "RecordConflict"
            assert daemon_rpc({"op": "daemon-status"})["pid"] == original_pid
            sibling_result = cli(
                "send", sibling, "approval failure isolated",
                "--no-wait", "--delivery-id", "f1-approval-sibling", "--json",
            )
            assert sibling_result.returncode == 0, (
                sibling_result.stdout, sibling_result.stderr,
                log.read_text(errors="replace")[-2000:],
            )
            assert trace.read_text().count("RPC thread/start") == 2
            assert trace.read_text().count("RPC turn/start") == 2
        finally:
            gate.touch(exist_ok=True)
            if process.poll() is None:
                try:
                    daemon_rpc({"op": "daemon-stop"}, timeout=1)
                except Exception:
                    pass
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=3)


@pytest.mark.parametrize("fault", ["integrity", "busy", "stale"])
def test_f1_store_errors_refuse_unsafe_retry_and_generation_flip(
    short_root, tmp_path, monkeypatch, fault,
):
    """A bad generation/owner is not a normal same-field CAS conflict."""
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.root import GenerationBusy, StaleGeneration, StorageIntegrityError
    store = require_ccx_active(short_root)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    prepared = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, prepared.record)
    daemon.workers[B] = worker
    daemon.by_thread["native-test-thread"] = B
    tries = []
    original_revalidate = store.revalidate

    def refuse_mutation(*args, **kwargs):
        tries.append("mutation")
        if fault == "integrity":
            raise StorageIntegrityError("record owner inode not trusted")
        raise GenerationBusy("worker record lease unavailable")

    if fault == "stale":
        def refuse_generation():
            tries.append("admission")
            raise StaleGeneration("generation changed while worker was loaded")
        monkeypatch.setattr(store, "revalidate", refuse_generation)
    else:
        monkeypatch.setattr(store, "mutate_record", refuse_mutation)
    daemon._on_notification(
        "turn/started", {"threadId": "native-test-thread", "turn": {"id": "native-one"}},
    )
    assert tries == (["admission"] if fault == "stale" else ["mutation"])
    if fault == "stale":
        monkeypatch.setattr(store, "revalidate", original_revalidate)
    assert worker.persistence_error["kind"] == {
        "integrity": "StorageIntegrityError", "busy": "GenerationBusy",
        "stale": "StaleGeneration",
    }[fault]
    assert store.record(B)["lifecycle"]["state"] == "prepared"
    assert store.record(B)["stats"]["turns"] == 0
    assert daemon.stop.is_set() is (fault == "stale")
    assert daemon.stopping is (fault == "stale")


def test_f1_stale_generation_rejects_native_approval_without_record_write(
    short_root, tmp_path, monkeypatch,
):
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.root import StaleGeneration
    store = require_ccx_active(short_root)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    prepared = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, prepared.record)
    daemon.workers[B] = worker
    daemon.by_thread["native-test-thread"] = B
    refusals = []

    class Client:
        def reject_request(self, request_id, reason):
            refusals.append((request_id, reason))

    daemon.client = Client()
    original_revalidate = store.revalidate
    def refuse_generation():
        raise StaleGeneration("the original root is no longer ACTIVE")
    monkeypatch.setattr(store, "revalidate", refuse_generation)
    daemon._on_server_request(
        42, "item/commandExecution/requestApproval",
        {"threadId": "native-test-thread", "turnId": "native-one", "command": "rm -rf ?"},
    )
    assert len(refusals) == 1 and refusals[0][0] == 42
    assert "could not durably admit" in refusals[0][1]
    assert worker.public_status()["pendingApprovals"] == []
    assert worker.persistence_error["kind"] == "StaleGeneration"
    assert daemon.stop.is_set() and daemon.stopping
    monkeypatch.setattr(store, "revalidate", original_revalidate)
    assert store.record(B)["pendingApprovals"] == []


def test_f1_externally_terminalized_worker_never_restores_cached_busy(
    short_root, tmp_path, monkeypatch,
):
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.root import RecordConflict
    store = require_ccx_active(short_root)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    record = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B).record
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, record)
    store.patch_lifecycle(B, changes={"state": "dead", "activeTurnId": None})
    worker._externally_terminalized = True
    worker.record["lifecycle"]["state"] = "busy"
    worker.record["lifecycle"]["activeTurnId"] = "native-stale"
    with pytest.raises(RecordConflict, match="terminalized"):
        worker._save()
    assert worker.public_status()["state"] == "dead"
    assert worker.public_status()["activeTurnId"] is None
    assert store.record(B)["lifecycle"]["state"] == "dead"
    assert store.record(B)["lifecycle"]["activeTurnId"] is None


def _f1_observation_worker(short_root, tmp_path, monkeypatch):
    """Real ACTIVE-root store/evidence plus the original daemon Worker reducer."""
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    from ccx.ops.creation_steps import prepare_worker
    store = require_ccx_active(short_root)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    original = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, original.record)
    daemon.workers[B] = worker
    daemon.by_thread["native-f1-thread"] = B
    return store, daemon, worker


def _f1_turn_notification(daemon, method, turn_id, *, item=None):
    params = {"threadId": "native-f1-thread", "turnId": turn_id}
    if method == "turn/started":
        params["turn"] = {"id": turn_id}
    elif method == "turn/completed":
        params["turn"] = {"id": turn_id, "status": "completed", "items": []}
    if item is not None:
        params["item"] = item
    daemon._on_notification(method, params)


def test_codex_quota_stop_persists_on_exact_native_turn(short_root, tmp_path, monkeypatch):
    """A reached cap survives the real synthetic global record patch path."""
    import threading

    store, daemon, worker = _f1_observation_worker(short_root, tmp_path, monkeypatch)
    ready = threading.Event()
    interrupted = []
    worker._interrupt_turn = lambda turn_id: interrupted.append(turn_id) or ready.set()
    with worker.lock:
        worker.record["lifecycle"].update(state="busy", activeTurnId="capped-turn")
        worker._save()
        worker.cap_turn_id = "capped-turn"
        worker.active_caps = {"7d": 50}
    daemon._on_notification("account/rateLimits/updated", {
        "rateLimits": {"primary": {"windowDurationMins": 10080,
                                   "usedPercent": 50,
                                   "resetsAt": 4102444800}},
    })
    assert ready.wait(1)
    assert interrupted == ["capped-turn"]
    assert store.record(B)["quotaStop"] == {
        "turnId": "capped-turn", "reason": "usage cap 7d reached: 50% >= 50%",
    }
    assert worker.active_caps is None


def test_agent_message_observes_nonconflicting_terminal(
    short_root, tmp_path, monkeypatch,
):
    """External kill is adopted by an unrelated successful CAS before Stop."""
    from ccx.codex.evidence import read_index
    store, daemon, worker = _f1_observation_worker(short_root, tmp_path, monkeypatch)
    _f1_turn_notification(daemon, "turn/started", "native-A")
    assert store.record(B)["lifecycle"]["activeTurnId"] == "native-A"
    store.patch_lifecycle(B, changes={"state": "dead", "activeTurnId": None})
    # A normal item completion only patches lastReport. There is NO CAS conflict:
    # `_save_current_locked` must notice that its fresh response absorbed dead.
    _f1_turn_notification(daemon, "item/completed", "native-A", item={
        "id": "answer-A", "type": "agentMessage", "phase": "final_answer",
        "text": "A completed despite the external kill",
    })
    assert store.record(B)["lastReport"]["text"] == "A completed despite the external kill"
    assert worker._externally_terminalized
    assert worker.killed.is_set()
    _f1_turn_notification(daemon, "turn/completed", "native-A")
    after = store.record(B)
    assert after["lifecycle"]["state"] == "dead"
    assert after["lifecycle"]["activeTurnId"] is None
    assert after["stats"]["turns"] == 0
    assert after.get("lastTurn") is None
    # N1: native completion is still forensic evidence after a terminal CAS,
    # even though it cannot resurrect the worker or change lifecycle.
    assert read_index(B)["turns"]["native-A"]["sawTurnCompleted"] is True


def test_f1_b2_late_A_after_B_is_ignored_but_valid_C_settles(
    short_root, tmp_path, monkeypatch,
):
    """A/B/C each count once; late A must not roll lastTurn B backwards."""
    store, daemon, worker = _f1_observation_worker(short_root, tmp_path, monkeypatch)
    for tid in ("native-A", "native-B"):
        _f1_turn_notification(daemon, "turn/started", tid)
        _f1_turn_notification(daemon, "turn/completed", tid)
    before = store.record(B)
    assert before["stats"]["turns"] == 2
    assert before["lastTurn"]["turnId"] == "native-B"
    _f1_turn_notification(daemon, "turn/completed", "native-A")
    after = store.record(B)
    assert after["stats"]["turns"] == 2
    assert after["lastTurn"] == before["lastTurn"]
    assert after["lifecycle"] == before["lifecycle"]
    _f1_turn_notification(daemon, "turn/started", "native-C")
    _f1_turn_notification(daemon, "turn/completed", "native-C")
    settled = store.record(B)
    assert settled["stats"]["turns"] == 3
    assert settled["lastTurn"]["turnId"] == "native-C"
    _f1_turn_notification(daemon, "turn/completed", "native-B")
    assert store.record(B)["stats"]["turns"] == 3
    assert store.record(B)["lastTurn"]["turnId"] == "native-C"
    assert {"native-A", "native-B", "native-C"}.issubset(worker.completed_turn_ids)


def test_duplicate_native_turn_guard_survives_daemon_restart(
    short_root, tmp_path, monkeypatch,
):
    """Reload original Worker from disk/journal; late A cannot settle twice."""
    from ccx.codex.evidence import read_index
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    store, daemon, worker = _f1_observation_worker(short_root, tmp_path, monkeypatch)
    for tid in ("native-A", "native-B"):
        _f1_turn_notification(daemon, "turn/started", tid)
        _f1_turn_notification(daemon, "turn/completed", tid)
    before = store.record(B)
    assert before["stats"]["turns"] == 2
    index = read_index(B)
    assert index["turns"]["native-A"]["sawTurnCompleted"]
    assert index["turns"]["native-B"]["sawTurnCompleted"]
    worker.evidence.close()
    # No cached original Worker or completed-id set survives this simulated
    # daemon lifetime. The independent journal + record supply the evidence.
    new_daemon = CodexDaemon()
    new_daemon._uuid_store = store
    reloaded = Worker(new_daemon, store.record(B))
    new_daemon.workers[B] = reloaded
    new_daemon.by_thread["native-f1-thread"] = B
    assert {"native-A", "native-B"}.issubset(reloaded.completed_turn_ids)
    _f1_turn_notification(new_daemon, "turn/completed", "native-A")
    assert store.record(B)["stats"]["turns"] == 2
    assert store.record(B)["lastTurn"]["turnId"] == "native-B"
    _f1_turn_notification(new_daemon, "turn/started", "native-C")
    _f1_turn_notification(new_daemon, "turn/completed", "native-C")
    assert store.record(B)["stats"]["turns"] == 3
    assert store.record(B)["lastTurn"]["turnId"] == "native-C"


@pytest.mark.parametrize("restart", [False, True])
def test_f1_s1_redelivered_start_of_completed_turn_cannot_stick_busy(
    short_root, tmp_path, monkeypatch, restart,
):
    """A duplicate start+completion of settled A is journaled, never reopened."""
    from ccx.codex.evidence import read_index
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    store, daemon, worker = _f1_observation_worker(short_root, tmp_path, monkeypatch)
    _f1_turn_notification(daemon, "turn/started", "native-A")
    _f1_turn_notification(daemon, "turn/completed", "native-A")
    assert store.record(B)["lifecycle"]["state"] == "idle"
    if restart:
        worker.evidence.close()
        daemon = CodexDaemon()
        daemon._uuid_store = store
        worker = Worker(daemon, store.record(B))
        daemon.workers[B] = worker
        daemon.by_thread["native-f1-thread"] = B
        assert "native-A" in worker.completed_turn_ids
    _f1_turn_notification(daemon, "turn/started", "native-A")
    _f1_turn_notification(daemon, "turn/completed", "native-A")
    replayed = store.record(B)
    assert replayed["lifecycle"]["state"] == "idle"
    assert replayed["lifecycle"]["activeTurnId"] is None
    assert replayed["stats"]["turns"] == 1
    assert replayed["lastTurn"]["turnId"] == "native-A"
    assert worker.persistence_error is None
    assert read_index(B)["turns"]["native-A"]["sawTurnCompleted"]
    _f1_turn_notification(daemon, "turn/started", "native-B")
    assert store.record(B)["lifecycle"]["activeTurnId"] == "native-B"
    _f1_turn_notification(daemon, "turn/completed", "native-B")
    settled = store.record(B)
    assert settled["lifecycle"]["state"] == "idle"
    assert settled["stats"]["turns"] == 2
    assert settled["lastTurn"]["turnId"] == "native-B"


# F2 exercises the same physical fake binary and singleton as the existing
# normal Codex CLI test, with optional fake-native reply/source faults.
from contextlib import contextmanager
import hashlib


@contextmanager
def _f2_fake_processes(short_root, tmp_path, monkeypatch, **fake_options):
    from ccx.codex.runtime import daemon_rpc
    linger = str(fake_options.pop("linger_seconds", 120))
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    trace = tmp_path / "f2-physical-provider-trace"
    fake = _fake_codex_executable(tmp_path, trace, **fake_options)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(short_root),
           "CCX_CODEX_BIN": str(fake), "PYTHONPATH": str(repo),
           "CCX_CODEXD_LINGER": linger}
    for name in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(name, None)
    processes = []

    # F2 tests drive notifyd.process_registration themselves, including exact
    # retry/settlement timing. Keep those fake daemon turns from independently
    # reviving notifyd; test_notify_revive exercises the real callback instead.
    test_daemon = ('import ccx.notify.daemon as notify_daemon; notify_daemon.revive_notify_daemon_after_turn = lambda *_: None; from ccx.codex.daemon.daemon import main; main()')

    def start_daemon():
        log = (tmp_path / f"f2-daemon-{len(processes)}.log").open("wb")
        process = subprocess.Popen(
            [sys.executable, "-B", "-c", test_daemon],
            env=env, cwd=str(repo), stdout=log, stderr=log,
            start_new_session=True,
        )
        log.close()
        processes.append(process)
        until = time.monotonic() + 9
        status = {}
        while time.monotonic() < until:
            status = daemon_rpc({"op": "daemon-status"}, timeout=.5)
            if status.get("ok") is True and status.get("state") == "ready":
                break
            if process.poll() is not None:
                break
            time.sleep(.04)
        assert status.get("state") == "ready" and status.get("pid") == process.pid, (
            status, (tmp_path / f"f2-daemon-{len(processes)-1}.log").read_text(
                errors="replace")[-3000:]
        )
        return process

    def cli(*argv, timeout=20):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", *argv],
            env=env, cwd=str(repo), capture_output=True, text=True,
            timeout=timeout,
        )

    def restart(*, force=False):
        if processes[-1].poll() is None:
            if not force:
                # The reply callback fsyncs the candidate marker BEFORE it
                # releases the held slot, so a test that saw the marker can
                # still race a correct P1 normal-stop refusal. Wait (bounded)
                # for the daemon's own pending/held counts; never auto-force.
                until = time.monotonic() + 5
                while True:
                    evidence = daemon_rpc({"op": "daemon-status"}, timeout=2)[
                        "nativeStartEvidence"]
                    if evidence["pending"] + evidence["held"] == 0:
                        break
                    assert time.monotonic() < until, evidence
                    time.sleep(.05)
            assert daemon_rpc({"op": "daemon-stop", "force": force}, timeout=2)["ok"]
            processes[-1].wait(timeout=5)
        return start_daemon()

    start_daemon()
    try:
        yield cli, restart, trace, require_ccx_active(short_root), daemon_rpc
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                try:
                    daemon_rpc({"op": "daemon-stop"}, timeout=.7)
                except Exception:
                    pass
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=3)


def _f2_marker(store, worker_id):
    token = store.native_activation(worker_id)["submissionId"]
    name = hashlib.sha256(("activate\x00" + token).encode()).hexdigest() + ".json"
    return (store.pointer.generation_root / "runtime-metadata" /
            "codex-native-effects" / worker_id / name)


def _f2_until_candidate(store, wid, seconds=6):
    path = _f2_marker(store, wid)
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if path.exists():
            data = json.loads(path.read_text())
            if data.get("nativeCandidate"):
                return data
        time.sleep(.03)
    return json.loads(path.read_text()) if path.exists() else None


def _f2_new(cli, cwd, wid, *more):
    return cli("new", "--engine", "codex", "--cwd", str(cwd), "--worker-id", wid,
               *more, "--json")


def test_f2_three_second_start_keeps_lease_short_and_confirms_original(
    short_root, tmp_path, monkeypatch,
):
    from ccx.store.root import NativeActionDenied
    from ccx.ops.creation_steps import prepare_worker
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        start_delay=3.0, persisted_threads=tmp_path / "f2-native-history.json",
    ) as (cli, _restart, trace, store, rpc):
        other = prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
        args = [sys.executable, "-B", "-m", "ccx", "new", "--engine", "codex",
                "--cwd", str(tmp_path), "--worker-id", B, "--start-timeout", "9", "--json"]
        repo = Path(__file__).resolve().parent.parent
        env = {**os.environ, "CCX_ROOT": str(short_root),
               "CCX_CODEX_BIN": str(tmp_path / "fake-codex"),
               "PYTHONPATH": str(repo)}
        launched = time.monotonic()
        started = subprocess.Popen(
            args, env=env, cwd=str(repo), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True,
        )
        try:
            deadline = time.monotonic() + 6
            while time.monotonic() < deadline:
                if trace.exists() and "RPC thread/start" in trace.read_text():
                    break
                time.sleep(.025)
            assert "RPC thread/start" in trace.read_text()
            # Admission is complete while provider takes three seconds to
            # reply. Another worker's generation lease must not be blocked.
            with store.lease(timeout=.4):
                assert store.native_activation(B)["state"] == "submitted-unknown"
            with pytest.raises(NativeActionDenied):
                store.authorize_native_action(B, "send")
            out, err = started.communicate(timeout=13)
            assert started.returncode == 0, (out, err)
            assert time.monotonic() - launched >= 3
            result = json.loads(out)
            assert result["worker"] == f"codex:{B}" and result["native_ready"]
            assert result["worker_id"] == B
            assert store.record(A)["native"]["sessionId"] == other.record["native"]["sessionId"]
            marker = json.loads(_f2_marker(store, B).read_text())
            native = store.record(B)["native"]
            assert marker["nativeCandidate"]["threadId"] == native["threadId"]
            assert marker["source"] == native["threadSource"] == f"ccx-worker:{B}"
            assert marker["createSha256"] == store.record(B)["createSha256"]
            assert marker["generation"] == store.generation
            assert trace.read_text().count("RPC thread/start") == 1
            assert trace.read_text().count("RPC thread/read") >= 1
            assert "RPC thread/list" not in trace.read_text()
            replay = _f2_new(cli, tmp_path, B, "--start-timeout", "2")
            assert replay.returncode == 0 and json.loads(replay.stdout)["worker_id"] == B
            assert trace.read_text().count("RPC thread/start") == 1
        finally:
            if started.poll() is None:
                started.terminate()
                started.wait(timeout=4)


def test_f2_lost_cli_response_reconciles_saved_candidate_after_daemon_restart(
    short_root, tmp_path, monkeypatch,
):
    from ccx.store.root import NativeActionDenied
    history = tmp_path / "f2-native-durable-threads.json"
    factory = _fake_codex_executable

    def resumable(*args, **kwargs):
        # This node only: the restarted app-server resumes the exact persisted
        # original thread (promptless, so empty history) with positive idle.
        binary = factory(*args, **kwargs)
        text = binary.read_text()
        before = "        elif method=='thread/unsubscribe':\n"
        assert text.count(before) == 1
        binary.write_text(text.replace(before, (
            "        elif method=='thread/resume':\n"
            "            tid=params.get('threadId')\n"
            "            if tid not in threads: raise ValueError('unknown native thread')\n"
            "            result={'thread':dict(threads[tid],status={'type':'idle'}),"
            "'model':threads[tid].get('model')}\n" + before), 1))
        return binary

    monkeypatch.setitem(globals(), "_fake_codex_executable", resumable)
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=3.0,
        persisted_threads=history,
    ) as (cli, restart, trace, store, rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", ".2")
        partial = json.loads(first.stdout)
        assert first.returncode == 3 and partial["worker_id"] == B, (first.stdout, first.stderr)
        assert partial["native_ready"] is False
        assert partial["activation_state"] == "submitted-unknown"
        with pytest.raises(NativeActionDenied):
            store.authorize_native_action(B, "send")
        early_retry = _f2_new(cli, tmp_path, B, "--start-timeout", ".1")
        assert early_retry.returncode == 3, (early_retry.stdout, early_retry.stderr)
        assert trace.read_text().count("RPC thread/start") == 1
        candidate = _f2_until_candidate(store, B)
        assert candidate and candidate.get("nativeCandidate"), candidate
        native_id = candidate["nativeCandidate"]["threadId"]
        assert store.record(B)["native"]["threadId"] is None
        assert candidate["token"] == store.native_activation(B)["submissionId"]
        assert candidate["source"] == f"ccx-worker:{B}"
        # The daemon/client both exited before the claim/record confirmation,
        # but the original provider response candidate was already fsynced.
        restart()
        recovered = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
        assert recovered.returncode == 0, (recovered.stdout, recovered.stderr)
        bound = store.record(B)
        assert bound["native"]["threadId"] == native_id
        assert bound["native"]["threadSource"] == f"ccx-worker:{B}"
        assert store.native_activation(B)["state"] == "confirmed"
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()
        assert trace.read_text().count("RPC thread/read") >= 1
        # A normal initial turn is permitted only AFTER the immutable original
        # create binding. A duplicate delivery token never sends twice.
        send_args = ("send", B, "f2 recovered original native thread",
                     "--no-wait", "--delivery-id", "f2-recovered-delivery", "--json")
        sent = cli(*send_args)
        assert sent.returncode == 0, (sent.stdout, sent.stderr)
        again = cli(*send_args)
        assert again.returncode == 0, (again.stdout, again.stderr)
        assert trace.read_text().count("RPC turn/start") == 1
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC thread/resume") == 1
        resumed = trace.read_text().split("RPC thread/resume ", 1)[1].split("RPC ", 1)[0]
        assert f"'threadId': {native_id!r}" in resumed, resumed


def test_f2_first_thread_read_failure_preserves_original_candidate(
    short_root, tmp_path, monkeypatch,
):
    read_mode = tmp_path / "f2-read-mode"
    read_mode.write_text("fail")
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, read_mode=read_mode,
    ) as (cli, _restart, trace, store, rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", "4")
        assert first.returncode == 3, (first.stdout, first.stderr)
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC thread/read") >= 1
        candidate = _f2_until_candidate(store, B, .5)
        assert candidate and candidate.get("nativeCandidate")
        thread_id = candidate["nativeCandidate"]["threadId"]
        assert store.native_activation(B)["state"] == "submitted-unknown"
        assert store.record(B)["native"]["threadId"] is None
        read_mode.unlink()
        recovered = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
        assert recovered.returncode == 0, (recovered.stdout, recovered.stderr)
        assert store.record(B)["native"]["threadId"] == thread_id
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC thread/read") >= 2
        assert "RPC thread/list" not in trace.read_text()


@pytest.mark.parametrize("source_mode", ["foreign", "null", "wrong-id"])
def test_f2_foreign_or_missing_independent_thread_source_never_binds(
    short_root, tmp_path, monkeypatch, source_mode,
):
    read_mode = tmp_path / "f2-foreign-mode"
    read_mode.write_text(source_mode)
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, read_mode=read_mode,
    ) as (cli, _restart, trace, store, rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", "3")
        assert first.returncode == 3, (first.stdout, first.stderr)
        assert _f2_until_candidate(store, B, .5)["nativeCandidate"]["threadId"]
        assert store.native_activation(B)["state"] == "submitted-unknown"
        repeated = _f2_new(cli, tmp_path, B, "--start-timeout", "2")
        assert repeated.returncode == 3, (repeated.stdout, repeated.stderr)
        assert json.loads(repeated.stdout)["native_ready"] is False
        assert store.record(B)["native"]["threadId"] is None
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()


@pytest.mark.parametrize("tamper", ["absent", "source", "token", "candidate"])
def test_f2_missing_or_tampered_original_effect_refuses_adoption(
    short_root, tmp_path, monkeypatch, tamper,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=3.0,
    ) as (cli, _restart, trace, store, rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", ".1")
        assert first.returncode == 3, (first.stdout, first.stderr)
        marker = _f2_marker(store, B)
        candidate = _f2_until_candidate(store, B)
        assert candidate and candidate.get("nativeCandidate"), candidate
        if tamper == "absent":
            marker.unlink()
        else:
            if tamper == "source":
                candidate["source"] = f"ccx-worker:{A}"
            elif tamper == "token":
                candidate["token"] = "forged-original-effect"
            elif tamper == "candidate":
                candidate["nativeCandidate"]["threadId"] = "foreign-native-id"
            marker.write_text(json.dumps(candidate, sort_keys=True))
        repeated = _f2_new(cli, tmp_path, B, "--start-timeout", ".2")
        assert repeated.returncode == 3, (repeated.stdout, repeated.stderr)
        result = json.loads(repeated.stdout)
        assert result["worker_id"] == B and not result["native_ready"]
        if tamper in {"source", "token"}:
            # Contradictory original immutable proof is an integrity refusal,
            # WITH the exact claimed UUID for the caller's forensic recovery.
            assert result["integrity_error"] is True, result
            assert result["cause_type"] == "StorageIntegrityError", result
            assert result["native_start_error"], result
        else:
            # Missing/foreign candidates remain unknown, never adopted.
            assert result["integrity_error"] is False
        assert store.native_activation(B)["state"] == "submitted-unknown"
        assert store.record(B)["native"]["threadId"] is None
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()


def test_f2_no_original_reply_candidate_never_scans_or_replays_even_after_restart(
    short_root, tmp_path, monkeypatch,
):
    history = tmp_path / "f2-accepted-without-native-response.json"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, no_start_reply=True,
        persisted_threads=history,
    ) as (cli, restart, trace, store, rpc):
        result = _f2_new(cli, tmp_path, B, "--start-timeout", ".2")
        assert result.returncode == 3, (result.stdout, result.stderr)
        assert trace.read_text().count("RPC thread/start") == 1
        assert history.exists() and f"ccx-worker:{B}" in history.read_text()
        marker = json.loads(_f2_marker(store, B).read_text())
        assert "nativeCandidate" not in marker
        # P1: the unanswered original pins a normal stop; simulate the
        # daemon's death with an explicit forced operator stop.
        restart(force=True)
        again = _f2_new(cli, tmp_path, B, "--start-timeout", ".2")
        assert again.returncode == 3, (again.stdout, again.stderr)
        assert json.loads(again.stdout)["activation_state"] == "submitted-unknown"
        assert store.native_activation(B)["state"] == "submitted-unknown"
        assert store.record(B)["native"]["threadId"] is None
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()
        assert "RPC thread/read" not in trace.read_text()


def test_f2_same_prefix_two_codex_workers_plus_claude_source_isolation(
    short_root, tmp_path, monkeypatch,
):
    from ccx.ops.creation_steps import prepare_worker
    second = "aaaaaaaa-9876-4000-8000-000000000099"
    assert A[:8] == B[:8] == second[:8]
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=3.0,
    ) as (cli, _restart, trace, store, rpc):
        claude = prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
        partial = _f2_new(cli, tmp_path, B, "--start-timeout", ".1")
        assert partial.returncode == 3, (partial.stdout, partial.stderr)
        distinct = _f2_new(cli, tmp_path, second, "--start-timeout", "9")
        assert distinct.returncode == 0, (distinct.stdout, distinct.stderr)
        recovered = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
        assert recovered.returncode == 0, (recovered.stdout, recovered.stderr)
        first_native = store.record(B)["native"]
        second_native = store.record(second)["native"]
        assert first_native["threadId"] != second_native["threadId"]
        assert first_native["threadSource"] == f"ccx-worker:{B}"
        assert second_native["threadSource"] == f"ccx-worker:{second}"
        assert store.record(A)["native"]["sessionId"] == claude.record["native"]["sessionId"]
        assert store.native_activation(B)["state"] == "confirmed"
        assert store.native_activation(second)["state"] == "confirmed"
        assert trace.read_text().count("RPC thread/start") == 2
        assert "RPC thread/list" not in trace.read_text()
        denied = cli("send", B[:8], "cannot address display short",
                     "--no-wait", "--json")
        assert denied.returncode != 0


def test_f2_caller_process_dies_after_native_dispatch_before_reply(
    short_root, tmp_path, monkeypatch,
):
    """An abruptly lost CLI reply cannot replay the original native create."""
    from ccx.store.root import NativeActionDenied
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=3.0,
    ) as (cli, _restart, trace, store, rpc):
        repo = Path(__file__).resolve().parent.parent
        env = {**os.environ, "CCX_ROOT": str(short_root),
               "CCX_CODEX_BIN": str(tmp_path / "fake-codex"),
               "PYTHONPATH": str(repo)}
        for name in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
            env.pop(name, None)
        process = subprocess.Popen(
            [sys.executable, "-B", "-m", "ccx", "new", "--engine", "codex",
             "--cwd", str(tmp_path), "--worker-id", B, "--start-timeout", "9", "--json"],
            env=env, cwd=str(repo), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True,
        )
        try:
            until = time.monotonic() + 6
            while time.monotonic() < until:
                if trace.exists() and "RPC thread/start" in trace.read_text():
                    break
                time.sleep(.03)
            assert "RPC thread/start" in trace.read_text()
            assert store.native_activation(B)["state"] == "submitted-unknown"
            # Kill only this disposable CLI process. The already-dispatched
            # original request belongs to codexd and its PendingRequest.
            process.kill()
            process.communicate(timeout=3)
            assert process.returncode != 0
            with pytest.raises(NativeActionDenied):
                store.authorize_native_action(B, "send")
            candidate = _f2_until_candidate(store, B)
            assert candidate and candidate.get("nativeCandidate"), candidate
            original_thread = candidate["nativeCandidate"]["threadId"]
            assert store.record(B)["native"]["threadId"] is None
            result = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
            assert result.returncode == 0, (result.stdout, result.stderr)
            assert store.record(B)["native"]["threadId"] == original_thread
            assert store.native_activation(B)["state"] == "confirmed"
            assert trace.read_text().count("RPC thread/start") == 1
            assert "RPC thread/list" not in trace.read_text()
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=3)


def test_codex_cumulative_thread_tokens_roll_up_at_settlement_only(
    short_root, tmp_path, monkeypatch,
):
    """Native usage totals, not per-turn last or fabricated zeroes, feed list."""
    store, daemon, worker = _f1_observation_worker(short_root, tmp_path, monkeypatch)

    # Preparation has not observed a Codex token usage event.
    first = store.record(B)
    assert first["stats"] == {"turns": 0}
    assert next(r for r in store.list_workers() if r["engine"] == "codex")["input_tokens"] is None

    def usage(turn, input_total, output_total, last_input, last_output):
        daemon._on_notification("thread/tokenUsage/updated", {
            "threadId": "native-f1-thread", "turnId": turn,
            "tokenUsage": {"total": {"inputTokens": input_total,
                                     "outputTokens": output_total},
                           "last": {"inputTokens": last_input,
                                    "outputTokens": last_output}},
        })

    _f1_turn_notification(daemon, "turn/started", "tokens-A")
    usage("tokens-A", 400, 120, 150, 40)
    assert "inputTokens" not in store.record(B)["stats"]
    _f1_turn_notification(daemon, "turn/completed", "tokens-A")
    stats = store.record(B)["stats"]
    assert (stats["turns"], stats["inputTokens"], stats["outputTokens"]) == (1, 400, 120)
    _f1_turn_notification(daemon, "turn/completed", "tokens-A")
    assert store.record(B)["stats"] == stats  # duplicated completion cannot recount

    _f1_turn_notification(daemon, "turn/started", "tokens-B")
    usage("tokens-B", 650, 205, 200, 70)
    _f1_turn_notification(daemon, "turn/completed", "tokens-B")
    stats = store.record(B)["stats"]
    assert (stats["turns"], stats["inputTokens"], stats["outputTokens"]) == (2, 650, 205)
    # A stale/lower native total must not decrease cumulative accounting.
    _f1_turn_notification(daemon, "turn/started", "tokens-C")
    usage("tokens-C", 600, 180, 15, 2)
    _f1_turn_notification(daemon, "turn/completed", "tokens-C")
    assert store.record(B)["stats"]["inputTokens"] == 650
    assert store.record(B)["stats"]["outputTokens"] == 205
    projection = next(r for r in store.list_workers(engine="codex") if r["worker_id"] == B)
    assert (projection["input_tokens"], projection["output_tokens"]) == (650, 205)
    worker.evidence.close()
