"""Original Codex compact C3: actual singleton, disposable fake app-server.

All native events come from the local fake executable; this is not evidence
of real Codex-provider parity or authority to enable public C4 adapters.
"""
from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from ccx import constants
from ccx.codex import ops as codex_ops
from ccx.codex import runtime as codex_runtime
from ccx.ops.send import ensure_ready
from ccx.ops.creation_steps import (
    prepare_worker, dispatch_prepared_codex_native, reconcile_submitted_native,
)
from ccx.store.root import require_ccx_active
from test_generation_store import short_root, A, B
from test_codex_native_effects import _fake_codex_executable


def _compact_fake(tmp_path, trace: Path, gate: Path, mode: str) -> Path:
    """Only a test-local thread/compact/start extension to the accepted fake."""
    executable = _fake_codex_executable(tmp_path, trace, completion_gate=gate)
    script = executable.read_text()
    original = "        elif method=='thread/list':\n"
    extension = """
        elif method=='thread/compact/start':
            tid=params.get('threadId')
            if tid not in threads: raise ValueError('unknown original compact thread')
            # Physical fake provider witnesses the original submitted fence
            # and exact worker/operation marker before any native compact.
            import pathlib,hashlib
            worker_id=threads[tid]['threadSource'].split(':',1)[1]
            root=pathlib.Path(os.environ['CCX_ROOT'])
            generation=json.loads((root/'identity'/'current.json').read_text())['generation']
            shared=root/'identity'/'generations'/generation
            active=json.loads((shared/'workers'/'codex'/worker_id/'record.json').read_text())['activeCompact']
            if not isinstance(active,dict):raise ValueError('native compact missing original marker')
            token=active['operationId']
            key=hashlib.sha256(token.encode('utf-8')).hexdigest()+'.json'
            effect=json.loads((shared/'receipts'/'codex-compact'/'codex'/worker_id/key).read_text())
            if (effect['state']!='submitted-unknown'
                    or effect['workerId']!=worker_id
                    or effect['operationId']!=token):
                raise ValueError('native compact lacked submitted full-UUID receipt')
            with open(TRACE,'a',encoding='utf8') as f:
                f.write('C3_SUBMITTED_RECEIPT '+worker_id+' '+token+'\\n')
            counter+=1
            turn_id='native-internal-compact-'+str(counter)
            result={}
"""
    assert script.count(original) == 1
    script = script.replace(original, extension + original, 1)
    original = "        if method=='turn/start':\n"
    extension = """
        if method=='thread/compact/start':
            print(json.dumps({'jsonrpc':'2.0','method':'turn/started',
                'params':{'threadId':tid,'turn':{'id':turn_id}}}),flush=True)
            # A legacy compact event and generic idle cannot complete C3.
            print(json.dumps({'jsonrpc':'2.0','method':'thread/compacted',
                'params':{'threadId':tid,'turnId':turn_id}}),flush=True)
            print(json.dumps({'jsonrpc':'2.0','method':'thread/status/changed',
                'params':{'threadId':tid,'status':{'type':'idle'}}}),flush=True)
            until=time.monotonic()+10
            while not os.path.exists(GATE) and time.monotonic()<until:
                time.sleep(.01)
            if not os.path.exists(GATE):
                raise RuntimeError('fake compact event gate not released')
            item_mode=__COMPACT_MODE__
            if item_mode not in ('missing','failed','interrupted','cancelled'):
                print(json.dumps({'jsonrpc':'2.0','method':'item/completed',
                    'params':{'threadId':tid,
                        'turnId':turn_id if item_mode!='wrong' else 'foreign-turn',
                        'item':{'id':'native-compaction-item-'+turn_id,
                                'type':'contextCompaction'}}}),flush=True)
            native_status=item_mode if item_mode in ('failed','interrupted','cancelled') else 'completed'
            turn={'id':turn_id,'status':native_status,'items':[]}
            if native_status!='completed':
                turn['error']={'message':'fake compact internal turn failed'}
            threads[tid]['turns'].append(turn)
            print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',
                'params':{'threadId':tid,'turn':turn}}),flush=True)
"""
    assert script.count(original) == 1
    script = script.replace(
        original, extension.replace("__COMPACT_MODE__", repr(mode)) + original, 1,
    )
    executable.write_text(script)
    return executable


@pytest.fixture
def real_compact_host(short_root, tmp_path, monkeypatch, request):
    mode = getattr(request, "param", "success")
    gate = tmp_path / "release-native-compact"
    trace = tmp_path / "appserver-rpcs"
    executable = _compact_fake(tmp_path, trace, gate, mode)
    root = short_root
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setenv("CCX_ROOT", str(root))
    monkeypatch.setenv("CCX_CODEX_BIN", str(executable))
    monkeypatch.setenv("CCX_CODEXD_LINGER", "120")
    store = require_ccx_active(root)
    # A different engine may own the same eight display characters.
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    prepared = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "PYTHONPATH": str(repo)}
    # These fixture tests inspect the durable parent registration themselves.
    # Native turn/completed otherwise spawns a second, detached notifyd from
    # inside codexd (the parent pytest's monkeypatch cannot reach that child).
    # That asynchronous process can still write to short_root after daemon
    # shutdown, racing pytest's disposable-ccx directory cleanup. The actual
    # revival path has its own integration tests.
    isolated_daemon = (
        'import ccx.notify.daemon as notify_daemon; notify_daemon.revive_notify_daemon_after_turn = lambda *_: None; from ccx.codex.daemon.daemon import main; main()'
    )
    log_file = tmp_path / "fake-codexd.log"
    with log_file.open("wb") as output:
        daemon = subprocess.Popen(
            [sys.executable, "-B", "-c", isolated_daemon],
            env=env, cwd=str(repo), stdout=output, stderr=output,
            start_new_session=True,
        )
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            status = codex_runtime.daemon_rpc({"op": "daemon-status"}, timeout=.5)
            if status.get("ok") and status.get("state") == "ready":
                break
            if daemon.poll() is not None:
                break
            time.sleep(.04)
        assert status.get("state") == "ready", (
            status, log_file.read_text(errors="replace")[-4000:]
        )

        def native_start(budget):
            activation = store.native_activation(B)
            return codex_runtime.daemon_rpc({
                "op": "native-activate", "workerId": B,
                "submissionId": activation["submissionId"],
                "rpcGeneration": constants.CODEXD_RPC_GENERATION,
            }, timeout=budget)

        dispatched = dispatch_prepared_codex_native(
            store, prepared, bounded_native_submit=native_start, dispatch_budget_s=2.,
        )
        assert dispatched.activation_state == "submitted-unknown"
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            claim = store.native_activation(B)
            proof = codex_runtime.daemon_rpc({
                "op": "native-activation-status", "workerId": B,
                "submissionId": claim["submissionId"],
                "rpcGeneration": constants.CODEXD_RPC_GENERATION,
            }, timeout=2)
            if proof.get("accepted"):
                break
            time.sleep(.05)
        assert proof.get("accepted") is True, proof
        activated = reconcile_submitted_native(
            store, prepared, original_receipt=proof,
        )
        assert activated.activation_state == "confirmed"
        assert store.record(B)["native"]["threadSource"] == f"ccx-worker:{B}"
        assert ensure_ready(B, timeout=10)["state"] == "idle"
        yield store, trace, gate
    finally:
        gate.touch(exist_ok=True)
        if daemon.poll() is None:
            codex_runtime.daemon_rpc({"op": "daemon-stop", "force": True}, timeout=1)
        try:
            daemon.wait(timeout=3)
        except subprocess.TimeoutExpired:
            os.killpg(daemon.pid, signal.SIGKILL)
            daemon.wait(timeout=3)


def _physical_count(trace: Path) -> int:
    return trace.read_text().count("RPC thread/compact/start") if trace.exists() else 0


def _settled(worker: str, token: str, *, timeout=5.):
    end = time.monotonic() + timeout
    row = None
    while time.monotonic() < end:
        row = codex_ops.compact_observation(worker, token)
        if row["status"] not in ("submitted", "prepared"):
            return row
        time.sleep(.06)
    return row


def test_direct_daemon_receiptless_wrong_owner_and_shared_prefix_no_effect(
    real_compact_host,
):
    store, trace, _gate = real_compact_host
    tid = store.record(B)["native"]["threadId"]
    for request in (
        {"op": "compact", "short": B, "threadId": tid, "operationId": "missing"},
        {"op": "compact", "short": A, "threadId": tid, "operationId": "foreign"},
        {"op": "compact", "short": B[:8], "threadId": tid, "operationId": "prefix"},
        {"op": "compact", "short": B, "threadId": tid, "operationId": ""},
        {"op": "compact", "short": B, "threadId": tid, "operationId": "bad\x00id"},
        {"op": "compact", "short": B, "threadId": "foreign-thread", "operationId": "wrong-thread"},
    ):
        response = codex_runtime.daemon_call(request, timeout=1)
        assert response.get("ok") is False, response
    assert _physical_count(trace) == 0
    assert not store.read_receipt(B, "codex-compact", "missing")
    assert not store.worker_path(B, "compact.json").exists()


@pytest.mark.parametrize("lost_reply", ["transport-error", "served-indeterminate"])
def test_lost_ack_same_id_reconciles_busy_marker_one_physical_compact(
    real_compact_host, monkeypatch, lost_reply,
):
    store, trace, gate = real_compact_host
    tid = store.record(B)["native"]["threadId"]
    token = "original-codex-lost-ack"
    original = codex_runtime.daemon_call

    def lose_caller_reply(message, *args, **kwargs):
        result = original(message, *args, **kwargs)
        if message.get("op") == "compact":
            assert result.get("ok") is True, result
            if lost_reply == "transport-error":
                raise TimeoutError("the caller lost the positive compact ACK")
            return {"transport": "served", "ok": False,
                    "reconcileRequired": True, "error": "positive native ACK lost"}
        return result

    with monkeypatch.context() as patch:
        patch.setattr(codex_runtime, "daemon_call", lose_caller_reply)
        pending = codex_ops.compact_worker(B, operation_id=token, wait=False, timeout=10)
    assert pending["operationId"] == token and pending["compacted"] is False
    assert pending["status"] in ("submitted", "unknown")
    assert store.read_receipt(B, "codex-compact", token)["state"] == "submitted-unknown"
    assert _physical_count(trace) == 1
    assert "C3_SUBMITTED_RECEIPT " + B + " " + token in trace.read_text()
    active = store.record(B)["activeCompact"]
    assert active["operationId"] == token
    assert store.record(B)["lifecycle"]["state"] == "busy"

    retried = codex_ops.compact_worker(B, operation_id=token, wait=False, timeout=3)
    assert retried["compacted"] is False and retried["operationId"] == token
    replayed = original({
        "op": "compact", "short": B, "threadId": tid, "operationId": token,
    }, timeout=1)
    assert replayed["ok"] and replayed["replayed"] is True, replayed
    assert _physical_count(trace) == 1
    assert codex_ops.compact_observation(B, token)["status"] == "submitted"
    gate.touch()
    result = _settled(B, token)
    assert result["status"] == "succeeded", result
    assert result["compacted"] is True
    assert result["compactTurnId"] and result["contextItemId"]
    assert store.record(B)["lastCompact"]["nativeTurnStarted"] is True
    assert result["nativeTurnStatus"] == "completed"
    assert result["contextAfter"] is None  # No newer native token-usage.
    assert store.record(B)["activeCompact"] is None
    assert _physical_count(trace) == 1
    # C3's exact-ID source view is pure for later V1 status:
    # no record/receipt writes or another physical provider operation.
    watched = (store.worker_path(B, "record.json"),
               store.receipt_path(B, "codex-compact", token))
    snapshots = [(f.read_bytes(), f.stat().st_mtime_ns) for f in watched]
    assert codex_ops.compact_observation(B, token)["status"] == "succeeded"
    assert [(f.read_bytes(), f.stat().st_mtime_ns) for f in watched] == snapshots
    assert _physical_count(trace) == 1
    settled = codex_ops.compact_worker(B, operation_id=token, wait=False, timeout=3)
    assert settled["status"] == "succeeded" and settled["compacted"] is True, settled
    assert _physical_count(trace) == 1


@pytest.mark.parametrize("real_compact_host", ["failed", "interrupted", "cancelled", "wrong", "missing"], indirect=True)
def test_internal_failed_turn_foreign_or_missing_item_never_success(
    real_compact_host, request,
):
    store, trace, gate = real_compact_host
    mode = request.node.callspec.params["real_compact_host"]
    token = "original-codex-not-proven"
    accepted = codex_ops.compact_worker(B, operation_id=token, wait=False, timeout=10)
    assert accepted["compacted"] is False and accepted["operationId"] == token
    assert accepted["status"] == "submitted"
    assert _physical_count(trace) == 1
    assert store.read_receipt(B, "codex-compact", token)["state"] == "accepted"
    # Even the fake legacy signal and fake native idle do not prove success.
    assert codex_ops.compact_observation(B, token)["status"] == "submitted"
    gate.touch()
    outcome = _settled(B, token)
    assert outcome["status"] == ("failed" if mode in ("failed", "interrupted", "cancelled") else "unknown"), outcome
    assert outcome["compacted"] is False
    assert outcome["contextAfter"] is None
    assert store.record(B).get("compactCount") in (None, 0)
    assert _physical_count(trace) == 1
    from ccx.workers import WorkerClient
    page = WorkerClient().events(B, kinds=["compact.started", "compact.ended"])
    starts = [e for e in page["events"] if e["kind"] == "compact.started"]
    ends = [e for e in page["events"] if e["kind"] == "compact.ended"]
    assert len(starts) == len(ends) == 1
    assert ends[0]["outcome"] == ("failed" if mode in ("failed", "interrupted", "cancelled") else "unknown")
    assert starts[0].get("compact_id") == ends[0].get("compact_id")
