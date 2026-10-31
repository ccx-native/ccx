"""Full-UUID Codex auto/steer native effect through the EXISTING fake app-server.

Extends only the disposable fake executable from test_codex_native_effects:
hold one native turn in-progress, answer turn/steer with expectedTurnId, or
return an explicit JSON-RPC rejection. No live Codex binary or new harness.
"""
from __future__ import annotations
import ccx.ops.send as ops_send

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from ccx.codex import delivery as codex_delivery
from ccx import constants
from ccx.codex.provenance import parse_client_message_id
from ccx.codex.runtime import daemon_rpc
from ccx.errors import OperationConflict, SendIndeterminate
from ccx.ops.send import ensure_ready, send_turn
from ccx.ops.wait import reconcile_turn
from ccx.ops.creation_steps import (
    dispatch_prepared_codex_native, prepare_worker, reconcile_submitted_native,
)
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, short_root
from test_codex_native_effects import _fake_codex_executable


def _fake_with_steer(path: Path, trace: Path) -> Path:
    executable = _fake_codex_executable(path, trace)
    script = executable.read_text()
    old = "else 'completed')"
    assert script.count(old) == 1
    script = script.replace(
        old, "else 'inProgress' if text.startswith('steer-hold-')\n"
             "                    else 'completed')", 1,
    )
    old = "        elif method=='thread/list':"
    assert script.count(old) == 1
    script = script.replace(old, """        elif method=='turn/steer':
            tid=params.get('threadId')
            turns=threads.get(tid,{}).get('turns') or []
            turn=turns[-1] if turns else None
            inputs=params.get('input') or []
            prompt=inputs[0].get('text','') if inputs else ''
            if (turn is None or turn.get('id')!=params.get('expectedTurnId')
                    or turn.get('status')!='inProgress'
                    or prompt.startswith('native-error-')):
                print(json.dumps({'jsonrpc':'2.0','id':msg['id'],
                    'error':{'code':-32000,'message':'active turn changed'}}),flush=True)
                continue
            turn['items'].append({
                'type':'userMessage','text':prompt,
                'clientUserMessageId':params.get('clientUserMessageId')})
            if prompt.startswith('finish-original-'):
                turn['status']='completed'
                print(json.dumps({'jsonrpc':'2.0','method':'turn/completed',
                    'params':{'threadId':tid,'turn':turn}}),flush=True)
            result={}
        elif method=='thread/list':""", 1)
    old = "            if GATE and text.startswith(('f1-race-', 'f1-approval-')):"
    assert script.count(old) == 1
    script = script.replace(old, """            if text.startswith('steer-hold-'):
                continue  # Deliberately leave this native turn BUSY.
            if GATE and text.startswith(('f1-race-', 'f1-approval-')):""", 1)
    executable.write_text(script)
    executable.chmod(0o700)
    return executable


@pytest.fixture
def original_codex(short_root, tmp_path, monkeypatch):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    prepared = [
        prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=wid)
        for wid in (A, B)
    ]
    trace = tmp_path / "codex-appserver-trace"
    binary = _fake_with_steer(tmp_path, trace)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(short_root),
           "CCX_CODEX_BIN": str(binary), "PYTHONPATH": str(repo),
           "PYTHONDONTWRITEBYTECODE": "1", "CCX_REAL_PARITY": "0"}
    daemon_log = tmp_path / "codexd.log"
    with daemon_log.open("wb") as output:
        process = subprocess.Popen(
            [sys.executable, "-B", "-m", 'ccx.codex.daemon'], env=env,
            cwd=str(repo), stdout=output, stderr=output,
            start_new_session=True,
        )
    try:
        status = {}
        until = time.monotonic()+8
        while time.monotonic() < until:
            status = daemon_rpc({"op": "daemon-status"}, timeout=.5)
            if status.get("state") == "ready":
                break
            if process.poll() is not None:
                break
            time.sleep(.04)
        assert status.get("state") == "ready", (
            status, daemon_log.read_text(errors="replace")[-2500:]
        )
        # Genuine original native-create path binds two distinct provider IDs.
        for claim in prepared:
            def start_native(budget):
                original = store.native_activation(claim.worker)
                return daemon_rpc({
                    "op": "native-activate", "workerId": claim.worker_id,
                    "submissionId": original["submissionId"],
                    "rpcGeneration": constants.CODEXD_RPC_GENERATION,
                }, timeout=budget)
            dispatch_prepared_codex_native(
                store, claim, bounded_native_submit=start_native,
                dispatch_budget_s=2.0,
            )
            evidence = {}
            until = time.monotonic()+5
            while time.monotonic() < until:
                original = store.native_activation(claim.worker)
                evidence = daemon_rpc({
                    "op": "native-activation-status",
                    "workerId": claim.worker_id,
                    "submissionId": original["submissionId"],
                    "rpcGeneration": constants.CODEXD_RPC_GENERATION,
                }, timeout=2)
                if evidence.get("accepted") is True:
                    break
                assert evidence.get("ok") is True, evidence
                time.sleep(.04)
            assert evidence.get("accepted") is True, evidence
            reconcile_submitted_native(store, claim, original_receipt=evidence)
            ready = ensure_ready(claim.worker)
            assert ready["state"] == "idle", ready
        one = store.record(A)["native"]["threadId"]
        other = store.record(B)["native"]["threadId"]
        assert A[:8] == B[:8] and one != other
        yield store, trace
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=3)


def _hold_busy(store, trace, *, worker=B):
    prior = trace.read_text().count("RPC turn/start")
    result = send_turn(
        worker, "steer-hold-original-native-R",
        delivery_id="strict-open-"+worker[:13],
    )
    assert result["status"] == "accepted", result
    r = result["delivery"]["turn_id"]
    assert isinstance(r, str) and r
    until = time.monotonic()+2
    while time.monotonic() < until:
        rec = store.record(worker)
        if (rec["lifecycle"]["state"] == "busy"
                and rec["lifecycle"]["activeTurnId"] == r):
            break
        time.sleep(.02)
    assert store.record(worker)["lifecycle"]["activeTurnId"] == r
    assert trace.read_text().count("RPC turn/start") == prior+1
    return r


def test_busy_exact_steer_and_auto_routes_plus_idle_auto(original_codex):
    store, trace = original_codex
    before_sibling = store.record(A)
    r = _hold_busy(store, trace)
    prior_starts = trace.read_text().count("RPC turn/start")
    seen = []

    def forbidden_completion(*args):
        seen.append(args)
        pytest.fail("steer placement must not borrow turn completion")

    steer = send_turn(
        B, "steer into exact original R", delivery_id="steer-R",
        mode="steer_only", _observe_settlement=forbidden_completion,
    )
    assert steer["status"] == "accepted" and not steer["settled"]
    frozen = codex_delivery.read_receipt(B, "steer-R")
    assert frozen["requestedMode"] == "steer_only"
    assert frozen["route"] == "steer"
    assert frozen["targetTurnId"] == r == frozen["turnId"]
    assert frozen["placement"] == "current_turn" and frozen.get("settlement") is None
    parsed = parse_client_message_id(frozen["clientUserMessageId"])
    assert parsed["workerId"] == B and parsed["kind"] == "steer"
    assert len(seen) == 0
    assert trace.read_text().count("RPC turn/steer") == 1
    assert trace.read_text().count("RPC turn/start") == prior_starts
    assert store.record(A) == before_sibling

    reconciled = reconcile_turn(B, delivery_id="steer-R")
    assert reconciled["status"] == "accepted" and not reconciled["pending"]
    assert reconciled["route"] == "steer"
    assert reconciled["placement"] == "current_turn"
    assert reconciled["recommended_action"] == "none"

    auto_busy = send_turn(B, "auto-into-original-R", delivery_id="auto-busy", mode="auto")
    assert auto_busy["status"] == "accepted" and not auto_busy["settled"]
    routed = auto_busy["delivery"]
    assert (routed["route"], routed["target_turn_id"], routed["turn_id"]) == ("steer", r, r)
    assert parse_client_message_id(routed["client_user_message_id"])["kind"] == "steer"
    assert trace.read_text().count("RPC turn/steer") == 2
    assert trace.read_text().count("RPC turn/start") == prior_starts

    auto_idle = send_turn(A, "auto opens idle", delivery_id="auto-idle", mode="auto")
    opened = auto_idle["delivery"]
    assert auto_idle["status"] == "accepted"
    assert opened["route"] == "new_turn"
    assert opened["placement"] == "new_turn"
    assert opened["target_turn_id"] is None
    assert parse_client_message_id(opened["client_user_message_id"])["kind"] == "start"
    assert trace.read_text().count("RPC turn/start") == prior_starts+1

    # The native R can finish AFTER it accepted the steering instructions.
    # No steer receipt may inherit the opening turn's completed model reply.
    finalizer = send_turn(
        B, "finish-original-R", delivery_id="finish-R", mode="steer_only",
    )
    assert finalizer["status"] == "accepted" and not finalizer["settled"]
    after = reconcile_turn(B, delivery_id="steer-R")
    assert after["status"] == "accepted" and not after["pending"]
    assert after["delivery"]["settlement"] is None
    assert trace.read_text().count("RPC turn/steer") == 3
    assert trace.read_text().count("RPC turn/start") == prior_starts+1


def test_positive_refusal_no_fallback_and_strict_send_preserved(original_codex):
    store, trace = original_codex
    starts = trace.read_text().count("RPC turn/start")
    before = trace.read_text().count("RPC turn/steer")
    idle = send_turn(A, "cannot steer idle", delivery_id="steer-idle", mode="steer_only")
    assert idle["status"] == "rejected"
    assert idle["delivery"]["status"] == "rejected"
    assert store.read_receipt(A, "codex-send", "steer-idle")["state"] == "rejected"
    assert trace.read_text().count("RPC turn/start") == starts
    assert trace.read_text().count("RPC turn/steer") == before

    r = _hold_busy(store, trace)
    starts = trace.read_text().count("RPC turn/start")
    # An explicit native JSON-RPC rejection is definitive (even after the
    # existing local submitted marker), never re-steer a successor or start.
    refused = send_turn(
        B, "native-error-turn changed", delivery_id="steer-native-refused",
        mode="steer_only",
    )
    assert refused["status"] == "rejected"
    assert refused["delivery"]["route"] == "steer"
    assert refused["delivery"]["target_turn_id"] == r
    assert store.read_receipt(B, "codex-send", "steer-native-refused")["state"] == "rejected"
    assert trace.read_text().count("RPC turn/steer") == before+1
    assert trace.read_text().count("RPC turn/start") == starts

    # Strict new-turn keeps its idle-only native admission.
    from ccx.errors import DeliveryRejected
    with pytest.raises(DeliveryRejected):
        send_turn(B, "strict must not steer", delivery_id="strict-busy")
    assert trace.read_text().count("RPC turn/start") == starts
    assert trace.read_text().count("RPC turn/steer") == before+1


def test_daemon_strict_send_to_live_busy_original_is_typed_busy_without_effect(
    original_codex, monkeypatch,
):
    """Load saw idle, then the original turned busy: codexd's lock-held route
    decision is a definite pre-RPC refusal typed busy, never worker-state."""
    from ccx.ops import common as protocol_ops
    from ccx.codex.client import CodexClient
    from ccx.errors import DeliveryRejected
    from ccx.workers import WorkerClient

    store, trace = original_codex
    r = _hold_busy(store, trace)
    sibling = store.record(A)
    native = store.record(B)["native"]
    starts = trace.read_text().count("RPC turn/start")
    steers = trace.read_text().count("RPC turn/steer")
    # Skip only the caller-side idle load so the daemon decides under its lock.
    monkeypatch.setattr(ops_send, "ensure_ready", lambda *_a, **_k: None)
    for client, token in ((CodexClient(), "busy-codex-client"),
                          (WorkerClient(), "busy-worker-client")):
        with pytest.raises(DeliveryRejected) as refused:
            client.send(f"codex:{B}", "fresh while busy", delivery_id=token, wait=False)
        assert refused.value.refusal == "busy", vars(refused.value)
        assert refused.value.delivery_id == token
        assert refused.value.delivery_status == "rejected"
        assert codex_delivery.read_receipt(B, token)["rejectionCode"] == "busy"
        assert store.read_receipt(B, "codex-send", token)["state"] == "rejected"
    # A nearby non-busy refusal keeps its own worker-state code.
    idle = send_turn(A, "cannot steer idle", delivery_id="steer-idle-typed",
                     mode="steer_only")
    assert idle["status"] == "rejected"
    assert codex_delivery.read_receipt(A, "steer-idle-typed")["rejectionCode"] == "worker-state"
    assert trace.read_text().count("RPC turn/start") == starts
    assert trace.read_text().count("RPC turn/steer") == steers
    assert store.record(B)["lifecycle"]["activeTurnId"] == r
    assert store.record(B)["native"] == native
    assert store.record(A)["native"] == sibling["native"]


def test_same_token_lost_ack_never_replays_or_borrows_completion(original_codex):
    store, trace = original_codex
    r = _hold_busy(store, trace)
    token = "lost-steer-native-ACK"
    sent = []

    def lose_ack(handle, thread, text, delivery_id, budget):
        semantic = codex_delivery.read_receipt(handle, delivery_id)
        assert semantic["state"] == "prepared"
        assert semantic["clientUserMessageId"] is None
        effect = store.read_receipt(handle, "codex-send", delivery_id)
        assert effect["state"] == "submitted-unknown"
        result = daemon_rpc({
            "op": "native-send", "workerId": B, "threadId": thread,
            "text": text, "deliveryId": delivery_id,
            "clientUserMessageId": None,
            "rpcGeneration": constants.CODEXD_RPC_GENERATION,
        }, timeout=budget)
        assert result["accepted"] is True
        assert result["turnId"] == r and result["route"] == "steer"
        sent.append(result)
        raise TimeoutError("original native acceptance ACK lost after steer landed")

    with pytest.raises(SendIndeterminate) as lost:
        send_turn(B, "one steer maybe landed", delivery_id=token,
                  mode="steer_only", _native_submit=lose_ack)
    assert lost.value.delivery_id == token
    assert len(sent) == 1 and trace.read_text().count("RPC turn/steer") == 1
    assert codex_delivery.read_receipt(B, token)["state"] == "submitted"
    intent = store.read_receipt(B, "codex-send", token)
    assert intent["state"] == "submitted-unknown"
    from ccx.store.store import _digest
    owned = codex_delivery.read_receipt(B, token)
    assert owned["clientUserMessageId"] is not None
    assert codex_delivery.intent_spec(owned)["clientUserMessageId"] is None
    assert _digest(codex_delivery.intent_spec(owned)) == intent["intentSha256"]

    def no_replay(*args):
        pytest.fail("same submitted original token cannot call native again")

    with pytest.raises(SendIndeterminate) as again:
        send_turn(B, "one steer maybe landed", delivery_id=token,
                  mode="steer_only", _native_submit=no_replay)
    assert again.value.delivery_id == token
    assert trace.read_text().count("RPC turn/steer") == 1
    for changed, mode in (("changed text", "steer_only"),
                          ("one steer maybe landed", "auto")):
        with pytest.raises(OperationConflict):
            send_turn(B, changed, delivery_id=token, mode=mode,
                      _native_submit=no_replay)
    # Existing exact receipt observation is not a new model completion or
    # permission to submit once more.
    pending = reconcile_turn(B, delivery_id=token)
    assert pending["status"] == "submitted" and pending["pending"]
    assert trace.read_text().count("RPC turn/steer") == 1

def test_v1_auto_existing_op_and_codex_capabilities(original_codex):
    store, trace = original_codex
    from ccx.rpc.local_rpc import dispatch_full_uuid

    def call(operation: str, *, mode: str, delivery_id: str):
        return dispatch_full_uuid({
            "protocol_version": 2, "request_id": delivery_id,
            "op": operation, "params": {
                "target": f"codex:{B}", "text": "V1 busy Codex auto",
                "delivery_id": delivery_id, "mode": mode,
            },
        }, store)

    capabilities = dispatch_full_uuid({
        "protocol_version": 2, "request_id": "caps",
        "op": "system.info", "params": {},
    }, store)
    assert capabilities["ok"]
    assert set(capabilities["result"]) == {"protocol_version", "ccx_version"}
    assert capabilities["result"]["protocol_version"] == 2

    r = _hold_busy(store, trace)
    before = trace.read_text()
    rejected = call("turn.send", mode="steer_only", delivery_id="v1-internal-refuse")
    assert rejected["ok"] is False
    assert rejected["error"]["code"] == "invalid_request"
    assert "turn.steer" in rejected["error"]["message"]
    assert trace.read_text() == before

    delivered = call("turn.send", mode="auto", delivery_id="v1-native-auto")
    assert delivered["ok"], delivered
    value = delivered["result"]
    assert value["delivery"]["route"] == "steer"
    assert value["delivery"]["target_turn_id"] == r
    assert value["delivery"]["placement"] == "current_turn"
    assert value["delivery"]["settlement"] is None
    assert trace.read_text().count("RPC turn/steer") == 1
    assert trace.read_text().count("RPC turn/start") == 1
    assert codex_delivery.read_receipt(B, "v1-native-auto")["requestedMode"] == "auto"


def test_loss_before_daemon_route_reconciles_as_submitted_never_retry(original_codex):
    store, trace = original_codex
    _hold_busy(store, trace)
    token = "lost-before-native-route"
    starts = trace.read_text().count("RPC turn/start")
    attempts = []

    def lost_before_daemon(handle, thread, text, delivery_id, budget):
        attempts.append(delivery_id)
        raise TimeoutError("codexd admission reply lost before route selection")

    with pytest.raises(SendIndeterminate) as lost:
        send_turn(B, "maybe still queued", delivery_id=token, mode="auto",
                  _native_submit=lost_before_daemon)
    assert lost.value.delivery_id == token and attempts == [token]
    # Still prepared/unrouted, but the fsynced global intent advanced.
    receipt = codex_delivery.read_receipt(B, token)
    assert receipt["state"] == "prepared" and receipt["route"] is None
    assert store.read_receipt(B, "codex-send", token)["state"] == "submitted-unknown"

    value = reconcile_turn(B, delivery_id=token)
    assert value["status"] == "submitted" and value["pending"] is True
    assert value["recommended_action"] == "wait"
    assert value["delivery"]["recommended_action"] == "wait"
    assert value["delivery"]["status"] == "prepared"  # durable fact unchanged
    assert value["route"] is None and value["placement"] is None
    assert value["delivery"]["client_user_message_id"] is None

    def no_replay(*args):
        pytest.fail("same uncertain original token cannot call native again")

    with pytest.raises(SendIndeterminate) as again:
        send_turn(B, "maybe still queued", delivery_id=token, mode="auto",
                  _native_submit=no_replay)
    assert again.value.delivery_id == token
    assert codex_delivery.read_receipt(B, token) == receipt
    assert trace.read_text().count("RPC turn/steer") == 0
    assert trace.read_text().count("RPC turn/start") == starts
