"""Codex native refusal, uncertain delivery and original receipt proofs."""
from __future__ import annotations
import ccx.codex.daemon.native as daemon_native
import ccx.store.root as store_root
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")
from test_generation_store import short_root, A, B
from ccx import constants
from ccx.store.root import require_ccx_active
from ccx.store.worker_state import normalize_native_state


import hashlib
from contextlib import contextmanager
from test_codex_native_effects import (
    _fake_codex_executable, _f2_fake_processes, _f2_new, _f2_marker, _f2_until_candidate,
)
from test_codex_daemon_evidence import _r1_pending_original_codex

def test_r2_coded_native_rejection_same_uuid_replays_offline_original_error(
    short_root, tmp_path, monkeypatch,
):
    """Physical fake error response is sealed and replayed without codexd/bin."""
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.root import NativeActionDenied
    original_code = -32117
    original_message = "The original native thread was refused: café / retry forbidden"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        start_error_kind="coded", start_error_code=original_code,
        start_error_message=original_message,
    ) as (cli, _restart, trace, store, rpc):
        sibling = prepare_worker(
            store, engine="claude", cwd=str(tmp_path), worker_id=A,
        )
        assert A[:8] == B[:8]
        first = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
        assert first.returncode == 3, (first.stdout, first.stderr)
        result = json.loads(first.stdout)
        assert result["code"] == "native_rejected", (result, first.stderr)
        assert result["activation_state"] == "rejected"
        original = {"code": original_code, "message": original_message}
        assert result["native_rejection"] == original
        assert result["native_ready"] is False
        claim = store.native_activation(B)
        assert claim["state"] == "rejected"
        assert claim["nativeRejection"]["message"] == original_message
        marker = json.loads(_f2_marker(store, B).read_text())
        rejected = marker["nativeRejected"]
        assert rejected["code"] == original_code
        assert rejected["message"] == original_message
        assert rejected["requestId"] > 0
        assert "nativeCandidate" not in marker
        assert store.record(B)["native"]["threadId"] is None
        with pytest.raises(NativeActionDenied):
            store.authorize_native_action(B, "send")
        assert store.record(A)["native"]["sessionId"] == sibling.record["native"]["sessionId"]
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/read" not in trace.read_text()
        assert "RPC thread/list" not in trace.read_text()
        repeated = _f2_new(cli, tmp_path, B, "--start-timeout", "2")
        assert repeated.returncode == 3, (repeated.stdout, repeated.stderr)
        assert json.loads(repeated.stdout)["native_rejection"] == original
        assert trace.read_text().count("RPC thread/start") == 1
        # No provider binary and no daemon are necessary for the same owner
        # spec to receive its already sealed original refusal.
        stopped = rpc({"op": "daemon-stop"}, timeout=2)
        assert stopped["ok"] is True
        (tmp_path / "fake-codex").chmod(0o600)
        offline = _f2_new(cli, tmp_path, B, "--start-timeout", "1")
        assert offline.returncode == 3, (offline.stdout, offline.stderr)
        replay = json.loads(offline.stdout)
        assert replay["native_rejection"] == original
        assert replay["code"] == "native_rejected"
        assert replay["message"] == original_message
        assert replay["activation_state"] == "rejected"
        assert store.native_activation(B)["state"] == "rejected"
        assert trace.read_text().count("RPC thread/start") == 1
        bad_spec = _f2_new(cli, tmp_path, B, "--model", "different-model")
        assert bad_spec.returncode != 0
        assert trace.read_text().count("RPC thread/start") == 1


def test_r2_distinct_same_prefix_uuid_can_make_deliberate_new_creation(
    short_root, tmp_path, monkeypatch,
):
    """A new full UUID is independent; original rejected UUID is still sealed."""
    other = "aaaaaaaa-9876-4000-8000-000000000099"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        start_error_kind="coded", start_error_code=-32040,
        start_error_message="original deliberate provider refusal",
    ) as (cli, _restart, trace, store, _rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", "3")
        assert first.returncode == 3, first.stdout
        next_one = _f2_new(cli, tmp_path, other, "--start-timeout", "3")
        assert next_one.returncode == 3, next_one.stdout
        assert json.loads(next_one.stdout)["native_rejection"] == (
            json.loads(first.stdout)["native_rejection"]
        )
        assert store.native_activation(B)["state"] == "rejected"
        assert store.native_activation(other)["state"] == "rejected"
        assert store.record(B)["native"]["threadId"] is None
        assert store.record(other)["native"]["threadId"] is None
        assert store.native_activation(B)["submissionId"] != (
            store.native_activation(other)["submissionId"]
        )
        assert trace.read_text().count("RPC thread/start") == 2
        assert "RPC thread/list" not in trace.read_text()
        again = _f2_new(cli, tmp_path, B, "--start-timeout", "2")
        assert again.returncode == 3
        assert trace.read_text().count("RPC thread/start") == 2


@pytest.mark.parametrize("kind", ["uncoded", "reset"])
def test_r2_uncoded_provider_error_and_transport_reset_remain_unknown(
    short_root, tmp_path, monkeypatch, kind,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        start_error_kind=kind, start_error_message="no reliable provider code",
    ) as (cli, _restart, trace, store, _rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", "1")
        assert first.returncode == 3, (first.stdout, first.stderr)
        result = json.loads(first.stdout)
        assert result["activation_state"] == "submitted-unknown"
        assert result["code"] == "partial_failure"
        assert result.get("native_rejection") is None
        assert store.native_activation(B)["state"] == "submitted-unknown"
        assert store.record(B)["native"]["threadId"] is None
        assert "nativeRejected" not in json.loads(_f2_marker(store, B).read_text())
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()


def _r2_answer_original_rejection(client, index=0, *, code=-32042,
                                  message="Codex native start denied"):
    """Physical PendingRequest original JSON-RPC reply with the matching id."""
    pending = client.pending[index]
    pending.put({
        "jsonrpc": "2.0", "id": pending.request_id,
        "error": {"code": code, "message": message},
    })


def test_r2_error_receipt_marker_saved_then_claim_crash_offline_reconciles(
    short_root, tmp_path, monkeypatch,
):
    """One fsynced provider refusal is enough after a crash between writes."""
    import errno
    from ccx.store.root import NativeActionDenied
    from ccx.ops.creation_steps import prepare_worker
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    original = admit(B)
    old_write = store._write
    failed = []

    def inject_claim_failure(parts, name, payload):
        activation = payload.get("nativeActivation") if isinstance(payload, dict) else None
        if parts == ("claims",) and isinstance(activation, dict) and (
            activation.get("state") == "rejected"
        ) and not failed:
            failed.append(True)
            raise OSError(errno.EIO, "simulated crash after rejection marker fsync")
        return old_write(parts, name, payload)

    monkeypatch.setattr(store, "_write", inject_claim_failure)
    _r2_answer_original_rejection(client, code=-32142,
                                  message="original coded error survives process exit")
    assert failed
    assert store.native_activation(B)["state"] == "submitted-unknown"
    marker = json.loads(_f2_marker(store, B).read_text())
    assert marker["nativeRejected"]["code"] == -32142
    assert "nativeCandidate" not in marker
    assert len(daemon._native_start_held) == 1  # original refused reply held for retry
    assert client.reads == [] and len(client.native_writes) == 1
    # Simulate a different daemon/CLI process: no map, no app-server. The
    # generation store binds the *original* marker directly to its claim.
    restored = require_ccx_active(short_root)
    expected = restored.reconcile_codex_native_rejection(original.worker)
    assert expected["rejected"] and expected["code"] == -32142
    assert expected["message"] == "original coded error survives process exit"
    assert restored.native_activation(B)["state"] == "rejected"
    assert restored.reconcile_codex_native_rejection(B) == expected
    assert restored.record(B)["native"]["threadId"] is None
    with pytest.raises(NativeActionDenied):
        restored.authorize_native_action(B, "send")
    assert len(client.native_writes) == 1


def test_r2_transient_rejection_marker_write_retained_until_fsync_and_claim(
    short_root, tmp_path, monkeypatch,
):
    """R1 retry preserves a definite provider *error* as well as success."""
    import errno
    import ccx.store.store as generation
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    real_write = store_root._write_json
    errors = []

    def transient_marker_failure(fd, name, value):
        if "nativeRejected" in value and len(errors) < 2:
            errors.append(name)
            raise OSError(errno.ENOSPC, "temporary native rejection receipt ENOSPC")
        return real_write(fd, name, value)

    monkeypatch.setattr(store_root, "_write_json", transient_marker_failure)
    _r2_answer_original_rejection(client, message="coded proof needs two disk retries")
    assert len(errors) == 1
    assert store.native_activation(B)["state"] == "submitted-unknown"
    held = next(iter(daemon._native_start_held.values()))
    assert held["rejection"]["code"] == -32042
    assert held["candidate"] is None and not held["unsafe"]
    assert "nativeRejected" not in json.loads(_f2_marker(store, B).read_text())
    assert len(client.reads) == 0 and len(client.native_writes) == 1
    still = status(B)
    assert still["candidatePersistence"] == "transient"
    assert len(errors) == 2 and client.reads == []
    outcome = status(B)
    assert outcome["rejected"] is True and outcome["code"] == -32042
    assert outcome["message"] == "coded proof needs two disk retries"
    assert store.native_activation(B)["state"] == "rejected"
    assert not daemon._native_start_held
    assert json.loads(_f2_marker(store, B).read_text())["nativeRejected"]["code"] == -32042
    assert len(client.native_writes) == 1 and client.reads == []


@pytest.mark.parametrize("kind", [
    "local-protocol-coded", "transport-coded", "not-sent-coded",
    "mismatched-id", "no-code", "boolean-code",
])
def test_r2_untrusted_numeric_local_errors_and_unmatched_native_replies_unknown(
    short_root, tmp_path, monkeypatch, kind,
):
    """A numeric errno or exception code never proves provider refusal."""
    from ccx.codex.protocol import ProtocolError, ProtocolTransportError, ProtocolNotSent
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    pending = client.pending[0]
    if kind == "local-protocol-coded":
        pending.put(ProtocolError("local exception", code=-32101,
                                  request_id=pending.request_id))
    elif kind == "transport-coded":
        pending.put(ProtocolTransportError("broken connection", code=-32101,
                                           request_id=pending.request_id))
    elif kind == "not-sent-coded":
        pending.put(ProtocolNotSent("no request bytes sent", code=-32101,
                                    request_id=pending.request_id))
    else:
        error = {"message": "not an attributable coded rejection"}
        if kind == "mismatched-id":
            error["code"] = -32101
        elif kind == "boolean-code":
            error["code"] = True
        pending.put({"jsonrpc": "2.0",
                     "id": pending.request_id + 99 if kind == "mismatched-id" else pending.request_id,
                     "error": error})
    assert pending.original_provider_rejection() is None
    assert store.native_activation(B)["state"] == "submitted-unknown"
    assert "nativeRejected" not in json.loads(_f2_marker(store, B).read_text())
    assert "nativeCandidate" not in json.loads(_f2_marker(store, B).read_text())
    assert status(B)["status"] == "indeterminate"
    assert len(client.native_writes) == 1 and len(client.reads) == 0


@pytest.mark.parametrize("field", [
    "token", "source", "code", "message", "messageSha256", "candidate", "claim",
])
def test_r2_tampered_or_contradictory_terminal_rejection_refuses_replay(
    short_root, tmp_path, monkeypatch, field,
):
    from ccx.store.root import CreationConflict, StorageIntegrityError
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    _r2_answer_original_rejection(client, code=-32199,
                                  message="immutable native rejection")
    assert store.native_activation(B)["state"] == "rejected"
    marker_path = _f2_marker(store, B)
    marker = json.loads(marker_path.read_text())
    if field == "claim":
        path = store.pointer.generation_root / "claims" / f"{B}.json"
        claim = json.loads(path.read_text())
        claim["nativeActivation"]["nativeRejection"]["message"] = "forged replacement"
        path.write_text(json.dumps(claim, sort_keys=True))
    else:
        if field == "token":
            marker["token"] = "forged-native-submission"
        elif field == "source":
            marker["source"] = f"ccx-worker:{A}"
        elif field == "code":
            marker["nativeRejected"]["code"] = -100
        elif field == "message":
            marker["nativeRejected"]["message"] = "replacement content"
        elif field == "messageSha256":
            marker["nativeRejected"]["messageSha256"] = "0" * 64
        elif field == "candidate":
            marker["nativeCandidate"] = {
                "threadId": "false accepted thread", "submittedSource": f"ccx-worker:{B}",
            }
        marker_path.write_text(json.dumps(marker, sort_keys=True))
    with pytest.raises((CreationConflict, StorageIntegrityError)):
        store.reconcile_codex_native_rejection(B)
    assert len(client.native_writes) == 1
    assert len(client.reads) == 0
    assert store.record(B)["native"]["threadId"] is None


def test_r2_rejected_claim_never_confirms_or_restarts_native_start(
    short_root, tmp_path, monkeypatch,
):
    from ccx.store.root import CreationConflict, CreationIndeterminate
    from ccx.store.store import NativeActivation
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    prepared = admit(B)
    token = store.native_activation(B)["submissionId"]
    _r2_answer_original_rejection(client)
    assert store.native_activation(B)["state"] == "rejected"
    original = NativeActivation(B, "codex", f"ccx-worker:{B}", token,
                                "submitted-unknown", store.generation)
    with pytest.raises(CreationConflict, match="terminal rejected"):
        store.confirm_native_activation(original, native_evidence_sha256="a" * 64,
                                        native_binding={"threadId": "fake-accepted"})
    with pytest.raises(CreationIndeterminate):
        store.prepare_native_activation(store.reserve_creation(
            B, engine="codex", fingerprint=prepared.record["createSha256"],
            native_binding={},
        ), native_source=prepared.native_source)
    assert len(client.native_writes) == 1
    assert status(B)["rejected"] is True


def test_r2_coded_provider_refusal_after_lost_cli_reply_replays_offline(
    short_root, tmp_path, monkeypatch,
):
    """One original slow coded response survives CLI exit and daemon death."""
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=3.0,
        start_error_kind="coded", start_error_code=-32078,
        start_error_message="original slow error received only by native daemon",
    ) as (cli, _restart, trace, store, rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", ".1")
        assert first.returncode == 3, (first.stdout, first.stderr)
        assert json.loads(first.stdout)["activation_state"] == "submitted-unknown"
        assert trace.read_text().count("RPC thread/start") == 1
        until = time.monotonic() + 7
        while time.monotonic() < until:
            if store.native_activation(B)["state"] == "rejected":
                break
            time.sleep(.05)
        assert store.native_activation(B)["state"] == "rejected"
        rejected = json.loads(_f2_marker(store, B).read_text())["nativeRejected"]
        assert rejected["code"] == -32078
        assert rejected["message"] == "original slow error received only by native daemon"
        assert "nativeCandidate" not in json.loads(_f2_marker(store, B).read_text())
        # The durable rejection can be published before the daemon has
        # released the corresponding native-start slot. A normal stop must
        # still refuse while it is held; wait for the real count to drain.
        until = time.monotonic() + 5
        while True:
            evidence = rpc({"op": "daemon-status"}, timeout=2)["nativeStartEvidence"]
            if evidence["pending"] + evidence["held"] == 0:
                break
            assert time.monotonic() < until, evidence
            time.sleep(.05)
        assert rpc({"op": "daemon-stop"}, timeout=2)["ok"]
        (tmp_path / "fake-codex").chmod(0o600)
        repeated = _f2_new(cli, tmp_path, B, "--start-timeout", "1")
        assert repeated.returncode == 3, (repeated.stdout, repeated.stderr)
        replay = json.loads(repeated.stdout)
        assert replay["native_rejection"] == {
            "code": -32078,
            "message": "original slow error received only by native daemon",
        }
        assert replay["activation_state"] == "rejected"
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()


def test_r2_unsealed_rejection_marker_tamper_never_mints_terminal_claim(
    short_root, tmp_path, monkeypatch,
):
    """Corrupting a durable rejected reply before claim CAS fails closed."""
    import errno
    from ccx.store.root import StorageIntegrityError
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    original_write = store._write
    blocked = [True]

    def fail_claim_once(parts, filename, payload):
        activation = payload.get("nativeActivation") if isinstance(payload, dict) else None
        if parts == ("claims",) and isinstance(activation, dict) and (
            activation.get("state") == "rejected" and blocked[0]
        ):
            blocked[0] = False
            raise OSError(errno.EIO, "crash between error marker and claim")
        return original_write(parts, filename, payload)

    monkeypatch.setattr(store, "_write", fail_claim_once)
    _r2_answer_original_rejection(client, code=-32129,
                                  message="original provider verdict before tampering")
    assert store.native_activation(B)["state"] == "submitted-unknown"
    path = _f2_marker(store, B)
    marker = json.loads(path.read_text())
    marker["nativeRejected"]["code"] = -32130  # digest no longer matches original
    path.write_text(json.dumps(marker, sort_keys=True))
    with pytest.raises(StorageIntegrityError, match="malformed or contradictory"):
        require_ccx_active(short_root).reconcile_codex_native_rejection(B)
    assert store.native_activation(B)["state"] == "submitted-unknown"
    assert store.record(B)["native"]["threadId"] is None
    assert len(client.native_writes) == 1 and client.reads == []


def _l3_ready_original(store, worker_id, cwd):
    """Original exact first Codex claim; no provider thread and no remint."""
    from ccx.ops.creation_steps import prepare_worker
    prepared = prepare_worker(store, engine="codex", cwd=str(cwd), worker_id=worker_id)
    reserve = store.reserve_creation(
        worker_id, engine="codex", fingerprint=prepared.record["createSha256"],
        native_binding={},
    )
    act = store.prepare_native_activation(reserve, native_source=prepared.native_source)
    return prepared, act


def _l3_original_dispatch(store, daemon, prepared, activation):
    return store.native_activation_dispatch(
        activation, dispatch_budget_s=2.0,
        bounded_submit=lambda _budget: daemon._submit_original_codex_thread(
            prepared.worker_id, prepared.record, prepared.native_source,
            activation.submission_id,
        ),
    )


def test_l3_not_ready_pre_native_refusal_durably_replays_original_without_provider(
    short_root, tmp_path, monkeypatch,
):
    from ccx.store.root import NativeActionDenied, CreationConflict, CreationIndeterminate
    store, daemon, client, _admit, _answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    prepared, activation = _l3_ready_original(store, B, tmp_path)
    daemon.ready.clear()
    refusal = _l3_original_dispatch(store, daemon, prepared, activation)
    assert refusal["ok"] is False, refusal
    assert refusal["localRefusalReason"] == "Codex app-server not ready"
    assert refusal["localRefusalProven"] is True
    assert client.native_writes == []
    exact_marker = json.loads(_f2_marker(store, B).read_text())
    assert exact_marker["workerId"] == B
    assert exact_marker["generation"] == store.generation
    assert exact_marker["token"] == activation.submission_id
    assert exact_marker["source"] == f"ccx-worker:{B}"
    assert exact_marker["createSha256"] == prepared.record["createSha256"]
    assert exact_marker["nativeNotSubmitted"]["nativeRequestWritten"] is False
    assert exact_marker["nativeNotSubmitted"]["reason"] == refusal["localRefusalReason"]
    assert "nativeCandidate" not in exact_marker and "nativeRejected" not in exact_marker
    assert store.native_activation(B)["state"] == "submitted-unknown" or (
        store.native_activation(B)["state"] == "local-refused"
    )
    independent = require_ccx_active(short_root)
    sealed = independent.reconcile_codex_local_refusal(B)
    assert sealed["localRefused"] is True and sealed["reason"] == "Codex app-server not ready"
    assert independent.native_activation(B)["state"] == "local-refused"
    assert independent.reconcile_codex_local_refusal(B) == sealed
    assert status(B)["localRefused"] is True
    with pytest.raises(NativeActionDenied):
        independent.authorize_native_action(B, "send")
    with pytest.raises(CreationConflict, match="terminal"):
        independent.confirm_native_activation(
            activation, native_evidence_sha256="a" * 64,
            native_binding={"threadId": "forged-native"},
        )
    with pytest.raises(CreationIndeterminate):
        independent.prepare_native_activation(
            independent.reserve_creation(
                B, engine="codex", fingerprint=prepared.record["createSha256"],
                native_binding={},
            ), native_source=prepared.native_source,
        )
    assert client.native_writes == []


def test_l3_capacity_local_refusal_preserves_another_original_pending(
    short_root, tmp_path, monkeypatch,
):
    import ccx.codex.daemon.daemon as module
    from test_generation_store import C
    monkeypatch.setattr(daemon_native, "_CODEX_NATIVE_START_HELD_LIMIT", 1)
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    first = admit(B)
    prepared, activation = _l3_ready_original(store, C, tmp_path)
    response = _l3_original_dispatch(store, daemon, prepared, activation)
    assert response["ok"] is False and response["localRefusalProven"] is True
    assert "retention full" in response["localRefusalReason"]
    assert len(client.native_writes) == 1  # B only; C did not reach app-server
    refused = store.reconcile_codex_local_refusal(C)
    assert refused["stage"] == "capacity"
    assert store.native_activation(C)["state"] == "local-refused"
    assert len(daemon._native_start_held) == 1  # B's unresolved original intact
    answer(0, B, "legitimate-pending-sibling-native")
    assert status(B)["accepted"] is True
    assert len(client.native_writes) == 1
    assert store.reconcile_codex_local_refusal(C) == refused
    assert store.record(C)["native"]["threadId"] is None
    deliberate = "bbbbbbbb-9876-4000-8000-000000000099"  # C's SAME prefix
    prepared2, activation2 = _l3_ready_original(store, deliberate, tmp_path)
    accepted2 = _l3_original_dispatch(store, daemon, prepared2, activation2)
    assert accepted2["ok"] and accepted2["queued"]
    assert len(client.native_writes) == 2
    answer(1, deliberate, "native-distinct-new-uuid")
    assert status(deliberate)["accepted"] is True
    assert store.native_activation(C)["state"] == "local-refused"


def test_l3_local_refusal_marker_to_claim_crash_reconciles_offline(
    short_root, tmp_path, monkeypatch,
):
    import errno
    store, daemon, client, _admit, _answer, _status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    prepared, act = _l3_ready_original(store, B, tmp_path)
    daemon.ready.clear()
    original_write = store._write
    faulted = []

    def claim_crash(parts, name, value):
        activation = value.get("nativeActivation") if isinstance(value, dict) else None
        if parts == ("claims",) and isinstance(activation, dict) and (
            activation.get("state") == "local-refused" and not faulted
        ):
            faulted.append(name)
            raise OSError(errno.EIO, "crash after original local refusal marker fsync")
        return original_write(parts, name, value)

    monkeypatch.setattr(store, "_write", claim_crash)
    # The CLI has already fsynced submitted-unknown; now the independent
    # daemon handler can attempt the marker -> terminal claim write, rather
    # than contending with the still-held original CLI claim lock.
    store.native_activation_dispatch(
        act, bounded_submit=lambda _budget: {"ok": False, "error": "uncertain admission"},
    )
    result = daemon._submit_original_codex_thread(
        B, prepared.record, prepared.native_source, act.submission_id,
    )
    assert result["localRefusalProven"] and faulted
    assert store.native_activation(B)["state"] == "submitted-unknown"
    marker = json.loads(_f2_marker(store, B).read_text())
    assert marker["nativeNotSubmitted"]["stage"] == "not-ready"
    assert client.native_writes == []
    restored = require_ccx_active(short_root)  # fresh process-equivalent store
    sealed = restored.reconcile_codex_local_refusal(B)
    assert sealed["localRefused"] and sealed["reason"] == result["localRefusalReason"]
    assert restored.native_activation(B)["state"] == "local-refused"
    assert restored.reconcile_codex_local_refusal(B) == sealed
    assert client.native_writes == []


def test_l3_local_refusal_marker_fsync_failure_is_only_provisional_unknown(
    short_root, tmp_path, monkeypatch,
):
    import errno
    import ccx.store.store as generation
    store, daemon, client, _admit, _answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    prepared, act = _l3_ready_original(store, B, tmp_path)
    daemon.ready.clear()
    real_write = store_root._write_json
    failures = []

    def fail_only_local_proof(fd, name, value):
        if value.get("nativeNotSubmitted") is not None:
            failures.append(name)
            raise OSError(errno.ENOSPC, "disk full while writing L3 proof")
        return real_write(fd, name, value)

    monkeypatch.setattr(store_root, "_write_json", fail_only_local_proof)
    response = _l3_original_dispatch(store, daemon, prepared, act)
    assert response["localRefusalProven"] is False and failures
    assert response["localRefusalReason"] == "Codex app-server not ready"
    assert "proof unavailable" in response["error"]
    assert store.native_activation(B)["state"] == "submitted-unknown"
    assert not _f2_marker(store, B).exists()
    assert store.reconcile_codex_local_refusal(B) is None
    assert status(B)["status"] == "indeterminate"
    assert client.native_writes == []
    # Merely releasing disk pressure or finding no marker NEVER re-admits B.
    monkeypatch.setattr(store_root, "_write_json", real_write)
    assert store.reconcile_codex_local_refusal(B) is None


def test_l3_concurrent_same_id_pre_ipc_refusal_cannot_create_two_markers(
    short_root, tmp_path, monkeypatch,
):
    import threading
    store, daemon, client, _admit, _answer, _status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    prepared, act = _l3_ready_original(store, B, tmp_path)
    daemon.ready.clear()
    # Exact one-shot claim is already submitted; emulate two competing daemon
    # socket handlers, not two native dispatches or two claim transitions.
    store.native_activation_dispatch(
        act, bounded_submit=lambda _t: {"ok": False, "error": "pending"},
    )
    results = []
    errors = []
    barrier = threading.Barrier(2)

    def refuse_once():
        try:
            barrier.wait(timeout=4)
            results.append(daemon._submit_original_codex_thread(
                B, prepared.record, prepared.native_source, act.submission_id,
            ))
        except Exception as error:
            errors.append(error)

    calls = [threading.Thread(target=refuse_once, daemon=True) for _ in range(2)]
    for worker in calls: worker.start()
    for worker in calls: worker.join(timeout=8)
    assert not any(worker.is_alive() for worker in calls), "local refusal deadlock"
    assert not errors and len(results) == 2, errors
    assert sum(item["localRefusalProven"] for item in results) == 1, results
    assert len(list(_f2_marker(store, B).parent.glob("*.json"))) == 1
    assert store.reconcile_codex_local_refusal(B)["localRefused"]
    assert store.native_activation(B)["state"] == "local-refused"
    assert client.native_writes == []


@pytest.mark.parametrize("field", [
    "token", "source", "generation", "digest", "reason", "reason-sha",
    "proof-digest", "candidate", "rejected", "claim",
])
def test_l3_tampered_or_contradictory_original_proof_refused(
    short_root, tmp_path, monkeypatch, field,
):
    from ccx.store.root import CreationConflict, StorageIntegrityError, CreationIndeterminate
    store, daemon, client, _admit, _answer, _status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    prepared, act = _l3_ready_original(store, B, tmp_path)
    daemon.ready.clear()
    result = _l3_original_dispatch(store, daemon, prepared, act)
    assert result["localRefusalProven"]
    assert store.reconcile_codex_local_refusal(B)["localRefused"]
    marker_path = _f2_marker(store, B)
    marker = json.loads(marker_path.read_text())
    if field == "claim":
        claim_path = store.pointer.generation_root / "claims" / (B + ".json")
        claim = json.loads(claim_path.read_text())
        claim["nativeActivation"]["nativeNotSubmitted"]["reason"] = "forged"
        claim_path.write_text(json.dumps(claim, sort_keys=True))
    else:
        if field == "token": marker["token"] = "forged-token"
        if field == "source": marker["source"] = f"ccx-worker:{A}"
        if field == "generation": marker["generation"] = "g-" + "f" * 32
        if field == "digest": marker["createSha256"] = "0" * 64
        if field == "reason": marker["nativeNotSubmitted"]["reason"] = "forged"
        if field == "reason-sha": marker["nativeNotSubmitted"]["reasonSha256"] = "0" * 64
        if field == "proof-digest": marker["nativeNotSubmitted"]["localEvidenceSha256"] = "0" * 64
        if field == "candidate": marker["nativeCandidate"] = {"threadId": "forged"}
        if field == "rejected": marker["nativeRejected"] = {"code": -32001}
        marker_path.write_text(json.dumps(marker, sort_keys=True))
    with pytest.raises((CreationConflict, StorageIntegrityError, CreationIndeterminate)):
        store.reconcile_codex_local_refusal(B)
    assert client.native_writes == [] and store.record(B)["native"]["threadId"] is None


def test_l3_physical_real_daemon_capacity_refusal_replays_offline_no_b_start(
    short_root, tmp_path, monkeypatch,
):
    """The 17th original native create never writes to the real fake provider."""
    from ccx.ops.creation_steps import prepare_worker
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, no_start_reply=True,
    ) as (cli, _restart, trace, store, rpc):
        prior = prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
        for index in range(16):
            wid = f"{index + 1:08x}-0000-4000-8000-{index + 1:012x}"
            attempt = _f2_new(cli, tmp_path, wid, "--start-timeout", ".1")
            assert attempt.returncode == 3, (index, attempt.stdout, attempt.stderr)
            assert json.loads(attempt.stdout)["activation_state"] == "submitted-unknown"
        counts = rpc({"op": "daemon-status"}, timeout=2)["nativeStartEvidence"]
        assert counts["pending"] == 16 and counts["capacity"] == 16
        assert trace.read_text().count("RPC thread/start") == 16
        rejected = _f2_new(cli, tmp_path, B, "--start-timeout", "1")
        assert rejected.returncode == 3, (rejected.stdout, rejected.stderr)
        outcome = json.loads(rejected.stdout)
        assert outcome["code"] == "local_refused", outcome
        assert outcome["activation_state"] == "local-refused"
        assert outcome["native_local_refusal"]["proven"] is True
        assert "retention full" in outcome["native_local_refusal"]["reason"]
        assert trace.read_text().count("RPC thread/start") == 16
        assert store.record(B)["native"]["threadId"] is None
        assert store.native_activation(B)["state"] == "local-refused"
        assert store.resolve(A).engine == "claude"
        same = _f2_new(cli, tmp_path, B, "--start-timeout", "1")
        assert json.loads(same.stdout)["native_local_refusal"] == (
            outcome["native_local_refusal"]
        )
        assert trace.read_text().count("RPC thread/start") == 16
        # P1: 16 unanswered originals refuse a normal stop; force it here.
        refused = rpc({"op": "daemon-stop"}, timeout=2)
        assert refused["ok"] is False
        assert refused["nativeStartEvidence"]["pending"] == 16
        assert rpc({"op": "daemon-stop", "force": True}, timeout=2)["ok"] is True
        # No running daemon OR provider binary is needed to replay exact UUID.
        (tmp_path / "fake-codex").chmod(0o600)
        offline = _f2_new(cli, tmp_path, B, "--start-timeout", "1")
        assert offline.returncode == 3, (offline.stdout, offline.stderr)
        assert json.loads(offline.stdout)["native_local_refusal"] == (
            outcome["native_local_refusal"]
        )
        changed = _f2_new(cli, tmp_path, B, "--model", "different")
        assert changed.returncode != 0
        assert trace.read_text().count("RPC thread/start") == 16
        # Deliberately new same-prefix UUID after capacity frees is covered
        # by the isolated-claim test above; no daemon restart is needed here.


def test_l3_real_s3_exact_zero_byte_local_proof_and_no_native_wire(
    short_root, tmp_path, monkeypatch,
):
    """Only genuine AppServerClient ProtocolNotSent may prove a native no-write."""
    import ccx.store.store as generation
    import ccx.codex.daemon.daemon as codexd
    import ccx.codex.protocol as codex_protocol
    from ccx.codex.daemon.daemon import CodexDaemon

    # The native test needs a precise deadline jump AFTER its original marker
    # fsync; a physical .18s sleep with .2s budget can race host CPU scheduling
    # and expire before begin_request is even admitted. Mirror the existing F7
    # S3 admission-clock fixture, preserving the actual AppServerClient writer.
    real_time = time

    class AdmissionClock:
        advance_s = 0.0

        def monotonic(self):
            return real_time.monotonic() + self.advance_s

        def __getattr__(self, name):
            return getattr(real_time, name)

    clock = AdmissionClock()
    for module in (generation, codexd, codex_protocol):
        monkeypatch.setattr(module, "time", clock)
    from test_codex_paging_transport_faults import _fake_client, _S3_HEALTHY_PROVIDER

    # The original shell-owner preflight reads constants independently from
    # daemon._uuid_store. Both must name the same sealed ACTIVE test root.
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setenv("CCX_ROOT", str(short_root))
    with _fake_client(tmp_path, _S3_HEALTHY_PROVIDER) as client:
        store = require_ccx_active(short_root)
        prepared, act = _l3_ready_original(store, B, tmp_path)
        daemon = CodexDaemon()
        daemon._uuid_store = store
        daemon.client = client
        daemon.ready.set()
        real_write = store_root._write_json
        delayed = []

        def expensive_original_fence(fd, name, value):
            real_write(fd, name, value)
            if (value.get("daemonInstanceId") == daemon.instance_id
                    and value.get("kind") == "activate"
                    and value.get("nativeNotSubmitted") is None and not delayed):
                delayed.append(True)
                # Original marker is already fsynced and no native byte sent.
                # Expire the same .4s budget at the genuine transport writer,
                # without a sleep, a retry, or replacing AppServerClient.
                clock.advance_s += .55

        monkeypatch.setattr(store_root, "_write_json", expensive_original_fence)
        result = store.native_activation_dispatch(
            act, dispatch_budget_s=.4,
            bounded_submit=lambda budget: daemon._submit_original_codex_thread(
                B, prepared.record, prepared.native_source, act.submission_id,
                admission_deadline=time.monotonic() + budget,
            ),
        )
        assert delayed == [True] and clock.advance_s == .55
        assert result["localRefusalProven"] is True, result
        assert "not submitted" in result["localRefusalReason"]
        marker = json.loads(_f2_marker(store, B).read_text())
        assert marker["nativeNotSubmitted"]["stage"] == "transport-zero-bytes"
        assert marker["nativeNotSubmitted"]["nativeRequestWritten"] is False
        assert "nativeCandidate" not in marker and "nativeRejected" not in marker
        assert store.reconcile_codex_local_refusal(B)["stage"] == "transport-zero-bytes"
        assert store.native_activation(B)["state"] == "local-refused"
        assert client.failure is None
        assert client.request("ping", timeout=2)["ok"] is True


@pytest.mark.parametrize("local_exception", ["protocol", "transport", "fake-not-sent"])
def test_l3_numeric_local_error_or_unattributed_fake_zero_bytes_stays_unknown(
    short_root, tmp_path, monkeypatch, local_exception,
):
    from ccx.codex.protocol import ProtocolError, ProtocolTransportError, ProtocolNotSent
    from ccx.store.root import NativeEffectIndeterminate
    store, daemon, client, _admit, _answer, _status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    prepared, act = _l3_ready_original(store, B, tmp_path)

    def local_failure(method, params, *, write_deadline=None):
        if local_exception == "protocol":
            raise ProtocolError("local errno is not provider reply", code=-32603, method=method)
        if local_exception == "transport":
            raise ProtocolTransportError("native write might be partial", code=-32603,
                                         method=method)
        raise ProtocolNotSent("not a real AppServerClient", code=-32603, method=method)

    monkeypatch.setattr(client, "begin_request", local_failure)
    with pytest.raises(NativeEffectIndeterminate):
        _l3_original_dispatch(store, daemon, prepared, act)
    marker = json.loads(_f2_marker(store, B).read_text())
    assert marker["state"] == "submitted-unknown"
    assert "nativeNotSubmitted" not in marker
    assert "nativeRejected" not in marker and "nativeCandidate" not in marker
    assert store.native_activation(B)["state"] == "submitted-unknown"
    assert store.reconcile_codex_local_refusal(B) is None


def test_l3_cli_retains_provisional_daemon_reason_without_durable_proof(
    short_root, tmp_path, monkeypatch, capsys,
):
    """Provisional ENOSPC/refusal never becomes a falsely terminal claim."""
    from ccx.cli.main import main
    from ccx.codex import runtime as codex_runtime
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    fake_binary = tmp_path / "fake-codex-available"
    fake_binary.write_text("#!/bin/sh\nexit 0\n")
    fake_binary.chmod(0o700)
    monkeypatch.setenv("CCX_CODEX_BIN", str(fake_binary))
    monkeypatch.setattr(codex_runtime, "ensure_daemon", lambda **kwargs: {"ok": True})
    calls = []

    def served_provisional_refusal(message, timeout=2):
        calls.append(dict(message))
        if message.get("op") != "native-activate":
            raise AssertionError("no native status/secondary request after provisional refusal")
        return {
            "ok": False, "transport": "served", "error": (
                "Codex native start retention full; new provider submission refused; "
                "original not-submitted proof unavailable: OSError ENOSPC"
            ),
            "localRefusalReason": (
                "Codex native start retention full; new provider submission refused"
            ),
            "localRefusalProven": False,
        }

    monkeypatch.setattr(codex_runtime, "daemon_rpc", served_provisional_refusal)
    with pytest.raises(SystemExit) as outcome:
        main(["new", "--engine", "codex", "--cwd", str(tmp_path), "--worker-id", B,
              "--start-timeout", "1", "--json"])
    assert outcome.value.code == 3
    data = json.loads(capsys.readouterr().out)
    assert data["code"] == "partial_failure", data
    assert data["activation_state"] == "submitted-unknown"
    assert data["native_local_refusal"]["proven"] is False
    assert "retention full" in data["native_local_refusal"]["reason"]
    assert "ENOSPC" in data["native_start_error"]
    assert len(calls) == 1 and calls[0]["op"] == "native-activate"
    store = require_ccx_active(short_root)
    assert store.native_activation(B)["state"] == "submitted-unknown"
    assert store.reconcile_codex_local_refusal(B) is None
    assert not _f2_marker(store, B).exists()
