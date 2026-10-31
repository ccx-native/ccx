"""Codex C4A Python/V1 adapters; real disposable fake app-server C3 evidence.

This fixture uses provider-shaped local native events, not a real Codex provider.
Codex CLI compact remains gated for separately authorized C4B.
"""
from __future__ import annotations

from dataclasses import FrozenInstanceError
import inspect
from uuid import UUID
import time

import pytest

from ccx import CodexClient
from ccx import WorkerClient
from ccx.codex import ops as codex_ops
from ccx.codex import runtime as codex_runtime
from ccx.codex.types import CodexCompactResult
from ccx.errors import (
    CompactFailed, CompactIndeterminate, DeliveryRejected,
)
from ccx.rpc.local_rpc import dispatch_full_uuid
from test_generation_store import A, B, short_root
from test_compact_codex import (
    real_compact_host, _physical_count, _settled,
)
from test_v1_worker_protocol import request as v1_request


def test_python_codex_worker_no_wait_typed_settled_and_recorded_context(
    real_compact_host, monkeypatch,
):
    store, trace, gate = real_compact_host
    signature = inspect.signature(CodexClient.compact)
    assert set(signature.parameters) == {"self", "target", "timeout"}
    assert signature.parameters["timeout"].default == 300
    assert CodexCompactResult.__dataclass_fields__["compacted"].default is False

    worker = WorkerClient()
    typed_client = CodexClient()
    assert "compact" not in worker.capabilities("codex")
    assert "compact" not in worker.capabilities("claude")

    with pytest.raises(TypeError, match="operation_id"):
        typed_client.compact(B, operation_id="caller-supplied")
    missing = worker.compact_status(B, "any-internal-id")
    assert missing["operation_id"] == "any-internal-id"
    assert missing["operation_status"] == "not_found"
    assert not missing["compacted"]
    assert missing["worker"] == f"codex:{B}"
    assert _physical_count(trace) == 0
    assert store.read_receipt(B, "codex-compact", "any-internal-id") is None

    store.mutate_record(B, patches={"context": {
        "state": "idle", "tokens_used": 1234, "window_size": 8192,
        "percent_used": 15.1, "as_of_turn_ref": None,
        "as_of_ms": 1700000000000,
    }})
    assert worker.context(B)["tokens_used"] == 1234
    seen = []
    original_context = worker.context

    def read_once(target):
        seen.append(target)
        return original_context(target)

    monkeypatch.setattr(worker, "context", read_once)
    pending = worker.compact(B, wait=False, timeout=5)
    token = pending["operation_id"]
    UUID(token)  # Codex public caller generated a real internal identity.
    assert seen == [f"codex:{B}"]
    assert pending["worker"] == f"codex:{B}"
    assert pending["worker_id"] == B and pending["short"] == B[:8]
    assert pending["engine"] == "codex"
    assert pending["operation_status"] == "submitted"
    assert pending["compacted"] is False
    assert pending["tokens_before"] == 1234
    assert pending["tokens_after"] is None and pending["changed"] is None
    assert store.read_receipt(B, "codex-compact", token)["state"] == "accepted"
    assert _physical_count(trace) == 1

    active = worker.context(B)
    assert active["reason"] == "compacting"
    assert active["tokens_used"] is None
    gate.touch()
    verified = _settled(B, token)
    assert verified["status"] == "succeeded", verified
    assert verified["contextAfter"] is None

    # Reusing a caller-supplied ID reconciles the original, not another effect.
    replayed = worker.compact(B, wait=False, timeout=2, operation_id=token)
    assert replayed["operation_status"] == "succeeded"
    assert replayed["compacted"] is True
    assert replayed["operation_id"] == token
    assert replayed["tokens_after"] is None and replayed["changed"] is None
    observed = worker.compact_status(B, token)
    assert observed["operation_status"] == "succeeded"
    assert observed["operation_id"] == token and observed["compacted"] is True
    assert observed["tokens_after"] is None
    assert _physical_count(trace) == 1

    # A distinct, internally generated CodexClient operation can settle;
    # only its own native evidence produces a frozen successful result.
    completed = typed_client.compact(B, timeout=5)
    assert isinstance(completed, CodexCompactResult)
    assert completed.compacted is True
    assert completed.operation_status == "succeeded"
    assert completed.operation_id and completed.operation_id != token
    assert completed.compact_turn_id
    assert completed.context_after is None  # No newer native usage event.
    assert completed.as_dict()["worker"] == f"codex:{B}"
    assert completed.as_dict()["operation_id"] == completed.operation_id
    with pytest.raises(FrozenInstanceError):
        completed.compacted = False
    assert _physical_count(trace) == 2

    # This is an adapter classification check, NOT another native fault run.
    with monkeypatch.context() as patch:
        patch.setattr(codex_ops, "compact_worker", lambda *_a, **_kw: {
            "status": "failed", "compacted": False,
            "reason": "original internal compact turn failed",
        })
        with pytest.raises(CompactFailed) as failure:
            typed_client.compact(B, timeout=2)
    assert failure.value.operation_id
    assert "original internal compact turn failed" in str(failure.value)
    assert _physical_count(trace) == 2

    with monkeypatch.context() as patch:
        patch.setattr(codex_ops, "compact_worker", lambda *_a, **_kw: {
            "status": "rejected", "compacted": False,
            "reason": "original native request definitively refused",
        })
        with pytest.raises(DeliveryRejected) as refused:
            worker.compact(B, wait=False, timeout=2)
    assert refused.value.refusal == "native-rejected"
    assert refused.value.operation_id
    assert _physical_count(trace) == 2

    # An unconfirmed original binding is availability, never a raw ValueError;
    # a possibly landed effect stays indeterminate under its minted ID.
    from ccx.errors import SessionUnavailable
    from ccx.store.store import GenerationStore
    from ccx.store.root import NativeEffectIndeterminate

    with monkeypatch.context() as patch:
        patch.setattr(GenerationStore, "native_activation",
                      lambda *_a, **_kw: {"state": "submitted-unknown"})
        for call in (lambda: typed_client.compact(B, timeout=2),
                     lambda: worker.compact(B, wait=False, timeout=2)):
            with pytest.raises(SessionUnavailable):
                call()

    def landed(*_a, operation_id, **_kw):
        landed.ids.append(operation_id)
        raise NativeEffectIndeterminate("caller lost the compact ACK")

    landed.ids = []
    with monkeypatch.context() as patch:
        patch.setattr(codex_ops, "compact_worker", landed)
        for call in (lambda: typed_client.compact(B, timeout=2),
                     lambda: worker.compact(B, wait=True, timeout=2)):
            with pytest.raises(CompactIndeterminate) as lost:
                call()
            assert lost.value.operation_id == landed.ids[-1]
            assert lost.value.operation_status == "unknown"
    assert len(landed.ids) == 2
    assert _physical_count(trace) == 2


def test_codexclient_unproven_wait_raises_with_original_internal_id(
    real_compact_host, monkeypatch,
):
    store, trace, gate = real_compact_host
    with pytest.raises(CompactIndeterminate) as unknown:
        CodexClient().compact(B, timeout=2)
    token = unknown.value.operation_id
    assert token and UUID(token)
    assert unknown.value.operation_status == "submitted"
    assert store.read_receipt(B, "codex-compact", token)["state"] == "accepted"
    assert _physical_count(trace) == 1
    # Main contract: a waited WorkerClient compact returns only when settled.
    with monkeypatch.context() as patch:
        patch.setattr("uuid.uuid4", lambda: UUID(token))
        with pytest.raises(CompactIndeterminate) as waited:
            WorkerClient().compact(B, wait=True, timeout=1)
    assert waited.value.operation_id == token
    assert waited.value.operation_status == "submitted"
    assert _physical_count(trace) == 1
    gate.touch()
    assert _settled(B, token)["status"] == "succeeded"
    assert _physical_count(trace) == 1


def test_v1_codex_exact_id_pending_and_pure_status_original_bytes(
    real_compact_host, monkeypatch,
):
    store, trace, gate = real_compact_host
    caps = dispatch_full_uuid(v1_request("system.info"), store)
    assert caps["ok"] is True
    assert set(caps["result"]) == {"protocol_version", "ccx_version"}
    assert caps["result"]["protocol_version"] == 2

    for op in ("worker.compact", "worker.compact.status"):
        missing = dispatch_full_uuid(v1_request(op, B), store)
        assert missing["ok"] is False
        assert missing["error"]["code"] == "invalid_request"
        prefix = dispatch_full_uuid(
            v1_request(op, B[:8], operation_id="not-a-real-op"), store,
        )
        assert prefix["ok"] is False
        assert prefix["error"]["code"] == "invalid_request"
    assert _physical_count(trace) == 0

    absent = dispatch_full_uuid(
        v1_request("worker.compact.status", B, operation_id="unissued-id"),
        store,
    )
    assert absent["ok"] is True
    assert absent["result"]["status"] == "not_found"
    assert absent["result"]["compacted"] is None
    assert absent["result"]["recommended_action"] == "inspect"
    assert _physical_count(trace) == 0

    token = "v1-exact-original-codex-compact"
    pending_req = v1_request("worker.compact", B, operation_id=token)
    pending_req["timeout_ms"] = 1750  # after native admission, not a 1ms race
    pending = dispatch_full_uuid(pending_req, store)
    assert pending["ok"] is True, pending
    row = pending["result"]
    assert row["worker"] == f"codex:{B}"
    assert row["operation_id"] == token
    assert row["status"] == "submitted" and row["recommended_action"] == "wait"
    assert row["compacted"] is False
    assert row["context_after"] is None
    assert row["receipt_status"] == "accepted"
    assert row["receipt"]["operation_id"] == token
    assert _physical_count(trace) == 1

    original_marker = store.record(B)["activeCompact"]
    assert original_marker["operationId"] == token
    replay = v1_request("worker.compact", B, operation_id=token)
    replay["timeout_ms"] = 300
    same_id = dispatch_full_uuid(replay, store)
    assert same_id["ok"] is True
    assert same_id["result"]["status"] == "submitted"
    assert _physical_count(trace) == 1

    held = dispatch_full_uuid(
        v1_request("worker.compact.status", B, operation_id=token), store,
    )
    assert held["ok"] is True and held["result"]["status"] == "submitted"
    assert held["result"]["compacted"] is None
    assert held["result"]["recommended_action"] == "wait"
    assert held["result"]["context_after"] is None
    surface = held["result"]["input_surface"]
    assert (surface["compaction_ownership"], surface["operation_id"],
            surface["source"]) == ("held", token, "live")

    gate.touch()
    proven = _settled(B, token)
    assert proven["status"] == "succeeded"
    watched = (
        store.worker_path(B, "record.json"),
        store.receipt_path(B, "codex-compact", token),
    )
    baseline = [(f.read_bytes(), f.stat().st_mtime_ns) for f in watched]
    physical_before = _physical_count(trace)
    with monkeypatch.context() as patch:
        patch.setattr(
            codex_runtime, "ensure_daemon",
            lambda *_a, **_kw: pytest.fail("V1 status cannot start a daemon"),
        )
        status = dispatch_full_uuid(
            v1_request("worker.compact.status", B, operation_id=token), store,
        )
    assert status["ok"] is True, status
    row = status["result"]
    assert row["status"] == "succeeded" and row["compacted"] is True
    assert row["operation_id"] == token
    assert row["receipt_status"] == "accepted"
    assert row["compact_turn_id"] == proven["compactTurnId"]
    assert row["evidence"]["event_id"] == proven["contextItemId"]
    assert row["context_after"] is None  # C3 has no genuine newer usage.
    assert [(f.read_bytes(), f.stat().st_mtime_ns) for f in watched] == baseline
    assert _physical_count(trace) == physical_before == 1

    # Missing/incompatible daemon is UNKNOWN input ownership, never evidence
    # of historical native compact release or a reason to spawn one.
    with monkeypatch.context() as patch:
        patch.setattr(codex_runtime, "daemon_rpc", lambda *_a, **_kw: {
            "transport": "absent",
        })
        cold = dispatch_full_uuid(
            v1_request("worker.compact.status", B, operation_id=token), store,
        )
    assert cold["ok"] is True
    assert cold["result"]["input_surface"]["compaction_ownership"] == "unknown"
    assert cold["result"]["status"] == "succeeded"
    assert [(f.read_bytes(), f.stat().st_mtime_ns) for f in watched] == baseline
    assert _physical_count(trace) == 1

    # Known native failure is a typed refusal via the existing V1 classifier.
    # No second fake-provider failure matrix or effect is submitted here.
    with monkeypatch.context() as patch:
        patch.setattr(codex_ops, "compact_worker", lambda *_a, **_kw: {
            "status": "failed", "compacted": False,
            "reason": "original compact internal turn failed",
        })
        failed = dispatch_full_uuid(
            v1_request("worker.compact", B, operation_id="known-failure"),
            store,
        )
    assert failed["ok"] is False
    assert failed["error"]["details"]["refusal"] == "compact-failed"
    assert _physical_count(trace) == 1


@pytest.mark.parametrize(
    "case", ["no-wait-json", "no-wait-plain", "waited-timeout", "waited-settled"],
)
def test_codex_cli_compact_original_owner_prefix_and_n4_preflight(
    real_compact_host, case,
):
    """One original fake native effect per run; no Codex CLI parallel writer."""
    import json
    import os
    from pathlib import Path
    import re
    import subprocess
    import sys

    store, trace, gate = real_compact_host
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "PYTHONPATH": str(repo)}
    notify_dir = Path(env["CCX_ROOT"]) / "notify" / "registrations"
    registrations = set(notify_dir.glob("*.json"))
    # The same eight-character Claude source is a nonrunning real owner.
    assert store.record(A)["lifecycle"]["state"] not in ("idle", "busy", "waiting")

    def cli(target, *flags):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", "compact", target, *flags],
            env=env, cwd=str(repo), capture_output=True, text=True, timeout=25,
        )

    ambiguous = cli(B[:8], "--no-wait", "--json")
    assert ambiguous.returncode == 1, (ambiguous.stdout, ambiguous.stderr)
    assert ambiguous.stdout == ""
    assert "ambiguous" in ambiguous.stderr.lower()
    assert f"codex:{B}" in ambiguous.stderr
    assert f"claude:{A}" in ambiguous.stderr

    wrong_engine = cli(f"claude:{B}", "--no-wait", "--json")
    assert wrong_engine.returncode == 1, (
        wrong_engine.stdout, wrong_engine.stderr,
    )
    assert "is a codex worker; use codex:" in wrong_engine.stderr.lower()

    missing = cli("bbbbbbbb", "--no-wait")
    assert missing.returncode == 2, (missing.stdout, missing.stderr)
    assert "no such worker: bbbbbbbb" in missing.stderr

    no_user_token = cli(B, "--operation-id", "not-permitted", "--no-wait")
    assert no_user_token.returncode == 1
    assert "--operation-id is Claude-only" in no_user_token.stderr

    # Direct notifications work only with the caller's real Codex return
    # address. --settle by itself is invalid; no fake parent is inferred.
    for flags in (
        ("--notify-parent",), ("--settle",), ("--notify-parent", "--settle"),
    ):
        refused = cli(f"codex:{B[:8]}", *flags)
        assert refused.returncode == 1, (flags, refused.stdout, refused.stderr)
        assert refused.stdout == ""
        if flags == ("--settle",):
            assert "--settle requires a Codex worker and --notify-parent" in refused.stderr
        else:
            assert "operation_id=" in refused.stderr
            assert "parent notification" in refused.stderr.lower()
    assert _physical_count(trace) == 0
    assert not store.worker_path(B, "compact.json").exists()
    assert store.record(B).get("activeCompact") is None
    assert set(notify_dir.glob("*.json")) == registrations

    # Exactly one new original operation, never a new CLI native protocol.
    if case == "waited-settled":
        gate.touch()
    flags = {
        "no-wait-json": ("--no-wait", "--json"),
        "no-wait-plain": ("--no-wait",),
        "waited-timeout": ("--timeout", "2.5", "--json"),
        "waited-settled": ("--timeout", "8", "--json"),
    }[case]
    # Cover the exact full UUID and engine-qualified unique prefix with the
    # same original handler; discovery never reaches the backend as a prefix.
    target = B if case in ("no-wait-plain", "waited-settled") else f"codex:{B[:8]}"
    result = cli(target, *flags)
    assert _physical_count(trace) == 1, (
        case, result.stdout, result.stderr, trace.read_text(),
    )
    assert "thread/compact/start" in trace.read_text()
    if case == "waited-timeout":
        assert result.returncode == 6, (
            result.returncode, result.stdout, result.stderr,
        )
        assert result.stdout == ""
        assert "outcome is unknown" in result.stderr
        match = re.search(r"operation_id=([0-9a-f-]{36})", result.stderr)
        assert match, result.stderr
        token = match.group(1)
        assert store.record(B)["activeCompact"]["operationId"] == token
    elif case == "no-wait-plain":
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert "not proven complete" in result.stdout
        assert f"codex:{B}" in result.stdout
        match = re.search(r"operation_id=([0-9a-f-]{36})", result.stdout)
        assert match
        token = match.group(1)
    else:
        assert result.returncode == 0, (
            case, result.stdout, result.stderr,
        )
        row = json.loads(result.stdout)
        assert row["worker"] == f"codex:{B}"
        assert row["worker_id"] == B
        assert row["engine"] == "codex" and row["short"] == B[:8]
        assert row["thread_id"] == store.record(B)["native"]["threadId"]
        assert row["operation_id"]
        token = row["operation_id"]
        if case == "no-wait-json":
            assert row["operation_status"] == "submitted"
            assert row["accepted"] is True
            assert row["compacted"] is False
            assert row["context_after"] is None
        else:
            assert row["operation_status"] == "succeeded"
            assert row["accepted"] is True and row["compacted"] is True
            assert row["compact_turn_id"] and row["context_item_id"]
            assert row["native_turn_status"] == "completed"
            assert row["context_after"] is None  # No newer native usage.

    UUID(token)
    assert store.read_receipt(B, "codex-compact", token)["state"] == "accepted"
    assert not store.worker_path(B, "compact.json").exists()
    assert set(notify_dir.glob("*.json")) == registrations
    gate.touch()
    assert _settled(B, token)["status"] == "succeeded"
    assert _physical_count(trace) == 1


def test_worker_codex_compact_explicit_id_is_replayable_without_second_effect(real_compact_host):
    store, trace, gate = real_compact_host
    worker = WorkerClient()
    token = "caller-chosen-compact-r1"
    first = worker.compact(B, wait=False, timeout=5, operation_id=token)
    assert first["operation_id"] == token
    assert first["operation_status"] == "submitted"
    assert _physical_count(trace) == 1
    before = worker.compact_status(B, token)
    assert before["operation_id"] == token
    assert before["operation_status"] in ("submitted", "unknown")
    again = worker.compact(B, wait=False, timeout=2, operation_id=token)
    assert again["operation_id"] == token
    assert _physical_count(trace) == 1
    gate.touch()
    assert _settled(B, token)["status"] == "succeeded"
    final = worker.compact_status(B, token)
    assert final["operation_status"] == "succeeded"
    assert final["compacted"] is True
    assert _physical_count(trace) == 1
