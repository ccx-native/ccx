"""File-backed original compact observation and public refusal boundaries.

Retired pre-UUID Worker/daemon staging and delivery-mode matrices. These
checks use an ACTIVE full owner or pure public error mapping; no provider runs.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

from ccx.ops import compact_ops
from ccx import constants
from ccx.rpc import local_rpc
from ccx.codex.daemon.models import _compact_native_refusal
from ccx.codex.protocol import ProtocolError
from ccx.errors import DeliveryRejected
from test_generation_store import A, add_worker, fixture_root


def _original_root(monkeypatch, tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setenv("CCX_ROOT", str(root))
    return root


def _append(path, event):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event) + "\n")


def test_cold_rpc_does_not_create_state(tmp_path):
    root = tmp_path / "never-created"
    req = {"protocol_version": 2, "request_id": "read", "op": "worker.compact.status",
           "params": {"target": f"codex:{A}", "operation_id": "A"}}
    result = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "rpc"],
        input=json.dumps(req), text=True, capture_output=True,
        env={**os.environ, "CCX_ROOT": str(root)},
    )
    response = json.loads(result.stdout)
    assert response["error"]["code"] == "not_found", result.stderr
    from ccx.store.root import require_ccx_active
    assert require_ccx_active(root).list_workers() == []
    assert not (root / "run").exists()


def test_claude_unsubmitted_boundary_and_quoted_noop_are_not_proof(monkeypatch, tmp_path):
    root = _original_root(monkeypatch, tmp_path)
    transcript = tmp_path / "original-claude.jsonl"
    transcript.touch()
    compact_ops.begin_operation(
        A, operation_id="A", transcript=str(transcript),
        timeout_s=300, host_epoch="epoch",
    )
    _append(transcript, {
        "type": "system", "subtype": "compact_boundary", "uuid": "proof",
        "compactMetadata": {"trigger": "manual"},
    })
    assert compact_ops.evaluate_operation(A, "A")["status"] == "accepted"
    compact_ops.transition_operation(A, "A", "submitted", submitted=True)
    transcript.write_text(json.dumps({
        "type": "user", "message": {
            "content": "Explain Not enough messages to compact",
        },
    }) + "\n")
    assert compact_ops.evaluate_operation(A, "A")["status"] == "submitted"
    assert not (root / "sessions").exists()


def test_compaction_refusal_mapping_does_not_depend_on_busy_state():
    result = local_rpc._error_from_exception("request", DeliveryRejected(
        "compact", state="idle", refusal="compaction-busy",
        delivery_id="D", delivery_status="owned", operation_id="A"))
    assert result["error"]["code"] == "refused"
    assert result["error"]["details"]["reason"] == "busy"
    assert result["error"]["details"]["retryable"] is True
    assert result["error"]["details"]["delivery_status"] == "owned"


def test_native_compatibility_fallback_does_not_swallow_unrelated_errors():
    assert not _compact_native_refusal(ProtocolError(
        "other internal error", method="turn/start", code=-32603, request_id=1))
    assert not _compact_native_refusal(ProtocolError(
        "failed to submit turn input: ActiveTurnNotSteerable { turn_kind: Review }",
        method="turn/start", code=-32603, request_id=1))
    assert not _compact_native_refusal(ProtocolError(
        "cannot steer a compact turn", request_id=1,
        data={"activeTurnNotSteerable": {"turnKind": "review"}}))
