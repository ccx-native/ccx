from __future__ import annotations
import ccx.ops.send as ops_send

import json

import pytest

from ccx.claude import delivery as CD
from ccx import constants
from ccx.codex import delivery as XD
from ccx.notify import registry as N
from ccx.ops import common as P
from ccx.errors import InvalidRequest
from test_generation_store import A, B, add_worker, fixture_root


METADATA = {
    "agentInstanceId": "agent-123",
    "workId": "work-456",
    "executionId": "exec-789",
}


@pytest.fixture
def isolated_root(monkeypatch, tmp_path):
    root = fixture_root(tmp_path)
    monkeypatch.setenv("CCX_ROOT", str(root))
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    add_worker(root, A, "claude")
    add_worker(root, B, "codex")
    return root


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_delivery_receipt_binds_caller_metadata_idempotently(
    isolated_root, engine
):
    worker_id = A if engine == "claude" else B
    if engine == "claude":
        first, created = CD.prepare(
            worker_id,
            "delivery-meta",
            "go",
            mode="new_turn_only",
            caller_metadata=METADATA,
        )
        second, created_again = CD.prepare(
            worker_id,
            "delivery-meta",
            "go",
            mode="new_turn_only",
            caller_metadata=dict(METADATA),
        )
        public = CD.public_receipt(
            CD.read_receipt(worker_id, "delivery-meta")
        )
        conflict = CD.ClaudeDeliveryConflict
    else:
        first, created = XD.prepare(
            worker_id,
            "delivery-meta",
            "go",
            mode="new_turn_only",
            caller_metadata=METADATA,
        )
        second, created_again = XD.prepare(
            worker_id,
            "delivery-meta",
            "go",
            mode="new_turn_only",
            caller_metadata=dict(METADATA),
        )
        public = XD.public_receipt(
            XD.read_receipt(worker_id, "delivery-meta")
        )
        conflict = XD.DeliveryReceiptConflict

    assert created is True
    assert created_again is False
    assert first["callerMetadata"] == METADATA
    assert second["callerMetadata"] == METADATA
    assert public["caller_metadata"] == METADATA

    with pytest.raises(conflict, match="caller metadata"):
        if engine == "claude":
            CD.prepare(
                worker_id,
                "delivery-meta",
                "go",
                mode="new_turn_only",
                caller_metadata={**METADATA, "executionId": "different"},
            )
        else:
            XD.prepare(
                worker_id,
                "delivery-meta",
                "go",
                mode="new_turn_only",
                caller_metadata={**METADATA, "executionId": "different"},
            )

def test_send_caller_metadata_requires_notify_parent_before_mutation(isolated_root):
    with pytest.raises(InvalidRequest, match="requires notify_parent=true"):
        ops_send.send_turn(
            f"claude:{A}",
            "go",
            delivery_id="metadata-without-notify",
            caller_metadata=METADATA,
        )


def test_send_caller_metadata_has_closed_4096_byte_bound(isolated_root):
    with pytest.raises(InvalidRequest, match="4096-byte"):
        ops_send.send_turn(
            f"codex:{B}",
            "go",
            delivery_id="metadata-oversize",
            notify_parent=True,
            caller_metadata={"blob": "x" * 4096},
        )
