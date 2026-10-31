"""Revived Codex CLI sends must wait for their own terminal delivery proof.

Only a fake owner, fake native start/read, and the real delivery/CLI code.
"""
from __future__ import annotations
from ccx.codex.settle import send_settle_failure
import ccx.ops.report as ops_report
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait

import json

import pytest

from ccx.cli import main as cli
from ccx.codex import delivery as codex_delivery
from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx.ops import common as protocol_ops
from ccx.ops.creation_steps import prepare_worker
from ccx.store.store import _digest
from test_delivery_flow import _activate_fake_codex, codex_owners  # noqa: F401
from test_generation_store import B

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")


@pytest.mark.parametrize("json_mode", [False, True], ids=["plain", "json"])
def test_codex_send_wait_revived_turn_survives_provisional_native_failed(
    codex_owners, tmp_path, monkeypatch, capsys, json_mode,
):
    """Actual CLI plus semantic receipt; one transient failed native read.

    A resumed parent has an older completed turn and a held notification.
    Native thread/read briefly reports failed for the new turn, without a
    durable turn/completed failure; its exact original receipt then completes.
    A premature --wait failure must not override that successful settlement.
    """
    _root, store = codex_owners
    owner = f"codex:{B}"
    _activate_fake_codex(store, prepare_worker(
        store, engine="codex", cwd=str(tmp_path), worker_id=B))
    native = store.record(owner)["native"]
    source = store.codex_source(owner)
    thread_id = native["threadId"]
    # The earlier turn was completed, then this original worker was stopped.
    store.mutate_record(owner, patches={
        "lifecycle.state": "dead",
        "lastTurn": {"turnId": "before-revive", "status": "completed"},
    })
    delivery_id, turn_id = "revival-original-delivery", "revival-native-turn"
    prompt = "Reply with exactly REVIVED"
    sends = []
    observations = []
    returned_reports = []

    def native_submit(handle, thread, text, token, _budget):
        assert (handle, thread, text, token) == (
            owner, thread_id, prompt, delivery_id)
        receipt = codex_delivery.read_receipt(owner, token)
        assert receipt["state"] == "submitted"
        sends.append(token)
        store.mutate_record(owner, patches={"lifecycle.state": "busy"})
        return {
            "accepted": True, "workerId": B, "engine": "codex",
            "nativeSource": source, "threadId": thread_id,
            "turnId": turn_id, "deliveryId": delivery_id,
            "clientUserMessageId": receipt["clientUserMessageId"],
            "nativeEvidenceSha256": "a" * 64,
        }

    real_send = ops_send.send_turn
    monkeypatch.setattr(
        ops_send, "send_turn",
        lambda *args, **kwargs:
            real_send(*args, _native_submit=native_submit, **kwargs),
    )
    # No native evidence journal completion until the second observation.
    monkeypatch.setattr(
        codex_evidence, "read_journal",
        lambda _worker: codex_evidence.JournalRead(
            _worker, tmp_path / "absent-journal", exists=False, events=[],
            last_sequence=0, partial_tail=False, malformed_lines=[],
        ),
    )

    proof = {
        "ok": True, "status": "completed",
        "workerId": B, "engine": "codex",
        "nativeSource": source, "threadId": thread_id,
        "turnId": turn_id, "deliveryId": delivery_id, "text": "REVIVED",
    }
    proof["nativeEvidenceSha256"] = _digest({
        "source": source, "threadId": thread_id,
        "turnId": turn_id, "status": "completed", "text": "REVIVED",
    })

    def native_observe(message, *, timeout):
        assert message["op"] == "native-observe"
        assert (message["workerId"], message["deliveryId"], message["turnId"]) == (
            B, delivery_id, turn_id,
        )
        observations.append(message["op"])
        if len(observations) == 1:
            assert codex_delivery.read_receipt(owner, delivery_id).get("settlement") is None
            # Native thread/read is a non-durable snapshot. A Codex resume
            # can briefly expose failed before turn/completed says completed.
            return {
                "ok": True, "status": "failed", "workerId": B,
                "deliveryId": delivery_id, "turnId": turn_id, "error": None,
            }
        # The exact original receipt is proven completed before the next
        # pending observation is returned; the CLI must re-read this receipt.
        codex_delivery.mark_settled(
            owner, delivery_id, turn_id=turn_id,
            original_positive_completion=proof,
        )
        store.mutate_record(owner, patches={
            "lifecycle.state": "idle",
            "lastTurn": {"turnId": turn_id, "status": "completed"},
        })
        return {"ok": True, "status": "pending", "workerId": B}

    monkeypatch.setattr(codex_runtime, "daemon_rpc", native_observe)

    def stable_wait(worker, *, timeout, **_kwargs):
        assert worker == owner and timeout == 0
        return {
            "status": "busy",
            "lastTurn": {"turnId": "before-revive", "status": "completed"},
            "last_settled_turn": {
                "turn_ref": "ccx-turn:codex:v1:before-revive",
                "outcome": "completed",
            },
        }

    monkeypatch.setattr(ops_wait, "wait_worker", stable_wait)

    def exact_report(worker, *, turn_ref):
        assert (worker, turn_ref) == (
            owner, "ccx-turn:codex:v1:" + turn_id)
        returned_reports.append(turn_ref)
        return {"worker": worker, "turn_ref": turn_ref, "text": "REVIVED"}

    monkeypatch.setattr(ops_report, "report_worker", exact_report)
    args = ["send", owner, prompt, "--delivery-id", delivery_id,
            "--wait", "--timeout", "5"]
    if json_mode:
        args.append("--json")
    cli.main(args)

    output = capsys.readouterr().out
    if json_mode:
        result = json.loads(output)
        assert result["status"] == "settled" and result["settled"] is True
        assert result["report"]["text"] == "REVIVED"
        assert result["delivery"]["settlement"]["text"] == "REVIVED"
    else:
        assert output.splitlines() == [
            owner, f"delivery_id: {delivery_id}", "REVIVED",
        ]
        assert "status: failed" not in output
    assert sends == [delivery_id]  # no second native start/retry
    assert len(observations) == 2
    assert returned_reports == ["ccx-turn:codex:v1:" + turn_id]
    assert codex_delivery.read_receipt(owner, delivery_id)["settlement"] == proof


def test_failure_probe_still_rejects_proven_same_turn_failure(
    monkeypatch,
):
    owner = f"codex:{B}"
    original = "ccx-turn:codex:v1:exact-failed-turn"
    monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_kw: {
        "status": "idle",
        "lastTurn": {"turnId": "exact-failed-turn", "status": "failed",
                     "error": "positive terminal failure"},
    })
    result = send_settle_failure(
        owner, {"status": "accepted", "turn_ref": original})
    assert result == {"status": "failed", "error": "positive terminal failure"}
