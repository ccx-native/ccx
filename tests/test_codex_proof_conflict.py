"""Two observers can race to settle one Codex turn from different native views."""
from __future__ import annotations
import ccx.ops.send as ops_send

import io
import json
from types import SimpleNamespace

import pytest

from ccx.cli import main as cli
from ccx.codex import delivery as codex_delivery
from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx.ops import common as protocol_ops
from ccx.codex.completion import settle_completed
from ccx.ops.creation_steps import prepare_worker
from ccx.store.store import _digest
from test_delivery_flow import _activate_fake_codex, codex_owners  # noqa: F401
from test_generation_store import B


def _history_proof(*, source, thread, turn, token, text):
    return {
        "ok": True, "status": "completed", "workerId": B,
        "engine": "codex", "nativeSource": source, "threadId": thread,
        "turnId": turn, "deliveryId": token, "text": text,
        "nativeEvidenceSha256": _digest({
            "source": source, "threadId": thread, "turnId": turn,
            "status": "completed", "text": text,
        }),
    }


def test_cli_stdin_wait_keeps_first_completed_proof_when_history_differs(
    codex_owners, tmp_path, monkeypatch, capsys,
):
    """Fake codexd completes DURING the send --wait native history read.

    Live item/completed supplied a strong final_answer, but native thread/read
    holds only a legacy fallback. Both identify the SAME completed turn.
    Before the fix, the actual CLI send handler exits 3 for a changed proof.
    """
    _root, store = codex_owners
    _activate_fake_codex(store, prepare_worker(
        store, engine="codex", cwd=str(tmp_path), worker_id=B))
    thread = store.record(B)["native"]["threadId"]
    source = store.codex_source(B)
    token, turn = "macos-codex-send", "codex-stdin-completed"
    sent = []

    def fake_send(handle, native_thread, prompt, delivery_id, budget):
        assert (handle, native_thread, prompt, delivery_id) == (
            f"codex:{B}", thread, "Reply exactly CODEX_STDIN.", token)
        receipt = codex_delivery.read_receipt(B, token)
        sent.append(receipt["clientUserMessageId"])
        return {
            "accepted": True, "workerId": B, "engine": "codex",
            "nativeSource": source, "threadId": thread, "turnId": turn,
            "deliveryId": token, "clientUserMessageId": sent[0],
            "nativeEvidenceSha256": "a" * 64,
        }

    real_send = ops_send.send_turn
    monkeypatch.setattr(ops_send, "send_turn", lambda *args, **kwargs:
                        real_send(*args, _native_submit=fake_send, **kwargs))
    monkeypatch.setattr(codex_evidence, "read_journal", lambda _worker:
                        SimpleNamespace(complete=False, events=[]))
    writers, first = [], {}

    def native_observe(request, *, timeout):
        assert request["op"] == "native-observe"
        assert request["turnId"] == turn and request["deliveryId"] == token
        # Real codexd completion writer, racing the in-flight native history read.
        completion = settle_completed(
            store, f"codex:{B}", thread_id=thread,
            turn={"id": turn, "status": "completed", "items": [
                {"type": "agentMessage", "text": "legacy history fallback"},
            ]},
            live_report={"turnId": turn, "phase": "final_answer",
                         "text": "CODEX_STDIN"},
        )
        assert completion and completion["settlement"]["text"] == "CODEX_STDIN"
        first.update(completion["settlement"])
        writers.append("codex_completion.settle_completed")
        writers.append("protocol_ops.reconcile_turn/native-observe")
        return _history_proof(source=source, thread=thread, turn=turn, token=token,
                              text="legacy history fallback")

    monkeypatch.setattr(codex_runtime, "daemon_rpc", native_observe)
    monkeypatch.setattr("sys.stdin", io.StringIO("Reply exactly CODEX_STDIN."))
    cli.main(["send", B, "--stdin", "--delivery-id", token,
              "--wait", "--timeout", "5", "--json"])
    out = json.loads(capsys.readouterr().out)
    assert writers == ["codex_completion.settle_completed",
                       "protocol_ops.reconcile_turn/native-observe"]
    history = _history_proof(source=source, thread=thread, turn=turn, token=token,
                             text="legacy history fallback")
    assert {key for key in first if first[key] != history[key]} == {
        "text", "nativeEvidenceSha256"}
    assert out["status"] == "settled" and out["settled"] is True
    assert out["report"]["text"] == "CODEX_STDIN"
    assert out["delivery"]["settlement"] == first
    assert codex_delivery.read_receipt(B, token)["settlement"] == first
    assert len(sent) == 1


@pytest.mark.parametrize("field,replacement", [
    ("workerId", "different-worker"),
    ("engine", "claude"),
    ("nativeSource", "other-source"),
    ("threadId", "other-thread"),
    ("deliveryId", "other-token"),
])
def test_codex_completed_proof_identity_mismatch_still_conflicts(
    codex_owners, field, replacement,
):
    _root, _store = codex_owners
    token, turn = "original-delivery", "original-turn"
    codex_delivery.prepare(B, token, "message")
    codex_delivery.mark_submitted(B, token)
    codex_delivery.mark_accepted(B, token, turn_id=turn)
    proof = _history_proof(source=f"ccx-worker:{B}", thread="native-thread",
                           turn=turn, token=token, text="original")
    first = codex_delivery.mark_settled(
        B, token, turn_id=turn, original_positive_completion=proof)
    changed = {**proof, field: replacement}
    with pytest.raises(codex_delivery.DeliveryReceiptConflict,
                       match="completion record changed"):
        codex_delivery.mark_settled(
            B, token, turn_id=turn, original_positive_completion=changed)
    assert codex_delivery.read_receipt(B, token) == first


def test_codex_live_and_history_share_final_answer_selection():
    from ccx.codex.reports import select_report_for_turn

    turn = {"id": "same-turn", "status": "completed", "items": [
        {"type": "agentMessage", "text": "commentary", "phase": "commentary"},
        {"type": "agentMessage", "text": "historical fallback"},
    ]}
    same_final = {"turnId": "same-turn", "phase": "final_answer",
                  "text": "strong streamed final"}
    other_final = {**same_final, "turnId": "other-turn"}
    assert select_report_for_turn(turn, live_report=same_final)["text"] == "strong streamed final"
    assert select_report_for_turn(turn, live_report=other_final)["text"] == "historical fallback"
    history_final = {**turn, "items": turn["items"] + [
        {"type": "agentMessage", "phase": "final_answer", "text": "native final"},
    ]}
    assert select_report_for_turn(history_final, live_report={
        "turnId": "same-turn", "phase": None, "text": "weaker streamed fallback",
    })["text"] == "native final"
