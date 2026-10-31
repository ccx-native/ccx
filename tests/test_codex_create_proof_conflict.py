"""Waited Codex creation must not reject a second proof of the same first turn."""
from __future__ import annotations
import ccx.ops.send as ops_send

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx import WorkerClient
from ccx.codex import delivery as codex_delivery
from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx.ops import creation
from ccx import constants
from ccx.ops import common as protocol_ops
from ccx.codex.completion import settle_completed
from ccx.errors import CreatePartialFailure
from ccx.store.store import _digest
from test_delivery_flow import _activate_fake_codex
from test_generation_store import B, fixture_root


def _history_completion(*, source, thread, turn, text):
    return {
        "ok": True, "status": "completed", "workerId": B,
        "engine": "codex", "nativeSource": source, "threadId": thread,
        "turnId": turn, "deliveryId": "create:initial", "text": text,
        "nativeEvidenceSha256": _digest({
            "source": source, "threadId": thread, "turnId": turn,
            "status": "completed", "text": text,
        }),
    }


@pytest.mark.parametrize("strict_old_comparator", [False, True], ids=[
    "first-writer-wins", "reproduce-old-CreatePartialFailure",
])
def test_workers_create_codex_wait_initial_proof_race(
    tmp_path, monkeypatch, strict_old_comparator,
):
    """Actual workers.create(prompt=..., wait=True) through create:initial.

    A fake native Codex completion arrives while wait_delivery's source-fenced
    native-observe is in progress. Live item/completed has the final answer but
    thread/read yields a weaker legacy fallback for that same completed turn.
    The old full-proof comparator produces the WSL CreatePartialFailure; the
    production comparator returns the original immutable text to the creator.
    """
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setattr(codex_runtime, "list_models", lambda **_: ["gpt-5.6-luna"])
    worker = f"codex:{B}"
    turn = "initial-native-turn"
    prompt = "Reply exactly CODEX_INITIAL."
    sends = []
    first = {}
    competing = {}
    observations = []

    def fake_activate(store, original, info, *, start_timeout, deadline):
        assert original.worker_id == B
        return _activate_fake_codex(store, original).record

    monkeypatch.setattr(creation, "_complete_codex_create", fake_activate)

    def fake_send(handle, native_thread, message, token, budget):
        # This is the actual durable semantic first-delivery token.
        assert handle == worker and message == prompt and token == "create:initial"
        receipt = codex_delivery.read_receipt(worker, token)
        assert receipt["state"] == "submitted" and receipt.get("settlement") is None
        sends.append(token)
        return {
            "accepted": True, "workerId": B, "engine": "codex",
            "nativeSource": f"ccx-worker:{B}",
            "threadId": native_thread, "turnId": turn,
            "deliveryId": token,
            "clientUserMessageId": receipt["clientUserMessageId"],
            "nativeEvidenceSha256": "a" * 64,
        }

    real_send = ops_send.send_turn
    monkeypatch.setattr(ops_send, "send_turn", lambda *a, **kw:
                        real_send(*a, _native_submit=fake_send, **kw))
    monkeypatch.setattr(codex_evidence, "read_journal", lambda _wid:
                        SimpleNamespace(complete=False, events=[]))

    def fake_native_observe(message, *, timeout):
        assert message["op"] == "native-observe"
        assert message["workerId"] == B
        assert message["turnId"] == turn
        assert message["deliveryId"] == "create:initial"
        from ccx.store.root import require_ccx_active
        store = require_ccx_active(root)
        thread = store.record(worker)["native"]["threadId"]
        source = store.codex_source(worker)
        # The original live completion writer wins the durable receipt.
        live = settle_completed(
            store, worker, thread_id=thread,
            turn={"id": turn, "status": "completed", "items": [
                {"type": "agentMessage", "text": "legacy initial fallback"},
            ]},
            live_report={"turnId": turn, "phase": "final_answer",
                         "text": "CODEX_INITIAL"},
        )
        first.update(live["settlement"])
        # Independent history observer constructs a positive proof with the
        # *same* completed-turn identity but differing selected answer text.
        history = _history_completion(source=source, thread=thread, turn=turn,
                                      text="legacy initial fallback")
        competing.update(history)
        observations.append("native-observe")
        return history

    monkeypatch.setattr(codex_runtime, "daemon_rpc", fake_native_observe)

    if strict_old_comparator:
        # Re-enact the previous comparator without modifying production code:
        # the exact WSL error was raised when the second proof differed in text.
        mark_settled = codex_delivery.mark_settled

        def original_byte_equal_mark_settled(handle, token, *, turn_id,
                                             original_positive_completion):
            current = codex_delivery.read_receipt(handle, token)
            if (current is not None and current.get("settlement") is not None
                    and current["settlement"] != original_positive_completion):
                raise codex_delivery.DeliveryReceiptConflict(
                    "Codex original completed turn proof changed")
            return mark_settled(handle, token, turn_id=turn_id,
                                original_positive_completion=original_positive_completion)

        monkeypatch.setattr(codex_delivery, "mark_settled", original_byte_equal_mark_settled)
        with pytest.raises(CreatePartialFailure,
                           match="original first turn did not complete with exact proof") as err:
            WorkerClient().create(engine="codex", model="gpt-5.6-luna",
                                  cwd=str(tmp_path), worker_id=B,
                                  prompt=prompt, wait=True, timeout=5)
        assert err.value.phase == "wait-report"
        assert err.value.worker_id == B
        assert err.value.delivery_id == "create:initial"
        assert isinstance(err.value.cause, codex_delivery.DeliveryReceiptConflict)
        assert str(err.value.cause) == "Codex original completed turn proof changed"
    else:
        created = WorkerClient().create(engine="codex", model="gpt-5.6-luna",
                                        cwd=str(tmp_path), worker_id=B,
                                        prompt=prompt, wait=True, timeout=5)
        assert created["worker"] == worker
        assert created["delivery_id"] == "create:initial"
        assert created["delivery_status"] == "settled"
        assert created["report"]["text"] == "CODEX_INITIAL"
        assert created["report"]["delivery_id"] == "create:initial"
        assert created["report"]["turn_id"] == turn
    assert sends == ["create:initial"]
    assert observations == ["native-observe"]
    assert {field for field in first if first[field] != competing[field]} == {
        "text", "nativeEvidenceSha256"}
    receipt = codex_delivery.read_receipt(worker, "create:initial")
    assert receipt["settlement"] == first  # the first final answer is immutable
