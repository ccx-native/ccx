"""Exact, proof-backed settlement of Codex's original accepted turn receipts.

The live notification supplies the completed turn without a history read.  A
fast completion can precede the caller's accepted-receipt write; its durable
evidence journal is then the same notification source for a later catch-up.
"""

from __future__ import annotations

from typing import Any

from ccx.codex import delivery as codex_delivery
from ccx.codex import evidence as codex_evidence
from ccx.codex.reports import report_from_turn, select_report_for_turn


def settle_completed(
    store: Any, handle: str, *, thread_id: str, turn: dict[str, Any],
    live_report: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Settle only the exact admitted new-turn delivery and original source."""
    from ccx.store.store import _digest

    turn_id = turn.get("id") if isinstance(turn, dict) else None
    if (not isinstance(turn_id, str) or not turn_id
            or turn.get("status") != "completed"):
        return None
    owner = store.resolve(handle)
    if owner.engine != "codex":
        return None
    receipt = codex_delivery.receipt_for_turn(owner.worker, turn_id)
    if (not isinstance(receipt, dict)
            or receipt.get("state") != "accepted"
            or codex_delivery.route(receipt) != "new_turn"):
        return None
    if isinstance(receipt.get("settlement"), dict):
        return receipt
    source = store.codex_source(owner.worker)
    native = (store.record(owner.worker).get("native") or {})
    activation = store.native_activation(owner.worker)
    binding = activation.get("nativeBinding") or {}
    effect = store.read_receipt(owner.worker, "codex-send", receipt["deliveryId"])
    if (not isinstance(native, dict)
            or native.get("threadId") != thread_id
            or native.get("threadSource") != source
            or activation.get("state") != "confirmed"
            or activation.get("source") != source
            or not isinstance(binding, dict)
            or binding.get("threadId") != thread_id
            or not isinstance(effect, dict)
            or effect.get("state") != "accepted"
            or effect.get("workerId") != owner.worker_id
            or effect.get("engine") != "codex"
            or effect.get("generation") != store.generation
            or effect.get("action") != "send"
            or effect.get("intentSha256") != _digest(
                codex_delivery.intent_spec(receipt))):
        return None

    selected = select_report_for_turn(turn, live_report=live_report)
    text = selected["text"] if selected is not None else ""
    proof = {
        "ok": True, "status": "completed", "workerId": owner.worker_id,
        "engine": "codex", "nativeSource": source, "threadId": thread_id,
        "turnId": turn_id, "deliveryId": receipt["deliveryId"],
        "text": text, "nativeEvidenceSha256": _digest({
            "source": source, "threadId": thread_id, "turnId": turn_id,
            "status": "completed", "text": text,
        }),
    }
    store.revalidate()
    return codex_delivery.mark_settled(
        owner.worker, receipt["deliveryId"], turn_id=turn_id,
        original_positive_completion=proof,
    )


def settle_from_journal(store: Any, handle: str, turn_id: str) -> dict[str, Any] | None:
    """Catch a pre-acceptance completion from original sparse native events.

    This is not a substitute for native observation when events were missed.
    In that case reconciliation falls through to a source-fenced thread read.
    """
    owner = store.resolve(handle)
    native = store.record(owner.worker).get("native") or {}
    thread_id = native.get("threadId")
    if not isinstance(thread_id, str) or not thread_id:
        return None
    journal = codex_evidence.read_journal(owner.worker_id)
    if not journal.complete:
        return None
    items: list[dict[str, Any]] = []
    started = False
    for event in journal.events:
        if event.get("turnId") != turn_id:
            continue
        if event.get("threadId") != thread_id:
            return None
        params = event.get("params") or {}
        if event.get("method") == "turn/started":
            started_turn = params.get("turn")
            started = (isinstance(started_turn, dict)
                       and started_turn.get("id") == turn_id)
        elif event.get("method") == "item/completed":
            item = params.get("item")
            if isinstance(item, dict):
                items.append(item)
        elif event.get("method") == "turn/completed":
            turn = params.get("turn")
            if (not started or not isinstance(turn, dict)
                    or turn.get("id") != turn_id):
                return None
            # Keep native turn.items intact, but also honor a stronger
            # final_answer published in the live item/completed stream. The
            # same report_from_turn rule applies to both observation surfaces.
            stream_report = report_from_turn({"id": turn_id, "items": items})
            return settle_completed(
                store, owner.worker, thread_id=thread_id, turn=turn,
                live_report=stream_report,
            )
    return None
