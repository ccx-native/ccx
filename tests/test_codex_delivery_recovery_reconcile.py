"""Restart reconciliation of lost Codex send ACKs, with no codexd running.

Every durable fact is written by the real writers: the semantic receipt
(``codex_delivery``), the global native intent (``begin_native_intent`` and a
``dispatch_native_intent_once`` whose answer is lost), and the evidence
journal (``EvidenceJournal``, as codexd writes it). Only the native provider
is absent. ``reconcile_turn`` is the real caller; ``daemon_rpc`` is recorded
so any native contact is visible.
"""
from __future__ import annotations

import os
import threading

import pytest

from ccx.codex import delivery as codex_delivery
from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.rpc import local_rpc
from ccx.codex.delivery_recovery import recover
from ccx.codex.provenance import new_client_message_id
from ccx.store.root import NativeEffectIndeterminate, StaleGeneration, require_ccx_active
from ccx.store.store import OperationIntent, _digest
from ccx.ops.wait import reconcile_turn
from ccx.ops.send import send_turn
from test_generation_store import A, B, G2, add_worker, put, short_root, switch_pointer  # noqa: F401

TURN_R = "native-turn-R"
TURN_N = "native-turn-N"


@pytest.fixture
def bound(short_root, monkeypatch):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    add_worker(short_root, A, "codex", confirmed=True)
    store = add_worker(short_root, B, "codex", confirmed=True)
    calls: list[dict] = []

    def recorded(message, timeout=None):
        calls.append(dict(message))
        return {"ok": False, "error": "no codexd in this test"}

    monkeypatch.setattr(codex_runtime, "daemon_rpc", recorded)
    return store, store.record(B)["native"]["threadId"], calls


def lost(store, token, *, route="new_turn", target=None, text="lost ack"):
    """A V2-shaped send whose native answer never reached the caller."""
    mode = "new_turn_only" if route == "new_turn" else "steer_only"
    receipt, _ = codex_delivery.prepare(B, token, text, mode=mode)
    intent = store.begin_native_intent(
        B, family="codex-send", action="send", operation_id=token,
        intent_sha256=_digest(codex_delivery.intent_spec(receipt)))
    if route == "steer":
        codex_delivery.mark_owned(B, token, route_kind="steer", target_turn_id=target)
    codex_delivery.mark_submitted(B, token)

    def answer_lost(_budget):
        raise TimeoutError("native acknowledgment lost")

    with pytest.raises(NativeEffectIndeterminate):
        store.dispatch_native_intent_once(intent, bounded_submit=answer_lost)
    return codex_delivery.read_receipt(B, token)


def journal(*events, worker=B):
    writer = codex_evidence.EvidenceJournal(worker)
    for method, params in events:
        writer.record(method, params)
    writer.close()


def user_message(thread, turn, client, *, item_id="um-1", kind="userMessage"):
    item = {"type": kind, "id": item_id, "clientId": client,
            "content": [{"type": "text", "text": "prompt text"}]}
    return [(method, {"threadId": thread, "turnId": turn, "item": item})
            for method in ("item/started", "item/completed")]


def completed_turn(thread, turn):
    answer = {"type": "agentMessage", "id": "answer-" + turn, "text": "done"}
    return [("turn/started", {"threadId": thread, "turn": {"id": turn}}),
            ("item/completed", {"threadId": thread, "turnId": turn, "item": answer}),
            ("turn/completed", {"threadId": thread,
                                "turn": {"id": turn, "status": "completed", "items": [answer]}})]


def ack_digest(store, receipt, turn):
    """Exactly what codexd's native-send acknowledgment would carry."""
    return _digest({
        "source": store.native_activation(B)["source"],
        "threadId": store.record(B)["native"]["threadId"], "turnId": turn,
        "deliveryId": receipt["deliveryId"], "route": codex_delivery.route(receipt),
        "targetTurnId": receipt.get("targetTurnId"),
        "clientUserMessageId": receipt["clientUserMessageId"],
    })


def late_ack(store, receipt, turn):
    """The caller's own acceptance writes, as protocol_ops performs them."""
    effect = store.read_receipt(B, "codex-send", receipt["deliveryId"])
    codex_delivery.mark_accepted(B, receipt["deliveryId"], turn_id=turn)
    store.settle_native_intent(OperationIntent(
        effect["workerId"], "codex", "codex-send", receipt["deliveryId"],
        effect["intentSha256"], "submitted-unknown", store.generation,
    ), state="accepted", native_evidence_sha256=ack_digest(store, receipt, turn))


def no_native_send(calls):
    assert not [c for c in calls if c.get("op") in ("native-send", "send")], calls


# -- positive evidence ---------------------------------------------------------

def test_new_turn_recovers_then_settles_by_exact_turn(bound):
    store, thread, calls = bound
    receipt = lost(store, "restart-start")
    journal(*completed_turn(thread, TURN_N)[:1],
            *user_message(thread, TURN_N, receipt["clientUserMessageId"]),
            *completed_turn(thread, TURN_N)[1:])
    result = reconcile_turn(B, delivery_id="restart-start")
    assert result["status"] == "settled", result
    assert result["delivery"]["turn_id"] == TURN_N
    assert result["delivery"]["placement"] == "new_turn"
    effect = store.read_receipt(B, "codex-send", "restart-start")
    assert effect["state"] == "accepted"
    assert effect["nativeEvidenceSha256"] == ack_digest(store, receipt, TURN_N)
    assert reconcile_turn(B, turn_id=TURN_N)["delivery"]["delivery_id"] == "restart-start"
    no_native_send(calls)
    assert calls == []  # Settled from the journal: not even a native read.


def test_turn_lookup_recovers_before_calling_a_turn_foreign(bound):
    store, thread, calls = bound
    receipt = lost(store, "by-turn-start")
    journal(*completed_turn(thread, TURN_N)[:1],
            *user_message(thread, TURN_N, receipt["clientUserMessageId"]),
            *completed_turn(thread, TURN_N)[1:])
    result = reconcile_turn(B, turn_id=TURN_N)
    assert result["status"] == "settled" and result["delivery"]["delivery_id"] == "by-turn-start"
    no_native_send(calls)


def test_steer_recovers_placement_only_never_completion(bound):
    store, thread, calls = bound
    receipt = lost(store, "restart-steer", route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]),
            *completed_turn(thread, TURN_R)[1:])
    result = reconcile_turn(B, delivery_id="restart-steer")
    assert result["status"] == "accepted", result
    assert result["pending"] is False
    assert result["delivery"]["placement"] == "current_turn"
    assert result["delivery"]["turn_id"] == TURN_R
    assert result["delivery"]["settlement"] is None
    assert codex_delivery.read_receipt(B, "restart-steer").get("settlement") is None
    no_native_send(calls)


def test_v2_turn_reconcile_projection_of_a_recovered_delivery(bound):
    store, thread, _calls = bound
    receipt = lost(store, "v2-steer", route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]))
    answer = local_rpc.dispatch({
        "protocol_version": 2, "request_id": "p2-v2-reconcile", "op": "turn.reconcile",
        "timeout_ms": 5000, "params": {"target": f"codex:{B}", "delivery_id": "v2-steer"},
    })
    assert answer["ok"] is True, answer
    delivery = answer["result"]["delivery"]
    assert delivery["status"] == "accepted" and delivery["placement"] == "current_turn"
    assert delivery["turn_ref"] == f"ccx-turn:codex:v1:{TURN_R}"
    assert delivery["pending"] is False and delivery["recommended_action"] == "none"


def test_resend_of_a_recovered_token_never_calls_native(bound):
    store, thread, calls = bound
    receipt = lost(store, "resend-steer", route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]))
    reconcile_turn(B, delivery_id="resend-steer")

    def never(*_args):
        pytest.fail("a recovered delivery must never be sent natively again")

    again = send_turn(B, "lost ack", delivery_id="resend-steer", mode="steer_only",
                      _native_submit=never)
    assert again["status"] == "accepted"
    no_native_send(calls)


# -- conservative on anything else ----------------------------------------------

def _foreign_client(_receipt):
    return new_client_message_id(A, "steer")


def _other_client(_receipt):
    return new_client_message_id(B, "steer")


NEGATIVE = {
    "absent_even_after_turn_completed": lambda thread, r: completed_turn(thread, TURN_R),
    "foreign_worker_client": lambda thread, r: user_message(thread, TURN_R, _foreign_client(r)),
    "other_client_same_worker": lambda thread, r: user_message(thread, TURN_R, _other_client(r)),
    "duplicate_items": lambda thread, r: (
        user_message(thread, TURN_R, r["clientUserMessageId"], item_id="um-1")
        + user_message(thread, TURN_R, r["clientUserMessageId"], item_id="um-2")),
    "same_id_on_another_item_type": lambda thread, r: (
        user_message(thread, TURN_R, r["clientUserMessageId"])
        + user_message(thread, TURN_R, r["clientUserMessageId"], item_id="x",
                       kind="agentMessage")),
    "two_turns": lambda thread, r: (
        user_message(thread, TURN_R, r["clientUserMessageId"])
        + user_message(thread, "native-turn-S", r["clientUserMessageId"])),
    "wrong_target_turn": lambda thread, r: user_message(
        thread, "native-turn-S", r["clientUserMessageId"]),
    "wrong_thread": lambda thread, r: user_message(
        "native-thread-foreign", TURN_R, r["clientUserMessageId"]),
}


@pytest.mark.parametrize("case", sorted(NEGATIVE))
def test_missing_or_mismatched_evidence_stays_unknown(bound, case):
    store, thread, calls = bound
    receipt = lost(store, "neg-" + case, route="steer", target=TURN_R)
    journal(*NEGATIVE[case](thread, receipt))
    result = reconcile_turn(B, delivery_id="neg-" + case)
    assert result["status"] == "submitted", (case, result)
    assert result["pending"] is True and result["recommended_action"] == "wait"
    assert store.read_receipt(B, "codex-send", "neg-" + case)["state"] == "submitted-unknown"
    no_native_send(calls)


def test_evidence_in_another_workers_journal_is_not_this_workers(bound):
    store, thread, calls = bound
    receipt = lost(store, "other-journal", route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]), worker=A)
    assert reconcile_turn(B, delivery_id="other-journal")["status"] == "submitted"


@pytest.mark.parametrize("damage", ["partial_tail", "sequence_gap", "recorded_degraded"])
def test_degraded_journal_never_decides(bound, damage):
    store, thread, calls = bound
    receipt = lost(store, "degraded-" + damage, route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]))
    path = store.pointer.generation_root / "workers" / "codex" / B / "evidence.jsonl"
    if damage == "partial_tail":
        with path.open("a") as handle:
            handle.write('{"sequence": 99, "method": "turn/st')
    elif damage == "sequence_gap":
        lines = path.read_text().splitlines()
        path.write_text("\n".join(lines[:1] + lines[2:]) + "\n")
    else:
        rec_path = store.pointer.generation_root / "workers" / "codex" / B / "record.json"
        import json
        rec = json.loads(rec_path.read_text())
        rec["evidence"] = {"status": "degraded", "degradedReason": "lost", "sequence": 1}
        put(rec_path, rec)
    assert reconcile_turn(B, delivery_id="degraded-" + damage)["status"] == "submitted"


def test_missing_or_mismatched_native_intent_stays_unknown(bound):
    store, thread, _calls = bound
    receipt = lost(store, "no-intent", route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]))
    receipts = store.pointer.generation_root / "receipts" / "codex-send" / "codex" / B
    for name in os.listdir(receipts):
        if name.endswith(".json"):
            os.unlink(receipts / name)
    assert reconcile_turn(B, delivery_id="no-intent")["status"] == "submitted"


def test_unconfirmed_source_binding_stays_unknown(bound):
    store, thread, _calls = bound
    receipt = lost(store, "rebound", route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]))
    rec_path = store.pointer.generation_root / "workers" / "codex" / B / "record.json"
    import json
    rec = json.loads(rec_path.read_text())
    rec["native"]["threadSource"] = "ccx-worker:someone-else"
    put(rec_path, rec)
    assert recover(store, B, "rebound")["state"] == "submitted"


def test_stale_generation_store_cannot_accept(bound, short_root):
    store, thread, _calls = bound
    receipt = lost(store, "stale-gen", route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]))
    switch_pointer(short_root, G2)
    with pytest.raises(Exception) as refused:
        recover(store, B, "stale-gen")
    assert isinstance(refused.value, StaleGeneration) or "generation" in str(refused.value).lower()


# -- late acknowledgment and concurrency ---------------------------------------

def test_late_ack_after_recovery_and_recovery_after_ack_converge(bound):
    store, thread, calls = bound
    first = lost(store, "ack-after", route="steer", target=TURN_R)
    second = lost(store, "ack-before", route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, first["clientUserMessageId"], item_id="u1"),
            *user_message(thread, TURN_R, second["clientUserMessageId"], item_id="u2"))
    assert recover(store, B, "ack-after")["state"] == "accepted"
    late_ack(store, first, TURN_R)  # Idempotent: same turn, same digest.
    late_ack(store, second, TURN_R)
    assert recover(store, B, "ack-before")["state"] == "accepted"
    for token in ("ack-after", "ack-before"):
        assert codex_delivery.read_receipt(B, token)["turnId"] == TURN_R
        assert store.read_receipt(B, "codex-send", token)["state"] == "accepted"
    no_native_send(calls)


def test_a_late_ack_naming_another_turn_is_a_conflict_not_overwritten(bound):
    store, thread, _calls = bound
    receipt = lost(store, "ack-conflict", route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]))
    recover(store, B, "ack-conflict")
    with pytest.raises(codex_delivery.DeliveryReceiptConflict):
        codex_delivery.mark_accepted(B, "ack-conflict", turn_id="native-turn-other")
    assert codex_delivery.read_receipt(B, "ack-conflict")["turnId"] == TURN_R


def test_concurrent_resolvers_and_late_ack_leave_one_answer(bound):
    store, thread, _calls = bound
    receipt = lost(store, "concurrent", route="new_turn")
    journal(*completed_turn(thread, TURN_N)[:1],
            *user_message(thread, TURN_N, receipt["clientUserMessageId"]),
            *completed_turn(thread, TURN_N)[1:])
    errors: list[BaseException] = []
    start = threading.Barrier(9)

    def run(action):
        try:
            start.wait()
            action()
        except BaseException as error:  # noqa: BLE001
            errors.append(error)

    from ccx.store.root import GenerationBusy
    actions = [lambda: recover(store, B, "concurrent")] * 8 + [lambda: late_ack(store, receipt, TURN_N)]
    threads = [threading.Thread(target=run, args=(a,)) for a in actions]
    for t in threads:
        t.start()
    for t in threads:
        t.join(20)
    assert all(isinstance(e, GenerationBusy) for e in errors), errors
    final = reconcile_turn(B, delivery_id="concurrent")
    assert final["status"] == "settled" and final["delivery"]["turn_id"] == TURN_N
    effect = store.read_receipt(B, "codex-send", "concurrent")
    assert effect["state"] == "accepted"
    assert effect["nativeEvidenceSha256"] == ack_digest(store, receipt, TURN_N)


def test_busy_native_intent_is_completed_by_a_later_reconcile(bound):
    """While a dispatching caller holds the native intent, the receipt is
    accepted but the intent stays submitted-unknown; a later reconcile
    completes it (and the settlement that depends on it)."""
    from ccx.store.root import _flock_at
    import hashlib
    store, thread, _calls = bound
    receipt = lost(store, "busy-intent")
    journal(*completed_turn(thread, TURN_N)[:1],
            *user_message(thread, TURN_N, receipt["clientUserMessageId"]),
            *completed_turn(thread, TURN_N)[1:])
    key = hashlib.sha256(b"busy-intent").hexdigest()
    with store.lease():
        with store._gen("receipts", "codex-send", "codex", B) as fd:
            with _flock_at(fd, key + ".lock", exclusive=True):
                held = recover(store, B, "busy-intent")
    assert held["state"] == "accepted" and held.get("settlement") is None
    assert store.read_receipt(B, "codex-send", "busy-intent")["state"] == "submitted-unknown"
    final = reconcile_turn(B, delivery_id="busy-intent")
    assert final["status"] == "settled"
    assert store.read_receipt(B, "codex-send", "busy-intent")["state"] == "accepted"


# -- generation cutover at the acceptance boundary ------------------------------

def _cooperating_cutover(root, generation, *, timeout=.3) -> bool:
    """What a real cutover runner does: LOCK_EX on the never-replaced
    activation lock, then flip the pointer. False when a lease refuses it."""
    from ccx.store.root import GenerationBusy, _flock_at, _open_dir
    try:
        with _open_dir(root, "identity") as identity:
            with _flock_at(identity, "activation.lock", exclusive=True, timeout=timeout):
                switch_pointer(root, generation)
                return True
    except GenerationBusy:
        return False


def _twin_generations(store, short_root, thread):
    """G1 and G2 both hold worker B with a lost, still-submitted 'twin'."""
    from test_generation_store import G1
    receipt = lost(store, "twin", route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]))
    switch_pointer(short_root, G2)
    g2 = add_worker(short_root, B, "codex", confirmed=True)
    lost(g2, "twin", route="steer", target=TURN_R)
    switch_pointer(short_root, G1)
    g1 = require_ccx_active(short_root)
    assert g1.generation == G1 and g1.generation == store.generation
    return g1, g2


@pytest.mark.parametrize("boundary", ["mark_accepted", "settle_native_intent"])
def test_cutover_at_the_final_write_boundary_never_crosses_generations(
        bound, short_root, monkeypatch, boundary):
    from test_generation_store import G1
    store, thread, _calls = bound
    g1, g2 = _twin_generations(store, short_root, thread)
    attempts = []
    with monkeypatch.context() as failpoint:
        if boundary == "mark_accepted":
            real = codex_delivery.mark_accepted

            def cut_then_accept(*args, **kwargs):
                attempts.append(_cooperating_cutover(short_root, G2))
                return real(*args, **kwargs)

            failpoint.setattr(codex_delivery, "mark_accepted", cut_then_accept)
        else:
            real = type(g1).settle_native_intent

            def cut_then_settle(self, *args, **kwargs):
                attempts.append(_cooperating_cutover(short_root, G2))
                return real(self, *args, **kwargs)

            failpoint.setattr(type(g1), "settle_native_intent", cut_then_settle)
        try:
            recover(g1, B, "twin")
        except Exception:  # noqa: BLE001 - a refusal is an acceptable outcome
            pass
    # Whatever happened, G2's twin was never touched with G1's evidence.
    pointer_now = require_ccx_active(short_root).generation
    switch_pointer(short_root, G2)
    assert codex_delivery.read_receipt(B, "twin")["state"] == "submitted"
    assert g2.read_receipt(B, "codex-send", "twin")["state"] == "submitted-unknown"
    switch_pointer(short_root, G1)
    g1_receipt = codex_delivery.read_receipt(B, "twin")
    g1_effect = g1.read_receipt(B, "codex-send", "twin")
    # The original lease pinned G1 across both writes: the cooperating
    # cutover was refused at the boundary, and acceptance completed in G1.
    assert attempts == [False]
    assert pointer_now == G1
    assert g1_receipt["state"] == "accepted" and g1_effect["state"] == "accepted"


# -- terminal global proof must be the derived placement proof ------------------

@pytest.mark.parametrize("proof", ["same", "different", "malformed"])
def test_terminal_global_proof_must_match_the_derived_placement(bound, proof):
    store, thread, calls = bound
    receipt = lost(store, "proof-" + proof, route="steer", target=TURN_R)
    journal(*user_message(thread, TURN_R, receipt["clientUserMessageId"]))
    effect = store.read_receipt(B, "codex-send", "proof-" + proof)
    sha = {"same": ack_digest(store, receipt, TURN_R),
           "different": ack_digest(store, receipt, "native-turn-elsewhere"),
           "malformed": "not-a-digest"}[proof]
    key_dir = store.pointer.generation_root / "receipts" / "codex-send" / "codex" / B
    import hashlib
    import json
    path = key_dir / (hashlib.sha256(("proof-" + proof).encode()).hexdigest() + ".json")
    put(path, {**effect, "state": "accepted", "nativeEvidenceSha256": sha})
    result = reconcile_turn(B, delivery_id="proof-" + proof)
    if proof == "same":
        # Crash repair: the global proof IS this placement.
        assert result["status"] == "accepted" and result["delivery"]["turn_id"] == TURN_R
        late_ack_semantic = codex_delivery.mark_accepted(B, "proof-same", turn_id=TURN_R)
        assert late_ack_semantic["turnId"] == TURN_R  # A late ACK still converges.
    else:
        assert result["status"] == "submitted", result
        assert codex_delivery.read_receipt(B, "proof-" + proof)["state"] == "submitted"
    assert json.loads(path.read_text())["nativeEvidenceSha256"] == sha  # Never rewritten.
    no_native_send(calls)
