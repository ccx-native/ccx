"""Original Claude semantic receipts on global full UUID storage."""
from __future__ import annotations

import multiprocessing
import os
from pathlib import Path
import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from test_generation_store import A, B, G1, fixture_root
from ccx.claude import delivery as delivery
from ccx import constants
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from ccx.store.identity import InvalidTarget, UnknownWorker


@pytest.fixture
def owners(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    store = require_ccx_active(root)
    for wid in (A, B):
        prepare_worker(store, engine="claude", cwd=str(tmp_path),
                       worker_id=wid, name="colliding name")
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    return root, store


def test_semantic_original_claude_delivery_accept_retry(owners):
    root, store = owners
    initial, created = delivery.prepare(
        A, "token", "hello", mode="new_turn_only")
    assert created and initial["workerId"] == A
    assert initial["engine"] == "claude"
    assert "workerShort" not in initial
    assert delivery.read_receipt(A, "token") == initial
    owned = delivery.mark_owned(A, "token", route="new_turn")
    assert owned["state"] == "owned"
    attempt = delivery.mark_attempt(A, "token", maybe_written=True)
    assert attempt["attempts"] == 1 and attempt["maybeWritten"]
    accepted = delivery.mark_accepted(
        A, "token", placement="new_turn",
        turn_ref="ccx-turn:claude:v1:original-turn",
        evidence="real-native-positive-evidence",
    )
    assert accepted["state"] == "accepted"
    same, created = delivery.prepare(A, "token", "hello",
                                     mode="new_turn_only")
    assert not created and same == accepted
    assert delivery.public_receipt(same)["status"] == "accepted"
    assert delivery.public_receipt(same)["recommended_action"] == "none"
    assert not (root / "operations").exists()
    root_ = root / "identity" / "generations" / G1 / "semantic-deliveries"
    assert len(list((root_ / "claude" / A).glob("*.json"))) == 1


def test_claude_same_prefix_separate_receipts_and_conflicting_spec(owners):
    root, store = owners
    first, _ = delivery.prepare(A, "same-token", "first", mode="new_turn_only")
    second, _ = delivery.prepare(B, "same-token", "second", mode="new_turn_only")
    assert first["workerId"] == A and second["workerId"] == B
    assert first["promptSha256"] != second["promptSha256"]
    with pytest.raises(delivery.ClaudeDeliveryConflict):
        delivery.prepare(A, "same-token", "second", mode="new_turn_only")
    assert delivery.read_receipt(B, "same-token")["promptText"] == "second"


def test_claude_rejected_semantic_retry_keeps_original(owners):
    root, store = owners
    delivery.prepare(A, "id", "text", mode="new_turn_only")
    rejected = delivery.mark_rejected(A, "id", code="verified-denial",
                                     message="rejected")
    prior, again = delivery.prepare(A, "id", "text", mode="new_turn_only")
    assert not again and prior == rejected
    assert delivery.public_receipt(prior)["status"] == "rejected"
    with pytest.raises(delivery.ClaudeDeliveryError):
        delivery.mark_accepted(A, "id", placement="new_turn",
                               turn_ref="turn-untrusted")


def test_claude_unowned_alias_no_lock_files(owners):
    root, store = owners
    for target in ("aaaaaaaa", "display name", "claude:aaaaaaaa"):
        with pytest.raises(InvalidTarget):
            delivery.prepare(target, "id", "text", mode="new_turn_only")
    with pytest.raises(UnknownWorker):
        delivery.prepare("bbbbbbbb-1111-4111-8111-111111111111",
                         "id", "text", mode="new_turn_only")
    assert not (root / "identity" / "generations" / G1 /
                "semantic-deliveries").exists()


def _parallel(args):
    root, wid, ready, release, results = args
    from ccx.claude import delivery as claude_delivery
    from ccx import constants
    constants.CCX_ROOT = Path(root)
    ready.put(os.getpid())
    release.wait(10)
    result, new = claude_delivery.prepare(
        wid, "parallel-id", "same-text", mode="new_turn_only")
    results.put((new, result["workerId"], result["promptSha256"]))


def test_claude_same_token_race_one_preparer_four_real_processes(owners):
    from ccx.system import oslayer
    root, store = owners
    # Keep the four-process original-token race; use clean spawned children
    # on macOS rather than forking another thread's locks from pytest-xdist.
    mp = multiprocessing.get_context(
        "spawn" if oslayer.UNIX_SUN_PATH_BYTES == 104 else "fork")
    manager = mp.Manager()
    ready, results, release = manager.Queue(), manager.Queue(), manager.Event()
    procs = [
        mp.Process(target=_parallel,
                   args=((str(root), A, ready, release, results),))
        for _ in range(4)
    ]
    try:
        for proc in procs:
            proc.start()
        for _ in procs:
            ready.get(timeout=10)
        release.set()
        outcomes = [results.get(timeout=10) for _ in procs]
        assert sum(bool(new) for new, _owner, _sha in outcomes) == 1
        assert {wid for _new, wid, _sha in outcomes} == {A}
        assert len({sha for _new, _wid, sha in outcomes}) == 1
        for proc in procs:
            proc.join(10)
            assert proc.exitcode == 0
    finally:
        release.set()
        for proc in procs:
            if proc.is_alive():
                proc.terminate()
                proc.join(3)
        manager.shutdown()



def _activate_fake_claude(store, worker):
    from ccx.ops.creation_steps import activate_prepared_worker
    invoked = []
    def start_native(budget):
        fence = store.native_activation(worker.worker)
        assert fence["state"] == "submitted-unknown"
        invoked.append(fence["submissionId"])
        return {
            "accepted": True,
            "workerId": worker.worker_id,
            "engine": "claude",
            "source": worker.native_source,
            "submissionId": fence["submissionId"],
            "nativeEvidenceSha256": "b" * 64,
            "nativeBinding": {"sessionId": worker.record["native"]["sessionId"]},
        }
    confirmed = activate_prepared_worker(
        store, worker, bounded_native_submit=start_native)
    assert confirmed.activation_state == "confirmed"
    assert store.authorize_native_action(worker.worker, "send").worker_id == worker.worker_id
    assert len(invoked) == 1
    return confirmed


def test_original_claude_vertical_new_native_send_settlement_report_retry(owners):
    from ccx.ops.send import send_turn
    from ccx.ops.wait import reconcile_turn
    from ccx.ops.report import report_worker
    import fcntl
    from ccx.store.root import NativeEffectIndeterminate
    root, store = owners
    prepared = prepare_worker(
        store, engine="claude", cwd=str(root.parent), worker_id=A,
        name="colliding name")
    confirmed = _activate_fake_claude(store, prepared)
    assert confirmed.worker_id == A
    sid = confirmed.record["native"]["sessionId"]
    called = []
    observed = []
    delivery_id = "caller-known-original-id"
    turn_ref = "ccx-turn:claude:v1:original-turn-abc"
    def submit_native(handle, native_id, prompt, token, budget):
        assert handle == f"claude:{A}"
        assert native_id == sid
        assert (prompt, token) == ("hello managed", delivery_id)
        assert 0 < budget <= 2
        receipt = store.read_receipt(handle, "claude-delivery", token)
        assert receipt["state"] == "submitted-unknown"
        called.append((handle, token))
        return {
            "accepted": True, "workerId": A, "engine": "claude",
            "deliveryId": delivery_id, "nativeSource": "claude-session:" + sid,
            "sessionId": sid, "turnRef": turn_ref, "nativeEvidenceSha256": "a"*64,
        }
    def settle_native(handle, original_ref):
        assert (handle, original_ref) == (f"claude:{A}", turn_ref)
        # Native completion wait may take an arbitrary time. No activation
        # lock must be held while querying this independent evidence.
        fd = os.open(root / "identity" / "activation.lock", os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        observed.append(original_ref)
        return {
            "workerId": A, "nativeSource": "claude-session:" + sid,
            "deliveryId": delivery_id, "turnRef": turn_ref,
            "status": "completed", "text": "THE actual fake provider answer",
            "nativeEvidenceSha256": "c"*64,
            "usage": {"inputTokens": 4, "outputTokens": 5},
        }
    sent = send_turn(
        f"claude:{A}", "hello managed", delivery_id=delivery_id,
        _native_submit=submit_native, _observe_settlement=settle_native,
    )
    assert sent["status"] == "accepted" and sent["settled"]
    assert sent["worker"]["worker_id"] == A
    assert sent["delivery"]["status"] == "accepted"
    assert sent["delivery"]["turn_ref"] == turn_ref
    assert sent["delivery"]["settlement"]["status"] == "completed"
    assert called == [(f"claude:{A}", delivery_id)]
    assert observed == [turn_ref]
    probe = reconcile_turn(A, delivery_id=delivery_id)
    assert probe["status"] == "settled"
    assert probe["delivery"]["delivery_id"] == delivery_id
    assert probe["delivery"]["settlement"]["text"] == "THE actual fake provider answer"
    report = report_worker(A, turn_ref=turn_ref, max_chars=10)
    assert report["worker"] == f"claude:{A}"
    assert report["text"] == "THE actual"
    assert report["truncated"] is True
    assert report["delivery_id"] == delivery_id
    repeat = send_turn(
        A, "hello managed", delivery_id=delivery_id,
        _native_submit=submit_native, _observe_settlement=settle_native,
    )
    assert repeat == sent
    assert called == [(f"claude:{A}", delivery_id)]
    assert observed == [turn_ref]
    assert len(list((root / "identity" / "generations" / G1 /
                     "semantic-deliveries" / "claude" / A).glob("*.json"))) == 1
    assert not (root / "operations").exists()
    assert not (root / "sessions").exists()
    with pytest.raises(delivery.ClaudeDeliveryConflict):
        send_turn(A, "changed content", delivery_id=delivery_id,
                  _native_submit=submit_native)
    assert len(called) == 1


def test_claude_lost_native_send_reply_never_replays_original(owners):
    from ccx.ops.send import send_turn
    from ccx.ops.report import report_worker
    from ccx.store.root import NativeEffectIndeterminate
    from ccx.errors import SessionUnavailable
    root, store = owners
    prepared = prepare_worker(store, engine="claude", cwd=str(root.parent),
                              worker_id=A, name="colliding name")
    _activate_fake_claude(store, prepared)
    calls = []
    def lost(handle, sid, text, token, budget):
        calls.append("original provider may have accepted")
        raise TimeoutError("provider reply lost")
    with pytest.raises(NativeEffectIndeterminate):
        send_turn(A, "text", delivery_id="lost-id", _native_submit=lost)
    assert calls == ["original provider may have accepted"]
    original = delivery.read_receipt(A, "lost-id")
    # Without a host ownership acknowledgement, an interrupted IPC cannot
    # pretend the semantic journal itself was owned or written. The separately
    # fsynced native-effect fence is still indeterminate and disallows replay.
    assert original["state"] == "prepared" and not original["maybeWritten"]
    assert store.read_receipt(A, "claude-delivery", "lost-id")["state"] == "submitted-unknown"
    with pytest.raises(NativeEffectIndeterminate):
        send_turn(A, "text", delivery_id="lost-id", _native_submit=lost)
    assert len(calls) == 1
    with pytest.raises(SessionUnavailable):
        report_worker(A)


def test_codex_prepared_native_worker_refuses_without_effect_sidecars(codex_owners):
    from ccx.ops.send import send_turn
    from ccx.store.root import NativeActionDenied
    root, store = codex_owners
    # Real codexd transport exists, but no original threadId was created or
    # positively bound for this merely prepared worker. This refusal precedes
    # all semantic receipts, native effects and app-server requests.
    for target in (A, f"codex:{A}"):
        with pytest.raises(NativeActionDenied, match="not ready"):
            send_turn(target, "start", delivery_id="never-started")
    gen = root / "identity" / "generations" / G1
    assert not (gen / "receipts").exists()
    assert not (gen / "semantic-deliveries").exists()
    assert not (root / "run").exists()


def test_claude_unactivated_native_send_refuses_without_writing_receipts(owners):
    from ccx.ops.send import send_turn
    from ccx.store.root import NativeActionDenied
    root, store = owners
    # The normal host transport is now connected, but a merely prepared
    # worker has no positive original native activation and cannot send.
    # This refusal must precede even the first receipt-sidecar write.
    with pytest.raises(NativeActionDenied, match="native activation"):
        send_turn(A, "text", delivery_id="not-started")
    assert not (root / "identity" / "generations" / G1 /
                "semantic-deliveries").exists()
    assert not (root / "identity" / "generations" / G1 /
                "receipts").exists()



@pytest.fixture
def codex_owners(tmp_path, monkeypatch):
    from ccx.codex import delivery as codex_delivery
    root = fixture_root(tmp_path)
    store = require_ccx_active(root)
    for wid in (A, B):
        prepare_worker(store, engine="codex", cwd=str(tmp_path),
                       worker_id=wid, name="same name")
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    return root, store


def test_original_codex_receipt_full_UUID_prepared_submitted_accepted(codex_owners):
    from ccx.codex import delivery as codex_delivery
    root, store = codex_owners
    initial, created = codex_delivery.prepare(
        A, "call-1", "text", mode="new_turn_only")
    assert created and initial["workerId"] == A
    assert initial["engine"] == "codex"
    assert initial["state"] == "prepared"
    assert "workerShort" not in initial
    submitted = codex_delivery.mark_submitted(A, "call-1",
                                               runtime_id="original-instance-1")
    assert submitted["state"] == "submitted"
    assert submitted["clientUserMessageId"] == initial["clientUserMessageId"]
    accepted = codex_delivery.mark_accepted(
        A, "call-1", turn_id="native-turn-one")
    assert accepted["state"] == "accepted"
    assert accepted["turnRef"] == "ccx-turn:codex:v1:native-turn-one"
    assert codex_delivery.receipt_for_turn(A, "native-turn-one") == accepted
    reply, created = codex_delivery.prepare(
        A, "call-1", "text", mode="new_turn_only")
    assert not created and reply == accepted
    assert codex_delivery.public_receipt(reply)["status"] == "accepted"
    assert not (root / "codex").exists()
    assert not (root / "operations").exists()


def test_original_codex_receipts_same_prefix_and_no_short_alias(codex_owners):
    from ccx.codex import delivery as codex_delivery
    root, _store = codex_owners
    first, _ = codex_delivery.prepare(A, "same-id", "first")
    second, _ = codex_delivery.prepare(B, "same-id", "second")
    assert first["workerId"] == A and second["workerId"] == B
    assert first["promptSha256"] != second["promptSha256"]
    with pytest.raises(codex_delivery.DeliveryReceiptConflict):
        codex_delivery.prepare(A, "same-id", "second")
    for bad in ("aaaaaaaa", "codex:aaaaaaaa", "same name"):
        with pytest.raises(InvalidTarget):
            codex_delivery.prepare(bad, "fresh", "text")
    assert not (root / "codex").exists()



def _activate_fake_codex(store, worker):
    from ccx.ops.creation_steps import activate_prepared_worker
    calls = []
    def native_start(budget):
        pending = store.native_activation(worker.worker)
        assert pending["state"] == "submitted-unknown"
        calls.append(pending["submissionId"])
        return {
            "accepted": True, "workerId": worker.worker_id,
            "engine": "codex", "source": worker.native_source,
            "submissionId": pending["submissionId"],
            "nativeEvidenceSha256": "d"*64,
            "nativeBinding": {"threadId": "native-thread-"+worker.worker_id},
        }
    confirmed = activate_prepared_worker(
        store, worker, bounded_native_submit=native_start,
    )
    assert confirmed.activation_state == "confirmed"
    assert len(calls) == 1
    assert store.authorize_native_action(worker.worker, "send").worker_id == worker.worker_id
    return confirmed


def test_codex_second_vertical_new_native_turn_settle_report_same_ID_retry(codex_owners):
    from ccx.ops.send import send_turn
    from ccx.ops.wait import reconcile_turn
    from ccx.ops.report import report_worker
    from ccx.codex import delivery as codex_delivery
    from ccx.codex.provenance import parse_client_message_id
    import fcntl
    root, store = codex_owners
    prepared = prepare_worker(
        store, engine="codex", cwd=str(root.parent), worker_id=B)
    confirmed = _activate_fake_codex(store, prepared)
    native_thread = confirmed.record["native"]["threadId"]
    assert native_thread == "native-thread-"+B
    calls, observed = [], []
    turn_id = "codex-native-turn-88"
    delivery_id = "codex-original-token"
    native_ref = "ccx-turn:codex:v1:"+turn_id

    def native_send(handle, thread, prompt, token, budget):
        assert (handle, thread, prompt, token) == (
            f"codex:{B}", native_thread, "Do a task", delivery_id)
        assert 0 < budget <= 2
        global_receipt = store.read_receipt(handle, "codex-send", token)
        original = codex_delivery.read_receipt(handle, token)
        assert global_receipt["state"] == "submitted-unknown"
        assert original["state"] == "submitted"
        client_message_id = original["clientUserMessageId"]
        provenance = parse_client_message_id(client_message_id)
        assert provenance["workerId"] == B
        assert provenance["kind"] == "start"
        calls.append(client_message_id)
        return {
            "accepted": True, "workerId": B, "engine": "codex",
            "deliveryId": token, "nativeSource": f"ccx-worker:{B}",
            "threadId": native_thread, "turnId": turn_id,
            "clientUserMessageId": client_message_id,
            "nativeEvidenceSha256": "a"*64,
        }

    def native_settlement(handle, ref):
        assert (handle, ref) == (f"codex:{B}", native_ref)
        fd = os.open(root / "identity" / "activation.lock", os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        observed.append(ref)
        return {
            "workerId": B, "nativeSource": f"ccx-worker:{B}",
            "deliveryId": delivery_id, "threadId": native_thread,
            "turnId": turn_id, "status": "completed",
            "text": "Native Codex completed task correctly",
            "nativeEvidenceSha256": "b"*64,
            "usage": {"inputTokens": 8, "outputTokens": 13},
        }

    sent = send_turn(
        f"codex:{B}", "Do a task", delivery_id=delivery_id,
        cap={"5h": 60, "7d": 80},
        _native_submit=native_send, _observe_settlement=native_settlement)
    assert sent["status"] == "accepted" and sent["settled"]
    assert sent["worker"]["worker"] == f"codex:{B}"
    assert sent["delivery"]["turn_id"] == turn_id
    assert sent["delivery"]["turn_ref"] == native_ref
    assert sent["delivery"]["cap"] == {"5h": 60, "7d": 80}
    assert codex_delivery.read_receipt(B, delivery_id)["cap"] == {"5h": 60, "7d": 80}
    assert len(calls) == 1 and observed == [native_ref]
    recon = reconcile_turn(B, delivery_id=delivery_id)
    assert recon["status"] == "settled"
    assert recon["delivery"]["client_user_message_id"] == calls[0]
    by_turn = reconcile_turn(B, turn_id=turn_id)
    assert by_turn["delivery"]["delivery_id"] == delivery_id
    report = report_worker(B, turn_ref=native_ref)
    assert report["turn_id"] == turn_id
    assert report["text"] == "Native Codex completed task correctly"
    assert report["source"] == "original-native-completion-receipt"
    replay = send_turn(
        B, "Do a task", delivery_id=delivery_id,
        cap={"5h": 60, "7d": 80},
        _native_submit=native_send, _observe_settlement=native_settlement)
    assert replay == sent
    assert len(calls) == 1
    assert observed == [native_ref]
    assert not (root / "codex").exists()
    assert not (root / "operations").exists()
    from ccx.errors import OperationConflict
    # Public send surfaces the accepted N1C OperationConflict; the internal
    # receipt conflict remains its cause, before any second native effect.
    with pytest.raises(OperationConflict) as conflict:
        send_turn(B, "different prompt", delivery_id=delivery_id,
                  _native_submit=native_send)
    assert isinstance(conflict.value.__cause__, codex_delivery.DeliveryReceiptConflict)
    with pytest.raises(OperationConflict) as changed_cap:
        send_turn(B, "Do a task", delivery_id=delivery_id,
                  cap={"5h": 90, "7d": 80}, _native_submit=native_send)
    assert isinstance(changed_cap.value.__cause__, codex_delivery.DeliveryReceiptConflict)
    assert len(calls) == 1


def test_codex_original_lost_native_reply_fenced_and_not_replayed(codex_owners):
    from ccx.ops.send import send_turn
    from ccx.ops.report import report_worker
    from ccx.store.root import NativeEffectIndeterminate
    from ccx.errors import SendIndeterminate, SessionUnavailable
    from ccx.codex import delivery as codex_delivery
    root, store = codex_owners
    prepared = prepare_worker(store, engine="codex",
                              cwd=str(root.parent), worker_id=A)
    _activate_fake_codex(store, prepared)
    called = []
    def lost(handle, thread, prompt, token, budget):
        called.append("submitted original")
        raise TimeoutError("native turn/start reply was lost")
    # Public send raises the token-bearing SendIndeterminate (N1C); the
    # internal native-effect fence remains its cause.
    with pytest.raises(SendIndeterminate) as lost_once:
        send_turn(A, "text", delivery_id="lost", _native_submit=lost)
    assert isinstance(lost_once.value.__cause__, NativeEffectIndeterminate)
    assert lost_once.value.delivery_id == "lost"
    assert called == ["submitted original"]
    assert codex_delivery.read_receipt(A, "lost")["state"] == "submitted"
    assert store.read_receipt(A, "codex-send", "lost")["state"] == "submitted-unknown"
    with pytest.raises(SendIndeterminate) as lost_again:
        send_turn(A, "text", delivery_id="lost", _native_submit=lost)
    assert isinstance(lost_again.value.__cause__, NativeEffectIndeterminate)
    assert called == ["submitted original"]
    with pytest.raises(SessionUnavailable):
        report_worker(A)


def test_codex_native_response_wrong_original_thread_fences_unknown(codex_owners):
    from ccx.ops.send import send_turn
    from ccx.store.root import NativeEffectIndeterminate
    from ccx.errors import SendIndeterminate
    from ccx.codex import delivery as codex_delivery
    root, store = codex_owners
    prepared = prepare_worker(store, engine="codex", cwd=str(root.parent),
                              worker_id=A)
    _activate_fake_codex(store, prepared)
    called = []
    def forged(handle, thread, prompt, token, budget):
        called.append("original")
        receipt = codex_delivery.read_receipt(handle, token)
        return {
            "accepted": True, "workerId": A, "engine": "codex",
            "deliveryId": token, "nativeSource": f"ccx-worker:{A}",
            "threadId": "foreign-thread", "turnId": "wrong-native-turn",
            "clientUserMessageId": receipt["clientUserMessageId"],
            "nativeEvidenceSha256": "f"*64,
        }
    for _attempt in range(2):
        with pytest.raises(SendIndeterminate) as fenced:
            send_turn(A, "text", delivery_id="wrong-native", _native_submit=forged)
        assert isinstance(fenced.value.__cause__, NativeEffectIndeterminate)
        assert fenced.value.delivery_id == "wrong-native"
    assert called == ["original"]



def test_codex_capped_admission_rejects_as_budget_stopped_before_turn_start(codex_owners):
    from ccx.codex import delivery as codex_delivery
    from ccx.errors import BudgetStopped
    from ccx.ops.send import send_turn
    root, store = codex_owners
    prepared = prepare_worker(store, engine="codex", cwd=str(root.parent), worker_id=B)
    confirmed = _activate_fake_codex(store, prepared)
    original_thread = confirmed.record["native"]["threadId"]
    calls = []

    def denied(handle, native_thread, prompt, token, timeout):
        assert native_thread == original_thread
        assert prompt == "quota-bounded" and token == "cap-admission"
        calls.append(token)
        receipt = codex_delivery.read_receipt(handle, token)
        return {
            "accepted": False, "nativeStatus": "rejected",
            "rejectionCode": "budget-stopped",
            "rejectionMessage": "usage cap 7d already exceeded (60% >= 50%); no turn delivered",
            "workerId": B, "engine": "codex",
            "deliveryId": token, "nativeSource": f"ccx-worker:{B}",
            "threadId": original_thread,
            "clientUserMessageId": receipt["clientUserMessageId"],
            "nativeEvidenceSha256": "a" * 64,
        }

    for _ in range(2):
        with pytest.raises(BudgetStopped, match="no turn delivered") as stopped:
            send_turn(f"codex:{B}", "quota-bounded", delivery_id="cap-admission",
                      cap={"7d": 50}, _native_submit=denied)
        assert stopped.value.phase == "preflight"
    assert calls == ["cap-admission"]
    assert codex_delivery.read_receipt(f"codex:{B}", "cap-admission")["state"] == "rejected"


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_both_engines_positive_native_rejection_retries_original_ID(request, engine):
    from ccx.ops.send import send_turn
    from ccx.ops.wait import reconcile_turn
    from ccx.codex import delivery as codex_delivery
    if engine == "claude":
        root, store = request.getfixturevalue("owners")
        wid = A
        prepared = prepare_worker(store, engine=engine,
                                  cwd=str(root.parent), worker_id=wid)
        confirmed = _activate_fake_claude(store, prepared)
        native_id = confirmed.record["native"]["sessionId"]
        family = "claude-delivery"
        source = "claude-session:"+native_id
    else:
        root, store = request.getfixturevalue("codex_owners")
        wid = B
        prepared = prepare_worker(store, engine=engine,
                                  cwd=str(root.parent), worker_id=wid)
        confirmed = _activate_fake_codex(store, prepared)
        native_id = confirmed.record["native"]["threadId"]
        family = "codex-send"
        source = f"ccx-worker:{wid}"
    calls = []
    token = "id-original-refused"
    def reject_native(handle, nid, prompt, delivery_id, budget):
        assert (handle, nid, delivery_id) == (f"{engine}:{wid}", native_id, token)
        calls.append(delivery_id)
        result = {
            "accepted": False, "nativeStatus": "rejected",
            "workerId": wid, "engine": engine,
            "deliveryId": token, "nativeSource": source,
            "nativeEvidenceSha256": "a"*64,
            "rejectionCode": "native-not-idle",
            "rejectionMessage": "definitive native refusal",
        }
        if engine == "claude":
            result["sessionId"] = native_id
        else:
            result["threadId"] = native_id
            result["clientUserMessageId"] = codex_delivery.read_receipt(
                handle, token)["clientUserMessageId"]
        return result
    first = send_turn(f"{engine}:{wid}", "original",
                      delivery_id=token, _native_submit=reject_native)
    assert first["status"] == "rejected"
    assert first["delivery"]["rejection_code"] == "native-not-idle"
    assert first["delivery"]["recommended_action"] == "none"
    again = send_turn(wid, "original", delivery_id=token,
                      _native_submit=reject_native)
    assert again == first and calls == [token]
    receipt = store.read_receipt(wid, family, token)
    assert receipt["state"] == "rejected"
    probe = reconcile_turn(wid, delivery_id=token)
    assert probe["status"] == "rejected"
    assert not probe["pending"]


def test_claude_after_accepted_turn_wait_lost_then_reobserve_without_resend(owners):
    from ccx.ops.send import send_turn
    from ccx.ops.report import report_worker
    root, store = owners
    prepared = prepare_worker(
        store, engine="claude", cwd=str(root.parent), worker_id=A)
    prepared = _activate_fake_claude(store, prepared)
    sid = prepared.record["native"]["sessionId"]
    calls = []
    ref = "ccx-turn:claude:v1:one-original-turn"
    def submit_native(handle, native_id, text, token, budget):
        calls.append(token)
        return {
            "accepted": True, "workerId": A, "engine": "claude",
            "deliveryId": token, "nativeSource": "claude-session:"+sid,
            "sessionId": sid, "turnRef": ref,
            "nativeEvidenceSha256": "a"*64,
        }
    def lost_completion(handle, turn_ref):
        raise TimeoutError("model-turn observation failed, NOT a native send")
    with pytest.raises(TimeoutError):
        send_turn(A, "text", delivery_id="observed-later",
                  _native_submit=submit_native,
                  _observe_settlement=lost_completion)
    assert calls == ["observed-later"]
    original = delivery.read_receipt(A, "observed-later")
    assert original["state"] == "accepted"
    assert original.get("settlement") is None
    with pytest.raises(Exception):
        report_worker(A)
    def positive(handle, turn_ref):
        assert (handle, turn_ref) == (f"claude:{A}", ref)
        return {
            "workerId": A, "nativeSource": "claude-session:"+sid,
            "deliveryId": "observed-later", "turnRef": ref,
            "status": "completed", "text": "original delayed reply",
            "nativeEvidenceSha256": "b"*64,
        }
    retry = send_turn(A, "text", delivery_id="observed-later",
                      _native_submit=submit_native,
                      _observe_settlement=positive)
    assert retry["settled"] and calls == ["observed-later"]
    assert report_worker(A)["text"] == "original delayed reply"


def test_two_engine_same_prefix_same_semantic_id_has_distinct_native_receipts(tmp_path, monkeypatch):
    from ccx.ops.create import create_worker
    from ccx.ops.send import send_turn
    from ccx.ops.creation_steps import prepare_worker
    from ccx.errors import CreatePartialFailure
    from ccx.codex import delivery as codex_delivery
    root = fixture_root(tmp_path)
    store = require_ccx_active(root)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    from ccx.ops import creation
    from ccx.store.root import NativeEffectIndeterminate
    def synthetic_failure(*_args, **_kwargs):
        raise NativeEffectIndeterminate("injected pre-submission activation failure")
    monkeypatch.setattr(creation, "_complete_claude_create", synthetic_failure)
    monkeypatch.setattr(creation, "_complete_codex_create", synthetic_failure)
    # This is the ACTUAL public V1 protocol creation entrypoint, rather than
    # a test helper constructing records on behalf of the runtime.
    for engine, worker in (("claude", A), ("codex", B)):
        with pytest.raises(CreatePartialFailure) as pending:
            create_worker(engine=engine, cwd=str(tmp_path),
                          worker_id=worker, name="same collision")
        assert pending.value.worker_id == worker
        assert pending.value.phase == "native-activation"
        assert store.resolve(worker).engine == engine
    from ccx.constants import DEFAULT_IDLE_TIMEOUT_S
    first = prepare_worker(
        store, engine="claude", cwd=str(tmp_path),
        worker_id=A, idle_timeout=DEFAULT_IDLE_TIMEOUT_S)
    second = prepare_worker(
        store, engine="codex", cwd=str(tmp_path),
        worker_id=B, idle_timeout=DEFAULT_IDLE_TIMEOUT_S)
    assert first.reconciled and second.reconciled
    cl = _activate_fake_claude(store, first)
    cx = _activate_fake_codex(store, second)
    session = cl.record["native"]["sessionId"]
    thread = cx.record["native"]["threadId"]
    token = "identical-caller-operation"
    calls = []
    def fake(handle, native_id, prompt, delivery, budget):
        calls.append(handle)
        common = {
            "accepted": True, "workerId": handle.split(":", 1)[1],
            "engine": handle.split(":", 1)[0], "deliveryId": delivery,
            "nativeEvidenceSha256": "d"*64,
        }
        if handle.startswith("claude:"):
            return {
                **common, "nativeSource": "claude-session:"+session,
                "sessionId": session, "turnRef": "ccx-turn:claude:v1:turn-one",
            }
        assert native_id == thread
        cid = codex_delivery.read_receipt(handle, delivery)["clientUserMessageId"]
        return {
            **common, "nativeSource": f"ccx-worker:{B}",
            "threadId": thread, "turnId": "codex-turn-one",
            "clientUserMessageId": cid,
        }
    claude = send_turn(A, "hello", delivery_id=token, _native_submit=fake)
    codex = send_turn(B, "hello", delivery_id=token, _native_submit=fake)
    assert {x["worker"]["worker_id"] for x in (claude, codex)} == {A, B}
    assert calls == [f"claude:{A}", f"codex:{B}"]
    for wid, family in ((A, "claude-delivery"), (B, "codex-send")):
        assert store.read_receipt(wid, family, token)["state"] == "accepted"
    assert delivery.read_receipt(A, token)["workerId"] == A
    assert codex_delivery.read_receipt(B, token)["workerId"] == B
    again = send_turn(A, "hello", delivery_id=token, _native_submit=fake)
    assert again == claude and len(calls) == 2
    assert not (root / "operations").exists()



def test_codex_strict_original_turn_source_rejects_unrelated_corruption(codex_owners):
    from ccx.codex import delivery as codex_delivery
    root, store = codex_owners
    codex_delivery.prepare(A, "owned", "text")
    codex_delivery.mark_submitted(A, "owned")
    codex_delivery.mark_accepted(A, "owned", turn_id="original-turn")
    directory = (root / "identity" / "generations" / G1 /
                 "semantic-deliveries" / "codex" / A)
    damaged = directory / ("a"*64 + ".json")
    damaged.write_text('{"bad":', encoding="utf-8")
    damaged.chmod(0o600)
    assert [x["deliveryId"] for x in codex_delivery.iter_receipts(A)] == ["owned"]
    with pytest.raises(codex_delivery.DeliveryReceiptError,
                       match="Cannot read"):
        codex_delivery.receipt_for_turn(A, "original-turn")
    assert damaged.read_text() == '{"bad":'


def test_claude_status_receipts_ignore_unrelated_corrupt_forensic_file(owners):
    root, store = owners
    delivery.prepare(A, "owned", "text", mode="new_turn_only")
    directory = (root / "identity" / "generations" / G1 /
                 "semantic-deliveries" / "claude" / A)
    damaged = directory / ("f"*64 + ".json")
    damaged.write_text("invalid-json", encoding="utf-8")
    damaged.chmod(0o600)
    assert [x["deliveryId"] for x in delivery.iter_receipts(A)] == ["owned"]
    assert delivery.read_receipt(A, "owned")["state"] == "prepared"
    assert damaged.read_text() == "invalid-json"



def test_codex_original_historical_client_ids_remain_parseable_not_minted():
    from ccx.codex.provenance import (
        new_client_message_id, parse_client_message_id, origin_of,
    )
    old = "ccx:v1:abc12345:start:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    original = parse_client_message_id(old)
    assert original == {
        "short": "abc12345", "kind": "start",
        "uuid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    }
    assert origin_of(old) == "ccx-start"
    with pytest.raises(ValueError):
        new_client_message_id("abc12345", "start")
    updated = new_client_message_id(A, "start")
    assert parse_client_message_id(updated)["workerId"] == A
    assert origin_of(updated) == "ccx-start"


def test_original_host_stop_semantic_then_effect_crash_repairs_without_resend(owners):
    """The host can die after fsyncing positive semantic Stop, before effect terminal."""
    from ccx.claude import delivery as claude_delivery
    from ccx.ops.wait import reconcile_turn
    from ccx.store.store import _digest
    root, store = owners
    prepared = prepare_worker(
        store, engine="claude", cwd=str(root.parent), worker_id=A)
    confirmed = _activate_fake_claude(store, prepared)
    session_id = confirmed.record["native"]["sessionId"]
    delivery_id = "fsync-before-effect-terminal"
    text = "original host accepted this exact turn"
    old, created = claude_delivery.prepare(
        A, delivery_id, text, mode="new_turn_only")
    assert created
    source = "claude-session:" + session_id
    spec = _digest({
        "deliveryId": delivery_id, "promptSha256": old["promptSha256"],
        "promptByteCount": old["promptByteCount"],
        "requestedMode": old["requestedMode"],
        "callerMetadata": old.get("callerMetadata"), "cap": old.get("cap"),
    })
    attempt = store.begin_native_intent(
        A, action="send", family="claude-delivery",
        operation_id=delivery_id, intent_sha256=spec,
    )
    store.dispatch_native_intent_once(
        attempt, bounded_submit=lambda budget: {"ok": True, "owned": True},
    )
    claude_delivery.mark_owned(A, delivery_id, route="new_turn")
    claude_delivery.mark_attempt(A, delivery_id, maybe_written=True)
    turn_ref = "ccx-turn:claude:v1:original-host-turn-1"
    claude_delivery.mark_accepted(
        A, delivery_id, placement="new_turn", turn_ref=turn_ref,
        evidence="user-prompt-submit",
    )
    proof = {
        "workerId": A, "engine": "claude",
        "nativeSource": source, "sessionId": session_id,
        "deliveryId": delivery_id, "turnRef": turn_ref,
        "status": "completed",
        "text": "Real host source observed after Stop",
        "nativeEvidenceSha256": "c"*64,
    }
    claude_delivery.mark_settled(
        A, delivery_id, turn_ref=turn_ref,
        original_positive_completion=proof,
    )
    assert store.read_receipt(A, "claude-delivery", delivery_id)["state"] == (
        "submitted-unknown"
    )
    after = reconcile_turn(A, delivery_id=delivery_id)
    assert after["status"] == "settled"
    assert after["delivery"]["settlement"]["text"] == proof["text"]
    assert store.read_receipt(A, "claude-delivery", delivery_id)["state"] == "accepted"
    again = reconcile_turn(A, delivery_id=delivery_id)
    assert again["delivery"] == after["delivery"]


def test_codex_cli_reconcile_receipts_exit_codes(codex_owners, monkeypatch, capsys):
    """Real CLI reconcile reads the original receipt only: no codexd, no writes."""
    import json
    from ccx.cli import main as cli
    from ccx.codex import delivery as codex_delivery
    from ccx.codex import runtime as codex_runtime
    from ccx.errors import SendIndeterminate
    from ccx.ops.send import send_turn
    root, store = codex_owners
    _activate_fake_codex(store, prepare_worker(
        store, engine="codex", cwd=str(root.parent), worker_id=A))
    codex_delivery.prepare(A, "never-sent", "text", mode="new_turn_only")
    def lost(handle, thread, prompt, token, budget):
        raise TimeoutError("native turn/start reply was lost")
    with pytest.raises(SendIndeterminate):
        send_turn(A, "text", delivery_id="auto-lost", mode="auto",
                  _native_submit=lost)
    # Raw prepared semantic receipt whose original codex-send effect advanced.
    assert codex_delivery.read_receipt(A, "auto-lost")["state"] == "prepared"
    assert store.read_receipt(A, "codex-send", "auto-lost")["state"] != "prepared"

    generation = store.pointer.generation_root
    def receipt_bytes():
        return {path: path.read_bytes()
                for folder in ("semantic-deliveries", "receipts")
                for path in sorted((generation / folder).rglob("*"))
                if path.is_file()}
    before = receipt_bytes()
    assert before
    monkeypatch.setattr(codex_runtime, "daemon_rpc",
                        lambda *a, **k: pytest.fail("reconcile contacted codexd"))

    def reconcile(*key):
        # A[:13] is unique (B shares only the first eight characters).
        with pytest.raises(SystemExit) as stopped:
            cli.main(["reconcile", A[:13], *key, "--json"])
        return stopped.value.code, capsys.readouterr().out

    code, out = reconcile("--delivery-id", "never-sent")
    assert code == constants.EXIT_REJECTED
    out = json.loads(out)
    assert (out["reconcile_status"], out["pending"], out["retryable"]) == (
        "not_delivered", False, True)
    assert (out["worker"], out["worker_id"], out["engine"]) == ("codex:" + A, A, "codex")
    assert out["delivery"]["delivery_id"] == "never-sent"

    for key in (("--delivery-id", "unknown"), ("--turn-id", "native-turn-unknown")):
        code, out = reconcile(*key)
        assert code == constants.EXIT_NOT_FOUND
        out = json.loads(out)
        assert (out["reconcile_status"], out["delivery"]) == ("not_found", None)
        assert out["worker_id"] == A

    code, out = reconcile("--delivery-id", "auto-lost")
    assert code == constants.EXIT_TURN_INCOMPLETE
    out = json.loads(out)
    assert (out["reconcile_status"], out["pending"], out["retryable"]) == (
        "pending", True, False)
    assert out["delivery"]["status"] == "prepared"
    assert out["delivery"]["recommended_action"] == "wait"
    assert receipt_bytes() == before
