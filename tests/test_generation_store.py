"""Generation ownership, sealed roots, records and original-effect fences."""
from __future__ import annotations
import ccx.store.root as store_root

import hashlib
import json
import multiprocessing
import os
import socket
import stat
import sys
import tempfile
import time
from pathlib import Path

import pytest

from ccx.store.root import CreationConflict, CreationIndeterminate, GenerationBusy, GenerationUnavailable, NativeActionDenied, NativeEffectIndeterminate, StaleGeneration, StorageIntegrityError, inspect_generation, require_ccx_active
from ccx.store.store import fingerprint_v2
from ccx.store.identity import EngineMismatch, InvalidTarget, OwnerConflict, UnknownWorker

A = "aaaaaaaa-1234-4000-8000-000000000001"
B = "aaaaaaaa-5678-4000-8000-000000000002"
C = "bbbbbbbb-1234-4000-8000-000000000003"
G1 = "g-" + "1" * 32
G2 = "g-" + "2" * 32


@pytest.fixture
def short_root(socket_tmp_path):
    """Both Linux and Darwin AF_UNIX tests need a genuinely short root."""
    yield fixture_root(socket_tmp_path)


def privdir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.chmod(0o700)


def put(path: Path, obj: dict) -> None:
    privdir(path.parent)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")))
    path.chmod(0o600)


def fixture_root(tmp_path: Path, phase: str = "ACTIVE",
                 generation: str = G1) -> Path:
    root = tmp_path / "disposable-ccx"
    privdir(root)
    identity = root / "identity"
    privdir(identity)
    privdir(identity / "generations")
    privdir(identity / "generations" / generation)
    manifest = {
        "schemaVersion": 1, "rootRealpath": str(root),
        "generation": generation, "sealed": True,
    }
    path = identity / "generations" / generation / "manifest.json"
    put(path, manifest)
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    put(identity / "current.json", {
        "layout": 2, "phase": phase, "generation": generation,
        "manifestSha256": checksum, "rootRealpath": str(root),
    })
    lock = identity / "activation.lock"
    lock.touch(mode=0o600)
    lock.chmod(0o600)
    return root


def switch_pointer(root: Path, generation: str = G2, phase: str = "ACTIVE") -> None:
    new = root / "identity" / "generations" / generation
    privdir(new)
    path = new / "manifest.json"
    put(path, {"schemaVersion": 1, "generation": generation,
               "rootRealpath": str(root), "sealed": True})
    data = {
        "layout": 2, "phase": phase, "generation": generation,
        "rootRealpath": str(root),
        "manifestSha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    put(root / "identity" / "next.json", data)
    os.replace(root / "identity" / "next.json", root / "identity" / "current.json")


def spec(engine: str) -> str:
    return fingerprint_v2(engine=engine, cwd="/tmp/fixture", create_args={"model": "fixture"})


def record(wid: str, engine: str, *, native_state: str = "thread-bound",
           terminal: bool = False, native_create: dict | None = None) -> dict:
    native = ({"sessionId": wid} if engine == "claude"
              else {"threadId": f"native-thread-{wid}", "nativeState": native_state})
    if native_state == "unbound-unknown":
        native.pop("threadId", None)
    out = {"workerId": wid, "engine": engine, "name": "display is not a key",
           "native": native, "terminalOnly": terminal}
    if native_create is not None:
        out["nativeCreate"] = native_create
    return out


def add_worker(root: Path, wid: str, engine: str = "claude", **kwargs):
    confirmed = kwargs.pop("confirmed", False)
    ctx = require_ccx_active(root)
    fingerprint = spec(engine)
    reservation = ctx.reserve_creation(wid, engine=engine, fingerprint=fingerprint)
    ctx.publish_first_record(reservation, record(wid, engine, **kwargs))
    if confirmed:
        assert not kwargs.get("terminal") and not kwargs.get("native_create")
        assert kwargs.get("native_state", "thread-bound") == "thread-bound"
        # Synthetic confirmed CURRENT binding for store-operation tests. A
        # native ID alone is not a capability after the v1 guard is removed.
        rec = ctx.record(wid)
        claim = ctx._claim(wid)
        nb = ({"sessionId": rec["native"]["sessionId"]} if engine == "claude"
              else {"threadId": rec["native"]["threadId"]})
        source = (f"claude-session:{nb['sessionId']}" if engine == "claude"
                  else f"ccx-worker:{wid}")
        rec.update(fingerprintVersion=2, createSha256=fingerprint)
        rec["native"]["nativeState"] = "session-bound" if engine == "claude" else "thread-bound"
        if engine == "codex":
            rec["native"]["threadSource"] = source
        claim["activatedAtMs"] = 1
        claim["nativeActivation"] = {
            "state": "confirmed", "source": source,
            "submissionId": "fixture-original-acceptance",
            "nativeEvidenceSha256": "c" * 64, "nativeBinding": nb,
        }
        g = ctx.pointer.generation_root
        put(g / "claims" / f"{wid}.json", claim)
        put(g / "workers" / engine / wid / "record.json", rec)
    return ctx


def _child_parallel_owner(args):
    root, wid, engine = args
    from ccx.store.root import require_ccx_active
    from ccx.store.store import fingerprint_v2
    ctx = require_ccx_active(Path(root))
    digest = fingerprint_v2(engine=engine, cwd="/tmp/fixture", create_args={"model": "fixture"})
    outcome = ctx.reserve_creation(wid, engine=engine, fingerprint=digest)
    data = {
        "workerId": wid, "engine": engine, "name": "display is not a key",
        "native": ({"sessionId": wid} if engine == "claude"
                   else {"threadId": f"native-thread-{wid}", "nativeState": "thread-bound"}),
        "terminalOnly": False,
    }
    ctx.publish_first_record(outcome, data)
    return wid, outcome.reconciled


def test_absent_pointer_refuses_without_mkdir_or_incidental_writes(tmp_path):
    root = tmp_path / "uninitialized"
    privdir(root)
    before = tuple(root.iterdir())
    with pytest.raises(GenerationUnavailable):
        require_ccx_active(root)
    assert tuple(root.iterdir()) == before


def test_verify_only_is_not_a_supported_generation(tmp_path):
    root = fixture_root(tmp_path, phase="VERIFY_ONLY")
    before = sorted(str(x.relative_to(root)) for x in root.rglob("*"))
    with pytest.raises(GenerationUnavailable):
        inspect_generation(root)
    with pytest.raises(GenerationUnavailable):
        require_ccx_active(root)
    assert before == sorted(str(x.relative_to(root)) for x in root.rglob("*"))


@pytest.mark.parametrize("invalid", [
    {"phase": "LEGACY"}, {"layout": 1}, {"generation": "../else"},
    {"manifestSha256": "0" * 64}, {"rootRealpath": "/wrong-root"},
])
def test_pointer_malformed_or_mismatched_refuses_before_disk_write(tmp_path, invalid):
    root = fixture_root(tmp_path)
    path = root / "identity" / "current.json"
    obj = json.loads(path.read_text())
    obj.update(invalid)
    put(path, obj)
    before = path.read_bytes()
    with pytest.raises(GenerationUnavailable):
        require_ccx_active(root)
    assert path.read_bytes() == before


def test_activation_lock_required_offline_and_root_symlinks_rejected(tmp_path):
    root = fixture_root(tmp_path)
    (root / "identity" / "activation.lock").unlink()
    with pytest.raises(GenerationUnavailable, match="activation lock"):
        require_ccx_active(root)
    linked = tmp_path / "symlink"
    linked.symlink_to(root, target_is_directory=True)
    with pytest.raises(GenerationUnavailable):
        require_ccx_active(linked)


def test_pointer_switch_fences_old_context_and_cannot_write_old_generation(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    switch_pointer(root)
    with pytest.raises(StaleGeneration):
        ctx.revalidate()
    with pytest.raises(StaleGeneration):
        ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    assert not (root / "identity" / "generations" / G1 / "owners").exists()


def test_global_full_id_ownership_same_prefix_same_and_cross_engine(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    add_worker(root, B, "codex")
    ctx = require_ccx_active(root)
    assert ctx.resolve(A).engine == "claude"
    assert ctx.resolve(f"codex:{B}").worker_id == B
    assert ctx.record(A)["native"]["sessionId"] == A
    assert ctx.record(B)["native"]["threadId"] == f"native-thread-{B}"
    assert ctx.worker_path(A) != ctx.worker_path(B)
    assert A in str(ctx.worker_path(A)) and B in str(ctx.worker_path(B))
    assert not (root / "sessions").exists()
    with pytest.raises(EngineMismatch):
        ctx.resolve(f"codex:{A}")
    with pytest.raises(OwnerConflict):
        ctx.reserve_creation(A, engine="codex", fingerprint=spec("codex"))


def test_same_full_id_exact_spec_retry_and_changed_spec_refusal(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    first = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    assert not first.reconciled
    same = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    assert same.reconciled and not same.record_committed
    ctx.publish_first_record(first, record(A, "claude"))
    again = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    assert again.record_committed and again.reconciled
    assert ctx.resolve(A).worker_id == A
    with pytest.raises(CreationConflict):
        ctx.reserve_creation(A, engine="claude", fingerprint="f" * 64)
    with pytest.raises(CreationConflict):
        ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"),
                             native_binding={"sessionId": B})


def test_fingerprint_v2_fork_anchor_frozen(tmp_path):
    f1 = fingerprint_v2(engine="claude", cwd="/tmp/fixture", create_args={"model": "a"},
                        fork_anchor={"sourceWorkerId": B, "resumeAt": 9})
    f2 = fingerprint_v2(engine="claude", cwd="/tmp/fixture", create_args={"model": "a"},
                        fork_anchor={"sourceWorkerId": B, "resumeAt": 10})
    assert f1 != f2


def test_crash_after_owner_publish_is_indeterminate_no_second_native_start(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    def crash(where):
        if where == "after-owner":
            raise RuntimeError("crash point")
    with pytest.raises(RuntimeError):
        ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"), _failpoint=crash)
    original_owner = root / "identity" / "generations" / G1 / "owners" / f"{A}.json"
    expected_intent = json.loads(original_owner.read_text())["createIntent"]
    retry = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    assert retry.reconciled and not retry.record_committed
    assert retry.activation_state == "unprepared" and retry.submission_id is None
    assert json.loads(original_owner.read_text())["createIntent"] == expected_intent
    with pytest.raises(CreationConflict):
        ctx.reserve_creation(A, engine="claude", fingerprint="f" * 64)
    with pytest.raises(UnknownWorker):
        ctx.resolve(A)


def test_crash_after_claim_and_record_recovers_exact_ID_without_native(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    def fault(name):
        if name == "after-claim":
            raise RuntimeError("before record")
    with pytest.raises(RuntimeError):
        ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"), _failpoint=fault)
    same = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    assert same.reconciled and not same.record_committed
    def fail_after_record(name):
        if name == "after-record":
            raise RuntimeError("before owner promoted")
    with pytest.raises(RuntimeError):
        ctx.publish_first_record(same, record(A, "claude"), _failpoint=fail_after_record)
    with pytest.raises(UnknownWorker):
        ctx.resolve(A)
    ctx.publish_first_record(same, record(A, "claude"))
    assert ctx.resolve(A).worker_id == A
    # Retry cannot invent or replay any native activation.
    assert ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude")).reconciled


def test_owner_claim_without_record_and_historical_without_original_create_digest(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    g = root / "identity" / "generations" / G1
    put(g / "owners" / f"{A}.json", {
        "schemaVersion": 2, "generation": G1, "workerId": A, "engine": "claude",
        "status": "indeterminate-reserved",
    })
    put(g / "claims" / f"{A}.json", {
        "schemaVersion": 2, "generation": G1, "workerId": A, "engine": "claude",
        "createSha256": spec("claude"), "fingerprintVersion": 2,
        "nativeBinding": {}, "forkAnchor": None, "activatedAtMs": 1700000000000,
    })
    with pytest.raises(UnknownWorker):
        ctx.resolve(A)
    with pytest.raises(CreationIndeterminate):
        ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    # Separate historical claimless owner has a valid record for READ only.
    put(g / "owners" / f"{B}.json", {
        "schemaVersion": 2, "generation": G1, "workerId": B, "engine": "claude",
        "status": "worker", "provenance": "historical-ccx-minted-session-id",
    })
    privdir(g / "workers")
    privdir(g / "workers" / "claude")
    put(g / "workers" / "claude" / B / "record.json", record(B, "claude"))
    assert ctx.resolve(B).worker_id == B
    with pytest.raises(CreationIndeterminate, match="predates durable create intent"):
        ctx.reserve_creation(B, engine="claude", fingerprint=spec("claude"))


def test_same_semantic_token_isolated_for_workers_with_same_prefix(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, confirmed=True)
    add_worker(root, B, confirmed=True)
    ctx = require_ccx_active(root)
    first = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                    operation_id="same-token", intent_sha256="a" * 64)
    second = ctx.begin_native_intent(B, action="send", family="claude-delivery",
                                     operation_id="same-token", intent_sha256="a" * 64)
    assert ctx.receipt_path(A, "claude-delivery", "same-token") != ctx.receipt_path(B, "claude-delivery", "same-token")
    assert first.worker_id != second.worker_id
    assert ctx.read_receipt(A, "claude-delivery", "same-token")["state"] == "prepared"
    assert first.dispatch_token and not first.replayed
    ctx.dispatch_native_intent_once(
        first, bounded_submit=lambda budget: {"nativeEvidenceSha256": "c" * 64}
    )
    assert ctx.read_receipt(A, "claude-delivery", "same-token")["state"] == "submitted-unknown"
    with pytest.raises(NativeEffectIndeterminate):
        ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                operation_id="same-token", intent_sha256="a" * 64)
    settled = ctx.settle_native_intent(first, state="accepted", native_evidence_sha256="c" * 64)
    assert settled["state"] == "accepted"
    assert ctx.read_receipt(B, "claude-delivery", "same-token")["state"] == "prepared"
    replay = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                     operation_id="same-token", intent_sha256="a" * 64)
    assert replay.replayed and replay.state == "accepted"
    assert replay.native_evidence_sha256 == "c" * 64
    with pytest.raises(NativeEffectIndeterminate):
        ctx.dispatch_native_intent_once(replay, bounded_submit=lambda budget: None)
    assert ctx.settle_native_intent(first, state="accepted",
                                    native_evidence_sha256="c" * 64) == settled
    with pytest.raises(CreationConflict):
        ctx.settle_native_intent(first, state="accepted", native_evidence_sha256="d" * 64)


def test_terminal_unbound_unknown_refuses_without_native_call_or_receipt_write(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, "codex", native_state="unbound-unknown", terminal=True)
    ctx = require_ccx_active(root)
    invoked = []
    def fake_native_start():
        invoked.append("thread/start")
    # Native callback is only reachable if a bounded dispatch fence exists.
    for action in ("send", "resume", "compact", "start"):
        with pytest.raises(NativeActionDenied):
            ctx.begin_native_intent(A, action=action, family="codex-send",
                                    operation_id=action, intent_sha256="a" * 64)
        assert invoked == []
    assert not (root / "identity" / "generations" / G1 / "receipts").exists()


def test_pending_native_source_requires_positive_reconciliation_even_with_thread(tmp_path):
    root = fixture_root(tmp_path)
    marker = {"state": "unknown", "threadSource": "ccx-worker:aaaaaaaa",
              "schemaVersion": 1}
    add_worker(root, A, "codex", native_create=marker)
    ctx = require_ccx_active(root)
    with pytest.raises(NativeActionDenied):
        ctx.begin_native_intent(A, action="send", family="codex-send",
                                operation_id="id1", intent_sha256="a" * 64)


def test_native_unknown_result_fence_survives_lost_response_and_generation_change(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, "codex", confirmed=True)
    ctx = require_ccx_active(root)
    attempt = ctx.begin_native_intent(A, action="send", family="codex-send",
                                      operation_id="id", intent_sha256="a" * 64)
    ctx.dispatch_native_intent_once(attempt, bounded_submit=lambda budget: {"submitted": True})
    assert ctx.settle_native_intent(attempt, state="unknown", native_evidence_sha256=None)["state"] == "submitted-unknown"
    switch_pointer(root)
    with pytest.raises(StaleGeneration):
        ctx.settle_native_intent(attempt, state="accepted", native_evidence_sha256="b" * 64)
    # The submitted unknown receipt is still retained in the OLD pinned generation.
    path = root / "identity" / "generations" / G1 / "receipts" / "codex-send" / "codex" / A
    receipts = list(path.glob("*.json"))
    assert len(receipts) == 1 and json.loads(receipts[0].read_text())["state"] == "submitted-unknown"


def test_bounded_activation_lock_conflicts_with_exclusive_runner_lock(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    import fcntl
    with ctx.lease():
        fd = os.open(root / "identity" / "activation.lock", os.O_RDONLY | os.O_NOFOLLOW)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(fd)


def test_cross_process_claiming_same_UUID_serialized_and_same_prefix_independent(tmp_path):
    root = fixture_root(tmp_path)
    arguments = [(str(root), A, "claude")] * 4 + [(str(root), B, "codex")] * 4
    context = multiprocessing.get_context("fork")
    with context.Pool(processes=4) as pool:
        results = pool.map(_child_parallel_owner, arguments)
    assert len(results) == 8
    ctx = require_ccx_active(root)
    assert ctx.resolve(A).engine == "claude"
    assert ctx.resolve(B).engine == "codex"
    assert len(list((root / "identity" / "generations" / G1 / "owners").glob("*.json"))) == 2
    assert len(list((root / "identity" / "generations" / G1 / "claims").glob("*.json"))) == 2


def test_offline_verifier_is_pure_and_cannot_write_verify_only(tmp_path):
    root = fixture_root(tmp_path, phase="VERIFY_ONLY")
    before = [(str(x.relative_to(root)), x.stat().st_mtime_ns)
              for x in root.rglob("*") if x.is_file()]
    with pytest.raises(GenerationUnavailable):
        inspect_generation(root)
    with pytest.raises(GenerationUnavailable):
        require_ccx_active(root)
    assert before == [(str(x.relative_to(root)), x.stat().st_mtime_ns)
                      for x in root.rglob("*") if x.is_file()]



def test_root_body_oserror_is_not_mislabeled_symlink(tmp_path):
    import errno
    from ccx.store.root import _root_dir_fd, _StrictFileError
    root = fixture_root(tmp_path)
    with pytest.raises(OSError) as error:
        with _root_dir_fd(root):
            raise OSError(errno.ENOSPC, "simulated device full")
    assert error.value.errno == errno.ENOSPC
    assert not isinstance(error.value, _StrictFileError)


def test_unsupported_claim_schema_is_not_rewritten(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    ctx = require_ccx_active(root)
    claim_path = ctx.pointer.generation_root / "claims" / f"{A}.json"
    claim = json.loads(claim_path.read_text())
    claim["fingerprintVersion"] = 1
    put(claim_path, claim)
    claim_before = claim_path.read_bytes()
    with pytest.raises(CreationConflict, match="immutable create intent"):
        ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    assert claim_path.read_bytes() == claim_before


def test_native_read_gate_denies_before_spy_start_or_dispatch(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, "codex", native_state="unbound-unknown", terminal=True)
    ctx = require_ccx_active(root)
    class FakeProvider:
        starts = 0
        def start(self):
            self.starts += 1
    fake = FakeProvider()
    for action in ("start", "send", "resume", "compact"):
        with pytest.raises(NativeActionDenied):
            ctx.authorize_native_action(A, action)
            fake.start()
    assert fake.starts == 0
    assert not (root / "identity" / "generations" / G1 / "receipts").exists()


def test_native_bound_thread_has_guarded_send_but_no_generic_new_start(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, "codex", confirmed=True)
    ctx = require_ccx_active(root)
    assert ctx.authorize_native_action(A, "send").worker_id == A
    with pytest.raises(NativeActionDenied):
        ctx.authorize_native_action(A, "start")


def test_native_binding_in_record_must_match_claimed_original(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    claim = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"),
                                 native_binding={"sessionId": A})
    forged = record(A, "claude")
    forged["native"]["sessionId"] = B
    with pytest.raises(CreationConflict, match="native binding"):
        ctx.publish_first_record(claim, forged)
    with pytest.raises(UnknownWorker):
        ctx.resolve(A)
    ctx.publish_first_record(claim, record(A, "claude"))
    assert ctx.resolve(A).worker_id == A


def test_receipts_same_token_cross_engine_prefix_cannot_collide(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude", confirmed=True)
    add_worker(root, B, "codex", confirmed=True)
    ctx = require_ccx_active(root)
    r1 = ctx.begin_native_intent(A, action="send", family="answer",
                                 operation_id="same-op-token", intent_sha256="a" * 64)
    r2 = ctx.begin_native_intent(B, action="send", family="answer",
                                 operation_id="same-op-token", intent_sha256="a" * 64)
    assert ctx.receipt_path(A, "answer", "same-op-token") != ctx.receipt_path(B, "answer", "same-op-token")
    assert ctx.read_receipt(A, "answer", "same-op-token")["workerId"] == A
    assert ctx.read_receipt(B, "answer", "same-op-token")["workerId"] == B
    assert r1.engine != r2.engine


def test_shorts_names_native_id_rejected_by_generation_before_any_claim_write(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    for alias in ("aaaaaaaa", "claude:aaaaaaaa", "display", "claude-session:" + A):
        with pytest.raises((InvalidTarget, GenerationUnavailable, UnknownWorker)):
            ctx.reserve_creation(alias, engine="claude", fingerprint=spec("claude"))
    assert not (root / "identity" / "generations" / G1 / "owners").exists()


def test_private_root_and_foreign_file_permissions_fail_closed(tmp_path):
    root = fixture_root(tmp_path)
    root.chmod(0o755)
    with pytest.raises(StorageIntegrityError, match="private"):
        require_ccx_active(root)
    root.chmod(0o700)
    add_worker(root, A)
    owner_path = root / "identity" / "generations" / G1 / "owners" / f"{A}.json"
    owner_path.chmod(0o644)
    ctx = require_ccx_active(root)
    with pytest.raises(StorageIntegrityError, match="private"):
        ctx.resolve(A)


def test_semantic_receipt_wrong_owner_on_disk_is_detected(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, confirmed=True)
    ctx = require_ccx_active(root)
    attempt = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                      operation_id="request-id", intent_sha256="a" * 64)
    path = ctx.receipt_path(A, "claude-delivery", "request-id")
    original = json.loads(path.read_text())
    original["workerId"] = B
    put(path, original)
    with pytest.raises(StorageIntegrityError, match="identity mismatch"):
        ctx.read_receipt(A, "claude-delivery", "request-id")


def test_native_submitted_intent_does_not_hold_activation_lease_while_waiting(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, "codex", confirmed=True)
    ctx = require_ccx_active(root)
    attempt = ctx.begin_native_intent(A, action="send", family="codex-send",
                                       operation_id="request-id", intent_sha256="a" * 64)
    ctx.dispatch_native_intent_once(attempt, bounded_submit=lambda budget: {"submitted": True})
    import fcntl
    fd = os.open(root / "identity" / "activation.lock", os.O_RDONLY | os.O_NOFOLLOW)
    try:
        # A future operator's exclusive lease can start while a native turn is
        # in an unknown terminal-wait state: the receipt, not the lock, fences it.
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
    assert ctx.read_receipt(A, "codex-send", "request-id")["state"] == "submitted-unknown"


def test_o1_native_activation_state_is_not_implied_by_record_retry(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    initial = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"),
                                   native_binding={"sessionId": A})
    assert initial.activation_state == "unprepared"
    assert initial.record_committed is False
    ctx.publish_first_record(initial, record(A, "claude"))
    retry = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"),
                                 native_binding={"sessionId": A})
    assert retry.reconciled and retry.record_committed
    assert retry.activation_state == "unprepared" and retry.submission_id is None
    assert ctx.native_activation(A)["state"] == "unprepared"
    prepared = ctx.prepare_native_activation(retry, native_source=f"claude-session:{A}")
    assert prepared.state == "prepared" and prepared.submission_id
    assert ctx.native_activation(A)["state"] == "prepared"
    same = ctx.prepare_native_activation(retry, native_source=f"claude-session:{A}")
    assert same.submission_id == prepared.submission_id
    with pytest.raises(CreationConflict):
        ctx.prepare_native_activation(retry, native_source=f"claude-session:{B}")


def test_o1_submission_is_fsynced_before_fake_native_call_and_confirmed_once(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    r = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"),
                             native_binding={"sessionId": A})
    ctx.publish_first_record(r, record(A, "claude"))
    ready = ctx.prepare_native_activation(r, native_source=f"claude-session:{A}")
    observed = []
    def native_send(deadline):
        assert 0 < deadline <= 2
        claim = ctx.native_activation(A)
        assert claim["state"] == "submitted-unknown"
        assert claim["submissionId"] == ready.submission_id
        observed.append("native-send-once")
        return {"nativeId": A, "nativeEvidenceSha256": "f" * 64}
    result = ctx.native_activation_dispatch(ready, bounded_submit=native_send)
    assert result["nativeId"] == A and observed == ["native-send-once"]
    assert ctx.native_activation(A)["state"] == "submitted-unknown"
    with pytest.raises(CreationIndeterminate):
        ctx.native_activation_dispatch(ready, bounded_submit=lambda _t: observed.append("duplicate"))
    assert observed == ["native-send-once"]
    finished = ctx.confirm_native_activation(
        ready, native_evidence_sha256="f" * 64, native_binding={"sessionId": A})
    assert finished.state == "confirmed"
    confirmed = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"),
                                     native_binding={"sessionId": A})
    assert confirmed.activation_state == "confirmed"
    assert confirmed.submission_id == ready.submission_id
    assert ctx.confirm_native_activation(
        ready, native_evidence_sha256="f" * 64, native_binding={"sessionId": A}
    ).state == "confirmed"
    with pytest.raises(CreationConflict):
        ctx.confirm_native_activation(
            ready, native_evidence_sha256="a" * 64, native_binding={"sessionId": B})


def test_o1_crash_after_durable_submitted_never_dispatches_or_restarts(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    r = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    ctx.publish_first_record(r, record(A, "claude"))
    ready = ctx.prepare_native_activation(r, native_source=f"claude-session:{A}")
    fake_calls = []
    def fault(stage):
        if stage == "after-submitted":
            raise RuntimeError("process died after durable submission fence")
    with pytest.raises(RuntimeError):
        ctx.native_activation_dispatch(ready, bounded_submit=lambda t: fake_calls.append(t),
                                       _failpoint=fault)
    assert fake_calls == []
    assert ctx.native_activation(A)["state"] == "submitted-unknown"
    retry = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    assert retry.activation_state == "submitted-unknown"
    with pytest.raises(CreationIndeterminate):
        ctx.prepare_native_activation(retry, native_source=f"claude-session:{A}")
    with pytest.raises(CreationIndeterminate):
        ctx.native_activation_dispatch(ready, bounded_submit=lambda t: fake_calls.append(t))
    assert fake_calls == []


def test_o1_lost_reply_preserves_native_unknown_and_prevents_replay(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    r = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    ctx.publish_first_record(r, record(A, "claude"))
    ready = ctx.prepare_native_activation(r, native_source=f"claude-session:{A}")
    calls = []
    def lost(_deadline):
        calls.append("one")
        raise TimeoutError("native response lost after send")
    with pytest.raises(NativeEffectIndeterminate):
        ctx.native_activation_dispatch(ready, bounded_submit=lost)
    assert calls == ["one"]
    with pytest.raises(CreationIndeterminate):
        ctx.native_activation_dispatch(ready, bounded_submit=lost)
    assert calls == ["one"]
    assert ctx.native_activation(A)["state"] == "submitted-unknown"
    with pytest.raises(CreationIndeterminate):
        ctx.confirm_native_activation(ready, native_evidence_sha256=None,
                                      native_binding={"sessionId": A})


def test_o1_pointer_change_between_intent_and_dispatch_never_calls_native(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    r = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    ctx.publish_first_record(r, record(A, "claude"))
    ready = ctx.prepare_native_activation(r, native_source=f"claude-session:{A}")
    called = []
    def flip(stage):
        if stage == "after-submitted":
            switch_pointer(root)
    with pytest.raises(StaleGeneration):
        ctx.native_activation_dispatch(ready, bounded_submit=lambda t: called.append(t),
                                       _failpoint=flip)
    assert called == []
    claim = root / "identity" / "generations" / G1 / "claims" / f"{A}.json"
    assert json.loads(claim.read_text())["nativeActivation"]["state"] == "submitted-unknown"


def test_o1_codex_native_source_cannot_use_short_or_preexisting_thread(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    r = ctx.reserve_creation(A, engine="codex", fingerprint=spec("codex"))
    native = record(A, "codex")
    native["native"].pop("threadId")
    native["native"].pop("nativeState")
    ctx.publish_first_record(r, native)
    with pytest.raises(CreationConflict):
        ctx.prepare_native_activation(r, native_source="ccx-worker:aaaaaaaa")
    prepared = ctx.prepare_native_activation(r, native_source=f"ccx-worker:{A}")
    assert prepared.state == "prepared"
    with pytest.raises(CreationIndeterminate):
        ctx.confirm_native_activation(prepared, native_evidence_sha256="a" * 64,
                                      native_binding={"threadId": "native-id"})
    with pytest.raises(RuntimeError, match="crash"):
        ctx.native_activation_dispatch(
            prepared, bounded_submit=lambda t: None,
            _failpoint=lambda stage: (_ for _ in ()).throw(RuntimeError("crash"))
            if stage == "after-submitted" else None,
        )  # failpoint exception itself passes through, see more direct tests



def _record_process_patch(args):
    """Real separate process: read stale snapshot, then patch against fresh disk."""
    root, wid, update, kind, started, release, output = args
    from ccx.store.root import require_ccx_active, RecordConflict
    ctx = require_ccx_active(Path(root))
    stale = ctx.record(wid)
    started.put((kind, stale.get("recordRevision", 0)))
    release.wait(timeout=10)
    try:
        if kind == "increment":
            out = ctx.mutate_record(wid, increments=update)
        elif kind == "cas":
            out = ctx.mutate_record(wid, patches=update,
                                    expect={"createArgs.model": "old"})
        else:
            out = ctx.mutate_record(wid, patches=update)
        output.put((kind, "ok", out.get("recordRevision")))
    except RecordConflict:
        output.put((kind, "conflict", None))


def _darwin_openat_lock_probe(args):
    """Exercise the bounded lock opener after Monterey's raw O_CREAT race."""
    root, ready, release, results = args
    from ccx.store.root import _flock_at
    try:
        ctx = require_ccx_active(Path(root))
        with ctx._gen("locks", "records", create=True) as parent_fd:
            original = os.fstat(parent_fd)
            ready.put(os.getpid())
            assert release.wait(timeout=10)
            try:
                with _flock_at(parent_fd, A + ".lock", exclusive=True, create=True,
                               timeout=1.0):
                    inode = os.stat(A + ".lock", dir_fd=parent_fd,
                                    follow_symlinks=False)
                    results.put(("ok", inode.st_dev, inode.st_ino))
            except Exception as exc:
                now = os.fstat(parent_fd)
                reason = exc.__cause__
                results.put(("error", type(exc).__name__,
                             getattr(reason, "errno", None),
                             (original.st_dev, original.st_ino, original.st_nlink),
                             (now.st_dev, now.st_ino, now.st_nlink)))
    except Exception as exc:
        results.put(("setup-error", type(exc).__name__, str(exc)))


@pytest.mark.skipif(sys.platform != "darwin", reason="test exact Monterey openat(O_CREAT) concurrency")
def test_darwin_concurrent_pinned_openat_can_create_one_stable_lock(tmp_path):
    """Four real competing Mac processes all acquire one original lock inode."""
    root = fixture_root(tmp_path)
    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager:
        ready, results, release = manager.Queue(), manager.Queue(), manager.Event()
        children = [ctx.Process(
            target=_darwin_openat_lock_probe,
            args=((str(root), ready, release, results),),
        ) for _ in range(4)]
        try:
            for child in children:
                child.start()
            for _ in children:
                ready.get(timeout=10)
            release.set()
            observations = [results.get(timeout=10) for _ in children]
            for child in children:
                child.join(timeout=10)
                assert child.exitcode == 0
            assert all(item[0] == "ok" for item in observations), observations
            assert len({item[1:] for item in observations}) == 1, observations
        finally:
            release.set()
            for child in children:
                if child.is_alive():
                    child.terminate()
                    child.join(timeout=5)


@pytest.mark.parametrize("missing_attempts", (1, 6))
def test_stable_creation_lock_retries_only_bounded_missing_creates(
    tmp_path, monkeypatch, missing_attempts,
):
    """A transient Mac creation ENOENT is retried; persistent missing refuses."""
    from ccx.store import root as generation

    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    with ctx._gen("locks", "records", create=True) as parent_fd:
        original = os.open
        calls = []

        def transient(name, flags, *args, **kwargs):
            if name == A + ".lock" and flags & os.O_CREAT:
                calls.append(1)
                if len(calls) <= missing_attempts:
                    raise FileNotFoundError(2, "injected Mac openat create race", name)
            return original(name, flags, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(generation.os, "open", transient)
            if missing_attempts == 1:
                with store_root._flock_at(parent_fd, A + ".lock",
                                          exclusive=True, create=True):
                    st = os.stat(A + ".lock", dir_fd=parent_fd,
                                 follow_symlinks=False)
                    assert stat.S_ISREG(st.st_mode) and st.st_uid == os.getuid()
                assert len(calls) == 2
            else:
                with pytest.raises(
                    GenerationUnavailable, match="stable lock creation failed"
                ):
                    with store_root._flock_at(parent_fd, A + ".lock",
                                              exclusive=True, create=True):
                        pytest.fail("accepted an unproven lock")
                assert len(calls) == 6
                with pytest.raises(FileNotFoundError):
                    os.stat(A + ".lock", dir_fd=parent_fd,
                            follow_symlinks=False)
        with pytest.raises(GenerationUnavailable, match="preinstalled"):
            with store_root._flock_at(parent_fd, B + ".lock",
                                      exclusive=True, create=False):
                pytest.fail("created absent preinstalled lock")


def test_record_mutation_atomic_per_field_and_revision(tmp_path):
    from ccx.store.root import RecordConflict
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    reserve = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    rec = record(A, "claude")
    rec.update(lifecycle={"state": "booting", "hostPid": None},
               createArgs={"model": "old"}, stats={"turns": 0, "sampleCount": 0}, recordRevision=0)
    ctx.publish_first_record(reserve, rec)
    stale = ctx.record(A)
    first = ctx.mutate_record(A, patches={"lifecycle.state": "idle"},
                              expect={"lifecycle.state": "booting"})
    assert first["recordRevision"] == 1
    with pytest.raises(RecordConflict):
        ctx.mutate_record(A, patches={"lifecycle.state": "dead"},
                          expect={"lifecycle.state": "booting"})
    second = ctx.mutate_record(A, patches={"createArgs.model": "new"},
                               expect={"createArgs.model": "old"})
    final = ctx.mutate_record(A, increments={"stats.turns": 2, "stats.sampleCount": 3})
    assert final["recordRevision"] == 3
    assert final["createArgs"]["model"] == "new"
    assert final["lifecycle"] == {"state": "idle", "hostPid": None}
    assert final["stats"]["turns"] == 2 and final["stats"]["sampleCount"] == 3
    assert final["native"] == stale["native"]
    with pytest.raises(RecordConflict):
        ctx.mutate_record(A, patches={"name": "stale"}, expected_revision=0)
    with pytest.raises(StorageIntegrityError):
        ctx.mutate_record(A, patches={"workerId": B})
    with pytest.raises(StorageIntegrityError):
        ctx.mutate_record(A, patches={"native.sessionId": B})
    with pytest.raises(StorageIntegrityError):
        ctx.mutate_record(A, patches={"stats.turns": 4}, increments={"stats.turns": 1})


def test_record_multiprocess_lifecycle_config_analytics_merge(tmp_path):
    from ccx.system import oslayer
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    res = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    rec = record(A, "claude")
    rec.update(lifecycle={"state": "booting", "hostPid": None},
               createArgs={"model": "old"},
               stats={"turns": 0, "sampleCount": 0},
               recordRevision=0)
    ctx.publish_first_record(res, rec)
    # macOS's normal spawn starts a clean worker instead of inheriting locks
    # from an already-threaded pytest process. Five writers still begin from
    # the same stale snapshot and must merge under per-field CAS.
    mp = multiprocessing.get_context(
        "spawn" if oslayer.UNIX_SUN_PATH_BYTES == 104 else "fork")
    manager = mp.Manager()
    started = manager.Queue()
    result = manager.Queue()
    release = manager.Event()
    specs = [
        ("config", {"createArgs.model": "changed"}),
        ("lifecycle", {"lifecycle.state": "running"}),
        ("increment", {"stats.turns": 1}),
        ("increment", {"stats.turns": 1}),
        ("increment", {"stats.sampleCount": 1}),
    ]
    procs = [
        mp.Process(target=_record_process_patch,
                   args=((str(root), A, changes, kind, started, release, result),))
        for kind, changes in specs
    ]
    try:
        for proc in procs:
            proc.start()
        for _ in procs:
            started.get(timeout=10)  # every process already read the same stale snapshot
        release.set()
        outcomes = [result.get(timeout=10) for _ in procs]
        for proc in procs:
            proc.join(timeout=10)
            assert proc.exitcode == 0
        assert all(outcome[1] == "ok" for outcome in outcomes)
        latest = require_ccx_active(root).record(A)
        assert latest["lifecycle"]["state"] == "running"
        assert latest["createArgs"]["model"] == "changed"
        assert latest["stats"]["turns"] == 2
        assert latest["stats"]["sampleCount"] == 1
        assert latest["recordRevision"] == 5
        assert latest["native"]["sessionId"] == A
    finally:
        release.set()
        for proc in procs:
            if proc.is_alive():
                proc.terminate()
                proc.join(3)
        manager.shutdown()


def test_record_multiprocess_same_field_CAS_one_conflicts(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    res = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    rec = record(A, "claude")
    rec.update(createArgs={"model": "old"}, recordRevision=0)
    ctx.publish_first_record(res, rec)
    mp = multiprocessing.get_context("fork")
    manager = mp.Manager()
    started = manager.Queue()
    result = manager.Queue()
    release = manager.Event()
    procs = [
        mp.Process(target=_record_process_patch,
                   args=((str(root), A, {"createArgs.model": value},
                          "cas", started, release, result),))
        for value in ("first", "second")
    ]
    try:
        for proc in procs:
            proc.start()
        for _ in procs:
            started.get(timeout=10)
        release.set()
        outcomes = [result.get(timeout=10) for _ in procs]
        assert sorted(status for _kind, status, _rev in outcomes) == ["conflict", "ok"]
        for proc in procs:
            proc.join(timeout=10)
            assert proc.exitcode == 0
        latest = require_ccx_active(root).record(A)
        assert latest["createArgs"]["model"] in ("first", "second")
        assert latest["recordRevision"] == 1
    finally:
        release.set()
        for proc in procs:
            if proc.is_alive():
                proc.terminate()
                proc.join(3)
        manager.shutdown()


def test_semantic_receipt_prepared_retry_revokes_old_token_and_replays_rejection(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, confirmed=True)
    ctx = require_ccx_active(root)
    first = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                    operation_id="semantic-token", intent_sha256="a" * 64)
    assert first.state == "prepared" and not first.replayed
    after = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                    operation_id="semantic-token", intent_sha256="a" * 64)
    assert after.state == "prepared" and after.dispatch_token != first.dispatch_token
    calls = []
    with pytest.raises(NativeEffectIndeterminate, match="superseded"):
        ctx.dispatch_native_intent_once(first, bounded_submit=lambda deadline: calls.append(1))
    assert not calls
    ctx.dispatch_native_intent_once(
        after, bounded_submit=lambda deadline: calls.append(2) or {"nativeEvidenceSha256": "a" * 64}
    )
    assert calls == [2]
    settled = ctx.settle_native_intent(after, state="rejected", native_evidence_sha256="a" * 64)
    assert settled["state"] == "rejected"
    again = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                    operation_id="semantic-token", intent_sha256="a" * 64)
    assert again.replayed and again.state == "rejected"
    assert again.native_evidence_sha256 == "a" * 64
    assert again.dispatch_token is None
    with pytest.raises(NativeEffectIndeterminate):
        ctx.dispatch_native_intent_once(again, bounded_submit=lambda deadline: calls.append(3))
    assert calls == [2]


def test_semantic_receipt_terminal_replay_when_worker_becomes_terminal_only(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, confirmed=True)
    ctx = require_ccx_active(root)
    initial = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                      operation_id="prior-turn", intent_sha256="a" * 64)
    ctx.dispatch_native_intent_once(initial, bounded_submit=lambda deadline: {"transportAccepted": True})
    ctx.settle_native_intent(initial, state="accepted", native_evidence_sha256="b" * 64)
    receipt_path = ctx.receipt_path(A, "claude-delivery", "prior-turn")
    original_bytes, original_mtime = receipt_path.read_bytes(), receipt_path.stat().st_mtime_ns
    full_record = root / "identity" / "generations" / G1 / "workers" / "claude" / A / "record.json"
    source = json.loads(full_record.read_text())
    source["terminalOnly"] = True
    put(full_record, source)  # offline fixture change, not an API call
    replay = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                     operation_id="prior-turn", intent_sha256="a" * 64)
    assert replay.replayed and replay.state == "accepted"
    assert (receipt_path.read_bytes(), receipt_path.stat().st_mtime_ns) == (original_bytes, original_mtime)
    with pytest.raises(NativeActionDenied):
        ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                operation_id="new-turn", intent_sha256="a" * 64)
    assert not ctx.receipt_path(A, "claude-delivery", "new-turn").exists()


def test_semantic_pointer_change_between_prepared_and_dispatch_calls_no_provider(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, "codex", confirmed=True)
    ctx = require_ccx_active(root)
    p = ctx.begin_native_intent(A, action="send", family="codex-send",
                                operation_id="op", intent_sha256="a" * 64)
    assert ctx.read_receipt(A, "codex-send", "op")["state"] == "prepared"
    switch_pointer(root)
    calls = []
    with pytest.raises(StaleGeneration):
        ctx.dispatch_native_intent_once(p, bounded_submit=lambda deadline: calls.append(1))
    assert not calls
    original = root / "identity" / "generations" / G1 / "receipts" / "codex-send" / "codex" / A
    assert json.loads(next(original.glob("*.json")).read_text())["state"] == "prepared"


def test_semantic_pointer_change_after_durable_submission_fences_unknown(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, confirmed=True)
    ctx = require_ccx_active(root)
    p = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                operation_id="op", intent_sha256="a" * 64)
    calls = []
    def flip(stage):
        if stage == "after-submitted":
            switch_pointer(root)
    with pytest.raises(StaleGeneration):
        ctx.dispatch_native_intent_once(p, bounded_submit=lambda deadline: calls.append(1),
                                        _failpoint=flip)
    assert calls == []
    original = root / "identity" / "generations" / G1 / "receipts" / "claude-delivery" / "claude" / A
    assert json.loads(next(original.glob("*.json")).read_text())["state"] == "submitted-unknown"


def test_semantic_receipt_lost_reply_never_replays_provider(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, confirmed=True)
    ctx = require_ccx_active(root)
    p = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                operation_id="op", intent_sha256="a" * 64)
    calls = []
    def fake_native(_timeout):
        calls.append("original")
        raise TimeoutError("accepted but reply lost")
    with pytest.raises(NativeEffectIndeterminate):
        ctx.dispatch_native_intent_once(p, bounded_submit=fake_native)
    assert calls == ["original"]
    assert ctx.read_receipt(A, "claude-delivery", "op")["state"] == "submitted-unknown"
    with pytest.raises(NativeEffectIndeterminate):
        ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                operation_id="op", intent_sha256="a" * 64)
    with pytest.raises(NativeEffectIndeterminate):
        ctx.dispatch_native_intent_once(p, bounded_submit=fake_native)
    assert calls == ["original"]


def test_semantic_receipt_cannot_claim_terminal_before_native_submission(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A, confirmed=True)
    ctx = require_ccx_active(root)
    p = ctx.begin_native_intent(A, action="send", family="claude-delivery",
                                operation_id="op", intent_sha256="a" * 64)
    with pytest.raises(NativeEffectIndeterminate, match="before durable native dispatch"):
        ctx.settle_native_intent(p, state="accepted", native_evidence_sha256="f" * 64)
    assert ctx.read_receipt(A, "claude-delivery", "op")["state"] == "prepared"


def test_direct_generation_store_constructor_enforces_activation_lock(tmp_path):
    from ccx.store.store import GenerationStore
    from ccx.store.root import inspect_generation
    root = fixture_root(tmp_path)
    pointer = inspect_generation(root)
    (root / "identity" / "activation.lock").unlink()
    with pytest.raises(GenerationUnavailable, match="activation lock"):
        GenerationStore(pointer)
    with pytest.raises(GenerationUnavailable, match="activation lock"):
        require_ccx_active(root)


def test_runtime_seal_is_small_and_full_plan_can_be_large(tmp_path):
    from ccx.store.root import inspect_generation
    root = fixture_root(tmp_path)
    gen = root / "identity" / "generations" / G1
    plan = gen / "full-plan.json"
    # I3 owns verifying the full plan. I2b runtime must not deserialize it
    # for every call, only hash the tiny immutable manifest.json.
    large = {"sourceEvidence": "x" * (2 * 1024 * 1024)}
    put(plan, large)
    digest = hashlib.sha256(plan.read_bytes()).hexdigest()
    seal_file = gen / "manifest.json"
    seal = json.loads(seal_file.read_text())
    seal["planSha256"] = digest
    put(seal_file, seal)
    pointer_file = root / "identity" / "current.json"
    pointer = json.loads(pointer_file.read_text())
    pointer["manifestSha256"] = hashlib.sha256(seal_file.read_bytes()).hexdigest()
    put(pointer_file, pointer)
    assert len(seal_file.read_bytes()) < 65536
    assert len(plan.read_bytes()) > 2 * 1024 * 1024
    assert inspect_generation(root).generation == G1
    active = require_ccx_active(root)
    for _ in range(5):
        active.revalidate()
    assert active.pointer.manifest_sha256 == pointer["manifestSha256"]


def test_oversized_runtime_seal_refuses_even_when_pointer_checksum_matches(tmp_path):
    root = fixture_root(tmp_path)
    gen = root / "identity" / "generations" / G1
    seal_file = gen / "manifest.json"
    seal = json.loads(seal_file.read_text())
    seal["oversized"] = "x" * 70000
    put(seal_file, seal)
    pointer_file = root / "identity" / "current.json"
    pointer = json.loads(pointer_file.read_text())
    pointer["manifestSha256"] = hashlib.sha256(seal_file.read_bytes()).hexdigest()
    put(pointer_file, pointer)
    with pytest.raises(GenerationUnavailable, match="sealed generation manifest"):
        inspect_generation(root)


def test_pinned_worker_sidecar_read_cannot_follow_symlink_or_create_missing(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    ctx = require_ccx_active(root)
    assert ctx.read_worker_sidecar(A, "settings.json") is None
    dest = root / "identity" / "generations" / G1 / "workers" / "claude" / A / "settings.json"
    assert not dest.exists()
    outside = tmp_path / "native-provider-owned.json"
    put(outside, {"secret": "outside"})
    dest.symlink_to(outside)
    with pytest.raises(StorageIntegrityError, match="owner-private regular"):
        ctx.read_worker_sidecar(A, "settings.json")
    dest.unlink()
    put(dest, {"model": "allowed"})
    assert ctx.read_worker_sidecar("claude:" + A, "settings.json") == {"model": "allowed"}
    with pytest.raises(StorageIntegrityError):
        ctx.read_worker_sidecar(A, "../../provider")
    assert outside.read_text().find("outside") >= 0


def test_pointer_validity_rechecks_raw_hash_not_cached_mtime(tmp_path):
    root = fixture_root(tmp_path)
    active = require_ccx_active(root)
    pointer = root / "identity" / "current.json"
    before = pointer.stat().st_mtime_ns
    obj = json.loads(pointer.read_text())
    obj["manifestSha256"] = "f" * 64
    put(pointer, obj)
    os.utime(pointer, ns=(before, before))
    with pytest.raises(GenerationUnavailable):
        active.revalidate()



def test_direct_constructor_symlink_activation_lock_typed_refusal(tmp_path):
    from ccx.store.store import GenerationStore
    from ccx.store.root import inspect_generation
    root = fixture_root(tmp_path)
    pointer = inspect_generation(root)
    lock = root / "identity" / "activation.lock"
    lock.unlink()
    linked = tmp_path / "not-a-lock"
    linked.write_text("native provider")
    lock.symlink_to(linked)
    with pytest.raises(GenerationUnavailable, match="activation lock"):
        GenerationStore(pointer)
    assert linked.read_text() == "native provider"

def _parallel_initialize(path_text: str) -> tuple[bool, str]:
    from ccx.store.root import GenerationError, initialize_empty_root
    try:
        g = initialize_empty_root(Path(path_text))
        return True, g.generation
    except GenerationError as exc:
        return False, type(exc).__name__


def test_explicit_new_install_initializes_empty_sealed_active_root(tmp_path):
    from ccx.store.root import initialize_empty_root, inspect_generation
    root = tmp_path / "private-fresh-root"
    root.mkdir(mode=0o700)
    ctx = initialize_empty_root(root)
    assert ctx.generation.startswith("g-")
    assert inspect_generation(root).phase == "ACTIVE"
    assert ctx.root == root
    manifest = root / "identity" / "generations" / ctx.generation / "manifest.json"
    assert json.loads(manifest.read_text())["newEmptyInstall"] is True
    assert (root / "identity" / "activation.lock").stat().st_mode & 0o777 == 0o600
    assert manifest.stat().st_mode & 0o777 == 0o600
    assert not (root / "sessions").exists()
    with pytest.raises(GenerationUnavailable, match="empty"):
        initialize_empty_root(root)
    with pytest.raises(UnknownWorker):
        ctx.resolve(A)  # explicit init never creates an implicit worker


def test_explicit_init_rejects_legacy_nonempty_root_without_mutation(tmp_path):
    from ccx.store.root import initialize_empty_root
    root = tmp_path / "legacy"
    root.mkdir(mode=0o700)
    old = root / "sessions"
    old.mkdir(mode=0o700)
    record_bytes = b"legacy data stays intact"
    record_path = old / "aaaaaaaa.json"
    record_path.write_bytes(record_bytes)
    before = [(str(f.relative_to(root)), f.stat().st_mtime_ns)
              for f in root.rglob("*")]
    with pytest.raises(GenerationUnavailable, match="empty"):
        initialize_empty_root(root)
    assert not (root / "identity").exists()
    assert record_path.read_bytes() == record_bytes
    assert before == [(str(f.relative_to(root)), f.stat().st_mtime_ns)
                      for f in root.rglob("*")]


def test_explicit_init_failpoint_leaves_original_unknown_not_automatically_repaired(tmp_path):
    from ccx.store.root import initialize_empty_root
    for mark in ("after-init-marker", "after-activation-lock", "after-sealed-generation"):
        root = tmp_path / mark
        root.mkdir(mode=0o700)
        def fail(stage):
            if stage == mark:
                raise RuntimeError("power loss")
        with pytest.raises(RuntimeError):
            initialize_empty_root(root, _failpoint=fail)
        assert (root / ".ccx-init.lock").exists()
        assert not (root / "identity" / "current.json").exists()
        with pytest.raises(GenerationUnavailable, match="empty"):
            initialize_empty_root(root)
        with pytest.raises(GenerationUnavailable):
            require_ccx_active(root)


def test_explicit_init_rejects_symlink_and_unprivate_root_no_chmod(tmp_path):
    from ccx.store.root import GenerationError, initialize_empty_root
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    link = tmp_path / "link"
    link.symlink_to(root, target_is_directory=True)
    with pytest.raises(GenerationError):
        initialize_empty_root(link)
    assert not list(root.iterdir())
    root.chmod(0o755)
    with pytest.raises(GenerationUnavailable, match="private"):
        initialize_empty_root(root)
    assert root.stat().st_mode & 0o777 == 0o755
    assert not list(root.iterdir())


def test_explicit_init_concurrent_processes_create_one_generation_only(tmp_path):
    from ccx.store.root import initialize_empty_root
    root = tmp_path / "private-root"
    root.mkdir(mode=0o700)
    mp = multiprocessing.get_context("fork")
    with mp.Pool(4) as pool:
        outcomes = pool.map(_parallel_initialize, [str(root)] * 4)
    assert sum(success for success, _result in outcomes) == 1
    ctx = require_ccx_active(root)
    assert [g.name for g in (root / "identity" / "generations").iterdir()] == [ctx.generation]
    assert (root / ".ccx-init.lock").exists()
    assert not (root / "sessions").exists()


def test_ccx_init_cli_is_explicit_and_no_legacy_bypass(tmp_path):
    import subprocess
    import sys
    root = tmp_path / "selected-empty-ccx"
    root.mkdir(mode=0o700)
    env = {**os.environ, "CCX_ROOT": str(root), "PYTHONPATH": str(Path(__file__).parent.parent)}
    cmd = [sys.executable, "-B", "-m", "ccx", "init", "--root", str(root), "--json"]
    run = subprocess.run(cmd, env=env, text=True, capture_output=True, timeout=20)
    assert run.returncode == 0, run.stderr
    got = json.loads(run.stdout)
    assert got["root"] == str(root) and got["phase"] == "ACTIVE"
    assert got["workerCount"] == 0
    duplicate = subprocess.run(cmd, env=env, text=True, capture_output=True, timeout=20)
    assert duplicate.returncode != 0 and "already initialized" in duplicate.stderr
    different = tmp_path / "different"
    different.mkdir(mode=0o700)
    overridden = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "init", "--root", str(different)],
        env=env, text=True, capture_output=True, timeout=20,
    )
    assert overridden.returncode == 0, overridden.stderr
    assert (different / "identity" / "current.json").exists()
    assert not (root / "sessions").exists()



def test_cli_read_initializes_but_internal_relay_and_pure_rpc_do_not(tmp_path):
    import subprocess
    import sys
    root = tmp_path / "empty-new-root"
    root.mkdir(mode=0o700)
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    # Internal relays and protocol validation cannot initialize this root.
    relay = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "hooks-relay", "aaaaaaaa", "SessionStart"],
        env=env, capture_output=True, text=True, timeout=20,
    )
    assert relay.returncode == 3
    assert relay.stderr.startswith("ccx unavailable: ")
    assert not list(root.iterdir())

    pure_rpc = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "rpc"],
        env=env, input="", capture_output=True, text=True, timeout=20,
    )
    assert pure_rpc.returncode == 0 and pure_rpc.stderr == ""
    assert len(pure_rpc.stdout.splitlines()) == 1
    parsed = json.loads(pure_rpc.stdout)
    assert parsed["ok"] is False and parsed["error"]["code"] == "invalid_request"
    assert not list(root.iterdir())
    version = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "--version"],
        env=env, capture_output=True, text=True, timeout=20,
    )
    assert version.returncode == 0 and not list(root.iterdir())

    # A user read now creates one sealed generation and an empty inventory.
    listed = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "list", "--json"],
        env=env, capture_output=True, text=True, timeout=20,
    )
    assert listed.returncode == 0 and json.loads(listed.stdout) == []
    assert listed.stderr == f"ccx: created store at {root}\n"
    assert require_ccx_active(root).list_workers() == []
    missing = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "send", "aaaaaaaa", "hi"],
        env=env, capture_output=True, text=True, timeout=20,
    )
    assert missing.returncode != 0
    assert "ccx: created store" not in missing.stderr
    assert not (root / "sessions").exists()
    assert not (root / "run").exists()

def test_cli_active_list_peek_rpc_preserve_no_write_boundary(tmp_path):
    import subprocess
    import sys
    from ccx.store.root import initialize_empty_root
    root = tmp_path / "fresh"
    root.mkdir(mode=0o700)
    initialize_empty_root(root)
    paths_before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    listed = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "list", "--json"],
        env=env, capture_output=True, text=True, timeout=20,
    )
    assert listed.returncode == 0 and json.loads(listed.stdout) == []
    peek = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "peek", A],
        env=env, capture_output=True, text=True, timeout=20,
    )
    assert peek.returncode == 2
    assert peek.stderr == f"no such worker: {A}\n"
    rpc = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "rpc"],
        env=env, capture_output=True, text=True, timeout=20,
        input="",
    )
    assert rpc.returncode == 0  # V1 errors are in JSON envelope
    assert json.loads(rpc.stdout)["error"]["code"] == "invalid_request"
    assert sorted(str(p.relative_to(root)) for p in root.rglob("*")) == paths_before
    assert not (root / "sessions").exists()


# X1: runtime JSON reads are immutable *open-fd* snapshots. A live writer
# publishes a new inode with os.replace; migration source scans stay strict.
def test_x1_runtime_open_fd_snapshot_survives_atomic_record_replace(tmp_path, monkeypatch):
    import threading
    import ccx.store.root as generation

    root = fixture_root(tmp_path)
    ctx = add_worker(root, A)
    before = ctx.record(A)
    opened = threading.Event()
    release = threading.Event()
    result = []
    errors = []
    real_open = os.open

    def pause_after_open(name, flags, *args, **kwargs):
        child = real_open(name, flags, *args, **kwargs)
        if name == "record.json" and threading.current_thread().name == "x1-snapshot-reader":
            # Opening already pinned the OLD record inode; the live writer may
            # now atomically replace only its PATH while we still hold this fd.
            opened.set()
            if not release.wait(8):
                raise AssertionError("X1 read barrier did not release")
        return child

    monkeypatch.setattr(generation.os, "open", pause_after_open)

    def read_original():
        try:
            result.append(ctx.record(A))
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    reader = threading.Thread(target=read_original, name="x1-snapshot-reader", daemon=True)
    reader.start()
    try:
        assert opened.wait(6), "the original record inode was never opened"
        newer = ctx.mutate_record(A, patches={"name": "post-atomic-replacement"})
        assert newer["recordRevision"] == 1
    finally:
        release.set()
        reader.join(timeout=8)
    assert not reader.is_alive() and not errors, errors
    assert len(result) == 1 and result[0] == before
    assert result[0].get("recordRevision", 0) == 0
    fresh = ctx.record(A)
    assert fresh["name"] == "post-atomic-replacement"
    assert fresh["recordRevision"] == 1
    assert fresh["workerId"] == result[0]["workerId"] == A
    assert ctx.resolve(A).worker_id == A


def test_x1_concurrent_atomic_mutations_and_reads_never_misclassify_replace(tmp_path):
    import threading

    root = fixture_root(tmp_path)
    ctx = add_worker(root, A)
    writer_done = threading.Event()
    started = threading.Event()
    errors = []
    observed = []

    def mutate_many():
        try:
            started.set()
            for revision in range(1, 151):
                changed = ctx.mutate_record(A, patches={"name": f"revision-{revision}"})
                assert changed["recordRevision"] == revision
        except Exception as error:  # noqa: BLE001
            errors.append(error)
        finally:
            writer_done.set()

    def read_many():
        try:
            assert started.wait(5)
            reads = 0
            while reads < 550 or not writer_done.is_set():
                current = ctx.record(A)
                assert current["workerId"] == A and current["engine"] == "claude"
                assert current["native"]["sessionId"] == A
                assert type(current.get("recordRevision", 0)) is int
                assert 0 <= current.get("recordRevision", 0) <= 150
                if reads % 13 == 0:
                    assert ctx.resolve(A).worker_id == A
                observed.append(current.get("recordRevision", 0))
                reads += 1
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    writer = threading.Thread(target=mutate_many, daemon=True)
    reader = threading.Thread(target=read_many, daemon=True)
    reader.start()
    writer.start()
    writer.join(timeout=24)
    reader.join(timeout=24)
    assert not writer.is_alive() and not reader.is_alive(), "X1 writer/read test hung"
    assert not errors, errors
    assert len(observed) >= 550
    assert ctx.record(A)["recordRevision"] == 150
    assert ctx.record(A)["name"] == "revision-150"


@pytest.mark.parametrize("change", ["truncate", "grow", "same-size-edit", "mode-flip"])
def test_x1_runtime_same_inode_modified_in_place_fails_closed(tmp_path, monkeypatch, change):
    import ccx.store.root as generation

    directory = tmp_path / "owned-json"
    privdir(directory)
    path = directory / "record.json"
    path.write_bytes(b'{"value":"AAAA","count":1}\n')
    path.chmod(0o600)
    inode = path.stat().st_ino
    original_read = os.read
    changed = False

    def mutate_before_read(child, size):
        nonlocal changed
        if not changed and os.fstat(child).st_ino == inode:
            changed = True
            old_mtime = path.stat().st_mtime_ns
            if change == "truncate":
                with path.open("r+b") as output:
                    output.truncate(5)
            elif change == "grow":
                with path.open("ab") as output:
                    output.write(b' ' * 64)
            elif change == "same-size-edit":
                with path.open("r+b") as output:
                    output.seek(10)
                    output.write(b"B")  # same-size content overwrite
            else:
                path.chmod(0o644)  # same inode, now unsafe permissions
            # Ensure content edits are detected even with coarse mtime ticks.
            if change != "mode-flip":
                os.utime(path, ns=(old_mtime + 2_000_000_000, old_mtime + 2_000_000_000))
        return original_read(child, size)

    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        with monkeypatch.context() as patched:
            patched.setattr(generation.os, "read", mutate_before_read)
            with pytest.raises(StorageIntegrityError, match="inode changed during read"):
                store_root._read_json(directory_fd, "record.json")
    finally:
        os.close(directory_fd)
    assert changed


@pytest.mark.parametrize("kind", [
    "missing", "symlink", "symlink-swap", "fifo", "directory",
    "world-readable", "untrusted-owner", "oversize", "malformed-json",
    "invalid-utf8", "non-object", "private-valid",
])
def test_x1_runtime_live_reader_keeps_security_and_json_bounds(tmp_path, monkeypatch, kind):
    import ccx.store.root as generation
    import types

    owned = tmp_path / "pinned"
    privdir(owned)
    leaf = owned / "record.json"
    secret = tmp_path / "outside-secret.json"
    put(secret, {"secret": "provider-owned-must-not-be-read"})
    if kind == "symlink":
        leaf.symlink_to(secret)
    elif kind == "symlink-swap":
        put(leaf, {"value": "before"})
    elif kind == "fifo":
        os.mkfifo(leaf, mode=0o600)
    elif kind == "directory":
        privdir(leaf)
    elif kind == "world-readable":
        put(leaf, {"value": "unsafe"})
        leaf.chmod(0o644)
    elif kind == "oversize":
        leaf.write_bytes(b"{" + b"x" * 200 + b"}")
        leaf.chmod(0o600)
    elif kind == "malformed-json":
        leaf.write_bytes(b'{"missing":')
        leaf.chmod(0o600)
    elif kind == "invalid-utf8":
        leaf.write_bytes(b'{"invalid":"\xff"}')
        leaf.chmod(0o600)
    elif kind == "non-object":
        leaf.write_bytes(b'["valid JSON", "not an object"]')
        leaf.chmod(0o600)
    elif kind in ("untrusted-owner", "private-valid"):
        put(leaf, {"value": "allowed"})

    if kind == "symlink-swap":
        real_open = os.open
        swapped = False

        def swap_before_open(name, flags, *args, **kwargs):
            nonlocal swapped
            if name == "record.json" and not swapped:
                swapped = True
                leaf.unlink()
                leaf.symlink_to(secret)
            return real_open(name, flags, *args, **kwargs)

        monkeypatch.setattr(generation.os, "open", swap_before_open)
    elif kind == "untrusted-owner":
        real_fstat = os.fstat
        inode = leaf.stat().st_ino

        def foreign_owner(child):
            actual = real_fstat(child)
            if actual.st_ino != inode:
                return actual
            return types.SimpleNamespace(
                st_mode=actual.st_mode, st_uid=os.getuid() + 1,
                st_dev=actual.st_dev, st_ino=actual.st_ino,
                st_size=actual.st_size, st_mtime_ns=actual.st_mtime_ns,
            )

        monkeypatch.setattr(generation.os, "fstat", foreign_owner)

    parent = os.open(owned, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    started = time.monotonic()
    try:
        if kind == "missing":
            assert store_root._read_json(parent, "record.json") is None
            assert not leaf.exists()
        elif kind == "private-valid":
            assert store_root._read_json(parent, "record.json") == {"value": "allowed"}
        else:
            with pytest.raises(StorageIntegrityError):
                store_root._read_json(parent, "record.json", max_bytes=128)
    finally:
        os.close(parent)
    if kind == "fifo":
        assert time.monotonic() - started < .4, "FIFO open must never block"
    if kind in {"symlink", "symlink-swap"}:
        assert leaf.is_symlink()
        assert secret.read_text().find("provider-owned-must-not-be-read") >= 0
    if kind == "world-readable":
        assert leaf.stat().st_mode & 0o004  # never chmod an unsafe input


def test_x1_runtime_snapshot_does_not_weaken_pointer_or_original_native_identity(tmp_path):
    from ccx.ops.creation_steps import prepare_worker

    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    prepare_worker(ctx, engine="claude", cwd=str(tmp_path), worker_id=A)
    assert ctx.resolve(A).worker_id == A
    with pytest.raises(NativeActionDenied):
        # A v2 first record is never sufficient proof of native activation.
        ctx.authorize_native_action(A, "send")
    claim_path = root / "identity" / "generations" / G1 / "claims" / (A + ".json")
    claim = json.loads(claim_path.read_text())
    claim["nativeActivation"] = {
        "state": "confirmed", "source": "claude-session:foreign",
        "submissionId": "forged", "nativeEvidenceSha256": "f" * 64,
        "nativeBinding": {"sessionId": A},
    }
    put(claim_path, claim)
    with pytest.raises(NativeActionDenied):
        ctx.authorize_native_action(A, "send")
    owner_path = root / "identity" / "generations" / G1 / "owners" / (A + ".json")
    owner = json.loads(owner_path.read_text())
    owner["engine"] = "codex"
    put(owner_path, owner)
    with pytest.raises(StorageIntegrityError):
        ctx.resolve(A)
    # A valid new pointer is still a generation fence despite both pointers
    # being internally consistent JSON snapshots.
    switch_pointer(root)
    with pytest.raises(StaleGeneration):
        ctx.revalidate()
    with pytest.raises(StaleGeneration):
        ctx.record(A)


@pytest.mark.parametrize("malformed", [False, True])
def test_x1_runtime_reader_closes_child_fd_on_success_and_failure(tmp_path, monkeypatch,
                                                                 malformed):
    import ccx.store.root as generation

    owned = tmp_path / "owned"
    privdir(owned)
    leaf = owned / "record.json"
    leaf.write_bytes(b'{"valid":true}\n' if not malformed else b'{"broken":')
    leaf.chmod(0o600)
    parent = os.open(owned, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    original_open = os.open
    opened = []

    def capture_open(name, flags, *args, **kwargs):
        child = original_open(name, flags, *args, **kwargs)
        if name == "record.json" and kwargs.get("dir_fd") == parent:
            opened.append(child)
        return child

    try:
        with monkeypatch.context() as patch:
            patch.setattr(generation.os, "open", capture_open)
            if malformed:
                with pytest.raises(StorageIntegrityError, match="malformed JSON"):
                    store_root._read_json(parent, "record.json")
            else:
                assert store_root._read_json(parent, "record.json") == {"valid": True}
        assert len(opened) == 1
        with pytest.raises(OSError):
            os.fstat(opened[0])  # EBADF: closed, never retained across calls
    finally:
        os.close(parent)
