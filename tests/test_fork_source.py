"""F1a: managed full-UUID fork SOURCE planning only; no native fork or create."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.ops import forking
from ccx.store import paths
from ccx.rpc import rpc
from ccx.errors import OperationConflict, SessionUnavailable
from ccx.ops.creation_steps import prepare_worker, activate_prepared_worker
from ccx.store.identity import InvalidTarget, UnknownWorker
from ccx.store.root import CreationIndeterminate, NativeActionDenied, require_ccx_active
from ccx.store.store import fingerprint_v2
from test_generation_store import A, B, C, short_root as short_root


@pytest.fixture
def sources(short_root, tmp_path, monkeypatch):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    native_home = tmp_path / "native-home"
    native_home.mkdir(mode=0o700)
    monkeypatch.setattr(paths, "CLAUDE_HOME", native_home)
    store = require_ccx_active(short_root)
    first = prepare_worker(
        store, engine="claude", cwd=str(tmp_path), worker_id=A, model="sonnet",
        effort="medium", permissions="restricted", agent="Explore",
        allowed_tools="Read", idle_timeout=321,
    )
    second = prepare_worker(
        store, engine="codex", cwd=str(tmp_path), worker_id=B, model="model-src",
        effort="high", sandbox="workspace-write", approval_policy="on-request",
        idle_timeout=456,
    )
    for prepared in (first, second):
        def native(_seconds, p=prepared):
            claim = store.native_activation(p.worker)
            assert claim["state"] == "submitted-unknown"
            return {
                "accepted": True, "workerId": p.worker_id, "engine": p.engine,
                "source": p.native_source, "submissionId": claim["submissionId"],
                "nativeEvidenceSha256": "a" * 64,
                "nativeBinding": (
                    {"sessionId": p.record["native"]["sessionId"]}
                    if p.engine == "claude" else {"threadId": "native-thread-B"}
                ),
            }
        activate_prepared_worker(store, prepared, bounded_native_submit=native)
    store.mutate_record(A, patches={"lifecycle.state": "idle", "stats.turns": 1})
    store.mutate_record(B, patches={
        "lifecycle.state": "idle",
        "stats.turns": 1,
        "lastTurn": {"turnId": "native-turn-9", "status": "completed"},
    })
    sid = first.record["native"]["sessionId"]
    original = paths.transcript_path_for(str(tmp_path), sid)
    original.parent.mkdir(mode=0o700, parents=True)
    (native_home / "projects").chmod(0o700)
    original.parent.chmod(0o700)
    original.write_text("\n".join(json.dumps(row) for row in (
        {"type": "user", "uuid": "u1", "sessionId": sid,
         "message": {"content": "hello"}},
        {"type": "assistant", "uuid": "a1", "sessionId": sid,
         "message": {"content": [{"type": "text", "text": "original"}],
                     "stop_reason": "end_turn"}},
    )) + "\n")
    original.chmod(0o600)
    observed = []

    def native_history(tid, *, cwd, worker_id, timeout, expected_source,
                       limit=None, newest_first=False):
        assert expected_source == store.codex_source(worker_id)
        observed.append((tid, worker_id))
        assert (tid, worker_id, cwd) == ("native-thread-B", B, str(tmp_path))
        return {"id": tid, "threadSource": f"ccx-worker:{B}",
                "status": {"type": "idle"},
                "turns": [{"id": "native-turn-9", "status": "completed", "items": []}]}

    monkeypatch.setattr(codex_runtime, "read_thread", native_history)
    return store, original, observed


def _assert_unclaimed(store, old_owners):
    assert {row["worker_id"] for row in store.list_workers(limit=None)} == old_owners


def test_f1a_owned_native_sources_settled_settings_and_preclaim_refusals(
    sources, monkeypatch,
):
    store, original, observed = sources
    before = original.read_bytes()
    original_owners = {A, B}
    assert A[:8] == B[:8]
    claude = forking.plan("claude:" + A, model="opus")
    anchor = claude["fork_anchor"]
    assert claude["engine"] == anchor["engine"] == "claude"
    assert anchor["sourceWorkerId"] == A
    assert anchor["sourceNativeSessionId"] == store.record(A)["native"]["sessionId"]
    assert anchor["sourceNativeSessionId"] != A
    assert anchor["resumeAt"] == "a1" and anchor["transcript"] == str(original)
    assert claude["cwd"] == store.record(A)["cwd"]
    assert claude["launch"]["model"] == "opus"
    assert claude["launch"]["effort"] == "medium"
    assert claude["launch"]["agent"] == "Explore"
    assert claude["launch"]["allowed_tools"] == "Read"
    assert claude["launch"]["permissions"] == "restricted"
    assert claude["launch"]["idle_timeout"] == 321
    assert claude["name"] == f"ccx-{A[:8]} (fork)"
    assert not claude["existing"]
    assert claude["seed"]["stats"]["turns"] == 1
    codex = forking.plan(B, effort="low")
    assert observed == [("native-thread-B", B)]
    assert codex["fork_anchor"]["sourceWorkerId"] == B
    assert codex["fork_anchor"]["sourceNativeThreadId"] == "native-thread-B"
    assert codex["fork_anchor"]["lastTurnId"] == "native-turn-9"
    assert codex["launch"]["model"] == "model-src"
    assert codex["launch"]["sandbox"] == "workspace-write"
    assert codex["launch"]["approval_policy"] == "on-request"
    assert codex["launch"]["effort"] == "low"
    assert codex["launch"]["idle_timeout"] == 456
    assert codex["seed"]["lastTurn"]["turnId"] == "native-turn-9"
    assert original.read_bytes() == before
    _assert_unclaimed(store, original_owners)

    # No native history means no fork; stale record does not invent a turn.
    for patches, reason in (
        ({"lifecycle.state": "busy"}, "source_busy"),
        ({"lifecycle.state": "waiting"}, "source_waiting"),
        ({"pendingSend": {"deliveryId": "pending"}}, "source_busy"),
        ({"activeCompact": {"operationId": "compacting"}}, "source_busy"),
    ):
        store.mutate_record(B, patches=patches)
        with pytest.raises(OperationConflict) as refused:
            forking.plan(B)
        assert refused.value.reason == reason
        store.mutate_record(B, patches={
            **{key: None for key in patches if key != "lifecycle.state"},
            "lifecycle.state": "idle",
        })
    with pytest.raises(InvalidTarget):
        forking.plan(A[:8])
    with pytest.raises(UnknownWorker):
        forking.plan(store.record(A)["native"]["sessionId"])
    # The accepted semantic receipts/compact journal are additional original
    # source gates, not a new progress tracker or provider operation.
    from ccx.codex import delivery as codex_delivery
    from ccx.ops import compact_ops
    with monkeypatch.context() as patch:
        patch.setattr(codex_delivery, "iter_receipts",
                      lambda _owner: iter(({"state": "submitted"},)))
        with pytest.raises(OperationConflict, match="pending original send"):
            forking.plan(B)
    # An accepted steer is never settled by reconcile; with the original
    # native thread idle and its last turn completed, it is not busy.
    with monkeypatch.context() as patch:
        patch.setattr(codex_delivery, "iter_receipts", lambda _owner: iter(({
            "state": "accepted", "route": "steer",
            "turnId": "native-turn-9", "settlement": None,
        },)))
        steered = forking.plan(B)
    assert steered["fork_anchor"]["lastTurnId"] == "native-turn-9"
    assert steered["existing"] is False
    with monkeypatch.context() as patch:
        patch.setattr(compact_ops, "read_journal",
                      lambda _owner: {"operations": [{"status": "pasted"}]})
        with pytest.raises(OperationConflict, match="native compact pending"):
            forking.plan(A)
    # A foreign native response may name the exact thread but not its owner.
    from ccx.codex import runtime as codex_runtime
    original_read = codex_runtime.read_thread
    codex_runtime.read_thread = lambda *_a, **_kw: {
        **original_read("native-thread-B", cwd=store.record(B)["cwd"],
                       worker_id=B, timeout=1,
                       expected_source=store.codex_source(B)),
        "threadSource": "ccx-worker:" + A,
    }
    try:
        with pytest.raises(SessionUnavailable, match="source conversation changed"):
            forking.plan(B)
    finally:
        codex_runtime.read_thread = original_read
    _assert_unclaimed(store, original_owners)
    # Another fully owned but pre-native worker is not a native fork source.
    prepare_worker(store, engine="claude", cwd=str(original.parent),
                   worker_id=C)
    # Even a forged idle-looking lifecycle cannot override an unconfirmed
    # first native activation (no original SessionStart).
    store.mutate_record(C, patches={"lifecycle.state": "idle"})
    with pytest.raises(NativeActionDenied):
        forking.plan(C)


def _publish_frozen_child(store, selected, child_id=C, native_sid=None):
    anchor = selected["fork_anchor"]
    engine = anchor["engine"]
    digest = fingerprint_v2(
        engine=engine, cwd=anchor["cwd"], create_args=anchor["createArgs"],
        fork_anchor=anchor,
    )
    sid = native_sid or str(uuid4())
    original_native = {"sessionId": sid} if engine == "claude" else {}
    reserved = store.reserve_creation(
        child_id, engine=engine, fingerprint=digest,
        native_binding=original_native, fork_anchor=anchor,
    )
    fork = {key: anchor[key] for key in (
        ("engine", "sourceWorkerId", "sourceNativeSessionId", "resumeAt", "forkedAtMs")
        if engine == "claude" else
        ("engine", "sourceWorkerId", "sourceNativeThreadId", "lastTurnId", "forkedAtMs")
    )}
    store.publish_first_record(reserved, {
        "schemaVersion": 2, "workerId": child_id, "engine": engine,
        "short": child_id[:8], "name": "original fork name",
        "cwd": anchor["cwd"], "createArgs": anchor["createArgs"],
        "createSha256": digest, "fingerprintVersion": 2, "forkAnchor": anchor,
        "forkedFrom": fork, "recordRevision": 0,
        "native": {**original_native, "nativeState": "not-started-proven",
                   **({"threadId": None} if engine == "codex" else {})},
        "terminalOnly": False,
        "lifecycle": {"state": "idle-killed"},
    })
    return sid


def test_f1a_same_child_frozen_retry_without_source_reads_or_remint(sources, monkeypatch):
    store, _original, observed = sources
    selected = forking.plan(A, model="opus")
    child_sid = _publish_frozen_child(store, selected)
    original_anchor = selected["fork_anchor"]
    assert child_sid not in (A, B, C, original_anchor["sourceNativeSessionId"])
    store.mutate_record(C, patches={
        "name": "renamed after create", "createArgs.model": "mutated after fork",
    })
    store.mutate_record(A, patches={"lifecycle.state": "dead",
                                    "createArgs.model": "new source model"})
    with monkeypatch.context() as patch:
        patch.setattr(forking, "_claude_point", lambda *_a: pytest.fail(
            "same child retry must never reread source"))
        patch.setattr(store.__class__, "resolve", lambda *_a: pytest.fail(
            "same child retry must not require the source still being live"))
        again = forking.plan("claude:" + A, worker_id=C, model="opus")
    assert again["existing"] and again["source"] is None
    assert again["fork_anchor"] == original_anchor
    assert again["launch"]["model"] == "opus"
    assert again["seed"] == selected["seed"]
    assert again["name"] == "renamed after create"
    assert store.record(C)["native"]["sessionId"] == child_sid
    assert observed == []
    for target, override in ((B, "opus"), (A, "changed"), (A, None)):
        with pytest.raises(OperationConflict, match="another original"):
            forking.plan(target, worker_id=C, model=override)
    with pytest.raises(OperationConflict):
        forking.plan(A, worker_id=A)  # ordinary nonfork source, never its own child

    # Original reserved-but-no-record identity cannot be reconstructed.
    reserved_id = "cccccccc-1234-4000-8000-000000000004"
    frozen = selected["fork_anchor"]
    store.reserve_creation(
        reserved_id, engine="claude",
        fingerprint=fingerprint_v2(
            engine="claude", cwd=frozen["cwd"],
            create_args=frozen["createArgs"], fork_anchor=frozen,
        ),
        native_binding={"sessionId": str(uuid4())}, fork_anchor=frozen,
    )
    with pytest.raises(CreationIndeterminate, match=reserved_id):
        forking.plan(A, worker_id=reserved_id, model="opus")
    assert store.record(C)["forkAnchor"] == original_anchor
    # Corrupt only this disposable child record: the source native point
    # cannot be overwritten by a matching owner/claim with different lineage.
    record_path = store.worker_path(C)
    forged = json.loads(record_path.read_text())
    forged["forkedFrom"]["resumeAt"] = "not-original"
    record_path.write_text(json.dumps(forged))
    with pytest.raises(CreationIndeterminate, match="native point"):
        forking.plan(A, worker_id=C, model="opus")


def test_f1a_lineage_copy_proof_pure_and_existing_revive_never_recopies(
    sources, monkeypatch,
):
    store, _original, _observed = sources
    planned = forking.plan(A)
    child_sid = _publish_frozen_child(store, planned)
    frozen = planned["fork_anchor"]
    record = store.record(C)
    assert forking.claude_fork_pending(record)
    assert forking.inherited_offset(record) == 0
    fake_tasks = [{"task_id": "old"}, {"task_id": "new"}]
    assert forking.inherited_task_filter(record, fake_tasks) == fake_tasks
    assert forking.public_lineage(record) == {
        "worker": "claude:" + A, "worker_id": A,
        "engine": "claude", "short": A[:8],
        "session_id": frozen["sourceNativeSessionId"],
        "thread_id": None, "forked_at_ms": frozen["forkedAtMs"],
    }
    proof = {
        "workerId": C, "nativeSessionId": child_sid, "forkAnchor": frozen,
        "copiedAtMs": 100, "inheritedBytes": 256, "inheritedOpenTaskIds": ["old"],
    }
    proven = {**record, "lifecycle": {**record["lifecycle"], "nativeForkProof": proof}}
    assert not forking.claude_fork_pending(proven)
    assert forking.inherited_offset(proven) == 256
    assert forking.inherited_task_filter(proven, fake_tasks) == [{"task_id": "new"}]
    assert forking.claude_fork_pending({
        **proven, "lifecycle": {"nativeForkProof": {**proof, "nativeSessionId": A}}
    })
    assert forking.inherited_offset({
        **proven, "lifecycle": {"nativeForkProof": {**proof, "forkAnchor": {}}}
    }) == 0
    assert forking.public_lineage({"forkedFrom": {"engine": "claude", "short": A[:8]}})[
        "worker"
    ] is None

    # Mark the original child fake-native confirmed, then exercise the real
    # rpc._respawn_locked -> forking.ensure_claude_fork_materialized call site.
    claim = store.reserve_creation(
        C, engine="claude", fingerprint=record["createSha256"],
        native_binding={"sessionId": child_sid}, fork_anchor=frozen,
    )
    pending = store.prepare_native_activation(claim, native_source="claude-session:" + child_sid)
    store.native_activation_dispatch(
        pending, bounded_submit=lambda _budget: {"ok": True},
    )
    store.confirm_native_activation(
        pending, native_evidence_sha256="b" * 64,
        native_binding={"sessionId": child_sid},
    )
    store.bind_confirmed_native(C)
    monkeypatch.setattr(rpc, "_drain_old_host", lambda *_a, **_k: True)
    monkeypatch.setattr(rpc, "_missing_claude_bin", lambda: None)
    monkeypatch.setattr(rpc, "ensure_trusted_paths", lambda *_a, **_k: None)
    monkeypatch.setattr(rpc, "write_settings", lambda *_a, **_k: None)
    monkeypatch.setattr(rpc, "spawn_host_detached", lambda *_a, **_k: pytest.fail(
        "unproven UUID fork must not launch a new host"))
    monkeypatch.setattr(forking, "_run_materializer", lambda *_a, **_k: pytest.fail(
        "unproven UUID fork must never run old copy"))
    assert rpc._respawn_locked(C) != 0
    assert forking.claude_fork_pending(store.record(C))
    assert store.native_activation(C)["state"] == "confirmed"
