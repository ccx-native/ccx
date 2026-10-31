"""I2b real module-level record readers/writers use exact UUID and disk CAS."""
from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path

import pytest

from test_generation_store import A, B, C, G1, fixture_root, put, switch_pointer
from ccx import constants
from ccx.constants import S
from ccx.store import records
from ccx.codex import records as codex_records
from ccx.codex import runtime as codex_runtime
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import GenerationUnavailable, RecordConflict, StaleGeneration, StorageIntegrityError, require_ccx_active
from ccx.store.identity import EngineMismatch, InvalidTarget, UnknownWorker
from ccx.errors import InvalidRequest


@pytest.fixture
def pair(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    store = require_ccx_active(root)
    claude = prepare_worker(store, engine="claude",
                            cwd=str(tmp_path), worker_id=A)
    codex = prepare_worker(store, engine="codex",
                           cwd=str(tmp_path), worker_id=B)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    return root, store, claude, codex


def test_module_record_reads_use_full_uuid_and_no_legacy_paths(pair):
    root, store, claude, codex = pair
    record_a = records.read_record(A)
    record_b = codex_records.read_record(f"codex:{B}")
    assert record_a["workerId"] == A
    assert record_b["workerId"] == B
    assert record_a["native"]["sessionId"] == claude.record["native"]["sessionId"]
    assert record_b["native"]["threadId"] is None
    assert records.read_record(f"claude:{A}") == record_a
    assert codex_records.read_record(B) == record_b
    assert [x["workerId"] for x in records.iter_session_records()] == [A]
    assert [x["workerId"] for x in codex_records.iter_records()] == [B]
    assert codex_runtime.record(B)["workerId"] == B
    assert codex_runtime.record(f"codex:{B}")["workerId"] == B
    with pytest.raises(InvalidRequest, match="a Claude handle"):
        codex_runtime.record(f"claude:{A}")
    with pytest.raises(InvalidTarget):
        codex_runtime.record("claude:")
    for f in (records.read_record, codex_records.read_record):
        with pytest.raises(InvalidTarget):
            f("aaaaaaaa")
        with pytest.raises(InvalidTarget):
            f("same name")
    with pytest.raises(EngineMismatch):
        records.read_record(f"codex:{B}")
    with pytest.raises(EngineMismatch):
        codex_records.read_record(f"claude:{A}")
    with pytest.raises(UnknownWorker):
        records.read_record(C)
    with pytest.raises(UnknownWorker):
        codex_records.read_record(C)
    assert not (root / "sessions").exists()
    assert not (root / "codex").exists()
    assert not (root / "pid").exists()
    assert not (root / "run").exists()


def test_original_cas_snapshot_rejects_stale_while_preserving_other_changes(pair):
    root, store, claude, _ = pair
    first = records.read_record(A)
    stale = records.read_record(A)
    assert first["recordRevision"] == 0
    first["stats"]["turns"] = 5
    records.write_record(first)
    assert first["recordRevision"] == 1
    assert first["stats"]["turns"] == 5
    stale["name"] = "stale name"
    with pytest.raises(RecordConflict, match="stale"):
        records.write_record(stale)
    after = records.read_record(A)
    assert after["stats"]["turns"] == 5
    assert after["name"] != "stale name"
    assert after["native"]["sessionId"] == claude.record["native"]["sessionId"]
    updated = records.merge_lifecycle(A, state="idle", hostPid=456)
    assert updated["recordRevision"] == 2
    assert updated["lifecycle"]["hostPid"] == 456
    assert updated["lifecycle"]["state"] == "idle"
    assert updated["lifecycle"]["since"] >= after["lifecycle"]["since"]
    assert records.read_record(A)["stats"]["turns"] == 5
    # Another process's analytics update must be preserved in a later
    # correctly refreshed snapshot, not overwritten from a stale one.
    store.mutate_record(A, patches={"stats.sampleCount": 3})
    renewed = records.read_record(A)
    renewed["name"] = "renamed"
    records.write_record(renewed)
    assert renewed["recordRevision"] == 4
    assert records.read_record(A)["stats"]["sampleCount"] == 3
    assert not (root / "sessions").exists()


def test_codex_module_writer_requires_version_and_preserves_native_thread(pair):
    root, store, _, codex = pair
    first = codex_records.read_record(B)
    stale = codex_records.read_record(B)
    first["stats"]["turns"] = 2
    codex_records.write_record(first)
    assert first["recordRevision"] == 1
    assert codex_records.read_record(B)["stats"]["turns"] == 2
    stale["lifecycle"]["state"] = "working"
    with pytest.raises(RecordConflict):
        codex_records.write_record(stale)
    next_ = codex_records.read_record(B)
    next_["lifecycle"]["state"] = "idle"
    codex_records.write_record(next_)
    assert next_["recordRevision"] == 2
    assert next_["stats"]["turns"] == 2
    assert next_["native"]["threadId"] is None
    assert next_["native"]["nativeState"] == "not-started-proven"
    assert not (root / "codex").exists()


@pytest.mark.parametrize("engine,field,value", [
    ("claude", "workerId", C),
    ("claude", "engine", "codex"),
    ("claude", "native", {"sessionId": "stolen"}),
    ("claude", "createdAt", 0),
    ("claude", "createSha256", "a" * 64),
    ("claude", "fingerprintVersion", 1),
    ("claude", "terminalOnly", True),
    ("claude", "cwd", "/other"),
    ("codex", "native", {"threadId": "provider-thief"}),
    ("codex", "forkAnchor", {"sourceWorkerId": C}),
])
def test_module_snapshot_rejects_immutable_identity_change(pair, engine, field, value):
    root, store, claude, codex = pair
    wid = A if engine == "claude" else B
    module = records if engine == "claude" else codex_records
    before = module.read_record(wid)
    modified = module.read_record(wid)
    modified[field] = value
    with pytest.raises((StorageIntegrityError, UnknownWorker)):
        module.write_record(modified)
    assert module.read_record(wid) == before
    assert not (root / "sessions").exists()


def test_record_writer_refuses_missing_revision_deleted_fields_and_nested_loss(pair):
    _, _, _, _ = pair
    original = records.read_record(A)
    missing = dict(original)
    missing.pop("recordRevision")
    with pytest.raises(RecordConflict, match="[rR]evision"):
        records.write_record(missing)
    without_lifecycle = dict(original)
    without_lifecycle.pop("lifecycle")
    with pytest.raises(StorageIntegrityError, match="remove"):
        records.write_record(without_lifecycle)
    truncated = records.read_record(A)
    truncated["lifecycle"] = {"state": "idle"}
    with pytest.raises(StorageIntegrityError, match="drop"):
        records.write_record(truncated)
    assert records.read_record(A) == original


def _old_snapshot_writer(args):
    root, worker, updated_name, started, release, results = args
    from pathlib import Path
    from ccx import constants
    from ccx.store import records
    from ccx.store.root import RecordConflict
    constants.CCX_ROOT = Path(root)
    snapshot = records.read_record(worker)
    started.put(snapshot["recordRevision"])
    release.wait(timeout=10)
    snapshot["name"] = updated_name
    try:
        records.write_record(snapshot)
        results.put(("ok", updated_name))
    except RecordConflict:
        results.put(("stale", updated_name))


def test_real_multiprocess_old_snapshot_writer_cas_conflicts_one_winner(pair):
    root, store, _, _ = pair
    mp = multiprocessing.get_context("fork")
    manager = mp.Manager()
    started, results, release = manager.Queue(), manager.Queue(), manager.Event()
    tasks = [
        mp.Process(
            target=_old_snapshot_writer,
            args=((str(root), A, f"new-{i}", started, release, results),),
        )
        for i in range(5)
    ]
    try:
        for process in tasks:
            process.start()
        for _ in tasks:
            assert started.get(timeout=10) == 0
        release.set()
        outcomes = [results.get(timeout=10) for _ in tasks]
        assert sorted(status for status, _name in outcomes) == ["ok"] + ["stale"] * 4
        for process in tasks:
            process.join(5)
            assert process.exitcode == 0
        latest = records.read_record(A)
        assert latest["recordRevision"] == 1
        assert latest["name"] in {v for status, v in outcomes if status == "ok"}
        assert latest["stats"]["turns"] == 0
    finally:
        release.set()
        for process in tasks:
            if process.is_alive():
                process.terminate()
                process.join(3)
        manager.shutdown()


def _lifecycle_patcher(args):
    root, worker, field, value, started, release, result = args
    from pathlib import Path
    from ccx import constants
    from ccx.store import records
    constants.CCX_ROOT = Path(root)
    started.put(field)
    release.wait(timeout=10)
    updated = records.merge_lifecycle(worker, **{field: value})
    result.put((field, updated["recordRevision"]))


def test_real_multiprocess_independent_lifecycle_patches_are_lossless(pair):
    root, store, _, _ = pair
    mp = multiprocessing.get_context("fork")
    manager = mp.Manager()
    started, results, release = manager.Queue(), manager.Queue(), manager.Event()
    edits = [("hostPid", 111), ("hostStart", "positive"), ("workerPid", 222),
             ("lastError", "none"), ("state", "idle")]
    processes = [
        mp.Process(target=_lifecycle_patcher,
                   args=((str(root), A, k, v, started, release, results),))
        for k, v in edits
    ]
    try:
        for proc in processes:
            proc.start()
        for _ in processes:
            started.get(timeout=10)
        release.set()
        for _ in processes:
            results.get(timeout=10)
        for proc in processes:
            proc.join(5)
            assert proc.exitcode == 0
        lc = records.read_record(A)["lifecycle"]
        assert all(lc[k] == v for k, v in edits)
        assert records.read_record(A)["recordRevision"] == len(edits)
    finally:
        release.set()
        for proc in processes:
            if proc.is_alive():
                proc.terminate()
                proc.join(3)
        manager.shutdown()


def test_records_module_stale_generation_refuses_before_snapshot_write(pair):
    root, store, claude, codex = pair
    original = records.read_record(A)
    switch_pointer(root)
    with pytest.raises((StaleGeneration, GenerationUnavailable, UnknownWorker)):
        records.read_record(A)
    with pytest.raises((StaleGeneration, GenerationUnavailable, UnknownWorker)):
        codex_records.read_record(B)
    original["name"] = "unapproved"
    with pytest.raises((StaleGeneration, GenerationUnavailable, UnknownWorker)):
        records.write_record(original)
    prior = (root / "identity" / "generations" / G1 /
             "workers" / "claude" / A / "record.json")
    assert json.loads(prior.read_text())["name"] != "unapproved"
    assert not (root / "sessions").exists()


def test_records_module_absent_root_no_legacy_dir_created(tmp_path, monkeypatch):
    root = tmp_path / "fresh-empty"
    root.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    for operation in (
        lambda: records.read_record(A),
        lambda: codex_records.read_record(B),
        lambda: records.merge_lifecycle(A, state="idle"),
        lambda: codex_records.write_record({"workerId": B, "engine": "codex"}),
    ):
        with pytest.raises((GenerationUnavailable, RecordConflict)):
            operation()
    assert not list(root.iterdir())


def test_record_mutations_reject_unowned_or_reserved_before_creating_lock(tmp_path):
    from ccx.store.identity import EngineMismatch
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    generation = root / "identity" / "generations" / G1
    locks = generation / "locks" / "records"
    unknown_record = {
        "workerId": A, "engine": "claude", "recordRevision": 0,
        "name": "not mine",
    }
    with pytest.raises(UnknownWorker):
        ctx.mutate_record(A, patches={"name": "unowned"})
    with pytest.raises(UnknownWorker):
        ctx.replace_record_if_revision(
            A, unknown_record, expected_revision=0,
        )
    assert not locks.exists()
    # An exact create intent/claim RESERVED by this UUID is not an active
    # worker, and must not be edited or stamped as such.
    claim = ctx.reserve_creation(
        A, engine="claude", fingerprint="a" * 64,
        native_binding={"sessionId": B},
    )
    assert claim.activation_state == "unprepared"
    with pytest.raises(UnknownWorker):
        ctx.mutate_record(A, patches={"name": "reservation bypass"})
    with pytest.raises(UnknownWorker):
        ctx.replace_record_if_revision(
            A, unknown_record, expected_revision=0,
        )
    assert not locks.exists()
    # Physical current owner exists but engine qualifier is forged.
    owner_file = generation / "owners" / f"{A}.json"
    obj = json.loads(owner_file.read_text())
    obj["status"] = "worker"
    put(owner_file, obj)
    put(generation / "workers" / "claude" / A / "record.json", {
        "workerId": A, "engine": "claude",
        "recordRevision": 0, "native": {"sessionId": B},
    })
    with pytest.raises(EngineMismatch):
        ctx.mutate_record(f"codex:{A}", patches={"name": "wrong engine"})
    assert not locks.exists()


def _independent_codex_metadata_writer(root: str, worker_id: str, result) -> None:
    """Separate real process edits config/analytics while Worker is cached."""
    from pathlib import Path
    from ccx import constants
    from ccx.store.root import require_ccx_active
    constants.CCX_ROOT=Path(root)
    store=require_ccx_active(Path(root))
    outcome=store.mutate_record(
        worker_id,
        patches={
            "name":"CLI-process rename",
            "createArgs.model":"external-model",
            "stats.sampleCount":9,
        },
    )
    result.put(outcome["recordRevision"])


def test_original_codex_worker_semantic_save_preserves_real_process_fields(pair):
    """Original daemon Worker and separate CLI process share ONE fresh CAS."""
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    root, store, _, codex = pair
    daemon=CodexDaemon()
    daemon._uuid_store=store
    worker=Worker(daemon, codex_records.read_record(B))
    worker.record["lifecycle"]["state"]="idle"
    worker.record["stats"]["turns"] += 1
    mp=multiprocessing.get_context("fork")
    results=mp.Queue()
    child=mp.Process(
        target=_independent_codex_metadata_writer,
        args=(str(root),B,results),
    )
    child.start()
    child.join(timeout=8)
    if child.is_alive():
        child.terminate();child.join(timeout=3)
    assert child.exitcode==0
    from_child=results.get(timeout=3)
    assert from_child==1
    worker._save()
    current=store.record(B)
    assert current["lifecycle"]["state"]=="idle"
    assert current["stats"]["turns"]==1
    assert current["name"]=="CLI-process rename"
    assert current["createArgs"]["model"]=="external-model"
    assert current["stats"]["sampleCount"]==9
    assert current["recordRevision"]>=2
    assert current["native"]["threadId"] is None
    assert worker.record==current
    assert worker._disk_baseline==current
    assert not (root/"codex").exists() and not (root/"sessions").exists()


def test_original_codex_worker_same_field_conflict_never_rewrites_fresh_disk(pair):
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    root, store, _, _ = pair
    daemon=CodexDaemon()
    daemon._uuid_store=store
    worker=Worker(daemon, store.record(B))
    worker.record["lifecycle"]["state"]="idle"
    store.patch_lifecycle(B,changes={"state":"waiting"})
    revision=store.record(B)["recordRevision"]
    with pytest.raises(RecordConflict,match="field changed"):
        worker._save()
    result=store.record(B)
    assert result["lifecycle"]["state"]=="waiting"
    assert result["recordRevision"]==revision
    assert result["native"]["threadId"] is None


def test_codex_native_evidence_symlink_never_followed_or_chmodded(pair,tmp_path):
    from ccx.codex.evidence import EvidenceJournal, read_journal
    root, store, _, _ = pair
    original=tmp_path/"outside-record-protected"
    original.write_text("FOREIGN-DO-NOT-TOUCH")
    original.chmod(0o600)
    path=store.pointer.generation_root/"workers"/"codex"/B/"evidence.jsonl"
    assert not path.exists()
    path.symlink_to(original)
    with pytest.raises(OSError):
        read_journal(B)
    journal=EvidenceJournal(B)
    assert journal.record("turn/completed",{
        "threadId":"unowned-native-thread",
        "turn":{"id":"t","status":"completed"},
    }) is None
    assert journal.degraded is not None
    assert original.read_text()=="FOREIGN-DO-NOT-TOUCH"
    assert path.is_symlink()
    assert not (root/"codex").exists()


def test_host_a_default_short_validated_and_explicit_rename_wins_ai_title(pair, monkeypatch):
    from ccx.claude.host import session as host
    from ccx.store.identity import InvalidTarget
    root, store, claude, _ = pair
    assert records.default_claude_name(A) == "ccx-aaaaaaaa"
    with pytest.raises(InvalidTarget):
        records.default_claude_name("aaaaaaaa")
    monkeypatch.setattr(host, "read_ai_title", lambda *_: "Assistant generated title")
    assert host.adopt_ai_title(A, claude.record["transcript"]) is True
    assert store.record(A)["name"] == "Assistant generated title"
    before = store.record(A)["recordRevision"]
    assert host.adopt_ai_title(A, claude.record["transcript"]) is False
    assert store.record(A)["recordRevision"] == before

    store.rename_worker(A, engine="claude", name=None)  # reset to fallback
    real = type(store).mutate_record
    injected = []

    def concurrent_rename(self, target, *, patches=None, expect=None, **kwargs):
        if (patches or {}).get("name") == "Assistant generated title" and not injected:
            injected.append(True)
            real(self, target, patches={"name": "Human rename", "createArgs.model": "user-model"})
        return real(self, target, patches=patches, expect=expect, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(type(store), "mutate_record", concurrent_rename)
        assert host.adopt_ai_title(A, claude.record["transcript"]) is False
    latest = store.record(A)
    assert injected and latest["name"] == "Human rename"
    assert latest["createArgs"]["model"] == "user-model"
    assert latest["native"] == claude.record["native"]


def test_host_a_boot_consumes_only_cap_and_stale_waiting_with_racing_config(pair, monkeypatch):
    root, store, _, _ = pair
    original = store.record(A)
    store.mutate_record(A, patches={
        "activeCap": {"7d": 48}, "waitingFor": {"id": "stale-gate"},
    })
    real = type(store).mutate_record
    injected = []

    def competing_config(self, target, *, removals=(), **kwargs):
        if removals and not injected:
            injected.append(True)
            real(self, target, patches={
                "name": "Custom title during boot", "createArgs.model": "current-model",
                "activeCap": {"7d": 72},
            })
        return real(self, target, removals=removals, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(type(store), "mutate_record", competing_config)
        consumed, saved = records.take_host_boot_fields(A)
    assert injected and consumed == {"7d": 72}
    assert "activeCap" not in saved and "waitingFor" not in saved
    assert saved["name"] == "Custom title during boot"
    assert saved["createArgs"]["model"] == "current-model"
    assert saved["native"] == original["native"]
    before = store.record(A)["recordRevision"]
    cap, replay = records.take_host_boot_fields(A)
    assert cap is None and replay["recordRevision"] == before  # idempotent
    with pytest.raises(StorageIntegrityError):
        store.mutate_record(A, removals=("native",))


def test_host_a_recover_count_monotonic_and_leave_config_intact(pair):
    root, store, _, _ = pair
    store.mutate_record(A, patches={"name": "CLI preference", "createArgs.model": "current"})
    saved = records.restore_host_turn_count(A, 6)
    assert saved["stats"]["turns"] == 6
    assert records.restore_host_turn_count(A, 2)["stats"]["turns"] == 6
    assert records.restore_host_turn_count(A, 6)["recordRevision"] == saved["recordRevision"]
    assert store.record(A)["name"] == "CLI preference"
    assert store.record(A)["createArgs"]["model"] == "current"


def test_host_a_settlement_rebases_rename_config_external_dead_without_resurrection(
    pair, monkeypatch,
):
    root, store, _, _ = pair
    store.patch_lifecycle(A, changes={"state": "busy", "hostPid": 1122})
    original = type(store).mutate_record
    intervened = []
    turn = "ccx-turn:claude:v1:aaaaaaaa-1234-4000-8000-000000000099"
    bg_summary = {"runningCount": 1, "kinds": {"bash": 1, "monitor": 0,
                  "subagent": 0}, "monitors": [], "asOfMs": 1700000001200}

    def external_write_mid_settlement(self, target, *, patches=None, **kwargs):
        if (patches or {}).get("lastSettledTurn") is not None and not intervened:
            intervened.append(True)
            original(self, target, patches={
                "name": "External human rename", "createArgs.model": "model-new",
                "lifecycle.state": S.DEAD,
            })
        return original(self, target, patches=patches, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(type(store), "mutate_record", external_write_mid_settlement)
        saved = records.commit_host_turn_settlement(
            A, turn_ref=turn, prior_turns=0, outcome="completed",
            settled_at_ms=1700000001200, proposed_title="AI would overwrite human",
            background_task_summary=bg_summary,
        )
    assert intervened
    assert saved["stats"]["turns"] == 1
    assert saved["lastSettledTurn"] == {
        "turn": 1, "turnRef": turn, "outcome": "completed",
        "settledAtMs": 1700000001200, "context": None,
    }
    assert saved["lastSettledAt"] == saved["lastTurnAt"] == 1700000001200
    assert saved["backgroundTaskSummary"] == bg_summary
    assert saved["lifecycle"]["state"] == S.DEAD
    assert saved["name"] == "External human rename"
    assert saved["createArgs"]["model"] == "model-new"
    revision = saved["recordRevision"]
    repeated = records.commit_host_turn_settlement(
        A, turn_ref=turn, prior_turns=0, outcome="completed",
        settled_at_ms=1700000002900, proposed_title="another AI title",
        background_task_summary={**bg_summary, "runningCount": 99},
    )
    assert repeated["recordRevision"] == revision
    assert repeated["stats"]["turns"] == 1
    assert repeated["lastSettledTurn"] == saved["lastSettledTurn"]
    assert repeated["backgroundTaskSummary"] == bg_summary
    with pytest.raises(RecordConflict, match="contradicts"):
        records.commit_host_turn_settlement(
            A, turn_ref=turn, prior_turns=0, outcome="unknown",
            settled_at_ms=1700000002900,
        )
    assert records.host_live_transition(A, new_state="idle", host_pid=1122) is None
    final = records.host_terminal_transition(
        A, state="crashed", host_pid=1122, last_error="stale host crash",
        clear_child=True,
    )
    assert final["lifecycle"]["state"] == S.DEAD
    assert final["lifecycle"].get("lastError") != "stale host crash"
    assert store.record(A)["name"] == "External human rename"
    assert store.record(A)["lastSettledTurn"]["turnRef"] == turn


def test_host_a_nonterminal_host_state_requires_same_pid_and_respects_latest_dead(pair):
    root, store, _, _ = pair
    store.patch_lifecycle(A, changes={"hostPid": 7654, "state": "booting"})
    assert records.host_live_transition(A, new_state="idle", host_pid=7655) is None
    assert store.record(A)["lifecycle"]["state"] == "booting"
    ok = records.host_live_transition(A, new_state="idle", host_pid=7654)
    assert ok["lifecycle"]["state"] == "idle"
    assert type(ok["lifecycle"]["since"]) is int
    store.patch_lifecycle(A, changes={"state": S.DEAD})
    assert records.host_live_transition(A, new_state="busy", host_pid=7654) is None
    assert store.record(A)["lifecycle"]["state"] == S.DEAD


def test_host_a_transient_clear_noop_does_not_change_regular_patch_revision(pair):
    root, store, _, _ = pair
    before = store.record(A)
    same = store.mutate_record(A, patches={"name": before["name"]})
    assert same["recordRevision"] == before["recordRevision"] + 1
    absent = store.mutate_record(A, removals=("waitingFor", "activeCap"))
    assert absent["recordRevision"] == same["recordRevision"]
    assert absent["workerId"] == A and absent["native"] == before["native"]
