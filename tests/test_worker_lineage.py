"""Owned managed-caller lineage across original create, fork and inventory."""
from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx import WorkerClient
from ccx import constants
from ccx.ops import creation
from ccx.ops import forking
from ccx.store import records
from ccx.ops.creation_steps import activate_prepared_worker, prepare_worker
from ccx.errors import InvalidRequest, SessionNotFound
from ccx.store.root import StorageIntegrityError, require_ccx_active
from test_generation_store import A, B, C, fixture_root, put, short_root as short_root
from test_fork_source import sources as sources


D = "dddddddd-1234-4000-8000-000000000004"
E = "eeeeeeee-1234-4000-8000-000000000005"
F = "ffffffff-1234-4000-8000-000000000006"
G = "99999999-1234-4000-8000-000000000007"


def stamp(monkeypatch, engine: str | None = None, wid: str | None = None):
    for field in (constants.SELF_WORKER_ID_ENV, constants.SELF_ENGINE_ENV,
                  constants.SELF_SHORT_ENV, "CODEX_THREAD_ID", "CLAUDE_CODE_SESSION_ID"):
        monkeypatch.delenv(field, raising=False)
    if engine is not None:
        monkeypatch.setenv(constants.SELF_ENGINE_ENV, engine)
    if wid is not None:
        monkeypatch.setenv(constants.SELF_WORKER_ID_ENV, wid)


def edge(engine: str, wid: str) -> dict:
    return {"kind": "ccx-worker", "engine": engine, "workerId": wid}


@pytest.fixture
def fake_completion(monkeypatch):
    """No provider binary/host: original native claim gets one test receipt."""
    submissions = []

    def complete(store, original):
        if original.activation_state == "confirmed":
            return store.record(original.worker)

        def accept(_seconds):
            claim = store.native_activation(original.worker)
            submissions.append(original.worker)
            return {
                "accepted": True, "workerId": original.worker_id,
                "engine": original.engine, "source": original.native_source,
                "submissionId": claim["submissionId"],
                "nativeEvidenceSha256": "a" * 64,
                "nativeBinding": ({"sessionId": original.record["native"]["sessionId"]}
                                  if original.engine == "claude" else
                                  {"threadId": "fake-native-" + original.worker_id}),
            }

        return activate_prepared_worker(
            store, original, bounded_native_submit=accept,
        ).record

    monkeypatch.setattr(
        creation, "_complete_claude_create",
        lambda store, original, **_kw: complete(store, original),
    )
    monkeypatch.setattr(
        creation, "_complete_codex_create",
        lambda store, original, _info, **_kw: complete(store, original),
    )
    return submissions


@pytest.fixture
def managed(tmp_path, monkeypatch, fake_completion):
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    stamp(monkeypatch)
    store = require_ccx_active(root)
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    return store, fake_completion


def test_attested_cross_engine_create_owns_full_caller_not_same_prefix(
    managed, tmp_path, monkeypatch,
):
    store, submissions = managed
    client = WorkerClient()
    stamp(monkeypatch, "codex", B)
    created = client.create(engine="claude", cwd=str(tmp_path), worker_id=C)
    assert created["worker_id"] == C
    assert store.record(C)["spawnedBy"] == edge("codex", B)

    stamp(monkeypatch, "claude", A)
    created = client.create(engine="codex", cwd=str(tmp_path), worker_id=D)
    assert created["worker_id"] == D
    assert store.record(D)["spawnedBy"] == edge("claude", A)
    assert submissions == [f"claude:{C}", f"codex:{D}"]
    assert [r["worker_id"] for r in client.list(parent=B)["workers"]] == [C]
    assert [r["worker_id"] for r in client.list(parent=A)["workers"]] == [D]
    assert client.list(parent=f"codex:{B}")["workers"][0]["spawned_by"] == edge("codex", B)
    assert A[:8] == B[:8]  # The shared display prefix cannot define an edge.


@pytest.mark.parametrize("unsafe", [
    {"CLAUDE_CODE_SESSION_ID": "native-claude"},
    {"CODEX_THREAD_ID": "native-codex"},
    {"CCX_SELF_WORKER_ID": A},  # partial managed stamp
    {"CCX_SELF_ENGINE": "codex"},
    {"CCX_SELF_SHORT": A[:8]},
    {"CCX_SELF_WORKER_ID": A, "CCX_SELF_ENGINE": "codex"},
    {"CCX_SELF_WORKER_ID": C, "CCX_SELF_ENGINE": "claude"},
])
def test_native_external_and_bad_stamps_are_not_owned_parents(
    managed, tmp_path, monkeypatch, unsafe,
):
    store, submissions = managed
    stamp(monkeypatch)
    for key, value in unsafe.items():
        monkeypatch.setenv(key, value)
    made = WorkerClient().create(engine="codex", cwd=str(tmp_path), worker_id=D)
    assert made["worker_id"] == D
    assert store.record(D).get("spawnedBy") is None
    assert WorkerClient().list(parent=A)["workers"] == []
    assert submissions == [f"codex:{D}"]


def test_original_caller_survives_retry_and_no_native_replay(
    managed, tmp_path, monkeypatch,
):
    store, submissions = managed
    stamp(monkeypatch, "claude", A)
    client = WorkerClient()
    first = client.create(engine="codex", cwd=str(tmp_path), worker_id=C)
    initial = store.record(C)
    stamp(monkeypatch, "codex", B)
    again = client.create(engine="codex", cwd=str(tmp_path), worker_id=C)
    assert not again["created"]
    assert store.record(C) == initial
    assert first["worker"] == again["worker"]
    assert submissions == [f"codex:{C}"]
    assert [r["worker_id"] for r in client.list(parent=A)["workers"]] == [C]
    assert client.list(parent=B)["workers"] == []


@pytest.mark.parametrize("crash_at", ["after-owner", "after-claim", "after-record"])
def test_first_owner_claim_and_partial_record_recover_original_edge(
    managed, tmp_path, monkeypatch, crash_at,
):
    store, _ = managed
    stamp(monkeypatch, "claude", A)
    caller = records.spawned_by_from_env()

    def crash(stage):
        if stage == crash_at:
            raise RuntimeError("simulated power loss")

    with pytest.raises(RuntimeError, match="power loss"):
        prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=C,
                       spawned_by=caller, _failpoint=crash)
    owner_path = (store.pointer.generation_root / "owners" / f"{C}.json")
    assert json.loads(owner_path.read_text())["createIntent"]["spawnedBy"] == caller
    stamp(monkeypatch, "codex", B)
    replay = prepare_worker(store, engine="codex", cwd=str(tmp_path),
                            worker_id=C, spawned_by=records.spawned_by_from_env())
    assert replay.reconciled
    assert replay.record["spawnedBy"] == caller
    assert store.native_activation(C)["state"] == "unprepared"
    assert [r["worker_id"] for r in WorkerClient().list(parent=A)["workers"]] == [C]
    assert WorkerClient().list(parent=B)["workers"] == []


@pytest.mark.parametrize("engine,source,caller_engine,caller_id", [
    ("claude", A, "codex", B),
    ("codex", B, "claude", A),
])
def test_fork_source_is_not_caller_parent(
    sources, fake_completion, monkeypatch, engine, source, caller_engine, caller_id,
):
    store, _transcript, _reads = sources
    stamp(monkeypatch, caller_engine, caller_id)
    result = forking.fork(f"{engine}:{source}", worker_id=C)
    assert result["forked_from"]["worker_id"] == source
    assert store.record(C)["forkedFrom"]["sourceWorkerId"] == source
    assert store.record(C)["spawnedBy"] == edge(caller_engine, caller_id)
    assert [r["worker_id"] for r in WorkerClient().list(parent=caller_id)["workers"]] == [C]
    assert WorkerClient().list(parent=source)["workers"] == []


def test_exact_direct_children_filters_before_limit_and_preserves_stopped(
    managed, tmp_path, monkeypatch,
):
    store, submissions = managed
    other = tmp_path / "other"
    other.mkdir()
    client = WorkerClient()
    stamp(monkeypatch, "claude", A)
    client.create(engine="codex", cwd=str(tmp_path), worker_id=C)
    client.create(engine="claude", cwd=str(other), worker_id=D)
    store.mutate_record(C, patches={"lifecycle.state": "idle-killed"})
    store.mutate_record(D, patches={"lifecycle.state": "idle"})
    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    stamp(monkeypatch, "codex", B)
    client.create(engine="claude", cwd=str(tmp_path), worker_id=E)
    stamp(monkeypatch, "codex", C)  # Grandchild, never a direct child of A.
    client.create(engine="codex", cwd=str(tmp_path), worker_id=F)
    stamp(monkeypatch)
    client.create(engine="codex", cwd=str(tmp_path), worker_id=G)
    assert {r["worker_id"] for r in client.list(parent=A)["workers"]} == {C, D}
    assert {r["worker_id"] for r in client.list(parent=B)["workers"]} == {E}
    assert {r["worker_id"] for r in client.list(parent=C)["workers"]} == {F}
    assert [r["worker_id"] for r in client.list(parent=A, engine="codex")["workers"]] == [C]
    assert [r["worker_id"] for r in client.list(parent=A, cwd=str(other))["workers"]] == [D]
    assert [r["worker_id"] for r in client.list(parent=A, state="stopped")["workers"]] == [C]
    assert client.status(C)["native_state"] == "idle-killed"
    assert len(client.list(parent=A, limit=1)["workers"]) == 1
    assert client.list(parent=A, limit=0)["workers"] == []
    assert len(submissions) == 5
    for invalid in (A[:8], "claude:" + A[:8], "native-thread", "wrong:" + A):
        with pytest.raises(InvalidRequest):
            client.list(parent=invalid)
    with pytest.raises(SessionNotFound):
        client.list(parent="cccccccc-1234-4000-8000-000000000003")
    with pytest.raises(InvalidRequest):
        client.list(parent=f"codex:{A}")


def test_unclaimed_or_forged_record_parent_is_not_an_owned_edge(managed, tmp_path):
    store, _ = managed
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=C)
    path = store.pointer.generation_root / "workers" / "claude" / C / "record.json"
    fake = store.record(C)
    fake["spawnedBy"] = edge("claude", A)
    put(path, fake)
    with pytest.raises(StorageIntegrityError, match="unproven managed parent edge"):
        store.list_workers(parent=A)


def test_first_owner_edge_cannot_disappear_from_record_inventory(managed, tmp_path):
    store, _ = managed
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=C,
                   spawned_by=edge("claude", A))
    path = store.pointer.generation_root / "workers" / "claude" / C / "record.json"
    fake = store.record(C)
    del fake["spawnedBy"]
    put(path, fake)
    with pytest.raises(StorageIntegrityError, match="unproven managed parent edge"):
        store.list_workers(parent=A)
