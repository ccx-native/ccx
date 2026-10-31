"""Current-v2 offline owner attestation; synthetic source evidence only."""
from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from ccx import WorkerClient
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.ops import forking
from ccx.claude import spawn
from ccx.codex.daemon.daemon import CodexDaemon
from ccx.ops.creation_steps import prepare_worker
from ccx.errors import SessionUnavailable
from ccx.store.root import CreationConflict, CreationIndeterminate, NativeActionDenied, StorageIntegrityError, require_ccx_active
from ccx.store.store import CreationReservation, converted_binding_digest
from test_generation_store import A, B, C, fixture_root, put, short_root as short_root


OLD = "5e13de4f"
TAG = "ccx-worker:5e13de4f-f957-42d7-a220-f7664c5dd65a"
OTHER = "ccx-worker:other-owner"
THREAD = "converted-native-thread"


@pytest.fixture
def offline(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    pointer_path = root / "identity" / "current.json"
    pointer = json.loads(pointer_path.read_text())
    manifest_path = root / "identity" / "generations" / pointer["generation"] / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["planSha256"] = "b" * 64
    put(manifest_path, manifest)
    pointer["manifestSha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    put(pointer_path, pointer)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    return require_ccx_active(root)


def _converted(store, cwd: Path, worker_id: str, engine: str, *,
               imported_from=None, forked_from=None, spawned_by=None):
    """Test-only staged triplet: no provider submission or original ACK."""
    initial = prepare_worker(store, engine=engine, cwd=str(cwd), worker_id=worker_id)
    record = dict(initial.record)
    if imported_from is not None:
        # An actual historical import's native session is the resumed session.
        imported_from = {**imported_from, "sessionId": record["native"]["sessionId"]}
    nb = ({"sessionId": record["native"]["sessionId"]} if engine == "claude"
          else {"threadId": THREAD})
    proof = (None if engine == "claude" else {
        "kind": "codex-session-meta", "threadId": THREAD,
        "threadSource": TAG, "headerPath": "sessions/first.jsonl",
        "headerSha256": "e" * 64,
    })
    cf = {
        "legacyShort": OLD, "sourcePath": f"records/{OLD}.json",
        "sourceRecordSha256": "a" * 64, "planSha256": "b" * 64,
        "originalClaimSha256": None, "importedFrom": imported_from,
        "nativeProof": proof,
    }
    digest = converted_binding_digest(worker_id, engine, nb, cf)
    owner = store._owner(worker_id)
    owner["provenance"] = "ccx-convert"
    owner["createIntent"] = {
        "fingerprintVersion": 2, "createSha256": digest,
        "nativeBinding": nb, "forkAnchor": None,
        "activationStateAtReservation": "converted", "convertedFrom": cf,
        **({"spawnedBy": spawned_by} if spawned_by is not None else {}),
    }
    claim = store._claim(worker_id)
    claim.update(createSha256=digest, nativeBinding=nb, forkAnchor=None,
                 activatedAtMs=None)
    claim["nativeActivation"] = {
        "state": "confirmed",
        "source": (f"claude-session:{nb['sessionId']}" if engine == "claude" else TAG),
        "submissionId": None, "nativeEvidenceSha256": digest, "nativeBinding": nb,
    }
    record.update(
        createSha256=digest, fingerprintVersion=2, forkAnchor=None,
        native={**nb, "nativeState": "session-bound" if engine == "claude" else "thread-bound",
                **({"threadSource": TAG} if engine == "codex" else {})},
        terminalOnly=False,
        spawnedBy=spawned_by,
    )
    record["lifecycle"] = {**record["lifecycle"], "state": "idle-killed"}
    if imported_from is not None:
        record.update(origin="ccx-import", importedFrom=imported_from)
    if forked_from is not None:
        record["forkedFrom"] = forked_from
    home = store.pointer.generation_root
    put(home / "owners" / f"{worker_id}.json", owner)
    put(home / "claims" / f"{worker_id}.json", claim)
    put(home / "workers" / engine / worker_id / "record.json", record)
    return record, cf


def test_converted_claude_import_and_fork_read_same_original_sid_without_recopy(
    offline, tmp_path, monkeypatch,
):
    imported = {"sessionId": "historical-session"}
    parent, _ = _converted(offline, tmp_path, A, "claude", imported_from=imported)
    child, _ = _converted(offline, tmp_path, B, "claude", forked_from={
        "engine": "claude", "sourceWorkerId": A,
        "sourceNativeSessionId": parent["native"]["sessionId"],
        "forkedAtMs": None,
    }, spawned_by={"kind": "ccx-worker", "engine": "claude", "workerId": A})
    assert offline.creation_binding(A, engine="claude",
                                    fingerprint=parent["createSha256"]) == {
                                        "sessionId": parent["native"]["sessionId"],
                                    }
    for wid in (A, B):
        assert offline.native_activation(wid)["submissionId"] is None
        assert offline._claim(wid)["activatedAtMs"] is None
        assert offline.authorize_native_action(wid, "resume").worker_id == wid
        assert not forking.claude_fork_pending(offline.record(wid))
    assert not forking.claude_fork_pending(child)
    assert forking.public_lineage(child)["worker_id"] == A
    assert WorkerClient().list(parent=A)["workers"][0]["worker_id"] == B
    # Confirmed imported/fork native source is the original SID, not the new UUID.
    offline.mutate_record(A, patches={"lifecycle.state": "resuming"})
    offline.mutate_record(B, patches={"lifecycle.state": "resuming"})
    for row in (parent, child):
        transcript = Path(row["transcript"])
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text('{"type":"user"}\n')
        assert spawn._verified_full_uuid_host_start(row["workerId"]) == row["workerId"]
    assert offline.import_native_owner(parent["native"]["sessionId"])["worker_id"] == A


def test_converted_claim_immutable_cannot_create_or_fake_submission(offline, tmp_path):
    record, _ = _converted(offline, tmp_path, A, "claude")
    digest = record["createSha256"]
    assert offline.authorize_native_action(A, "send").worker_id == A
    with pytest.raises(CreationConflict):
        offline.reserve_creation(A, engine="claude", fingerprint=digest,
                                 native_binding={"sessionId": record["native"]["sessionId"]})
    with pytest.raises(CreationIndeterminate):
        offline.prepare_native_activation(
            CreationReservation(A, "claude", digest, True, True, "confirmed"),
            native_source=f"claude-session:{record['native']['sessionId']}",
        )
    with pytest.raises(CreationConflict):
        prepare_worker(offline, engine="claude", cwd=str(tmp_path), worker_id=A)
    assert offline._claim(A)["nativeActivation"]["submissionId"] is None
    assert offline._claim(A)["activatedAtMs"] is None


def test_attestation_mutation_or_non_v2_refuses_before_native(offline, tmp_path):
    _converted(offline, tmp_path, A, "codex")
    assert offline.codex_source(A) == TAG
    claim_path = offline.pointer.generation_root / "claims" / f"{A}.json"
    original = json.loads(claim_path.read_text())
    changed = json.loads(json.dumps(original))
    changed["nativeActivation"]["source"] = OTHER
    put(claim_path, changed)
    with pytest.raises(StorageIntegrityError):
        offline.codex_source(A)
    put(claim_path, original)
    owner_path = offline.pointer.generation_root / "owners" / f"{A}.json"
    owner = json.loads(owner_path.read_text())
    owner["createIntent"]["convertedFrom"]["planSha256"] = "f" * 64
    put(owner_path, owner)
    with pytest.raises(StorageIntegrityError):
        offline.codex_source(A)
    record_path = offline.pointer.generation_root / "workers" / "codex" / A / "record.json"
    record = json.loads(record_path.read_text())
    record["fingerprintVersion"] = 1
    put(record_path, record)
    with pytest.raises(NativeActionDenied, match="v2"):
        offline.authorize_native_action(A, "send")


class _Native:
    def __init__(self, tag=TAG, resume_tag=None):
        self.tag, self.resume_tag = tag, resume_tag
        self.calls = []
        self.process = SimpleNamespace(poll=lambda: None, pid=12345)

    def request(self, method, params, timeout=30):
        self.calls.append(method)
        if method == "thread/read":
            return {"thread": {"id": THREAD, "threadSource": self.tag,
                               "status": {"type": "idle"}, "turns": []}}
        if method == "thread/resume":
            return {"thread": {"id": THREAD, "threadSource": self.resume_tag,
                               "status": {"type": "idle"}}}
        if method in {"thread/items/list", "thread/turns/list"}:
            return {"data": [], "nextCursor": None}
        raise AssertionError(f"unexpected fake native effect: {method}")


def test_converted_codex_original_tag_daemon_adopt_and_optional_resume_echo(
    offline, tmp_path, monkeypatch,
):
    _converted(offline, tmp_path, A, "codex")
    assert offline.codex_source(A) == TAG
    native = _Native()
    daemon = CodexDaemon()
    daemon._uuid_store = offline
    daemon.client = native
    daemon.ready.set()
    original = daemon.handle({"op": "thread-read", "workerId": A, "threadId": THREAD})
    assert original["ok"] and original["thread"]["threadSource"] == TAG
    loaded = daemon.handle({"op": "load", "short": A, "timeout": 3})
    assert loaded["ok"], loaded
    assert loaded["loaded"] is True and loaded["state"] == "idle"
    assert native.calls.count("thread/resume") == 1
    assert "thread/start" not in native.calls and "thread/fork" not in native.calls

    monkeypatch.setattr(codex_runtime, "observation_daemon", lambda *_a, **_kw: False)
    @contextmanager
    def cold(**_kw):
        yield native
    monkeypatch.setattr(codex_runtime, "_short_lived_app_server", cold)
    assert codex_runtime.read_thread(
        THREAD, cwd=str(tmp_path), worker_id=A, expected_source=offline.codex_source(A),
    )["threadSource"] == TAG
    assert codex_runtime.list_thread_items(
        THREAD, cwd=str(tmp_path), worker_id=A, expected_source=offline.codex_source(A),
    )["data"] == []
    assert "thread/start" not in native.calls and "thread/fork" not in native.calls


def test_converted_codex_foreign_resume_echo_refused_after_positive_read(
    offline, tmp_path,
):
    _converted(offline, tmp_path, A, "codex")
    native = _Native(tag=TAG, resume_tag=OTHER)
    daemon = CodexDaemon()
    daemon._uuid_store = offline
    daemon.client = native
    daemon.ready.set()
    refused = daemon.handle({"op": "load", "short": A, "timeout": 3})
    assert not refused["ok"] and "thread belongs to another worker" in refused["error"]
    assert offline.record(A)["lifecycle"]["state"] == "crashed"
    assert offline.record(A)["lifecycle"]["lastError"] == refused["error"]
    assert native.calls == ["thread/read", "thread/resume"]
    assert "thread/start" not in native.calls


def test_converted_codex_original_tag_one_send_via_disposable_fake_daemon(
    short_root, tmp_path, monkeypatch,
):
    """Physical fake app-server: resume, then one native turn; no new thread."""
    import test_codex_native_effects as native_fake
    from ccx.codex.client import CodexClient

    store = require_ccx_active(short_root)
    _converted(store, tmp_path, A, "codex")
    native_threads = tmp_path / "synthetic-history.json"
    native_threads.write_text(json.dumps({
        THREAD: {"id": THREAD, "threadSource": TAG, "model": "fake",
                 "status": {"type": "idle"}, "turns": []},
    }))
    original = native_fake._fake_codex_executable

    def resumable(*args, **kwargs):
        path = original(*args, **kwargs)
        script = path.read_text()
        marker = "        elif method=='thread/unsubscribe':\n"
        assert script.count(marker) == 1
        path.write_text(script.replace(marker, (
            "        elif method=='thread/resume':\n"
            "            tid=params.get('threadId')\n"
            "            if tid not in threads: raise ValueError('unknown original thread')\n"
            "            result={'thread':dict(threads[tid],status={'type':'idle'})}\n"
            + marker
        ), 1))
        return path

    monkeypatch.setattr(native_fake, "_fake_codex_executable", resumable)
    with native_fake._f2_fake_processes(
        short_root, tmp_path, monkeypatch, persisted_threads=native_threads,
    ) as (_cli, _restart, trace, _store, _rpc):
        result = CodexClient().send(
            A, "one converted original turn", delivery_id="converted-test-original",
            wait=False,
        )
        assert result.worker_id == A
        data = trace.read_text()
        assert data.count("RPC thread/resume") == 1
        assert data.count("RPC turn/start") == 1
        assert "RPC thread/start" not in data and "RPC thread/fork" not in data
        assert THREAD in data and TAG == store.codex_source(A)
        assert TAG in native_threads.read_text()


def test_foreign_original_codex_header_refused_before_resume_or_history(
    offline, tmp_path, monkeypatch,
):
    _converted(offline, tmp_path, A, "codex")
    daemon = CodexDaemon()
    daemon._uuid_store = offline
    native = _Native(tag=OTHER)
    daemon.client = native
    daemon.ready.set()
    refusal = daemon.handle({"op": "load", "short": A, "timeout": 3})
    assert not refusal["ok"] and "foreign" in refusal["error"]
    assert native.calls == ["thread/read"]
    monkeypatch.setattr(codex_runtime, "observation_daemon", lambda *_a, **_kw: False)
    @contextmanager
    def cold(**_kw):
        yield native
    monkeypatch.setattr(codex_runtime, "_short_lived_app_server", cold)
    with pytest.raises(SessionUnavailable, match="belongs to another worker"):
        codex_runtime.read_thread(
            THREAD, cwd=str(tmp_path), worker_id=A, expected_source=offline.codex_source(A),
        )
    assert "thread/resume" not in native.calls


def test_fresh_codex_tag_and_non_v2_guard_remain_strict(offline, tmp_path):
    prepared = prepare_worker(offline, engine="codex", cwd=str(tmp_path), worker_id=A)
    reservation = offline.reserve_creation(
        A, engine="codex", fingerprint=prepared.record["createSha256"],
    )
    with pytest.raises(CreationConflict, match="must name full"):
        offline.prepare_native_activation(reservation, native_source=TAG)
    record_path = offline.pointer.generation_root / "workers" / "codex" / A / "record.json"
    rec = json.loads(record_path.read_text())
    rec["fingerprintVersion"] = 1
    put(record_path, rec)
    with pytest.raises(NativeActionDenied, match="v2"):
        offline.authorize_native_action(A, "send")
