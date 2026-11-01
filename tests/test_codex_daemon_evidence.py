"""Codex original evidence persistence, bounded retries and daemon shutdown."""
from __future__ import annotations
import ccx.codex.daemon.models as daemon_models
import ccx.codex.daemon.native as daemon_native
import ccx.store.root as store_root
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest
from test_generation_store import short_root, A, B
from ccx import constants
from ccx.store.root import require_ccx_active
from ccx.store.worker_state import normalize_native_state


import hashlib
from contextlib import contextmanager
from test_codex_native_effects import (
    _fake_codex_executable, _f2_fake_processes, _f2_new, _f2_marker, _f2_until_candidate,
)


def test_idle_kill_does_not_publish_terminal_state_while_still_routable():
    """The janitor claims teardown and drops routing before terminal save.

    A premature _state("idle-killed") would publish an apparently finished
    worker while the native thread is still addressable through codexd.
    """
    import threading
    from types import SimpleNamespace
    from ccx.codex.daemon.daemon import CodexDaemon

    daemon = CodexDaemon()
    thread = "test-original-codex-thread"
    writes = []
    worker = SimpleNamespace(
        short=A, lock=threading.RLock(), native_threads={thread}, teardown_claimed=False,
        attach_count=0, _bg_open={},
        record={"lifecycle": {"state": "idle", "since": 1}, "pendingApprovals": []},
        evidence=SimpleNamespace(observed=False, close=lambda: None),
        _journal_lifecycle_locked=lambda **kw: None,
    )
    daemon.workers[A] = worker
    daemon.by_thread[thread] = A
    worker._state = lambda _: pytest.fail("early terminal publication while routable")

    def save():
        assert worker.teardown_claimed and thread not in daemon.by_thread
        writes.append(worker.record["lifecycle"]["state"])
    worker._save = save
    daemon._idle_kill(worker)
    assert writes == ["idle-killed"]
    assert worker.record["lifecycle"]["since"] > 1
    assert A not in daemon.workers
    assert thread not in daemon.by_thread


def test_r1_real_daemon_reply_during_activation_lease_contention_recovers(
    short_root, tmp_path, monkeypatch,
):
    """Original callback's failed fsync is retained; same-ID status retries it.

    Real codexd, real Unix RPC, real physical fake app-server, and the actual
    exclusive stable generation lock reproduce the Opus R1 defect. No provider
    call takes place inside the callback or while the lease is held.
    """
    import fcntl
    from ccx.store.root import NativeActionDenied

    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=3.0,
    ) as (cli, _restart, trace, store, rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", ".2")
        assert first.returncode == 3, (first.stdout, first.stderr)
        assert store.native_activation(B)["state"] == "submitted-unknown"
        marker = _f2_marker(store, B)
        assert marker.is_file()
        assert trace.read_text().count("RPC thread/start") == 1
        with pytest.raises(NativeActionDenied):
            store.authorize_native_action(B, "send")

        # External writer/cutover-like lock is deliberately held during the
        # ONLY native response, longer than the callback's .5s lease window.
        with (short_root / "identity" / "activation.lock").open("rb") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                time.sleep(3.7)
                assert "nativeCandidate" not in json.loads(marker.read_text())
                assert trace.read_text().count("RPC thread/start") == 1
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        log = (tmp_path / "f2-daemon-0.log").read_text(errors="replace")
        assert "original Codex start reply pending disk save" in log, log[-2000:]
        assert "transient" in log
        assert "nativeCandidate" not in json.loads(marker.read_text())

        # This SAME full-UUID retry may only fsync the held original reply
        # and independently thread/read its precise candidate (no new start).
        retried = _f2_new(cli, tmp_path, B, "--start-timeout", "6")
        assert retried.returncode == 0, (retried.stdout, retried.stderr)
        result = json.loads(retried.stdout)
        assert result["native_ready"] is True and result["worker_id"] == B
        candidate = json.loads(marker.read_text())["nativeCandidate"]
        assert candidate["threadId"] == store.record(B)["native"]["threadId"]
        assert store.native_activation(B)["state"] == "confirmed"
        text = trace.read_text()
        assert text.count("RPC thread/start") == 1
        assert text.count("RPC thread/read") >= 1
        assert "RPC thread/list" not in text
        assert "RPC thread/resume" not in text
        assert "RPC turn/start" not in text


def _r1_pending_original_codex(short_root, tmp_path, monkeypatch):
    """Full UUID generation/claim/effect with a fake *pending* app-server reply.

    The daemon uses its original native admission and actual disk writers; the
    provider object exposes only begin_request/thread/read, never a live binary.
    """
    from types import SimpleNamespace
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.protocol import PendingRequest
    from ccx.ops.creation_steps import prepare_worker

    store = require_ccx_active(short_root)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    daemon = CodexDaemon()
    daemon._uuid_store = store
    daemon.ready.set()

    class FakeClient:
        def __init__(self):
            self.process = SimpleNamespace(pid=os.getpid())
            self.pending = []
            self.native_writes = []
            self.reads = []

        def begin_request(self, method, params, *, write_deadline=None):
            assert method == "thread/start"
            assert write_deadline is None or write_deadline > time.monotonic()
            self.native_writes.append((method, dict(params)))
            pending = PendingRequest(method)
            pending.request_id = len(self.native_writes)
            self.pending.append(pending)
            return pending

        def request(self, method, params, timeout=2):
            assert method == "thread/read", "reader callback must never invoke provider"
            self.reads.append((method, dict(params)))
            return {"thread": {
                "id": params["threadId"],
                "threadSource": self.sources.get(params["threadId"]),
            }}

    client = FakeClient()
    client.sources = {}
    daemon.client = client

    def admit(worker_id):
        prepared = prepare_worker(
            store, engine="codex", cwd=str(tmp_path), worker_id=worker_id,
        )
        reservation = store.reserve_creation(
            worker_id, engine="codex", fingerprint=prepared.record["createSha256"],
            native_binding={},
        )
        original = store.prepare_native_activation(
            reservation, native_source=prepared.native_source,
        )
        admitted = store.native_activation_dispatch(
            original, dispatch_budget_s=2., bounded_submit=lambda _budget:
            daemon._submit_original_codex_thread(
                worker_id, prepared.record, prepared.native_source,
                original.submission_id,
            ),
        )
        if admitted.get("localRefusalReason") is not None:
            # Historical R1 tests call this helper expecting a transport-unknown
            # exception when capacity is unavailable. L3 now stores a more
            # accurate terminal local-fate marker for that different UUID.
            from ccx.store.root import NativeEffectIndeterminate
            raise NativeEffectIndeterminate(
                "native transport result unknown; " + admitted["localRefusalReason"]
            )
        assert admitted["ok"] and admitted["queued"]
        return prepared

    def answer(index, worker_id, tid):
        client.sources[tid] = f"ccx-worker:{worker_id}"
        pending = client.pending[index]
        pending.put({"jsonrpc": "2.0", "id": pending.request_id,
                     "result": {"thread": {"id": tid}, "model": "gpt-fake"}})

    def status(worker_id):
        return daemon._observe_original_codex_thread({
            "workerId": worker_id,
            "submissionId": store.native_activation(worker_id)["submissionId"],
        })

    return store, daemon, client, admit, answer, status


def test_r1_once_transient_enospc_held_reply_recovered_by_status(
    short_root, tmp_path, monkeypatch,
):
    import errno
    import ccx.store.store as generation
    from ccx.ops.creation_steps import reconcile_submitted_native
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    original = admit(B)
    real_write = store_root._write_json
    injected = []

    def one_disk_full(fd, name, value):
        if "nativeCandidate" in value and len(injected) < 2:
            injected.append(name)
            raise OSError(errno.ENOSPC, "injected disk temporarily full")
        return real_write(fd, name, value)

    monkeypatch.setattr(store_root, "_write_json", one_disk_full)
    answer(0, B, "original-thread-from-only-reply")
    assert injected and len(client.native_writes) == 1
    marker = json.loads(_f2_marker(store, B).read_text())
    assert "nativeCandidate" not in marker
    held = list(daemon._native_start_held.values())
    assert len(held) == 1
    assert held[0]["candidate"]["threadId"] == "original-thread-from-only-reply"
    assert "Errno 28" in held[0]["error"]
    assert not held[0]["unsafe"]
    assert client.reads == []  # no provider RPC in the reader callback
    still_blocked = status(B)
    assert still_blocked == {
        "ok": True, "status": "indeterminate", "workerId": B,
        "candidatePersistence": "transient", "error": still_blocked["error"],
    }
    assert "Errno 28" in still_blocked["error"]
    assert client.reads == []
    assert "nativeCandidate" not in json.loads(_f2_marker(store, B).read_text())
    recovered = status(B)
    assert recovered["accepted"] is True and len(client.reads) == 1
    confirmed = reconcile_submitted_native(store, original, original_receipt=recovered)
    assert confirmed.activation_state == "confirmed"
    assert store.record(B)["native"]["threadId"] == "original-thread-from-only-reply"
    assert daemon._native_start_held == {}
    assert len(client.native_writes) == 1
    assert json.loads(_f2_marker(store, B).read_text())["nativeCandidate"]["threadId"] == (
        "original-thread-from-only-reply"
    )


def test_r1_callback_and_status_race_fsync_one_immutable_candidate(
    short_root, tmp_path, monkeypatch,
):
    import threading
    import ccx.store.store as generation

    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    real_write = store_root._write_json
    saving, release = threading.Event(), threading.Event()
    writes = []

    def hold_first_candidate_write(fd, name, value):
        if "nativeCandidate" in value:
            writes.append(value["nativeCandidate"]["threadId"])
            if len(writes) == 1:
                saving.set()
                assert release.wait(3), "simulated storage stall was not released"
        return real_write(fd, name, value)

    monkeypatch.setattr(store_root, "_write_json", hold_first_candidate_write)
    callback = threading.Thread(
        target=lambda: answer(0, B, "original-race-candidate"),
        name="fake-appserver-reader",
    )
    callback.start()
    assert saving.wait(3)
    entered_status_save = threading.Event()
    real_save = daemon._save_held_codex_candidate
    def status_save(key):
        if threading.current_thread().name == "status-handler":
            entered_status_save.set()
        return real_save(key)
    monkeypatch.setattr(daemon, "_save_held_codex_candidate", status_save)
    results = []
    observer = threading.Thread(target=lambda: results.append(status(B)), name="status-handler")
    observer.start()
    assert entered_status_save.wait(3), "status did not contend for original marker"
    release.set()
    callback.join(5)
    observer.join(5)
    assert not callback.is_alive() and not observer.is_alive()
    assert results and results[0].get("accepted") is True, results
    assert len(client.native_writes) == 1
    assert len(client.reads) == 1
    assert writes and set(writes) == {"original-race-candidate"}
    assert json.loads(_f2_marker(store, B).read_text())["nativeCandidate"]["threadId"] == (
        "original-race-candidate"
    )
    assert daemon._native_start_held == {}


@pytest.mark.parametrize("fault", ["tampered", "stale"])
def test_r1_corrupt_original_marker_or_stale_generation_never_rewrites(
    short_root, tmp_path, monkeypatch, fault,
):
    import errno
    import ccx.store.store as generation
    from ccx.store.root import NativeEffectIndeterminate, StaleGeneration

    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    real_write = store_root._write_json
    failed = []

    def fail_candidate_once(fd, name, value):
        if "nativeCandidate" in value and not failed:
            failed.append(True)
            raise OSError(errno.EIO, "simulated transient disk EIO")
        return real_write(fd, name, value)

    monkeypatch.setattr(store_root, "_write_json", fail_candidate_once)
    answer(0, B, "original-positive-candidate")
    assert failed and len(daemon._native_start_held) == 1
    marker_path = _f2_marker(store, B)
    if fault == "tampered":
        modified = json.loads(marker_path.read_text())
        modified["createSha256"] = "0" * 64
        marker_path.write_text(json.dumps(modified, sort_keys=True), encoding="utf-8")
        original_disk = marker_path.read_bytes()
        with pytest.raises(NativeEffectIndeterminate, match="unsafe"):
            status(B)
        assert marker_path.read_bytes() == original_disk
    else:
        original_revalidate = store.revalidate
        def stale():
            raise StaleGeneration("pinned ACTIVE generation changed")
        monkeypatch.setattr(store, "revalidate", stale)
        result = daemon._save_held_codex_candidate((store.generation, B,
            next(iter(daemon._native_start_held))[2]))
        assert result["status"] == "unsafe" and "StaleGeneration" in result["error"]
        monkeypatch.setattr(store, "revalidate", original_revalidate)
        with pytest.raises(NativeEffectIndeterminate, match="unsafe"):
            status(B)
        assert "nativeCandidate" not in json.loads(marker_path.read_text())
    assert daemon._native_start_held == {}
    assert daemon._native_start_counts()["unsafe"] == 1
    assert daemon._native_start_counts()["held"] == 0
    assert len(client.native_writes) == 1 and client.reads == []
    with pytest.raises(NativeEffectIndeterminate, match="unsafe"):
        status(B)


def test_r1_retention_capacity_refuses_new_dispatch_and_never_evicts(
    short_root, tmp_path, monkeypatch,
):
    import ccx.codex.daemon.daemon as codexd_module
    from test_generation_store import C
    from ccx.store.root import NativeEffectIndeterminate

    monkeypatch.setattr(daemon_native, "_CODEX_NATIVE_START_HELD_LIMIT", 1)
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    assert len(daemon._native_start_held) == 1
    assert len(client.native_writes) == 1
    # The original B provider response has not arrived, so memory cannot
    # silently evict its slot just to permit another worker's new start.
    with pytest.raises(NativeEffectIndeterminate, match="native transport result unknown"):
        admit(C)
    assert len(daemon._native_start_held) == 1
    assert len(client.native_writes) == 1
    assert store.native_activation(C)["state"] == "submitted-unknown"
    assert "nativeCandidate" not in json.loads(_f2_marker(store, B).read_text())
    answer(0, B, "retained-first-worker-thread")
    assert daemon._native_start_held == {}
    assert status(B)["accepted"] is True
    # Only a distinct, deliberately requested full UUID may use newly freed
    # capacity. The previously fenced C is NEVER dispatched again.
    third = "cccccccc-5678-4000-8000-000000000004"
    admit(third)
    assert len(client.native_writes) == 2
    assert len(daemon._native_start_held) == 1
    answer(1, third, "new-distinct-worker-thread")
    assert status(third)["accepted"] is True
    assert len(client.native_writes) == 2
    assert daemon._native_start_held == {}


def test_r1_positive_unsaved_candidate_counts_against_memory_capacity(
    short_root, tmp_path, monkeypatch,
):
    import errno
    import ccx.codex.daemon.daemon as codexd_module
    import ccx.store.store as generation
    from test_generation_store import C
    from ccx.store.root import NativeEffectIndeterminate

    monkeypatch.setattr(daemon_native, "_CODEX_NATIVE_START_HELD_LIMIT", 1)
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    real_write = store_root._write_json
    blocked = [True]
    def disk_full(fd, name, value):
        if "nativeCandidate" in value and blocked[0]:
            raise OSError(errno.ENOSPC, "positive reply held while out of space")
        return real_write(fd, name, value)
    monkeypatch.setattr(store_root, "_write_json", disk_full)
    answer(0, B, "positive-but-unsaved")
    held = next(iter(daemon._native_start_held.values()))
    assert held["candidate"]["threadId"] == "positive-but-unsaved"
    assert not held["unsafe"]
    assert "nativeCandidate" not in json.loads(_f2_marker(store, B).read_text())
    with pytest.raises(NativeEffectIndeterminate, match="native transport result unknown"):
        admit(C)
    assert len(client.native_writes) == 1
    assert len(daemon._native_start_held) == 1
    assert status(B)["candidatePersistence"] == "transient"
    assert len(client.reads) == 0
    blocked[0] = False
    assert status(B)["accepted"] is True
    assert json.loads(_f2_marker(store, B).read_text())["nativeCandidate"]["threadId"] == (
        "positive-but-unsaved"
    )
    assert not daemon._native_start_held
    # Capacity is available for a separately chosen full UUID only.
    another = "eeeeeeee-5678-4000-8000-000000000005"
    admit(another)
    assert len(client.native_writes) == 2
    answer(1, another, "second-unique-original")
    assert status(another)["accepted"] is True
    assert daemon._native_start_held == {}
    assert len(client.native_writes) == 2


def test_r1_oversized_positive_reply_remains_visible_unsafe_not_evicted(
    short_root, tmp_path, monkeypatch,
):
    import ccx.codex.daemon.daemon as codexd_module
    from ccx.store.root import NativeEffectIndeterminate

    monkeypatch.setattr(daemon_native, "_CODEX_NATIVE_START_HELD_LIMIT", 1)
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    too_long_id = "native-id-" + "x" * (daemon_native._CODEX_NATIVE_START_ID_MAX_BYTES + 1)
    answer(0, B, too_long_id)
    assert not daemon._native_start_held
    assert daemon._native_start_counts()["unsafe"] == 1
    assert "exceeds retained bound" in next(iter(daemon._native_start_unsafe.values()))
    with pytest.raises(NativeEffectIndeterminate, match="unsafe"):
        status(B)
    assert len(client.reads) == 0
    assert "nativeCandidate" not in json.loads(_f2_marker(store, B).read_text())
    from test_generation_store import C
    admit(C)  # unsafe diagnostic cannot permanently exhaust positive capacity
    assert len(client.native_writes) == 2
    assert daemon._native_start_counts()["pending"] == 1
    answer(1, C, "healthy-next-original-thread")
    assert status(C)["accepted"] is True
    assert not daemon._native_start_held
    assert daemon._native_start_counts()["unsafe"] == 1


def test_r1_l1_inflight_original_start_survives_short_idle_linger(
    short_root, tmp_path, monkeypatch,
):
    """A still-unanswered real native thread/start pins an otherwise empty daemon.

    A short idle window must not terminate the original PendingRequest and
    turn its eventual thread ID into unprovable 'unknown'. Nothing observes or
    resubmits the provider request while the app-server takes four seconds.
    """
    history = tmp_path / "r1-l1-provider-threads.json"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, linger_seconds=2,
        start_delay=4.0, persisted_threads=history,
    ) as (cli, _restart, trace, store, rpc):
        started = time.monotonic()
        first = _f2_new(cli, tmp_path, B, "--start-timeout", ".1")
        assert first.returncode == 3, (first.stdout, first.stderr)
        initial = rpc({"op": "daemon-status"}, timeout=.6)
        assert initial["state"] == "ready" and not initial["workers"]
        assert initial["nativeStartEvidence"]["pending"] == 1
        assert initial["nativeStartEvidence"]["held"] == 0
        assert initial["emptyForSeconds"] is None
        # Past the entire configured idle window and still before the native
        # reply: the app-server, pending entry and original daemon survive.
        while time.monotonic() - started < 2.8:
            time.sleep(.04)
        still = rpc({"op": "daemon-status"}, timeout=.6)
        assert still["pid"] == initial["pid"]
        assert still["state"] == "ready"
        assert still["nativeStartEvidence"]["pending"] == 1
        assert still["emptyForSeconds"] is None
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()
        # The daemon saves the eventual candidate without any status/native
        # activation RPC from this test. The caller gave up long ago.
        candidate = _f2_until_candidate(store, B, seconds=6)
        assert candidate and candidate.get("nativeCandidate"), candidate
        assert candidate["nativeCandidate"]["threadId"]
        assert store.record(B)["native"]["threadId"] is None
        assert store.native_activation(B)["state"] == "submitted-unknown"
        assert trace.read_text().count("RPC thread/start") == 1


def test_r1_l1_auto_retry_held_reply_after_lock_release_without_caller_retry(
    short_root, tmp_path, monkeypatch,
):
    """Opus L1: callback loses a lease, janitor restores exact evidence itself."""
    import fcntl
    history = tmp_path / "r1-l1-transient-provider-threads.json"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, linger_seconds=2,
        start_delay=3.0, persisted_threads=history,
    ) as (cli, _restart, trace, store, rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", ".1")
        assert first.returncode == 3, (first.stdout, first.stderr)
        marker = _f2_marker(store, B)
        assert marker.exists()
        pid = rpc({"op": "daemon-status"}, timeout=.5)["pid"]
        assert trace.read_text().count("RPC thread/start") == 1
        with (short_root / "identity" / "activation.lock").open("rb") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                # Native reply + callback lease timeout + at least one janitor
                # retry all occur under maintenance/cutover-like contention.
                time.sleep(4.7)
                os.kill(pid, 0)  # check ONLY this disposable daemon process
                assert "nativeCandidate" not in json.loads(marker.read_text())
                assert trace.read_text().count("RPC thread/start") == 1
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        log = (tmp_path / "f2-daemon-0.log").read_text(errors="replace")
        assert "original Codex start reply pending disk save" in log, log[-2000:]
        assert "transient" in log
        assert "nativeCandidate" not in json.loads(marker.read_text())
        # NO call to native-activation-status and NO CLI retry. The existing
        # janitor only re-fsyncs the original candidate after its backoff.
        recovered = _f2_until_candidate(store, B, seconds=9)
        assert recovered and recovered.get("nativeCandidate"), recovered
        assert recovered["nativeCandidate"]["threadId"]
        assert store.native_activation(B)["state"] == "submitted-unknown"
        assert store.record(B)["native"]["threadId"] is None
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()
        assert "RPC thread/resume" not in trace.read_text()
        assert "RPC turn/start" not in trace.read_text()


def test_r1_l1_explicit_daemon_stop_not_blocked_by_forever_pending_native_start(
    short_root, tmp_path, monkeypatch,
):
    """One original native start persists, pins daemon and stops without replay.

    Under CPU starvation a .1s CREATE budget can expire before the original
    daemon even admits the request, positively sealing `local-refused`. That
    cannot test a FOREVER-PENDING submitted native effect. Instead gate the
    assertion on a positive ORIGINAL fake app-server `thread/start` line,
    as the existing F2 physical-daemon proof does. Give this fixture an idle
    grace longer than the CLI's 9s start budget: a 1s grace can commit daemon
    shutdown before admission, legitimately sealing local-refused. The separate
    short-linger test checks that pending starts prevent idle shutdown.
    """
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, linger_seconds=30,
        no_start_reply=True, persisted_threads=tmp_path / "r1-l1-no-reply.json",
    ) as (_cli, _restart, trace, store, rpc):
        # A normal CLI is a disposable owned subprocess. The observation
        # budget is long enough to admit a native thread under -n16 contention;
        # native fake NEVER answers that first thread/start. We positively
        # observe the original physical write before testing pending-stop.
        repo = Path(__file__).resolve().parent.parent
        env = {**os.environ, "CCX_ROOT": str(short_root),
               "CCX_CODEX_BIN": str(tmp_path / "fake-codex"),
               "PYTHONPATH": str(repo), "CCX_CODEXD_LINGER": "30"}
        for key in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
            env.pop(key, None)
        child = subprocess.Popen(
            [sys.executable, "-B", "-m", "ccx", "new", "--engine", "codex",
             "--cwd", str(tmp_path), "--worker-id", B,
             "--start-timeout", "9", "--json"],
            env=env, cwd=str(repo), stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            until = time.monotonic() + 7
            while time.monotonic() < until:
                if trace.exists() and "RPC thread/start" in trace.read_text():
                    break
                if child.poll() is not None:
                    break
                time.sleep(.025)  # existing F2 native-start proof cadence
            assert trace.exists() and trace.read_text().count("RPC thread/start") == 1, (
                "original native thread/start was never positively admitted",
                child.poll(), store.native_activation(B).get("state"),
            )
            assert store.native_activation(B)["state"] == "submitted-unknown"
            # Leave the native effect pending before exercising explicit shutdown.
            time.sleep(2.4)
            # Native pending proof comes from the ORIGINAL daemon-stop refusal,
            # not from a schedulable 0.6s status observation.
            refused = rpc({"op": "daemon-stop"}, timeout=1.5)
            assert refused.get("transport") == "served" and refused["ok"] is False, refused
            assert "1 pending" in refused["error"]
            assert refused["nativeStartEvidence"]["pending"] == 1
            stopped = rpc({"op": "daemon-stop", "force": True}, timeout=1.5)
            assert stopped.get("transport") == "served" and stopped["ok"] and stopped["stopped"], stopped
            assert stopped["nativeStartEvidence"]["pending"] == 1
            assert trace.read_text().count("RPC thread/start") == 1
            assert store.native_activation(B)["state"] == "submitted-unknown"
            assert store.record(B)["native"]["threadId"] is None
            assert "RPC thread/list" not in trace.read_text()
            result, stderr = child.communicate(timeout=13)
            assert child.returncode == 3, (child.returncode, result, stderr)
        finally:
            if child.poll() is None:
                child.terminate()
                try:child.wait(timeout=4)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=4)


def test_r1_l2_janitor_retries_only_due_transient_evidence_with_backoff(
    short_root, tmp_path, monkeypatch,
):
    """Maintenance fsync of the original response is bounded and provider-free."""
    import errno
    import ccx.store.store as generation
    from ccx.ops.creation_steps import reconcile_submitted_native

    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    prepared = admit(B)
    real_write = store_root._write_json
    attempts = []

    def transient_twice(fd, name, value):
        if "nativeCandidate" in value:
            attempts.append(name)
            if len(attempts) <= 2:
                raise OSError(errno.EIO, "injected transient storage fault")
        return real_write(fd, name, value)

    monkeypatch.setattr(store_root, "_write_json", transient_twice)
    answer(0, B, "janitor-held-original-native-thread")
    assert len(attempts) == 1 and client.reads == []
    first = daemon._native_start_counts()
    assert first["pending"] == 0 and first["held"] == 1
    assert first["unsafe"] == 0
    with daemon._native_start_hold_lock:
        held = next(iter(daemon._native_start_held.values()))
        held["retryAfter"] = time.monotonic() - 1
    daemon._janitor_native_start_pass()  # another bounded EIO
    assert len(attempts) == 2
    with daemon._native_start_hold_lock:
        held = next(iter(daemon._native_start_held.values()))
        assert held["retryAfter"] > time.monotonic()
    daemon._janitor_native_start_pass()  # backoff; no write at all
    assert len(attempts) == 2
    with daemon._native_start_hold_lock:
        held["retryAfter"] = time.monotonic() - 1
    daemon._janitor_native_start_pass()  # same original candidate; success
    assert len(attempts) == 3
    assert daemon._native_start_counts()["held"] == 0
    assert client.reads == []  # janitor never calls thread/read or native starts
    assert len(client.native_writes) == 1
    assert json.loads(_f2_marker(store, B).read_text())["nativeCandidate"]["threadId"] == (
        "janitor-held-original-native-thread"
    )
    observed = status(B)
    assert observed["accepted"] is True
    bound = reconcile_submitted_native(store, prepared, original_receipt=observed)
    assert bound.activation_state == "confirmed"
    assert len(client.native_writes) == 1


def test_r1_l2_bounded_unsafe_diagnostics_preserve_capacity_without_replay(
    short_root, tmp_path, monkeypatch,
):
    import ccx.codex.daemon.daemon as codexd_module
    from test_generation_store import C
    from ccx.store.root import NativeEffectIndeterminate

    monkeypatch.setattr(daemon_native, "_CODEX_NATIVE_START_HELD_LIMIT", 1)
    monkeypatch.setattr(daemon_native, "_CODEX_NATIVE_START_UNSAFE_LIMIT", 2)
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    failed_ids = (B, C, "cccccccc-5678-4000-8000-000000000004")
    for i, wid in enumerate(failed_ids):
        admit(wid)
        answer(i, wid, "native-id-" + "x" * (
            daemon_native._CODEX_NATIVE_START_ID_MAX_BYTES + 1))
        assert daemon._native_start_held == {}
        assert store.native_activation(wid)["state"] == "submitted-unknown"
        assert "nativeCandidate" not in json.loads(_f2_marker(store, wid).read_text())
        assert len(client.native_writes) == i + 1
    counts = daemon.handle({"op": "daemon-status"})["nativeStartEvidence"]
    assert counts == {
        "pending": 0, "held": 0, "unsafe": 2,
        "unsafeTotal": 3, "capacity": 1,
    }
    # The first diagnostic rolls off the bounded history, not the original
    # claim/effect marker. The source-only thread is never adopted or replayed.
    assert len(daemon._native_start_unsafe) == 2
    assert store.native_activation(B)["state"] == "submitted-unknown"
    assert status(B)["status"] == "indeterminate"
    with pytest.raises(NativeEffectIndeterminate, match="unsafe"):
        status(failed_ids[-1])
    assert client.reads == []
    healthy = "eeeeeeee-5678-4000-8000-000000000005"
    admit(healthy)
    answer(3, healthy, "healthy-new-full-uuid")
    assert status(healthy)["accepted"] is True
    assert len(client.native_writes) == 4
    assert daemon._native_start_held == {}
    assert daemon.handle({"op": "daemon-status"})["nativeStartEvidence"]["unsafeTotal"] == 3


def test_r1_l2_unsafe_diagnostics_do_not_pin_idle_and_explicit_stop_skips_retry(
    short_root, tmp_path, monkeypatch,
):
    import errno
    import ccx.codex.daemon.daemon as codexd_module
    import ccx.store.store as generation

    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    # Reset a nearly-expired idle clock at the exact original admission.
    daemon.empty_since = time.monotonic() - 100
    admit(B)
    assert daemon.empty_since is None
    assert daemon._janitor_daemon_pass() is False
    assert daemon._native_start_counts()["pending"] == 1
    real_write = store_root._write_json
    failed = []
    def always_enospc(fd, name, value):
        if "nativeCandidate" in value:
            failed.append(name)
            raise OSError(errno.ENOSPC, "permanent injected disk full")
        return real_write(fd, name, value)
    monkeypatch.setattr(store_root, "_write_json", always_enospc)
    answer(0, B, "kept-for-retry")
    assert len(failed) == 1
    assert daemon._native_start_counts()["held"] == 1
    assert daemon._janitor_daemon_pass() is False
    # P1: a normal stop refuses while safe evidence is held unsaved. A forced
    # operator stop still has priority; no loop may defer it indefinitely.
    refused = daemon.handle({"op": "daemon-stop"})
    assert refused["ok"] is False and "1 held unsaved" in refused["error"]
    assert not daemon.stop.is_set() and not daemon.stopping
    stopped = daemon.handle({"op": "daemon-stop", "force": True})
    assert stopped["ok"] and stopped["stopped"] and daemon.stop.is_set()
    held = next(iter(daemon._native_start_held.values()))
    held["retryAfter"] = time.monotonic() - 1
    daemon._janitor_native_start_pass()
    assert len(failed) == 1
    assert len(client.native_writes) == 1
    assert daemon._native_start_counts()["held"] == 1

    # Separately, a solely *unsafe* diagnostic no longer makes idle janitor
    # think there is a safe in-flight or recoverable positive reply.
    store2, daemon2, client2, admit2, answer2, status2 = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    # This helper opens the same disposable root, but B is already reserved;
    # choose a genuinely distinct globally owned full UUID.
    next_uuid = "cccccccc-5678-4000-8000-000000000004"
    admit2(next_uuid)
    monkeypatch.setattr(store_root, "_write_json", real_write)
    answer2(0, next_uuid, "native-id-" + "x" * (
        daemon_native._CODEX_NATIVE_START_ID_MAX_BYTES + 1))
    assert daemon2._native_start_held == {}
    assert daemon2._native_start_counts()["unsafe"] == 1
    monkeypatch.setattr(daemon_models, "linger_seconds", lambda: 0.)
    assert daemon2._janitor_daemon_pass() is True
    assert daemon2.stop.is_set()
    assert len(client2.native_writes) == 1


def test_r1_l1_blocked_maintenance_fsync_cannot_block_explicit_daemon_stop(
    short_root, tmp_path, monkeypatch,
):
    """Even a stalled maintenance writer must not delay explicit shutdown."""
    import errno
    import threading
    import ccx.store.store as generation
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    real_write = store_root._write_json
    first_failed = []
    in_write, release = threading.Event(), threading.Event()

    def stalled_fsync(fd, name, value):
        if "nativeCandidate" in value:
            if not first_failed:
                first_failed.append(True)
                raise OSError(errno.EIO, "one transient callback save failure")
            in_write.set()
            assert release.wait(4), "test must release blocked persistence"
        return real_write(fd, name, value)

    monkeypatch.setattr(store_root, "_write_json", stalled_fsync)
    answer(0, B, "native-with-stalled-maintenance-writer")
    assert first_failed and len(daemon._native_start_held) == 1
    with daemon._native_start_hold_lock:
        held = next(iter(daemon._native_start_held.values()))
        held["retryAfter"] = time.monotonic() - 1
    maintenance = threading.Thread(
        target=daemon._janitor_native_start_pass,
        name="ccx-test-maintenance", daemon=True,
    )
    maintenance.start()
    assert in_write.wait(3), "janitor retry did not enter blocked write"
    try:
        started = time.monotonic()
        refused = daemon.handle({"op": "daemon-stop"})
        assert refused["ok"] is False
        assert refused["nativeStartEvidence"]["held"] == 1
        stopped = daemon.handle({"op": "daemon-stop", "force": True})
        assert stopped["ok"] and stopped["stopped"]
        assert daemon.stop.is_set()
        assert time.monotonic() - started < .5
        assert len(client.native_writes) == 1
        assert client.reads == []
    finally:
        release.set()
        maintenance.join(timeout=4)
    assert not maintenance.is_alive()
    # A write already in flight may still complete. It never opens a second
    # provider operation or delays the operator's stop decision.
    assert len(client.native_writes) == 1
    assert len(client.reads) == 0


def test_p1_normal_stop_refuses_pending_then_held_until_saved(
    short_root, tmp_path, monkeypatch,
):
    """Unsettled original starts pin a NORMAL stop with exact counts."""
    import errno
    import ccx.store.store as generation
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    pending = daemon.handle({"op": "daemon-stop"})
    assert pending["ok"] is False
    assert "1 pending, 0 held unsaved" in pending["error"]
    assert pending["nativeStartEvidence"]["pending"] == 1
    assert not daemon.stopping and not daemon.stop.is_set()
    real_write = store_root._write_json
    failures = []

    def once_enospc(fd, name, value):
        if "nativeCandidate" in value and not failures:
            failures.append(name)
            raise OSError(errno.ENOSPC, "injected transient disk full")
        return real_write(fd, name, value)

    monkeypatch.setattr(store_root, "_write_json", once_enospc)
    answer(0, B, "p1-held-original-thread")
    held = daemon.handle({"op": "daemon-stop"})
    assert held["ok"] is False and "0 pending, 1 held unsaved" in held["error"]
    assert held["nativeStartEvidence"]["held"] == 1
    assert not daemon.stop.is_set()
    # Once the original reply is durable nothing pins the daemon any more.
    assert status(B)["accepted"] is True
    assert daemon._native_start_counts()["held"] == 0
    stopped = daemon.handle({"op": "daemon-stop"})
    assert stopped["ok"] and stopped["stopped"] and daemon.stop.is_set()
    assert len(client.native_writes) == 1


def test_p1_force_stop_prompt_with_pending_and_held(
    short_root, tmp_path, monkeypatch,
):
    from test_generation_store import C
    import errno
    import ccx.store.store as generation
    store, daemon, client, admit, answer, status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    admit(B)
    admit(C)
    real_write = store_root._write_json

    def always_eio(fd, name, value):
        if "nativeCandidate" in value:
            raise OSError(errno.EIO, "injected transient I/O error")
        return real_write(fd, name, value)

    monkeypatch.setattr(store_root, "_write_json", always_eio)
    answer(1, C, "p1-held-c")
    assert daemon._native_start_counts() | {} == {
        **daemon._native_start_counts(), "pending": 1, "held": 1,
    }
    started = time.monotonic()
    stopped = daemon.handle({"op": "daemon-stop", "force": True})
    assert time.monotonic() - started < .5
    assert stopped["ok"] and stopped["stopped"] and daemon.stopping
    assert stopped["nativeStartEvidence"]["pending"] == 1
    assert stopped["nativeStartEvidence"]["held"] == 1
    assert len(client.native_writes) == 2


def _p1_race_daemon(short_root, tmp_path, monkeypatch):
    from ccx.ops.creation_steps import prepare_worker
    store, daemon, client, _admit, _answer, _status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    prepared = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    outcome = {}

    def admission():
        outcome["admit"] = daemon._submit_original_codex_thread(
            B, prepared.record, prepared.native_source, "p1-race-original",
        )

    def stop():
        outcome["stop"] = daemon.handle({"op": "daemon-stop"})

    return daemon, client, outcome, admission, stop


def test_p1_admission_waits_for_normal_stop_decision(
    short_root, tmp_path, monkeypatch,
):
    """A start arriving during the stop decision must then see `stopping`."""
    import threading
    daemon, client, outcome, admission, stop = _p1_race_daemon(
        short_root, tmp_path, monkeypatch,
    )
    deciding = threading.Event()
    real_counts = daemon._native_start_counts

    def slow_counts():
        deciding.set()
        time.sleep(.2)  # stop holds daemon.lock across its whole decision
        return real_counts()

    monkeypatch.setattr(daemon, "_native_start_counts", slow_counts)
    stopper = threading.Thread(target=stop)
    stopper.start()
    assert deciding.wait(2)
    admitter = threading.Thread(target=admission)
    admitter.start()
    stopper.join(3)
    admitter.join(3)
    assert outcome["stop"]["ok"] is True
    assert outcome["admit"]["ok"] is False
    assert outcome["admit"]["localRefusalProven"] is True
    assert "stopping" in outcome["admit"]["error"]
    assert client.native_writes == []


def test_p1_normal_stop_waits_for_new_admission_reservation(
    short_root, tmp_path, monkeypatch,
):
    """A stop arriving during admission must count the reserved start."""
    import threading
    daemon, client, outcome, admission, stop = _p1_race_daemon(
        short_root, tmp_path, monkeypatch,
    )
    reserving = threading.Event()
    real_lock = daemon._native_start_hold_lock

    class PausingHoldLock:
        """Pause ONLY the admission thread inside its reservation section."""
        def __enter__(self):
            real_lock.acquire()
            if threading.current_thread().name == "p1-admit" and not reserving.is_set():
                reserving.set()
                time.sleep(.2)
            return self

        def __exit__(self, *exc):
            real_lock.release()
            return False

    daemon._native_start_hold_lock = PausingHoldLock()
    admitter = threading.Thread(target=admission, name="p1-admit")
    admitter.start()
    assert reserving.wait(2)
    stopper = threading.Thread(target=stop)
    stopper.start()
    admitter.join(3)
    stopper.join(3)
    assert outcome["admit"]["ok"] is True and outcome["admit"]["queued"] is True
    assert outcome["stop"]["ok"] is False
    assert outcome["stop"]["nativeStartEvidence"]["pending"] == 1
    assert not daemon.stop.is_set()
    assert len(client.native_writes) == 1


def test_p1_sigterm_stops_promptly_with_pending_original_start(
    short_root, tmp_path, monkeypatch,
):
    """Signals keep their prompt semantics; nothing is replayed."""
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, no_start_reply=True,
        persisted_threads=tmp_path / "p1-signal.json",
    ) as (cli, _restart, trace, store, rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", ".1")
        assert first.returncode == 3, (first.stdout, first.stderr)
        status = rpc({"op": "daemon-status"}, timeout=.6)
        assert status["nativeStartEvidence"]["pending"] == 1
        assert rpc({"op": "daemon-stop"}, timeout=1)["ok"] is False
        started = time.monotonic()
        os.kill(status["pid"], signal.SIGTERM)
        deadline = time.monotonic() + 3
        def exited(pid):
            # The harness reaps later; a zombie has already finished exiting.
            try:
                with open(f"/proc/{pid}/stat") as stat_file:
                    return stat_file.read().rsplit(")", 1)[1].split()[0] == "Z"
            except FileNotFoundError:
                return True
        while time.monotonic() < deadline:
            if exited(status["pid"]):
                break
            time.sleep(.05)
        else:
            pytest.fail("codexd ignored SIGTERM with a pending original start")
        assert time.monotonic() - started < 3
        assert trace.read_text().count("RPC thread/start") == 1
        assert store.native_activation(B)["state"] == "submitted-unknown"


# M1: the regular CLI, singleton codexd and physical fake app-server are all
# real processes. Temporary CLI hooks only hold selected interleavings.
