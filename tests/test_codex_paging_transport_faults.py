"""S3: bounded real-fd Codex JSONL writes; disposable fake app-servers only."""
from __future__ import annotations
import ccx.store.root as store_root

from contextlib import contextmanager
import errno
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

from ccx.codex.protocol import (
    AppServerClient, ProtocolError, ProtocolNotSent, ProtocolTransportError,
)
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import NativeActionDenied, NativeEffectIndeterminate, require_ccx_active
from test_generation_store import short_root, A, B


@contextmanager
def _fake_client(tmp_path: Path, script: str):
    child = tmp_path / "s3-fake-appserver.py"
    child.write_text(script, encoding="utf-8")
    client = AppServerClient(
        [sys.executable, "-B", str(child)], cwd=str(tmp_path),
        on_notification=lambda *_: None,
        on_server_request=lambda *_: None,
        stderr=subprocess.DEVNULL,
    )
    try:
        yield client
    finally:
        if client.process.poll() is None:
            client.process.kill()
        client.process.wait(timeout=4)
        client._reader.join(timeout=2)
        if client.process.stdin and not client.process.stdin.closed:
            client.process.stdin.close()


_NO_STDIN_READER = "import time\nwhile True:\n    time.sleep(1)\n"


def _fill_stalled_pipe(client: AppServerClient) -> int:
    """Physically exhaust a nonblocking pipe that the fake app-server never reads."""
    fd = client.process.stdin.fileno()
    total = 0
    while True:
        try:
            total += os.write(fd, b"x" * 4096)
        except BlockingIOError:
            break
    assert total >= 4096
    return total


def test_s3_real_stalled_stdin_pipe_short_local_deadline_preserves_siblings(tmp_path):
    """The original caller's short no-byte budget is not pipe corruption."""
    with _fake_client(tmp_path, _NO_STDIN_READER) as client:
        waiting = client.begin_request("original", {"text": "not read"})
        called = []
        waiting.add_done_callback(lambda pending: called.append(pending))
        assert _fill_stalled_pipe(client) >= 4096
        start = time.monotonic()
        with pytest.raises(ProtocolNotSent, match="deadline"):
            client.begin_request(
                "thread/start", {"source": "original-worker"},
                write_deadline=time.monotonic() + .24,
            )
        assert time.monotonic() - start < .9
        assert client.failure is None and not client._closed.is_set()
        assert not waiting.done() and called == []
        with client._state_lock:
            assert list(client._pending.values()) == [waiting]
        # An independent real transport break still wakes the sibling, with
        # no writer-lock dependency or speculative native-start retry.
        client.process.kill()
        client.process.wait(timeout=3)
        assert waiting.wait(1.5)
        assert client.failure is not None and called == [waiting]
        with pytest.raises(ProtocolTransportError):
            waiting.result()
        with pytest.raises(ProtocolNotSent):
            client.begin_request("thread/start", {"source": "replayed"})


def test_s3_writer_lock_budget_expiry_preserves_sibling_until_real_failure(tmp_path):
    """The held writer lock cannot cause a fatal latch on its own."""
    with _fake_client(tmp_path, _NO_STDIN_READER) as client:
        earlier = client.begin_request("earlier", {})
        calls = []
        earlier.add_done_callback(lambda p: calls.append(p))
        client._lock.acquire()  # actual original writer-serialization lock
        try:
            started = time.monotonic()
            with pytest.raises(ProtocolNotSent, match="writer lock"):
                client.begin_request(
                    "thread/start", {}, write_deadline=time.monotonic() + .15,
                )
            assert time.monotonic() - started < .6
            assert client.failure is None
            assert not earlier.done() and calls == []
            with client._state_lock:
                assert list(client._pending.values()) == [earlier]
            # The genuine provider/reader death is independently fatal, even
            # while the writer lock remains contested by another thread.
            client.process.kill()
            client.process.wait(timeout=3)
            assert earlier.wait(1.5)
            assert client.failure is not None and calls == [earlier]
        finally:
            client._lock.release()
        with pytest.raises(ProtocolTransportError):
            earlier.result()
        with pytest.raises(ProtocolNotSent):
            client.begin_request("after", {})


def test_s3_partial_frame_fails_shared_transport_before_next_jsonl(tmp_path, monkeypatch):
    import ccx.codex.protocol as protocol
    with _fake_client(tmp_path, _NO_STDIN_READER) as client:
        sibling = client.begin_request("sibling", {})
        notices = []
        sibling.add_done_callback(lambda pending: notices.append(pending))
        child_fd = client.process.stdin.fileno()
        real_write = os.write
        attempted = []

        def partial_then_pipe_error(fd, data):
            if fd != child_fd:
                return real_write(fd, data)
            attempted.append(bytes(data))
            if len(attempted) == 1:
                return real_write(fd, data[:7])  # an actually escaped prefix
            raise BrokenPipeError(errno.EPIPE, "injected failure after partial JSONL")

        monkeypatch.setattr(protocol.os, "write", partial_then_pipe_error)
        with pytest.raises(ProtocolTransportError, match="pipe write failed") as exc:
            client.begin_request("thread/start", {"text": "once"})
        assert not isinstance(exc.value, ProtocolNotSent)
        assert len(attempted) == 2 and attempted[1] == attempted[0][7:]
        assert client.failure is not None and sibling.done()
        with pytest.raises(ProtocolTransportError):
            sibling.result()
        assert notices == [sibling]
        with pytest.raises(ProtocolNotSent):
            client.begin_request("thread/start", {"text": "not repeated"})
        assert len(attempted) == 2


def test_s3_reader_fail_races_writer_without_deadlock_or_duplicate_callbacks(tmp_path):
    with _fake_client(tmp_path, _NO_STDIN_READER) as client:
        original = client.begin_request("thread/start", {"id": "original"})
        observed = []
        original.add_done_callback(lambda pending: observed.append(pending))
        assert _fill_stalled_pipe(client)
        finished = []
        started = threading.Event()
        def write_again():
            started.set()
            try:
                client.begin_request("sibling", {},
                                     write_deadline=time.monotonic() + .7)
            except ProtocolTransportError as error:
                finished.append(error)
        worker = threading.Thread(target=write_again, daemon=True)
        worker.start()
        assert started.wait(1)
        # Independently terminate the fake provider while the writer is
        # waiting for pipe availability. Its reader publishes failure without
        # acquiring the writer lock; both pending callbacks fire exactly once.
        client.process.kill()
        client.process.wait(timeout=3)
        worker.join(timeout=2)
        assert not worker.is_alive()
        assert finished and client.failure is not None
        assert observed == [original]
        with pytest.raises(ProtocolTransportError):
            original.result()
        with pytest.raises(ProtocolNotSent):
            client.begin_request("thread/start", {})


def test_s3_notification_and_server_response_have_finite_write_budget(tmp_path):
    with _fake_client(tmp_path, _NO_STDIN_READER) as client:
        _fill_stalled_pipe(client)
        client._WRITE_TIMEOUT_S = .15
        start = time.monotonic()
        with pytest.raises(ProtocolTransportError):
            client.notify("daemon/notice", {"text": "no app-server reader"})
        assert time.monotonic() - start < .7
        assert client.failure is not None
        with pytest.raises(ProtocolTransportError):
            client.respond("server-request-1", {"decision": "allow"})
        with pytest.raises(ProtocolTransportError):
            client.reject_request("server-request-2", "denied")


def test_s3_utf8_jsonl_provider_coded_rejection_and_response_timeout_distinct(tmp_path):
    trace = tmp_path / "s3-jsonl-received.jsonl"
    script = (
        "import json,sys\n"
        f"trace={str(trace)!r}\n"
        "for line in sys.stdin:\n"
        "    obj=json.loads(line)\n"
        "    with open(trace,'a',encoding='utf-8') as f: f.write(json.dumps(obj,ensure_ascii=False)+'\\n')\n"
        "    if obj.get('method') == 'thread/start':\n"
        "        answer={'jsonrpc':'2.0','id':obj['id'],'error':{'code':-32016,'message':'Rejeté 🌙'}}\n"
        "    elif obj.get('method') == 'silent':\n"
        "        continue\n"
        "    elif 'id' in obj:\n"
        "        answer={'jsonrpc':'2.0','id':obj['id'],'result':{'ok':True}}\n"
        "    else: continue\n"
        "    print(json.dumps(answer,ensure_ascii=False),flush=True)\n"
    )
    with _fake_client(tmp_path, script) as client:
        pending = client.begin_request("thread/start", {"text": "niño ☕"})
        assert pending.wait(3)
        proof = pending.original_provider_rejection()
        assert proof is not None and proof["code"] == -32016
        assert proof["message"] == "Rejeté 🌙"
        with pytest.raises(ProtocolError) as error:
            pending.result()
        assert error.value.code == -32016
        assert client.request("normal", {"payload": "café"}, timeout=2)["ok"]
        with pytest.raises(ProtocolError, match="silent timed out") as timeout:
            client.request("silent", timeout=.15)
        assert not isinstance(timeout.value, ProtocolTransportError)
        assert client.failure is None  # an unanswered read does not poison wire
        client.notify("client/notification", {"text": "élève"})
        client.respond("server-request", {"response": "東京"})
        until = time.monotonic() + 3
        while time.monotonic() < until:
            if trace.exists() and len(trace.read_text().splitlines()) >= 5:
                break
            time.sleep(.02)
        received = [json.loads(line) for line in trace.read_text().splitlines()]
        assert [item.get("method") for item in received[:4]] == [
            "thread/start", "normal", "silent", "client/notification",
        ]
        assert received[0]["params"]["text"] == "niño ☕"
        assert received[3]["params"]["text"] == "élève"
        assert received[4]["result"]["response"] == "東京"
        assert client.failure is None


def test_s3_stalled_native_dispatch_releases_generation_lease_within_budget(
    short_root, tmp_path, monkeypatch,
):
    from ccx.codex.daemon.daemon import CodexDaemon
    with _fake_client(tmp_path, _NO_STDIN_READER) as client:
        from ccx.store.root import NativeActionDenied
        store = require_ccx_active(short_root)
        from ccx import constants
        monkeypatch.setattr(constants, "CCX_ROOT", short_root)
        monkeypatch.setenv("CCX_ROOT", str(short_root))
        worker = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
        reservation = store.reserve_creation(
            B, engine="codex", fingerprint=worker.record["createSha256"],
            native_binding={},
        )
        original = store.prepare_native_activation(
            reservation, native_source=worker.native_source,
        )
        daemon = CodexDaemon()
        daemon._uuid_store = store
        daemon.client = client
        daemon.ready.set()
        _fill_stalled_pipe(client)
        attempts = []
        native_begin = client.begin_request
        def one_attempt(method, params, *, write_deadline=None):
            attempts.append((method, write_deadline))
            return native_begin(method, params, write_deadline=write_deadline)
        monkeypatch.setattr(client, "begin_request", one_attempt)
        effects = []
        started = time.monotonic()

        def dispatch_original():
            try:
                effects.append(store.native_activation_dispatch(
                    original, dispatch_budget_s=.75,
                    bounded_submit=lambda remaining: daemon._submit_original_codex_thread(
                        B, worker.record, worker.native_source, original.submission_id,
                        admission_deadline=time.monotonic() + remaining,
                    ),
                ))
            except NativeEffectIndeterminate as error:
                effects.append(error)
        thread = threading.Thread(target=dispatch_original, daemon=True)
        thread.start()
        # Separate exclusive lock descriptor, as a cutover runner would use.
        # It must become available even if the provider never drains stdin.
        lock_path = short_root / "identity" / "activation.lock"
        acquired_at = None
        with lock_path.open("rb") as exclusive:
            while time.monotonic() - started < 1.6:
                try:
                    fcntl.flock(exclusive.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    time.sleep(.015)
                else:
                    acquired_at = time.monotonic()
                    fcntl.flock(exclusive.fileno(), fcntl.LOCK_UN)
                    # If obtained before dispatch got its lease, wait for
                    # admission to conclude and prove it is still bounded.
                    if not thread.is_alive():
                        break
                    time.sleep(.015)
        thread.join(timeout=2)
        assert not thread.is_alive(), "stalled stdin cannot outlive <=.75s lease budget"
        assert acquired_at is not None and time.monotonic() - started < 1.6
        assert effects and len(attempts) == 1, (effects, attempts)
        assert (isinstance(effects[0], NativeEffectIndeterminate)
                or effects[0].get("localRefusalProven") is True), effects
        assert attempts[0][0] == "thread/start"
        assert attempts[0][1] - started <= .95
        assert store.native_activation(B)["state"] == "submitted-unknown"
        assert store.record(B)["native"]["threadId"] is None
        assert client.failure is None  # short original timeout was request-only
        assert store.reconcile_codex_local_refusal(B)["localRefused"] is True
        assert store.native_activation(B)["state"] == "local-refused"
        with client._state_lock:
            assert not client._pending
        with pytest.raises(NativeActionDenied):
            store.authorize_native_action(B, "send")
        with pytest.raises(ProtocolNotSent):
            client.begin_request(
                "health-check", {"readOnly": True},
                write_deadline=time.monotonic() - .001,
            )
        assert [method for method, _ in attempts] == ["thread/start", "health-check"]
        assert client.failure is None
        assert store.native_activation(B)["state"] == "local-refused"


def test_s3_failure_callback_runs_after_writer_lock_released(tmp_path, monkeypatch):
    """A failure callback may itself inspect the writer; no client-lock reentry."""
    import ccx.codex.protocol as protocol
    with _fake_client(tmp_path, _NO_STDIN_READER) as client:
        original = client.begin_request("sibling", {})
        child_fd = client.process.stdin.fileno()
        real_write = os.write
        callback_observed = []

        def on_failed(pending):
            acquired = client._lock.acquire(blocking=False)
            callback_observed.append((acquired, pending.done()))
            if acquired:
                client._lock.release()
            # No frame or native retry can be issued from a callback after a
            # fatal partial write has already been latched.
            with pytest.raises(ProtocolNotSent):
                client.begin_request("read-only-after-failure", {})

        original.add_done_callback(on_failed)
        calls = []
        def partial_then_error(fd, raw):
            if fd == child_fd:
                calls.append(raw)
                if len(calls) == 1:
                    return real_write(fd, raw[:3])
                raise BrokenPipeError(errno.EPIPE, "broken midway")
            return real_write(fd, raw)

        monkeypatch.setattr(protocol.os, "write", partial_then_error)
        with pytest.raises(ProtocolTransportError):
            client.begin_request("thread/start", {})
        assert callback_observed == [(True, True)]
        assert len(calls) == 2
        assert client.failure is not None
        with pytest.raises(ProtocolTransportError):
            original.result()


# Opus T1-a/b/c use the ACTUAL shared AppServerClient and physical disposable
# fake app-server process. No real Codex provider is invoked.
_S3_HEALTHY_PROVIDER = '''
import json
import sys
import time
for raw in sys.stdin:
    request = json.loads(raw)
    method = request.get("method")
    if method == "slow-sibling":
        time.sleep(.25)
    if "id" in request:
        print(json.dumps({"jsonrpc":"2.0", "id":request["id"], "result":{
            "ok":True, "method":method, "params":request.get("params") or {},
        }}), flush=True)
'''


def test_s3_t1a_expired_request_budget_before_first_byte_keeps_echo_child_healthy(tmp_path):
    """Opus T1-a: pre-expired write deadline is LOCAL, not daemon-fatal."""
    with _fake_client(tmp_path, _S3_HEALTHY_PROVIDER) as client:
        with pytest.raises(ProtocolNotSent, match="deadline"):
            client.begin_request(
                "thread/start", {"text": "do not emit"},
                write_deadline=time.monotonic() - .001,
            )
        assert client.failure is None and not client._closed.is_set()
        with client._state_lock:
            assert not client._pending  # no abandoned original pending sink
        pong = client.request("ping", {"text": "still alive"}, timeout=1)
        assert pong == {
            "ok": True, "method": "ping", "params": {"text": "still alive"},
        }
        assert client.failure is None


def test_s3_t1b_contended_writer_lock_preserves_live_pending_sibling(tmp_path):
    """Opus T1-b: brief writer contention never poisons a live sibling."""
    with _fake_client(tmp_path, _S3_HEALTHY_PROVIDER) as client:
        sibling = client.begin_request("slow-sibling", {"text": "already submitted"})
        notified = []
        sibling.add_done_callback(lambda pending: notified.append(pending))
        client._lock.acquire()
        try:
            started = time.monotonic()
            with pytest.raises(ProtocolNotSent, match="writer lock"):
                client.begin_request(
                    "thread/start", {"text": "would be a duplicate"},
                    write_deadline=time.monotonic() + .05,
                )
            assert time.monotonic() - started < .4
            assert client.failure is None and not client._closed.is_set()
            assert not sibling.done()
            with client._state_lock:
                assert list(client._pending.values()) == [sibling]
        finally:
            client._lock.release()
        assert sibling.wait(2)
        assert notified == [sibling]
        assert sibling.result()["method"] == "slow-sibling"
        assert client.request("ping", {"kind": "healthy"}, timeout=1)["ok"]
        with client._state_lock:
            assert not client._pending
        assert client.failure is None


def test_s3_t1c_marker_fsync_spent_admission_budget_existing_daemon_worker_healthy(
    short_root, tmp_path, monkeypatch,
):
    """Opus T1-c: slow ORIGINAL marker fsync must not crash sibling worker."""
    import ccx.store.store as generation
    import ccx.codex.protocol as codex_protocol
    import ccx.codex.daemon.daemon as codexd
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.store.root import CreationIndeterminate

    # Advance the SAME admission clock used by generation, codexd and the
    # app-server writer only *after* the positive durable marker is fsynced.
    # A real sleep and a wall-clock '< 1.5s' check measure host CPU scheduling
    # under load, not the 0.4s native admission invariant.
    real_time = time
    class AdmissionClock:
        advance_s = 0.0

        def monotonic(self):
            return real_time.monotonic() + self.advance_s

        def __getattr__(self, name):
            return getattr(real_time, name)

    clock = AdmissionClock()
    for module in (generation, codexd, codex_protocol):
        monkeypatch.setattr(module, "time", clock)

    with _fake_client(tmp_path, _S3_HEALTHY_PROVIDER) as client:
        store = require_ccx_active(short_root)
        from ccx import constants
        monkeypatch.setattr(constants, "CCX_ROOT", short_root)
        monkeypatch.setenv("CCX_ROOT", str(short_root))
        prepared = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
        reservation = store.reserve_creation(
            B, engine="codex", fingerprint=prepared.record["createSha256"],
            native_binding={},
        )
        original = store.prepare_native_activation(
            reservation, native_source=prepared.native_source,
        )
        daemon = CodexDaemon()
        daemon._uuid_store = store
        daemon.client = client
        daemon.ready.set()

        class ExistingWorker:
            def state(self):
                return "idle"

        daemon.workers["existing-worker"] = ExistingWorker()
        assert client.request("existing-worker-ping", timeout=1)["ok"]

        physical_native_calls = []
        original_begin = client.begin_request

        def counted_begin(method, params, *, write_deadline=None):
            physical_native_calls.append(method)
            return original_begin(method, params, write_deadline=write_deadline)

        monkeypatch.setattr(client, "begin_request", counted_begin)
        real_write = store_root._write_json
        delayed = []

        def slow_original_marker(fd, filename, content):
            result = real_write(fd, filename, content)
            if (isinstance(content, dict)
                    and content.get("kind") == "activate"
                    and content.get("daemonInstanceId") == daemon.instance_id
                    and not delayed):
                delayed.append(filename)
                # The positive ORIGINAL submission marker is already fsynced.
                # Deterministically exhaust the same 0.4s admission budget
                # before the native pipe write, without sleeping or retrying.
                clock.advance_s += .55
            return result

        monkeypatch.setattr(store_root, "_write_json", slow_original_marker)
        original_result = store.native_activation_dispatch(
            original, dispatch_budget_s=.4,
            bounded_submit=lambda remaining: daemon._submit_original_codex_thread(
                B, prepared.record, prepared.native_source,
                original.submission_id,
                admission_deadline=time.monotonic() + remaining,
            ),
        )
        assert original_result["localRefusalProven"] is True
        assert clock.advance_s == .55  # one original fsynced marker, one budget jump
        assert len(delayed) == 1 and physical_native_calls == ["thread/start"]
        effect_directory = (store.pointer.generation_root / "runtime-metadata"
                            / "codex-native-effects" / B)
        assert len(list(effect_directory.glob("*.json"))) == 1
        assert store.native_activation(B)["state"] == "submitted-unknown"
        assert store.record(B)["native"]["threadId"] is None
        # L3 now seals only the exact zero-byte original: still NO remint or retry.
        assert store.reconcile_codex_local_refusal(B)["localRefused"] is True
        assert store.native_activation(B)["state"] == "local-refused"
        with pytest.raises(CreationIndeterminate):
            store.native_activation_dispatch(
                original, dispatch_budget_s=.2,
                bounded_submit=lambda _budget: physical_native_calls.append("replay"),
            )
        assert physical_native_calls == ["thread/start"]
        assert client.failure is None and daemon.runtime_failure() is None
        assert not daemon.stopping and not daemon.stop.is_set()
        assert daemon.handle({"op": "daemon-status"})["workers"] == {
            "existing-worker": "idle",
        }
        assert client.request("existing-worker-ping", timeout=1)["ok"]
        assert client.failure is None
        with client._state_lock:
            assert not client._pending


def test_s3_zero_byte_epipe_is_genuine_fatal_channel_failure(tmp_path, monkeypatch):
    """Not all no-byte errors are budget expiry: actual EPIPE still kills wire."""
    import ccx.codex.protocol as protocol
    with _fake_client(tmp_path, _NO_STDIN_READER) as client:
        sibling = client.begin_request("sibling", {})
        delivered = []
        sibling.add_done_callback(lambda pending: delivered.append(pending))
        fd = client.process.stdin.fileno()
        real_write = os.write
        calls = []

        def broken_without_any_bytes(write_fd, frame):
            if write_fd == fd:
                calls.append(frame)
                raise BrokenPipeError(errno.EPIPE, "injected real broken pipe")
            return real_write(write_fd, frame)

        monkeypatch.setattr(protocol.os, "write", broken_without_any_bytes)
        with pytest.raises(ProtocolTransportError, match="pipe write failed") as fault:
            client.begin_request("thread/start", {})
        assert not isinstance(fault.value, ProtocolNotSent)
        assert calls and sibling.done() and delivered == [sibling]
        assert client.failure is not None and client._closed.is_set()
        with pytest.raises(ProtocolTransportError):
            sibling.result()
        with pytest.raises(ProtocolNotSent):
            client.begin_request("thread/start", {"replay": True})
        assert len(calls) == 1


def test_s3_full_ordinary_stdin_stall_remains_fatal(tmp_path):
    """A genuinely unhealthy pipe failing the full normal write window is fatal."""
    with _fake_client(tmp_path, _NO_STDIN_READER) as client:
        sibling = client.begin_request("already-pending", {})
        delivered = []
        sibling.add_done_callback(lambda pending: delivered.append(pending))
        _fill_stalled_pipe(client)
        client._WRITE_TIMEOUT_S = .2
        started = time.monotonic()
        with pytest.raises(ProtocolTransportError, match="full write window") as failure:
            client.begin_request("thread/start", {"one": True})
        assert not isinstance(failure.value, ProtocolNotSent)
        assert .15 <= time.monotonic() - started < .75
        assert client.failure is not None and sibling.done()
        assert delivered == [sibling]
        with pytest.raises(ProtocolTransportError):
            sibling.result()
        with pytest.raises(ProtocolNotSent):
            client.begin_request("thread/start", {"cannot-replay": True})


def test_s3_t2_lone_surrogate_json_ascii_escape_roundtrips(tmp_path):
    """Opus T2: historical JSON escaping supports surrogateescape filenames."""
    trace = tmp_path / "surrogate-jsonl-raw.bin"
    child = (
        "import json,sys\n"
        f"path={str(trace)!r}\n"
        "for line in sys.stdin:\n"
        "    with open(path,'ab') as f: f.write(line.encode('ascii'))\n"
        "    message=json.loads(line)\n"
        "    if 'id' in message:\n"
        "        print(json.dumps({'jsonrpc':'2.0','id':message['id'],\n"
        "            'result':{'text':message.get('params',{}).get('text')}},\n"
        "            ensure_ascii=True),flush=True)\n"
    )
    text = "path from surrogateescape: bad \udcff and Unicode 🚀"
    with _fake_client(tmp_path, child) as client:
        answer = client.request("turn/start", {"text": text}, timeout=2)
        assert answer["text"] == text
        wire = trace.read_bytes()
        assert b"\\udcff" in wire and b"\\ud83d\\ude80" in wire
        assert json.loads(wire)["params"]["text"] == text
        assert client.failure is None
        with client._state_lock:
            assert not client._pending


def test_s3_overload_retry_reuses_semantic_client_id_with_real_jsonl(tmp_path):
    """An explicit overload resends the same supplied semantic message, once."""
    trace = tmp_path / "s3-overload-jsonl.jsonl"
    script = (
        "import json,sys\n"
        f"trace={str(trace)!r}\n"
        "seen=0\n"
        "for line in sys.stdin:\n"
        "    frame=json.loads(line)\n"
        "    with open(trace,'a',encoding='utf-8') as output:\n"
        "        output.write(json.dumps(frame,sort_keys=True)+'\\n')\n"
        "        output.flush()\n"
        "    seen+=1\n"
        "    if seen==1:\n"
        "        response={'jsonrpc':'2.0','id':frame['id'],\n"
        "                  'error':{'code':-32001,'message':'retry later'}}\n"
        "    else:\n"
        "        response={'jsonrpc':'2.0','id':frame['id'],\n"
        "                  'result':{'turn':{'id':'turn-after-overload'}}}\n"
        "    print(json.dumps(response),flush=True)\n"
    )
    cid = ("ccx:v2:aaaaaaaa-1234-4000-8000-000000000001:start:"
           "bbbbbbbb-1234-4000-8000-000000000002")
    params = {
        "threadId": "native-thread-1",
        "input": [{"type": "text", "text": "one semantic turn"}],
        "clientUserMessageId": cid,
    }
    with _fake_client(tmp_path, script) as client:
        result = client.request("turn/start", params, timeout=3)
        assert result == {"turn": {"id": "turn-after-overload"}}
        frames = [json.loads(line) for line in trace.read_text().splitlines()]
        assert len(frames) == 2  # flushed before replying: no third native frame
        assert [frame["method"] for frame in frames] == ["turn/start", "turn/start"]
        assert [frame["params"] for frame in frames] == [params, params]
        assert frames[0]["params"]["clientUserMessageId"] == cid
        assert frames[0]["id"] != frames[1]["id"]
        assert client.failure is None
