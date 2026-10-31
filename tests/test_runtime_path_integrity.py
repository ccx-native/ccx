"""Existing host/codexd physical paths require ACTIVE full-owner authority."""
from __future__ import annotations

from pathlib import Path
import pytest

from test_generation_store import A, B, G1, fixture_root, short_root, switch_pointer
from ccx import constants
from ccx.store import paths
from ccx.codex import paths as codex_paths
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import GenerationUnavailable, require_ccx_active
from ccx.store.identity import EngineMismatch, InvalidTarget, UnknownWorker


def test_original_path_modules_full_UUID_and_generation_no_legacy_root(short_root, tmp_path, monkeypatch):
    root = short_root
    store = require_ccx_active(root)
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    gen = root / "identity" / "generations" / G1
    assert paths.record_path(A) == gen / "workers" / "claude" / A / "record.json"
    assert paths.host_pid_path(A) == gen / "workers" / "claude" / A / "host.pid"
    assert paths.events_ndjson_path(A) == gen / "workers" / "claude" / A / "events.ndjson"
    assert paths.host_log_path(A) == gen / "workers" / "claude" / A / "host.log"
    from ccx.rpc.transport import plan_transport
    claude_plan = plan_transport(store, target="claude:" + A)
    assert paths.sock_path(A) == claude_plan.final
    assert claude_plan.final.name == A + ".sock"
    assert codex_paths.record_path(B) == gen / "workers" / "codex" / B / "record.json"
    assert codex_paths.evidence_path(B) == gen / "workers" / "codex" / B / "evidence.jsonl"
    assert codex_paths.codexd_sock_path() == plan_transport(store, role="codexd").final
    assert not (root / "sessions").exists() and not (root / "codex").exists()
    assert not (root / "sock").exists() and not (root / "pid").exists()
    paths.ensure_dirs()
    codex_paths.ensure_dirs()
    assert not (root / "sock").exists() and not (root / "sessions").exists()
    assert not (root / "run").exists()
    for f in (paths.record_path, paths.sock_path, codex_paths.record_path):
        with pytest.raises(InvalidTarget):
            f("aaaaaaaa")
    for f in (paths.record_path, paths.sock_path):
        with pytest.raises(EngineMismatch):
            f(f"codex:{B}")
    with pytest.raises(EngineMismatch):
        codex_paths.evidence_path(f"claude:{A}")


def test_unmigrated_path_helpers_refuse_before_incidental_mkdir(tmp_path, monkeypatch):
    root = tmp_path / "legacy"
    root.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    for f in (
        paths.ensure_dirs, paths.sessions_dir, codex_paths.ensure_dirs,
        codex_paths.root, codex_paths.codexd_sock_path,
        lambda: paths.record_path(A),
        lambda: codex_paths.evidence_path(B),
    ):
        with pytest.raises(GenerationUnavailable):
            f()
        assert list(root.iterdir()) == []


def test_pointer_flip_rejects_foreign_first_generation_no_old_path_reuse(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    store = require_ccx_active(root)
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    old = paths.record_path(A)
    switch_pointer(root)
    with pytest.raises(UnknownWorker):
        paths.record_path(A)
    with pytest.raises(UnknownWorker):
        paths.sock_path(A)
    assert old.exists()
    assert not (root / "sessions").exists()



def _submitted_claude_start(root: Path, cwd: Path, worker_id: str):
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.root import require_ccx_active
    store = require_ccx_active(root)
    prepared = prepare_worker(store, engine="claude", cwd=str(cwd),
                              worker_id=worker_id)
    reservation = store.reserve_creation(
        worker_id, engine="claude",
        fingerprint=prepared.record["createSha256"],
        native_binding={"sessionId": prepared.record["native"]["sessionId"]},
    )
    activation = store.prepare_native_activation(
        reservation, native_source=prepared.native_source,
    )
    # The launcher tests need only the durable submitted-unknown fence, never
    # an actual native admission. Inject a crash right AFTER its atomic fsync:
    # if a loaded scheduler pauses between fsync and the 1s fake transport
    # deadline, an otherwise correct fixture used to raise before the fake
    # bounded_submit was even invoked.
    class PostSubmissionCrash(Exception):
        pass

    def after_fence(phase):
        if phase == "after-submitted":
            raise PostSubmissionCrash()

    with pytest.raises(PostSubmissionCrash):
        store.native_activation_dispatch(
            activation,
            bounded_submit=lambda _budget: pytest.fail("no native transport expected"),
            _failpoint=after_fence,
        )
    assert store.native_activation(worker_id)["state"] == "submitted-unknown"
    return prepared


def test_real_spawn_module_never_launches_unproven_full_worker(short_root, tmp_path, monkeypatch):
    from ccx.claude import spawn
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.root import CreationIndeterminate
    import subprocess, json
    root = short_root
    store = require_ccx_active(root)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    called = []
    monkeypatch.setattr(spawn.subprocess, "run",
                        lambda *a, **k: called.append(a))
    with pytest.raises(CreationIndeterminate, match="submitted"):
        spawn.spawn_host_detached(A)
    assert called == []
    assert not (root / "run").exists()
    assert not (root / "sessions").exists()


def test_real_host_launcher_receives_complete_owned_worker_ID(short_root, tmp_path, monkeypatch):
    from ccx.claude import spawn
    import subprocess, json
    root = short_root
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    prepared = _submitted_claude_start(root, tmp_path, A)
    invoked = []
    def fake_launch(argv, **kwargs):
        invoked.append((argv, kwargs))
        assert argv[2:4] == ["host", A]
        assert "--detach" in argv
        assert "env" not in kwargs
        return subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps({"ok": True, "worker_id": A}).encode(),
            stderr=b"",
        )
    monkeypatch.setattr(spawn.subprocess, "run", fake_launch)
    verdict = spawn.spawn_host_detached(A, sock_wait_s=.5)
    assert verdict["ok"] is True and verdict["worker_id"] == A
    assert len(invoked) == 1
    assert not (root / "sessions").exists()
    assert not (root / "run").exists()


def _hold_actual_host_lock(args):
    root, wid, acquired, release = args
    from pathlib import Path
    from ccx.claude.host import session as host
    from ccx.store.root import require_ccx_active
    store = require_ccx_active(Path(root))
    with host._host_process_lease(store, wid):
        acquired.set()
        release.wait(10)


def test_original_host_cmd_requires_native_submission_and_lifetime_lock(short_root, tmp_path, monkeypatch):
    import multiprocessing, fcntl, os
    from types import SimpleNamespace
    from ccx.claude.host import session as host
    from ccx.store.root import CreationIndeterminate, StorageIntegrityError
    root = short_root
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    from ccx.ops.creation_steps import prepare_worker
    store = require_ccx_active(root)
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    called = []
    monkeypatch.setattr(host, "_run_host", lambda args: called.append(args.short))
    with pytest.raises(CreationIndeterminate):
        host.cmd_host(SimpleNamespace(short=A, detach=False))
    assert called == []
    assert not (root / "identity" / "generations" / G1 /
                "runtime-metadata" / "host-locks").exists()
    _submitted_claude_start(root, tmp_path, A)
    mp = multiprocessing.get_context("fork")
    acquired, release = mp.Event(), mp.Event()
    child = mp.Process(target=_hold_actual_host_lock,
                       args=((str(root), A, acquired, release),))
    child.start()
    try:
        assert acquired.wait(5)
        with pytest.raises(StorageIntegrityError, match="another Claude host"):
            host.cmd_host(SimpleNamespace(short=A, detach=False))
        assert called == []
    finally:
        release.set()
        child.join(5)
        if child.is_alive():
            child.terminate()
            child.join(5)
    assert child.exitcode == 0
    host.cmd_host(SimpleNamespace(short=A, detach=False))
    assert called == [A]
    assert not (root / "sock").exists() and not (root / "pid").exists()


def test_host_lifetime_flock_does_not_hold_activation_during_native_turn(short_root, tmp_path, monkeypatch):
    from ccx.claude.host import session as host
    import fcntl, os
    root = short_root
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    prepared = _submitted_claude_start(root, tmp_path, A)
    store = require_ccx_active(root)
    with host._host_process_lease(store, A):
        fd = os.open(root / "identity" / "activation.lock", os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
    assert not (root / "sessions").exists()



@pytest.mark.parametrize("mode,positive_stop", [
    ("", True), ("wrong_session", False), ("missing_assistant", False),
])
def test_existing_host_loop_with_real_fake_claude_on_full_UUID(
    short_root, tmp_path, monkeypatch, mode, positive_stop,
):
    'An actual subprocess runs ccx.claude.host.cmd_host and tests/fake_claude.py.\n\n    No native Claude binary/provider, service launch, production socket or\n    user data. Its SessionStart hook must cross the real AF_UNIX listener.\n    '
    import os, subprocess, sys, time, signal
    from ccx.store import paths
    from ccx.store.root import require_ccx_active
    root = short_root
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    worker = _submitted_claude_start(root, tmp_path, A)
    wrapper = tmp_path / "fake-claude-wrapper"
    from pathlib import Path
    repo = Path(__file__).resolve().parent.parent
    fake = repo / "tests" / "fake_claude.py"
    wrapper.write_text(
        "#!/bin/sh\nexec "+sys.executable+" "+str(fake)+" \"$@\"\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o700)
    env = {
        **os.environ,
        "CCX_ROOT": str(root), "CCX_CLAUDE_BIN": str(wrapper),
        "PYTHONPATH": str(repo),
        "FAKE_MODE": mode, "FAKE_LOG": str(tmp_path / "fake-input.log"),
    }
    env.pop("CCX_SELF_SHORT", None)
    env.pop("CCX_SELF_WORKER_ID", None)
    env.pop("CCX_SELF_ENGINE", None)
    logfile = tmp_path / "real-host-fake.log"
    with logfile.open("wb") as output:
        proc = subprocess.Popen(
            [sys.executable, "-B", "-c",
             "from types import SimpleNamespace; from ccx.claude.host.session import cmd_host; cmd_host(SimpleNamespace(short='"+A+"',detach=False))"],
            env=env, stdout=output, stderr=output,
            cwd=str(repo), start_new_session=True,
        )
        try:
            deadline = time.monotonic() + 12
            reached = None
            while time.monotonic() < deadline:
                rec = require_ccx_active(root).record(A)
                state = (rec.get("lifecycle") or {}).get("state")
                if state == "idle" and paths.sock_path(A).is_socket():
                    reached = rec
                    break
                if proc.poll() is not None:
                    break
                time.sleep(.05)
            assert reached is not None, (
                "host never received fake SessionStart; rc="+str(proc.poll())
                +" log="+logfile.read_text(errors="replace")[-4500:]
            )
            assert reached["native"]["sessionId"] == (
                worker.record["native"]["sessionId"]
            )
            assert reached["lifecycle"]["hostPid"] == proc.pid
            assert paths.sock_path(A).is_socket()
            assert not (root / "sessions").exists()
            assert not (root / "sock").exists()
            assert not (root / "pid").exists()
            assert not (root / "codex").exists()
            # An old, unkeyed IPC write (or even a claimed semantic ID with
            # no global fsynced effect fence) must NOT reach the real PTY.
            from ccx.rpc.rpc import _rpc
            denied = _rpc(A, {"op": "send", "text": "should-not-send"},
                          timeout=2.0)
            assert denied["ok"] is False and "legacy unkeyed" in denied["error"]
            denied = _rpc(A, {
                "op": "deliver", "delivery_id": "unproven-token",
                "mode": "new_turn_only", "text": "should-not-send",
            }, timeout=2.0)
            assert denied["ok"] is False
            assert "native effect fence absent" in denied["error"]
            assert not (tmp_path / "fake-input.log").exists()

            # Original existing V1 send through a real host AF_UNIX socket.
            # The native reply is ONLY ownership, never positive acceptance.
            from ccx.store.store import NativeActivation
            from ccx.store.root import NativeEffectIndeterminate
            from ccx.ops.send import send_turn
            from ccx.claude import delivery as claude_delivery
            import hashlib, json
            store = require_ccx_active(root)
            act = store.native_activation(A)
            origin = NativeActivation(
                A, "claude", worker.native_source, act["submissionId"],
                "submitted-unknown", store.generation,
            )
            token = hashlib.sha256((
                A + "|" + worker.record["native"]["sessionId"] + "|" +
                str(reached["lifecycle"]["hostPid"]) + "|" +
                str(reached["lifecycle"]["workerPid"])
            ).encode()).hexdigest()
            store.confirm_native_activation(
                origin, native_evidence_sha256=token,
                native_binding={"sessionId": worker.record["native"]["sessionId"]},
            )
            store.bind_confirmed_native(A)
            answers = []
            def real_socket_callback(handle, native_id, prompt, delivery_id, budget):
                reply = _rpc(A, {
                    "op": "deliver", "delivery_id": delivery_id,
                    "mode": "new_turn_only", "text": prompt,
                }, timeout=budget)
                answers.append(reply)
                return reply
            pending = send_turn(
                A, "real fake host prompt", delivery_id="actual-pty-turn",
                _native_submit=real_socket_callback,
            )
            assert pending["status"] in ("owned", "accepted")
            assert pending["delivery"]["status"] in ("pending", "accepted")
            assert pending["worker"]["worker_id"] == A
            assert len(answers) == 1
            # A second caller asking for the same original token while the
            # real host is still working must not send another PTY prompt.
            retried_calls = []
            def deny_second_native(*_args):
                retried_calls.append(True)
                raise AssertionError("host-owning original cannot be resent")
            outstanding = send_turn(
                A, "real fake host prompt", delivery_id="actual-pty-turn",
                _native_submit=deny_second_native,
            )
            assert outstanding["status"] in ("owned", "accepted")
            assert retried_calls == []
            deadline = time.monotonic() + 12
            native_bytes = b""
            while time.monotonic() < deadline:
                if (tmp_path / "fake-input.log").exists():
                    native_bytes = (tmp_path / "fake-input.log").read_bytes()
                if b"real fake host prompt" in native_bytes:
                    break
                time.sleep(.05)
            events = [
                json.loads(line) for line in paths.events_ndjson_path(A).read_text().splitlines()
            ]
            types = [(e.get("type"), e.get("reason")) for e in events]
            original = claude_delivery.read_receipt(A, "actual-pty-turn")
            hot = paths.steer_deliveries_path(A)
            journal = json.loads(hot.read_text()) if hot.exists() else None
            assert b"real fake host prompt" in native_bytes, (
                "original host PTY failed: answers=" + repr(answers)
                + " semantic=" + repr(original)
                + " journal=" + repr(journal)
                + " events=" + repr(types[-25:])
                + " currentRecord=" + repr(store.record(A).get("lifecycle"))
                + " hostPoll=" + repr(proc.poll())
                + " files=" + repr(sorted(
                    str(p.relative_to(root)) for p in root.rglob("*")
                    if p.is_file() and p.stat().st_size < 8192
                )[-40:])
                + " log=" + logfile.read_text(errors="replace")[-1200:]
            )
            # Real fake-provider hook/PTY evidence, not a synthetic callback:
            # original semantic delivery is accepted only if UserPromptSubmit
            # proves the exact original text and a Stop ends that same turn.
            accepted_deadline = time.monotonic() + 5
            final_semantic = None
            complete_event = None
            while time.monotonic() < accepted_deadline:
                final_semantic = claude_delivery.read_receipt(A, "actual-pty-turn")
                published = [json.loads(line) for line in
                             paths.events_ndjson_path(A).read_text().splitlines()]
                # The original turn_complete event identifies the immutable
                # turn_ref; delivery_id belongs to the earlier turn_start.
                # Never invent a deliveryId on the settled event itself.
                completed = [ev for ev in published if ev.get("type") == "turn_complete"
                             and ev.get("turn_ref") == final_semantic.get("turnRef")
                             and final_semantic.get("turnRef")]
                if final_semantic["state"] == "accepted" and completed:
                    complete_event = completed[-1]
                    break
                time.sleep(.05)
            assert final_semantic["state"] == "accepted", (
                "fake real-host UserPromptSubmit did not authenticate original semantic turn: "
                + repr(final_semantic)
                + " events=" + repr([ev.get("type") for ev in published[-20:]])
            )
            assert complete_event is not None, (
                "fake real-host Stop never completed same original native turn; "
                + repr([ev for ev in published[-20:]])
            )

            assert complete_event["turn"] == 1
            assert complete_event["turn_ref"] == final_semantic["turnRef"]
            assert final_semantic["evidence"] == "user-prompt-submit"
            from ccx.ops.wait import reconcile_turn
            present = reconcile_turn(A, delivery_id="actual-pty-turn")
            assert present["delivery"]["turn_ref"] == final_semantic["turnRef"]
            assert present["status"] in ("accepted", "settled")
            from ccx.ops.report import report_worker
            report_deadline = time.monotonic() + 5
            result = None
            while time.monotonic() < report_deadline:
                final_semantic = claude_delivery.read_receipt(A, "actual-pty-turn")
                if isinstance(final_semantic.get("settlement"), dict):
                    result = report_worker(A, turn_ref=final_semantic["turnRef"])
                    break
                time.sleep(.05)
            if not positive_stop:
                assert result is None, (
                    "foreign/missing native Stop source must NEVER settle a turn"
                )
                assert claude_delivery.read_receipt(
                    A, "actual-pty-turn"
                ).get("settlement") is None
                assert store.read_receipt(
                    A, "claude-delivery", "actual-pty-turn"
                )["state"] == "submitted-unknown"
                from ccx.errors import SessionUnavailable
                with pytest.raises(SessionUnavailable):
                    report_worker(A)
            else:
                assert result is not None, (
                    "positive native Stop source not reconciled into original full-UUID "
                    "delivery receipt: " + repr(final_semantic)
                    + " events=" + repr([item.get("type") for item in published[-15:]])
                )
                assert result["status"] == "completed"
                assert result["text"] == "Fake Claude completed: real fake host prompt"
                assert result["delivery_id"] == "actual-pty-turn"
                assert result["turn_ref"] == complete_event["turn_ref"]
                assert store.read_receipt(A, "claude-delivery",
                                          "actual-pty-turn")["state"] == "accepted"
            # A retry of the exact original delivery returns the original
            # confirmed receipt, never another native send/PTY prompt.
            sends = []
            def forbidden_resend(*_args):
                sends.append(1)
                raise AssertionError("same-ID retry must not enter native IPC")
            repeat = send_turn(A, "real fake host prompt",
                               delivery_id="actual-pty-turn",
                               _native_submit=forbidden_resend)
            assert repeat["settled"] is positive_stop
            assert repeat["status"] == "accepted"
            assert sends == []
            assert (tmp_path / "fake-input.log").read_bytes().count(
                b"real fake host prompt"
            ) == 1

        finally:
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                    proc.wait(timeout=3)
                except (OSError, subprocess.TimeoutExpired):
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except OSError:
                        pass
                    proc.wait(timeout=4)
