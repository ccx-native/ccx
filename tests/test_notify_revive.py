"""Turn-end notifyd recovery, all on disposable roots and fake native clients."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon

import threading
import time

import pytest

from ccx.codex import control as codex_control
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.codex.daemon.daemon import CodexDaemon
from ccx.codex.daemon.worker import Worker
from test_generation_store import A, B, short_root
from test_notify_ext import FakeControl, _frozen_child, native_caller, originals


def test_codex_turn_end_revives_dead_notifyd_and_delivers_once(originals, monkeypatch):
    store, _trace, _ = originals
    parent = native_caller(monkeypatch)
    reg = _frozen_child(store, token="revive-codex-child", parent=parent)
    control = FakeControl()
    monkeypatch.setattr(codex_control, "CodexControlClient", lambda: control)

    alive = [False]  # fake killed notifyd; never launch a native daemon
    monkeypatch.setattr(notify_daemon, "_notifyd_alive", lambda: alive[0])
    finished = threading.Event()
    launches = []

    def fake_ensure():
        try:
            launches.append("revived")
            assert not notify_daemon._notifyd_alive()
            alive[0] = True
            notifyd.process_registration(parent_notify.read_registration(reg["eventId"]))
        finally:
            finished.set()

    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", fake_ensure)
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, store.record(B))
    tid = store.record(B)["native"]["threadId"]
    params = {"threadId": tid, "turn": {"id": "revival-trigger", "status": "completed"}}
    try:
        worker.on_notification("turn/started", {"threadId": tid, "turn": {"id": "revival-trigger"}})
        worker.on_notification("turn/completed", params)
        assert finished.wait(4), "turn end did not request notifyd recovery"
        assert launches == ["revived"]
        assert parent_notify.read_registration(reg["eventId"])["state"] == "delivered"
        assert len(control.placed) == 1
        assert control.placed[0][1]["deliveryId"] == reg["eventId"] + ":settled"
        worker.on_notification("turn/completed", params)  # duplicate native event
        assert launches == ["revived"]
        notifyd.process_registration(parent_notify.read_registration(reg["eventId"]))
        assert len(control.placed) == 1
        assert store.record(B)["stats"]["turns"] == 1
    finally:
        worker.evidence.close()


@pytest.mark.parametrize("engine,worker_id", [
    ("claude", A), ("codex", A), ("claude", B),
])
def test_turn_end_without_matching_registration_does_not_spawn(
    engine, worker_id, monkeypatch,
):
    # A pending wake for *another* full worker or engine is not this worker's.
    monkeypatch.setattr(notify_daemon, "iter_registrations", lambda: [
        {"worker": {"engine": "codex", "workerId": B}},
    ])
    calls = []
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda: calls.append(1))
    notify_daemon._revive_for_worker(engine, worker_id)
    assert calls == []
    monkeypatch.setattr(notify_daemon, "iter_registrations", lambda: [])
    notify_daemon._revive_for_worker(engine, worker_id)
    assert calls == []


def test_turn_end_off_loop_failure_is_bounded_and_never_blocks(monkeypatch, capsys):
    monkeypatch.setattr(notify_daemon, "_revive_warned", False)
    monkeypatch.setattr(notify_daemon, "iter_registrations", lambda: [
        {"worker": {"engine": "claude", "workerId": A}},
    ])
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    logged = threading.Event()
    original_log = notify_daemon._log_revive_failure

    def log_and_signal(error):
        original_log(error)
        logged.set()

    monkeypatch.setattr(notify_daemon, "_log_revive_failure", log_and_signal)

    def failing_ensure():
        entered.set()
        try:
            assert release.wait(4), "test did not release fake slow daemon startup"
            raise RuntimeError("simulated restart failure\n" + "x" * 1000)
        finally:
            finished.set()

    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", failing_ensure)
    start = time.monotonic()
    try:
        # This is the only call made by host.finalize_managed_turn: neither a
        # blocking registry read nor the up-to-5s daemon readiness wait is on PTY.
        notify_daemon.revive_notify_daemon_after_turn("claude", A)
        assert time.monotonic() - start < 0.5
        assert entered.wait(2)
        assert not finished.is_set(), "fake ensure should still be blocking"
    finally:
        release.set()
    assert finished.wait(2)
    assert logged.wait(2)
    # A second failure also cannot escape to the turn path or spam the log.
    notify_daemon._revive_for_worker("claude", A)
    output = capsys.readouterr().err
    assert output.count("turn-end revival failed") == 1
    assert "simulated restart failure" in output
    assert len(output) < 275


def test_codex_turn_completion_survives_revival_failure(originals, monkeypatch):
    store, _trace, _ = originals
    monkeypatch.setattr(notify_daemon, "_revive_warned", False)
    pending = {"worker": {"engine": "codex", "workerId": B}}
    monkeypatch.setattr(notify_daemon, "iter_registrations", lambda: [pending])
    checked = threading.Event()

    def fake_ensure():
        checked.set()
        raise RuntimeError("notifyd is unavailable")

    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", fake_ensure)
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, store.record(B))
    tid = store.record(B)["native"]["threadId"]
    try:
        worker.on_notification("turn/started", {"threadId": tid, "turn": {"id": "failed-revive"}})
        worker.on_notification("turn/completed", {"threadId": tid, "turn": {
            "id": "failed-revive", "status": "completed",
        }})
        assert checked.wait(2)
        assert store.record(B)["stats"]["turns"] == 1
        assert store.record(B)["lifecycle"]["state"] == "idle"
    finally:
        worker.evidence.close()
