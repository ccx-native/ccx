from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import json
import os
from pathlib import Path
import pytest
from ccx import constants
from ccx.system import oslayer
from ccx.notify import registry as N
from ccx.codex import notify_history as NH
from ccx.notify.paths import daemon_generation_path, ensure_dirs
from test_generation_store import G1, short_root  # noqa: F401 - fixture


def _stamp_world(monkeypatch, short_root):
    monkeypatch.setenv("CCX_ROOT", str(short_root))
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    ensure_dirs()
    start = oslayer.process_info(os.getpid()).start_token
    assert start
    return os.getpid(), start


def test_notifyd_spawn_uses_current_package_as_process_cwd(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("CCX_ROOT", str(tmp_path))
    states = iter([False, False, True])
    monkeypatch.setattr(notify_daemon, "_notifyd_alive", lambda: next(states))
    monkeypatch.setattr(notify_daemon, "_retire_incompatible_notifyd", lambda **kwargs: None)
    captured = {}

    class Process:
        def wait(self):
            return 0

        def poll(self):
            return None

    def popen(argv, **kwargs):
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return Process()

    monkeypatch.setattr(notify_daemon.subprocess, "Popen", popen)

    notify_daemon.ensure_notify_daemon(timeout=1)

    import ccx
    # The directory holding the ccx package: never the package itself, whose
    # types.py would shadow the stdlib module for `python -m`.
    expected = str(Path(ccx.__file__).resolve().parents[1])
    assert captured["argv"][-2:] == ["-m", "ccx.notify"]
    assert captured["kwargs"]["cwd"] == expected
    assert captured["kwargs"]["env"]["PYTHONPATH"].split(os.pathsep)[0] == expected


def test_notification_reconciliation_pages_native_items_newest_first():
    delivery = "event-paged:settled"
    calls = []
    pages = [
        {"data": [{"turnId": "new", "item": {"type": "agentMessage", "text": "new"}}], "nextCursor": "cursor-2"},
        {"data": [{"turnId": "older", "item": {
            "type": "functionCallOutput", "name": "worker_notification",
            "output": json.dumps({"eventId": "event-paged", "deliveryId": delivery}),
        }}], "nextCursor": "cursor-3"},
    ]
    def request(method, params, timeout=30):
        calls.append((method, dict(params), timeout))
        return pages.pop(0)
    assert NH.notification_delivery_match(request, "parent", delivery) is not None
    assert [method for method, _, _ in calls] == ["thread/items/list", "thread/items/list"]
    assert calls[0][1]["sortDirection"] == "desc"
    assert calls[1][1]["cursor"] == "cursor-2"


def test_notification_reconciliation_timeout_never_falls_back_to_full_thread_read():
    calls = []
    def request(method, params, timeout=30):
        calls.append(method)
        if method == "thread/items/list":
            raise TimeoutError("thread/items/list timed out")
        raise AssertionError("thread/read fallback must not run after a paginated timeout")
    with pytest.raises(TimeoutError):
        NH.notification_delivery_match(request, "parent", "event:settled")
    assert calls == ["thread/items/list"]



@pytest.mark.parametrize("case", [
    "current", "old", "dead", "reused", "unprovable", "wrong-active", "missing", "malformed",
])
def test_notifyd_single_stamp_presence_and_retirement(short_root, monkeypatch, case):
    """Current owner stays; prior protocol retires; stale/unknown never signals."""
    pid, start = _stamp_world(monkeypatch, short_root)
    stamp = daemon_generation_path()
    if case == "dead":
        pid = 900_000_001
    if case == "reused":
        start = "1"
    version = constants.NOTIFYD_GENERATION + (case == "old")
    generation = G1 if case != "wrong-active" else "g-" + "2" * 32
    if case == "malformed":
        stamp.write_text(f"{pid} {version}\n")
    elif case != "missing":
        stamp.write_text(f"{pid} {version} {generation} {start}\n")
    original = oslayer.process_info
    sent = []
    def process_info(observed):
        if case == "unprovable" or sent:
            presence = "unprovable" if case == "unprovable" else "dead"
            return oslayer.ProcessInfo(observed, presence, None, None, None)
        return original(observed)
    monkeypatch.setattr(oslayer, "process_info", process_info)
    monkeypatch.setattr(N.os, "kill", lambda target, signal: sent.append((target, signal)))
    real_flock = N.fcntl.flock

    def only_spawn_lock(lock, flags):
        # Health and retirement never probe the daemon role lock. The spawn
        # lock still serializes competing startup attempts.
        assert not str(lock.name).endswith("notifyd.lock")
        return real_flock(lock, flags)
    monkeypatch.setattr(N.fcntl, "flock", only_spawn_lock)
    assert notify_daemon._notifyd_alive() is (case == "current")
    if case in ("unprovable", "wrong-active"):
        with pytest.raises(N.SessionUnavailable):
            notify_daemon._retire_incompatible_notifyd(timeout=.3)
    else:
        assert notify_daemon._retire_incompatible_notifyd(timeout=.3) is (case == "current")
    assert sent == ([(pid, notify_daemon.signal.SIGTERM)] if case == "old" else [])
    if case in ("dead", "missing", "malformed", "reused"):
        launched = []

        class Child:
            def poll(self):
                return None

            def wait(self):
                return 0
        monkeypatch.setattr(notify_daemon.subprocess, "Popen",
                            lambda *a, **kw: launched.append((a, kw)) or Child())
        monkeypatch.setattr(notify_daemon, "_notifyd_alive", lambda: bool(launched))
        notify_daemon.ensure_notify_daemon(timeout=.5)
        assert len(launched) == 1 and not stamp.exists() and not sent
