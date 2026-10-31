"""Read-only runtime presence on held event pages, follow heartbeats and V2.

Fake runtimes hold the REAL lifetime leases (``host._host_process_lease`` and
``transport.daemon_process_lease``) and nothing else: no provider, host loop,
app-server or socket. Each test kills only the exact PID it started.

Tests that observe real kernel leases are Linux-only (``/proc/locks``); the
parser, the unobservable fallback (Darwin), grace and source-integrity tests
are portable.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from ccx import constants
from ccx.rpc import local_rpc
from ccx.system import oslayer
from ccx.store import runtime_presence
from ccx.ops import worker_events
from ccx.store.root import require_ccx_active
from ccx.store.liveness import proc_start_token
from test_generation_store import (  # noqa: F401 (short_root is a fixture)
    A, B, C, G2, add_worker, put, short_root, switch_pointer,
)
from test_worker_events import _append, _event, _journal
from test_worker_events_cli import _environment, _follow_process

WT = Path(__file__).resolve().parent.parent
linux_locks = pytest.mark.skipif(
    sys.platform != "linux", reason="real lease observation reads Linux /proc/locks")

_CLAUDE_HOST = '\nimport sys, time\nfrom ccx.store.root import require_ccx_active\nfrom ccx.claude.host.session import _host_process_lease\nwith _host_process_lease(require_ccx_active(sys.argv[1]), sys.argv[2]):\n    print("ready", flush=True)\n    time.sleep(120)\n'

_CODEXD = '\nimport sys, time\nfrom ccx.store.root import require_ccx_active\nfrom ccx.rpc.transport import daemon_process_lease, ensure_transport_parent, plan_transport\nstore = require_ccx_active(sys.argv[1])\nplan = plan_transport(store, role="codexd")\nensure_transport_parent(store, plan)\nwith daemon_process_lease(store, plan):\n    print("ready", flush=True)\n    time.sleep(120)\n'


class FakeRuntimes:
    """Start lease-only fake runtimes; kill and reap exactly those PIDs."""

    def __init__(self, root: Path):
        self.root = root
        self.started: list[subprocess.Popen] = []

    def start(self, script: str, *args: str) -> subprocess.Popen:
        env = {"PATH": os.defpath, "PYTHONPATH": str(WT), "CCX_ROOT": str(self.root),
               "HOME": str(self.root.parent), "PYTHONDONTWRITEBYTECODE": "1"}
        proc = subprocess.Popen(
            [sys.executable, "-B", "-c", script, str(self.root), *args],
            env=env, stdout=subprocess.PIPE, text=True,
        )
        self.started.append(proc)
        assert proc.stdout.readline().strip() == "ready"
        return proc

    def claude(self, wid: str = A, state: str = "busy") -> subprocess.Popen:
        proc = self.start(_CLAUDE_HOST, wid)
        set_lifecycle(self.root, wid, state=state, pid=proc.pid)
        return proc

    def codexd(self) -> subprocess.Popen:
        return self.start(_CODEXD)

    @staticmethod
    def kill(proc: subprocess.Popen) -> None:
        proc.kill()
        proc.wait(timeout=5)

    def close(self) -> None:
        for proc in self.started:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)
            proc.stdout.close()


def set_lifecycle(root: Path, wid: str, *, state: str, pid: int | None,
                  engine: str = "claude") -> None:
    store = require_ccx_active(root)
    path = store.pointer.generation_root / "workers" / engine / wid / "record.json"
    rec = json.loads(path.read_text())
    rec["lifecycle"] = {"state": state, "since": 1, "hostPid": pid,
                        "hostStart": proc_start_token(pid) if pid else None}
    put(path, rec)


@pytest.fixture
def owned(short_root, monkeypatch):
    add_worker(short_root, A, confirmed=True)
    add_worker(short_root, B, "codex", confirmed=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    runtimes = FakeRuntimes(short_root)
    try:
        yield short_root, require_ccx_active(short_root), runtimes
    finally:
        runtimes.close()


def held(store, target, *, wait=1.0, **kwargs):
    start = time.monotonic()
    page = worker_events.wait_full_uuid_event_page(store, target, wait=wait, **kwargs)
    return page, time.monotonic() - start


@linux_locks
def test_live_claude_host_is_live_and_never_gone(owned):
    root, store, runtimes = owned
    runtimes.claude()
    seen = set()
    for _ in range(3):
        page, _elapsed = held(store, f"claude:{A}", wait=0.8, tail=0)
        seen.add(page["runtime"])
        assert page["timed_out"] is True and page["state"] == "busy"
    assert seen == {"live"}
    assert worker_events.wait_full_uuid_event_page(store, f"claude:{A}")["runtime"] == "live"


@linux_locks
def test_abrupt_kill_is_gone_within_the_held_read_then_revival_is_live(owned):
    root, store, runtimes = owned
    host = runtimes.claude()
    cursor = worker_events.read_full_uuid_event_page(store, f"claude:{A}", tail=0)["next_cursor"]
    import threading
    threading.Timer(0.5, runtimes.kill, args=(host,)).start()
    page, elapsed = held(store, f"claude:{A}", wait=10.0, after=cursor)
    # Seen live, then gone for the 0.35 s grace: returned long before 10 s.
    assert page["runtime"] == "gone"
    assert page["events"] == [] and page["timed_out"] is False
    assert elapsed < 0.5 + runtime_presence.HOST_MISSING_GRACE_S + 1.0
    assert page["next_cursor"] == cursor and page["gap"] is None
    assert page["state"] == "busy"  # Recorded state untouched: not a settlement.

    # Reconnect: a fresh subscriber sees it on its first held page.
    again, elapsed = held(store, f"claude:{A}", wait=1.0, after=cursor)
    assert again["runtime"] == "gone" and again["timed_out"] is True
    assert elapsed >= 1.0

    runtimes.claude(state="busy")
    revived, _ = held(store, f"claude:{A}", wait=0.5, after=cursor)
    assert revived["runtime"] == "live"


@linux_locks
def test_startup_grace_and_unproven_absence_are_unknown(owned):
    root, store, runtimes = owned
    # Never started in this generation and booting: the 5 s grace holds.
    set_lifecycle(root, A, state="booting", pid=None)
    page, _ = held(store, f"claude:{A}", wait=1.0, tail=0)
    assert page["runtime"] == "unknown"
    # Free lease while the recorded host process is alive (a start before its
    # lease, or a lease this reader cannot see): never gone.
    set_lifecycle(root, A, state="busy", pid=os.getpid())
    page, _ = held(store, f"claude:{A}", wait=1.0, tail=0)
    assert page["runtime"] == "unknown"
    # One unheld sample cannot cover the grace.
    set_lifecycle(root, A, state="busy", pid=None)
    assert worker_events.wait_full_uuid_event_page(
        store, f"claude:{A}", tail=0)["runtime"] == "unknown"
    page, _ = held(store, f"claude:{A}", wait=0.6, tail=0)
    assert page["runtime"] == "gone"


@linux_locks
def test_codexd_lease_live_gone_and_unreadable_lock_table(owned, monkeypatch):
    root, store, runtimes = owned
    daemon = runtimes.codexd()
    page, _ = held(store, f"codex:{B}", wait=0.6, tail=0)
    assert page["runtime"] == "live"
    runtimes.kill(daemon)
    page, _ = held(store, f"codex:{B}", wait=0.6, tail=0)
    assert page["runtime"] == "gone"
    runtimes.codexd()
    monkeypatch.setattr(oslayer, "PROC_LOCKS", str(root / "no-such-lock-table"))
    page, _ = held(store, f"codex:{B}", wait=0.6, tail=0)
    assert page["runtime"] == "unknown"


@linux_locks
def test_lease_identity_is_exact_worker_and_generation(owned):
    root, store, runtimes = owned
    # Another Claude worker's host is not this worker's runtime.
    add_worker(root, C, confirmed=True)
    runtimes.claude(wid=C)
    set_lifecycle(root, A, state="busy", pid=None)
    page, _ = held(store, f"claude:{A}", wait=0.6, tail=0)
    assert page["runtime"] == "gone"
    # A lease held in the old generation is not the new generation's runtime.
    runtimes.claude()
    switch_pointer(root, G2)
    add_worker(root, A, confirmed=True)
    fresh = require_ccx_active(root)
    page, _ = held(fresh, f"claude:{A}", wait=0.6, tail=0)
    assert page["runtime"] == "gone"


def test_proc_locks_parser_skips_waiters_and_other_kinds(tmp_path, monkeypatch):
    table = tmp_path / "locks"
    table.write_text(
        "1: FLOCK  ADVISORY  WRITE 10 08:30:4224972 0 EOF\n"
        "1: -> FLOCK  ADVISORY  WRITE 11 08:30:99 0 EOF\n"
        "2: POSIX  ADVISORY  WRITE 12 08:30:77 0 EOF\n"
        "3: FLOCK  ADVISORY  READ 13 fd:01:55 0 EOF\n"
        "garbage\n"
    )
    monkeypatch.setattr(oslayer, "PROC_LOCKS", str(table))
    assert oslayer._linux_held_flock_inodes() == {(8, 0x30, 4224972), (0xfd, 1, 55)}
    monkeypatch.setattr(oslayer, "PROC_LOCKS", str(tmp_path / "absent"))
    assert oslayer._linux_held_flock_inodes() is None
    assert oslayer._darwin_held_flock_inodes() is None


@linux_locks
def test_presence_is_read_only_no_locks_writes_calls_or_spawns(owned, monkeypatch):
    root, store, runtimes = owned
    host = runtimes.claude()
    _append(_journal(root), _event("E1", 1))

    def snapshot():
        return {str(p): (p.stat().st_mtime_ns, p.stat().st_size,
                         p.read_bytes() if p.is_file() else None)
                for p in sorted(root.rglob("*")) if not p.is_symlink()}

    before = snapshot()
    import fcntl
    import socket
    from ccx.codex import runtime as codex_runtime
    from ccx.ops import common as protocol_ops

    def forbidden(*_args, **_kwargs):
        raise AssertionError("presence must not lock, spawn or call a runtime")

    native_flock = fcntl.flock

    def no_lease_lock(fd, operation):
        # The store's own short leases are ordinary; a lifetime lease is not.
        number = fd if isinstance(fd, int) else fd.fileno()
        if os.readlink(oslayer.pinned_fd_path(number)).endswith(".lease.lock"):
            raise AssertionError("presence must never take a runtime lifetime lease")
        return native_flock(fd, operation)

    monkeypatch.setattr(fcntl, "flock", no_lease_lock)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(os, "fork", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(codex_runtime, "daemon_rpc", forbidden)
    monkeypatch.setattr("ccx.ops.wait.claude_host_rpc", forbidden)
    first = worker_events.read_full_uuid_event_page(store, f"claude:{A}", tail=5)
    page, _ = held(store, f"claude:{A}", wait=0.5, after=first["next_cursor"])
    assert page["runtime"] == "live"
    monkeypatch.undo()
    runtimes.kill(host)
    monkeypatch.setattr(fcntl, "flock", no_lease_lock)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    page, _ = held(store, f"claude:{A}", wait=0.6, after=first["next_cursor"])
    assert page["runtime"] == "gone"
    monkeypatch.undo()
    assert snapshot() == before


@linux_locks
def test_cursor_replay_and_page_shape_are_unchanged_but_additive(owned):
    root, store, runtimes = owned
    runtimes.claude()
    path = _journal(root)
    start = worker_events.read_full_uuid_event_page(store, f"claude:{A}", tail=0)
    _append(path, _event("E1", 1), _event("E1", 2), _event("E1", 3))
    raw = worker_events.read_full_uuid_event_page(
        store, f"claude:{A}", after=start["next_cursor"], limit=2)
    page, _ = held(store, f"claude:{A}", after=start["next_cursor"], limit=2)
    assert set(page) - set(raw) == {"state", "native_state", "state_source",
                                    "timed_out", "runtime"}
    for key in raw:
        assert page[key] == raw[key]
    rest, _ = held(store, f"claude:{A}", after=page["next_cursor"])
    assert [(e["epoch"], e["seq"]) for e in rest["events"]] == [("E1", 3)]
    assert rest["runtime"] == "live" and rest["timed_out"] is False


@linux_locks
def test_v2_worker_events_projects_runtime_on_held_timeouts(owned):
    root, store, runtimes = owned
    host = runtimes.claude()

    def call(wait_ms):
        return local_rpc.dispatch({
            "protocol_version": 2, "request_id": f"presence-{wait_ms}-{time.time()}",
            "op": "worker.events", "timeout_ms": 5000,
            "params": {"target": f"claude:{A}", "wait_ms": wait_ms, "tail": 0},
        })

    live = call(500)
    assert live["ok"] is True, live
    assert live["result"]["runtime"] == "live" and live["result"]["timed_out"] is True
    runtimes.kill(host)
    gone = call(600)
    assert gone["result"]["runtime"] == "gone" and gone["result"]["timed_out"] is True
    assert gone["result"]["events"] == []
    assert call(0)["result"]["runtime"] == "unknown"


@linux_locks
def test_cli_snapshot_and_follow_heartbeats_carry_runtime(owned):
    root, store, runtimes = owned
    host = runtimes.claude()
    snap = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "events", A, "--json", "--tail", "0"],
        cwd=root.parent, env=_environment(root), capture_output=True, text=True, timeout=15,
    )
    assert snap.returncode == 0, snap.stderr
    assert json.loads(snap.stdout)["runtime"] == "live"
    with _follow_process(root, A, "--follow", "--json", "--tail", "0",
                         "--heartbeat", "5", "--timeout", "4") as follow:
        first = json.loads(follow.stdout.readline())
        assert first["kind"] == "heartbeat" and first["runtime"] == "live"
        runtimes.kill(host)
        # The change is a heartbeat at once, not after the 5 s period.
        started = time.monotonic()
        second = json.loads(follow.stdout.readline())
        assert second["kind"] == "heartbeat" and second["runtime"] == "gone"
        assert time.monotonic() - started < 3.0
        assert second["state"] == "busy" and second["cursor"] == first["cursor"]


# -- source integrity before presence -----------------------------------------

def _gone_with_source_change(monkeypatch, mutate):
    """Lease held at entry; on the 2nd poll the source changes and the lease
    is free past the grace, so the gone transition and the source change are
    visible in the SAME held-loop iteration. Fully deterministic: the fake
    observer clock only moves when the held loop sleeps."""
    import types
    now = [0.0]
    polls = [0]
    samples = iter(["held"])
    monkeypatch.setattr(runtime_presence, "lease",
                        lambda *_a, **_k: next(samples, "free"))

    class Observer(runtime_presence.RuntimeObserver):
        def __init__(self, store, target):
            super().__init__(store, target, clock=lambda: now[0])

    monkeypatch.setattr(runtime_presence, "RuntimeObserver", Observer)

    def sleep(_seconds):
        polls[0] += 1
        now[0] += 1.0  # Past any grace on every poll.
        if polls[0] == 2:
            mutate()

    monkeypatch.setattr(worker_events, "time", types.SimpleNamespace(
        monotonic=time.monotonic, time=time.time, sleep=sleep))
    return polls


@pytest.mark.parametrize("change", ["loose_mode", "symlink"])
def test_unsafe_journal_replacement_still_refuses_when_runtime_goes(
        owned, monkeypatch, change):
    root, store, _runtimes = owned
    path = _journal(root)
    _append(path, _event("E1", 1))
    cursor = worker_events.read_full_uuid_event_page(store, f"claude:{A}", tail=0)["next_cursor"]

    def mutate():
        if change == "loose_mode":
            path.chmod(0o644)
        else:
            target = path.with_suffix(".elsewhere")
            target.write_bytes(path.read_bytes())
            target.chmod(0o600)
            path.unlink()
            path.symlink_to(target)

    _gone_with_source_change(monkeypatch, mutate)
    with pytest.raises(worker_events.StorageIntegrityError):
        worker_events.wait_full_uuid_event_page(store, f"claude:{A}", after=cursor, wait=30)


def test_truncated_or_rotated_journal_reports_its_gap_when_runtime_goes(owned, monkeypatch):
    root, store, _runtimes = owned
    path = _journal(root)
    _append(path, _event("E1", 1), _event("E1", 2))
    cursor = worker_events.read_full_uuid_event_page(store, f"claude:{A}", tail=0)["next_cursor"]

    def mutate():
        path.write_text(json.dumps(_event("E9", 1)) + "\n")
        path.chmod(0o600)

    _gone_with_source_change(monkeypatch, mutate)
    page = worker_events.wait_full_uuid_event_page(store, f"claude:{A}", after=cursor, wait=30)
    # Never an apparently valid empty page at the stale cursor.
    assert page["gap"]["reason"] == "cursor_not_retained"
    assert [(e["epoch"], e["seq"]) for e in page["events"]] == [("E9", 1)]
    assert page["next_cursor"] != cursor


# -- unobservable kernel lock table (Darwin, or an unreadable /proc/locks) -----

def test_no_lock_table_is_unknown_even_without_a_lease_file(owned, monkeypatch):
    root, store, _runtimes = owned
    monkeypatch.setattr(oslayer, "held_flock_inodes", lambda: None)
    for target, engine, wid in ((f"claude:{A}", "claude", A), (f"codex:{B}", "codex", B)):
        # Never started here: no lease file, no recorded process.
        assert runtime_presence._lease_file(store, engine, wid) is None
        if engine == "claude":
            set_lifecycle(root, A, state="busy", pid=None)
        assert runtime_presence.lease(store, engine, wid, store.record(target)) == "unknown"
        page, _ = held(store, target, wait=0.6, tail=0)
        assert page["runtime"] == "unknown"


# -- startup grace across its boundary, on a fake clock ------------------------

@pytest.mark.parametrize("state,grace", [
    ("busy", runtime_presence.HOST_MISSING_GRACE_S),
    ("booting", runtime_presence.BOOTING_GRACE_S),
    ("resuming", runtime_presence.BOOTING_GRACE_S),
])
def test_grace_boundary_on_a_fake_clock(owned, monkeypatch, state, grace):
    root, store, _runtimes = owned
    set_lifecycle(root, A, state=state, pid=None)
    leases = iter(["free", "free", "free", "held", "free", "free"])
    monkeypatch.setattr(runtime_presence, "lease", lambda *_a, **_k: next(leases))
    now = [0.0]
    observer = runtime_presence.RuntimeObserver(store, f"claude:{A}", clock=lambda: now[0])
    assert observer.sample() == "unknown"      # free since 0
    now[0] = grace - 0.001
    assert observer.sample() == "unknown"      # just inside the grace
    now[0] = grace
    assert observer.sample() == "gone"         # exactly at the boundary
    assert observer.sample() == "live"         # a held lease resets the window
    now[0] = 2 * grace
    assert observer.sample() == "unknown"      # a new window starts at the next free
    now[0] = 2 * grace + grace + 0.001         # (the exact boundary is checked above)
    assert observer.sample() == "gone"


# -- existing refusals are unchanged by the observer wiring --------------------

class _NoPresence:
    """The base behavior: no observer at all."""

    def __init__(self, _store, _target):
        self.value, self.seen_live = None, False

    def sample(self):
        return None


def _refusal(monkeypatch, store, target, observer_cls, *, before=None, during=None, **kwargs):
    import types
    with monkeypatch.context() as patch:
        patch.setattr(runtime_presence, "RuntimeObserver", observer_cls)
        patch.setattr(runtime_presence, "lease", lambda *_a, **_k: "held")
        if during is not None:
            calls = [0]

            def sleep(seconds):
                calls[0] += 1
                if calls[0] == 1:
                    during()
                time.sleep(min(seconds, 0.01))

            patch.setattr(worker_events, "time", types.SimpleNamespace(
                monotonic=time.monotonic, time=time.time, sleep=sleep))
        try:
            worker_events.wait_full_uuid_event_page(store, target, **kwargs)
        except Exception as exc:  # noqa: BLE001 - the refusal is the result
            return type(exc), str(exc)
    return None


@pytest.mark.parametrize("case", [
    "unknown_worker", "foreign_worker_cursor", "legacy_cursor",
    "stale_generation_store", "generation_switch_during_hold", "symlinked_journal",
])
def test_source_root_generation_worker_refusals_unchanged(owned, monkeypatch, tmp_path, case):
    results = []
    for observer_cls in (_NoPresence, runtime_presence.RuntimeObserver):
        root = tmp_path / observer_cls.__name__ / "r"
        root.parent.mkdir()
        from test_generation_store import fixture_root
        root = fixture_root(root.parent)
        add_worker(root, A, confirmed=True)
        add_worker(root, B, "codex", confirmed=True)
        monkeypatch.setattr(constants, "CCX_ROOT", root)
        store = require_ccx_active(root)
        _append(_journal(root), _event("E1", 1))
        cursor_a = worker_events.read_full_uuid_event_page(store, f"claude:{A}", tail=0)["next_cursor"]
        cursor_b = worker_events.read_full_uuid_event_page(store, f"codex:{B}", tail=0)["next_cursor"]
        target, kwargs, before, during = f"claude:{A}", {"wait": 0.3}, None, None
        if case == "unknown_worker":
            target, kwargs = f"claude:{C}", {"wait": 0.3, "tail": 0}
        elif case == "foreign_worker_cursor":
            kwargs["after"] = cursor_b
        elif case == "legacy_cursor":
            kwargs["after"] = "ev1.legacy"
        elif case == "stale_generation_store":
            switch_pointer(root, G2)
            add_worker(root, A, confirmed=True)
            kwargs["after"] = cursor_a
        elif case == "generation_switch_during_hold":
            kwargs["after"] = cursor_a

            def during():
                switch_pointer(root, G2)
                add_worker(root, A, confirmed=True)
        else:
            path = _journal(root)
            other = path.with_suffix(".elsewhere")
            other.write_bytes(path.read_bytes())
            other.chmod(0o600)
            path.unlink()
            path.symlink_to(other)
            kwargs["after"] = cursor_a
        outcome = _refusal(monkeypatch, store, target, observer_cls, during=during, **kwargs)
        results.append(outcome)
    base, wired = results
    assert base is not None, f"{case} is expected to refuse"
    # Same exception type; messages may embed this run's paths/ids.
    assert wired is not None and wired[0] is base[0], (case, base, wired)
