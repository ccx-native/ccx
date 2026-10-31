from __future__ import annotations

import json
import os
import signal
import time

import pytest

from ccx.rpc import serve as S
from ccx import constants
from ccx.rpc.local_rpc import _serialized, _success, parse_request
from test_generation_store import fixture_root

# A real serve parent is single-threaded; under the full suite, threads other
# tests leave behind make Python warn on every fork.
pytestmark = pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded:DeprecationWarning"
)


@pytest.fixture(autouse=True)
def active_rpc_generation(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    monkeypatch.setenv("CCX_ROOT", str(root))
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    return root


def _line(request_id, *, timeout_ms=None, op="system.info", **params):
    value = {
        "protocol_version": 2,
        "request_id": request_id,
        "op": op,
        "params": params,
    }
    if timeout_ms is not None:
        value["timeout_ms"] = timeout_ms
    return (json.dumps(value) + "\n").encode()


def _serve(payload: bytes) -> list[dict]:
    """Run the server to completion over pipes; return responses in arrival order."""
    in_r, in_w = os.pipe()
    out_r, out_w = os.pipe()
    os.write(in_w, payload)
    os.close(in_w)
    previous_sigpipe = signal.getsignal(signal.SIGPIPE)
    try:
        assert S.serve(in_r, out_w) == 0
    finally:
        signal.signal(signal.SIGPIPE, previous_sigpipe)
        os.close(in_r)
        os.close(out_w)
    raw = b""
    while chunk := os.read(out_r, 65536):
        raw += chunk
    os.close(out_r)
    return [json.loads(line) for line in raw.splitlines()]


def _fake_respond(behaviour):
    """Replace the child's dispatch with ``behaviour(request) -> result | None``."""

    def respond(read, *, expected_generation):
        assert expected_generation == "g-" + "1" * 32
        request = read()
        result = behaviour(request)
        return _serialized(_success(request["request_id"], result or {}))

    return respond


def test_child_without_response_is_answered_as_internal_error(monkeypatch, tmp_path):
    marker = tmp_path / "no-response-dispatched"

    def exit_without_reply(read, *, expected_generation):
        marker.write_text(f"{read()['request_id']}:{expected_generation}")
        os._exit(3)

    monkeypatch.setattr(S, "respond", exit_without_reply)
    responses = _serve(_line("crash"))
    assert marker.read_text() == "crash:g-" + "1" * 32
    assert responses[0]["request_id"] == "crash"
    assert responses[0]["error"]["code"] == "invalid_request"


def test_failed_parent_death_arm_refuses_read_only_but_not_mutation(monkeypatch):
    """Only read-only children are fail-closed; mutation proceeds untouched."""
    monkeypatch.setattr(S.oslayer, "arm_parent_death", lambda parent: os._exit(0))
    monkeypatch.setattr(S, "respond", _fake_respond(
        lambda request: {"handled": request["op"]}))

    denied = _serve(_line("unarmed-read"))
    assert denied[0]["error"]["code"] == "invalid_request"

    mutation = _serve(_line("unarmed-write", op="worker.kill"))
    assert mutation[0]["result"] == {"handled": "worker.kill"}


def test_child_past_its_budget_is_killed_and_answered(monkeypatch):
    monkeypatch.setattr(S, "DEADLINE_GRACE_S", 0.1)
    monkeypatch.setattr(S, "respond", _fake_respond(lambda request: time.sleep(30)))
    started = time.monotonic()
    responses = _serve(_line("wedged", timeout_ms=100))
    assert time.monotonic() - started < 5
    assert responses[0]["request_id"] == "wedged"
    assert responses[0]["error"]["code"] == "incomplete"


def test_process_spawned_by_a_request_does_not_hold_the_response(monkeypatch):
    def spawn_and_answer(request):
        if os.fork() == 0:
            # Holds every inherited fd, the response pipe included.
            time.sleep(3)
            os._exit(0)
        return {"spawned": True}

    monkeypatch.setattr(S, "respond", _fake_respond(spawn_and_answer))
    started = time.monotonic()
    responses = _serve(_line("spawner"))
    assert time.monotonic() - started < 2
    assert responses[0]["result"] == {"spawned": True}


def test_child_streams_are_detached_from_the_protocol_stream(monkeypatch):
    def noisy(request):
        os.write(1, b"stray stdout\n")
        assert os.read(0, 10) == b""
        return {"quiet": True}

    monkeypatch.setattr(S, "respond", _fake_respond(noisy))
    responses = _serve(_line("noisy"))
    assert responses == [{
        "ok": True, "protocol_version": 2, "request_id": "noisy",
        "result": {"quiet": True},
    }]


def test_child_dying_silently_while_a_descendant_holds_its_pipe(monkeypatch, tmp_path):
    marker = tmp_path / "descendant-forked"

    def spawn_then_die(read, *, expected_generation):
        request_id = read()["request_id"]
        if os.fork() == 0:
            time.sleep(4)  # holds the response pipe open
            os._exit(0)
        # Only the original request child writes this, after its fork succeeded.
        marker.write_text(f"{request_id}:{expected_generation}")
        os._exit(3)

    monkeypatch.setattr(S, "respond", spawn_then_die)
    started = time.monotonic()
    responses = _serve(_line("orphaned-pipe", timeout_ms=30_000))
    assert marker.read_text() == "orphaned-pipe:g-" + "1" * 32
    assert time.monotonic() - started < 3, "answered on exit, not at the deadline"
    assert responses[0]["error"]["code"] == "invalid_request"


_PARENT_DEATH_SCRIPT = r"""
import os, sys, time
import ccx.rpc.serve as S
pid_dir = sys.argv[1]

def respond(read, *, expected_generation):
    assert expected_generation == "g-" + "1" * 32
    request = read()
    with open(os.path.join(pid_dir, request["request_id"]), "w") as f:
        f.write(str(os.getpid()))
    time.sleep(30)
    return b""

S.respond = respond
S.serve()
"""


def _alive(pid: int) -> bool:
    from ccx.system import oslayer

    # On both OS families, only kernel-proven absence or zombie state is dead.
    # A read-only child on Darwin is not dead just because /proc is absent.
    return oslayer.process_info(pid).presence != "dead"


@pytest.mark.skipif(not os.path.exists("/proc"), reason="Linux parent-death signal")
def test_read_only_operations_are_the_observing_ones():
    assert {"worker.wait", "turn.reconcile", "notification.inspect"} <= S.READ_ONLY_OPS
    assert not {"turn.send", "worker.create", "worker.compact", "input.answer",
                "turn.cancel", "worker.kill"} & S.READ_ONLY_OPS


def test_read_only_requests_die_with_the_serve_parent(tmp_path):
    import signal
    import subprocess
    import sys

    proc = subprocess.Popen(
        [sys.executable, "-c", _PARENT_DEATH_SCRIPT, str(tmp_path)],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
    )
    try:
        for rid, op in (("read", "worker.wait"), ("write", "turn.send")):
            proc.stdin.write((json.dumps({"protocol_version": 2, "request_id": rid,
                                          "op": op, "params": {}}) + "\n").encode())
        proc.stdin.flush()
        # The autouse generation fixture already places a directory in
        # tmp_path. Counting entries can stop after only the first request's
        # marker appears, before the second child has published its PID.
        def published_pid(name):
            try:
                pid = int((tmp_path / name).read_text())
            except (FileNotFoundError, ValueError):
                return None
            return pid if pid > 0 else None

        deadline = time.monotonic() + 10
        read_pid = write_pid = None
        while time.monotonic() < deadline:
            read_pid = published_pid("read")
            write_pid = published_pid("write")
            if read_pid is not None and write_pid is not None:
                break
            time.sleep(0.05)
        assert read_pid is not None and write_pid is not None, (
            "both request children must publish their PIDs", read_pid,
            write_pid, proc.poll())
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        deadline = time.monotonic() + 5
        while _alive(read_pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not _alive(read_pid), "a held read dies with its server"
        assert _alive(write_pid), "a mutation is left to finish"
    finally:
        for name in ("read", "write"):
            path = tmp_path / name
            if path.exists():
                try:
                    os.kill(int(path.read_text()), signal.SIGKILL)
                except ProcessLookupError:
                    pass
        if proc.poll() is None:
            proc.kill()


def test_regular_file_stdin_is_served_to_eof(monkeypatch, tmp_path):
    monkeypatch.setattr(S, "respond", _fake_respond(lambda request: {"ok": 1}))
    requests = tmp_path / "requests.jsonl"
    requests.write_bytes(_line("a") + _line("b"))
    out_r, out_w = os.pipe()
    previous_sigpipe = signal.getsignal(signal.SIGPIPE)
    with requests.open("rb") as stdin:
        try:
            assert S.serve(stdin.fileno(), out_w) == 0
        finally:
            signal.signal(signal.SIGPIPE, previous_sigpipe)
            os.close(out_w)
    raw = b""
    while chunk := os.read(out_r, 65536):
        raw += chunk
    os.close(out_r)
    assert sorted(json.loads(line)["request_id"] for line in raw.splitlines()) == ["a", "b"]
