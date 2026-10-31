"""Original full-UUID V1 persistent stdio transport, disposable ACTIVE roots only."""
from __future__ import annotations

import json
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from ccx import constants
from ccx.rpc import local_rpc
from ccx.rpc import serve as rpc_serve
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from test_generation_store import (
    A, G1, G2, short_root, switch_pointer,
)

pytestmark = pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded:DeprecationWarning"
)


def _line(rid: str, op: str, **params) -> bytes:
    return (json.dumps({
        "protocol_version": 2, "request_id": rid, "op": op,
        "params": params,
    }) + "\n").encode()


def _serve_lines(payload: bytes) -> list[dict]:
    """Real server/selector/children over finite original transport pipes."""
    input_read, input_write = os.pipe()
    output_read, output_write = os.pipe()
    before = signal.getsignal(signal.SIGPIPE)
    try:
        with os.fdopen(input_write, "wb", closefd=True) as stream:
            stream.write(payload)
        assert rpc_serve.serve(input_read, output_write) == 0
    finally:
        signal.signal(signal.SIGPIPE, before)
        os.close(input_read)
        os.close(output_write)
    with os.fdopen(output_read, "rb", closefd=True) as stream:
        return [json.loads(line) for line in stream if line.strip()]


@pytest.fixture
def owned_root(short_root, monkeypatch):
    ctx = require_ccx_active(short_root)
    prepare_worker(ctx, engine="claude", worker_id=A, cwd=str(short_root))
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    return short_root


def test_original_serve_request_isolation_order_duplicate_and_capability_truth(
    owned_root, monkeypatch,
):
    def held_wait(params, timeout):
        assert params["target"] == A
        time.sleep(.2)
        return {"worker": f"claude:{A}", "status": "idle"}
    monkeypatch.setattr(local_rpc, "_op_worker_wait", held_wait)
    replies = _serve_lines(
        _line("held", "worker.wait", target=A)
        + _line("inspect", "worker.inspect", target=A)
        + _line("caps", "system.info")
        + _line("held", "worker.wait", target=A)  # same ID before first answered
        + _line("unknown", "worker.unknown", target=A)
        + _line("prefix", "worker.inspect", target=A[:8])
    )
    assert len(replies) == 6, replies
    held = [row for row in replies if row["request_id"] == "held"]
    assert len(held) == 2 and {row["ok"] for row in held} == {False, True}
    assert held[0]["error"]["code"] == "refused"
    assert held[1]["result"]["worker"] == f"claude:{A}"
    assert replies.index(held[1]) > replies.index(next(
        row for row in replies if row["request_id"] == "caps"
    ))
    inspect = next(row for row in replies if row["request_id"] == "inspect")
    assert inspect["ok"] and inspect["result"]["worker"] == f"claude:{A}"
    caps = next(row for row in replies if row["request_id"] == "caps")
    assert caps["ok"] is True
    assert caps["result"]["protocol_version"] == 2
    assert set(caps["result"]) == {"protocol_version", "ccx_version"}
    assert next(row for row in replies if row["request_id"] == "unknown")[
        "error"]["code"] == "invalid_request"
    assert next(row for row in replies if row["request_id"] == "prefix")[
        "error"]["code"] == "invalid_request"
    assert not (owned_root / "sessions").exists()
    assert not (owned_root / "run").exists()


def test_request_cancel_kills_only_observation_and_never_reissues_native_mutation(
    owned_root, monkeypatch, tmp_path,
):
    effects = tmp_path / "original-effects.log"

    def waiting(params, timeout):
        time.sleep(15)
        return {"status": "idle"}

    def original_mutation(params, timeout):
        with effects.open("a", encoding="utf-8") as stream:
            stream.write(params["delivery_id"] + "\n")
        time.sleep(.2)
        return {"delivery_id": params["delivery_id"], "status": "accepted"}

    monkeypatch.setattr(local_rpc, "_op_worker_wait", waiting)
    monkeypatch.setattr(local_rpc, "_op_turn_send", original_mutation)
    started = time.monotonic()
    replies = _serve_lines(
        _line("held", "worker.wait", target=A)
        + _line("cancel-observe", "request.cancel", cancel_request_id="held")
        + _line("held", "system.info")  # freed ID/slot may be reused
        + _line("native", "turn.send", target=A, text="no provider",
                delivery_id="one-original-effect")
        + _line("cancel-native", "request.cancel", cancel_request_id="native")
        + _line("absent", "request.cancel", cancel_request_id="never-sent")
    )
    assert time.monotonic() - started < 8
    assert len(replies) == 6, replies
    cancelled = next(row for row in replies if row["request_id"] == "held"
                     and not row["ok"])
    assert cancelled["error"]["code"] == "cancelled"
    cancel = next(row for row in replies if row["request_id"] == "cancel-observe")
    assert replies.index(cancelled) < replies.index(cancel)
    assert cancel["result"] == {
        "cancel_request_id": "held", "cancelled": True, "reason": None,
    }
    assert any(row["request_id"] == "held" and row["ok"] for row in replies)
    assert next(row for row in replies if row["request_id"] == "cancel-native")[
        "result"] == {
            "cancel_request_id": "native", "cancelled": False,
            "reason": "not_cancellable",
        }
    assert next(row for row in replies if row["request_id"] == "absent")[
        "result"]["reason"] == "not_in_flight"
    assert next(row for row in replies if row["request_id"] == "native")[
        "result"]["delivery"]["delivery_id"] == "one-original-effect"
    assert effects.read_text().splitlines() == ["one-original-effect"]
    assert not (owned_root / "sessions").exists()


def test_server_does_not_adopt_next_active_generation_after_original_boot(
    owned_root, tmp_path,
):
    root = owned_root
    command = [sys.executable, "-B", "-m", "ccx", "rpc", "--serve"]
    env = dict(os.environ, CCX_ROOT=str(root),
               PYTHONPATH=str(Path(__file__).resolve().parent.parent))
    process = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, bufsize=0, env=env,
    )

    def exchange(request: bytes) -> dict:
        assert process.stdin is not None and process.stdout is not None
        process.stdin.write(request)
        process.stdin.flush()
        readable, _, _ = select.select([process.stdout.fileno()], [], [], 8)
        assert readable, "serve did not produce a bounded response"
        reply = process.stdout.readline()
        assert reply, "serve closed its output before a complete response"
        return json.loads(reply)

    try:
        # Boot completed on ACTIVE/G1 and the first worker view belongs to G1.
        previous = exchange(_line("before", "worker.inspect", target=A))
        assert previous["ok"] is True
        assert previous["result"]["worker"] == f"claude:{A}"
        original_record = (
            root / "identity" / "generations" / G1 / "workers"
            / "claude" / A / "record.json"
        )
        frozen = original_record.read_bytes()
        switch_pointer(root, G2)
        future = require_ccx_active(root)
        prepare_worker(future, engine="claude", worker_id=A, cwd=str(root))
        # Same full ID in G2 must NEVER be served by G1 parent.
        new_record = (
            root / "identity" / "generations" / G2 / "workers"
            / "claude" / A / "record.json"
        )
        frozen_new = new_record.read_bytes()
        stale = exchange(_line("later", "worker.inspect", target=A))
        assert stale["ok"] is False and stale["request_id"] == "later"
        assert stale["error"]["code"] == "unavailable", stale
        pure = exchange(_line("pure", "system.info"))
        assert pure["ok"] is True
        assert set(pure["result"]) == {"protocol_version", "ccx_version"}
        malformed = exchange(_line("bad", "worker.inspect", target=A[:8]))
        assert malformed["error"]["code"] == "unavailable"
        assert original_record.read_bytes() == frozen
        assert new_record.read_bytes() == frozen_new
    finally:
        if process.stdin is not None:
            process.stdin.close()
        try:
            code = process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
            raise AssertionError("serve did not drain and exit after EOF")
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            stderr = process.stderr.read().decode(errors="replace")
            process.stderr.close()
        else:
            stderr = ""
        assert code == 0, (code, stderr)
    assert not (root / "sessions").exists()
    assert not (root / "run").exists()
