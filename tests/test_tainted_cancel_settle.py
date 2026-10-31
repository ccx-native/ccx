"""Original Claude host/PTY red regression for an ESC during subagent taint.

Only a disposable fake Claude emits hooks and native session status. No real
provider, user profile, external daemon or unrelated worker is involved.
"""
from __future__ import annotations
import ccx.claude.host.successor as H_successor

import json
from pathlib import Path
import shlex
import sys
import time
from types import SimpleNamespace

import pytest

from ccx import CcxClient, constants
from ccx.claude.host import session as H
from ccx.store.root import require_ccx_active
from ccx.rpc.rpc import _rpc
from test_claude_native_cli import _stop_disposable_original
from test_cancel_core import (
    _native_escape_count, _provider_native_identity, _request, _wait_ref,
)
from test_create_cross_surface import _first_wait_original_claude_ready
from test_generation_store import A, short_root


def _tainted_proof():
    """Original target ID and three strictly ordered native time fences."""
    state = {"current": H.S.BUSY, "active_turn_ref": "original-ref",
             "subagent_taint": {}}
    obs = {"turn_ref": "original-ref", "settling": False,
           "interrupt_at_ms": 3000}
    return state, obs, SimpleNamespace(status="idle", status_updated_at=3001)


def test_passive_native_idle_backstop_allows_parent_waiting_but_never_stale_or_busy():
    state, obs, native = _tainted_proof()
    state["current"] = H.S.WAITING
    obs["passive"] = True
    assert H_successor.interrupt_native_idle_after_escape(state, obs, native)
    obs["passive"] = False
    assert not H_successor.interrupt_native_idle_after_escape(state, obs, native)
    obs["passive"] = True
    native.status = "busy"
    assert not H_successor.interrupt_native_idle_after_escape(state, obs, native)
    native.status = "idle"
    native.status_updated_at = obs["interrupt_at_ms"]
    assert not H_successor.interrupt_native_idle_after_escape(state, obs, native)


def test_native_idle_after_escape_needs_only_exact_target_and_strict_esc_time():
    state, obs, native = _tainted_proof()
    # An agent can still be tainted and a previous parent BUSY unobservable:
    # an exact native IDLE after ESC is enough for *all* session work.
    state["subagent_taint"] = {"recent-agent": 5000}
    native.status_updated_at = obs["interrupt_at_ms"]
    assert H_successor.interrupt_native_idle_after_escape(state, obs, native) is False
    native.status_updated_at += 1
    assert H_successor.interrupt_native_idle_after_escape(state, obs, native) is True
    native.status = "shell"
    assert H_successor.interrupt_native_idle_after_escape(state, obs, native) is True
    # A successor cannot inherit an old cancellation observation.
    state["active_turn_ref"] = "new-successor-ref"
    assert H_successor.interrupt_native_idle_after_escape(state, obs, native) is False


@pytest.mark.parametrize("case", [
    "quiescent-no-stop", "marker-first", "late-stop", "untainted-control",
])
def test_tainted_escape_recovers_exact_turn_on_fresh_post_escape_native_idle(
    short_root, tmp_path, monkeypatch, case,
):
    """Only exact original native evidence settles; Stop/marker always win."""
    repo = Path(__file__).resolve().parent.parent
    original = (repo / "tests" / "fake_claude.py").read_text()
    anchor = '        hook("UserPromptSubmit", {"prompt": text})\n        native_row('
    assert original.count(anchor) == 1
    fake = tmp_path / "tainted-cancel-fake-claude.py"
    fake.write_text(original.replace(anchor, (
        '        hook("UserPromptSubmit", {"prompt": text})\n'
        '        if text.startswith("TAINT_CANCEL"):\n'
        '            hook("SubagentStop", {"agent_id": "original-late-subagent"})\n'
        '        if "NATIVE_STOP" in text:\n'
        '            until = time.monotonic() + 20\n'
        '            while not Path(os.environ["FAKE_TEST_STOP_GATE"]).exists() and time.monotonic() < until:\n'
        '                time.sleep(0.025)\n'
        '            hook("Stop", {"session_id": SESSION_ID,\n'
        '                          "last_assistant_message": "fake original native Stop"})\n'
        '        native_row('
    ), 1))
    wrapper = tmp_path / "tainted-cancel-claude"
    wrapper.write_text(
        "#!/bin/sh\nexec " + shlex.quote(sys.executable) + " "
        + shlex.quote(str(fake)) + ' "$@"\n'
    )
    wrapper.chmod(0o700)
    provider_home = tmp_path / "isolated-original-provider-home"
    provider_home.mkdir(mode=0o700)
    stop_gate = tmp_path / "release-original-native-stop"
    for key, value in {
        "CCX_ROOT": str(short_root), "CCX_CLAUDE_BIN": str(wrapper),
        "CCX_SPAWN_SCOPE": "0",
        "CCX_BOOT_TIMEOUT_S": "15", "PYTHONPATH": str(repo),
        "CLAUDE_CONFIG_DIR": str(provider_home),
        "FAKE_LOG": str(tmp_path / "owned-pty-writes.log"),
        "FAKE_MODE": "nostop",
        "FAKE_TEST_STOP_GATE": str(stop_gate),
    }.items():
        monkeypatch.setenv(key, value)
    for key in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(wrapper))
    store = require_ccx_active(short_root)
    handle = f"claude:{A}"
    events_path = (store.pointer.generation_root / "workers" / "claude"
                   / A / "events.ndjson")

    def events():
        if not events_path.exists():
            return []
        return [json.loads(line) for line in events_path.read_text().splitlines()]

    def wait_event(kind, *, timeout=9):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            found = next((e for e in events() if e.get("type") == kind), None)
            if found:
                return found
            time.sleep(.04)
        raise AssertionError((kind, [e.get("type") for e in events()]))

    def host_status():
        return _rpc(handle, {"op": "status"}, timeout=2)

    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        prompt = ("CLEAN_CANCEL no taint original" if case == "untainted-control"
                  else "TAINT_CANCEL original NATIVE_STOP" if case == "late-stop"
                  else "TAINT_CANCEL no Stop nor native marker")
        first = CcxClient().new(prompt,
                                cwd=str(tmp_path), worker_id=A,
                                wait=False, start_timeout=4)
        assert first.worker_id == A
        ref = _wait_ref(handle)
        assert wait_event("turn_start")["turn_ref"] == ref
        if case != "untainted-control":
            wait_event("subagent_finished")
        else:
            assert not any(e.get("type") == "subagent_finished" for e in events())
        assert host_status()["state"] == "busy"

        # One native PID/session/start, never an invented or shared session.
        record = store.record(A)
        pid = record["lifecycle"]["workerPid"]
        sid = record["native"]["sessionId"]
        kernel_start = record["lifecycle"]["workerStart"]
        home = provider_home / "sessions"
        home.mkdir(mode=0o700)
        path = home / f"{pid}.json"
        busy_ms = int(time.time() * 1000) + 2000
        provider_start, provider_domain = _provider_native_identity(kernel_start)
        busy = dict(pid=pid, sessionId=sid, kind="interactive",
                    procStart=provider_start, **provider_domain,
                    status="busy", statusUpdatedAt=busy_ms)
        path.write_text(json.dumps(busy))
        assert host_status()["activeTurnRef"] == ref

        # Exercise the public V1 exact-turn cancellation rather than writing a
        # raw PTY event. The identical cancellation_id is idempotent: no retry
        # may issue a second Escape while terminality remains unproven.
        token = "one-tainted-original-cancel"
        sent = _request(store, handle, token, ref, timeout_ms=550)
        assert sent["ok"] is True and sent["result"]["cancellation"]["turn_ref"] == ref, sent
        assert sent["result"]["cancellation"]["status"] in {"submitted", "unknown", "confirmed"}
        assert wait_event("interrupt_sent")["turn_ref"] == ref
        same = _request(store, handle, token, ref, timeout_ms=180)
        assert same["ok"] is True and same["result"]["cancellation"]["turn_ref"] == ref
        assert sum(e.get("type") == "interrupt_sent" and e.get("turn_ref") == ref
                   for e in events()) == 1
        if case != "late-stop":
            assert _native_escape_count(tmp_path / "owned-pty-writes.log") == 1
        # For native Stop fixtures the provider is deliberately blocked in
        # the hook until the gate opens, and has not yet read ESC from its PTY.
        # Host interrupt_sent still independently proves one physical write.

        # No-taint control still uses the original native lifecycle, while
        # the other cases begin with the same tainted exact-turn Escape.
        fresh_ms = max(int(time.time() * 1000) + 2200, busy_ms + 1000)
        expected_outcome = "interrupted"
        if case == "marker-first":
            original = Path(record["transcript"])
            original.parent.mkdir(parents=True, exist_ok=True)
            with original.open("a", encoding="utf-8") as source:
                source.write(json.dumps({"type": "user", "message": {
                    "content": "[Request interrupted by user]"}}) + "\n")
            expected_outcome = "interrupted"
        elif case == "late-stop":
            # Honest unconfirmed after 12s cannot prevent a later exact
            # provider Stop from completing the target exactly once.
            assert wait_event("interrupt_unconfirmed", timeout=16)["turn_ref"] == ref
            stop_gate.write_text("release\n")
            expected_outcome = "completed"  # real native Stop wins over ineffective Escape
        else:
            path.write_text(json.dumps({**busy, "status": "idle",
                                        "statusUpdatedAt": fresh_ms}))

        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            row = store.record(A)
            if (row.get("lastSettledTurn") or {}).get("turnRef") == ref:
                break
            time.sleep(.05)
        latest = store.record(A).get("lastSettledTurn") or {}
        assert latest.get("turnRef") == ref and latest.get("outcome") == expected_outcome, (
            "tainted ESC did not settle from exact original native proof",
            host_status()["state"], [e.get("type") for e in events()],
        )
        assert host_status()["state"] == "idle"
        end_kind = "turn_complete"
        assert sum(e.get("type") == end_kind and e.get("turn_ref") == ref
                   for e in events()) == 1
        if expected_outcome == "completed":
            assert any(e.get("type") == "turn_complete" and not e.get("interrupted")
                       for e in events())
        elif expected_outcome == "interrupted":
            assert any(e.get("type") == "turn_complete" and e.get("interrupted")
                       for e in events())
        assert sum(e.get("type") == "interrupt_sent" and e.get("turn_ref") == ref
                   for e in events()) == 1
        if case != "late-stop":
            assert _native_escape_count(tmp_path / "owned-pty-writes.log") == 1
        else:
            assert _native_escape_count(tmp_path / "owned-pty-writes.log") <= 1
    finally:
        stop_gate.write_text("release\n")
        _stop_disposable_original(short_root)
