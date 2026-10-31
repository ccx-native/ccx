"""Full-UUID original Claude question notifications: source, effect and W1."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.cli.commands.turns as commands_turns
import ccx.notify.select as notify_select
import ccx.ops.input as ops_input
import ccx.notify.settle as notify_settle
import ccx.ops.wait as ops_wait

from argparse import Namespace
import json
import uuid

import pytest

from ccx import client as client_module
from ccx.codex import control as codex_control
from ccx.cli.commands import _common as commands
from ccx.rpc import local_rpc
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.errors import DeliveryRejected, OperationConflict
from ccx.store.store import _digest
from test_generation_store import A, C, short_root as short_root
from test_notify_claude import owners as owners
from test_notify_claude_permission import _managed_parent
from test_notify_ext import FakeControl
from test_claude_native_cli import generated_hook_host as generated_hook_host
from test_notify_standalone import _native_caller
from test_notify_receiptless import (
    _claude_sources, _claude_hook_events, _claude_line,
)


def _args(gate, *, text="Yes", option=None, text_answer=None):
    return Namespace(
        target=A, request_id=gate, text=text, option=option,
        text_answer=text_answer, json=True, notify_parent=True,
        wait=False, no_wait=False, timeout=6, stdin=False, file=None,
    )


def _ids(store, gate):
    response = str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"ccx-input-decision:{store.generation}:{A}:{gate}",
    ))
    event = str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"ccx-notify:full-uuid-claude-input:{store.generation}:{A}:{gate}",
    ))
    return response, event


@pytest.fixture
def question_world(owners, monkeypatch, tmp_path):
    _root, store = owners
    transcript = _claude_sources(store, monkeypatch, tmp_path)
    r1 = "ccx-turn:claude:v1:original-question-r1"
    gate = {"id": "original-question-req", "tool": "AskUserQuestion",
            "input": {"questions": [{"question": "Proceed?",
                                     "options": [{"label": "Yes"}, {"label": "No"}]}]}}
    _claude_hook_events(store, {"type": "turn_start", "turn_ref": r1,
                                "turn": 1, "report_start_offset": 0})
    store.mutate_record("claude:" + A, patches={
        "waitingFor": gate, "lifecycle.state": "waiting",
        "lifecycle.activeTurnRef": r1,
    })
    _native_caller(monkeypatch)
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_a: True)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    protocol_ops._CLIENT.value = None
    state = {"gate": gate, "turn": r1, "native": "accepted"}
    native_calls = []

    def original_host(wid, msg, timeout=10):
        assert wid == A
        if msg["op"] == "status":
            return {"ok": True, "state": "waiting",
                    "waitingFor": state["gate"], "activeTurnRef": state["turn"]}
        assert msg["op"] == "answer-permission"  # NOT a new SEND/deliver
        token = msg["response_id"]
        receipt = store.read_receipt("claude:" + A, "answer", token)
        assert receipt["state"] == "submitted-unknown"
        event_id = _ids(store, msg["request_id"])[1]
        event = parent_notify.read_registration(event_id)
        assert event["state"] == "armed"
        assert event["operation"]["kind"] == "claude-input-answer"
        assert event["operation"]["selectedTurnRef"] == r1
        assert event["operation"]["payloadSha256"] == _digest(msg["answers"])
        assert event["operation"]["intentSha256"] == receipt["intentSha256"]
        assert json.loads(parent_notify.registration_path(event_id).read_text()) == event
        sid = store.record(A)["native"]["sessionId"]
        # Hand-written ORIGINAL layout, not a second implementation of the helper.
        legacy = _digest({
            "workerId": A, "engine": "claude", "generation": store.generation,
            "nativeId": sid, "nativeSource": "claude-session:" + sid,
            "kind": "answer", "requestId": msg["request_id"],
            "responseId": token, "payloadSha256": _digest(msg["answers"]),
            "action": "answer",
        })
        assert legacy == receipt["intentSha256"]
        assert ops_input._input_intent_digest(
            worker_id=A, engine="claude", generation=store.generation,
            native_id=sid, native_source="claude-session:" + sid,
            kind="answer", request_id=msg["request_id"], response_id=token,
            payload_sha256=event["operation"]["payloadSha256"],
        ) == legacy
        native_calls.append(dict(msg))
        if state["native"] == "ambiguous":
            raise TimeoutError("original input ACK lost AFTER native write")
        return {"ok": True, "request_id": msg["request_id"],
                "response_id": token, "decision": "answer"}

    monkeypatch.setattr(client_module, "_rpc", original_host)
    return store, transcript, r1, gate, state, native_calls


@pytest.mark.parametrize("parent_kind,structured", [
    ("native", False), ("managed", True),
])
def test_question_source_cli_original_gate_and_immutable_answer(
    question_world, tmp_path, monkeypatch, capsys, parent_kind, structured,
):
    store, _transcript, r1, gate, state, calls = question_world
    parent = (_managed_parent(store, monkeypatch, tmp_path)
              if parent_kind == "managed" else _native_caller(monkeypatch))
    request_id = gate["id"]
    expected, event_id = _ids(store, request_id)
    # Wrong current ID cannot map answers onto a different current gate.
    with pytest.raises(SystemExit) as error:
        commands_turns.cmd_answer(_args("old-question", text="Yes"))
    assert error.value.code == 3
    guidance = capsys.readouterr().err
    assert "response_id=" in guidance and "notification_event_id=" in guidance
    assert not calls and not parent_notify.iter_registrations()

    if structured:
        gate = {"id": request_id, "tool": "AskUserQuestion", "input": {
            "questions": [{"question": "Proceed?", "options": [{"label": "Yes"}]},
                          {"question": "Why?"}]}}
        state["gate"] = gate
        store.mutate_record("claude:" + A, patches={"waitingFor": gate})
        args = _args(request_id, text=None, option=[(1, "Yes")],
                     text_answer=[(2, "because")])
        answers = {"Proceed?": "Yes", "Why?": "because"}
    else:
        args = _args(None)  # omitted CLI ID selects the one brokered Q
        answers = {"Proceed?": "Yes"}
    commands_turns.cmd_answer(args)
    public = json.loads(capsys.readouterr().out)
    assert public["status"] == "accepted" and public["ok"]
    assert public["original_gate_answered"] and public["input_mode"] == "native-input"
    assert public["worker_id"] == A and public["response_id"] == expected
    assert public["notify_parent"]["event_id"] == event_id
    assert public["notify_parent"]["selected_turn_ref"] == r1
    assert public["notify_parent"]["parent"] == (
        "codex:" + C if parent_kind == "managed" else parent)
    assert calls[0]["answers"] == answers and len(calls) == 1
    event = parent_notify.read_registration(event_id)
    assert set(event["operation"]) == {
        "kind", "gateId", "responseId", "nativeSourceId", "intentSha256",
        "selectedTurnRef", "payloadSha256",
    }
    # Original input already accepted; replay needs no live successor gate.
    state["gate"] = {"id": "later-question", "tool": "AskUserQuestion"}
    store.mutate_record("claude:" + A, patches={"waitingFor": state["gate"]})
    same = ops_input.answer_input(
        "claude:" + A, request_id=request_id, response_id=expected,
        answers=answers, notify_parent=True,
    )
    assert same["status"] == "accepted" and len(calls) == 1
    assert same["notification"]["event_id"] == event_id
    with pytest.raises(OperationConflict):
        ops_input.answer_input(
            "claude:" + A, request_id=request_id, response_id=expected,
            answers={**answers, "Proceed?": "No"}, notify_parent=True,
        )
    assert len(calls) == 1
    # Retry the captured original ID explicitly: another omitted invocation
    # would instead select the successor, not act as a replay ledger.
    with pytest.raises(SystemExit) as stale:
        commands_turns.cmd_answer(args if structured else _args(request_id))
    assert stale.value.code == 3
    err = capsys.readouterr().err
    assert expected in err and event_id in err and len(calls) == 1


def test_question_unknown_replay_late_event_and_definitive_noeffect(
    question_world, monkeypatch, capsys,
):
    store, _transcript, _r1, gate, state, calls = question_world
    request_id = gate["id"]
    token, event = _ids(store, request_id)
    state["native"] = "ambiguous"
    with pytest.raises(SystemExit) as unknown:
        commands_turns.cmd_answer(_args(None))  # the first attempt freezes the original Q
    assert unknown.value.code == 6
    result = json.loads(capsys.readouterr().out)
    assert result["status"] in {"unknown", "submitted"}
    assert not result["original_gate_answered"]
    assert parent_notify.read_registration(event)["state"] == "armed"
    assert len(calls) == 1
    # The exact same normalized V1/core payload first observes existing intent.
    state["gate"] = {"id": "a-new-question", "tool": "AskUserQuestion"}
    store.mutate_record("claude:" + A, patches={"waitingFor": state["gate"]})
    replay = ops_input.answer_input(
        "claude:" + A, request_id=request_id, response_id=token,
        answers={"Proceed?": "Yes"}, notify_parent=True,
    )
    assert replay["status"] == "unknown" and len(calls) == 1
    from ccx.ops import worker_events
    monkeypatch.setattr(worker_events, "read_full_uuid_event_page",
        lambda *_a, **_kw: {"events": [{
            "type": "question_answered", "request_id": request_id,
            "response_id": token, "cursor": "original-event-cursor",
            "epoch": "old-host", "seq": 7,
        }]})
    proven = ops_input.answer_input(
        "claude:" + A, request_id=request_id, response_id=token,
        answers={"Proceed?": "Yes"}, notify_parent=True,
    )
    assert proven["status"] == "accepted" and proven["evidence"] == "host-event:old-host:7"
    assert len(calls) == 1 and proven["notification"]["event_id"] == event

    # A separate fresh world is exercised through the same prepared source
    # in a distinct test below; this unknown event cannot be rearmed or cancelled.
    assert parent_notify.read_registration(event)["state"] == "armed"


def test_omitted_question_wrong_kind_refuses_before_notifier_and_native_effect(
    question_world, monkeypatch, capsys,
):
    store, _transcript, _r1, gate, state, calls = question_world
    permission = {"id": "different-kind", "tool": "Bash"}
    state["gate"] = permission
    store.mutate_record("claude:" + A, patches={"waitingFor": permission})
    monkeypatch.setattr(commands_turns, "_prepare_parent_notification",
                        lambda *_a: pytest.fail("parent prepared before gate bound"))
    with pytest.raises(SystemExit) as unavailable:
        commands_turns.cmd_answer(_args(None))
    assert unavailable.value.code == 3
    assert "No pending question" in capsys.readouterr().err
    assert not calls and not parent_notify.iter_registrations()
    assert store.read_receipt("claude:" + A, "answer", _ids(store, gate["id"])[0]) is None


def test_question_definitive_noeffect_cancels_only_original_event(
    question_world, monkeypatch,
):
    store, _transcript, _r1, gate, _state, calls = question_world
    token, event = _ids(store, gate["id"])
    from ccx.client import CcxClient
    protocol_ops._CLIENT.value = CcxClient()
    def definitive(_target, **kw):
        assert kw["fallback_to_send"] is False and kw["wait"] is False
        assert parent_notify.read_registration(event)["state"] == "armed"
        assert store.read_receipt(A, "answer", token)["state"] == "submitted-unknown"
        calls.append({"native": "definitive"})
        raise DeliveryRejected("exact request no longer pending",
                               reason="request id mismatch")
    monkeypatch.setattr(protocol_ops._CLIENT.value, "answer", definitive)
    first = ops_input.answer_input(
        "claude:" + A, request_id=gate["id"], response_id=token,
        answers={"Proceed?": "Yes"}, notify_parent=True,
    )
    assert first["status"] == "rejected" and first["notification"]["state"] == "cancelled"
    assert parent_notify.read_registration(event)["state"] == "cancelled"
    same = ops_input.answer_input(
        "claude:" + A, request_id=gate["id"], response_id=token,
        answers={"Proceed?": "Yes"}, notify_parent=True,
    )
    assert same["status"] == "rejected" and same["notification"]["event_id"] == event
    assert len(calls) == 1


def test_question_original_r_new_same_r_gate_then_exact_stop_and_one_w1(
    question_world, monkeypatch,
):
    store, transcript, r1, gate, state, calls = question_world
    first = ops_input.answer_input(
        "claude:" + A, request_id=gate["id"], response_id="r1-question-response",
        answers={"Proceed?": "Yes"}, notify_parent=True,
    )
    assert first["status"] == "accepted" and len(calls) == 1
    event = first["notification"]["event_id"]
    reg = parent_notify.read_registration(event)
    assert notify_settle.settlement_for_registration(reg) is None
    successor = {"id": "new-actionable-gate", "tool": "Bash", "input": {"command": "true"}}
    store.mutate_record("claude:" + A, patches={"waitingFor": successor})
    state["gate"] = successor
    monkeypatch.setattr(ops_wait, "wait_worker",
        lambda *_a, **_kw: {
            "worker": "claude:" + A, "state": "waiting",
            "active_turn_ref": r1, "waiting_for": successor,
        })
    waiting = notify_settle.settlement_for_registration(reg)
    assert waiting["status"] == "waiting" and waiting["report"] is None
    assert waiting["waitingFor"]["id"] == successor["id"]
    _claude_line(transcript, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "EXACT original question answer"}]}})
    offset = transcript.stat().st_size
    _claude_hook_events(store, {"type": "turn_complete", "turn_ref": r1, "turn": 1},
                        {"type": "turn_start", "turn_ref": "ccx-turn:claude:v1:later-R2",
                         "turn": 2, "report_start_offset": offset})
    _claude_line(transcript, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "WRONG latest R2"}]}})
    terminal = notify_settle.settlement_for_registration(reg)
    assert terminal["status"] == "completed" and terminal["turnRef"] == r1
    assert terminal["report"] == "EXACT original question answer"
    assert "WRONG" not in str(terminal)
    native = FakeControl()
    monkeypatch.setattr(codex_control, "CodexControlClient", lambda: native)
    notifyd.process_registration(reg)
    delivered = parent_notify.read_registration(event)
    assert delivered["state"] == "delivered"
    assert delivered["payload"]["turnRef"] == r1
    assert delivered["payload"]["report"] == "EXACT original question answer"
    notifyd.process_registration(delivered)
    assert len(native.methods("turn/start")) == 1


def test_v1_already_allowlisted_question_notify_parent_real_input_effect(question_world):
    _store, _transcript, _r1, gate, _state, calls = question_world
    response = local_rpc.dispatch({
        "protocol_version": 2, "request_id": "v1-call",
        "op": "input.answer", "timeout_ms": 5000,
        "params": {"target": "claude:" + A, "request_id": gate["id"],
                   "response_id": "v1-question-answer", "answers": {"Proceed?": "Yes"},
                   "notify_parent": True},
    })
    assert response["ok"] is True, response
    result = response["result"]
    assert result["input"]["status"] == "accepted" and result["notification"]["source"] == "input"
    assert result["notification"]["operation_kind"] == "claude-input-answer"
    assert len(calls) == 1


@pytest.mark.parametrize("generated_hook_host", ["restricted"], indirect=True)
def test_generated_original_question_hook_notify_one_real_answer_and_parent_wake(
    generated_hook_host, monkeypatch, capsys, request,
):
    """One fake native host/hook: original updatedInput, R and Codex W1."""
    from pathlib import Path
    import time
    from ccx.store import paths
    from test_claude_native_cli import _generated_hook_results

    store, call, env, hook_log = generated_hook_host
    _isolate_disposable_notifyd_for_fake_native(store, request)
    started = call("send", A, "ask generated question", "--no-wait",
                   "--delivery-id", "n4-question-original-fake-hook", "--json")
    assert started.returncode == 0, (started.stdout, started.stderr)
    deadline = time.monotonic() + 15
    gate = None
    while time.monotonic() < deadline:
        gate = store.record(A).get("waitingFor")
        if isinstance(gate, dict) and gate.get("id"):
            break
        time.sleep(.06)
    assert gate and gate.get("tool") == "AskUserQuestion", gate
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", env["CLAUDE_CONFIG_DIR"])
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    protocol_ops._CLIENT.value = None
    parent = _native_caller(monkeypatch)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    args = _args(gate["id"])
    args.timeout = 15
    commands_turns.cmd_answer(args)
    first = json.loads(capsys.readouterr().out)
    assert first["status"] == "accepted"
    assert first["original_gate_answered"]
    assert first["response_id"] == _ids(store, gate["id"])[0]
    assert first["notify_parent"]["parent"] == parent
    assert first["notify_parent"]["source"] == "input"
    event = first["notify_parent"]["event_id"]
    row = parent_notify.read_registration(event)
    assert row["operation"]["kind"] == "claude-input-answer"
    assert row["operation"]["payloadSha256"] == _digest({"Which option?": "Yes"})
    r1 = row["operation"]["selectedTurnRef"]

    deadline = time.monotonic() + 15
    settled = None
    while time.monotonic() < deadline:
        settled = notify_settle.settlement_for_registration(row)
        # The exact turn's terminal event can precede its report window being
        # readable. Await the actual report, not merely the terminal status.
        if (settled and settled.get("status") == "completed"
                and isinstance(settled.get("report"), str)):
            break
        time.sleep(.06)
    assert (settled and settled["status"] == "completed"
            and isinstance(settled.get("report"), str)), settled
    assert settled["turnRef"] == r1 and "Fake Claude completed" in settled["report"]
    hook_answers = [item for item in _generated_hook_results(hook_log)
                    if item["event"] == "PermissionRequest"]
    assert len(hook_answers) == 1 and hook_answers[0]["rc"] == 0
    assert json.loads(hook_answers[0]["stdout"])[
        "hookSpecificOutput"]["decision"]["updatedInput"]["answers"] == {
            "Which option?": "Yes"}
    native = FakeControl()
    monkeypatch.setattr(codex_control, "CodexControlClient", lambda: native)
    _drive_fake_native_notify_once(row, monkeypatch)
    delivered = parent_notify.read_registration(event)
    assert delivered["state"] == "delivered"
    assert delivered["payload"]["turnRef"] == r1
    assert delivered["payload"]["report"] == settled["report"]
    notifyd.process_registration(delivered)
    assert len(native.methods("turn/start")) == 1



def _isolate_disposable_notifyd_for_fake_native(store, request):
    """Fence only THIS fake test root's independent notifyd before registering.

    The physical fake Claude host may asynchronously revive notifyd in a
    separate process, where pytest's fake CodexControlClient monkeypatch has
    no effect. Hold the original root's shared spawn flock during the
    generated permission/question/MCP operation and the manual fake delivery;
    notifyd cannot start, and the source-owned original no-effect/wake proof
    remains fully exercised by pytest's one native fake parent client.
    """
    import fcntl
    import os
    import stat
    from pathlib import Path
    from ccx import constants
    from ccx.ops import maintenance as maintenance
    from ccx.notify.paths import daemon_spawn_lock_path, ensure_dirs

    store.revalidate()
    root = Path(store.root).resolve(strict=True)
    assert root == Path(constants.CCX_ROOT).resolve(strict=True)
    assert root == Path(os.environ["CCX_ROOT"]).resolve(strict=True)
    assert root.stat().st_uid == os.getuid()
    ensure_dirs()
    path = daemon_spawn_lock_path()
    assert path.parent.resolve(strict=True) == root / "notify"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    lock = os.fdopen(fd, "a+")
    try:
        st = os.fstat(lock.fileno())
        assert st.st_uid == os.getuid() and stat.S_IMODE(st.st_mode) == 0o600
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # No time window with a pre-existing independent writer: abort rather
        # than stopping/replaying a possibly placed native effect.
        assert maintenance._bound_notifyd_pid(store) is None, (
            "independent original notifyd already active before fake registration")
    except BaseException:
        lock.close()
        raise
    def release_after_exact_owned_host_cleanup():
        # The source-owned host's async notifyd revival thread can be WAITING
        # on our spawn flock. Stop only this disposable worker's original
        # PID/start-stamped process BEFORE releasing that flock, so it cannot
        # launch a late notifier after the fake test has finished.
        try:
            from test_claude_native_cli import _stop_disposable_original
            _stop_disposable_original(root)
        finally:
            lock.close()

    request.addfinalizer(release_after_exact_owned_host_cleanup)
    return path

def _drive_fake_native_notify_once(reg, monkeypatch):
    """Drive a test-owned original native wake only after proven no-effect CAS.

    The independent fake Claude host may activate a disposable notifyd while
    this pytest process snapshots the registration. A single manual call on
    that now-stale snapshot may NOT settle a newer source-owned row (its
    payload, attempted marker, or backoff can have advanced). Re-read the
    exact persisted event before EACH call and inspect its result. Only a
    positively no-effect DeliveryDeferred outcome authorizes a bounded fresh
    test-native call; possible native placement must always fail closed.

    This helper never mutates disk retry policy, sleeps, replays native caller
    intent or overrides original identity/source validation in notifyd.
    """
    from ccx.notify import daemon as _notifyd

    event_id = reg["eventId"]
    original_now = _notifyd.now_ms

    def read_verified(*, repeat=False):
        current = parent_notify.read_registration(event_id)
        assert isinstance(current, dict) and current.get("eventId") == event_id
        if current.get("state") == "delivered":
            return current
        assert current.get("state") in {"armed", "ready"}, (
            "original notification no longer actionable", current.get("state"))
        assert current.get("deliveryAttemptedAtMs") is None, (
            "original native wake may have been placed; no replay")
        error = current.get("lastError")
        if error is not None:
            assert str(error).startswith("DeliveryDeferred:"), (
                "original outcome lacks positive no-effect proof", str(error).split(":", 1)[0])
        if repeat:
            assert current.get("state") == "ready" and isinstance(error, str) and (
                error.startswith("DeliveryDeferred:")), (
                "never repeat without positive original no-effect proof")
        return current

    def once_due_clock():
        real_now = original_now()
        latest = parent_notify.read_registration(event_id)
        assert isinstance(latest, dict) and latest.get("eventId") == event_id
        due = int(latest.get("nextAttemptAtMs") or 0)
        if latest.get("state") == "ready" and due > real_now:
            assert str(latest.get("lastError") or "").startswith("DeliveryDeferred:"), (
                "test fake notifier has no positive native non-write proof")
            recorded_attempt = latest.get("deliveryAttemptedAtMs")
            assert (recorded_attempt is None or (
                self_marked_at[0] is not None
                and recorded_attempt == self_marked_at[0])), (
                "original native wake may have been placed by another process; no replay")
            assert isinstance(latest.get("payload"), dict)
            return due
        return real_now

    for attempt in range(3):  # max three calls, NEVER an unproved native retry
        # A marker from THIS pytest process's current original notifyd call
        # is NOT a competing-daemon wake. Capture exact provenance only at
        # the single manual call's original attempted-marker CAS.
        self_marked_at = [None]
        original_mark_attempt = _notifyd._mark_final_attempt_started

        def mark_only_our_current_attempt(row_event, delivery_id):
            assert row_event == event_id
            before = parent_notify.read_registration(event_id)
            assert isinstance(before, dict)
            assert before.get("deliveryAttemptedAtMs") is None, (
                "a different original process may have placed a native wake")
            result = original_mark_attempt(row_event, delivery_id)
            after = parent_notify.read_registration(event_id)
            assert isinstance(after, dict)
            stamped = after.get("deliveryAttemptedAtMs")
            assert type(stamped) is int and stamped > 0
            self_marked_at[0] = stamped
            return result

        current = read_verified(repeat=attempt > 0)
        if current["state"] == "delivered":
            return
        with monkeypatch.context() as local_clock:
            local_clock.setattr(_notifyd, "now_ms", once_due_clock)
            local_clock.setattr(_notifyd, "_mark_final_attempt_started",
                                mark_only_our_current_attempt)
            # A concurrently deferred original can change since the previous
            # read; bind the one process_registration invocation to a fresh
            # immutable source-owned row right before its delivery path.
            fresh = read_verified(repeat=attempt > 0)
            if fresh["state"] == "delivered":
                return
            _notifyd.process_registration(fresh)
        outcome = read_verified()
        if outcome["state"] == "delivered":
            return
        # An actual uncertain native effect retained its attempted marker and
        # raises above; only positive no-effect may authorize the next call.
        read_verified(repeat=True)
    raise AssertionError("three provably no-effect original deliveries did not settle")


def test_fake_notify_single_logical_due_placement_and_ambiguous_guard(monkeypatch):
    """The test's one manual fake notification cannot replay possible writes."""
    from ccx.notify import daemon as _notifyd

    future = _notifyd.now_ms() + 60000
    original_clock = _notifyd.now_ms
    record = {
        "eventId": "isolated-test-only-event", "state": "ready",
        "nextAttemptAtMs": future, "lastError": "DeliveryDeferred: positive no effect",
        "deliveryAttemptedAtMs": None, "payload": {"test": True},
    }
    calls = []
    monkeypatch.setattr(parent_notify, "read_registration", lambda _key: record)
    def fake_delivered(_reg):
        calls.append((_notifyd.now_ms(), dict(_reg)))
        record["state"] = "delivered"  # fake original success; no native process
    monkeypatch.setattr(_notifyd, "process_registration", fake_delivered)
    _drive_fake_native_notify_once(record, monkeypatch)
    assert len(calls) == 1 and calls[0][0] == future
    assert _notifyd.now_ms is original_clock
    assert record["nextAttemptAtMs"] == future  # never changed retry policy

    # Reproduce the genuinely observed race without a native process: the
    # FIRST read is 'armed', then a different original daemon positively
    # defers a zero-byte attempt BEFORE our manual due-time check executes.
    record["state"] = "armed"
    record["nextAttemptAtMs"] = 0
    reads = [0]
    def shifting_original_row(_key):
        reads[0] += 1
        if reads[0] == 2:
            record.update(state="ready", nextAttemptAtMs=future,
                          lastError="DeliveryDeferred: positive no effect",
                          deliveryAttemptedAtMs=None)
        return record
    monkeypatch.setattr(parent_notify, "read_registration", shifting_original_row)
    _drive_fake_native_notify_once(record, monkeypatch)
    assert len(calls) == 2 and calls[1][0] == future
    assert _notifyd.now_ms is original_clock
    assert record["nextAttemptAtMs"] == future
    record.update(state="ready", lastError="DeliveryDeferred: proven no effect")
    record["deliveryAttemptedAtMs"] = future-1
    with pytest.raises(AssertionError, match="may have been placed"):
        _drive_fake_native_notify_once(record, monkeypatch)
    assert len(calls) == 2


def test_fake_notify_stale_original_row_only_redrives_proven_no_effect(monkeypatch):
    """RED: concurrent durable defer after snapshot needs fresh single safe drive.

    Simulate real notifyd's positive no-effect CAS *after* the fixture takes
    its first row snapshot but before its manual process_registration can
    settle it. Only the second original snapshot can prove ready/no-effect.
    No native provider, clock sleeps or non-scoped worker processes.
    """
    from ccx.notify import daemon as _notifyd

    event = "test-stale-positive-no-effect"
    original = _notifyd.now_ms
    future = original() + 60_000
    durable = {"eventId": event, "state": "armed", "payload": None,
               "nextAttemptAtMs": 0, "lastError": None,
               "deliveryAttemptedAtMs": None}
    calls = []

    def source_snapshot(_event):
        assert _event == event
        return dict(durable)  # simulate an immutable original JSON read

    def process_original(snapshot):
        calls.append((dict(snapshot), _notifyd.now_ms()))
        if len(calls) == 1:
            assert snapshot["state"] == "armed"
            durable.update(state="ready", payload={"original": True},
                           lastError="DeliveryDeferred: no original native effect",
                           nextAttemptAtMs=future, deliveryAttemptedAtMs=None)
        else:
            assert snapshot["state"] == "ready", "must re-read original after CAS"
            assert snapshot["deliveryAttemptedAtMs"] is None
            assert calls[-1][1] >= future
            durable["state"] = "delivered"  # fake original native accepted once

    monkeypatch.setattr(parent_notify, "read_registration", source_snapshot)
    monkeypatch.setattr(_notifyd, "process_registration", process_original)
    _drive_fake_native_notify_once(durable, monkeypatch)
    assert durable["state"] == "delivered", "stale positive no-effect CAS was not resumed"
    assert len(calls) == 2  # first had no write; second one actual effect
    assert calls[0][0]["state"] == "armed"
    assert calls[1][0]["state"] == "ready"
    assert durable["nextAttemptAtMs"] == future  # no disk backoff modification
    assert _notifyd.now_ms is original


def test_fake_notify_stale_original_ambiguous_write_never_redriven(monkeypatch):
    """RED: a pending original write marker is not native retry permission."""
    from ccx.notify import daemon as _notifyd

    event = "test-stale-ambiguous-native-attempt"
    durable = {"eventId": event, "state": "armed", "payload": None,
               "nextAttemptAtMs": 0, "deliveryAttemptedAtMs": None}
    original = _notifyd.now_ms
    calls = []

    def process_original(snapshot):
        calls.append(dict(snapshot))
        assert len(calls) == 1, "a possibly placed original effect MUST NEVER replay"
        durable.update(state="ready", payload={"original": True},
                       lastError="DeliveryDeferred: native may have reached parent",
                       nextAttemptAtMs=original()+60_000,
                       deliveryAttemptedAtMs=original())

    monkeypatch.setattr(parent_notify, "read_registration", lambda _key: dict(durable))
    monkeypatch.setattr(_notifyd, "process_registration", process_original)
    with pytest.raises(AssertionError, match="ambiguous|may have been placed"):
        _drive_fake_native_notify_once(durable, monkeypatch)
    assert len(calls) == 1 and durable["state"] == "ready"
    assert _notifyd.now_ms is original


def test_fake_notify_self_stamped_marker_during_own_successful_delivery(monkeypatch):
    """RED: the pytest caller itself stamps original attemptedAtMs before delivered.

    No separate notifyd, native provider, model, or subprocess participates.
    Actual notifyd.process_registration and its original local persistence
    functions run against a function-scoped fake registration CAS store.
    """
    import os
    from ccx.notify import daemon as _notifyd

    original_now = _notifyd.now_ms
    due = original_now() + 60_000
    row = {"eventId": "test-pid-stamped-in-pytest", "state": "ready",
           "nextAttemptAtMs": due, "lastError": "DeliveryDeferred: positive no effect",
           "deliveryAttemptedAtMs": None,
           "payload": {"eventId": "test-pid-stamped-in-pytest",
                       "deliveryId": "test:original-fake"}}
    stamped = []
    delivered = []
    native = []

    def mutate_local(event_id, update):
        assert event_id == row["eventId"]
        # Original mutate_registration is a JSON CAS: local changes are NOT
        # visible to a simultaneous read until the atomic write completes.
        pending = dict(row)
        update(pending)
        row.clear()
        row.update(pending)
        return dict(row)

    real_start = _notifyd._mark_final_attempt_started
    def stamp_with_original_process_identity(event_id, delivery_id):
        stamped.append((os.getpid(), "pytest", event_id))
        return real_start(event_id, delivery_id)

    def controlled_native_delivery(_reg, _payload):
        native.append((os.getpid(), "fake-CodexControlClient"))
        assert row["deliveryAttemptedAtMs"] is not None
        return {"ok": True, "original": "fake", "effects": 1}

    monkeypatch.setattr(parent_notify, "read_registration",
                        lambda event: dict(row) if event == row["eventId"] else None)
    monkeypatch.setattr(notify_select, "_original_notification_registration",
                        lambda _row: None)
    monkeypatch.setattr(_notifyd, "mutate_registration", mutate_local)
    monkeypatch.setattr(_notifyd, "_mark_final_attempt_started",
                        stamp_with_original_process_identity)
    monkeypatch.setattr(_notifyd, "deliver_to_parent", controlled_native_delivery)
    _drive_fake_native_notify_once(row, monkeypatch)
    assert row["state"] == "delivered", (row["state"], stamped, native)
    assert stamped == [(os.getpid(), "pytest", row["eventId"])]
    assert native == [(os.getpid(), "fake-CodexControlClient")]
    print("ORIGINAL_ATTEMPT_MARKER_WRITER_ROLE=pytest LOCAL_PID=" + str(os.getpid()))
    assert row["deliveryAttemptedAtMs"] == due
    assert _notifyd.now_ms is original_now


def test_disposable_notifyd_spawn_lock_blocks_another_process(
    short_root, monkeypatch, request,
):
    """A different process cannot spawn a notifyd for THIS exact fake root."""
    import os
    import subprocess
    import sys
    from ccx import constants
    from ccx.store.root import require_ccx_active

    monkeypatch.setenv("CCX_ROOT", str(short_root))
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    root_lock = _isolate_disposable_notifyd_for_fake_native(store, request)
    contender = (
        "import fcntl,sys\n"
        "with open(sys.argv[1],'a+') as role:\n"
        " try: fcntl.flock(role,fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
        " except BlockingIOError: print('spawn-flock-blocked');sys.exit(0)\n"
        " raise SystemExit('incorrectly acquired original notifier spawn lock')\n"
    )
    child = subprocess.run([sys.executable, "-B", "-c", contender, str(root_lock)],
                           stdin=subprocess.DEVNULL, capture_output=True,
                           text=True, timeout=4)
    assert child.returncode == 0 and child.stdout.strip() == "spawn-flock-blocked", (
        child.returncode, child.stdout, child.stderr)
    assert root_lock.parent == short_root / "notify"
    assert root_lock.stat().st_uid == os.getuid()
