"""C1A: original full-UUID Claude /compact host admission (fake provider only).

Test the actual detached host/PTY and authenticated host RPC, not a mock of
handle_client_msg. Native terminal proof and public compact remain C1B.
"""
from __future__ import annotations

import json
from pathlib import Path
import threading
import time

import pytest

from ccx import CcxClient
from ccx.ops.compact_ops import compact_intent_digest
from ccx.errors import CompactIndeterminate, SessionNotFound
from ccx.rpc.rpc import _rpc
from ccx.store.store import _digest
from test_generation_store import A, B, short_root, add_worker
from test_claude_native_cli import generated_hook_host


def test_compact_intent_is_one_canonical_native_action_without_wait_budgets():
    sid = "f1234567-1234-4234-8234-1234567890ab"
    fields = {
        "workerId": A, "engine": "claude",
        "generation": "g-" + "1" * 32,
        "nativeSessionId": sid, "operationId": "native-compact-1",
        "action": "compact",
    }
    original = compact_intent_digest(
        A, generation=fields["generation"],
        native_session_id=sid, operation_id=fields["operationId"],
    )
    assert original == _digest(fields)
    assert len(original) == 64
    assert original == compact_intent_digest(
        A, generation=fields["generation"],
        native_session_id=sid, operation_id=fields["operationId"],
    )
    # Wait and execution timeout are caller observation/pump budgets, not
    # immutable native effect identity or an alternative digest input.


def test_real_host_compact_requires_original_effect_receipt_before_journal_or_pty(
    generated_hook_host, short_root,
):
    # The accepted fixture starts the *real* detached host and a disposable
    # generated-hook fake Claude, with no provider/network connection.
    store, _call, env, _hooks = generated_hook_host
    sid = store.record(A)["native"]["sessionId"]
    assert sid != A and store.native_activation(A)["state"] == "confirmed"
    native_log = Path(env["FAKE_LOG"])
    add_worker(short_root, B, "claude")
    assert B[:8] == A[:8] and B != A
    deadline = time.monotonic() + 5
    status = None
    while time.monotonic() < deadline:
        status = _rpc(A, {"op": "status"}, timeout=1.0)
        if status.get("ok") and status.get("state") == "idle":
            break
        time.sleep(.08)
    assert status is not None and status.get("ok") and status.get("state") == "idle", status

    # This RPC performs the actual same-connection verified host handshake;
    # handshaken UID/socket alone MUST NOT permit a compact.
    for frame in (
        {"op": "compact", "timeout": 30},  # no mint inside host
        {"op": "compact", "operation_id": "no-effect-receipt", "timeout": 30},
    ):
        refused = _rpc(A, frame, timeout=2)
        assert refused.get("ok") is False, refused
        assert "compact" in str(refused.get("error"))
    journal = store.worker_path(A, "compact.json")
    assert not journal.exists()
    assert not native_log.exists() or b"/compact" not in native_log.read_bytes()

    # Prepare real global receipts and persist submitted-unknown using the
    # existing generation-native dispatch primitive, but make no provider
    # call from this test-only transition.
    def receipt(operation_id, digest):
        intent = store.begin_native_intent(
            A, action="compact", family="compact",
            operation_id=operation_id, intent_sha256=digest,
        )
        store.dispatch_native_intent_once(
            intent, bounded_submit=lambda _budget: {"ok": True},
        )
        assert store.read_receipt(A, "compact", operation_id)["state"] == "submitted-unknown"
        return store.receipt_path(A, "compact", operation_id)

    canonical = lambda op: compact_intent_digest(
        A, generation=store.generation, native_session_id=sid,
        operation_id=op,
    )
    malformed = (
        ("wrong-native-sid", "digest"),
        ("wrong-worker-owner", "worker"),
        ("wrong-generation", "generation"),
    )
    for token, mismatch in malformed:
        digest = (compact_intent_digest(
            A, generation=store.generation,
            native_session_id="00000000-0000-4000-8000-000000000001",
            operation_id=token,
        ) if mismatch == "digest" else canonical(token))
        path = receipt(token, digest)
        if mismatch in ("worker", "generation"):
            obj = json.loads(path.read_text())
            if mismatch == "worker":
                obj["workerId"] = B
            else:
                obj["generation"] = "g-" + "2" * 32
            path.write_text(json.dumps(obj))
            path.chmod(0o600)
        refused = _rpc(A, {
            "op": "compact", "operation_id": token, "timeout": 30,
        }, timeout=2)
        assert refused.get("ok") is False, (token, refused)
        assert "admission denied" in str(refused.get("error")), (token, refused)
        assert not journal.exists()
        assert not native_log.exists() or b"/compact" not in native_log.read_bytes()

    # A terminal *rejected* original effect with no matching journal is
    # not a license to create a new operation through the old host path.
    rejected_id = "native-compact-proven-no-effect"
    rejected_intent = store.begin_native_intent(
        A, action="compact", family="compact",
        operation_id=rejected_id, intent_sha256=canonical(rejected_id),
    )
    store.dispatch_native_intent_once(
        rejected_intent, bounded_submit=lambda _budget: {"ok": False},
    )
    store.settle_native_intent(
        rejected_intent, state="rejected",
        native_evidence_sha256=_digest({
            "workerId": A, "operationId": rejected_id,
            "nativeRefusal": "test-only definitive no-effect",
        }),
    )
    denied = _rpc(A, {
        "op": "compact", "operation_id": rejected_id,
    }, timeout=2)
    assert denied.get("ok") is False
    assert "terminal native compact receipt lacks original journal" in denied["error"]
    assert not journal.exists()

    # The single authentic intent is the only one allowed to reach the
    # existing compact operation journal and schedule the original pump.
    token = "authorized-original-compact"
    intent = store.begin_native_intent(
        A, action="compact", family="compact",
        operation_id=token, intent_sha256=canonical(token),
    )
    accepted = store.dispatch_native_intent_once(
        intent, dispatch_budget_s=2,
        bounded_submit=lambda budget: _rpc(
            A, {"op": "compact", "operation_id": token, "timeout": 30},
            timeout=budget,
        ),
    )
    assert accepted.get("ok") is True, accepted
    assert (accepted.get("workerId"), accepted.get("engine"),
            accepted.get("generation"), accepted.get("nativeSessionId"),
            accepted.get("operationId")) == (
        A, "claude", store.generation, sid, token,
    )
    assert accepted["operation"]["operation_id"] == token
    assert journal.exists()
    assert store.read_receipt(A, "compact", token)["state"] == "submitted-unknown"
    # C1A does not settle a global native-effect receipt, nor claim that
    # any native /compact boundary succeeded (the C1B caller owns that).
    wait_for_paste = time.monotonic() + 8
    while time.monotonic() < wait_for_paste:
        if native_log.exists() and b"/compact" in native_log.read_bytes():
            break
        time.sleep(.05)
    assert native_log.exists() and b"/compact" in native_log.read_bytes()
    before = native_log.read_bytes().count(b"/compact")
    assert before == 1
    # C1B will settle this accepted native *effect* from the exact host
    # ACK; that remains distinct from terminal native compaction.
    store.settle_native_intent(
        intent, state="accepted",
        native_evidence_sha256=_digest({
            "workerId": accepted["workerId"],
            "generation": accepted["generation"],
            "operationId": accepted["operationId"],
            "journal": accepted["operation"],
        }),
    )
    assert store.read_receipt(A, "compact", token)["state"] == "accepted"
    # A different execution/observation budget does not conflict with
    # the original immutable native action or schedule another paste.
    replay = _rpc(A, {
        "op": "compact", "operation_id": token, "timeout": 65,
    }, timeout=2)
    assert replay["ok"] is True, replay
    assert replay["operation"]["operation_id"] == token
    assert (replay["workerId"], replay["operationId"]) == (A, token)
    time.sleep(.15)
    assert native_log.read_bytes().count(b"/compact") == before
    assert not store.worker_path(B, "compact.json").exists()


def _native_append(path, *events):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as out:
        for event in events:
            out.write(json.dumps(event) + "\n")
    path.chmod(0o600)


def _manual_command():
    return {"type": "user",
            "message": {"content": "<command-name>/compact</command-name>"}}


def _manual_boundary(evidence="bound-original-native-event"):
    return {
        "type": "system", "subtype": "compact_boundary", "uuid": evidence,
        "content": "Conversation compacted",
        "compactMetadata": {
            "trigger": "manual", "preTokens": 1234,
            "postTokens": 100, "durationMs": 43,
        },
    }


def _original_native_source(store):
    rec = store.record(A)
    path = Path(rec["transcript"])
    assert path.name == rec["native"]["sessionId"] + ".jsonl"
    _native_append(path, {"type": "assistant", "message": {
        "model": "native-fake-model", "usage": {"input_tokens": 1234},
        "content": [{"type": "text", "text": "prior"}],
    }})
    return path


def test_compact_public_acceptance_then_original_manual_boundary(
    generated_hook_host, monkeypatch,
):
    from ccx.store import paths
    from ccx.claude.transcript import render_transcript_json

    store, _call, env, _hooks = generated_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    source = _original_native_source(store)
    turns_before = store.record(A)["stats"]["turns"]
    client = CcxClient()
    op = "direct-public-compact"
    accepted = client.compact(A, wait=False, operation_id=op, timeout=12)
    assert accepted.worker_id == A and accepted.short == A[:8]
    assert accepted.as_dict()["worker"] == "claude:" + A
    assert accepted.operation_id == op
    assert accepted.operation_status in {"accepted", "pasted", "submitted"}
    assert accepted.compacted is False and accepted.post_tokens is None
    assert store.read_receipt(A, "compact", op)["state"] == "accepted"

    _native_append(source, _manual_command(),
                   _manual_boundary("real-original-manual-boundary"))
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline:
        result = client.compact_status(A, op)
        if result.operation_status == "succeeded":
            break
        time.sleep(.08)
    assert result.operation_status == "succeeded", result
    assert result.compacted
    assert result.evidence_uuid == "real-original-manual-boundary"
    assert (result.pre_tokens, result.post_tokens) == (1234, 100)
    # We inserted transcript evidence ourselves; the fake provider did not
    # finish the host's input-surface lifecycle. Blocking compact cannot succeed.
    with pytest.raises(CompactIndeterminate):
        client.compact(A, operation_id=op, timeout=.5)
    assert store.record(A)["stats"]["turns"] == turns_before

    render_transcript_json(A)
    context = client.context(A)
    assert context.reason == "just_compacted" and context.tokens_used is None

    # Reading exact journal terminal evidence is nonreviving, non-native,
    # and does not write receipts or unrelated worker records.
    def forbidden(*_args, **_kwargs):
        pytest.fail("status must not use native RPC or revive")
    monkeypatch.setattr('ccx.rpc.rpc._rpc', forbidden)
    monkeypatch.setattr('ccx.ops.compact.ensure_ready', forbidden)
    with pytest.raises(SessionNotFound):
        client.compact_status(A, "unknown-compact-operation")
    journal = store.worker_path(A, "compact.json")
    before = journal.read_bytes()
    assert client.compact_status(A, op).operation_status == "succeeded"
    assert journal.read_bytes() == before


def test_compact_lost_host_ack_reconciles_without_repaste_or_foreign_inode(
    generated_hook_host, monkeypatch,
):
    from ccx.store import paths
    from ccx.rpc import rpc as rpc_mod

    store, _call, env, _hooks = generated_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    original_source = _original_native_source(store)
    true_rpc = rpc_mod._rpc
    submitted = []

    def lose_ack(worker, message, timeout=5):
        if message.get("op") == "compact":
            reply = true_rpc(worker, message, timeout=timeout)
            assert reply.get("ok") is True, reply
            submitted.append(message["operation_id"])
            # The host has durably accepted this ID, but the caller sees only
            # an ambiguous transport loss. No second native IPC is permitted.
            return {"ok": False, "error": "no reply"}
        return true_rpc(worker, message, timeout=timeout)

    monkeypatch.setattr(rpc_mod, "_rpc", lose_ack)
    client = CcxClient()
    token = "compact-original-lost-ACK"
    first = client.compact(A, operation_id=token, wait=False, timeout=10)
    assert first.operation_id == token and not first.compacted
    assert store.read_receipt(A, "compact", token)["state"] == "submitted-unknown"
    replay = client.compact(
        A, operation_id=token, wait=False, timeout=10, execution_timeout=6,
    )
    assert replay.operation_id == token and not replay.compacted
    assert submitted == [token]

    # A different inode at the exact same pathname cannot become the
    # original operation's manual compact evidence.
    replacement = original_source.with_suffix(".candidate")
    replacement.write_bytes(
        original_source.read_bytes()
        + (json.dumps(_manual_command()) + "\n").encode()
        + (json.dumps(_manual_boundary("borrowed-swapped-source")) + "\n").encode()
    )
    replacement.chmod(0o600)
    replacement.replace(original_source)
    status = client.compact_status(A, token)
    assert status.operation_status == "unknown"
    assert status.compacted is False and status.evidence_uuid is None
    with pytest.raises(CompactIndeterminate) as caught:
        client.compact(A, operation_id=token, wait=True, timeout=1)
    assert caught.value.operation_id == token
    assert submitted == [token]


@pytest.mark.parametrize("native_outcome", ["noop", "failed"])
def test_compact_native_noop_or_stderr_is_not_manual_success(
    generated_hook_host, monkeypatch, native_outcome,
):
    from ccx.store import paths
    from ccx.errors import CompactFailed

    store, _call, env, _hooks = generated_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    source = _original_native_source(store)
    client = CcxClient()
    operation_id = "native-compact-" + native_outcome
    accepted = client.compact(A, wait=False, operation_id=operation_id, timeout=12)
    assert accepted.compacted is False
    if native_outcome == "noop":
        content = "<local-command-stdout>Not enough messages to compact</local-command-stdout>"
    else:
        content = (
            "<local-command-stderr>"
            "Error during compaction: conversation cannot be compacted"
            "</local-command-stderr>"
        )
    _native_append(source, _manual_command(), {
        "type": "system", "subtype": "local_command",
        "content": content, "uuid": "matching-" + native_outcome,
    })
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        observed = client.compact_status(A, operation_id)
        if observed.operation_status in ("noop", "failed"):
            break
        time.sleep(.08)
    assert observed.operation_status == native_outcome, observed
    assert observed.compacted is False and observed.post_tokens is None
    if native_outcome == "failed":
        with pytest.raises(CompactFailed) as caught:
            client.compact(A, operation_id=operation_id, timeout=2)
        assert caught.value.operation_id == operation_id
    else:
        # The injected local-command verdict is observable, but no native
        # host lifecycle completed to make the next operation admissible.
        with pytest.raises(CompactIndeterminate):
            client.compact(A, operation_id=operation_id, timeout=.5)
        assert client.compact_status(A, operation_id).operation_status == "noop"


def test_compact_real_busy_host_refusal_is_definitive_no_effect(
    generated_hook_host, monkeypatch,
):
    from ccx.store import paths
    from ccx.errors import DeliveryRejected
    from ccx.ops.compact_ops import get_operation

    store, _call, env, _hooks = generated_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    _original_native_source(store)
    client = CcxClient()
    holding_id = "native-compact-holding-input"
    assert client.compact(A, wait=False, operation_id=holding_id).compacted is False

    # Simulate an actual ready->busy admission race at the *client* preflight.
    # The actual host remains the authority and returns its real busy refusal.
    monkeypatch.setattr('ccx.ops.compact.ensure_ready', lambda *_args, **_kw: {})
    refused_id = "native-compact-refused-before-journal"
    with pytest.raises(DeliveryRejected, match="new operation_id"):
        client.compact(A, wait=False, operation_id=refused_id, timeout=4)
    assert store.read_receipt(A, "compact", refused_id)["state"] == "rejected"
    assert get_operation(A, refused_id) is None
    assert get_operation(A, holding_id) is not None
    with pytest.raises(DeliveryRejected, match="new operation_id"):
        client.compact(A, wait=False, operation_id=refused_id, timeout=4)


@pytest.fixture
def forced_compact_hook_host(request, tmp_path, monkeypatch):
    """The existing generated-hook host, with only its fake's compact hooks opted in."""
    mode = getattr(request, "param", "normal")
    modes = ["compact_hooks"]
    if mode == "late":
        modes.append("compact_late_stop")
    elif mode == "queued":
        modes.append("compact_queued_prompt")
    else:
        assert mode == "normal"
    queue_gate = tmp_path / "compact-queued-start"
    stop_gate = tmp_path / "compact-model-stop"
    monkeypatch.setenv("FAKE_MODE", ",".join(modes))
    monkeypatch.setenv("FAKE_COMPACT_HOOK_QUEUE", str(queue_gate))
    monkeypatch.setenv("FAKE_COMPACT_HOOK_RELEASE", str(stop_gate))
    store, call, env, hooks = request.getfixturevalue("generated_hook_host")
    return store, call, env, hooks, queue_gate, stop_gate


def _compact_hook_until(label, read, matches, *, seconds=12):
    deadline = time.monotonic() + seconds
    value = None
    while time.monotonic() < deadline:
        value = read()
        if matches(value):
            return value
        time.sleep(.07)
    pytest.fail(f"{label} not observed: {value!r}")


def _compact_hook_lines(path):
    if not path.exists():
        return []
    # Only complete lines: the detached host or generated hook may be appending.
    return [json.loads(row) for row in path.read_bytes().split(b"\n")[:-1] if row]


def _compact_hook_events(store):
    return _compact_hook_lines(store.worker_path(A, "events.ndjson"))


@pytest.mark.parametrize("forced_compact_hook_host", ["normal"], indirect=True)
def test_generated_compact_ups_stop_never_mints_model_ref_and_next_send_has_new_fence(
    forced_compact_hook_host, monkeypatch,
):
    from ccx.store import paths
    from ccx.claude.managed_turns import report_window_for_turn_ref, settlement_for_turn_ref

    store, call, env, hooks, _queue, _stop = forced_compact_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    source = _original_native_source(store)
    turns_before = store.record(A)["stats"]["turns"]
    client = CcxClient()
    op = "generated-compact-ups-original"
    assert client.compact(A, wait=False, operation_id=op, timeout=12).operation_id == op
    terminal = _compact_hook_until(
        "actual compact boundary", lambda: client.compact_status(A, op),
        lambda result: result.operation_status == "succeeded",
    )
    assert terminal.evidence_uuid == "fake-compact-generated-hook-boundary"
    arrivals = _compact_hook_until(
        "generated compact UPS and Stop", lambda: _compact_hook_lines(hooks),
        lambda rows: {"UserPromptSubmit", "Stop"} <= {r.get("event") for r in rows},
    )
    assert all(r["rc"] == 0 and r["stdout"] == "{}\n"
               for r in arrivals if r["event"] in {"UserPromptSubmit", "Stop"})
    state = _compact_hook_until(
        "idle after compact without model ref",
        lambda: _rpc(A, {"op": "status"}, timeout=1),
        lambda s: s.get("ok") and s.get("state") == "idle",
    )
    assert state["activeTurnRef"] is None, state
    compact_events = _compact_hook_events(store)
    assert any(e.get("type") == "compact_hook_stop" for e in compact_events)
    assert not [e for e in compact_events if e.get("type") in {
        "turn_start", "turn_complete", "turn_settled"}], compact_events
    assert store.record(A)["stats"]["turns"] == turns_before
    compact_end_offset = source.stat().st_size

    sent = call("send", A, "ordinary model prompt after compact", "--no-wait",
                "--delivery-id", "after-compact-original", "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    record = _compact_hook_until(
        "post-compact original model completed", lambda: store.record(A),
        lambda rec: (rec.get("lastSettledTurn") or {}).get("outcome") == "completed",
        seconds=18,
    )
    ref = record["lastSettledTurn"]["turnRef"]
    assert record["stats"]["turns"] == turns_before + 1
    model_events = _compact_hook_until(
        "exact model completion event after record CAS", lambda: _compact_hook_events(store),
        lambda rows: any(e.get("type") == "turn_complete" and e.get("turn_ref") == ref
                         for e in rows),
    )
    starts = [e for e in model_events if e.get("type") == "turn_start"]
    completes = [e for e in model_events if e.get("type") == "turn_complete"]
    assert len(starts) == len(completes) == 1, model_events
    assert starts[0]["turn_ref"] == completes[0]["turn_ref"] == ref
    assert starts[0]["source"] == "user-prompt-submit"
    assert starts[0]["report_start_offset"] >= compact_end_offset
    semantic, retained = settlement_for_turn_ref(A, ref)
    assert retained and semantic["eventType"] == "turn_complete"
    window, retained = report_window_for_turn_ref(A, ref)
    assert retained and window["startOffset"] == starts[0]["report_start_offset"]
    assert sum(r["event"] == "UserPromptSubmit"
               for r in _compact_hook_lines(hooks)) >= 2


@pytest.mark.parametrize("forced_compact_hook_host", ["queued"], indirect=True)
def test_generated_noncompact_prompt_during_compact_retains_busy_hold_and_own_ref(
    forced_compact_hook_host, monkeypatch,
):
    from ccx.store import paths
    from ccx.ops.compact_ops import get_operation

    store, _call, env, hooks, queue_gate, stop_gate = forced_compact_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    _original_native_source(store)
    before = store.record(A)["stats"]["turns"]
    client = CcxClient()
    op = "generated-compact-queued-model"
    assert client.compact(A, wait=False, operation_id=op, timeout=12).operation_id == op
    _compact_hook_until(
        "original compact transcript submission", lambda: get_operation(A, op),
        lambda value: value is not None and value.get("status") == "submitted",
    )
    queue_gate.touch()  # Allow a real model UPS while the CCX compact still owns BUSY.
    _compact_hook_until(
        "compaction terminal", lambda: client.compact_status(A, op),
        lambda result: result.operation_status == "succeeded",
    )
    held = _compact_hook_until(
        "genuine queued turn held BUSY after compact",
        lambda: _rpc(A, {"op": "status"}, timeout=1),
        lambda s: s.get("ok") and s.get("state") == "busy" and s.get("activeTurnRef"),
    )
    ref = held["activeTurnRef"]
    events = _compact_hook_until(
        "post-compact queued hold", lambda: _compact_hook_events(store),
        lambda rows: any(e.get("type") == "compact_turn_in_flight" for e in rows),
    )
    assert len([e for e in events if e.get("type") == "turn_start"]) == 1
    assert store.record(A)["stats"]["turns"] == before
    assert len([e for e in events if e.get("type") == "turn_complete"]) == 0
    stop_gate.touch()
    record = _compact_hook_until(
        "queued original model Stop", lambda: store.record(A),
        lambda r: (r.get("lastSettledTurn") or {}).get("turnRef") == ref,
    )
    assert record["lastSettledTurn"]["outcome"] == "completed"
    assert record["stats"]["turns"] == before + 1
    _compact_hook_until(
        "queued exact semantic completion broadcast", lambda: _compact_hook_events(store),
        lambda rows: any(e.get("type") == "turn_complete" and e.get("turn_ref") == ref
                         for e in rows),
    )
    assert sum(r["event"] == "UserPromptSubmit"
               for r in _compact_hook_lines(hooks)) == 2


@pytest.mark.parametrize("forced_compact_hook_host", ["queued"], indirect=True)
def test_waited_compact_releases_fork_admission_before_return(
    forced_compact_hook_host, monkeypatch,
):
    """The CLI compact's success must include release of the queued native turn."""
    from ccx.ops.compact_ops import get_operation
    from ccx.ops.forking import _settled
    from ccx.store import paths

    store, call, env, _hooks, queue_gate, stop_gate = forced_compact_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    _original_native_source(store)
    operation_id = "compact-immediate-fork-admission"
    assert CcxClient().compact(
        A, wait=False, operation_id=operation_id, timeout=24,
    ).operation_id == operation_id

    def finish_native_turn():
        # Fake Claude runs an actual UserPromptSubmit after /compact, then
        # holds its Stop. The transcript boundary precedes this Stop.
        try:
            _compact_hook_until(
                "queued native /compact submitted",
                lambda: get_operation(A, operation_id),
                lambda op: op is not None and op.get("status") == "submitted",
            )
            queue_gate.touch()
            _compact_hook_until(
                "native compact boundary written",
                lambda: get_operation(A, operation_id),
                lambda op: op is not None and op.get("status") == "succeeded",
            )
            time.sleep(2)
        finally:
            stop_gate.touch()

    release = threading.Thread(target=finish_native_turn, daemon=True)
    release.start()
    try:
        compacted = call(
            "compact", A, "--operation-id", operation_id,
            "--timeout", "24", "--json", timeout=32,
        )
        assert compacted.returncode == 0, (compacted.stdout, compacted.stderr)
        assert json.loads(compacted.stdout)["operation_status"] == "succeeded"
        # This is ccx fork's exact admission guard. No sleep between commands.
        _settled(store.record(A))
    finally:
        stop_gate.touch()
        release.join(timeout=15)


@pytest.mark.parametrize("forced_compact_hook_host", ["late"], indirect=True)
def test_generated_late_compact_stop_fallback_has_no_exact_report_start_offset(
    forced_compact_hook_host, monkeypatch,
):
    from ccx.store import paths
    from ccx.claude.managed_turns import report_window_for_turn_ref

    store, _call, env, hooks, _queue, stop_gate = forced_compact_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    _original_native_source(store)
    client = CcxClient()
    op = "generated-late-compact-stop"
    assert client.compact(A, wait=False, operation_id=op, timeout=12).operation_id == op
    _compact_hook_until(
        "compact settles before delayed Stop", lambda: client.compact_status(A, op),
        lambda result: result.operation_status == "succeeded",
    )
    idle = _compact_hook_until(
        "idle before late compact Stop", lambda: _rpc(A, {"op": "status"}, timeout=1),
        lambda s: s.get("ok") and s.get("state") == "idle",
    )
    assert idle["activeTurnRef"] is None
    assert not any(e.get("type") == "turn_start" for e in _compact_hook_events(store))
    stop_gate.touch()
    record = _compact_hook_until(
        "late Stop fallback boundary", lambda: store.record(A),
        lambda r: bool((r.get("lastSettledTurn") or {}).get("turnRef")),
    )
    ref = record["lastSettledTurn"]["turnRef"]
    starts = [e for e in _compact_hook_events(store) if e.get("type") == "turn_start"]
    assert len(starts) == 1 and starts[0]["turn_ref"] == ref
    assert starts[0]["source"] == "stop-fallback"
    assert "report_start_offset" not in starts[0]
    window, retained = report_window_for_turn_ref(A, ref)
    assert retained and window["startOffset"] is None
    # A semantic Stop without a start fence remains a Q1 status-only/null-report
    # receiptless observation in later STEP2B. This slice does not fix late Stop.
    # The generated relay appends its result only after the host call returns.
    _compact_hook_until(
        "late compact Stop hook result", lambda: _compact_hook_lines(hooks),
        lambda rows: any(r["event"] == "Stop" and r["rc"] == 0 for r in rows),
    )
