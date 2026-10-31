"""C1 direct UUID-native compact notifications, using accepted disposable native fakes."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.deliver as notify_deliver
import ccx.notify.settle as notify_settle
import ccx.cli.commands.turns as commands_turns

from argparse import Namespace
import json
from pathlib import Path
import time

import pytest

from ccx import CodexClient
from ccx.cli import codex as codex_commands
from ccx.codex import ops as codex_ops
from ccx.cli.commands import _common as commands
from ccx.notify import registry as parent_notify
from ccx.store import paths
from ccx.ops.compact_ops import compact_intent_digest
from ccx.errors import InvalidRequest, SessionUnavailable
from ccx.store.store import GenerationStore
from ccx.store.root import NativeEffectIndeterminate
from test_generation_store import A, B, C, short_root  # noqa: F401 - pytest fixture
from test_compact_claude import (
    _manual_boundary, _manual_command, _native_append, _original_native_source,
)
from test_compact_codex import (
    real_compact_host, _physical_count, _settled,  # noqa: F401 - pytest fixture
)
from test_claude_native_cli import generated_hook_host  # noqa: F401 - pytest fixture
from test_notify_ext import native_caller


def _claude_args(token):
    return Namespace(
        target=f"claude:{A}", operation_id=token, notify_parent=True,
        settle=False, no_wait=False, timeout=12, json=True,
    )


def _codex_args(*, settle=True):
    return Namespace(
        target=f"codex:{B}", notify_parent=True, settle=settle,
        no_wait=False, timeout=12, json=True,
    )


def _no_daemon(monkeypatch):
    # Original machine placement is already covered by the accepted W1 tests.
    # C1 checks the producer/observer without detaching a notifier.
    monkeypatch.setattr(commands_turns, "prepare_notify_parent", lambda parent: parent)
    monkeypatch.setattr(codex_commands, "prepare_notify_parent", lambda parent: parent)
    monkeypatch.setattr(notify_daemon, "_retire_incompatible_notifyd",
                        lambda **_kwargs: None)


def test_claude_direct_prearm_same_id_replay_exact_native_boundary(
    generated_hook_host, monkeypatch, capsys,
):
    store, _call, env, _hooks = generated_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    native = _original_native_source(store)
    native_log = Path(env["FAKE_LOG"])
    parent = native_caller(monkeypatch)
    _no_daemon(monkeypatch)
    token = "c1-claude-original-operation-id"
    sid = store.record(A)["native"]["sessionId"]
    # Malformed operation IDs never reach the UUIDv5 key or a native effect.
    with pytest.raises(InvalidRequest, match="exact native operation"):
        parent_notify.create_registration(
            parent=parent, worker_engine="claude", worker_id=A, worker_name=None,
            operation={"kind": "claude-compact", "nativeSourceId": sid},
            source="compact",
        )
    # Wrong original SID never creates a direct event or submits any compact.
    with pytest.raises(SessionUnavailable, match="Compaction worker changed"):
        parent_notify.create_registration(
            parent=parent, worker_engine="claude", worker_id=A, worker_name=None,
            operation={"kind": "claude-compact", "operationId": "wrong-sid",
                       "nativeSourceId": "foreign-native-claude-session"},
            source="compact",
        )
    assert parent_notify.iter_registrations() == []
    assert store.read_receipt(A, "compact", "wrong-sid") is None
    before_turns = (store.record(A).get("stats") or {}).get("turns")
    native_intents = []
    original_begin = GenerationStore.begin_native_intent

    def before_effect(self, target, *, family, operation_id, **kwargs):
        if family == "compact" and operation_id == token:
            regs = parent_notify.iter_registrations()
            assert len(regs) == 1 and regs[0]["state"] == "armed"
            assert regs[0]["operation"]["operationId"] == token
            assert json.loads(parent_notify.registration_path(
                regs[0]["eventId"]).read_text()) == regs[0]
            assert self.read_receipt(A, "compact", token) is None
            native_intents.append(regs[0]["eventId"])
        return original_begin(self, target, family=family,
                              operation_id=operation_id, **kwargs)

    monkeypatch.setattr(GenerationStore, "begin_native_intent", before_effect)
    commands_turns.cmd_compact(_claude_args(token))
    out = json.loads(capsys.readouterr().out)
    event_id = out["notify_parent"]["event_id"]
    reg = parent_notify.read_registration(event_id)
    assert out["operation_id"] == token and out["compacted"] is False
    assert out["settled"] is False
    assert out["notify_parent"]["parent"] == parent
    assert reg["source"] == "compact"
    assert reg["worker"]["workerId"] == A and reg["worker"]["engine"] == "claude"
    assert reg["operation"] == {
        "kind": "claude-compact", "operationId": token,
        "nativeSourceId": store.record(A)["native"]["sessionId"],
    }
    assert reg["registrationKeySha256"] and reg["state"] == "armed"
    assert native_intents == [event_id]  # Event fsync precedes native intent.
    assert json.loads(parent_notify.registration_path(event_id).read_text()) == reg
    receipt = store.read_receipt(A, "compact", token)
    assert receipt["state"] == "accepted"
    assert receipt["operationId"] == token
    assert notify_settle.settlement_for_registration(reg) is None

    # The exact previously armed operation replays, without allocating an
    # event, reviving another session, or resubmitting native /compact.
    commands_turns.cmd_compact(_claude_args(token))
    again = json.loads(capsys.readouterr().out)
    assert again["notify_parent"]["event_id"] == event_id
    until = time.monotonic() + 6
    while time.monotonic() < until:
        if native_log.exists() and native_log.read_bytes().count(b"/compact") == 1:
            break
        time.sleep(.05)
    assert native_log.read_bytes().count(b"/compact") == 1
    assert len(list(parent_notify.registrations_dir().glob("*.json"))) == 1

    _native_append(native, _manual_command(), _manual_boundary("c1-original-boundary"))
    final = None
    until = time.monotonic() + 5
    while time.monotonic() < until:
        final = notify_settle.settlement_for_registration(reg)
        if final is not None:
            break
        time.sleep(.05)
    assert final and final["status"] == "completed", final
    assert final["operationStatus"] == "succeeded"
    assert final["operationId"] == token and final["compacted"] is True
    assert final["evidenceId"] == "c1-original-boundary"
    assert (final["preTokens"], final["postTokens"]) == (1234, 100)
    assert final["report"] is None
    payload = notify_deliver.payload_for_registration(reg, final)
    assert payload["operationType"] == "compact"
    assert payload["operationId"] == token and payload["operationStatus"] == "succeeded"
    assert payload["compacted"] is True and payload["report"] is None
    assert payload["deliveryId"] == event_id + ":settled"
    assert (store.record(A).get("stats") or {}).get("turns") == before_turns
    assert native_log.read_bytes().count(b"/compact") == 1

    # A global submitted receipt without a prearmed event belongs to an
    # ordinary prior compact; a new direct event cannot claim it afterward.
    ordinary_id = "ordinary-compact-not-notified"
    intent = store.begin_native_intent(
        A, family="compact", action="compact", operation_id=ordinary_id,
        intent_sha256=compact_intent_digest(
            A, generation=store.generation, native_session_id=sid,
            operation_id=ordinary_id),
    )
    with pytest.raises(NativeEffectIndeterminate):
        store.dispatch_native_intent_once(intent, bounded_submit=lambda _budget: None)
    assert store.read_receipt(A, "compact", ordinary_id)["state"] == "submitted-unknown"
    with pytest.raises(InvalidRequest, match="retroactively"):
        parent_notify.create_registration(
            parent=parent, worker_engine="claude", worker_id=A, worker_name=None,
            operation={"kind": "claude-compact", "operationId": ordinary_id,
                       "nativeSourceId": sid},
            source="compact",
        )
    assert native_log.read_bytes().count(b"/compact") == 1

    # Once an ordinary compact has submitted, a new pre-effect registration
    # cannot claim it under a different Codex return address or mode.
    with pytest.raises(InvalidRequest, match="different parent"):
        parent_notify.create_registration(
            parent={"kind": "codex-native-thread", "threadId": "foreign-parent"},
            worker_engine="claude", worker_id=A, worker_name=None,
            operation=reg["operation"], source="compact",
        )


@pytest.mark.parametrize("receipt_state,uncertain", [
    ("absent", False), ("prepared", False),
    ("submitted-unknown", True), ("unreadable", True),
])
def test_claude_direct_failure_uses_effect_receipt_not_exception(
    generated_hook_host, monkeypatch, capsys, receipt_state, uncertain,
):
    store, _call, env, _hooks = generated_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    native_caller(monkeypatch)
    _no_daemon(monkeypatch)
    token = "c1-classify-" + receipt_state
    sid = store.record(A)["native"]["sessionId"]
    observed = []
    original_read = GenerationStore.read_receipt

    class Refusal:
        def compact(self, target, *, wait, timeout, operation_id):
            # The caller MUST fsync the notification before any native intent.
            regs = parent_notify.iter_registrations()
            assert len(regs) == 1 and regs[0]["source"] == "compact"
            assert regs[0]["operation"]["operationId"] == operation_id == token
            observed.append(regs[0]["eventId"])
            if receipt_state == "unreadable":

                def unreadable(self, child, family, operation_id):
                    if family == "compact" and operation_id == token:
                        raise OSError("original compact effect receipt unavailable")
                    return original_read(self, child, family, operation_id)

                monkeypatch.setattr(GenerationStore, "read_receipt", unreadable)
            if receipt_state in {"prepared", "submitted-unknown"}:
                intent = store.begin_native_intent(
                    A, family="compact", action="compact", operation_id=token,
                    intent_sha256=compact_intent_digest(
                        A, generation=store.generation,
                        native_session_id=sid, operation_id=token),
                )
                if receipt_state == "submitted-unknown":
                    store.dispatch_native_intent_once(
                        intent, bounded_submit=lambda _budget: None)
            raise SessionUnavailable("native admission reply unavailable")

    # cmd_compact imports CcxClient locally, so patch its original import site.
    monkeypatch.setattr("ccx.client.CcxClient", Refusal)
    with pytest.raises(SystemExit) as exit_result:
        commands_turns.cmd_compact(_claude_args(token))
    assert exit_result.value.code == (6 if uncertain else 3)
    error = capsys.readouterr().err
    assert f"operation_id={token}" in error and f"event_id={observed[0]}" in error
    if receipt_state == "unreadable":
        monkeypatch.setattr(GenerationStore, "read_receipt", original_read)
    reg = parent_notify.read_registration(observed[0])
    assert reg["state"] == ("armed" if uncertain else "cancelled")
    assert reg["operation"]["nativeSourceId"] == sid
    if uncertain:
        assert notify_settle.settlement_for_registration(reg) is None
    else:
        assert reg["cancelReason"] and parent_notify.iter_registrations() == []
        # A second CLI call with the same cancelled original ID MUST NOT
        # run the core, even though no native receipt was accepted.
        with pytest.raises(SystemExit) as second:
            commands_turns.cmd_compact(_claude_args(token))
        assert second.value.code == 5
        capsys.readouterr()
        assert observed == [reg["eventId"]]


def test_codex_direct_exact_native_compact_and_recursive_frozen_verdict(
    real_compact_host, monkeypatch, capsys,
):
    store, trace, gate = real_compact_host
    parent = native_caller(monkeypatch)
    _no_daemon(monkeypatch)
    count = _physical_count(trace)
    # A Codex busy refusal may leave a PREPARED global intent. That is
    # positive no-effect evidence even when the command's return is data.
    prepared_tokens = []

    def prepared_refusal(target, *, operation_id, wait, timeout):
        regs = parent_notify.iter_registrations()
        assert len(regs) == 1 and regs[0]["operation"]["operationId"] == operation_id
        native = store.record(B)["native"]
        store.begin_native_intent(
            B, family="codex-compact", action="compact",
            operation_id=operation_id,
            intent_sha256=codex_ops.compact_intent_digest(
                B, store.generation, native["threadId"],
                native["threadSource"], operation_id),
        )
        prepared_tokens.append(operation_id)
        return {"status": "prepared", "receipt_status": "prepared",
                "compacted": False, "reason": "busy before native compact"}

    with monkeypatch.context() as patch:
        patch.setattr(codex_ops, "compact_worker", prepared_refusal)
        with pytest.raises(SystemExit) as refusal:
            codex_commands.cmd_compact(_codex_args(settle=False))
    assert refusal.value.code == 5
    capsys.readouterr()
    assert _physical_count(trace) == count
    first = parent_notify.iter_registrations(include_terminal=True)
    assert len(first) == 1 and first[0]["state"] == "cancelled"
    assert store.read_receipt(B, "codex-compact", prepared_tokens[0])["state"] == "prepared"

    # The existing fake app-server processes a compact gate synchronously.
    # Create the descendant BEFORE withholding that native compact terminal.
    child = CodexClient().create(cwd=store.record(B)["cwd"], worker_id=C)
    assert child.worker_id == C

    # A recursive compact notification is published under an exact Codex
    # parent, before the native compact effect, even with --settle.
    original_begin = GenerationStore.begin_native_intent
    before_native = []

    def pre_effect(self, target, *, family, operation_id, **kwargs):
        if family == "codex-compact":
            regs = parent_notify.iter_registrations()
            assert len(regs) == 1 and regs[0]["state"] == "armed"
            assert regs[0]["operation"]["operationId"] == operation_id
            assert self.read_receipt(B, family, operation_id) is None
            before_native.append(regs[0]["eventId"])
        return original_begin(self, target, family=family,
                              operation_id=operation_id, **kwargs)

    monkeypatch.setattr(GenerationStore, "begin_native_intent", pre_effect)
    codex_commands.cmd_compact(_codex_args())
    out = json.loads(capsys.readouterr().out)
    token = out["operation_id"]
    reg = parent_notify.read_registration(out["notify_parent"]["event_id"])
    assert out["worker"] == f"codex:{B}" and out["settled"] is False
    assert out["notify_parent"]["settle"] is True
    assert reg["parent"] == parent and reg["settlementMode"] == "orchestration"
    assert before_native == [reg["eventId"]]
    assert reg["operation"] == {
        "kind": "codex-compact", "operationId": token,
        "nativeSourceId": store.record(B)["native"]["threadId"],
    }
    assert store.read_receipt(B, "codex-compact", token)["state"] in {
        "accepted", "submitted-unknown",
    }
    assert _physical_count(trace) == count + 1
    assert notify_settle.settlement_for_registration(reg) is None

    # Build one *existing* child registration, not another compact/receipt
    # framework. It keeps the original compact verdict frozen while B's
    # descendant notification is unresolved.
    from ccx.codex import delivery as codex_delivery
    descendant_token = "c1-original-descendant"
    codex_delivery.prepare(
        f"codex:{C}", descendant_token, "original descendant",
        mode="new_turn_only",
    )
    child_parent = {"kind": "ccx-codex", "workerId": B,
                    "threadId": store.record(B)["native"]["threadId"],
                    "generation": store.generation}
    descendant = parent_notify.create_registration(
        parent=child_parent, worker_engine="codex", worker_id=C,
        worker_name=None, operation={"kind": "codex-turn",
                                    "deliveryId": descendant_token},
        source="send",
    )
    gate.touch()
    settled = _settled(B, token)
    assert settled["status"] == "succeeded" and settled["compacted"] is True
    assert settled["compactTurnId"] and settled["contextItemId"]
    assert settled["nativeTurnStatus"] == "completed"
    assert notify_settle.settlement_for_registration(reg) is None
    frozen = parent_notify.read_registration(reg["eventId"])["operationSettlement"]
    assert frozen["status"] == "completed" and frozen["operationStatus"] == "succeeded"
    assert frozen["operationId"] == token and frozen["report"] is None
    assert frozen["compactTurnId"] == settled["compactTurnId"]
    assert frozen["contextItemId"] == settled["contextItemId"]

    store.mutate_record(B, patches={
        "lastReport": {"turnId": "WRONG-later-model-turn",
                       "text": "WRONG newer assistant reply"},
    })
    assert notify_settle.settlement_for_registration(reg) is None
    assert parent_notify.read_registration(reg["eventId"])["operationSettlement"] == frozen
    parent_notify.cancel_registration(descendant["eventId"], "descendant finished")
    result = notify_settle.settlement_for_registration(reg)
    assert result and result["settlementVerdict"] == "settled", result
    assert result["operationId"] == token and result["operationStatus"] == "succeeded"
    assert result["report"] is None and "WRONG" not in str(result)
    payload = notify_deliver.payload_for_registration(reg, result)
    assert payload["settle"] is True and payload["operationType"] == "compact"
    assert payload["operationId"] == token
    assert payload["deliveryId"] == reg["eventId"] + ":settled"
    assert payload["report"] is None
    assert _physical_count(trace) == count + 1

    # A core can return a pending STATUS rather than raise after its native
    # ACK was lost. Submitted-unknown must still exit 6 and retain ONE event,
    # never report successful acceptance or send a second compact.
    monkeypatch.setattr(GenerationStore, "begin_native_intent", original_begin)
    ambiguous_tokens = []

    def lost_ack_status(target, *, operation_id, wait, timeout):
        native = store.record(B)["native"]
        intent = store.begin_native_intent(
            B, family="codex-compact", action="compact", operation_id=operation_id,
            intent_sha256=codex_ops.compact_intent_digest(
                B, store.generation, native["threadId"],
                native["threadSource"], operation_id),
        )
        with pytest.raises(NativeEffectIndeterminate):
            store.dispatch_native_intent_once(
                intent, bounded_submit=lambda _budget: None)
        ambiguous_tokens.append(operation_id)
        return {"status": "submitted", "receipt_status": "submitted-unknown",
                "compacted": False}

    with monkeypatch.context() as patch:
        patch.setattr(codex_ops, "compact_worker", lost_ack_status)
        with pytest.raises(SystemExit) as unproven:
            codex_commands.cmd_compact(_codex_args(settle=False))
    assert unproven.value.code == 6
    error = capsys.readouterr().err
    assert f"operation_id={ambiguous_tokens[0]}" in error and "event_id=" in error
    assert store.read_receipt(B, "codex-compact", ambiguous_tokens[0])["state"] == "submitted-unknown"
    matching = [r for r in parent_notify.iter_registrations(include_terminal=True)
                if r["operation"].get("operationId") == ambiguous_tokens[0]]
    assert len(matching) == 1 and matching[0]["state"] == "armed"
    assert notify_settle.settlement_for_registration(matching[0]) is None
    assert _physical_count(trace) == count + 1
