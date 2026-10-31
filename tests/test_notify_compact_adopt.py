"""C2: independent observers adopt only an authenticated already-active native compact."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.deliver as notify_deliver
import ccx.notify.select as notify_select
import ccx.notify.settle as notify_settle
import ccx.cli.commands.turns as commands_turns
import ccx.ops.wait as ops_wait

from argparse import Namespace
import json
from pathlib import Path
import time
from uuid import UUID

import pytest

from ccx import CodexClient
from ccx.cli import codex as codex_commands
from ccx.codex import delivery as codex_delivery
from ccx.codex import ops as codex_ops
from ccx.cli.commands import _common as commands
from ccx.notify import registry as parent_notify
from ccx.store import paths
from ccx.ops import common as protocol_ops
from ccx.client import CcxClient
from ccx.errors import OperationConflict, SessionUnavailable
from ccx.store.store import GenerationStore
from test_generation_store import A, B, C, short_root  # noqa: F401 - pytest fixture
from test_compact_claude import (
    _manual_boundary, _manual_command, _native_append, _original_native_source,
)
from test_compact_codex import (
    real_compact_host, _physical_count, _settled,  # noqa: F401 - pytest fixture
)
from test_claude_native_cli import generated_hook_host  # noqa: F401 - pytest fixture
from test_notify_registration import originals  # noqa: F401 - pytest fixture
from test_notify_claude import owners  # noqa: F401 - pytest fixture
from test_notify_ext import native_caller


def _notify_args(engine: str, *, settle=False) -> Namespace:
    return Namespace(target=f"{engine}:{A if engine == 'claude' else B}",
                     json=True, next_turn=False, settle=settle)


def _no_supervisor(monkeypatch) -> None:
    # Existing W1 tests prove actual Codex machine delivery; C2 proves the
    # source selection and unchanged provider effects, not network retries.
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_k: None)
    monkeypatch.setattr(notify_daemon, "_retire_incompatible_notifyd",
                        lambda **_k: None)


def _adopted(reg, engine: str, operation_id: str, parent: dict) -> None:
    assert reg["version"] == 3 and reg["state"] == "armed"
    assert reg["source"] == "notify" and reg["parent"] == parent
    assert reg["worker"]["engine"] == engine
    assert reg["worker"]["workerId"] == (A if engine == "claude" else B)
    assert reg["registrationKeySha256"] is None
    event = UUID(reg["eventId"])
    assert event.version == 4 and str(event) == reg["eventId"]
    operation = reg["operation"]
    assert operation["kind"] == f"{engine}-compact"
    assert operation["selectionMode"] == "active-compact"
    assert operation["operationId"] == operation_id
    assert operation.keys() == {
        "kind", "selectionMode", "operationId", "nativeSourceId"}
    assert not any(key in operation for key in ("turnId", "selectedTurnRef",
                                                 "deliveryId", "targetCompactCount"))
    assert json.loads(parent_notify.registration_path(reg["eventId"]).read_text()) == reg


def test_claude_active_native_compact_two_parents_no_extra_effect_and_original_boundary(
    generated_hook_host, monkeypatch, capsys,
):
    store, _call, env, _hooks = generated_hook_host
    monkeypatch.setenv("CCX_ROOT", str(store.root))  # this test's own notify root
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    source = _original_native_source(store)
    native_log = Path(env["FAKE_LOG"])
    _no_supervisor(monkeypatch)
    operation_id = "c2-claude-one-actual-original"
    original = CcxClient().compact(A, wait=False, timeout=12, operation_id=operation_id)
    assert original.operation_id == operation_id and original.compacted is False
    assert store.read_receipt(A, "compact", operation_id)["state"] == "accepted"
    until = time.monotonic() + 5
    while time.monotonic() < until:
        if native_log.exists() and native_log.read_bytes().count(b"/compact") == 1:
            break
        time.sleep(.05)
    assert native_log.read_bytes().count(b"/compact") == 1

    seen = []
    for thread in ("c2-parent-native-1", "c2-parent-native-2"):
        parent = native_caller(monkeypatch, thread_id=thread)
        commands_turns.cmd_notify(_notify_args("claude"))
        public = json.loads(capsys.readouterr().out)
        reg = parent_notify.read_registration(public["event_id"])
        _adopted(reg, "claude", operation_id, parent)
        assert public["selection_mode"] == "active-compact"
        assert public["selected_state"] == "compacting"
        assert public["operation_type"] == "compact"
        assert public["operation_id"] == operation_id
        assert public["selected_turn_ref"] is None
        assert public["delivery_id"] is None
        assert public["armed"] is True
        assert notify_settle.settlement_for_registration(reg) is None
        seen.append(reg)
    assert seen[0]["eventId"] != seen[1]["eventId"]
    assert native_log.read_bytes().count(b"/compact") == 1
    assert len(parent_notify.iter_registrations()) == 2

    # The same operation ID with a foreign native source must never mint
    # another independent UUIDv4 notification or borrow another result.
    with pytest.raises(SessionUnavailable):
        parent_notify.create_registration(
            parent=seen[0]["parent"], worker_engine="claude", worker_id=A,
            worker_name=None,
            operation={**seen[0]["operation"], "nativeSourceId": "foreign-claude-session"},
            source="notify",
        )
    assert len(parent_notify.iter_registrations()) == 2
    assert native_log.read_bytes().count(b"/compact") == 1

    # Selection is not publication. If the original active compact has
    # changed by the locked publication check, refuse rather than mint an
    # event for a different/ended operation.
    with monkeypatch.context() as patch:
        patch.setattr(notify_select, "_active_compact_selection", lambda *_a: None)
        with pytest.raises(SessionUnavailable, match="changed before publication"):
            parent_notify.create_registration(
                parent={"kind": "codex-native-thread",
                        "threadId": "c2-parent-mid-arm-race"},
                worker_engine="claude", worker_id=A, worker_name=None,
                operation=seen[0]["operation"], source="notify",
            )
    assert len(parent_notify.iter_registrations()) == 2
    assert native_log.read_bytes().count(b"/compact") == 1

    # This observation is NOT an ordinary turn and cannot use cached reports.
    _native_append(source, _manual_command(), _manual_boundary("c2-exact-claude-boundary"))
    until = time.monotonic() + 6
    verdict = None
    while time.monotonic() < until:
        verdict = notify_settle.settlement_for_registration(seen[0])
        if verdict:
            break
        time.sleep(.05)
    assert verdict and verdict["status"] == "completed", verdict
    assert verdict["operationId"] == operation_id
    assert verdict["evidenceId"] == "c2-exact-claude-boundary"
    assert verdict["operationStatus"] == "succeeded"
    for reg in seen:
        final = notify_settle.settlement_for_registration(reg)
        assert final and final["status"] == "completed"
        assert final["report"] is None
        payload = notify_deliver.payload_for_registration(reg, final)
        assert payload["source"] == "notify"
        assert payload["selectionMode"] == "active-compact"
        assert payload["operationType"] == "compact"
        assert payload["operationId"] == operation_id
        assert payload["report"] is None
        assert payload["deliveryId"] == reg["eventId"] + ":settled"
    assert native_log.read_bytes().count(b"/compact") == 1

    # A completed original may be observed by OLD arms, but must not be
    # newly adopted after the original operation is terminal.
    with pytest.raises((OperationConflict, SessionUnavailable)):
        parent_notify.create_registration(
            parent=seen[0]["parent"], worker_engine="claude", worker_id=A,
            worker_name=None, operation=seen[0]["operation"], source="notify")
    assert len(parent_notify.iter_registrations()) == 2


def test_codex_native_active_compact_independent_observers_recursive_original_freeze(
    real_compact_host, monkeypatch, capsys,
):
    store, trace, gate = real_compact_host
    monkeypatch.setenv("CCX_ROOT", str(store.root))  # this test's own notify root
    _no_supervisor(monkeypatch)
    # The fake app server serializes native compact while its terminal gate
    # is held, so any descendant must be CREATED before the native compact.
    created = CodexClient().create(cwd=store.record(B)["cwd"], worker_id=C)
    assert created.worker_id == C
    operation_id = "c2-codex-exact-native-compact"
    original = codex_ops.compact_worker(
        B, operation_id=operation_id, wait=False, timeout=12)
    assert original["operationId"] == operation_id
    assert original["compacted"] is False
    assert store.read_receipt(B, "codex-compact", operation_id)["state"] in {
        "submitted-unknown", "accepted"}
    count = _physical_count(trace)
    assert count == 1
    before_notify = trace.read_text()
    regs = []
    for thread, settle in (("c2-codex-parent-1", False),
                           ("c2-codex-parent-2", True)):
        parent = native_caller(monkeypatch, thread_id=thread)
        codex_commands.cmd_notify(_notify_args("codex", settle=settle))
        public = json.loads(capsys.readouterr().out)
        reg = parent_notify.read_registration(public["event_id"])
        _adopted(reg, "codex", operation_id, parent)
        assert public["selected_state"] == "compacting"
        assert public["settle"] is settle
        assert public["operation_id"] == operation_id
        assert public["selected_turn_ref"] is None
        assert notify_settle.settlement_for_registration(reg) is None
        regs.append(reg)
    assert regs[0]["eventId"] != regs[1]["eventId"]
    assert trace.read_text() == before_notify  # no native compact/model turn
    assert _physical_count(trace) == count

    with pytest.raises(SessionUnavailable):
        parent_notify.create_registration(
            parent=regs[0]["parent"], worker_engine="codex", worker_id=B,
            worker_name=None,
            operation={**regs[0]["operation"], "nativeSourceId": "foreign-thread"},
            source="notify",
        )
    assert len(parent_notify.iter_registrations()) == 2
    assert _physical_count(trace) == count

    # The existing Codex recursive graph defers delivery without borrowing a
    # later model turn's report or losing the ORIGINAL compact's native IDs.
    descendant_id = "c2-only-descendant"
    codex_delivery.prepare(
        f"codex:{C}", descendant_id, "original pending child",
        mode="new_turn_only",
    )
    descendant = parent_notify.create_registration(
        parent={"kind": "ccx-codex", "workerId": B,
                "threadId": store.record(B)["native"]["threadId"],
                "generation": store.generation},
        worker_engine="codex", worker_id=C, worker_name=None,
        operation={"kind": "codex-turn", "deliveryId": descendant_id},
        source="send",
    )
    gate.touch()
    exact = _settled(B, operation_id)
    assert exact["status"] == "succeeded" and exact["compacted"] is True
    assert exact["compactTurnId"] and exact["contextItemId"]
    first = notify_settle.settlement_for_registration(regs[0])
    assert first and first["status"] == "completed"
    assert first["compactTurnId"] == exact["compactTurnId"]
    assert first["report"] is None
    assert notify_settle.settlement_for_registration(regs[1]) is None
    frozen = parent_notify.read_registration(regs[1]["eventId"])["operationSettlement"]
    assert frozen["operationId"] == operation_id
    assert frozen["compactTurnId"] == exact["compactTurnId"]
    assert frozen["contextItemId"] == exact["contextItemId"]
    assert frozen["report"] is None
    store.mutate_record(B, patches={
        "lastReport": {"turnId": "WRONG-later-model-turn",
                       "text": "WRONG newer model answer"},
    })
    assert notify_settle.settlement_for_registration(regs[1]) is None
    assert parent_notify.read_registration(regs[1]["eventId"])["operationSettlement"] == frozen
    parent_notify.cancel_registration(descendant["eventId"], "descendant settled")
    final = notify_settle.settlement_for_registration(regs[1])
    assert final and final["settlementVerdict"] == "settled"
    assert final["operationId"] == operation_id
    assert final["compactTurnId"] == exact["compactTurnId"]
    assert final["report"] is None and "WRONG" not in str(final)
    payload = notify_deliver.payload_for_registration(regs[1], final)
    assert payload["source"] == "notify" and payload["operationType"] == "compact"
    assert payload["operationId"] == operation_id
    assert payload["report"] is None and payload["settle"] is True
    assert payload["deliveryId"] == regs[1]["eventId"] + ":settled"
    assert _physical_count(trace) == count


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_unproven_active_marker_refuses_before_event_or_model_turn(engine, request, monkeypatch):
    # Existing fake owners have confirmed native sources, but no ORIGINAL
    # compact receipt. A plausible marker alone can never reserve an R.
    _no_supervisor(monkeypatch)
    if engine == "claude":
        _root, store = request.getfixturevalue("owners")
        monkeypatch.setenv("CCX_ROOT", str(store.root))
        from ccx.ops import compact_ops
        monkeypatch.setattr(compact_ops, "active_operation",
                            lambda *a: {"operation_id": "fake-unsubmitted-compact"})
        target, wid = f"claude:{A}", A
    else:
        store, trace, _tmp = request.getfixturevalue("originals")
        monkeypatch.setenv("CCX_ROOT", str(store.root))
        original_record = GenerationStore.record

        def fake_marker(self, target):
            value = original_record(self, target)
            if target in (f"codex:{B}", B):
                return {**value, "activeCompact": {
                    "operationId": "fake-unsubmitted-compact"}}
            return value

        monkeypatch.setattr(GenerationStore, "record", fake_marker)
        before = trace.read_text()
        target, wid = f"codex:{B}", B
    native_caller(monkeypatch, thread_id="c2-unproven-native-parent")
    monkeypatch.setattr(
        ops_wait, "wait_worker",
        lambda *a, **kw: pytest.fail("unproven compact fell through to model-turn read"),
    )
    with pytest.raises(OperationConflict, match="no recorded submission"):
        notify_select.select_current_turn(target)
    original = store.record(target)["native"]
    native_id = original["sessionId" if engine == "claude" else "threadId"]
    with pytest.raises(SessionUnavailable, match="no authenticated original effect"):
        parent_notify.create_registration(
            parent={"kind": "codex-native-thread",
                    "threadId": "c2-unproven-native-parent"},
            worker_engine=engine, worker_id=wid, worker_name=None,
            operation={"kind": f"{engine}-compact",
                       "selectionMode": "active-compact",
                       "operationId": "fake-unsubmitted-compact",
                       "nativeSourceId": native_id},
            source="notify",
        )
    assert parent_notify.iter_registrations() == []
    if engine == "codex":
        assert trace.read_text() == before
