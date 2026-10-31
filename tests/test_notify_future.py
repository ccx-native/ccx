"""Future Claude notification: one durable cursor, one exact post-arm R."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.select as notify_select
import ccx.notify.settle as notify_settle
import ccx.cli.commands.turns as commands_turns
import ccx.ops.wait as ops_wait

from argparse import Namespace
import json
from pathlib import Path
import time

import pytest

from ccx.codex import control as codex_control
from ccx.cli.commands import _common as commands
from ccx.ops import compact_ops
from ccx import constants
from ccx.ops import event_feed
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.rpc import rpc
from ccx.errors import SessionUnavailable
from ccx.notify.client import ParentNotificationClient
from test_generation_store import A, C, short_root
from test_notify_claude import owners
from test_notify_receiptless import _claude_line, _claude_sources
from test_notify_standalone import _native_caller, _managed_caller
from test_notify_ext import FakeControl
from test_claude_native_cli import generated_hook_host
from test_notify_registration import machine_originals
from test_delivery_flow import _activate_fake_claude
from ccx.ops.creation_steps import prepare_worker


def _event(store, *, epoch="one", worker_id=A, **fields):
    path = store.worker_path(worker_id, "events.ndjson")
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text().splitlines() if path.exists() else []
    seq = (sum(json.loads(row).get("epoch") == epoch for row in existing) + 1)
    with path.open("a") as handle:
        handle.write(json.dumps({"epoch": epoch, "seq": seq, "ts": time.time(),
                                 **fields}) + "\n")
    path.chmod(0o600)
    return path


def _fake_live(owners, tmp_path, monkeypatch):
    root, store = owners
    original = _claude_sources(store, monkeypatch, tmp_path)
    monkeypatch.setenv("CCX_ROOT", str(root))
    store.patch_lifecycle(A, changes={"state": "idle"})
    _event(store, type="state", **{"from": "booting", "to": "idle"})
    current = {"state": "idle", "ref": None, "gate": None, "settled": None}
    def status(*_args, **_kw):
        return {"ok": True, "state": current["state"], "compacting": False,
                "activeTurnRef": current["ref"], "waitingFor": current["gate"]}
    def wait(*_args, **_kw):
        return {"worker": "claude:" + A, "state": current["state"],
                "status": current["state"],
                "reason": "worker is idle" if current["state"] == "idle" else None,
                "active_turn_ref": current["ref"], "waiting_for": current["gate"],
                "last_settled_turn": {"turn_ref": current["settled"]} if current["settled"] else None}
    monkeypatch.setattr(rpc, "_rpc", status)
    monkeypatch.setattr(ops_wait, "wait_worker", wait)
    monkeypatch.setattr(compact_ops, "active_operation", lambda *_: None)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    _native_caller(monkeypatch)
    # Fake only: positive presence of the existing host socket, never a process.
    from ccx.store import paths
    host_socket = tmp_path / "fake-host-present"
    host_socket.touch()
    monkeypatch.setattr(paths, "sock_path", lambda *_: host_socket)
    return store, original, current


def _arm(monkeypatch, store):
    selected = notify_select.select_future_turn("claude:" + A)
    reg = parent_notify.create_registration(
        parent=_native_caller(monkeypatch), worker_engine="claude", worker_id=A,
        worker_name=None, operation=selected["operation"], source="notify")
    assert reg["operation"]["selectedTurnRef"] is None
    assert "deliveryId" not in reg["operation"]
    return reg


def test_future_idle_arm_quiet_until_exact_r_then_one_final_wake(
    owners, tmp_path, monkeypatch, capsys,
):
    store, source, current = _fake_live(owners, tmp_path, monkeypatch)
    monkeypatch.setattr(codex_control, "CodexControlClient", lambda: control)
    control = FakeControl()
    commands_turns.cmd_notify(Namespace(target="claude:" + A, next_turn=True, json=True))
    output = json.loads(capsys.readouterr().out)
    assert output["armed"] is True and output["wait_mode"] == "next-turn"
    assert output["selection_mode"] == "future"
    reg = parent_notify.read_registration(output["event_id"])
    assert reg["eventId"] == output["event_id"] and reg["registrationKeySha256"] is None
    assert notify_settle.settlement_for_registration(reg) is None
    assert parent_notify._current_worker_name(reg["worker"])  # full UUID key
    notifyd.process_registration(reg)
    pending = parent_notify.read_registration(reg["eventId"])
    assert pending["state"] == "armed" and pending.get("payload") is None
    assert pending.get("deliveryAttemptedAtMs") is None
    assert control.methods("turn/start") == []  # no wake while future R absent

    r1 = "ccx-turn:claude:v1:future-genuine-1"
    r2 = "ccx-turn:claude:v1:future-genuine-2"
    _event(store, type="turn_start", turn_ref=r1, turn=1,
           source="user-prompt-submit", report_start_offset=0)
    assert notify_settle.settlement_for_registration(reg) is None
    latched = parent_notify.read_registration(reg["eventId"])
    assert latched["operation"]["selectedTurnRef"] == r1
    _claude_line(source, {"type": "assistant", "message": {"content": [
        {"type": "text", "text": "Exact future R1"}]}})
    end = source.stat().st_size
    _event(store, type="turn_complete", turn_ref=r1, turn=1)
    _event(store, type="turn_start", turn_ref=r2, turn=2, report_start_offset=end)
    _claude_line(source, {"type": "assistant", "message": {"content": [
        {"type": "text", "text": "WRONG future R2"}]}})
    current["settled"] = r1
    result = notify_settle.settlement_for_registration(latched)
    assert (result["status"], result["turnRef"], result["report"]) == (
        "completed", r1, "Exact future R1")
    notifyd.process_registration(latched)
    final = parent_notify.read_registration(reg["eventId"])
    assert final["state"] == "delivered" and final["payload"]["report"] == "Exact future R1"
    notifyd.process_registration(final)
    assert len(control.methods("turn/start")) == 1
    assert [payload["phase"] for _, payload in control.placed] == ["settled"]
    assert final["payload"]["deliveryId"] == reg["eventId"] + ":settled"
    assert ParentNotificationClient().get(reg["eventId"])["selected_turn_ref"] == r1


@pytest.mark.parametrize("case", ["older-recovery", "older-terminal", "next-epoch", "cursor-gap"])
def test_future_first_start_is_f1_not_prearm_recovery_or_gap(
    owners, tmp_path, monkeypatch, case,
):
    store, _source, current = _fake_live(owners, tmp_path, monkeypatch)
    old = "ccx-turn:claude:v1:prearm-older"
    _event(store, type="turn_start", turn_ref=old, turn=1, report_start_offset=0)
    _event(store, type="turn_complete", turn_ref=old, turn=1)
    reg = _arm(monkeypatch, store)
    if case == "older-recovery":
        _event(store, type="turn_identity_recovered", turn_ref=old, turn=1,
               report_start_offset=0)
        assert notify_settle.settlement_for_registration(reg) is None
        assert parent_notify.read_registration(reg["eventId"])["operation"]["selectedTurnRef"] is None
    if case == "older-terminal":
        _event(store, type="turn_complete", turn_ref=old, turn=1)
        assert notify_settle.settlement_for_registration(reg) is None
        assert parent_notify.read_registration(reg["eventId"])["operation"]["selectedTurnRef"] is None
    if case == "cursor-gap":
        # Rotated-out baseline must never replay a retained new-epoch R.
        _event(store, epoch="later", type="turn_start",
               turn_ref="ccx-turn:claude:v1:new-after-gap", turn=2,
               report_start_offset=0)
        path = store.worker_path(A, "events.ndjson")
        lines = [row for row in path.read_text().splitlines()
                 if json.loads(row)["epoch"] == "later"]
        path.write_text("\n".join(lines) + "\n")
        assert notify_settle.settlement_for_registration(reg) is None
        assert parent_notify.read_registration(reg["eventId"])["operation"]["selectedTurnRef"] is None
        return
    new = "ccx-turn:claude:v1:genuine-after-prearm"
    _event(store, epoch="later" if case == "next-epoch" else "one",
           type="turn_start", turn_ref=new, turn=2, report_start_offset=0)
    assert notify_settle.settlement_for_registration(reg) is None
    assert parent_notify.read_registration(reg["eventId"])["operation"]["selectedTurnRef"] == new
    _event(store, epoch="later" if case == "next-epoch" else "one",
           type="turn_start", turn_ref="ccx-turn:claude:v1:even-later", turn=3)
    assert notify_settle.settlement_for_registration(reg) is None
    assert parent_notify.read_registration(reg["eventId"])["operation"]["selectedTurnRef"] == new


@pytest.mark.parametrize("terminal,start_offset,expected", [
    ("turn_complete", None, "unknown"),
    ("turn_settled", None, "unknown"),
    ("turn_settled", 0, "unknown"),
])
def test_future_first_no_offset_or_liveness_terminal_is_unknown_null(
    owners, tmp_path, monkeypatch, terminal, start_offset, expected,
):
    store, _source, _current = _fake_live(owners, tmp_path, monkeypatch)
    reg = _arm(monkeypatch, store)
    r1 = "ccx-turn:claude:v1:missed-hooks-first"
    fields = {"report_start_offset": start_offset} if start_offset is not None else {}
    _event(store, type="turn_start", turn_ref=r1, turn=1,
           source="stop-fallback", **fields)
    _event(store, type=terminal, turn_ref=r1, turn=1,
           **({"outcome": "completed"} if terminal == "turn_settled" else {}))
    _event(store, type="turn_start", turn_ref="ccx-turn:claude:v1:later", turn=2,
           report_start_offset=0)
    result = notify_settle.settlement_for_registration(reg)
    assert result["status"] == expected and result["report"] is None
    assert result["turnRef"] == r1
    assert parent_notify.read_registration(reg["eventId"])["operation"]["selectedTurnRef"] == r1


def test_future_new_gate_requires_postarm_evidence_and_exact_active_ref(
    owners, tmp_path, monkeypatch,
):
    store, _source, current = _fake_live(owners, tmp_path, monkeypatch)
    reg = _arm(monkeypatch, store)
    r1 = "ccx-turn:claude:v1:gate-r1"
    _event(store, type="turn_start", turn_ref=r1, turn=1, report_start_offset=0)
    current.update(state="waiting", ref=r1, gate={"id": "gate-one", "tool": "AskUserQuestion"})
    assert notify_settle.settlement_for_registration(reg) is None
    _event(store, type="state", **{"from": "busy", "to": "waiting"})
    waiting = notify_settle.settlement_for_registration(reg)
    assert waiting["status"] == "waiting" and waiting["report"] is None
    assert waiting["waitingFor"]["id"] == "gate-one"
    current["ref"] = "ccx-turn:claude:v1:later-gate"
    assert notify_settle.settlement_for_registration(reg) is None


@pytest.mark.parametrize("case", ["nonidle", "compact", "changed-sid", "child-dead"])
def test_future_admission_and_source_or_terminal_no_child_input(
    owners, tmp_path, monkeypatch, case,
):
    store, _source, current = _fake_live(owners, tmp_path, monkeypatch)
    if case == "nonidle":
        store.patch_lifecycle(A, changes={"state": "busy"})
        with pytest.raises(SessionUnavailable, match="live idle"):
            notify_select.select_future_turn("claude:" + A)
    elif case == "compact":
        monkeypatch.setattr(compact_ops, "active_operation",
                            lambda *_: {"operation_id": "in-progress"})
        with pytest.raises(SessionUnavailable, match="live idle"):
            notify_select.select_future_turn("claude:" + A)
        with pytest.raises(SystemExit) as denied:
            commands_turns.cmd_notify(Namespace(target="claude:" + A,
                                          next_turn=True, json=True))
        assert denied.value.code == 3
    else:
        reg = _arm(monkeypatch, store)
        if case == "changed-sid":
            modified = {**reg, "operation": {**reg["operation"],
                "nativeSourceId": "foreign-native-session"}}
            with pytest.raises(SessionUnavailable, match="native source changed"):
                notify_settle.settlement_for_registration(modified)
        else:
            store.patch_lifecycle(A, changes={"state": "dead", "lastError": "explicit stop"})
            result = notify_settle.settlement_for_registration(reg)
            assert result["status"] == "dead" and result["report"] is None
    if case in {"nonidle", "compact"}:
        assert parent_notify.iter_registrations() == []


def test_future_actual_generated_hook_after_armed_idle_without_notify_child_send(
    generated_hook_host, monkeypatch,
):
    from ccx.store import paths
    from test_compact_claude import _original_native_source

    store, call, env, hooks = generated_hook_host
    monkeypatch.setenv("CCX_ROOT", env["CCX_ROOT"])
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    _original_native_source(store)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    _native_caller(monkeypatch)
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        try:
            selected = notify_select.select_future_turn("claude:" + A)
            break
        except SessionUnavailable:
            time.sleep(.05)
    else:
        pytest.fail("generated hook host never served a live idle initial baseline")
    reg = parent_notify.create_registration(
        parent=_native_caller(monkeypatch), worker_engine="claude", worker_id=A,
        worker_name=None, operation=selected["operation"], source="notify")
    assert notify_settle.settlement_for_registration(reg) is None
    sent = call("send", A, "native fake generated future turn", "--no-wait",
                "--delivery-id", "future-example-trigger", "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    terminal = None
    deadline = time.monotonic() + 18
    while time.monotonic() < deadline:
        current = store.record(A).get("lastSettledTurn") or {}
        if current.get("outcome") == "completed":
            terminal = current
            break
        time.sleep(.08)
    assert terminal and terminal["turnRef"].startswith("ccx-turn:claude:v1:")
    # Allow the generated hook terminal broadcast to follow its record CAS.
    deadline = time.monotonic() + 10
    value = None
    while time.monotonic() < deadline:
        value = notify_settle.settlement_for_registration(reg)
        if value and value["status"] == "completed":
            break
        time.sleep(.06)
    assert value and value["turnRef"] == terminal["turnRef"]
    assert parent_notify.read_registration(reg["eventId"])["operation"]["selectedTurnRef"] == terminal["turnRef"]
    deadline = time.monotonic() + 5
    stop_logged = False
    while time.monotonic() < deadline:
        stop_logged = (hooks.exists() and any(json.loads(line)["event"] == "Stop"
                       for line in hooks.read_text().splitlines()))
        if stop_logged:
            break
        time.sleep(.05)
    assert stop_logged  # generated-hook child logging follows durable host Stop



def test_future_f1_rejects_initial_preselection_and_later_r2_row_tampering(
    owners, tmp_path, monkeypatch,
):
    store, _source, _current = _fake_live(owners, tmp_path, monkeypatch)
    reg = _arm(monkeypatch, store)
    r1 = "ccx-turn:claude:v1:eligible-first"
    r2 = "ccx-turn:claude:v1:chosen-later-forgery"
    _event(store, type="turn_start", turn_ref=r1, turn=1, report_start_offset=0)
    _event(store, type="turn_start", turn_ref=r2, turn=2, report_start_offset=0)
    wrong = {**reg["operation"], "selectedTurnRef": r2}
    from ccx.errors import InvalidRequest
    with pytest.raises(InvalidRequest, match="arm before"):
        parent_notify.create_registration(
            parent=_native_caller(monkeypatch), worker_engine="claude", worker_id=A,
            worker_name=None, operation=wrong, source="notify")
    with pytest.raises(SessionUnavailable, match="first post-arm"):
        notify_settle.settlement_for_registration({**reg, "operation": wrong})
    assert notify_settle.settlement_for_registration(reg) is None
    selected = parent_notify.read_registration(reg["eventId"])
    assert selected["operation"]["selectedTurnRef"] == r1
