"""Claude new uses the existing one-native-create and parent registration receipts.

All native Claude turns run the accepted disposable fake under a real CCX host.
The parent notifier is inert; no actual Codex parent wake or provider is used.
"""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.settle as notify_settle
import ccx.cli.commands.create as commands_create

import json
import time
import uuid

import pytest

from ccx.claude import delivery as claude_delivery
from ccx.cli.commands import _common as commands
from ccx.ops import creation
from ccx.notify import registry as parent_notify
from ccx.cli import parser
from ccx.errors import CreatePartialFailure, InvalidRequest
from ccx.ops.terminate import confirmed_kill_uuid
from ccx.store.store import GenerationStore
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, C, short_root  # noqa: F401 - fixture
from test_create_cross_surface import _deadline_fake_claude
from test_notify_registration import machine_originals, _n1c_managed_parent  # noqa: F401 - fixture
from test_notify_ext import native_caller

D = "dddddddd-1234-4000-8000-000000000004"


@pytest.fixture
def fake_claude(short_root, tmp_path, monkeypatch):
    """Real original host/SessionStart with a fake Claude executable only."""
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    native_input = tmp_path / "claude-create-notify-input.log"
    monkeypatch.setenv("FAKE_LOG", str(native_input))
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    store = require_ccx_active(short_root)
    yield store, launches, native_input
    for worker_id in (C, D):
        if store._owner(worker_id) is not None:
            confirmed_kill_uuid(store, f"claude:{worker_id}", timeout=8)


def _new(cwd, worker_id=C, *, prompt=None, json_mode=True):
    argv = ["new", "--engine", "claude", "--cwd", str(cwd), "--worker-id", worker_id]
    if prompt is not None:
        argv.append(prompt)
    argv.append("--notify-parent")
    if json_mode:
        argv.append("--json")
    return parser.build_parser().parse_args(argv)


def _cli_confirmed(args, capsys):
    """Same full ID across the native SessionStart window, never remint."""
    until = time.monotonic() + 13
    while True:
        try:
            commands_create.cmd_new(args)
        except SystemExit as stopped:
            captured = capsys.readouterr()
            if stopped.code != 3:
                pytest.fail(f"unexpected CLI refusal {stopped.code}: {captured}")
            partial = json.loads(captured.out)
            assert partial["phase"] == "native-activation", partial
            assert partial["worker_id"] == args.worker_id
            assert time.monotonic() < until, partial
            time.sleep(.06)
        else:
            captured = capsys.readouterr()
            if args.json:
                return json.loads(captured.out)
            return captured.out


def _rows_for(worker_id):
    return [row for row in parent_notify.iter_registrations(include_terminal=True)
            if row["worker"]["workerId"] == worker_id]


def test_claude_new_promptless_keyed_stable_managed_and_native_parent(
    machine_originals, fake_claude, tmp_path, monkeypatch, capsys,
):
    """One original create per child, zero SEND; replay never re-arms."""
    _client, codex_store, _trace, _daemon = machine_originals
    store, launches, _native_input = fake_claude
    assert store.generation == codex_store.generation
    _n1c_managed_parent(monkeypatch, store, A)
    managed = {"kind": "ccx-codex", "workerId": A,
               "threadId": store.record(A)["native"]["threadId"],
               "generation": store.generation}
    args = _new(tmp_path)
    first = _cli_confirmed(args, capsys)
    event = first["notify_parent"]
    expected = str(uuid.uuid5(uuid.NAMESPACE_URL,
        f"ccx-notify:full-uuid-claude-new:{store.generation}:{C}:promptless"))
    assert first["ok"] is True and first["worker_id"] == C
    assert event["event_id"] == expected and event["source"] == "new"
    assert event["selection_mode"] == "status-only"
    assert event["delivery_id"] is None and event["turn_id"] is None
    assert "delivery" not in first and "delivery_id" not in first
    assert len(_rows_for(C)) == 1
    row = _rows_for(C)[0]
    assert row["parent"] == managed and row["settlementMode"] is None
    assert row["operation"] == {"kind": "claude-stable", "expectTurn": False,
        "selectionMode": "status-only", "stableState": "idle"}
    assert notify_settle.settlement_for_registration(row) == {
        "status": "idle", "workerState": "idle", "turnStatus": None,
        "report": None, "error": None}
    assert claude_delivery.read_receipt("claude:" + C, "create:initial") is None
    native_sid = store.record(C)["native"]["sessionId"]
    assert native_sid and native_sid != C
    assert launches.read_text().splitlines() == ["provider-start"]

    parent_notify.cancel_registration(expected, "intentional wake cancellation")
    plain = _cli_confirmed(_new(tmp_path, json_mode=False), capsys)
    assert plain.splitlines()[0] == "claude:" + C
    assert f"notify: armed {expected}" in plain
    same = _cli_confirmed(args, capsys)
    assert same["notify_parent"]["event_id"] == expected
    assert same["notify_parent"]["state"] == "cancelled"
    assert len(_rows_for(C)) == 1
    assert store.record(C)["native"]["sessionId"] == native_sid
    assert launches.read_text().splitlines() == ["provider-start"]

    _n1c_managed_parent(monkeypatch, store, B)
    with pytest.raises(SystemExit):
        commands_create.cmd_new(args)
    foreign = json.loads(capsys.readouterr().out)
    assert foreign["phase"] == "notification-registration" and foreign["worker_id"] == C
    assert foreign["cause_type"] == "InvalidRequest"
    assert len(_rows_for(C)) == 1
    assert launches.read_text().splitlines() == ["provider-start"]

    native_parent = native_caller(monkeypatch, "caller-original-native-return")
    assert native_parent == {"kind": "codex-native-thread",
                             "threadId": "caller-original-native-return"}
    interactive = _cli_confirmed(_new(tmp_path, D), capsys)
    other = _rows_for(D)
    assert len(other) == 1 and other[0]["parent"] == native_parent
    assert interactive["notify_parent"]["event_id"] == str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"ccx-notify:full-uuid-claude-new:{store.generation}:{D}:promptless"))
    assert claude_delivery.read_receipt("claude:" + D, "create:initial") is None
    assert store.record(D)["native"]["sessionId"] != D
    assert launches.read_text().splitlines() == ["provider-start"] * 2


def test_claude_new_prompted_one_initial_send_receipt_before_native(
    fake_claude, tmp_path, monkeypatch, capsys,
):
    """One create, one create:initial; the v3 event is fsynced pre-input."""
    store, launches, native_input = fake_claude
    parent = native_caller(monkeypatch, "first-turn-native-parent")
    prompt = "one actual first Claude fake-host turn"
    original_begin = GenerationStore.begin_native_intent
    observed_events = []

    def before_effect(self, target, *, family, operation_id, **kw):
        if family == "claude-delivery" and operation_id == "create:initial":
            rows = _rows_for(C)
            assert len(rows) == 1
            reg = rows[0]
            receipt = claude_delivery.read_receipt("claude:" + C, operation_id)
            assert reg["state"] == "armed" and reg["source"] == "send"
            assert reg["parent"] == parent and reg["operation"]["deliveryId"] == operation_id
            assert reg["operation"]["kind"] == "claude-turn"
            assert receipt["state"] == "prepared" and receipt["requestedMode"] == "new_turn_only"
            assert json.loads(parent_notify.registration_path(reg["eventId"]).read_text()) == reg
            assert not native_input.exists() or prompt not in native_input.read_text()
            observed_events.append(reg["eventId"])
        return original_begin(self, target, family=family, operation_id=operation_id, **kw)

    monkeypatch.setattr(GenerationStore, "begin_native_intent", before_effect)
    result = _cli_confirmed(_new(tmp_path, prompt=prompt), capsys)
    assert result["ok"] is True and result["worker_id"] == C
    assert result["delivery_id"] == "create:initial"
    assert result["delivery_status"] in {"owned", "accepted"}
    assert result["delivery"]["owned"] is True
    if result["delivery_status"] == "owned":
        assert result["delivery"]["status"] == "pending"  # Not completed.
    assert result["notify_parent"]["event_id"] == observed_events[0]
    assert result["notify_parent"]["source"] == "send"
    assert len(observed_events) == 1 and len(_rows_for(C)) == 1
    assert store.record(C)["native"]["sessionId"] != C
    assert launches.read_text().splitlines() == ["provider-start"]
    until = time.monotonic() + 9
    while (not native_input.exists() or prompt not in native_input.read_text()):
        assert time.monotonic() < until, "original fake provider never received first prompt"
        time.sleep(.05)
    assert native_input.read_bytes().count(prompt.encode()) == 1
    assert launches.read_text().splitlines() == ["provider-start"]
    replay = _cli_confirmed(_new(tmp_path, prompt=prompt), capsys)
    assert replay["notify_parent"]["event_id"] == observed_events[0]
    assert len(observed_events) == 1 and len(_rows_for(C)) == 1
    assert native_input.read_bytes().count(prompt.encode()) == 1


def test_claude_new_parent_preclaim_and_after_create_partial_keep_original_id(
    fake_claude, tmp_path, monkeypatch, capsys,
):
    """Wrong caller/settle has no claim; a failed stable arm never re-creates."""
    store, launches, _native_input = fake_claude
    for key in ("CCX_SELF_SHORT", "CCX_SELF_ENGINE", "CCX_SELF_WORKER_ID",
                "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(SystemExit):
        commands_create.cmd_new(_new(tmp_path))
    assert "notify" in capsys.readouterr().err
    assert store._owner(C) is None
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "native-claude-parent-not-codex")
    with pytest.raises(SystemExit):
        commands_create.cmd_new(_new(tmp_path))
    assert "notify" in capsys.readouterr().err
    assert store._owner(C) is None
    parent = {"kind": "codex-native-thread", "threadId": "original-eligible-return"}
    with pytest.raises(InvalidRequest, match="recursive settlement currently requires a Codex target"):
        creation.create(engine="claude", cwd=str(tmp_path), worker_id=C,
                        parent=parent, settle=True)
    assert store._owner(C) is None
    assert not launches.exists()

    assert native_caller(monkeypatch, parent["threadId"]) == parent
    original_register = parent_notify.create_registration
    calls = []

    def unavailable(**kw):
        assert kw["source"] == "new" and kw["worker_id"] == C
        assert store.native_activation(C)["state"] == "confirmed"
        calls.append(kw["worker_id"])
        raise OSError("fake notification storage refused publication")

    with monkeypatch.context() as broken:
        broken.setattr(parent_notify, "create_registration", unavailable)
        with pytest.raises(CreatePartialFailure) as failure:
            creation.create(engine="claude", cwd=str(tmp_path), worker_id=C,
                            parent=parent, start_timeout=8)
    error = failure.value
    assert error.phase == "notification-registration" and error.worker_id == C
    assert error.info["native_ready"] is True and error.info["activation_state"] == "confirmed"
    assert calls == [C] and _rows_for(C) == []
    native_sid = store.record(C)["native"]["sessionId"]
    assert native_sid != C and launches.read_text().splitlines() == ["provider-start"]

    reconciled = creation.create(engine="claude", cwd=str(tmp_path), worker_id=C,
                                 parent=parent, start_timeout=4)
    assert reconciled["worker_id"] == C and reconciled["reconciled"] is True
    assert reconciled["notification"]["source"] == "new"
    assert len(_rows_for(C)) == 1 and _rows_for(C)[0]["parent"] == parent
    assert store.record(C)["native"]["sessionId"] == native_sid
    assert launches.read_text().splitlines() == ["provider-start"]
