"""Fake-provider regressions for the second open-source review round."""

from __future__ import annotations

import json
import os
import time

import pytest

from ccx import WorkerClient
from ccx.claude import delivery as claude_delivery, usage
from ccx.claude.host.successor import (
    queued_steer_needs_successor_hold, successor_hold_should_release,
)
from ccx.cli import codex as codex_commands
from ccx.cli.commands import read as read_commands, turns as turns_commands
from ccx.cli.parser import build_parser
from ccx.codex import evidence as codex_evidence, telemetry as codex_telemetry
from ccx.codex import delivery as codex_delivery
from ccx.errors import BudgetStopped, SessionUnavailable
from ccx.ops.send import send_turn
from ccx.ops import report as ops_report, conversation as conversation
from ccx.system.engine_binary import resolve_engine_binary
from test_generation_store import A, short_root  # noqa: F401
from test_notify_claude import owners, _fake_original_response  # noqa: F401
from test_notify_receiptless import (  # noqa: F401
    _claude_sources, _claude_line, _claude_hook_events,
)
from test_worker_telemetry import original_codex, original_sources  # noqa: F401
from test_conversation import _codex_turn, build_codex, all_items


def test_claude_capped_send_refreshes_stale_usage_before_any_delivery(
    owners, tmp_path, monkeypatch,
):
    _root, store = owners
    cache = tmp_path / "usage-cache.json"
    sample = usage._apply_sample(
        "get_usage", None,
        {"kind": "success", "body": {"seven_day": {"utilization": 12}}},
        time.time() - 300,
    )
    cache.write_text(json.dumps(sample))
    cache.chmod(0o600)
    monkeypatch.setattr(usage, "usage_cache_path", lambda: cache)
    monkeypatch.setattr(usage, "_usage_source", lambda: "get_usage")
    from ccx.claude import usage_control
    fetches = []

    def fake_usage(timeout):
        fetches.append(timeout)
        return {"kind": "success", "body": {"seven_day": {"utilization": 84}}}

    monkeypatch.setattr(usage_control, "request_usage", fake_usage)
    original = store.record(A)
    calls = []

    def never_deliver(*args):
        calls.append(args)
        raise AssertionError("preflight must not call a provider")

    with pytest.raises(BudgetStopped, match="usage cap 7d already exceeded") as caught:
        send_turn(A, "not delivered", delivery_id="live2-cap-stale",
                  cap={"7d": 60}, _native_submit=never_deliver)
    assert caught.value.exit_code == 7 and caught.value.phase == "preflight"
    assert "no turn delivered" in caught.value.reason
    assert len(fetches) == 1 and not calls
    assert claude_delivery.read_receipt(A, "live2-cap-stale") is None
    assert store.record(A)["lifecycle"] == original["lifecycle"]


@pytest.mark.parametrize("mode", ["new_turn_only", "steer_only"])
def test_claude_capped_send_refuses_each_route_without_history(
    owners, monkeypatch, mode,
):
    from ccx.claude import usage
    monkeypatch.setattr(usage, "fetch_usage", lambda **kw: {"five_hour": {"utilization": 90}})
    with pytest.raises(BudgetStopped) as caught:
        send_turn(A, "must remain unsubmitted", delivery_id="live2-cap-" + mode,
                  mode=mode, cap={"5h": 50}, _native_submit=lambda *a: pytest.fail("submitted"))
    assert caught.value.phase == "preflight"
    assert claude_delivery.read_receipt(A, "live2-cap-" + mode) is None


def test_claude_cap_unavailable_fails_open(owners, monkeypatch):
    _root, store = owners
    monkeypatch.setattr(usage, "fetch_usage", lambda **kw: None)
    submitted = []

    def fake_native(handle, sid, prompt, token, budget):
        submitted.append(prompt)
        return _fake_original_response(store, token, prompt)

    result = send_turn(A, "permitted when unknown", delivery_id="live2-cap-unknown",
                       cap={"7d": 1}, _native_submit=fake_native)
    assert submitted == ["permitted when unknown"]
    assert result["status"] == "accepted"


def test_claude_unrecognized_delivery_reply_records_diagnostic(owners):
    _root, store = owners
    token = "live2-unrecognized-reply"
    def fake_native(handle, sid, prompt, delivery_id, budget):
        assert delivery_id == token
        claude_delivery.mark_owned(handle, delivery_id, route="new_turn")
        return {"ok": True, "delivery": None,
                "error": "socket read timed out", "secret": "not logged"}

    result = send_turn(A, "one fake turn", delivery_id=token,
                       _native_submit=fake_native)
    assert result["status"] == "owned"
    assert result["delivery"]["delivery_id"] == token
    events = [json.loads(line) for line in
              store.worker_path(A, "events.ndjson").read_text().splitlines()]
    failures = [event for event in events
                if event["type"] == "delivery_reply_unrecognized"]
    assert len(failures) == 1
    event = failures[0]
    assert event["delivery_id"] == token and event["receipt_state"] == "owned"
    assert "socket read timed out" in event["reply"]
    assert "not logged" not in event["reply"] and len(event["reply"]) <= 300

    def ordinary_native(handle, sid, prompt, delivery_id, budget):
        claude_delivery.mark_owned(handle, delivery_id, route="new_turn")
        return {"ok": True, "delivery": {"delivery_id": delivery_id}}

    normal = send_turn(A, "normal receipt", delivery_id="live2-normal-reply",
                       _native_submit=ordinary_native)
    assert normal["status"] == "owned"
    assert len(store.worker_path(A, "events.ndjson").read_text().splitlines()) == 1


def test_stopped_claude_steer_connection_error_is_one_clean_sentence(
    owners, monkeypatch,
):
    from ccx.rpc import rpc
    monkeypatch.setattr(rpc, "_rpc", lambda *a, **kw: {"ok": False})
    with pytest.raises(SessionUnavailable) as caught:
        send_turn(A, "steer cannot reach stopped host", delivery_id="live2-stopped-steer",
                  mode="steer_only")
    assert "status; no delivery receipt written" in str(caught.value)
    assert "statusserving" not in str(caught.value)
    assert claude_delivery.read_receipt(A, "live2-stopped-steer") is None


def test_report_newer_unmanaged_turn_running_keeps_previous_reply(
    owners, tmp_path, monkeypatch, capsys,
):
    _root, store = owners
    source = _claude_sources(store, monkeypatch, tmp_path)
    target = "claude:" + A
    old, current = "ccx-turn:claude:v1:older", "ccx-turn:claude:v1:current"
    _claude_line(source, {"type": "user", "message": {"content": "old question"}})
    _claude_line(source, {"type": "assistant", "message": {"content": [
        {"type": "text", "text": "Last completed answer"}]}})
    offset = source.stat().st_size
    _claude_line(source, {"type": "user", "message": {"content": "running question"}})
    _claude_line(source, {"type": "assistant", "message": {"content": [
        {"type": "text", "text": "unfinished draft"}]}})
    _claude_hook_events(store,
        {"type": "turn_start", "turn_ref": old, "turn": 1, "report_start_offset": 0},
        {"type": "turn_complete", "turn_ref": old, "turn": 1},
        {"type": "turn_start", "turn_ref": current, "turn": 2,
         "report_start_offset": offset},
    )
    store.mutate_record(target, patches={
        "lastSettledTurn": {"turnRef": old, "settledAtMs": 1, "outcome": "completed"},
        "lifecycle.state": "busy", "lifecycle.activeTurnRef": current,
    })
    result = WorkerClient().report(target)
    assert result["text"] == "Last completed answer"
    assert result["note"] == "a newer turn is still running"
    assert result["source"] == "native-transcript-latest"
    read_commands.cmd_report(build_parser().parse_args(["report", target]))
    plain = capsys.readouterr()
    assert plain.out == "Last completed answer\n"
    assert "a newer turn is still running" in plain.err
    read_commands.cmd_report(build_parser().parse_args(["report", target, "--json"]))
    public = json.loads(capsys.readouterr().out)
    assert public["text"] == "Last completed answer" and public["note"] == result["note"]


def test_codex_no_thread_id_is_not_foreign_native_evidence(original_codex, monkeypatch):
    store, _cwd, worker_id, thread_id = original_codex
    original = codex_evidence.read_journal(worker_id)
    events = [
        {"method": "thread/tokenUsage/updated", "sequence": 1,
         "threadId": None, "params": {}},
        {"method": "turn/started", "sequence": 2,
         "threadId": thread_id, "params": {"turn": {"id": "owned"}}},
    ]
    original.events = events
    monkeypatch.setattr(codex_evidence, "read_journal", lambda worker: original)
    owner = store.resolve(worker_id)
    _rec, _thread, filtered, foreign, _idx = codex_telemetry._original_source(store, owner)
    assert foreign == []
    assert [event["sequence"] for event in filtered.events] == [2]
    events.append({"method": "turn/started", "sequence": 3,
                   "threadId": "genuinely-foreign", "params": {}})
    original.events = events
    _rec, _thread, _filtered, foreign, _idx = codex_telemetry._original_source(store, owner)
    assert len(foreign) == 1 and foreign[0]["threadId"] == "genuinely-foreign"


def test_codex_accepted_steer_never_implies_unknown_newer_message(
    original_codex, monkeypatch,
):
    store, _cwd, worker_id, thread_id = original_codex
    handle = "codex:" + worker_id
    store.mutate_record(handle, patches={
        "lastTurn": {"turnId": "old-native-turn", "status": "completed"},
    })
    old_ref = "ccx-turn:codex:v1:old-native-turn"
    receipts = [{
        "state": "accepted", "route": "new_turn", "turnRef": old_ref,
        "settlement": {"text": "Last reply"}, "createdAtMs": 1,
    }, {
        "state": "accepted", "route": "steer", "turnRef": old_ref,
        "placement": "current_turn", "settlement": None, "createdAtMs": 2,
    }]
    monkeypatch.setattr(codex_delivery, "iter_receipts", lambda *a: iter(receipts))
    original_report = ops_report.report_worker

    def exact_stub(target, *, turn_ref=None, **kw):
        if turn_ref is not None:
            assert turn_ref == old_ref
            return {"text": "Last reply", "has_final_message": True}
        return original_report(target, **kw)

    monkeypatch.setattr(ops_report, "report_worker", exact_stub)
    result = exact_stub(handle)
    assert result["text"] == "Last reply"
    assert result.get("note") != "a newer message's outcome is unknown"
    receipts[1]["route"] = "new_turn"
    assert exact_stub(handle)["note"] == "a newer message's outcome is unknown"


def test_queued_claude_steer_requires_busy_successor_until_native_idle():
    prior_ref = "ccx-turn:claude:v1:prior"
    slot = {"route": "steer", "target_turn_ref": prior_ref,
            "maybe_written": True, "phase": "await_evidence"}
    assert queued_steer_needs_successor_hold(slot, prior_ref)
    assert not queued_steer_needs_successor_hold(slot, "different-turn")
    assert not queued_steer_needs_successor_hold(
        {**slot, "phase": "current_turn"}, prior_ref)
    assert not successor_hold_should_release({"since": 1}, None, "busy")
    assert successor_hold_should_release({"since": 1}, None, "idle")


def test_codex_function_call_output_is_mapped_and_warnings_not_repeated():
    items = [
        {"type": "functionCallOutput", "id": "r1", "callId": "call-1",
         "name": "worker_notification", "output": "delivered"},
        {"type": "functionCallOutput", "id": "r2", "output": "second"},
        {"type": "unknown-to-ccx", "id": "u1"},
        {"type": "unknown-to-ccx", "id": "u2"},
    ]
    doc = build_codex(_codex_turn(items))
    normalized = {item["id"]: item for item in all_items(doc)}
    assert normalized["r1"]["kind"] == "tool-result"
    assert normalized["r1"]["callId"] == "call-1"
    assert normalized["r1"]["result"]["content"] == "delivered"
    assert normalized["r2"]["kind"] == "tool-result"
    warnings = doc["completeness"]["warnings"]
    assert len([w for w in warnings if "unknown-to-ccx" in w]) == 1
    assert not any("functionCallOutput" in w for w in warnings)


def test_codex_binary_uses_native_home_fallback(tmp_path, monkeypatch):
    home = tmp_path / "home"
    native = home / ".local" / "bin" / "codex"
    native.parent.mkdir(parents=True)
    native.write_text("#!/bin/sh\nexit 0\n")
    native.chmod(0o700)
    path = tmp_path / "empty"
    path.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("PATH", str(path))
    monkeypatch.delenv("CCX_CODEX_BIN", raising=False)
    assert resolve_engine_binary("codex") == str(native)
    monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "explicit-missing"))
    with pytest.raises(Exception, match="Codex CLI not found"):
        resolve_engine_binary("codex")


def test_codex_capped_send_json_refusal_has_structured_status(
    original_codex, monkeypatch, capsys,
):
    store, _cwd, worker_id, _tid = original_codex
    from ccx.ops import send as send_ops

    def stopped(*a, **kw):
        raise BudgetStopped("usage cap 7d already exceeded; no turn delivered",
                            reason="7d 92% >= 60%", phase="preflight")

    monkeypatch.setattr(send_ops, "send_turn", stopped)
    with pytest.raises(SystemExit) as caught:
        codex_commands.cmd_send(build_parser().parse_args([
            "send", "codex:" + worker_id, "do not deliver", "--cap-7d", "60", "--json",
        ]))
    assert caught.value.code == 7
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["code"] == "budget_stopped" and result["status"] == "budget-stopped"
    assert result["reason"] == "7d 92% >= 60%"


def test_claude_capped_send_json_refusal_has_structured_status(
    owners, monkeypatch, capsys,
):
    monkeypatch.setattr(usage, "fetch_usage", lambda **kw: {"seven_day": {"utilization": 81}})
    with pytest.raises(SystemExit) as caught:
        turns_commands.cmd_send(build_parser().parse_args([
            "send", "claude:" + A, "unsubmitted", "--cap-7d", "60", "--json",
        ]))
    assert caught.value.code == 7
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "budget-stopped" and "no turn delivered" in result["reason"]
