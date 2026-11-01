"""C3 recorded advisory list and source-fenced rich peek, no providers."""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from ccx.cli import main as cli
from ccx import constants
from ccx.system.cwd_paths import canonical_cwd
from ccx.store.root import require_ccx_active
from ccx.workers import WorkerClient
from test_generation_store import A, B, C, add_worker, record, spec, short_root
from test_worker_discovery import D
from test_conversation import build_codex, _codex_turn, _user_message
from test_worker_telemetry import original_sources, original_codex, _append_jsonl, _claude_entries

E = "dddddddd-1234-4000-8000-000000000006"


def test_list_recorded_intersection_before_limit(short_root, monkeypatch, capsys):
    store = require_ccx_active(short_root)
    now = int(time.time() * 1000)
    definitions = [(A, "claude", "idle", 50, False),
                   (B, "codex", "busy", 60, False),
                   (C, "codex", "dead", 100, False),
                   (D, "claude", "waiting", 90, False),
                   (E, "claude", "idle", 95, True)]
    for wid, engine, state, order, terminal in definitions:
        reservation = store.reserve_creation(wid, engine=engine, fingerprint=spec(engine))
        rec = record(wid, engine, terminal=terminal)
        rec.update(cwd=canonical_cwd("/tmp/c3-team"), createdAt=now - 200000,
                   lastTurnAt=now - (110 - order) * 1000,
                   lifecycle={"state": state})
        store.publish_first_record(reservation, rec)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setattr(WorkerClient, "conversation",
                        lambda *_a, **_kw: pytest.fail("list queried native transcript"))

    def listed(*flags):
        cli._main(["list", "--json", *flags])
        rows = json.loads(capsys.readouterr().out)
        assert all(row["state_source"] == "recorded" and row["loaded"] is None
                   for row in rows)
        return rows

    assert [x["worker_id"] for x in listed("--active", "--limit", "1")] == [D]
    assert [x["worker_id"] for x in listed("--sendable", "--limit", "1")] == [C]
    assert [x["worker_id"] for x in listed("--active", "--sendable", "--limit", "1")] == [A]
    assert [x["worker_id"] for x in listed("--sendable")] == [C, A]
    assert [x["worker_id"] for x in listed("--engine", "codex", "--sendable")] == [C]
    assert [x["worker_id"] for x in listed("--cwd-prefix", "/tmp", "--active", "--state", "waiting")] == [D]
    assert [x["worker_id"] for x in listed("--sendable", "--recent-hours", "1")] == [C, A]
    assert listed("--active", "--limit", "0") == []
    assert all(row["ambiguous_short"] for row in listed() if row["short"] == "aaaaaaaa")
    assert not (short_root / "run").exists() and not (short_root / "sessions").exists()


def _native_codex_document():
    original = _codex_turn([
        _user_message("i1", "build it"),
        {"type": "agentMessage", "id": "i2", "text": "starting"},
        {"type": "commandExecution", "id": "i3", "command": "make", "status": "failed",
         "exitCode": 1, "aggregatedOutput": "native error details"},
        _user_message("i4", "also run tests"),
        {"type": "agentMessage", "id": "i5", "text": "done"},
    ])
    original["turns"].append({"id": "t2", "status": "completed", "itemsView": "full",
                               "items": [_user_message("i6", "second original turn"),
                                         {"type": "agentMessage", "id": "i7", "text": "second answer"}]})
    doc = build_codex(original)
    doc["worker"]["handle"] = f"codex:{B}"
    doc["completeness"].update(status="source-limited", sourceLimited=True,
                               warnings=["native thread/read is source-limited"])
    doc["evidence"].update(status="degraded", available=False,
                           warnings=["original evidence journal is incomplete"])
    return doc


@pytest.fixture
def source_peek(short_root, monkeypatch):
    add_worker(short_root, A, "claude", terminal=True)
    add_worker(short_root, B, "codex", terminal=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    doc = _native_codex_document()
    reads = []

    def conversation(_self, worker, *, max_tool_result_bytes=None, **_kwargs):
        reads.append((worker, max_tool_result_bytes))
        return doc

    monkeypatch.setattr(WorkerClient, "conversation", conversation)
    return short_root, doc, reads


def test_rich_peek_original_native_order_warnings_and_one_read(source_peek, capsys):
    _, doc, reads = source_peek
    cli._main(["peek", f"codex:{B[:8]}", "--tail", "1", "--json"])
    value = json.loads(capsys.readouterr().out)
    assert reads == [(f"codex:{B}", 4096)]
    assert value["worker"]["nativeSessionId"] == doc["worker"]["nativeSessionId"]
    assert value["completeness"]["warnings"] == ["native thread/read is source-limited"]
    assert value["evidence"]["warnings"] == ["original evidence journal is incomplete"]
    assert value["tool_count"] == 1 and value["tools"][0]["result"]["isError"] is True
    assert "errors" not in value
    assert [row["id"] for row in value["recent_items"]] == ["i3", "i6", "i7"]
    assert [row["ordinal"] for row in value["recent_items"]] == [2, 0, 1]
    assert value["last_user"]["text"] == "second original turn"
    assert value["last_assistant"]["text"] == "second answer"
    assert not (source_peek[0] / "run").exists()


def test_rich_peek_source_ordinal_steer_and_one_based_turn_filter(source_peek, capsys):
    _, _, reads = source_peek
    cli._main(["peek", f"codex:{B}", "--tail", "1", "--since-turn", "1", "--json"])
    assert [x["id"] for x in json.loads(capsys.readouterr().out)["recent_items"]] == ["i3", "i6", "i7"]
    cli._main(["peek", f"codex:{B}", "--since-turn", "2", "--json"])
    only_second = json.loads(capsys.readouterr().out)
    assert only_second["turn_count"] == 1
    assert [x["id"] for x in only_second["recent_items"]] == ["i6", "i7"]
    assert only_second["tools"] == []
    assert len(reads) == 2 and all(worker == f"codex:{B}" for worker, _ in reads)


def test_peek_interleaved_codex_steer_stays_between_tool_and_assistant(source_peek, capsys):
    _, doc, reads = source_peek
    # The original thread has a mid-turn user steer in its INPUT bucket.
    # Project the first native turn only; buckets alone put i4 before i3.
    doc["turns"] = doc["turns"][:1]
    cli._main(["peek", f"codex:{B}", "--tail", "1", "--json"])
    output = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in output["recent_items"]] == ["i3", "i4", "i5"]
    assert [item["ordinal"] for item in output["recent_items"]] == [2, 3, 4]
    assert output["last_user"]["text"] == "also run tests"
    assert output["last_assistant"]["text"] == "done"
    assert reads == [(f"codex:{B}", 4096)]


def test_non_tool_engine_error_is_bounded_by_tail_and_since_turn(source_peek, capsys):
    _, doc, _ = source_peek
    first = doc["turns"][0]
    first["output"].append({"kind": "error", "id": "native-engine-error",
                            "ordinal": 5, "message": "native provider unavailable"})
    # Add a later real tool; --tail 1 must exclude the earlier engine error.
    second = doc["turns"][1]
    second["output"].append({"kind": "tool-call", "id": "later-tool",
                              "ordinal": 2, "status": "completed", "name": "shell"})

    cli._main(["peek", B, "--tail", "1", "--json"])
    recent = json.loads(capsys.readouterr().out)
    assert "errors" not in recent
    assert "native-engine-error" not in [i["id"] for i in recent["recent_items"]]
    assert "later-tool" in [i["id"] for i in recent["recent_items"]]

    cli._main(["peek", B, "--since-turn", "1", "--tail", "2", "--json"])
    narrow = json.loads(capsys.readouterr().out)
    assert [i["id"] for i in narrow["recent_items"] if i["kind"] == "error"] == ["native-engine-error"]
    cli._main(["peek", B, "--since-turn", "1", "--tail", "2"])
    human = capsys.readouterr().out
    assert "error: native provider unavailable" in human
    assert "{'kind': 'error'" not in human

    cli._main(["peek", B, "--since-turn", "2", "--json"])
    later = json.loads(capsys.readouterr().out)
    assert not any(i["kind"] == "error" for i in later["recent_items"])


def test_rich_peek_modes_output_cap_errors_and_warnings(source_peek, capsys):
    _, _, reads = source_peek
    for flags, ids in [(["--tools-only"], ["i3"]),
                       (["--text-only"], ["i6", "i7"])]:
        cli._main(["peek", B, "--json", *flags])
        assert [item["id"] for item in json.loads(capsys.readouterr().out)["recent_items"]] == ids
    cli._main(["peek", B, "--brief"])
    human = capsys.readouterr().out
    assert "evidence warning: original evidence journal is incomplete" in human
    assert "completeness warning: native thread/read is source-limited" in human
    assert "assistant: second answer" in human and "tools: 1 recorded" in human
    assert "user-text:" not in human
    with pytest.raises(SystemExit) as removed_flag:
        cli._main(["peek", B, "--errors-only"])
    assert removed_flag.value.code != 0
    capsys.readouterr()
    cli._main(["peek", B, "--max-chars", "180"])
    bounded = capsys.readouterr().out
    assert len(bounded) <= 180 and "truncated" in bounded
    cli._main(["peek", B, "--full", "--json"])
    assert json.loads(capsys.readouterr().out)["tool_count"] == 1
    assert reads[-1] == (f"codex:{B}", None)


def test_peek_status_no_native_and_rich_batch_refuses(source_peek, capsys):
    _, _, reads = source_peek
    cli._main(["peek", A, B, "--status", "--json"])
    statuses = json.loads(capsys.readouterr().out)["statuses"]
    assert [x["worker"] for x in statuses] == [f"claude:{A}", f"codex:{B}"]
    assert all(x["state_source"] == "recorded" and x["host_status"] == "unobserved"
               and "pending_send_count" not in x for x in statuses)
    assert reads == []
    for args in (["peek", A, B], ["peek", A, "--since-turn", "0"],
                 ["peek", A, "--tail", "-1"], ["peek", A, "--max-chars", "-1"],
                 ["peek", A, "--tools-only", "--text-only"]):
        with pytest.raises(SystemExit) as error:
            cli._main(args)
        assert error.value.code == 1
        capsys.readouterr()
    assert reads == []


def test_recorded_background_snapshot_surfaces_without_task_scan(source_peek, capsys):
    from ccx import CcxClient
    root, doc, reads = source_peek
    store = require_ccx_active(root)
    from ccx.ops.background import summary as make_summary
    summary = make_summary([
        {"id": "command:one", "kind": "command", "status": "running", "label": "one"},
        {"id": "monitor:mon", "kind": "monitor", "status": "running",
         "label": "mon", "expires_at_ms": 99999},
    ], as_of_ms=12345)
    store.mutate_record(A, patches={"backgroundTaskSummary": summary})

    cli._main(["peek", A, "--status", "--json"])
    assert json.loads(capsys.readouterr().out)["statuses"][0]["background_task_summary"] == summary
    assert reads == []
    cli._main(["peek", A, "--status"])
    assert "background: running=2 pending=0 command=1 monitor=1 subagent=0" in capsys.readouterr().out

    doc["worker"]["handle"] = "claude:" + A
    cli._main(["peek", A, "--json"])
    assert json.loads(capsys.readouterr().out)["background_task_summary"] == summary
    cli._main(["peek", A, "--brief"])
    assert "background: running=2" in capsys.readouterr().out

    assert CcxClient().peek(A).background_task_summary == summary
    assert CcxClient().status([A])[0].as_dict()["background_task_summary"] == summary
    assert WorkerClient().status(A)["background_task_summary"] == summary

    store.mutate_record(A, patches={"backgroundTaskSummary": make_summary([], as_of_ms=12346)})
    cli._main(["peek", A, "--status"])
    assert "background: running" not in capsys.readouterr().out


def test_peek_status_preserves_codex_original_brokered_gate(source_peek, capsys):
    root, _, reads = source_peek
    store = require_ccx_active(root)
    store.mutate_record(B, patches={
        "lifecycle.state": "waiting",
        "pendingApprovals": [{
            "gateId": "original-gate-c3", "kind": "commandExecution",
            "command": "native fake command", "runtimeId": "original-runtime",
        }],
    })
    cli._main(["peek", B, "--status", "--json"])
    status = json.loads(capsys.readouterr().out)["statuses"][0]
    assert status["pending_input"]["request_id"] == "original-gate-c3"
    assert status["pending_input"]["kind"] == "permission"
    assert status["pending_input"]["questions"] == []
    assert status["pending_approvals"][0]["gateId"] == "original-gate-c3"
    assert status["host_status"] == "unobserved" and reads == []


@pytest.mark.parametrize("engine,gate,expected", [
    ("claude", {"id": "question-7", "tool": "AskUserQuestion",
                "input": {"questions": [{"question": "Deploy now?"}]}},
     "awaiting: question request_id=question-7"),
    ("claude", {"id": "permission-8", "tool": "Bash",
                "input": {"command": "echo example"}},
     "awaiting: permission request_id=permission-8"),
    ("claude", {"id": "elicitation-9", "type": "elicitation",
                "tool": "mcp"},
     "awaiting: elicitation request_id=elicitation-9"),
    ("codex", [{"gateId": "codex-gate-10", "kind": "commandExecution",
                "command": "git status", "runtimeId": "original-runtime"}],
     "awaiting: approval gate_id=codex-gate-10 command=git status"),
])
def test_rich_peek_shows_recorded_pending_input_like_status(
    source_peek, monkeypatch, capsys, engine, gate, expected,
):
    root, doc, reads = source_peek
    wid = A if engine == "claude" else B
    if engine == "claude":
        # The synthetic schema-5 conversation remains exact-owner checked.
        doc["worker"]["handle"] = f"claude:{wid}"
    store = require_ccx_active(root)
    patches = {"lifecycle.state": "waiting"}
    patches["waitingFor" if engine == "claude" else "pendingApprovals"] = gate
    store.mutate_record(wid, patches=patches)

    cli._main(["peek", wid, "--status", "--json"])
    status = json.loads(capsys.readouterr().out)["statuses"][0]
    assert reads == []  # recorded status never reads the conversation
    cli._main(["peek", wid, "--json"])
    rich = json.loads(capsys.readouterr().out)
    assert rich["pending_input"] == status["pending_input"]
    assert rich["pending_input_observed"] is True
    if engine == "codex":
        assert rich["pending_approvals"] == status["pending_approvals"]
        assert rich["pending_approvals"][0]["gateId"] == "codex-gate-10"
    else:
        assert rich["waiting_for"] == gate

    for flags in ([], ["--brief"], ["--text-only"]):
        cli._main(["peek", wid, *flags])
        human = capsys.readouterr().out
        assert expected in human
        assert human.count("awaiting:") == 1
    assert len(reads) == 4  # one JSON + three text renderings; no native probes


def test_peek_help_has_one_target_description_and_positional_batch_usage():
    parser = cli.build_parser()
    peek = parser._subparsers._group_actions[0].choices["peek"]
    help_text = peek.format_help()
    assert "target [target ...]" in help_text
    assert len([line for line in help_text.splitlines()
                if line.startswith("  target ")]) == 1
    assert "More workers (only with --status)" not in help_text
    assert "only with --status" in help_text
    parsed = parser.parse_args(["peek", A, B, "--status"])
    assert parsed.target == A and parsed.extra_shorts == [B]


def test_peek_bad_source_identity_cannot_emit_false_complete_snapshot(source_peek, capsys):
    _, doc, reads = source_peek
    doc["worker"]["handle"] = f"claude:{A}"  # Source does not belong to requested Codex worker.
    with pytest.raises(SystemExit) as refused:
        cli._main(["peek", B, "--json"])
    assert refused.value.code == 3
    captured = capsys.readouterr()
    assert not captured.out and "conversation is incomplete" in captured.err
    assert reads == [(f"codex:{B}", 4096)]


def test_cli_peek_reads_actual_source_fenced_claude_not_other_session(original_sources, capsys):
    store, _ = original_sources
    native = Path(store.record(A)["transcript"])
    foreign = Path(store.record(B)["transcript"])
    _append_jsonl(native, *_claude_entries("a-only", 2))
    _append_jsonl(foreign, *_claude_entries("b-only", 3))
    cli._main(["peek", f"claude:{A}", "--json"])
    out = json.loads(capsys.readouterr().out)
    assert out["worker"]["handle"] == f"claude:{A}"
    assert out["worker"]["nativeSessionId"] == store.record(A)["native"]["sessionId"]
    assert out["last_user"]["text"] == "private a-only"
    assert out["last_assistant"]["text"] == "synthetic reply"
    assert "b-only" not in json.dumps(out)
    assert out["completeness"]["status"] == "complete"
    assert out["evidence"]["status"] == "absent"


def test_cli_peek_reads_one_original_codex_thread_with_native_id(
    original_codex, monkeypatch, capsys,
):
    from ccx.codex import runtime as codex_runtime
    store, cwd, wid, tid = original_codex
    native = _codex_turn([
        _user_message("native-user", "actual Codex native prompt"),
        {"type": "commandExecution", "id": "native-tool", "command": "echo fake",
         "exitCode": 0, "status": "completed", "aggregatedOutput": "fake output"},
        {"type": "agentMessage", "id": "native-final", "text": "actual original answer"},
    ])
    native["id"] = tid
    calls = []

    def read_thread(thread_id, *, cwd, timeout, worker_id, expected_source):
        assert expected_source == store.codex_source(wid)
        calls.append((thread_id, cwd, timeout, worker_id))
        return native

    monkeypatch.setattr(codex_runtime, "read_thread", read_thread)
    cli._main(["peek", f"codex:{wid}", "--json"])
    value = json.loads(capsys.readouterr().out)
    assert value["worker"]["handle"] == f"codex:{wid}"
    assert value["worker"]["nativeSessionId"] == tid
    assert [item["id"] for item in value["recent_items"]] == [
        "native-user", "native-tool", "native-final"]
    assert value["last_assistant"]["text"] == "actual original answer"
    assert value["tools"][0]["result"]["content"] == "fake output"
    assert calls == [(tid, str(cwd), 120, wid)]
    assert not (store.root / "sessions").exists()

def test_codex_background_status_cli_uses_recorded_summary(source_peek, capsys):
    from ccx.ops.background import summary as make_summary
    root, _, reads = source_peek
    store = require_ccx_active(root)
    expected = make_summary([{
        "id": "command:exec-one", "kind": "command",
        "status": "running", "label": "native command",
    }], as_of_ms=1793548540000)
    store.mutate_record(B, patches={"backgroundTaskSummary": expected})
    cli._main(["peek", B, "--status", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["statuses"][0]["background_task_summary"] == expected
    assert reads == []
