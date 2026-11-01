"""Sparse native evidence: sanitization, integrity, and exact identity joins."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from ccx.codex import evidence as codex_evidence
from ccx.ops import conversation as conv
from ccx.codex.evidence import EvidenceJournal, JournalRead, build_index, journal_path, read_journal
from ccx.codex.provenance import new_client_message_id
from test_generation_store import A, short_root
from test_worker_telemetry import original_sources, original_codex


def _scan(events=(), *, exists=True):
    return JournalRead(A, Path("unused-evidence.jsonl"), exists=exists,
                       events=list(events), last_sequence=len(events),
                       partial_tail=False, malformed_lines=[])


def test_both_surfaces_share_one_sanitizer():
    from ccx.system import sanitize

    assert conv.safe_native is sanitize.safe_native
    payload = {"type": "reasoning", "content": "x", "summary": ["s"]}
    assert sanitize.safe_native(payload) == {"type": "reasoning",
                                             "summary": ["s"]}


def test_codex_wait_json_carries_evidence_health(capsys):
    from argparse import Namespace

    from ccx.cli import codex as codex_commands

    codex_commands._emit_wait(
        {
            "ok": True, "short": "abc12345", "state": "idle",
            "evidence": {"schemaVersion": 1, "status": "degraded",
                         "sequence": 4, "degradedReason": "disk was full",
                         "degradedAtMs": 1780000000000},
        },
        Namespace(json=True),
    )

    output = json.loads(capsys.readouterr().out)
    assert output["state"] == "idle"
    assert output["evidence"]["status"] == "degraded"


def test_no_hidden_reasoning_reaches_the_journal():
    event = codex_evidence.envelope("item/completed", {
        "threadId": "thread-1", "turnId": "turn-1",
        "completedAtMs": 1780000009999,
        "item": {
            "id": "r1", "type": "reasoning",
            "content": [{"text": "HIDDEN CHAIN OF THOUGHT"}],
            "summary": ["visible summary"],
            "nested": {"inner": {"type": "reasoningV2",
                                 "content": "ALSO HIDDEN"}},
        },
    }, sequence=1)

    blob = json.dumps(event)
    assert "HIDDEN CHAIN OF THOUGHT" not in blob
    assert "ALSO HIDDEN" not in blob
    assert "visible summary" in blob
    item = event["params"]["item"]
    assert "content" not in item
    assert "content" not in item["nested"]["inner"]
    assert item["summary"] == ["visible summary"]


def test_large_completed_command_output_is_retained():
    output = "line\n" * 40000
    completed = codex_evidence.envelope(
        "item/completed", {"item": {"type": "commandExecution",
                                    "aggregatedOutput": output}}, sequence=1)
    assert completed["params"]["item"]["aggregatedOutput"] == output


def test_structured_error_and_will_retry_are_retained():
    event = codex_evidence.envelope("error", {
        "threadId": "thread-1", "turnId": "turn-1", "willRetry": True,
        "error": {"type": "stream_error", "message": "reconnecting",
                  "attempt": 2},
    }, sequence=1)

    params = event["params"]
    assert params["willRetry"] is True
    assert params["error"] == {"type": "stream_error",
                               "message": "reconnecting", "attempt": 2}


def test_streaming_deltas_are_never_journaled_but_usage_enrichment_is(original_codex):
    _, _, owner, _ = original_codex
    journal = EvidenceJournal(owner)
    try:
        for method in ("item/agentMessage/delta", "item/commandExecution/outputDelta",
                       "item/reasoning/delta"):
            assert journal.record(method, {"delta": "private"}) is None
        assert not journal_path(owner).exists()
        journal.record(codex_evidence.TOKEN_USAGE_METHOD, {"turnId": "turn-1",
                       "tokenUsage": {"last": {"inputTokens": 10}}})
        events = read_journal(owner).events
        assert [e["method"] for e in events] == [
            codex_evidence.HEADER_METHOD, codex_evidence.TOKEN_USAGE_METHOD]
        assert events[-1]["params"]["tokenUsage"]["last"]["inputTokens"] == 10
    finally:
        journal.close()


def test_a_trailing_partial_line_never_reuses_a_sequence(original_codex):
    _, _, owner, _ = original_codex
    journal = EvidenceJournal(owner)
    journal.record("turn/started", {"turn": {"id": "turn-1"}})
    journal.close()
    with journal_path(owner).open("a") as handle:
        handle.write('{"schemaVersion":1,"sequence":3,"method":"item/comple')
    scan = read_journal(owner)
    assert scan.partial_tail and not scan.complete and scan.last_sequence == 2
    reopened = EvidenceJournal(owner)
    try:
        reopened.record("turn/completed", {"turn": {"id": "turn-1"}})
        scan = read_journal(owner)
        assert [e["sequence"] for e in scan.events] == [1, 2, 4, 5]
        assert scan.malformed_lines == [3]
        assert any("gap (3 never landed)" in w for w in scan.warnings)
        assert not scan.complete
        assert "partial line" in reopened.state()["degradedReason"]
    finally:
        reopened.close()


@pytest.mark.parametrize("sequences, expected", [
    ([1, 2, 4], "gap"), ([1, 2, 2], "duplicate"), ([1, 3, 2], "backwards"),
])
def test_a_broken_sequence_chain_is_degraded(original_codex, sequences, expected):
    _, _, owner, tid = original_codex
    path = journal_path(owner)
    path.write_text("".join(json.dumps(codex_evidence.envelope(
        "turn/started", {"threadId": tid, "turn": {"id": "turn-1"}}, sequence=n
    )) + "\n" for n in sequences))
    path.chmod(0o600)
    scan = read_journal(owner)
    assert not scan.complete and scan.malformed_lines == []
    assert any(expected in w for w in scan.warnings), scan.warnings
    assert build_index(scan)["status"] == "degraded"


def test_a_parseable_line_that_is_not_an_envelope_is_degraded(original_codex):
    _, _, owner, _ = original_codex
    journal = EvidenceJournal(owner)
    journal.record("turn/started", {"turn": {"id": "turn-1"}})
    journal.close()
    with journal_path(owner).open("a") as handle:
        handle.write('not json\n{"sequence":3,"method":"turn/started"}\n')
        handle.write(json.dumps(codex_evidence.envelope(
            "turn/completed", {"turn": {"id": "turn-1"}}, sequence=4)) + "\n")
    scan = read_journal(owner)
    assert scan.malformed_lines == [3, 4] and not scan.complete
    assert scan.last_sequence == 4
    assert [e["sequence"] for e in scan.events] == [1, 2, 4]


def test_evidence_stays_degraded_once_it_has_lost_an_event(original_codex, monkeypatch):
    _, _, owner, _ = original_codex
    journal = EvidenceJournal(owner)
    journal.record("turn/started", {"turn": {"id": "turn-1"}})
    def fail(*args, **kwargs):
        raise OSError("disk full")
    with monkeypatch.context() as patch:
        patch.setattr(journal, "_write_locked", fail)
        assert journal.record("item/completed", {"item": {"id": "lost"}}) is None
    journal.record("turn/completed", {"turn": {"id": "turn-1"}})
    health = journal.state()
    journal.close()
    assert health["status"] == "degraded" and "disk full" in health["degradedReason"]
    reopened = EvidenceJournal(owner, health=health)
    try:
        reopened.record("turn/started", {"turn": {"id": "turn-2"}})
        assert reopened.state()["degradedReason"] == health["degradedReason"]
        assert reopened.state()["degradedAtMs"] == health["degradedAtMs"]
        assert build_index(read_journal(owner), health=health)["status"] == "degraded"
    finally:
        reopened.close()


def test_a_failed_final_flush_degrades_evidence_without_raising(original_codex, monkeypatch):
    _, _, owner, _ = original_codex
    journal = EvidenceJournal(owner)
    journal.record("turn/started", {"turn": {"id": "turn-1"}})
    inode = journal_path(owner).stat()
    fsync = os.fsync
    def fail(fd):
        actual = os.fstat(fd)
        if (actual.st_dev, actual.st_ino) == (inode.st_dev, inode.st_ino):
            raise OSError("fsync failed")
        return fsync(fd)
    monkeypatch.setattr(os, "fsync", fail)
    journal.close()
    assert journal.closed
    assert journal.state()["status"] == "degraded"
    assert "final flush failed" in journal.state()["degradedReason"]


def test_a_degraded_record_outranks_a_syntactically_clean_file():
    event = codex_evidence.envelope("turn/started", {}, sequence=1)
    scan = _scan([event])
    assert scan.complete
    index = build_index(scan, health={"status": "degraded", "sequence": 1,
                                     "degradedReason": "disk was full"})
    assert index["status"] == "degraded"
    assert any("disk was full" in w for w in index["warnings"])


def test_remembered_evidence_with_no_file_is_lost_not_absent():
    index = build_index(_scan(exists=False), health={"status": "complete", "sequence": 12})
    assert not index["available"] and index["status"] == "degraded"
    assert any("journal file is gone" in w for w in index["warnings"])


def test_a_file_ahead_of_the_record_is_not_evidence_loss():
    events = [codex_evidence.envelope("turn/started", {}, sequence=n) for n in (1, 2, 3)]
    index = build_index(_scan(events), health={"status": "complete", "sequence": 1})
    assert index["status"] == "complete" and index["warnings"] == []


def test_a_record_ahead_of_the_file_is_degraded():
    scan = _scan([codex_evidence.envelope("turn/started", {}, sequence=1)])
    index = build_index(scan, health={"status": "complete", "sequence": 9})
    assert index["status"] == "degraded"
    assert any("ends at 1" in w for w in index["warnings"])


CCX_START = new_client_message_id(A, "start")


def _client_thread(client_id, *, turn_id="turn-1", history_id="item-1"):
    return {"id": "thread-1", "cwd": "/workspace", "turns": [{
        "id": turn_id, "status": "completed", "itemsView": "full",
        "startedAt": 1780000000, "completedAt": 1780000006,
        "items": [{"type": "userMessage", "id": history_id,
                   "clientId": client_id,
                   "content": [{"type": "text", "text": "go"}]}],
    }]}


def _journal_user_message(*, client_id, item_id="live-user-1", turn_id="turn-1", extra_item_id=None):
    events = []
    for live_id in [item_id] + ([extra_item_id] if extra_item_id else []):
        item = {"id": live_id, "type": "userMessage", "clientId": client_id,
                "content": [{"type": "text", "text": "go"}]}
        for method, key, at in (("item/started", "startedAtMs", 1780000000123),
                                ("item/completed", "completedAtMs", 1780000000456)):
            events.append(codex_evidence.envelope(method, {
                "threadId": "thread-1", "turnId": turn_id, key: at, "item": item,
            }, sequence=len(events)+1))
    return build_index(_scan(events))


def _document(thread, evidence):
    return conv.build_codex({"workerId": A}, thread, state="idle",
                            worker=f"codex:{A}", evidence=evidence)


def test_a_user_message_is_matched_by_its_ccx_client_id():
    """The one identity both surfaces genuinely share, and it is exact."""
    index = _journal_user_message(client_id=CCX_START)

    doc = _document(_client_thread(CCX_START), index)
    message = doc["turns"][0]["input"][0]
    assert message["startedAtMs"] == 1780000000123
    assert message["completedAtMs"] == 1780000000456
    assert message["timingSource"] == "ccx-live-item-notification"
    # Neither native id was rewritten, and no alias was created.
    assert message["id"] == "item-1"
    assert message["clientId"] == CCX_START
    assert set(index["items"]["turn-1"]) == {"live-user-1"}
    assert doc["evidence"]["enrichedItemCount"] == 1


@pytest.mark.parametrize("history_client, live_client, turn_id, extra_live", [
    ("user_msg_42", "user_msg_42", "turn-1", None),
    (None, CCX_START, "turn-1", None),
    (CCX_START, CCX_START, "turn-9", None),
    (CCX_START, CCX_START, "turn-1", "live-user-2"),
], ids=["foreign", "null", "other-turn", "ambiguous"])
def test_the_client_id_join_refuses_anything_inexact(history_client, live_client, turn_id, extra_live):
    index = _journal_user_message(client_id=live_client, turn_id=turn_id, extra_item_id=extra_live)
    doc = _document(_client_thread(history_client), index)
    message = doc["turns"][0]["input"][0]
    assert "startedAtMs" not in message and "timingSource" not in message
    assert doc["evidence"]["enrichedItemCount"] == 0


def test_an_exact_item_id_match_still_wins():
    events = []
    for live_id, at in (("item-1", 1780000001000), ("live-user-1", 1780000002000)):
        events.append(codex_evidence.envelope("item/started", {
            "threadId": "thread-1", "turnId": "turn-1", "startedAtMs": at,
            "item": {"id": live_id, "type": "userMessage", "clientId": CCX_START},
        }, sequence=len(events)+1))
    message = _document(_client_thread(CCX_START), build_index(_scan(events)))["turns"][0]["input"][0]
    assert message["startedAtMs"] == 1780000001000


def test_a_half_watched_turn_cannot_convict_the_source():
    # A complete journal that began mid-turn cannot prove historical loss.
    events = [codex_evidence.envelope("item/completed", {
        "threadId": "thread-1", "turnId": "turn-1",
        "item": {"id": f"live-{n}", "type": "agentMessage"},
    }, sequence=n+1) for n in range(6)]
    index = build_index(_scan(events))
    doc = _document(_client_thread(None), index)
    assert doc["evidence"]["status"] == "complete"
    assert doc["completeness"]["status"] == "complete"
    assert len(doc["turns"][0]["input"]) == 1


def test_usage_fsync_policy_is_turn_identity_aware(original_codex, monkeypatch):
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker

    store, _, owner, tid = original_codex
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, store.record(owner))
    samples = []
    original = worker.evidence.record
    def capture(method, params, **kwargs):
        samples.append((method, params.get("turnId"), kwargs.get("durable")))
        return original(method, params, **kwargs)
    monkeypatch.setattr(worker.evidence, "record", capture)
    usage = {"threadId": tid, "turnId": "turn-old",
             "tokenUsage": {"last": {"inputTokens": 10}}}
    try:
        worker.on_notification("turn/started", {"threadId": tid, "turn": {"id": "turn-old"}})
        worker.on_notification(codex_evidence.TOKEN_USAGE_METHOD, usage)
        assert samples[-1] == (codex_evidence.TOKEN_USAGE_METHOD, "turn-old", False)
        worker.on_notification("turn/completed", {
            "threadId": tid, "turn": {"id": "turn-old", "status": "completed"}})
        worker.on_notification("turn/started", {"threadId": tid, "turn": {"id": "turn-new"}})
        worker.on_notification("turn/completed", {
            "threadId": tid, "turn": {"id": "turn-new", "status": "completed"}})
        worker.on_notification(codex_evidence.TOKEN_USAGE_METHOD, usage)
        assert samples[-1] == (codex_evidence.TOKEN_USAGE_METHOD, "turn-old", True)
    finally:
        worker.evidence.close()
    reloaded = Worker(daemon, store.record(owner))
    try:
        assert {"turn-old", "turn-new"} <= reloaded.completed_turn_ids
        late = []
        original = reloaded.evidence.record
        def capture_late(method, params, **kwargs):
            late.append(kwargs.get("durable"))
            return original(method, params, **kwargs)
        monkeypatch.setattr(reloaded.evidence, "record", capture_late)
        reloaded.on_notification(codex_evidence.TOKEN_USAGE_METHOD, usage)
        assert late == [True]
    finally:
        reloaded.evidence.close()


def test_codex_compact_sparse_only_from_admitted_items_even_with_duplicates_and_reload(
    original_codex,
):
    """The producer, not a cursor reader, owns native compact acceptance."""
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    from ccx.ops.event_feed import _codex_project
    from ccx.codex.evidence import COMPACT_STARTED_METHOD, COMPACT_ENDED_METHOD

    store, _, owner, tid = original_codex
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, store.record(owner))
    turn_id, item_id = "auto-compact-turn", "auto-item"

    def item(method, turn, item):
        return {"threadId": tid, "turnId": turn,
                "item": {"type": "contextCompaction", "id": item}}

    def sparse():
        events = read_journal(owner).events
        return [e for e in events if e["method"] in (
            COMPACT_STARTED_METHOD, COMPACT_ENDED_METHOD,
        )]

    try:
        worker.on_notification(
            "turn/started", {"threadId": tid, "turn": {"id": turn_id}},
        )
        # A foreign item with no admitted turn may be journaled for forensic
        # inspection, but may NEVER cause a neutral compact event.
        worker.on_notification("item/started", item("item/started", "foreign", "f"))
        worker.on_notification("item/completed", item("item/completed", "foreign", "f"))
        assert sparse() == []
        for _ in range(2):
            worker.on_notification("item/started", item("item/started", turn_id, item_id))
        first = sparse()
        assert [e["method"] for e in first] == [COMPACT_STARTED_METHOD]
        assert first[0]["params"]["compactId"] == f"{turn_id}:{item_id}"
        for _ in range(2):
            worker.on_notification("item/completed", item("item/completed", turn_id, item_id))
        original_events = read_journal(owner).events
        native = [e for e in original_events if e["method"] in (
            "item/started", "item/completed"
        ) and e.get("itemType") == "contextCompaction"]
        assert len(native) == 6  # raw duplicate and foreign records unchanged
        assert all(_codex_project(e) is None for e in native)
        sparse_lines = sparse()
        assert [e["method"] for e in sparse_lines] == [
            COMPACT_STARTED_METHOD, COMPACT_ENDED_METHOD,
        ]
        assert [e["params"]["compactId"] for e in sparse_lines] == [
            f"{turn_id}:{item_id}", f"{turn_id}:{item_id}",
        ]
        assert sparse_lines[-1]["params"]["outcome"] == "completed"
        worker.on_notification("turn/completed", {
            "threadId": tid,
            "turn": {"id": turn_id, "status": "completed", "items": []},
        })
    finally:
        worker.evidence.close()
    reloaded = Worker(daemon, store.record(owner))
    try:
        assert f"{turn_id}:{item_id}" in reloaded._compact_ended
        # Old turn is no longer admitted after a reload. Never republish.
        reloaded.on_notification("item/completed", item("item/completed", turn_id, item_id))
        assert len(sparse()) == 2
    finally:
        reloaded.evidence.close()


@pytest.mark.parametrize("case,expected", (
    ("normal", "completed"),
    ("failed", "failed"),
    ("systemError", "unknown"),
    ("no-start", "completed"),
    ("legacy", "completed"),
))
def test_codex_admitted_auto_compact_terminal_is_one_sparse_pair(
    original_codex, case, expected,
):
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    from ccx.codex.evidence import COMPACT_STARTED_METHOD, COMPACT_ENDED_METHOD

    store, _, owner, tid = original_codex
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, store.record(owner))
    turn_id = "auto-test-" + case
    native_item = {
        "threadId": tid, "turnId": turn_id,
        "item": {"type": "contextCompaction", "id": "compact-item"},
    }
    try:
        worker.on_notification(
            "turn/started", {"threadId": tid, "turn": {"id": turn_id}},
        )
        if case != "no-start" and case != "legacy":
            worker.on_notification("item/started", native_item)
        if case == "legacy":
            worker.on_notification("thread/compacted", {
                "threadId": tid, "turnId": turn_id,
            })
        if case in ("normal", "no-start"):
            worker.on_notification("item/completed", native_item)
        elif case == "systemError":
            worker.on_notification("thread/status/changed", {
                "threadId": tid, "status": {"type": "systemError"},
            })
        else:
            worker.on_notification("turn/completed", {
                "threadId": tid,
                "turn": {
                    "id": turn_id, "status": "failed" if case == "failed"
                    else "completed", "items": [],
                },
            })
        lines = [
            e for e in read_journal(owner).events
            if e["method"] in (COMPACT_STARTED_METHOD, COMPACT_ENDED_METHOD)
        ]
        assert [line["method"] for line in lines] == [
            COMPACT_STARTED_METHOD, COMPACT_ENDED_METHOD,
        ]
        assert lines[0]["params"]["compactId"] == lines[1]["params"]["compactId"]
        assert lines[-1]["params"]["outcome"] == expected
    finally:
        worker.evidence.close()


def test_background_command_promoted_at_turn_end_and_late_exit_pushed(original_codex):
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    from ccx.codex.evidence import BACKGROUND_CHANGED_METHOD
    from ccx.ops.event_feed import _codex_project

    store, _, owner, tid = original_codex
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, store.record(owner))
    turn = "background-turn"
    command = "exec-test"
    bg_params = {
        "threadId": tid, "turnId": turn,
        "item": {"id": command, "type": "commandExecution",
                 "command": "sleep 25", "status": "inProgress"},
    }
    def sparse():
        return [row for row in read_journal(owner).events
                if row.get("method") == BACKGROUND_CHANGED_METHOD]
    try:
        worker.on_notification("turn/started", {
            "threadId": tid, "turn": {"id": turn}})
        worker.on_notification("item/started", bg_params)
        assert sparse() == []
        worker.on_notification("turn/completed", {
            "threadId": tid, "turn": {"id": turn, "status": "completed"}})
        events = sparse()
        assert len(events) == 1 and events[0]["params"]["transition"] == "started"
        assert events[0]["params"]["summary"]["running_count"] == 1
        assert worker.record["lifecycle"]["state"] == "idle"
        assert worker._bg_open
        worker.on_notification("item/completed", {
            **bg_params, "item": {**bg_params["item"],
                                   "status": "completed", "exitCode": 0}})
        events = sparse()
        assert [row["params"]["transition"] for row in events] == [
            "started", "finished"]
        assert events[-1]["params"]["summary"]["running_count"] == 0
        assert worker._bg_open == {}
        assert store.record(owner)["backgroundTaskSummary"]["running_count"] == 0
        assert _codex_project(events[-1])["kind"] == "background.changed"
        # A replayed native exit must not double-publish completion.
        worker.on_notification("item/completed", {
            **bg_params, "item": {**bg_params["item"],
                                   "status": "completed", "exitCode": 0}})
        assert len(sparse()) == 2
    finally:
        worker.evidence.close()


def test_foreground_command_is_not_background(original_codex):
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker
    from ccx.codex.evidence import BACKGROUND_CHANGED_METHOD
    store, _, owner, tid = original_codex
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, store.record(owner))
    try:
        worker.on_notification("turn/started", {
            "threadId": tid, "turn": {"id": "foreground"}})
        item = {"threadId": tid, "turnId": "foreground",
                "item": {"id": "fast", "type": "commandExecution",
                         "status": "completed", "exitCode": 0}}
        worker.on_notification("item/started", item)
        worker.on_notification("item/completed", item)
        worker.on_notification("turn/completed", {
            "threadId": tid, "turn": {"id": "foreground", "status": "completed"}})
        assert worker._bg_open == {}
        assert not [row for row in read_journal(owner).events
                    if row.get("method") == BACKGROUND_CHANGED_METHOD]
    finally:
        worker.evidence.close()
