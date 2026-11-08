"""Claude turn rule, session bookkeeping and per-item fields in the normalized document."""
from __future__ import annotations

import json

from ccx.ops import conversation as conv
from ccx.claude.transcript import _iter_transcript


def _write(tmp_path, rows):
    src = tmp_path / "t.jsonl"
    src.write_text("".join(json.dumps(r) + "\n" for r in rows))
    return src


def _build(src):
    return conv.build_claude(
        {"short": "966a8a1b", "sessionId": "s", "cwd": "/w", "transcript": str(src)},
        state="idle", worker="claude:966a8a1b",
    )


def _ts(second):
    return f"2026-11-08T10:00:{second:02d}.000Z"


def _user(uuid, text, second, **extra):
    return {"type": "user", "uuid": uuid, "timestamp": _ts(second),
            "message": {"role": "user", "content": text}, **extra}


def _say(uuid, text, second):
    return {"type": "assistant", "uuid": uuid, "timestamp": _ts(second),
            "message": {"id": uuid, "role": "assistant",
                        "content": [{"type": "text", "text": text}]}}


def _queued(uuid, prompt, second, mode="prompt"):
    return {"type": "attachment", "uuid": uuid, "timestamp": _ts(second),
            "attachment": {"type": "queued_command", "prompt": prompt,
                           "commandMode": mode}}


def _turn_end(second):
    return {"type": "system", "subtype": "turn_duration", "timestamp": _ts(second),
            "durationMs": 1000}


def _inputs(doc):
    return [[(i["kind"], i.get("nativeType"), i.get("text")) for i in t["input"]]
            for t in doc["turns"]]


def test_a_queued_steer_or_notification_stays_in_the_running_turn(tmp_path):
    doc = _build(_write(tmp_path, [
        _user("u1", "fix it", 1),
        _say("a1", "working", 2),
        _queued("q1", "also the tests", 3),
        _queued("q2", "<task-notification><summary>done</summary></task-notification>",
                4, mode="task-notification"),
        _say("a2", "fixed both", 5),
        _turn_end(6),
        _user("u2", "thanks", 7),
    ]))
    assert _inputs(doc) == [
        [("user-text", "user", "fix it"),
         ("user-text", "queued_command", "also the tests"),
         ("user-text", "queued_command",
          "<task-notification><summary>done</summary></task-notification>")],
        [("user-text", "user", "thanks")],
    ]
    first = sorted(doc["turns"][0]["input"] + doc["turns"][0]["output"],
                   key=lambda i: i["ordinal"])
    # The steer keeps its place between the assistant texts.
    assert [i.get("text") for i in first] == [
        "fix it", "working", "also the tests",
        "<task-notification><summary>done</summary></task-notification>", "fixed both"]


def test_a_queued_message_after_the_turn_ended_opens_a_turn(tmp_path):
    doc = _build(_write(tmp_path, [
        _user("u1", "fix it", 1),
        _say("a1", "done", 2),
        _turn_end(3),
        _queued("q1", "and now the docs", 4),
        _say("a2", "docs done", 5),
    ]))
    assert [len(t["input"]) for t in doc["turns"]] == [1, 1]
    assert doc["turns"][1]["input"][0]["text"] == "and now the docs"
    assert [t["sequence"] for t in doc["turns"]] == [1, 2]


def test_session_bookkeeping_opens_no_turn_and_takes_no_ordinal(tmp_path):
    doc = _build(_write(tmp_path, [
        {"type": "session-owner", "sessionId": "s"},
        {"type": "cost-state", "sessionId": "s", "totalCostUSD": 0},
        {"type": "attachment", "uuid": "d1", "timestamp": _ts(0),
         "attachment": {"type": "deferred_tools_record", "entries": []}},
        _user("u1", "hello", 1),
        {"type": "queue-operation", "operation": "enqueue", "timestamp": _ts(2)},
        {"type": "tools-used", "sessionId": "s", "tools": []},
        {"type": "attachment", "uuid": "r1", "timestamp": _ts(2),
         "attachment": {"type": "silent_turn_reminder", "text": "x"}},
        _say("a1", "hi", 3),
    ]))
    assert len(doc["turns"]) == 1
    items = doc["turns"][0]["input"] + doc["turns"][0]["output"]
    assert sorted((i["ordinal"], i["kind"]) for i in items) == [
        (0, "user-text"), (1, "assistant-text")]
    # Account identifiers in session-owner never reach the document.
    assert "session-owner" not in json.dumps(doc)


def test_meta_rows_are_marked(tmp_path):
    doc = _build(_write(tmp_path, [
        _user("u1", "hello", 1),
        _user("m1", "Base directory for this skill: /x", 2, isMeta=True),
        _say("a1", "hi", 3),
    ]))
    texts = {i["text"]: i for i in doc["turns"][0]["input"]}
    assert texts["Base directory for this skill: /x"]["meta"] is True
    assert "meta" not in texts["hello"]


def test_items_carry_their_own_times_and_tool_calls_their_result_time(tmp_path):
    doc = _build(_write(tmp_path, [
        _user("u1", "run it", 1),
        {"type": "assistant", "uuid": "a1", "timestamp": _ts(2),
         "message": {"id": "a1", "role": "assistant", "content": [
             {"type": "tool_use", "id": "toolu_1", "name": "Bash",
              "input": {"command": "pytest"}}]}},
        {"type": "user", "uuid": "r1", "timestamp": _ts(9),
         "message": {"role": "user", "content": [
             {"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok"}]}},
        _say("a2", "passed", 10),
    ]))
    turn = doc["turns"][0]
    user, = turn["input"]
    call, text = turn["output"]
    assert user["startedAtMs"] == 1794132001000
    assert user["timingSource"] == "claude-transcript"
    assert (call["startedAtMs"], call["completedAtMs"]) == (1794132002000, 1794132009000)
    assert text["startedAtMs"] == 1794132010000 and "completedAtMs" not in text


def test_full_documents_skip_paging_fingerprints_with_identical_turns(tmp_path):
    src = _write(tmp_path, [
        _user("u1", "hello", 1), _say("a1", "hi", 2),
        _queued("q1", "more", 3), _say("a2", "ok", 4),
    ])
    events = list(_iter_transcript(str(src), {}))
    with_fp = conv._normalize_claude_events(events, conv._Counters(), None)
    without = conv._normalize_claude_events(events, conv._Counters(), None,
                                            fingerprints=False)
    assert with_fp[0] == without[0]
    assert all(r["_ccx_fingerprint"] for r in with_fp[2])
    assert all(r["_ccx_fingerprint"] is None for r in without[2])


def test_codex_function_call_output_keeps_its_namespace():
    thread = {"id": "th", "turns": [{"id": "t1", "status": "completed", "items": [
        {"type": "functionCallOutput", "id": "f1", "callId": "c1",
         "namespace": "ccx", "name": "worker_notification", "output": "{}"},
        {"type": "functionCallOutput", "id": "f2", "callId": "c2", "output": "x"},
    ]}]}
    doc = conv.build_codex({"short": "c0de1234", "threadId": "th", "cwd": "/w"},
                           thread, state="idle", worker="codex:c0de1234")
    out = doc["turns"][0]["output"]
    assert [(i["name"], i["namespace"]) for i in out] == [
        ("worker_notification", "ccx"), (None, None)]
