"""`ccx history`: a text projection of the normalized conversation document.

Everything is synthetic: Claude JSONL written in-test, a Codex history
database built in-test, and documents built by the real converters. The one
invariant that matters most: every address history prints is the item's
``ordinal`` in the normalized document (the numbers `ccx logs` prints).
"""
from __future__ import annotations

import io
import json
import re
import sqlite3
import time
from pathlib import Path

import pytest

from ccx import constants
from ccx.cli import main as cli
from ccx.codex import history_store
from ccx.errors import InvalidRequest
from ccx.ops import conversation as conv
from ccx.ops import history as hist
from ccx.workers import WorkerClient, _note_history_db

from test_conversation import _codex_turn, _user_message, build_codex
from test_generation_store import A, add_worker, short_root  # noqa: F401

TS0 = "2026-11-02T10:00:00.000Z"


def _ts(minutes: float) -> str:
    seconds = 10 * 3600 + int(minutes * 60)
    return f"2026-11-02T{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}.000Z"


def _user(uid, text, minute, **extra):
    return {"type": "user", "uuid": uid, "timestamp": _ts(minute),
            "message": {"role": "user", "content": text}, **extra}


def _assistant(uid, minute, *blocks, mid=None):
    return {"type": "assistant", "uuid": uid, "timestamp": _ts(minute),
            "message": {"id": mid or f"m-{uid}", "model": "claude-fake", "content": list(blocks)}}


def _text(text):
    return {"type": "text", "text": text}


def _bash(tid, command, description):
    return {"type": "tool_use", "id": tid, "name": "Bash",
            "input": {"command": command, "description": description}}


def _result(uid, tid, content, minute, is_error=False):
    return {"type": "user", "uuid": uid, "timestamp": _ts(minute),
            "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": tid, "content": content,
                 "is_error": is_error}]}}


def _queued(uid, prompt, minute):
    return {"type": "attachment", "uuid": uid, "timestamp": _ts(minute),
            "attachment": {"type": "queued_command", "prompt": prompt}}


NOTIFICATION = ("<task-notification>\n<task-id>b1</task-id>\n<status>completed</status>\n"
                "<summary>Background command \"Run the suite\" completed (exit code 0)</summary>\n"
                "</task-notification>")
MONITOR = ("<task-notification>\n<task-id>m1</task-id>\n<summary>Monitor event: build</summary>\n"
           "<event>build finished: 3 warnings</event>\n</task-notification>")
SUMMARY = ("This session is being continued from a previous conversation that ran out of "
           "context. The summary below covers the earlier portion of the conversation.\n\n"
           "Summary:\n1. Primary Request and Intent:\n   Ship the parser.\n"
           "8. Current Work:\n   Writing the history tests.\n"
           "9. Optional Next Step:\n   Run the suite.\n\n"
           "If you need specific details from before compaction, read the full transcript at: /x.jsonl")


def claude_events():
    return [
        _user("u1", "Please run the tests and fix the parser", 0),
        _assistant("a1", 0.5, _text("Running the suite first."),
                   _bash("t1", "cd /w && timeout 30 .venv/bin/python -m pytest -q 2>&1 | tail -3",
                         "Run the test suite")),
        _queued("q1", "also check the docs", 1),
        _result("r1", "t1", "3 failed, 40 passed", 2, is_error=True),
        _queued("q2", MONITOR, 2.5),
        _assistant("a2", 3, _text("Three tests fail in the parser; fixed and re-ran: all pass.")),
        _user("u2", NOTIFICATION, 10),
        _assistant("a3", 10.5, _text("The background run passed.")),
        _user("u3", "/compact", 20),
        {"type": "system", "subtype": "compact_boundary", "uuid": "cb1", "timestamp": _ts(21),
         "content": "Conversation compacted",
         "compactMetadata": {"trigger": "manual", "preTokens": 180000}},
        _user("cs1", SUMMARY, 21, isCompactSummary=True, parentUuid="cb1"),
        _user("u4", "[Request interrupted by user]", 30),
        _user("u5", "now deploy it", 31),
        _assistant("a5", 31.5, _text("Deploying."),
                   _bash("t5", "if grep -q ok status; then ./deploy.sh; fi", "Deploy")),
    ]


def write_jsonl(path: Path, events) -> Path:
    path.write_text("".join(json.dumps(e) + "\n" for e in events))
    return path


def claude_doc(tmp_path, events=None, state="idle"):
    path = write_jsonl(tmp_path / "t.jsonl", claude_events() if events is None else events)
    rec = {"short": "966a8a1b", "sessionId": "s", "cwd": "/workspace",
           "transcript": str(path), "createArgs": {}}
    return conv.build_claude(rec, state=state, worker="claude:966a8a1b", max_tool_result_bytes=4096)


def items_by_address(doc):
    return {(t["sequence"], i["ordinal"]): i for t in doc["turns"] for i in t["input"] + t["output"]}


def render(doc, now=None, **kwargs):
    out = io.StringIO()
    opts = hist.options(now=now, **kwargs)
    hint = hist.render_history(hist.project(doc, name="w"), opts, out, many=False, zone="UTC")
    return out.getvalue() + ("\n" + hist.hint_line(hint) if hint else "")


# --- projection ----------------------------------------------------------------

def test_every_projected_message_keeps_its_document_address(tmp_path):
    doc = claude_doc(tmp_path)
    items = items_by_address(doc)
    h = hist.project(doc)
    assert h.messages()
    for m in h.messages():
        assert (m.turn, m.n) in items


def test_every_printed_address_is_a_document_ordinal(tmp_path):
    doc = claude_doc(tmp_path)
    items = items_by_address(doc)
    outputs = [render(doc), render(doc, view="trace", turns="1-"), render(doc, view="chat", turns="1-"),
               render(doc, grep="."), render(doc, view="map"), render(doc, errors=True),
               render(doc, cmd="python|grep|deploy.sh")]
    full = re.compile(r"#(\d+)\.(\d+)")
    seen = 0
    for text in outputs:
        for t, n in full.findall(text):
            assert (int(t), int(n)) in items, (t, n, text)
            seen += 1
    # Inside a turn block, `[.N` / `.N` are short addresses in that turn.
    for block in render(doc, view="trace", turns="1-").split("━━ #")[1:]:
        turn = int(block.split()[0])
        for n in re.findall(r"^\s*\[?\.(\d+)", block, re.M):
            assert (turn, int(n)) in items
            seen += 1
    assert seen > 20


def test_roles_tags_and_finals(tmp_path):
    h = hist.project(claude_doc(tmp_path))
    tags = [(m.role, hist.tag(m)) for m in h.messages()]
    assert ("user", "U") in tags
    assert ("tool", "T") in tags
    assert ("compact", "══") in tags
    assert ("interrupt", "!") in tags
    notes = {m.kind: m for m in h.messages() if m.role == "notify"}
    assert notes["bg"].text.startswith('Background command "Run the suite" completed')
    assert notes["monitor"].out == "build finished: 3 warnings"
    finals = [m.text for m in h.messages() if m.final == "final"]
    assert "Three tests fail in the parser; fixed and re-ran: all pass." in finals
    compact = next(m for m in h.messages() if m.role == "compact")
    assert compact.text == "manual · 180k tokens"
    assert compact.out.startswith("Summary:") and compact.out.endswith("Run the suite.")
    assert all(not m.text.startswith("<task-notification>") for m in h.messages())


def test_queued_messages_stay_in_the_running_turn(tmp_path):
    h = hist.project(claude_doc(tmp_path))
    steer = next(m for m in h.messages() if m.text == "also check the docs")
    monitor = next(m for m in h.messages() if m.kind == "monitor")
    prompt = next(m for m in h.messages() if m.text.startswith("Please run the tests"))
    assert steer.turn == monitor.turn == prompt.turn
    assert [hist.tag(m) for m in (prompt, steer, monitor)] == ["U", "U+", "N[monitor]"]


def test_claude_items_carry_their_own_times(tmp_path):
    h = hist.project(claude_doc(tmp_path))
    tool = next(m for m in h.messages() if m.role == "tool" and "pytest" in (m.cmd or ""))
    assert tool.ts is not None and tool.dur == 90.0           # call at 0:30, result at 2:00
    late = next(m for m in h.messages() if m.text.startswith("Three tests fail"))
    assert late.ts - tool.ts == 150


def test_claude_meta_rows_are_not_user_messages(tmp_path):
    events = [_user("u1", "<local-command-caveat>Caveat: generated by a local command",
                    0, isMeta=True),
              _user("u2", "/model", 0.1),
              _user("u3", "Base directory for this skill: /skills/x", 1, isMeta=True),
              _user("u4", "real prompt", 2),
              _assistant("a4", 2.5, _text("answer"))]
    h = hist.project(claude_doc(tmp_path, events))
    tags = [(hist.tag(m), m.text[:20]) for m in h.messages()]
    assert ("U", "real prompt") in tags
    assert not any(t == "U" and text.startswith(("<local", "Base dir")) for t, text in tags)
    assert ("N[system]", "Base directory for t") in tags


def test_failed_tool_call_carries_command_description_and_error(tmp_path):
    h = hist.project(claude_doc(tmp_path))
    tool = next(m for m in h.messages() if m.role == "tool" and "pytest" in (m.cmd or ""))
    assert tool.err and tool.desc == "Run the test suite" and tool.out == "3 failed, 40 passed"


def test_busy_worker_latest_text_is_not_final(tmp_path):
    events = claude_events()[:2]          # tool call still running
    h = hist.project(claude_doc(tmp_path, events, state="busy"))
    latest = [m for m in h.messages() if m.role == "assistant"]
    assert hist.tag(latest[-1]) == "A…"
    assert not [m for m in h.messages() if m.final == "final"]


def test_meta_namespace_cron_and_timing_projection():
    """Fields the normalized document carries (meta, namespace, item timing)."""
    t0 = 1_793_000_000_000
    doc = {"worker": {"engine": "claude", "short": "s"}, "state": "idle", "turns": [
        {"sequence": 1, "startedAt": "2026-11-02T10:00:00Z", "completedAt": "2026-11-02T10:05:00Z",
         "status": "completed", "input": [
             {"kind": "user-text", "ordinal": 0, "text": "<local-command-caveat>x", "meta": True},
             {"kind": "user-text", "ordinal": 1, "text": "Base directory for this skill: /s",
              "meta": True}],
         "output": [
             {"kind": "assistant-text", "ordinal": 2, "text": "ok", "startedAtMs": t0},
             {"kind": "tool-call", "ordinal": 3, "name": "Bash",
              "arguments": {"command": "make"}, "status": "completed",
              "result": {"content": "done", "isError": False},
              "startedAtMs": t0, "completedAtMs": t0 + 12_000},
             {"kind": "other", "ordinal": 4, "nativeType": "system",
              "native": {"subtype": "scheduled_task_fire", "content": "cron"}}]},
        {"sequence": 2, "startedAt": "2026-11-02T11:00:00Z", "completedAt": None,
         "status": "completed", "input": [
             {"kind": "user-text", "ordinal": 0, "text": "check the queue", "meta": True}],
         "output": [
             {"kind": "tool-result", "ordinal": 1, "nativeType": "functionCallOutput",
              "name": "worker_notification", "namespace": "ccx",
              "result": {"content": json.dumps({"worker": "codex:ab", "status": "completed",
                                                "report": "all green"})}}]},
    ]}
    h = hist.project(doc)
    by = {(m.turn, m.n): m for m in h.messages()}
    assert (1, 0) not in by                                   # caveat hidden
    assert hist.tag(by[(1, 1)]) == "N[system]"
    assert by[(1, 2)].ts == t0 / 1000
    assert by[(1, 3)].dur == 12.0
    assert (1, 4) not in by                                    # the fire marker itself
    assert hist.tag(by[(2, 0)]) == "N[cron]" and by[(2, 0)].text == "check the queue"
    assert hist.tag(by[(2, 1)]) == "N[ccx]" and by[(2, 1)].text == "codex:ab completed: all green"


# --- output each operation picks -------------------------------------------------

def test_no_selector_gives_outline_with_addresses(tmp_path):
    text = render(claude_doc(tmp_path))
    assert re.search(r"^#1\.0 \S+ \S+ U: Please run the tests", text, re.M)
    assert "A*:" in text and "━━" not in text and "next:" not in text


def test_turns_give_chat_and_an_address_gives_one_message_whole(tmp_path):
    doc = claude_doc(tmp_path)
    assert "━━ #1" in render(doc, turns="1")
    compact = next(m for m in hist.project(doc).messages() if m.role == "compact")
    whole = render(doc, turns=f"{compact.turn}.{compact.n}")
    assert "Writing the history tests." in whole and "This session is being continued" not in whole
    tail = render(doc, turns=f"{compact.turn}.{compact.n}", max_chars=30)
    assert "earlier chars]" in tail and tail.rstrip().endswith("Run the suite.")
    assert "(no message #0.0" in render(doc, turns="0.0")


def test_search_gives_newest_hits_and_a_next_hint(tmp_path):
    doc = claude_doc(tmp_path)
    text = render(doc, grep="pars", limit=1)
    assert re.search(r"\d+ hits, newest 1 shown", text)
    assert "fixed and re-ran" in text and "Please run the tests" not in text
    assert "next: ccx history 966a8a1b --turns" in text
    assert "━━" in render(doc, grep="pars", context=0)
    assert "━━" in render(doc, grep="pars", view="chat")


def test_cmd_and_errors_select_tool_calls(tmp_path):
    doc = claude_doc(tmp_path)
    text = render(doc, cmd="python", role="tool,output")
    assert "1 hit" in text and "⤷ 3 failed, 40 passed" in text
    assert "1 hit" in render(doc, cmd="grep")                  # the `if` condition
    assert "1 hit" in render(doc, errors=True)


def test_role_selects_what_turns_show(tmp_path):
    doc = claude_doc(tmp_path)
    text = render(doc, role="notify")
    assert "N[bg]" in text and "U: Please run" not in text and "A*" not in text


def test_time_range_selects_overlapping_turns(tmp_path):
    doc = claude_doc(tmp_path)
    now = time.mktime(time.strptime("2026-11-02 12:00", "%Y-%m-%d %H:%M"))
    early = render(doc, since="2026-11-01", until="2026-11-01", now=now)
    assert "(no turns in range)" in early


def test_map_titles_from_compaction_current_work(tmp_path):
    text = render(claude_doc(tmp_path), view="map")
    assert "compaction, current work: Writing the history tests." in text


@pytest.mark.parametrize("kwargs, message", [
    ({"grep": "("}, "invalid regular expression"),
    ({"turns": "abc"}, "cannot read --turns"),
    ({"since": "yesterday"}, "cannot read time"),
    ({"limit": 0}, "--limit must be at least 1"),
    ({"role": "users"}, "unknown role users"),
])
def test_bad_input_is_a_usage_error(kwargs, message):
    with pytest.raises(InvalidRequest, match=re.escape(message)):
        hist.options(**kwargs)


# --- shell command words ---------------------------------------------------------

@pytest.mark.parametrize("command, words", [
    ("cd x && pytest -q | tail -3", ["cd", "pytest", "tail"]),
    ("PYTHONPATH=. timeout 30 .venv/bin/python3.12 -m pytest", ["python", "pytest"]),
    ("python -m pytest", ["python", "pytest"]),
    (".venv/bin/python -m pytest -q", ["python", "pytest"]),
    ("uv run python -m pytest", ["python", "pytest"]),
    ("python3 -B -X dev -m http.server 8000", ["python", "http.server"]),
    ("python -c 'import pytest' && python script.py -m x", ["python", "python"]),
    ("env -u CCX_SELF_WORKER_ID /usr/bin/python - <<'PY'\nimport os; os.system('rm -rf x')\nPY",
     ["python"]),
    ("ls a 2>&1 | head", ["ls", "head"]),
    ("foo &> log & bar", ["foo", "bar"]),
    ("echo 'a; b | c' && git status", ["echo", "git"]),
    ("if grep -q x f; then echo y; fi", ["grep", "echo"]),
    ("while pytest -x; do sleep 1; done", ["pytest", "sleep"]),
    ("until curl -s h; do sleep 2; done", ["curl", "sleep"]),
    ("FOO=1 \\\n  pytest -q", ["pytest"]),
    ("echo $((1+2)); pytest", ["echo", "pytest"]),
    ("x=$(git rev-parse HEAD); echo $x", ["git", "echo"]),
    ("bash -lc 'cd /w && make test'", ["cd", "make"]),
    ("sh -c \"rg foo | wc -l\"", ["rg", "wc"]),
    ("uv run --with rich python script.py", ["python"]),
    ("sudo -u bob systemctl restart x", ["systemctl"]),
    ("xargs -n1 -I{} rm {}", ["rm"]),
    ("for f in *.py; do black $f; done", ["black"]),
    ("ccx send w 'run python now'", ["ccx"]),
    ("# just a comment\nmake", ["make"]),
])
def test_command_words(command, words):
    assert hist.command_words(command) == words


# --- time ----------------------------------------------------------------------

NOW = time.mktime(time.strptime("2026-11-08 15:30", "%Y-%m-%d %H:%M"))


@pytest.mark.parametrize("value, end, expected", [
    ("2026-11-02", False, "2026-11-02 00:00"),
    ("2026-11-02", True, "2026-11-03 00:00"),
    ("2026-11-02T14:00", True, "2026-11-02 14:00"),
    ("11-02 14:00", False, "2026-11-02 14:00"),
    ("14:00", False, "2026-11-08 14:00"),
])
def test_parse_time_forms(value, end, expected):
    want = time.mktime(time.strptime(expected, "%Y-%m-%d %H:%M"))
    assert hist.parse_time(value, end=end, now=NOW) == want


def test_parse_time_relative():
    assert hist.parse_time("90m", now=NOW) == NOW - 5400
    assert hist.parse_time("2d", now=NOW) == NOW - 2 * 86400


# --- Codex: the local history database ---------------------------------------------

SCHEMA = """
CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL,
  installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, success BOOLEAN NOT NULL,
  checksum BLOB NOT NULL, execution_time BIGINT NOT NULL);
CREATE TABLE thread_turns (thread_id TEXT NOT NULL, turn_id TEXT NOT NULL,
  rollout_ordinal INTEGER NOT NULL, status TEXT NOT NULL, error_json TEXT, started_at INTEGER,
  completed_at INTEGER, duration_ms INTEGER, first_user_item_id TEXT, final_agent_item_id TEXT,
  PRIMARY KEY (thread_id, turn_id));
CREATE TABLE thread_items (thread_id TEXT NOT NULL, turn_id TEXT NOT NULL, item_id TEXT NOT NULL,
  rollout_ordinal INTEGER NOT NULL, created_at_ms INTEGER NOT NULL, item_json TEXT NOT NULL,
  item_type TEXT NOT NULL DEFAULT '', PRIMARY KEY (thread_id, turn_id, item_id));
"""
TID = "th-1"


def make_db(path: Path, *, version=8, schema=SCHEMA, turns=None, items=None) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(schema)
    conn.execute("INSERT INTO _sqlx_migrations VALUES (?, 'x', CURRENT_TIMESTAMP, 1, x'00', 0)",
                 (version,))
    turns = turns if turns is not None else [
        ("t1", 10, "completed", None, 1_793_000_000, 1_793_000_060),
        ("t2", 20, "interrupted", None, 1_793_000_100, 1_793_000_110),   # no items
        ("t3", 30, "inProgress", None, 1_793_000_200, None),
    ]
    items = items if items is not None else [
        ("t1", "i2", 12, {"type": "agentMessage", "id": "i2", "text": "done", "phase": "final_answer"}),
        ("t1", "i1", 11, {"type": "userMessage", "id": "i1", "clientId": None,
                          "content": [{"type": "text", "text": "start"}]}),
        ("t1", "r1", 13, {"type": "reasoning", "id": "r1", "summary": []}),
        ("t1", "c1", 14, {"type": "commandExecution", "id": "c1", "command": "/bin/bash -lc 'make'",
                          "status": "completed", "exitCode": 0, "aggregatedOutput": "ok",
                          "commandActions": [{"type": "unknown", "command": "make"}]}),
        ("t3", "i3", 31, {"type": "userMessage", "id": "i3", "clientId": None,
                          "content": [{"type": "text", "text": "next"}]}),
    ]
    for turn_id, ordinal, status, error, started, completed in turns:
        conn.execute("INSERT INTO thread_turns (thread_id, turn_id, rollout_ordinal, status,"
                     " error_json, started_at, completed_at) VALUES (?,?,?,?,?,?,?)",
                     (TID, turn_id, ordinal, status, error, started, completed))
    for turn_id, item_id, ordinal, item in items:
        conn.execute("INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal,"
                     " created_at_ms, item_json) VALUES (?,?,?,?,0,?)",
                     (TID, turn_id, item_id, ordinal, json.dumps(item)))
    conn.commit()
    conn.close()
    return path


def test_history_db_rebuilds_the_thread_read_shape(tmp_path):
    db = make_db(tmp_path / "h.sqlite")
    before = sorted(p.name for p in tmp_path.iterdir()), db.read_bytes()
    thread = history_store.read_thread_db(TID, path=db)
    assert (sorted(p.name for p in tmp_path.iterdir()), db.read_bytes()) == before  # read-only
    assert [t["id"] for t in thread["turns"]] == ["t1", "t2", "t3"]
    assert [i["id"] for i in thread["turns"][0]["items"]] == ["i1", "i2", "r1", "c1"]
    assert thread["turns"][1]["items"] == []
    assert thread["historySource"] == "codex-history-db" and thread["historyDbVersion"] == 8
    doc = build_codex(thread)
    assert [t["sequence"] for t in doc["turns"]] == [1, 2, 3]
    h = hist.project(doc)
    # Reasoning is numbered by the document (ordinal 2) but never shown.
    assert [(m.turn, m.n, hist.tag(m)) for m in h.messages()] == [
        (1, 0, "U"), (1, 1, "A*"), (1, 3, "T"), (3, 0, "U")]
    assert h.messages()[2].cmd == "make"


@pytest.mark.parametrize("change", ["missing_column", "missing_thread", "bad_status",
                                    "orphan_item", "bad_json", "no_file"])
def test_history_db_declines_on_any_doubt(tmp_path, change):
    path = tmp_path / "h.sqlite"
    if change == "missing_column":
        make_db(path)
        with sqlite3.connect(path) as conn:
            conn.execute("ALTER TABLE thread_items RENAME COLUMN item_json TO body")
    elif change == "bad_status":
        make_db(path, turns=[("t1", 1, "paused", None, 1, 2)], items=[])
    elif change == "orphan_item":
        make_db(path, items=[("zz", "i9", 99, {"type": "userMessage", "id": "i9"})])
    elif change == "bad_json":
        make_db(path)
        with sqlite3.connect(path) as conn:
            conn.execute("UPDATE thread_items SET item_json = '{' WHERE item_id = 'i1'")
    elif change != "no_file":
        make_db(path)
    thread_id = "other-thread" if change == "missing_thread" else TID
    assert history_store.read_thread_db(thread_id, path=path) is None


def test_history_db_path_honours_codex_home(monkeypatch, tmp_path):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    assert history_store.history_db_path() == tmp_path / "thread_history_1.sqlite"
    monkeypatch.setenv("CODEX_HOME", "")
    assert history_store.history_db_path() == Path("~/.codex/thread_history_1.sqlite").expanduser()


def test_running_turn_and_unverified_schema_are_noted(tmp_path):
    thread = history_store.read_thread_db(TID, path=make_db(tmp_path / "h.sqlite", version=9))
    doc = build_codex(thread)
    _note_history_db(doc, thread)
    complete = doc["completeness"]
    assert complete["status"] == "source-limited" and complete["sourceLimited"] is True
    assert any("schema version 9" in w for w in complete["warnings"])
    assert any("still running" in w for w in complete["warnings"])
    assert hist.project(doc).notes                      # shown under the header


def test_codex_steer_mid_turn_is_u_plus():
    doc = build_codex(_codex_turn([
        _user_message("i1", "build it"),
        {"type": "agentMessage", "id": "i2", "text": "on it", "phase": "commentary"},
        _user_message("i3", "and the docs"),
        {"type": "agentMessage", "id": "i4", "text": "both done", "phase": "final_answer"},
    ]))
    assert [hist.tag(m) for m in hist.project(doc).messages()] == ["U", "A", "U+", "A*"]


# --- CLI ---------------------------------------------------------------------------

@pytest.fixture
def cli_worker(short_root, monkeypatch, tmp_path):  # noqa: F811
    add_worker(short_root, A, "claude", terminal=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    doc = claude_doc(tmp_path)
    reads = []

    def conversation(_self, worker, *, max_tool_result_bytes=None, **kwargs):
        reads.append((worker, max_tool_result_bytes, kwargs))
        return doc

    monkeypatch.setattr(WorkerClient, "conversation", conversation)
    return doc, reads


def test_cli_reads_each_worker_once_through_the_history_db_option(cli_worker, capsys):
    _doc, reads = cli_worker
    cli._main(["history", A[:8], "--grep", "pars", "--limit", "1"])
    out = capsys.readouterr().out
    assert out.startswith("claude:966a8a1b") and "newest 1 shown" in out and "next:" in out
    assert reads == [(f"claude:{A}", 4096, {"_codex_history_db": True})]
    cli._main(["history", A[:8], "--turns", "1.0"])
    assert reads[-1][1] is None                     # one message prints whole


def test_cli_usage_errors_exit_before_reading(cli_worker, capsys):
    _doc, reads = cli_worker
    with pytest.raises(SystemExit) as exc:
        cli._main(["history", A[:8], "--since", "yesterday"])
    assert exc.value.code == 1 and reads == []
    assert "ccx history: cannot read time 'yesterday'" in capsys.readouterr().err


def test_cli_cmd_flag_does_not_shadow_the_command_name(cli_worker, capsys):
    cli._main(["history", A[:8], "--cmd", "python"])
    assert "1 hit" in capsys.readouterr().out


# --- Codex through the real original-source path -------------------------------------

from test_worker_telemetry import original_codex, original_sources  # noqa: E402,F401


def test_codex_conversation_reads_the_db_then_falls_back(original_codex, monkeypatch, tmp_path):  # noqa: F811
    from ccx.codex import runtime as codex_runtime

    store, _cwd, wid, tid = original_codex
    home = tmp_path / "codex-home"
    home.mkdir()
    monkeypatch.setenv("CODEX_HOME", str(home))
    served = []

    def read_thread(thread_id, **_kwargs):
        served.append(thread_id)
        return {"id": thread_id, "turns": [{"id": "t9", "status": "completed",
                                            "items": [_user_message("u9", "from app-server")]}]}

    monkeypatch.setattr(codex_runtime, "read_thread", read_thread)
    client = WorkerClient()

    # No database: app-server serves it.
    doc = client.conversation(wid, _codex_history_db=True)
    assert served == [tid] and doc["turns"][0]["turnId"] == "t9"

    # The database names this worker's verified thread: no app-server read.
    db = make_db(home / "thread_history_1.sqlite")
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE thread_turns SET thread_id = ?", (tid,))
        conn.execute("UPDATE thread_items SET thread_id = ?", (tid,))
    doc = client.conversation(wid, _codex_history_db=True)
    assert served == [tid] and [t["turnId"] for t in doc["turns"]] == ["t1", "t2", "t3"]
    assert doc["completeness"]["status"] == "source-limited"       # t3 is still running

    # Without the option (logs, peek) the database is never read.
    doc = client.conversation(wid)
    assert served == [tid, tid]


def test_codex_namespaced_output_is_a_notification():
    doc = build_codex(_codex_turn([
        {"type": "functionCallOutput", "id": "f1", "name": "worker_notification",
         "namespace": "ccx", "output": json.dumps({"worker": "claude:ab", "status": "completed",
                                                    "report": "review done"})},
        {"type": "agentMessage", "id": "i2", "text": "read it", "phase": "final_answer"},
    ]))
    first = hist.project(doc).messages()[0]
    assert hist.tag(first) == "N[ccx]" and first.text == "claude:ab completed: review done"


def test_a_notification_cut_by_the_result_bound_still_reads():
    payload = json.dumps({"kind": "ccx-worker-notification", "phase": "settled",
                          "worker": "claude:58ef24b5", "status": "completed",
                          "report": "ACCEPT: closes M1-M3. " + "x" * 6000})
    doc = build_codex(_codex_turn([
        {"type": "functionCallOutput", "id": "f1", "name": "worker_notification",
         "namespace": "ccx", "output": payload},
        {"type": "functionCallOutput", "id": "f2", "name": "worker_notification",
         "namespace": "ccx", "output": json.dumps({"kind": "heartbeat", "phase": "progress",
                                                    "worker": "codex:ab", "status": "running",
                                                    "activity": "running tests"})},
    ]), max_tool_result_bytes=4096)
    cut_, beat = hist.project(doc).messages()
    assert cut_.text.startswith("claude:58ef24b5 settled completed: ACCEPT: closes M1-M3.")
    assert beat.text == "codex:ab progress running: running tests"


# --- tool output bounds ---------------------------------------------------------------

LONG_OUTPUT = "START of the README\n" + "middle line\n" * 600 + "THE REAL END: 3 passed"


def long_output_events():
    return [_user("u1", "show the readme", 0),
            _assistant("a1", 0.5, _bash("t1", "cat README.md", "Show the README")),
            _result("r1", "t1", LONG_OUTPUT, 1),
            _assistant("a2", 1.5, _text("done"))]


def test_cut_output_says_so_and_full_output_shows_the_real_end(tmp_path):
    events = long_output_events()
    bounded = claude_doc(tmp_path, events)             # read at 4096 bytes
    text = render(bounded, cmd="cat", role="tool,output")
    assert "[output cut at 4 KB of 7 KB; --full or --turns 1.1 for all]" in text
    assert "THE REAL END" not in text
    trace = render(bounded, turns="1", view="trace", role="tool,output")
    assert "[output cut at 4 KB" in trace

    path = write_jsonl(tmp_path / "full.jsonl", events)
    rec = {"short": "966a8a1b", "sessionId": "s", "cwd": "/w", "transcript": str(path),
           "createArgs": {}}
    full = conv.build_claude(rec, state="idle", worker="claude:966a8a1b")
    text = render(full, cmd="cat", role="tool,output")
    assert "⤷" in text and "THE REAL END: 3 passed" in text and "output cut" not in text
    assert "1 hit" in render(full, grep="REAL END", role="output")


@pytest.mark.parametrize("flags, unbounded", [
    (["--grep", "x"], False),
    (["--view", "trace"], False),
    (["--role", "tool,output"], True),
    (["--grep", "x", "--role", "output"], True),
    (["--turns", "1.0"], True),
    (["--turns", "1.0,2-3"], True),
    (["--turns", "2-3"], False),
])
def test_cli_reads_results_unbounded_when_output_matters(cli_worker, capsys, flags, unbounded):
    _doc, reads = cli_worker
    cli._main(["history", A[:8], *flags])
    capsys.readouterr()
    assert reads[-1][1] == (None if unbounded else 4096)
