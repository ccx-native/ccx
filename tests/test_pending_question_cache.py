"""Invalidation correctness for the cached AskUserQuestion transcript scan.

`list` asks this once per row, so the answer is memoized on the raw JSONL's
(mtime_ns, size). These tests pin what makes that safe: a hit skips the scan, an
append rescans, a vanished file falls through to the live (missing-file) answer,
and the returned dict is never the cache's own object.
"""

import json
import os

import pytest

from ccx.claude.transcript import clear_pending_question_cache, pending_ask_user_question

OLD = 1_000_000_000_000_000_000


def _write(path, lines, *, mtime=OLD):
    path.write_text("".join(json.dumps(o) + "\n" for o in lines))
    os.utime(path, ns=(mtime, mtime))


def _ask(tid, questions):
    return {"message": {"content": [
        {"type": "tool_use", "name": "AskUserQuestion", "id": tid,
         "input": {"questions": questions}},
    ]}}


def _answer(tid):
    return {"message": {"content": [{"type": "tool_result", "tool_use_id": tid}]}}


@pytest.fixture(autouse=True)
def _clean():
    clear_pending_question_cache()
    yield
    clear_pending_question_cache()


def test_unchanged_transcript_is_not_rescanned(tmp_path):
    p = tmp_path / "t.jsonl"
    _write(p, [_ask("t1", [{"question": "pick?"}])])
    assert pending_ask_user_question(p)["toolUseId"] == "t1"
    # Replace the bytes while holding (mtime_ns, size) fixed: a rescan would see
    # no question, so the unchanged answer proves the file was not re-read.
    stat = p.stat()
    p.write_text("x" * stat.st_size)
    os.utime(p, ns=(stat.st_mtime_ns, stat.st_mtime_ns))
    assert pending_ask_user_question(p)["toolUseId"] == "t1"


def test_append_invalidates(tmp_path):
    p = tmp_path / "t.jsonl"
    _write(p, [_ask("t1", [{"question": "pick?"}])])
    assert pending_ask_user_question(p) is not None
    _write(p, [_ask("t1", [{"question": "pick?"}]), _answer("t1")], mtime=OLD * 2)
    assert pending_ask_user_question(p) is None


def test_deleted_file_returns_none_and_evicts(tmp_path):
    p = tmp_path / "t.jsonl"
    _write(p, [_ask("t1", [{"question": "pick?"}])])
    assert pending_ask_user_question(p) is not None
    p.unlink()
    assert pending_ask_user_question(p) is None
    # Evicted, not shadowed: a recreated transcript is scanned fresh.
    _write(p, [_ask("t2", [{"question": "again?"}])], mtime=OLD * 3)
    assert pending_ask_user_question(p)["toolUseId"] == "t2"


def test_missing_file_never_raises(tmp_path):
    assert pending_ask_user_question(tmp_path / "nope.jsonl") is None


def test_result_is_detached_from_the_cache(tmp_path):
    p = tmp_path / "t.jsonl"
    _write(p, [_ask("t1", [{"question": "pick?"}])])
    first = pending_ask_user_question(p)
    first["questions"].append({"question": "injected"})
    second = pending_ask_user_question(p)
    assert second is not first
    assert second["questions"] == [{"question": "pick?"}]


def test_freshly_written_transcript_is_not_cached(tmp_path):
    """Inside the mtime-ambiguity window the scan repeats every call."""
    p = tmp_path / "t.jsonl"
    p.write_text(json.dumps(_ask("t1", [{"question": "a"}])) + "\n")
    assert pending_ask_user_question(p)["toolUseId"] == "t1"
    size = p.stat().st_size
    p.write_text(json.dumps(_ask("t2", [{"question": "a"}])) + "\n")
    assert p.stat().st_size == size  # same size, possibly same mtime tick
    assert pending_ask_user_question(p)["toolUseId"] == "t2"


# --- the line-skipping scan answers like a full parse -------------------------

def _reference_scan(path):
    """The pre-optimization algorithm: decode every line via _iter_transcript."""
    from ccx.claude.transcript import _iter_transcript
    asked, answered = {}, set()
    for evt in _iter_transcript(path):
        for b in (evt.get("message") or {}).get("content") or []:
            if not isinstance(b, dict):
                continue
            if b.get("type") == "tool_use" and b.get("name") == "AskUserQuestion":
                asked[b.get("id")] = b.get("input") or {}
            elif b.get("type") == "tool_result" and b.get("tool_use_id") in asked:
                answered.add(b.get("tool_use_id"))
    pend = [(t, i) for t, i in asked.items() if t and t not in answered]
    return {"toolUseId": pend[-1][0], "questions": pend[-1][1].get("questions", [])} if pend else None


def _filler(n):
    return {"type": "assistant", "message": {"content": [{"type": "text", "text": f"line {n}"}]}}


def _bash_result(tid):
    return {"message": {"content": [{"type": "tool_result", "tool_use_id": tid, "content": "ok"}]}}


@pytest.mark.parametrize("lines", [
    [],
    [_filler(1), _filler(2)],
    # mentioned in prose only: kept for decoding, contributes nothing
    [{"message": {"content": [{"type": "text", "text": "use AskUserQuestion"}]}}],
    [_filler(1), _ask("t1", [{"question": "a?"}]), _filler(2)],
    [_ask("t1", [{"question": "a?"}]), _filler(1), _answer("t1"), _filler(2)],
    # answered, then a later unanswered one wins
    [_ask("t1", [{"question": "a?"}]), _answer("t1"), _ask("t2", [{"question": "b?"}])],
    # two dangling: the last asked is reported
    [_ask("t1", [{"question": "a?"}]), _ask("t2", [{"question": "b?"}]), _answer("t2")],
    # a result before its question does not count
    [_answer("t1"), _ask("t1", [{"question": "a?"}])],
    # re-asked after being answered stays answered
    [_ask("t1", [{"question": "a?"}]), _answer("t1"), _ask("t1", [{"question": "again?"}])],
    # an unrelated tool's result never answers the question
    [_ask("t1", [{"question": "a?"}]), _bash_result("t9")],
    # ask and answer on one line
    [{"message": {"content": [
        {"type": "tool_use", "name": "AskUserQuestion", "id": "t1", "input": {"questions": []}},
        {"type": "tool_result", "tool_use_id": "t1"}]}}],
    # subagent rows are not the session's own
    [{**_ask("t1", [{"question": "a?"}]), "isSidechain": True}],
    [_ask("t1", [{"question": "a?"}]), {**_answer("t1"), "isSidechain": True}],
])
def test_scan_matches_the_full_parse(tmp_path, lines):
    p = tmp_path / "t.jsonl"
    _write(p, lines)
    assert pending_ask_user_question(p) == _reference_scan(p)


def test_scan_skips_malformed_and_blank_lines(tmp_path):
    p = tmp_path / "t.jsonl"
    p.write_text(
        '{"AskUserQuestion": broken\n'
        "\n"
        + json.dumps(_ask("t1", [{"question": "a?"}])) + "\r\n"
        + '{"tool_result" "t1" not json}\n'
        + json.dumps(_filler(1))  # no trailing newline
    )
    os.utime(p, ns=(OLD, OLD))
    assert pending_ask_user_question(p) == {"toolUseId": "t1", "questions": [{"question": "a?"}]}
    assert pending_ask_user_question(p) == _reference_scan(p)
