'Unit tests for ccx.claude.host pure helpers.\n\nCovers `_permission_preview`, the function that builds the persisted/broadcast\npreview of a pending permission request. The key invariant: AskUserQuestion\ninput is never truncated, because `ccx answer` reads `input.questions` to map\noptions/text to the right question — truncating it leaves the answer path with\nzero questions and produces misleading errors for long question sets.\n'

from __future__ import annotations
import ccx.claude.host.events as H_events
import ccx.claude.host.hooks as H_hooks
import ccx.claude.host.pty as H_pty
import ccx.claude.host.subagents as H_subagents
import ccx.claude.host.successor as H_successor
from ccx.codex.settle import send_settle_failure
import ccx.ops.wait as ops_wait

import json
from types import SimpleNamespace
import io
import os
import pathlib
import pytest

from ccx.constants import PASTE_SUBMIT_DELAY_S, S as _S
from ccx.claude.host import session as H
from ccx.store import liveness as L
from ccx.system import oslayer
from ccx.cli.commands import _common as C

RELAY_WORKER = "aaaaaaaa-1234-4000-8000-000000000001"
RELAY_SESSION = "bbbbbbbb-1234-4000-8000-000000000002"


def test_native_hook_descendant_uses_kernel_parent_links(monkeypatch):
    starts = {200: "1234", 300: "5678", 400: "9999"}
    parents = {400: 300, 300: 200}
    monkeypatch.setattr(L, "proc_start_token", lambda pid: starts.get(pid))
    monkeypatch.setattr(H.oslayer, "process_info", lambda pid: oslayer.ProcessInfo(
        pid, "alive", parents.get(pid), "S", starts.get(pid)))
    assert H_subagents._native_hook_descendant(400, 200, "1234")
    assert not H_subagents._native_hook_descendant(400, 200, "different")
    for parent in (400, None):  # cycle and unreadable parent both refuse
        parents[300] = parent
        assert not H_subagents._native_hook_descendant(400, 200, "1234")


def _auq_req(num_questions=3, options_per=4, pad=400):
    """A realistic AskUserQuestion request whose JSON exceeds the 2000-char
    truncation threshold (long descriptions/previews)."""
    questions = []
    for qi in range(num_questions):
        opts = [
            {"label": f"opt{qi}-{oi}", "description": "x" * pad}
            for oi in range(options_per)
        ]
        questions.append({
            "question": f"Question number {qi} with a fairly long body? " + "y" * pad,
            "header": f"Q{qi}",
            "multiSelect": False,
            "options": opts,
        })
    return {
        "id": "perm-1",
        "tool_name": "AskUserQuestion",
        "tool_input": {"questions": questions},
        "requested_at": 1_000_000.0,
        "message": "answer please",
    }


def test_permission_preview_none():
    assert H_events._permission_preview(None) is None


def test_permission_preview_small_input_passes_through():
    req = {"id": "p", "tool_name": "Write", "tool_input": {"file_path": "/a", "content": "hi"}}
    out = H_events._permission_preview(req)
    assert out["input"] == {"file_path": "/a", "content": "hi"}
    assert out["tool"] == "Write"


def test_permission_preview_large_non_auq_truncated():
    big = {"file_path": "/a", "content": "z" * 5000}
    req = {"id": "p", "tool_name": "Write", "tool_input": big}
    out = H_events._permission_preview(req)
    assert out["input"].get("truncated") is True
    assert "preview" in out["input"]
    assert "questions" not in out["input"]


def test_permission_preview_auq_never_truncated():
    req = _auq_req()
    # Sanity: the raw input really is over the truncation threshold.
    assert len(json.dumps(req["tool_input"])) > 2000
    out = H_events._permission_preview(req)
    assert out["tool"] == "AskUserQuestion"
    assert "truncated" not in out["input"]
    assert out["input"]["questions"] == req["tool_input"]["questions"]


def test_native_permission_reconciliation_matches_full_original_input():
    req = {"id": "first", "kind": "permission", "tool_name": "Bash",
           "tool_input": {"command": "echo hi", "extra": {"x": "a"}}}
    good = {"tool_name": "Bash", "tool_input": req["tool_input"],
            "tool_use_id": "different-native-id", "tool_response": {"result": "yes"}}
    assert H_events._externally_completed_question(req, good)
    assert not H_events._externally_completed_question(req, {
        **good, "tool_input": {"command": "echo hi", "extra": {"x": "b"}}})
    assert not H_events._externally_completed_question(req, {
        **good, "tool_name": "Write"})
    assert not H_events._externally_completed_question(req, {
        **good, "tool_input": {"command": "echo hi"}})
    assert not H_events._externally_completed_question(req, {
        "tool_name": "Bash", "tool_use_id": "same-id", "tool_input": None})
    assert not H_events._externally_completed_question({**req, "kind": "elicitation"}, good)
    # The published gate is first even when its tool-use id differs. If it
    # does not match, the oldest exactly identical queued gate wins.
    second = {**req, "id": "second"}
    third = {**req, "id": "third"}
    assert H_events._oldest_native_gate(req, [second, third], good) is req
    assert H_events._oldest_native_gate(None, [second, third], good) is second
    different = {**req, "tool_input": {"command": "no"}}
    assert H_events._oldest_native_gate(different, [second, third], good) is second
    assert H_events._oldest_native_gate(different, [], good) is None


def test_auq_preview_feeds_answer_map_end_to_end():
    """The untruncated questions surfaced by `_permission_preview` must let the
    `ccx answer` mapping resolve structured options for a long AUQ — the M2 bug
    was that truncation left zero questions here and every answer errored."""
    req = _auq_req(num_questions=2, options_per=3)
    questions = H_events._permission_preview(req)["input"]["questions"]
    q_payload = {"source": "managed-hook", "questions": questions}

    args = SimpleNamespace(
        option=[("1", "opt0-1"), ("2", "opt1-2")],
        text_answer=None,
        text=None,
    )
    answers, err = C._question_answer_map(q_payload, args)
    assert err is None
    assert answers == {
        questions[0]["question"]: "opt0-1",
        questions[1]["question"]: "opt1-2",
    }


def test_truncated_payload_would_break_answer_map():
    """Characterizes the pre-fix failure mode: a truncated preview carries no
    `questions`, so structured answers go out of range with a misleading error."""
    q_payload = {"source": "managed-hook", "questions": []}
    args = SimpleNamespace(option=[("1", "whatever")], text_answer=None, text=None)
    answers, err = C._question_answer_map(q_payload, args)
    assert answers == {}
    assert err is not None and "out of range" in err


def test_read_new_api_error_scans_only_appended_lines(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": "normal"}]},
    }) + "\n")
    _, pos = H_hooks._read_new_api_error(p, 0)

    with p.open("a") as f:
        f.write(json.dumps({
            "type": "assistant",
            "isApiErrorMessage": True,
            "apiErrorStatus": 500,
            "error": "server_error",
            "requestId": "req_123",
            "message": {"content": [{"type": "text", "text": "API Error: 500"}]},
        }) + "\n")

    api_error, new_pos = H_hooks._read_new_api_error(p, pos)

    assert api_error["status"] == 500
    assert api_error["error"] == "server_error"
    assert api_error["request_id"] == "req_123"
    assert api_error["text"] == "API Error: 500"
    assert new_pos == p.stat().st_size


def test_read_new_api_error_leaves_partial_line_for_next_poll(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text('{"type":"assistant"')

    api_error, pos = H_hooks._read_new_api_error(p, 0)

    assert api_error is None
    assert pos == 0


def test_read_new_api_error_eof_position_skips_historical_error(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "assistant",
        "isApiErrorMessage": True,
        "apiErrorStatus": 500,
        "error": "server_error",
        "message": {"content": [{"type": "text", "text": "old error"}]},
    }) + "\n")

    api_error, pos = H_hooks._read_new_api_error(p, p.stat().st_size)

    assert api_error is None
    assert pos == p.stat().st_size


def test_read_new_api_error_restarts_after_truncation(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "assistant",
        "isApiErrorMessage": True,
        "apiErrorStatus": 503,
        "error": "overloaded_error",
        "message": {"content": [{"type": "text", "text": "new error"}]},
    }) + "\n")

    api_error, pos = H_hooks._read_new_api_error(p, 10_000)

    assert api_error["status"] == 503
    assert api_error["error"] == "overloaded_error"
    assert pos == p.stat().st_size


# --- worker_env: strip inherited child-session identity --------------------
# Regression: when ccx is driven from inside a Claude Code session, the worker
# inherited CLAUDE_CODE_CHILD_SESSION=1, which makes Claude treat the worker as
# a nested child and SKIP transcript persistence (messages buffered in memory,
# ~/.claude/projects/<cwd>/<id>.jsonl never written) — blinding report/peek/logs.

def test_worker_env_strips_inherited_child_session_identity():
    base = {
        "PATH": "/usr/bin",
        "HOME": "/home/user",
        "CLAUDE_CODE_CHILD_SESSION": "1",
        "CLAUDE_CODE_SESSION_ID": "parent-uuid",
        "CLAUDE_CODE_ENTRYPOINT": "cli",
    }
    env = H.worker_env(base)
    assert "CLAUDE_CODE_CHILD_SESSION" not in env
    assert "CLAUDE_CODE_SESSION_ID" not in env
    assert "CLAUDE_CODE_ENTRYPOINT" not in env
    # the documented identity set is exactly what gets stripped
    assert set(H.INHERITED_CLAUDE_IDENTITY_VARS) == {
        "CLAUDE_CODE_CHILD_SESSION",
        "CLAUDE_CODE_SESSION_ID",
        "CLAUDE_CODE_ENTRYPOINT",
    }


def test_worker_env_forces_persistence():
    env = H.worker_env({"CLAUDE_CODE_CHILD_SESSION": "1"})
    assert env["CLAUDE_CODE_FORCE_SESSION_PERSISTENCE"] == "1"


def test_worker_env_preserves_unrelated_vars():
    base = {"PATH": "/usr/bin:/bin", "HOME": "/home/user", "FOO": "bar"}
    env = H.worker_env(base)
    assert env["PATH"] == "/usr/bin:/bin"
    assert env["HOME"] == "/home/user"
    assert env["FOO"] == "bar"


def test_worker_env_tolerates_absent_vars():
    # No KeyError when the identity vars were never set (clean launch env).
    env = H.worker_env({"PATH": "/usr/bin"})
    assert env["PATH"] == "/usr/bin"
    assert env["CLAUDE_CODE_FORCE_SESSION_PERSISTENCE"] == "1"


def test_worker_env_does_not_mutate_input():
    base = {"CLAUDE_CODE_CHILD_SESSION": "1", "PATH": "/usr/bin"}
    H.worker_env(base)
    assert base == {"CLAUDE_CODE_CHILD_SESSION": "1", "PATH": "/usr/bin"}


def test_worker_env_defaults_to_os_environ(monkeypatch):
    monkeypatch.setenv("CLAUDE_CODE_CHILD_SESSION", "1")
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "parent-uuid")
    monkeypatch.setenv("CCX_WORKER_ENV_PROBE", "keep-me")
    env = H.worker_env()
    assert "CLAUDE_CODE_CHILD_SESSION" not in env
    assert "CLAUDE_CODE_SESSION_ID" not in env
    assert env["CCX_WORKER_ENV_PROBE"] == "keep-me"
    assert env["CLAUDE_CODE_FORCE_SESSION_PERSISTENCE"] == "1"
    # the helper copies; it must not mutate the live process environment
    import os as _os
    assert _os.environ.get("CLAUDE_CODE_CHILD_SESSION") == "1"


def test_should_fail_boot_stalled_boot_fails():
    state = {"current": _S.BOOTING, "since": 1000.0}
    # 120s elapsed > 90s timeout → boot-stall reason.
    reason = H.should_fail_boot(state, now=1120.0, timeout=90.0)
    assert reason is not None
    assert "boot stalled" in reason
    assert "SessionStart" in reason
    assert "90s" in reason


def test_should_fail_boot_fresh_boot_is_noop():
    state = {"current": _S.BOOTING, "since": 1000.0}
    # Only 5s elapsed → still booting, no failure.
    assert H.should_fail_boot(state, now=1005.0, timeout=90.0) is None


def test_should_fail_boot_non_booting_state_is_noop():
    # An old IDLE/BUSY session must never be reaped by the boot watchdog.
    for st in (_S.IDLE, _S.BUSY, _S.WAITING, _S.CRASHED):
        state = {"current": st, "since": 1000.0}
        assert H.should_fail_boot(state, now=99999.0, timeout=90.0) is None


def test_should_fail_boot_disabled_when_timeout_nonpositive():
    state = {"current": _S.BOOTING, "since": 1000.0}
    assert H.should_fail_boot(state, now=99999.0, timeout=0.0) is None


# --- steering-paste byte builders (anti-doubling clear) ---------------------

def test_paste_bytes_wraps_without_submitting():
    out = H_pty.paste_bytes("hello world")
    assert out == b"\x1b[200~hello world\x1b[201~"
    assert out.startswith(b"\x1b[200~")
    assert out.endswith(b"\x1b[201~")
    assert not out.endswith(b"\r")


def test_paste_bytes_handles_multiline_and_unicode():
    out = H_pty.paste_bytes("line1\nline2 — é")
    assert out == b"\x1b[200~" + "line1\nline2 — é".encode() + b"\x1b[201~"


def test_paste_submit_delay_keeps_small_prompts_fast():
    assert H_pty.paste_submit_delay("a" * 8192) == PASTE_SUBMIT_DELAY_S


def test_paste_submit_delay_stages_large_prompts_by_utf8_size():
    assert H_pty.paste_submit_delay("a" * 32_000) == 32_000 / (2 * 1024)
    assert H_pty.paste_submit_delay("é" * 16_000) == 32_000 / (2 * 1024)


def test_paste_submit_delay_is_bounded():
    assert H_pty.paste_submit_delay("a" * 1_000_000) == 30.0


def test_inline_paste_newline_cap_follows_pty_rows():
    assert H_pty.inline_paste_newline_cap(40) == 2
    assert H_pty.inline_paste_newline_cap(12) == 2
    assert H_pty.inline_paste_newline_cap(11) == 1
    assert H_pty.inline_paste_newline_cap(10) == 0
    assert H_pty.inline_paste_newline_cap(3) == 0


def test_paste_chunks_reassemble_exactly_under_both_limits():
    text = "\n".join(
        ["intro", "", "\tindented", "x" * 1500, "é😀" * 300, "/compact", ""]
        + [f"line {i}" for i in range(50)]
    )
    chunks = H_pty.paste_chunks(text, 2, max_units=100)
    assert "".join(chunks) == text
    for chunk in chunks:
        normalized = chunk.replace("\t", "    ")
        assert len(normalized.encode("utf-16-le")) // 2 <= 100
        assert chunk.count("\n") <= 2


def test_large_unicode_prompt_preserves_astral_pair_across_utf16_unit_500():
    # Claude 2.1.311's native editor shows two invisible characters when a
    # long prompt crosses 10k UTF-16 units with an astral pair at 499-500.
    # Prove the source and CCX paste frames stay well formed and identical;
    # the mutation belongs to native editor processing, not our byte writer.
    text = "a" * 499 + "🙂" + "\n" + "épsilon ζeta\n" + "b" * 10500
    assert len(text.encode("utf-16-le")) // 2 > 10_000
    chunks = H_pty.paste_chunks(text, 2)
    assert "".join(chunks) == text
    payload, _delay, count = H_pty.prompt_paste(text, rows=40)
    assert count == len(chunks)
    assert payload == b"".join(H_pty.paste_bytes(chunk) for chunk in chunks)
    assert payload.replace(b"\x1b[200~", b"").replace(b"\x1b[201~", b"") == text.encode("utf8")
    assert b"\xef\xbf\xbd" not in payload


def test_paste_chunks_never_split_crlf_and_count_it_once():
    text = "a\r\nb\r\nc\rd\ne"
    chunks = H_pty.paste_chunks(text, 1, max_units=640)
    assert "".join(chunks) == text
    assert chunks == ["a\r\n", "b\r\n", "c\r", "d\n", "e"]


def test_paste_chunks_keep_short_single_line_whole():
    assert H_pty.paste_chunks("hello world", 2) == ["hello world"]


def test_paste_chunks_refuse_line_breaks_when_no_paste_may_carry_one():
    assert H_pty.paste_chunks("one line", 0) == ["one line"]
    assert H_pty.paste_chunks("two\nlines", 0) is None


def test_prompt_paste_frames_chunks_and_submits_immediately():
    text = "\n".join(f"line {i}" for i in range(9))
    payload, delay, pastes = H_pty.prompt_paste(text, rows=40)
    assert pastes == 5
    assert payload.count(b"\x1b[200~") == pastes
    assert payload.count(b"\x1b[201~") == pastes
    assert payload.replace(b"\x1b[200~", b"").replace(b"\x1b[201~", b"") == text.encode()
    assert delay == PASTE_SUBMIT_DELAY_S


def test_prompt_paste_falls_back_to_one_staged_paste_on_short_pty():
    text = "a\n" * 20_000
    payload, delay, pastes = H_pty.prompt_paste(text, rows=10)
    assert (payload, pastes) == (H_pty.paste_bytes(text), 1)
    assert delay == H_pty.paste_submit_delay(text)


def test_prompt_paste_keeps_empty_text_a_single_paste():
    assert H_pty.prompt_paste("", rows=40) == (H_pty.paste_bytes(""), H.PASTE_SUBMIT_DELAY_S, 1)


def test_write_all_drains_short_writes(monkeypatch):
    received = bytearray()
    partials = []

    def short_write(_fd, payload):
        chunk = bytes(payload[:3])
        received.extend(chunk)
        return len(chunk)

    monkeypatch.setattr(H.os, "write", short_write)
    assert H_pty.write_all(
        123,
        b"abcdefgh",
        on_partial=lambda written, sent, total: partials.append(
            (written, sent, total)
        ),
    ) is True
    assert received == b"abcdefgh"
    assert partials == [(3, 3, 8), (3, 6, 8)]


def test_write_all_waits_for_writable_after_eagain(monkeypatch):
    attempts = 0

    def eagain_once(_fd, payload):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise BlockingIOError
        return len(payload)

    monkeypatch.setattr(H.os, "write", eagain_once)
    monkeypatch.setattr(H.select, "select", lambda *args: ([], [123], []))
    assert H_pty.write_all(123, b"payload") is True
    assert attempts == 2


def test_write_all_fails_when_writable_wait_expires(monkeypatch):
    def always_eagain(_fd, _payload):
        raise BlockingIOError

    monkeypatch.setattr(H.os, "write", always_eagain)
    monkeypatch.setattr(H.select, "select", lambda *args: ([], [], []))
    assert H_pty.write_all(123, b"payload") is False


def test_write_all_failed_single_escape_has_zero_bytes():
    # A failed one-byte Escape can never be a partially delivered interrupt.
    read_fd, write_fd = os.pipe()
    try:
        os.close(write_fd)
        assert H_pty.write_all(write_fd, b"\x1b") is False
        assert os.read(read_fd, 1) == b""
    finally:
        os.close(read_fd)


def test_unstick_and_clear_input_bytes():
    # The host writes these two on SEPARATE pump ticks (the inter-tick gap lets
    # the unstick's async editor update apply before the clear races it).
    assert H_pty.unstick_bytes() == b"\x1b[201~"
    assert H_pty.clear_input_bytes() == b"\x03"


def test_drain_bytes_is_a_ctrl_u_burst():
    from ccx.constants import CLEAR_KILL_REPEAT
    assert H_pty.drain_bytes() == b"\x15" * CLEAR_KILL_REPEAT
    assert CLEAR_KILL_REPEAT > 1


def test_drain_bytes_kill_repeat_override():
    assert H_pty.drain_bytes(kill_repeat=5) == b"\x15" * 5


# --- pty_tail_reason -------------------------------------------------------
# A worker that dies at boot prints why on the PTY and exits. Nothing else
# records that (the host log holds only launcher stdio), so without this the
# record kept an opaque state=crashed with lastError=None — the failure mode
# that made a `--agent <model>` mixup take a full investigation to diagnose.

def test_pty_tail_reason_recovers_boot_error():
    buf = bytearray(
        b"\x1b[?25l\x1b[2J"
        b"--agent 'fable' not found. Available agents: claude, Explore, Plan\r\n"
    )
    assert H_pty.pty_tail_reason(buf) == (
        "--agent 'fable' not found. Available agents: claude, Explore, Plan"
    )


def test_pty_tail_reason_drops_tui_residue_and_dedupes_frames():
    # Ink re-renders the same frame; box-drawing carries no message.
    buf = b"\x1b[32m\xe2\x95\xad\xe2\x94\x80\xe2\x95\xae\x1b[0m\r\nboom\r\nboom\r\n"
    assert H_pty.pty_tail_reason(buf) == "boom"


def test_pty_tail_reason_none_when_nothing_quotable():
    assert H_pty.pty_tail_reason(b"") is None
    assert H_pty.pty_tail_reason(b"\x1b[2J\x1b[H") is None
    assert H_pty.pty_tail_reason(b"\xe2\x94\x80\xe2\x94\x80\r\n") is None


def test_pty_tail_reason_is_bounded():
    reason = H_pty.pty_tail_reason(b"x" * 5000, max_chars=80)
    assert reason is not None and len(reason) == 80


# --- _read_interrupt_marker -----------------------------------------------------


def test_read_interrupt_marker_finds_tool_use_variant(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": "working"}]},
    }) + "\n")
    found, pos = H_hooks._read_interrupt_marker(p, 0)
    assert not found

    with p.open("a") as f:
        f.write(json.dumps({
            "type": "user",
            "message": {"content": [{
                "type": "tool_result",
                "content": "[Request interrupted by user for tool use]",
                "is_error": True,
                "tool_use_id": "toolu_x",
            }]},
        }) + "\n")

    found, new_pos = H_hooks._read_interrupt_marker(p, pos)
    assert found
    assert new_pos == p.stat().st_size


def test_read_interrupt_marker_plain_variant(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "user", "promptId": "current", "interruptedMessageId": "msg-1",
        "message": {"content": "[Request interrupted by user]"},
    }) + "\n")
    found, _ = H_hooks._read_interrupt_marker(p, 0, prompt_ids={"current"})
    assert found
    found, _ = H_hooks._read_interrupt_marker(p, 0, prompt_ids={"different"})
    assert not found


def test_read_interrupt_marker_ignores_non_user_mentions(tmp_path):
    # An assistant merely *quoting* the marker text must not confirm.
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "assistant",
        "message": {"content": [{"type": "text",
                                 "text": "the marker is [Request interrupted by user]"}]},
    }) + "\n")
    found, pos = H_hooks._read_interrupt_marker(p, 0)
    assert not found
    assert pos == p.stat().st_size


def test_read_interrupt_marker_eof_position_skips_history(tmp_path):
    # Arming the probe at current EOF must not match an OLD interrupt marker.
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "user",
        "message": {"content": "[Request interrupted by user]"},
    }) + "\n")
    found, _ = H_hooks._read_interrupt_marker(p, p.stat().st_size)
    assert not found


def test_read_interrupt_marker_leaves_partial_line(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text('{"type":"user","message":{"content":"[Request interrupted by user')
    found, pos = H_hooks._read_interrupt_marker(p, 0)
    assert not found
    assert pos == 0


# --- compact displacement -------------------------------------------------
#
# Regression cover for the stranded-compact incident: `/compact` is pasted
# without its Enter (Ink's autocomplete eats a same-write \r), and in that
# window Claude flushed a prompt queued by an earlier steer. The queued prompt
# ran as an ordinary turn, the command text was left in the editor, and the
# operation could only die at its 20-minute deadline — after which the host
# marked a perfectly healthy worker CRASHED.


def _compacting(*, submitted=False, pasted=True):
    return {"compacting": {"operation_id": "op-1",
                           "submitted": submitted,
                           "pasted_at": 1.0 if pasted else None}}


def test_compact_displaced_by_foreign_prompt():
    # The incident itself: a real prompt lands while our paste sits unsubmitted.
    assert H_successor.compact_displaced_by_prompt(_compacting(), "# Verifier addendum")


def test_compact_not_displaced_by_own_command():
    # Claude records a submitted /compact as an ordinary user event, so its own
    # UserPromptSubmit must never be read as displacement.
    assert not H_successor.compact_displaced_by_prompt(_compacting(), "/compact")
    assert not H_successor.compact_displaced_by_prompt(_compacting(), "  /compact  ")


def test_compact_not_displaced_once_submitted():
    # After acknowledgement the native command owns the surface; later hook
    # traffic belongs to compaction.
    assert not H_successor.compact_displaced_by_prompt(
        _compacting(submitted=True), "anything")


def test_compact_not_displaced_before_paste():
    # Nothing is in the editor yet, so nothing can have been displaced.
    assert not H_successor.compact_displaced_by_prompt(
        _compacting(pasted=False), "anything")


def test_compact_not_displaced_when_not_compacting():
    assert not H_successor.compact_displaced_by_prompt({"compacting": None}, "anything")


# --- compact_settle_fence -----------------------------------------------------
#
# Which transcript offset separates "compaction's own bookkeeping" from "a new
# turn started while we were settling". Pairs with the hardened
# `foreign_prompt_since`: see tests/test_transcript.py.

def test_compact_settle_fence_prefers_the_boundary_end_offset():
    op = {"fence_offset": 2127105, "result": {"end_offset": 2133962}}
    assert H_successor.compact_settle_fence(op) == 2133962


def test_compact_settle_fence_falls_back_to_the_paste_fence():
    # A no-op verdict has no boundary record, so no end offset.
    op = {"fence_offset": 2127105, "result": {"compacted": False}}
    assert H_successor.compact_settle_fence(op) == 2127105


def test_compact_settle_fence_tolerates_a_missing_or_junk_operation():
    assert H_successor.compact_settle_fence({}) == 0
    assert H_successor.compact_settle_fence({"result": None, "fence_offset": None}) == 0
    assert H_successor.compact_settle_fence({"fence_offset": "nope"}) == 0
    assert H_successor.compact_settle_fence({"result": {"end_offset": -1},
                                   "fence_offset": 17}) == 17


def test_successor_hold_releases_only_when_unattributed_native_inactive():
    pending = {"native_busy_status_updated_at": 10}
    assert H_successor.successor_hold_should_release(pending, None, "idle") is True
    assert H_successor.successor_hold_should_release(pending, None, "shell") is True
    assert H_successor.successor_hold_should_release(pending, None, "busy") is False
    assert H_successor.successor_hold_should_release(pending, "ccx-turn:claude:v1:next", "idle") is False
    assert H_successor.successor_hold_should_release(None, None, "idle") is False


def test_cancel_clear_blocks_write_pump_but_is_not_send_admission_error():
    state = {
        "semantic_delivery": None,
        "current": H.S.IDLE,
        "compact_clear": None,
        "cancel_clear": {"unstuck_at": None},
        "compacting": None,
    }
    assert H_successor.input_surface_write_blocked(state) is True
    state["cancel_clear"] = None
    assert H_successor.input_surface_write_blocked(state) is False
    state["compact_clear"] = {"unstuck_at": None}
    assert H_successor.input_surface_write_blocked(state) is True


def _subagent_hook(event="PreToolUse", agent_id="ac534a155bdcea0a1",
                   agent_type="general-purpose", **extra):
    data = {
        "session_id": "c29f6075-0f8d-419d-b436-80f0631ad783",
        "transcript_path": "/tmp/parent.jsonl",
        "hook_event_name": event,
        "agent_id": agent_id,
    }
    if agent_type is not None:
        data["agent_type"] = agent_type
    data.update(extra)
    return data


def _native(status, updated=100):
    from ccx.store.liveness import NativeClaudeState
    return NativeClaudeState(pid=1, session_id="s", kind="interactive",
                             status=status, status_updated_at=updated,
                             waiting_for=None, proc_start=None)


def test_hook_subagent_id_detects_subagent_payloads():
    assert H_subagents._hook_subagent_id(_subagent_hook()) == "ac534a155bdcea0a1"
    # The untyped internal agent still has an agent_id.
    assert H_subagents._hook_subagent_id(_subagent_hook(agent_id="a197aefc227ac2e96",
                                              agent_type=None)) == "a197aefc227ac2e96"
    assert H_subagents._hook_subagent_id({"subagentId": "x1"}) == "x1"


def test_hook_subagent_id_treats_parent_payloads_as_parent():
    assert H_subagents._hook_subagent_id({"hook_event_name": "PreToolUse"}) is None
    # `claude --agent X` parent sessions carry agent_type only.
    assert H_subagents._hook_subagent_id({"agent_type": "reviewer"}) is None
    assert H_subagents._hook_subagent_id({"agent_id": ""}) is None
    assert H_subagents._hook_subagent_id({"agent_id": 7}) is None
    assert H_subagents._hook_subagent_id(None) is None


def test_subagent_tool_events_are_distinct_and_turnless():
    start = H_subagents._subagent_tool_started_event(_subagent_hook(
        tool_name="Bash", tool_use_id="toolu_child",
        tool_input={"command": "echo hi"}))
    assert start == {
        "type": "subagent_tool_started",
        "agent_id": "ac534a155bdcea0a1",
        "agent_type": "general-purpose",
        "tool_use_id": "toolu_child",
        "tool": "Bash",
        "input_preview": '{"command": "echo hi"}',
    }
    done = H_subagents._subagent_tool_finished_event(_subagent_hook(
        event="PostToolUse", tool_name="Bash", tool_use_id="toolu_child",
        tool_response={"stdout": "hi"}), 12)
    assert done["type"] == "subagent_tool_finished"
    assert done["duration_ms"] == 12 and done["ok"] is True
    assert "turn" not in start and "turn" not in done


def test_subagent_boundary_reply_never_delivers_for_subagents():
    for event in ("PostToolUse", "SubagentStop"):
        assert H_subagents.subagent_boundary_reply(_subagent_hook(event=event)) == {
            "ok": True, "delivered": False, "reason": "subagent hook"}
    assert H_subagents.subagent_boundary_reply({"hook_event_name": "PostToolUse"}) is None


def test_notification_waiting_only_for_parent_permission_prompt():
    assert H_subagents.notification_sets_waiting({"notification_type": "permission_prompt"})
    assert not H_subagents.notification_sets_waiting(
        {"notification_type": "worker_permission_prompt"})
    assert not H_subagents.notification_sets_waiting({"notification_type": "idle_prompt"})
    assert not H_subagents.notification_sets_waiting(None)


def test_subagent_taint_begins_on_any_subagent_hook_including_stop():
    taint = {}
    assert H_subagents.note_subagent_taint(taint, "SubagentStart",
                                 _subagent_hook(event="SubagentStart"), 1000) is True
    # Already tainted: not a new beginning, but the evidence is refreshed.
    assert H_subagents.note_subagent_taint(taint, "PreToolUse", _subagent_hook(), 2000) is False
    assert taint == {"ac534a155bdcea0a1": 2000}
    # SubagentStop keeps the taint: presence lags the stop.
    H_subagents.note_subagent_taint(taint, "SubagentStop", _subagent_hook(event="SubagentStop"), 3000)
    assert taint == {"ac534a155bdcea0a1": 3000}


def test_subagent_taint_from_parent_stop_snapshot_only_adds():
    taint = {"a1": 1000}
    began = H_subagents.note_subagent_taint(taint, "Stop", {
        "hook_event_name": "Stop",
        "background_tasks": [
            {"id": "a2", "type": "subagent", "status": "running"},
            {"id": "a3", "type": "subagent", "status": "completed"},
            {"id": "b1", "type": "local_bash", "status": "running"},
        ],
    }, 5000)
    assert began is False
    # A (possibly delayed) parent Stop never removes newer evidence.
    assert taint == {"a1": 1000, "a2": 5000}
    assert H_subagents.note_subagent_taint({}, "PreToolUse", {"hook_event_name": "PreToolUse"}, 1) is False
    assert H_subagents.note_subagent_taint({}, "Stop", {"hook_event_name": "Stop"}, 1) is False


def test_subagent_taint_clears_only_on_fresh_quiescence():
    taint = {"a1": 1000, "a2": 3000}
    assert H_subagents.clear_subagent_taint(taint, _native("busy", 9000)) is False
    # A quiescent sample older than the newest evidence predates that agent.
    assert H_subagents.clear_subagent_taint(taint, _native("idle", 2500)) is False
    assert H_subagents.clear_subagent_taint(taint, _native("idle", None)) is False
    assert H_subagents.clear_subagent_taint(taint, None) is False
    assert taint
    # A same-millisecond status cannot be ordered against the evidence.
    assert H_subagents.clear_subagent_taint(taint, _native("shell", 3000)) is False
    assert H_subagents.clear_subagent_taint(taint, _native("shell", 3001)) is True
    assert taint == {}
    assert H_subagents.clear_subagent_taint(taint, _native("idle", 9999)) is False


def test_reordered_hook_delivery_never_lowers_a_taint_fence():
    """A later-fired hook delivered first, then an earlier-fired one: the
    fence stays at the newest evidence, so a status between them cannot clear."""
    taint = {}
    newer = dict(_subagent_hook(), _ccx_relay_ms=5000)
    older = dict(_subagent_hook(), _ccx_relay_ms=3000)
    H_subagents.note_subagent_taint(taint, "PreToolUse", newer, H_subagents.hook_fired_ms(newer, 5100))
    H_subagents.note_subagent_taint(taint, "PostToolUse", older, H_subagents.hook_fired_ms(older, 5200))
    assert taint == {"ac534a155bdcea0a1": 5000}
    assert H_subagents.clear_subagent_taint(taint, _native("idle", 4000)) is False
    # Same for a running id from a delayed parent Stop snapshot.
    late_stop = {"hook_event_name": "Stop", "background_tasks": [
        {"id": "ac534a155bdcea0a1", "type": "subagent", "status": "running"}]}
    H_subagents.note_subagent_taint(taint, "Stop", late_stop, 2000)
    assert taint == {"ac534a155bdcea0a1": 5000}
    assert H_subagents.clear_subagent_taint(taint, _native("idle", 5001)) is True


def test_interrupt_observation_survives_subagent_taint_until_post_escape_idle():
    obs = {"turn_ref": "ref-1", "interrupt_at_ms": 3000, "settling": False}
    state = {"interrupt_observation": obs, "active_turn_ref": "ref-1",
             "current": H.S.BUSY, "subagent_taint": {"agent": 9000}}
    assert H_successor.interrupt_native_idle_after_escape(state, obs, _native("busy", 9999)) is False
    assert H_successor.interrupt_native_idle_after_escape(state, obs, _native("idle", 3000)) is False
    # A newer IDLE is unambiguous despite the un-cleared newer agent taint.
    assert H_successor.interrupt_native_idle_after_escape(state, obs, _native("idle", 3001)) is True
    assert state["interrupt_observation"] is obs


def test_interrupt_observation_refuses_successor_and_preserves_hold():
    obs = {"turn_ref": "ref-1", "interrupt_at_ms": 3000, "settling": False}
    guard = {"turn_ref": "ref-0", "expires_at": 1e12}
    hold = {"since": 0.0}
    state = {"interrupt_observation": obs, "late_hook_guard": guard,
             "successor_pending": hold, "current": H.S.BUSY,
             "active_turn_ref": "ref-2", "subagent_taint": {"agent": 5000}}
    assert H_successor.interrupt_native_idle_after_escape(state, obs, _native("shell", 6000)) is False
    assert state["interrupt_observation"] is obs
    assert state["late_hook_guard"] is guard and state["successor_pending"] is hold


def test_guard_stays_conservative_while_tainted_without_prompt_ids():
    # A successor owns a ref: an old Stop is suppressed, its tools are not.
    assert H_successor.guarded_hook_is_late_while_tainted("Stop", "ref-2", None, None) is True
    assert H_successor.guarded_hook_is_late_while_tainted("PreToolUse", "ref-2", None, []) is False
    # No successor ref: the settled target's delayed hooks stay suppressed.
    assert H_successor.guarded_hook_is_late_while_tainted("Stop", None, "p2", []) is True
    assert H_successor.guarded_hook_is_late_while_tainted("PreToolUse", None, None, ["p1"]) is True


def test_guard_correlates_by_prompt_id_while_tainted():
    settled = ["p1", "p1-steer"]
    # A fast successor's genuine Stop is never swallowed...
    assert H_successor.guarded_hook_is_late_while_tainted("Stop", "ref-2", "p2", settled) is False
    assert H_successor.guarded_hook_is_late_while_tainted("Stop", None, "p2", settled) is False
    # ...while any prompt the settled turn carried stays late.
    assert H_successor.guarded_hook_is_late_while_tainted("Stop", "ref-2", "p1-steer", settled) is True
    assert H_successor.guarded_hook_is_late_while_tainted("PreToolUse", "ref-2", "p1", settled) is True
    assert H_successor.hook_prompt_id({"prompt_id": "p1"}) == "p1"
    assert H_successor.hook_prompt_id({"prompt_id": ""}) is None
    assert H_successor.hook_prompt_id({"prompt_id": 7}) is None


def test_successor_attribution_is_the_baseline_busy_edge_rule():
    busy = _native("busy", 6000)
    assert H_successor.successor_attribution(5000, busy) == (True, 6000)
    # Not newer than the inactive edge, or not busy: nothing to attribute.
    assert H_successor.successor_attribution(6000, busy) == (False, None)
    assert H_successor.successor_attribution(5000, _native("idle", 7000)) == (False, None)
    assert H_successor.successor_attribution(5000, None) == (False, None)
    # An ambiguous (tainted) hold still releases only on native quiescence.
    hold = {"since": 0.0, "subagent_ambiguous": True}
    assert H_successor.successor_hold_should_release(hold, None, "busy") is False
    assert H_successor.successor_hold_should_release(hold, None, "idle") is True


def test_tainted_successor_hold_needs_fresh_ordered_quiescence():
    ambiguous = {"since": 0.0, "native_busy_status_updated_at": 6500,
                 "subagent_ambiguous": True}
    taint = {"a1": 6000}
    for stale in (None, 5000, 6000):
        assert H_successor.successor_hold_quiescence_is_fresh(
            ambiguous, _native("idle", stale), taint) is False
    assert H_successor.successor_hold_quiescence_is_fresh(ambiguous, None, taint) is False
    # Newer than the taint fence but not than the hold's own busy edge.
    assert H_successor.successor_hold_quiescence_is_fresh(
        ambiguous, _native("idle", 6200), taint) is False
    assert H_successor.successor_hold_quiescence_is_fresh(
        ambiguous, _native("shell", 6501), taint) is True
    # Taint already cleared: an ambiguous hold still needs post-busy quiescence.
    assert H_successor.successor_hold_quiescence_is_fresh(
        ambiguous, _native("idle", 6500), {}) is False
    assert H_successor.successor_hold_quiescence_is_fresh(
        ambiguous, _native("idle", 6501), {}) is True
    # A hold created before taint began is fenced while taint lasts.
    plain = {"since": 0.0, "native_busy_status_updated_at": 4000}
    assert H_successor.successor_hold_quiescence_is_fresh(
        plain, _native("idle", 5000), taint) is False
    assert H_successor.successor_hold_quiescence_is_fresh(
        plain, _native("idle", 6001), taint) is True
    # Untainted, unambiguous: baseline (any inactive sample).
    assert H_successor.successor_hold_quiescence_is_fresh(plain, _native("idle", None), {}) is True


def test_post_compact_hold_never_releases_on_silence_while_tainted():
    # Untainted: combined hook + PTY silence past the grace releases.
    assert H_successor.post_compact_hold_may_release(100.0, 80.0, 85.0, False, 10.0) is True
    assert H_successor.post_compact_hold_may_release(100.0, 80.0, 95.0, False, 10.0) is False
    # Tainted: a parent in a long tool is silent and the child redraws; only
    # parent terminal evidence or fresh quiescence may release.
    assert H_successor.post_compact_hold_may_release(100.0, 0.0, 0.0, True, 10.0) is False


def test_hook_fired_ms_prefers_relay_stamp():
    assert H_subagents.hook_fired_ms({"_ccx_relay_ms": 1500}, 2000) == 1500
    assert H_subagents.hook_fired_ms({}, 2000) == 2000
    # Never trust a stamp from the future or a non-int.
    assert H_subagents.hook_fired_ms({"_ccx_relay_ms": 2500}, 2000) == 2000
    assert H_subagents.hook_fired_ms({"_ccx_relay_ms": True}, 2000) == 2000
    assert H_subagents.hook_fired_ms({"_ccx_relay_ms": "1500"}, 2000) == 2000


def test_delayed_hook_cannot_keep_taint_stuck_past_final_idle():
    """Agent ends; native goes idle at 5000; the parent Stop that listed it as
    running (fired at 4000) is delivered at 7000. The fire-time fence lets the
    stationary idle token still clear the taint."""
    taint = {}
    late_stop = {"hook_event_name": "Stop", "_ccx_relay_ms": 4000,
                 "background_tasks": [{"id": "a1", "type": "subagent",
                                       "status": "running"}]}
    H_subagents.note_subagent_taint(taint, "Stop", late_stop, H_subagents.hook_fired_ms(late_stop, 7000))
    assert taint == {"a1": 4000}
    assert H_subagents.clear_subagent_taint(taint, _native("idle", 5000)) is True


def test_host_scanners_ignore_sidechain_rows(tmp_path):
    p = tmp_path / "session.jsonl"
    rows = [
        {"type": "assistant", "isSidechain": True, "isApiErrorMessage": True,
         "apiErrorStatus": 500, "error": "server_error",
         "message": {"content": [{"type": "text", "text": "API Error: 500"}]}},
        {"type": "user", "isSidechain": True, "message": {"content": [
            {"type": "text", "text": "[Request interrupted by user]"}]}},
    ]
    p.write_text("".join(json.dumps(r) + "\n" for r in rows))
    api_error, _ = H_hooks._read_new_api_error(p, 0)
    assert api_error is None
    found, _ = H_hooks._read_interrupt_marker(p, 0)
    assert found is False


def test_input_ready_pty_read_ignores_subagent_rendering_only_when_tainted():
    # Untainted: PTY output keeps gating Ink readiness (unchanged behavior).
    assert H_successor.input_ready_pty_read(99.9, False, 100.0, 1.5) == 99.9
    # Tainted: an agent's progress rendering cannot hold the gate shut.
    assert H_successor.input_ready_pty_read(99.9, True, 100.0, 1.5) == 98.5
    assert H_successor.input_ready_pty_read(90.0, True, 100.0, 1.5) == 90.0
    # Never invent a first PTY read.
    assert H_successor.input_ready_pty_read(0.0, True, 100.0, 1.5) == 0.0


def _relay_permission(monkeypatch, capsys, create_args):
    """Relay one PermissionRequest for the exact owned full-UUID worker.

    ``create_args`` None means the record cannot be read at all.
    """
    brokered = []

    def read(short):
        if create_args is None:
            raise OSError("unreadable record")
        return {"engine": "claude", "workerId": RELAY_WORKER,
                "native": {"sessionId": RELAY_SESSION},
                "createArgs": create_args}

    monkeypatch.setattr(H_hooks, "read_record", read)
    monkeypatch.setattr(H_hooks, "_relay_broker_permission",
                        lambda short, data, **kw: brokered.append(data) or {
                            "decision": "deny", "reason": "operator said no"})
    monkeypatch.setenv("CCX_SELF_WORKER_ID", RELAY_WORKER)
    monkeypatch.setenv("CCX_SELF_ENGINE", "claude")
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps({
        "hook_event_name": "PermissionRequest",
        "session_id": RELAY_SESSION,
        "tool_name": "Bash",
        "tool_input": {"command": "rm -rf build"},
    })))
    H_hooks.cmd_hooks_relay(SimpleNamespace(short=RELAY_WORKER, event="PermissionRequest"))
    decision = json.loads(capsys.readouterr().out)["hookSpecificOutput"]["decision"]
    return decision["behavior"], brokered


@pytest.mark.parametrize("create_args, behavior, brokers", [
    ({"skip_permissions": True}, "allow", False),
    ({}, "allow", False),  # default: full permissions
    ({"skip_permissions": False}, "deny", True),
    # Unreadable record: no proof of the original worker, so deny at once;
    # never auto-allow and never fall back to Claude's own interactive prompt.
    (None, "deny", False),
])
def test_permission_relay_fails_closed_without_a_readable_record(
    monkeypatch, capsys, create_args, behavior, brokers
):
    got, brokered = _relay_permission(monkeypatch, capsys, create_args)
    assert got == behavior
    assert bool(brokered) is brokers


def test_relay_stops_polling_a_request_the_host_does_not_know(monkeypatch):
    calls = []

    def fake_rpc(short, msg, timeout=2.0, **kw):
        calls.append(msg["op"])
        if msg["op"] == "permission-request":
            return {"ok": True, "request_id": "r1"}
        return {"ok": True, "request_id": "r1", "decision": None, "unknown": True}

    monkeypatch.setattr(H_hooks, "_host_rpc", fake_rpc)
    monkeypatch.setattr(H_hooks, "read_record", lambda short: {"lifecycle": {}})
    monkeypatch.setattr(H_hooks, "PERMISSION_POLL_INTERVAL_S", 0)
    decision = H_hooks._relay_broker_permission("abc12345", {"tool_name": "Bash"})
    assert decision["decision"] == "deny"
    assert calls == ["permission-request", "permission-poll"]


def test_reaping_the_worker_leaves_other_children_to_their_waiters():
    import subprocess
    import time

    worker = subprocess.Popen(["sleep", "0"])
    other = subprocess.Popen(["sh", "-c", "exit 3"])
    # A completed child may already be absent to Darwin's libproc, whereas
    # Linux still reports a zombie in /proc. Poll the exact-owned waitpid
    # instead of requiring a particular pre-reap kernel state. We must leave
    # the unrelated child's exit status for its own subprocess waiter.
    deadline = time.monotonic() + 5
    while not H.reap_worker(worker.pid):
        assert time.monotonic() < deadline
        time.sleep(0.01)
    assert H.reap_worker(worker.pid) is True
    assert H.reap_worker(worker.pid) is True  # already reaped stays gone
    assert other.wait(5) == 3


def test_write_all_drains_bidirectional_pty_and_uses_progress_timeout():
    # A renderer writes one screen of output for each block of input. If the
    # master writes without reading, both sides eventually block. Total time
    # exceeds the timeout, but no individual write stalls that long.
    import fcntl
    import pty
    import select
    import threading
    import time
    import tty

    master, slave = pty.openpty()
    tty.setraw(slave)
    fcntl.fcntl(master, fcntl.F_SETFL, fcntl.fcntl(master, fcntl.F_GETFL) | os.O_NONBLOCK)
    payload = b"z" * 64_000
    received = bytearray()
    output = bytearray()
    errors = []

    def fake_ink():
        try:
            while len(received) < len(payload):
                chunk = os.read(slave, 512)
                if not chunk:
                    break
                received.extend(chunk)
                os.write(slave, b"r" * len(chunk))
                time.sleep(0.003)
        except OSError as error:
            errors.append(error)

    thread = threading.Thread(target=fake_ink, daemon=True)
    thread.start()

    def drain():
        try:
            chunk = os.read(master, 4096)
        except BlockingIOError:
            return True
        output.extend(chunk)
        return bool(chunk)

    try:
        start = time.monotonic()
        assert H_pty.write_all(master, payload, timeout_s=0.05, on_read=drain)
        assert time.monotonic() - start > 0.05
        until = time.monotonic() + 3
        while (thread.is_alive() or len(output) < len(payload)) and time.monotonic() < until:
            readable, _, _ = select.select([master], [], [], 0.05)
            if readable:
                drain()
        thread.join(timeout=0.5)
        assert not errors
        assert received == payload
        assert output == b"r" * len(payload)
    finally:
        os.close(master)
        os.close(slave)


def test_write_all_unconsumed_pty_fails_after_no_progress():
    import fcntl
    import pty
    import time
    import tty

    master, slave = pty.openpty()
    tty.setraw(slave)
    fcntl.fcntl(master, fcntl.F_SETFL, fcntl.fcntl(master, fcntl.F_GETFL) | os.O_NONBLOCK)
    try:
        start = time.monotonic()
        assert H_pty.write_all(master, b"a" * 128_000, timeout_s=0.05) is False
        assert time.monotonic() - start < 1
    finally:
        os.close(master)
        os.close(slave)


def test_stalled_paste_cleanup_requires_quiet_unstick_and_confirmed_clear():
    slot = {}
    now = 100.0
    assert H_pty.stalled_paste_clear_action(slot, now, now - 0.01) is None
    assert H_pty.stalled_paste_clear_action(slot, now, now - 2) == "unstick"
    slot["stalled_unstuck_at"] = 100.0
    assert H_pty.stalled_paste_clear_action(slot, 100.1, 98.0) is None
    assert H_pty.stalled_paste_clear_action(slot, 100.4, 98.0) == "clear"
    slot["stalled_cleared_at"] = 100.4
    assert H_pty.stalled_paste_clear_action(slot, 100.5, 98.0) is None
    assert H_pty.stalled_paste_clear_action(slot, 101.0, 100.5) is None
    assert H_pty.stalled_paste_clear_action(slot, 102.2, 100.5) == "reject"


def test_failed_semantic_pty_write_reports_unknown_without_crashing_worker(monkeypatch):
    from ccx.ops import common as protocol_ops

    monkeypatch.setattr(ops_wait, "wait_worker", lambda *a, **kw: (
        pytest.fail("write-stalled evidence should not poll worker death")))
    for kind in ("pty-write-failed", "pty-submit-failed"):
        verdict = send_settle_failure("claude:worker", {
            "delivery": {"status": "pending", "evidence_status": kind}
        })
        assert verdict["status"] == "unknown"
        assert "PTY input stalled" in verdict["error"]
        assert "do not resend" in verdict["error"]


def test_parent_hook_drained_during_idle_grace_cannot_finalize_old_candidate():
    """The exact post-ingress predicate rejects a just-reset candidate.

    The prior implementation treated a deleted settling_at as epoch zero and
    could finalize A immediately after a conflicting parent hook was drained.
    """
    from ccx.claude.host import session as H
    obs = {"turn_ref": "A", "passive": True, "settling": True,
           "settling_at": 100.0, "inactive_status_updated_at": 400}
    state = {"interrupt_observation": obs, "active_turn_ref": "A",
             "settling_turn_ref": "A"}
    assert not H_successor.settling_grace_elapsed(state, obs, now=100.1, grace_s=2.0)

    # Model drain_ready_ingress dispatching a contradictory parent PreToolUse:
    # the same mutable observation object is still present, but reset by hook.
    def drain_parent_tool_hook():
        obs["settling"] = False
        obs.pop("settling_at")
        obs.pop("inactive_status_updated_at")
        state["settling_turn_ref"] = None
    drain_parent_tool_hook()
    assert not H_successor.settling_grace_elapsed(state, obs, now=999.0, grace_s=2.0)

    # An entirely new B candidate created during ingress likewise must not be
    # mistaken for the old A, nor treated as having served its full grace.
    newer = {"turn_ref": "B", "passive": True, "settling": True,
             "settling_at": 998.9}
    state.update(active_turn_ref="B", settling_turn_ref="B", interrupt_observation=newer)
    assert not H_successor.settling_grace_elapsed(state, obs, now=999.0, grace_s=2.0)
    assert not H_successor.settling_grace_elapsed(state, newer, now=999.0, grace_s=2.0)
    assert H_successor.settling_grace_elapsed(state, newer, now=1001.1, grace_s=2.0)
