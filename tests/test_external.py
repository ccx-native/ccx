"""Import-only native Claude refusal reasons, without external worker modes."""
from __future__ import annotations

import pytest

from ccx.claude import external as E


def _candidate(tmp_path, *, live=False, status=None, wait_kind=E.WAIT_NONE,
               background=False, transcript=True, cwd=True):
    path = tmp_path / "native.jsonl"
    if transcript:
        path.write_text("")
    return {
        "sessionId": "abcd1234-0000-4000-8000-000000000000",
        "short": "abcd1234",
        "cwd": str(tmp_path) if cwd else "/nonexistent/import-cwd",
        "transcript": str(path) if transcript else None,
        "live": live, "status": status, "waitKind": wait_kind,
        "backgroundAgent": background,
    }


@pytest.mark.parametrize("live,status,waiting,expected", [
    (False, "busy", "permission prompt", E.WAIT_NONE),
    (True, "busy", "permission prompt", E.WAIT_PERMISSION),
    (True, "busy", "dialog open", E.WAIT_QUESTION),
    (True, "shell", None, E.WAIT_SHELL),
    (True, "busy", "worker request", E.WAIT_UNKNOWN),
])
def test_import_wait_kind(live, status, waiting, expected):
    assert E._wait_kind(live, status, waiting) == expected


@pytest.mark.parametrize("options,reason", [
    ({"background": True}, "native-bg"),
    ({"live": True, "status": "busy", "wait_kind": E.WAIT_PERMISSION}, "permission"),
    ({"live": True, "status": "busy", "wait_kind": E.WAIT_QUESTION}, "question"),
    ({"live": True, "status": "busy", "wait_kind": E.WAIT_SHELL}, "shell"),
    ({"live": True, "status": "busy", "wait_kind": E.WAIT_UNKNOWN}, "unknown"),
    ({"live": True, "status": "busy"}, "busy"),
    ({"live": True, "status": "resuming"}, "not-idle"),
    ({"transcript": False}, "no-transcript"),
    ({"cwd": False}, "no-cwd"),
])
def test_import_refusal_reasons(tmp_path, options, reason):
    assert E.import_eligibility(_candidate(tmp_path, **options))[1] == reason


@pytest.mark.parametrize("live,status", [(False, None), (True, "idle")])
def test_import_at_rest_or_live_idle_allowed(tmp_path, live, status):
    assert E.import_eligibility(_candidate(tmp_path, live=live, status=status))[:2] == (True, "ok")


def test_import_stopped_pending_question_remains_refused(tmp_path, monkeypatch):
    candidate = _candidate(tmp_path)
    monkeypatch.setattr(E, "pending_ask_user_question",
                        lambda _: {"questions": [{"question": "pending"}]})
    assert E.import_eligibility(candidate)[:2] == (False, "question")
