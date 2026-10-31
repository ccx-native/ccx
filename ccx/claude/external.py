"""Read-only exact native Claude session evidence for controlled CCX import.

A native SID is not a CCX worker target. Only the exact canonical original
session ID may be inspected for one owner-proven import. No external session
discovery, cache materialization, daemon control, reply or native-bg adoption.
"""
from __future__ import annotations

import json
import os
import pathlib

from ccx.system import oslayer
from ccx.constants import CLAUDE_HOME
from ccx.store.paths import transcript_path_for
from ccx.claude.transcript import pending_ask_user_question


WAIT_NONE = "none"


WAIT_QUESTION = "question"


WAIT_PERMISSION = "permission"


WAIT_SHELL = "shell"


WAIT_UNKNOWN = "unknown"


_BACKGROUND_KINDS = {"daemon", "daemon-worker", "bg", "background"}


def short_from_id(identifier: str) -> str:
    return identifier.split("-", 1)[0][:8]


def _job_state(short: str) -> dict | None:
    try:
        return json.loads((CLAUDE_HOME / "jobs" / short / "state.json").read_text())
    except (OSError, ValueError):
        return None


def _session_files(identifier: str) -> list[dict]:
    root = CLAUDE_HOME / "sessions"
    if not root.exists():
        return []
    out = []
    for p in root.glob("*.json"):
        try:
            meta = json.loads(p.read_text())
        except (OSError, ValueError):
            continue
        sid = meta.get("sessionId")
        if sid == identifier:
            out.append(meta)
    return out


def exact_import_candidate(session_id: str) -> dict:
    """Read ONLY an exact native Claude SID's provider-owned import facts."""
    import stat
    from uuid import UUID
    from ccx.errors import InvalidRequest, SessionNotFound, SessionUnavailable

    try:
        if str(UUID(session_id)) != session_id:
            raise ValueError("noncanonical")
    except (ValueError, TypeError, AttributeError) as exc:
        raise InvalidRequest("import needs the exact canonical native Claude sessionId") from exc
    # Never resolve prefixes/names or materialize a cached external CCX record.
    found = sorted((CLAUDE_HOME / "projects").glob(f"*/{session_id}.jsonl"))
    if not found:
        raise SessionNotFound("native Claude session transcript not found: " + session_id)
    if len(found) != 1:
        raise SessionUnavailable("ambiguous exact native Claude transcript locations: "
                                 + session_id)
    path = found[0]
    try:
        st = path.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_size <= 0:
            raise SessionUnavailable("native Claude transcript is missing or empty")
    except OSError as exc:
        raise SessionUnavailable("native Claude transcript is unavailable") from exc

    meta = _session_files(session_id)
    live = []
    # PID-keyed metadata survives process exit/reuse. Establish presence from
    # the kernel, not argv: unreadable argv must never turn a live original
    # into an apparently stopped import candidate.
    for entry in meta:
        pid = entry.get("pid")
        if type(pid) is not int or pid <= 0:
            continue  # no process to claim from this metadata row
        observed = oslayer.process_info(pid)
        if observed.presence == "dead":
            continue  # stopped originals keep their existing import policy
        start = entry.get("procStart")
        if (observed.presence != "alive"
                or not oslayer.provider_start_matches(
                    start, observed.start_token, entry.get("pidDomain"))):
            raise SessionUnavailable(
                "live native Claude original PID/procStart identity unproven"
            )
        live.append(entry)
    if len(live) > 1:
        raise SessionUnavailable("more than one live native Claude process names session")
    info = (live[0] if live else meta[0] if meta else {})
    if len(meta) > 1 and not live and any(
            (m.get("cwd"), m.get("kind")) !=
            (info.get("cwd"), info.get("kind")) for m in meta):
        raise SessionUnavailable("contradictory exact native Claude session metadata")
    original_cwd = _cwd_from_transcript(path)
    claimed_cwd = info.get("cwd")
    if original_cwd and claimed_cwd and os.path.realpath(original_cwd) != os.path.realpath(claimed_cwd):
        raise SessionUnavailable("native Claude transcript and session metadata cwd disagree")
    cwd = claimed_cwd or original_cwd
    if not isinstance(cwd, str) or not cwd or not os.path.isdir(cwd):
        raise SessionUnavailable("native Claude source cwd is unavailable")
    cwd = os.path.realpath(cwd)
    if path != transcript_path_for(cwd, session_id):
        raise SessionUnavailable("exact native Claude transcript does not match canonical cwd/project")
    short = short_from_id(session_id)
    job = _job_state(short)
    if isinstance(job, dict) and job.get("sessionId") is None:
        raise SessionUnavailable("native-bg job short collision has no exact session identity")
    background = (isinstance(job, dict) and job.get("sessionId") == session_id
                  or info.get("kind") in _BACKGROUND_KINDS)
    return {
        "sessionId": session_id,
        "short": short,
        "cwd": cwd,
        "transcript": str(path),
        "name": info.get("name"),
        "live": bool(live),
        "status": info.get("status") if live else None,
        "waitKind": _wait_kind(bool(live), info.get("status"), info.get("waitingFor")),
        "backgroundAgent": background,
    }


def _cwd_from_transcript(path: pathlib.Path) -> str | None:
    try:
        with path.open() as f:
            for line in f:
                try:
                    evt = json.loads(line)
                except ValueError:
                    continue
                cwd = evt.get("cwd")
                if cwd:
                    return cwd
    except OSError:
        return None
    return None


def _wait_kind(live: bool, status: str | None, waiting_for: str | None) -> str:
    if not live:
        return WAIT_NONE
    if waiting_for == "permission prompt":
        return WAIT_PERMISSION
    if waiting_for == "dialog open":
        return WAIT_QUESTION
    if status == "shell" or waiting_for == "sandbox request":
        return WAIT_SHELL
    if waiting_for in ("worker request", "input needed"):
        return WAIT_UNKNOWN
    return WAIT_NONE


def import_eligibility(rec: dict) -> tuple[bool, str, str]:
    """Can this exact native Claude session be imported under CCX management?

    Product rule: a regular external Claude session may be imported when it is
    **idle** — including a *live* idle one — or when it is stopped/not-live with
    a transcript at rest. Everything non-idle refuses: busy, any wait
    (permission/question/shell/unknown), native-bg, missing transcript, missing cwd.

    Note: importing a live-idle session is an *active resume*, not a passive
    attach — the original terminal must not be used afterward (the caller warns
    about this). ccx deliberately does not require closing the old terminal."""
    short = rec.get("short") or short_from_id(rec.get("sessionId") or "?")
    status = rec.get("status")
    live = bool(rec.get("live"))
    wait_kind = rec.get("waitKind") or WAIT_NONE
    cwd = rec.get("cwd") or ""

    if rec.get("backgroundAgent"):
        return False, "native-bg", (
            f"session {short} is a native Claude background agent; native-bg "
            "import is not supported. Inspect or manage that job through "
            "Claude's own interface; CCX cannot control or import it"
        )
    # Live, non-idle states first — the common, most-actionable "wait and retry"
    # refusals. Only an *idle* live session (and a stopped one) is importable.
    if live:
        if wait_kind == WAIT_PERMISSION:
            return False, "permission", (
                f"session {short} is waiting on a permission prompt owned by the "
                "Claude TUI; approve/reject it there, wait for the session to go "
                "idle, then import"
            )
        if wait_kind == WAIT_QUESTION:
            return False, "question", (
                f"session {short} is waiting on a question/dialog; answer it in the "
                "Claude TUI, wait for the session to go idle, then import"
            )
        if wait_kind == WAIT_SHELL:
            return False, "shell", (
                f"session {short} is in a shell command; let it return to the prompt, "
                "then import"
            )
        if wait_kind != WAIT_NONE:  # unknown / worker-request / input-needed
            return False, "unknown", (
                f"session {short} is in an unknown wait state ({wait_kind}); resolve it "
                "in the Claude TUI and import once it is idle"
            )
        if status == "busy":
            return False, "busy", (
                f"session {short} is busy (a turn is running); wait for the current "
                "turn to finish, then import"
            )
        if status != "idle":
            return False, "not-idle", (
                f"session {short} is live with status={status or 'unknown'}; import only "
                "when it is idle and not waiting"
            )
    # A stopped/non-live session can still have an unanswered AskUserQuestion as
    # the last tool use in the transcript. Resuming that under ccx would re-enter
    # the question during the import auto-continue turn, before an operator has a
    # clean `ccx answer` opportunity, so refuse it just like a live question wait.
    transcript = rec.get("transcript")
    if transcript and os.path.exists(transcript):
        pend = pending_ask_user_question(transcript)
        if pend and pend.get("questions"):
            return False, "question", (
                f"session {short} has an unanswered question/dialog in its transcript; "
                "answer or abandon it in Claude, wait for the session to go idle, then import"
            )
    # Preconditions for a clean resume-under-ccx (checked after the live-state
    # gate so a busy session reports 'busy', not an incidental cwd/transcript edge).
    if not transcript or not os.path.isfile(transcript):
        return False, "no-transcript", (
            f"session {short} has no transcript yet; start a turn in Claude before importing"
        )
    if not (cwd and os.path.isdir(cwd)):
        return False, "no-cwd", f"session {short} cwd does not exist: {cwd or '(unset)'}"
    return True, "ok", f"session {short} is importable"
