from __future__ import annotations
import json
import pathlib
import re
import socket
import sys
from ccx.constants import (
    BUDGET_EXIT,
    EXIT_ERROR,
    EXIT_API_TRANSIENT,
    EXIT_RATE_LIMITED,
    EXIT_TURN_INCOMPLETE,
    EXIT_UNAVAILABLE,
    EXIT_WAITING,
)
from ccx.errors import (
    CcxError,
    DeliveryRejected,
    InvalidRequest,
    SessionUnavailable,
)
from ccx.store.paths import sock_path
from ccx.store.records import read_record
from ccx.controllers.orchestrator import require_notify_parent
from ccx.notify.daemon import prepare_notify_parent
from ccx.notify.registry import public_registration as public_parent_notification
from ccx.rpc.rpc import (
    _rpc,
)
from ccx.ops.semantics import (
    KIND_ELICITATION,
    payload_guidance as _payload_guidance,  # noqa: F401
    pending_input_kind,
    question_answer_map as _pure_question_answer_map,
)


def _read_stdin_prompt(op: str) -> str:
    try:
        return sys.stdin.read()
    except OSError as e:
        sys.stderr.write(f"[ccx] {op}: failed to read prompt from stdin: {e}\n")
        sys.exit(EXIT_ERROR)


def _read_file_prompt(op: str, path: str) -> str:
    if path == "-":
        return _read_stdin_prompt(op)
    try:
        return pathlib.Path(path).read_text()
    except OSError as e:
        sys.stderr.write(f"[ccx] {op}: failed to read prompt file {path}: {e}\n")
        sys.exit(EXIT_ERROR)


def _resolve_prompt_source(args, attr: str, op: str, *, required: bool) -> str | None:
    """Resolve positional / --stdin / --file prompt input.

    `new` allows no prompt source; `send` requires exactly one. Validation lives
    here, not only in argparse, so programmatic command calls obey the same rules.
    """
    positional = getattr(args, attr, None)
    use_stdin = bool(getattr(args, "stdin", False))
    file_path = getattr(args, "file", None)
    sources = int(positional is not None) + int(use_stdin) + int(file_path is not None)
    if sources > 1:
        msg = f"[ccx] {op}: choose only one prompt source: positional text, --stdin, or --file"
        # `ccx new mywork --stdin` is almost always a name/prompt mix-up: the
        # positional is a *prompt*, names go through -n. Only hint when the
        # positional is a single name-like token, so real prompt text doesn't
        # get echoed back as a bogus suggestion.
        if (op == "new" and positional is not None
                and len(positional) <= 64
                and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", positional)):
            msg += (f" (the positional argument is the prompt, not a name — "
                    f"did you mean `-n {positional}`?)")
        sys.stderr.write(msg + "\n")
        sys.exit(EXIT_ERROR)
    if sources == 0:
        if required:
            sys.stderr.write(f"[ccx] {op}: prompt required (pass text, --stdin, or --file)\n")
            # A usage error, like argparse's own: bad input, never "not found".
            sys.exit(EXIT_ERROR)
        return None
    if use_stdin:
        return _read_stdin_prompt(op)
    if file_path is not None:
        return _read_file_prompt(op, file_path)
    return positional


def _require_parent_notification():
    try:
        parent = require_notify_parent()
        prepare_notify_parent(parent)
        return parent
    except CcxError as error:
        sys.stderr.write(f"[ccx] notify: {error}\n")
        raise SystemExit(error.exit_code) from None


def _prepare_parent_notification(args):
    if not getattr(args, "notify_parent", False):
        return None
    return _require_parent_notification()


def _emit_parent_notification_armed(
    short: str, registration: dict, *, json_mode: bool, base: dict | None = None
) -> None:
    public = public_parent_notification(registration)
    if json_mode:
        value = dict(base or {})
        value.setdefault("short", short)
        value["notify_parent"] = public
        print(json.dumps(value, indent=2))
        return
    if base is None:
        print(short)
    print(f"notify: armed {public['event_id']} -> Codex parent {public['parent']}")
    print(f"notify: {public['instruction']}")


def _nbwait_exit_code(status: str | None, api_error: dict | None = None) -> int:
    """Canonical status-to-exit-code mapping for wait outcomes.

    Original managed wait outcomes map to meaningful process exit codes.
    A wait verdict describes observation, not whether native input was sent.

    Documented contract:
      blocked / waiting       -> EXIT_WAITING (input gate still up; exit 4)
      gone / dead / crashed   -> EXIT_UNAVAILABLE (worker gone; managed adds
                                 host-dead: live-looking record, host not alive)
      budget-stopped          -> BUDGET_EXIT (cap trip)
      timeout / unknown       -> EXIT_TURN_INCOMPLETE (clean-completion miss; exit 6)

    `api_error` refines the terminal case into the three outcomes with different
    remedies: rate-limited (wait for the reset), overloaded (retry in minutes,
    costs no quota), and everything else (permanent — fix it or stop). Without
    the split, a script waiting on a run cannot tell them apart.
    """
    if status in ("blocked", "waiting"):
        return EXIT_WAITING
    if status in ("gone", "dead", "crashed", "idle-killed",
                  "host-dead", "unsupported"):
        if isinstance(api_error, dict):
            # Rate limiting wins over transience: a throttled 429 is marked
            # both, and the rate limit is the one that must not be missed.
            if api_error.get("rate_limited"):
                return EXIT_RATE_LIMITED
            if api_error.get("transient"):
                return EXIT_API_TRANSIENT
        return EXIT_UNAVAILABLE
    if status == "budget-stopped":
        return BUDGET_EXIT
    return EXIT_TURN_INCOMPLETE


def _has_structured_answer_args(args) -> bool:
    return bool(getattr(args, "option", None) or getattr(args, "text_answer", None))


def _question_answer_map(question: dict | None, args) -> tuple[dict[str, str], str | None]:
    'CLI-args adapter over the pure :func:`ccx.ops.semantics.question_answer_map`.'
    return _pure_question_answer_map(
        (question or {}).get("questions") or [],
        text=getattr(args, "text", None),
        options=getattr(args, "option", None),
        text_answers=getattr(args, "text_answer", None),
    )


def _render_question_wait_lines(short: str, waiting_for: dict, *, indent: bool = False) -> list[str]:
    prefix = (lambda s: _indent(s)) if indent else (lambda s: s)
    questions = ((waiting_for.get("input") or {}).get("questions") or [])
    lines = [f"waiting for answer: AskUserQuestion"]
    if waiting_for.get("id"):
        lines.append(prefix(f"id: {waiting_for.get('id')}"))
    for i, q in enumerate(questions, 1):
        header = q.get("header") or f"Q{i}"
        multi = " [multiSelect]" if q.get("multiSelect") else ""
        lines.append(prefix(f"{i}. [{header}] {q.get('question')}{multi}"))
        for opt in (q.get("options") or []):
            label = opt.get("label")
            desc = opt.get("description")
            suffix = f" — {desc}" if desc else ""
            lines.append(prefix(f"   - {label}{suffix}"))
    if len(questions) <= 1:
        lines.append(prefix(f"answer:      ccx answer {short} <option-or-text> --wait"))
    else:
        lines.append(prefix(f"answer:      ccx answer {short} --option 1 <label> --text-answer 2 <text> --wait"))
        lines.append(prefix(f"answer-json: ccx answer {short} '{{\"<question>\": \"<answer>\"}}' --wait"))
    return lines


def _elicitation_field_line(name: str, spec: dict, required: bool) -> str:
    kind = spec.get("type") or "string"
    if kind == "array":
        items = spec.get("items") if isinstance(spec.get("items"), dict) else {}
        options = items.get("enum") or [o.get("const") for o in (items.get("anyOf") or [])
                                         if isinstance(o, dict)]
        kind = f"list of {options}" if options else "list"
    else:
        options = spec.get("enum") or [o.get("const") for o in (spec.get("oneOf") or [])
                                       if isinstance(o, dict)]
        if options:
            kind = f"one of {options}"
    title = spec.get("title") or spec.get("description")
    extras = []
    if "default" in spec:
        extras.append(f"default {json.dumps(spec['default'], ensure_ascii=False)}")
    if required:
        extras.append("required")
    tail = f" ({', '.join(extras)})" if extras else ""
    label = f" — {title}" if title else ""
    return f"   - {name}: {kind}{tail}{label}"


def _render_elicitation_wait_lines(short: str, waiting_for: dict, *, indent: bool = False) -> list[str]:
    """Human rendering of a pending MCP elicitation plus the exact verbs."""
    prefix = (lambda s: _indent(s)) if indent else (lambda s: s)
    server = waiting_for.get("server") or "?"
    lines = [f"waiting for MCP input: {server} ({waiting_for.get('mode') or 'form'})"]
    if waiting_for.get("id"):
        lines.append(prefix(f"id: {waiting_for.get('id')}"))
    if waiting_for.get("message"):
        lines.append(prefix(_truncate(str(waiting_for.get("message")), 2000)))
    if waiting_for.get("url"):
        lines.append(prefix(f"url: {waiting_for.get('url')}"))
    schema = waiting_for.get("schema") if isinstance(waiting_for.get("schema"), dict) else {}
    props = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
    required = set(schema.get("required") or [])
    if props:
        lines.append(prefix("fields:"))
        for name, spec in props.items():
            lines.append(prefix(_elicitation_field_line(
                name, spec if isinstance(spec, dict) else {}, name in required)))
    if not waiting_for.get("id"):
        lines.append(prefix(f"This input is only available in the worker terminal; open it with ccx attach {short}"))
        return lines
    if waiting_for.get("mode") == "url":
        lines.append(prefix(f"accept:      ccx respond {short} --accept --wait   (after finishing the URL flow)"))
    elif props:
        example = " ".join(f"--field {name}=<value>" for name in list(props)[:2])
        lines.append(prefix(f"accept:      ccx respond {short} --accept {example} --wait"))
    else:
        lines.append(prefix(f"accept:      ccx respond {short} --accept --wait"))
    lines.append(prefix(f"decline:     ccx respond {short} --decline --wait"))
    lines.append(prefix(f"cancel:      ccx respond {short} --cancel --wait"))
    return lines


def _managed_question_json(short: str, waiting_for: dict) -> dict:
    questions = ((waiting_for.get("input") or {}).get("questions") or [])
    if len(questions) <= 1:
        answer_command = f"ccx answer {short} <option-or-text> --wait"
    else:
        answer_command = f"ccx answer {short} --option 1 <label> --text-answer 2 <text> --wait"
    return {
        "source": "managed-hook",
        "tool": "AskUserQuestion",
        "request_id": waiting_for.get("id"),
        "questions": questions,
        "can_answer": True,
        "answer_command": answer_command,
    }


def _uuid_input_guard(args, verb: str) -> None:
    """Reject unavailable notification handling before side effects."""
    if getattr(args, "notify_parent", False):
        raise InvalidRequest(
            f"{verb}: --notify-parent requires the separate N4 native input "
            "notification restoration"
        )


def _uuid_input_print(args, verb: str, request_id: str, result) -> None:
    """Render the actual original client result; fallback is a NEW message."""
    fallback = result.delivery_id is not None
    mode = "new-message-fallback" if fallback else "native-input"
    if args.json:
        print(json.dumps({
            "ok": True, **result.as_dict(),
            "request_id": request_id, "decision": verb,
            "input_mode": mode, "original_gate_answered": not fallback,
        }, ensure_ascii=False, indent=2))
        return
    if fallback:
        print(
            f"{verb}: original input host was dead; NEW user message delivered "
            "(former gate NOT answered)"
        )
        print(f"Delivery: {result.delivery_id}")
    elif result.settled:
        print(f"{verb}: input accepted; turn {result.turn_ref} completed")
    else:
        print(f"{verb} accepted for Claude request {request_id}")
    if result.reply is not None:
        print(result.reply)
    print(f"Worker: claude:{result.worker_id}")


def _uuid_input_error(verb: str, error: CcxError) -> None:
    message = str(error)
    if message.startswith(f"{verb}: "):
        message = message[len(verb) + 2:]
    print(f"[ccx] {verb}: {message}", file=sys.stderr)
    raise SystemExit(error.exit_code) from None


def _uuid_input_id(args, waiting: dict | None, kind: str) -> str:
    """Bind one current original right-kind gate when a CLI ID was omitted."""
    supplied = getattr(args, "request_id", None)
    if supplied is not None:
        return supplied
    if pending_input_kind(waiting) is None:
        raise DeliveryRejected(
            f"No pending {kind} request; check ccx peek for open input."
        )
    if (not isinstance(waiting, dict)
            or pending_input_kind(waiting) != kind
            or not isinstance(waiting.get("id"), str)
            or not waiting["id"]):
        raise SessionUnavailable(
            f"No pending {kind} request; check ccx peek for open input."
        )
    return waiting["id"]


def _read_waiting_for(short: str) -> dict | None:
    """Best-effort fetch of the live pending-permission preview: prefer the host
    (authoritative, in-memory) and fall back to the on-disk record."""
    if sock_path(short).exists():
        try:
            r = _rpc(short, {"op": "status"}, timeout=0.5)
            if r.get("ok") and r.get("waitingFor"):
                return r.get("waitingFor")
        except (OSError, ValueError, socket.timeout):
            pass
    return (read_record(short) or {}).get("waitingFor")


def emit_waiting(short: str, header: str | None = None) -> None:
    """Print an actionable description of the pending permission request to
    stdout, so a caller whose `send`/`new --wait` exited on WAITING knows what is
    blocked and exactly how to clear it. Human-readable in all modes — these are
    blocking-exit paths an operator or orchestrating agent reads directly, and the
    `Short:` line keeps it greppable like the other non-zero exits."""
    from ccx.client import CcxClient
    waiting = CcxClient().peek(short).waiting_for
    wf = waiting.as_dict() if waiting is not None else None
    request_id = (wf or {}).get("id")
    tool = (wf or {}).get("tool") or "?"
    if not request_id:
        print(header or f"Worker: claude:{short}")
        if (wf or {}).get("message"):
            print(_indent(_truncate(str(wf["message"]), 2000)))
        print("This input cannot be answered here; use ccx attach in the worker terminal")
        return
    if tool == "AskUserQuestion":
        print(header or f"Short: {short}")
        for line in _render_question_wait_lines(short, wf, indent=True):
            print(line.replace(" --wait", f" --request-id {request_id}"))
        return
    if pending_input_kind(wf) == KIND_ELICITATION:
        print(header or f"Short: {short}")
        for line in _render_elicitation_wait_lines(short, wf, indent=True):
            print(line.replace(" --wait", f" --request-id {request_id}"))
        return
    print(header or f"Short: {short}")
    print(f"waiting for approval: {tool}  request_id={request_id}")
    inp = (wf or {}).get("input")
    if inp is not None:
        try:
            rendered = json.dumps(inp, ensure_ascii=False, indent=2)
        except TypeError:
            rendered = str(inp)
        print(_indent(_truncate(rendered, 2000)))
    print(f"  approve:  ccx approve {short} --request-id {request_id}")
    print(f"  reject:   ccx reject {short} --request-id {request_id} --reason \"...\"")


def _truncate(s, n):
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[:n] + "…"


def _indent(s, prefix="  "):
    return "\n".join(prefix + line for line in s.splitlines())


def local_reset_time(value) -> str:
    """A provider reset time (epoch seconds or ISO text) in local time with its zone."""
    from datetime import datetime, timezone
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            moment = datetime.fromtimestamp(value, timezone.utc)
        elif isinstance(value, str) and value:
            moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
        else:
            return ""
    except (ValueError, OverflowError, OSError):
        return str(value)[:16].replace("T", " ")
    return moment.astimezone().strftime("%Y-%m-%d %H:%M %Z")
