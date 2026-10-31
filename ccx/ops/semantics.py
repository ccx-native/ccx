"""Pure, importable CCX semantic helpers — single-sourced client semantics.

This module holds four pure semantic domains shared by the real Python client,
CLI, worker facade and host:

1. **Pending-input kind** — classifying a raw ``waitingFor`` payload as an
   AskUserQuestion, an MCP elicitation, or a permission prompt
   (:func:`pending_input_kind`).
2. **Answer legality / mapping** — whether ``answer()`` is legal against the
   current pending input (:func:`answer_illegality`), the request-identity
   fence (:func:`answer_request_fence`), and the mapping of caller-supplied
   answers onto pending questions (:func:`question_answer_map`,
   :func:`question_answer_message`, :func:`parse_question_index`), and the
   MCP elicitation equivalents (:func:`elicitation_illegality`,
   :func:`elicitation_response_problem`, :func:`elicitation_field_content`).
3. **Event filtering** — the epoch-aware ``(epoch, seq)`` cursor semantics for
   the persisted event stream (:func:`filter_events_after`), including the
   documented unknown-epoch full replay.
4. **Optional payload bounds** — the managed paste-channel byte bound and its guidance
   message (:func:`send_payload_violation`, :func:`payload_guidance`).

Everything here is a pure function of its inputs (plus the process-level
``SEND_TEXT_MAX_BYTES`` setting). No I/O, no sockets, no records, no clock.
The real client, CLI and host import from here. Tests assert their common
semantic behavior without duplicating the rules.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from ccx import constants as _constants

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ccx.types import Event

# The tool name whose pending input is a structured question (answered with
# `answer`); every other pending tool is a permission prompt (approve/reject).
QUESTION_TOOL = "AskUserQuestion"

KIND_QUESTION = "question"
KIND_PERMISSION = "permission"
# An MCP server's `elicitation/create` form or URL request, brokered through
# Claude's `Elicitation` hook. Resolved with accept/decline/cancel (`respond`),
# never with `answer` or `approve`/`reject`.
KIND_ELICITATION = "elicitation"
ELICITATION_ACTIONS = ("accept", "decline", "cancel")

# Host wording for a request-identity fence violation; the host-side
# `answer-permission` RPC rejects with exactly this reason, so a client-side
# fence must use the same vocabulary (one condition for consumers).
REQUEST_ID_MISMATCH = "request id mismatch"


# --- 1. pending-input kind ---------------------------------------------------

def pending_input_kind(waiting_for: dict | None) -> str | None:
    """Classify a raw host ``waitingFor`` preview.

    Returns ``"question"`` for an AskUserQuestion, ``"elicitation"`` for an
    MCP elicitation, ``"permission"`` for any other pending tool approval, and
    ``None`` when there is no pending input. This is the single source for the
    kind distinction used by :class:`ccx.types.PendingInput`,
    ``CcxClient.answer``/``approve``/``reject``/``respond_elicitation`` guards,
    and the scripted client.
    """
    if not waiting_for or not isinstance(waiting_for, dict):
        return None
    if waiting_for.get("type") == KIND_ELICITATION:
        return KIND_ELICITATION
    return KIND_QUESTION if waiting_for.get("tool") == QUESTION_TOOL else KIND_PERMISSION


def wrong_verb_hint(kind: str | None) -> str:
    """The verb that resolves a pending input of ``kind`` (for error text)."""
    if kind == KIND_QUESTION:
        return "answer"
    if kind == KIND_ELICITATION:
        return "respond"
    return "approve/reject"


# --- 2. answer legality / mapping ---------------------------------------------

def answer_illegality(waiting_for: dict | None, short: str) -> str | None:
    """Why answering a *question* is illegal right now, or ``None`` if legal.

    Mirrors the real answer-verb legality matrix: no pending input at all, or
    pending input of the permission kind (which must be resolved with
    approve/reject, never `answer`). Host liveness (live answer vs
    ``fallback_to_send`` revive/degrade) is deliberately not part of this
    check — it is transport state, not answer legality, and both clients
    handle it after this gate.
    """
    kind = pending_input_kind(waiting_for)
    if kind is None:
        return f"no pending question found for {short}"
    if kind == KIND_ELICITATION:
        return (
            f"session {short} is waiting on an MCP elicitation from "
            f"{(waiting_for or {}).get('server') or '?'}; use respond instead"
        )
    if kind != KIND_QUESTION:
        return (
            f"session {short} is waiting on a permission prompt for "
            f"{(waiting_for or {}).get('tool') or '?'}; use approve/reject instead"
        )
    return None


def answer_request_fence(
    waiting_for: dict | None, expected_request_id: str | None
) -> str | None:
    """Request-identity fence for ``answer(expected_request_id=...)``.

    Returns ``None`` when no fence was requested or the pending request's id
    matches; otherwise the canonical :data:`REQUEST_ID_MISMATCH` reason. This
    closes the peek→answer TOCTOU window: a question that changed between
    inspection and answer is rejected instead of silently answering the new
    question with a stale decision.
    """
    if expected_request_id is None:
        return None
    if (waiting_for or {}).get("id") != expected_request_id:
        return REQUEST_ID_MISMATCH
    return None


def parse_question_index(raw, count: int) -> tuple[int, str | None]:
    """Parse a 1-based question index into a 0-based one, or an error."""
    try:
        idx = int(raw)
    except (TypeError, ValueError):
        return -1, f"question index must be an integer, got {raw!r}"
    if idx < 1 or idx > count:
        return -1, f"question index {idx} is out of range; expected 1..{count}"
    return idx - 1, None


def question_answer_map(
    questions: list[dict],
    *,
    text: str | None = None,
    options: list[tuple] | None = None,
    text_answers: list[tuple] | None = None,
) -> tuple[dict[str, str], str | None]:
    """Map caller-supplied answers onto pending questions.

    Pure core of the ``answer`` verb shared by the CLI, the Python client, and
    the scripted client. ``questions`` is the pending AskUserQuestion
    ``input.questions`` list. Exactly one of two shapes is legal:

    * structured: ``options=[(1-based index, label), ...]`` and/or
      ``text_answers=[(index, free text), ...]`` covering every question;
    * plain text: a single answer string for one question, or a JSON object
      mapping exact question text to answer text for several.

    Returns ``(answers, None)`` on success — mapping exact question text to
    answer text — or ``({}, error)`` describing the violation. An empty plain
    ``text`` yields ``({}, None)``: "no answer given" is the caller's error to
    raise, matching the historical split between mapping and emptiness.
    """
    questions = list(questions or [])
    if options or text_answers:
        if text:
            return {}, "do not combine positional/stdin/file answer text with --option/--text-answer"
        answers_by_index: dict[int, list[str]] = {}
        for raw_idx, value in (options or []):
            idx, err = parse_question_index(raw_idx, len(questions))
            if err:
                return {}, err
            answers_by_index.setdefault(idx, []).append(value)
        for raw_idx, value in (text_answers or []):
            idx, err = parse_question_index(raw_idx, len(questions))
            if err:
                return {}, err
            if idx in answers_by_index:
                return {}, f"question {idx + 1} has both --option and --text-answer"
            answers_by_index[idx] = [value]
        missing = [
            str(i + 1) for i, q in enumerate(questions)
            if q.get("question") and i not in answers_by_index
        ]
        if missing:
            return {}, "missing answer for question(s): " + ", ".join(missing)
        out: dict[str, str] = {}
        for i, vals in answers_by_index.items():
            qtext = questions[i].get("question")
            if qtext:
                out[str(qtext)] = ", ".join(v.strip() for v in vals if v.strip())
        return out, None

    answer = (text or "").strip()
    if not answer:
        return {}, None
    if len(questions) == 1:
        qtext = questions[0].get("question")
        return ({str(qtext): answer} if qtext else {}), None
    try:
        parsed = json.loads(answer)
    except ValueError:
        return {}, (
            "multiple pending questions require --option/--text-answer flags or a "
            "JSON object mapping exact question text to answer text"
        )
    if not isinstance(parsed, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in parsed.items()
    ):
        return {}, "answer JSON must be an object with string question keys and string answers"
    expected = {str(q.get("question")) for q in questions if q.get("question")}
    missing = sorted(expected - set(parsed))
    if missing:
        return {}, "answer JSON is missing question(s): " + "; ".join(missing)
    return parsed, None


def question_answer_message(answers: dict[str, str]) -> str:
    """Plain user-turn fallback for a dead AskUserQuestion host.

    Live AskUserQuestion answers go through the host's in-memory permission
    broker. If that host died, the original tool call is gone, but the
    persisted question preview still lets us carry the user's answer forward
    as a normal resumed message.
    """
    lines = ["User provided answer to the previous question:"]
    for question, answer in answers.items():
        lines.append(f"Question: {question}")
        lines.append(f"Answer: {answer}")
    return "\n".join(lines)


def elicitation_illegality(waiting_for: dict | None, short: str) -> str | None:
    """Why responding to an *elicitation* is illegal right now, or ``None``.

    The elicitation counterpart of :func:`answer_illegality`: no pending input,
    a pending input of another kind, or an elicitation CCX observed only from
    Claude's notification (no brokered hook request to release, so it can only
    be resolved in the attached terminal).
    """
    kind = pending_input_kind(waiting_for)
    if kind is None:
        return f"no pending elicitation found for {short}"
    if kind != KIND_ELICITATION:
        return (
            f"session {short} is waiting on a {kind}; use "
            f"{wrong_verb_hint(kind)} instead"
        )
    if not (waiting_for or {}).get("id"):
        return (
            f"session {short} shows an MCP elicitation dialog CCX did not broker; "
            f"resolve it in the terminal (ccx attach {short})"
        )
    return None


def _schema_properties(schema: dict | None) -> dict:
    props = (schema or {}).get("properties") if isinstance(schema, dict) else None
    return props if isinstance(props, dict) else {}


def _enum_values(spec: dict) -> list | None:
    """Allowed values of an MCP enum schema (legacy ``enum`` or titled
    ``oneOf``/``anyOf`` ``const`` options), or ``None`` when unconstrained."""
    if isinstance(spec.get("enum"), list):
        return list(spec["enum"])
    for key in ("oneOf", "anyOf"):
        options = spec.get(key)
        if isinstance(options, list):
            return [o.get("const") for o in options if isinstance(o, dict) and "const" in o]
    return None


def _field_problem(name: str, spec: dict, value) -> str | None:
    kind = spec.get("type")
    if kind == "boolean":
        return None if isinstance(value, bool) else f"field {name!r} must be a boolean"
    if kind in ("number", "integer"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return f"field {name!r} must be a {kind}"
        if kind == "integer" and not float(value).is_integer():
            return f"field {name!r} must be an integer"
        if isinstance(spec.get("minimum"), (int, float)) and value < spec["minimum"]:
            return f"field {name!r} must be >= {spec['minimum']}"
        if isinstance(spec.get("maximum"), (int, float)) and value > spec["maximum"]:
            return f"field {name!r} must be <= {spec['maximum']}"
        return None
    if kind == "array":
        if not isinstance(value, list):
            return f"field {name!r} must be a list"
        items = spec.get("items") if isinstance(spec.get("items"), dict) else {}
        allowed = _enum_values(items)
        bad = [v for v in value if allowed is not None and v not in allowed]
        if bad:
            return f"field {name!r} has value(s) outside {allowed}: {bad}"
        if isinstance(spec.get("minItems"), int) and len(value) < spec["minItems"]:
            return f"field {name!r} needs at least {spec['minItems']} item(s)"
        if isinstance(spec.get("maxItems"), int) and len(value) > spec["maxItems"]:
            return f"field {name!r} allows at most {spec['maxItems']} item(s)"
        return None
    # string (plain or enum). An unknown/absent type is treated as a string so
    # a newer schema keyword never makes a well-formed answer unsendable.
    if not isinstance(value, str):
        return f"field {name!r} must be a string"
    allowed = _enum_values(spec)
    if allowed is not None and value not in allowed:
        return f"field {name!r} must be one of {allowed}"
    if isinstance(spec.get("minLength"), int) and len(value) < spec["minLength"]:
        return f"field {name!r} must be at least {spec['minLength']} character(s)"
    if isinstance(spec.get("maxLength"), int) and len(value) > spec["maxLength"]:
        return f"field {name!r} must be at most {spec['maxLength']} character(s)"
    return None


def elicitation_response_problem(
    waiting_for: dict | None, action: str, content: dict | None,
) -> str | None:
    """Validate a response against the pending elicitation, or ``None`` if ok.

    ``decline``/``cancel`` carry no content. A form-mode ``accept`` must supply
    an object that satisfies the server's flat ``requestedSchema`` (every
    ``required`` field present, no unknown fields, primitive types, enum
    membership, and length/range bounds). A URL-mode ``accept`` only confirms
    the user finished the out-of-band flow, so it carries no content either.
    """
    if action not in ELICITATION_ACTIONS:
        return f"action must be one of {', '.join(ELICITATION_ACTIONS)}"
    if action != "accept":
        return None if not content else f"{action} does not take content"
    wf = waiting_for or {}
    if wf.get("mode") == "url":
        return None if not content else "a URL elicitation accept does not take content"
    if content is None:
        content = {}
    if not isinstance(content, dict):
        return "content must be an object"
    schema = wf.get("schema") if isinstance(wf.get("schema"), dict) else {}
    props = _schema_properties(schema)
    unknown = sorted(k for k in content if k not in props)
    if unknown:
        return "unknown field(s): " + ", ".join(unknown) + (
            f"; expected {sorted(props)}" if props else "")
    required = schema.get("required") if isinstance(schema.get("required"), list) else []
    missing = [k for k in required if k not in content]
    if missing:
        return "missing required field(s): " + ", ".join(str(k) for k in missing)
    for name, value in content.items():
        spec = props.get(name) if isinstance(props.get(name), dict) else {}
        problem = _field_problem(name, spec, value)
        if problem:
            return problem
    return None


_TRUE = {"true", "yes", "y", "1", "on"}
_FALSE = {"false", "no", "n", "0", "off"}


def elicitation_field_content(
    waiting_for: dict | None, pairs: list[tuple[str, str]],
) -> tuple[dict, str | None]:
    """Coerce CLI ``name=value`` strings into typed elicitation content.

    Each value is converted using the pending schema's declared field type
    (boolean words, numbers, integers; strings unchanged). Repeating a name
    builds a list for an array (multi-select) field. Returns ``(content,
    None)`` or ``({}, error)``; schema validation is left to
    :func:`elicitation_response_problem`.
    """
    props = _schema_properties((waiting_for or {}).get("schema"))
    out: dict = {}
    for name, raw in pairs:
        spec = props.get(name) if isinstance(props.get(name), dict) else {}
        kind = spec.get("type")
        if kind == "boolean":
            low = raw.strip().lower()
            if low in _TRUE:
                value = True
            elif low in _FALSE:
                value = False
            else:
                return {}, f"field {name!r} expects true/false, got {raw!r}"
        elif kind in ("number", "integer"):
            try:
                value = int(raw) if kind == "integer" else float(raw)
            except ValueError:
                return {}, f"field {name!r} expects a {kind}, got {raw!r}"
        else:
            value = raw
        if kind == "array":
            out.setdefault(name, []).append(value)
        elif name in out:
            return {}, f"field {name!r} given more than once"
        else:
            out[name] = value
    return out, None


# --- 3. event filtering --------------------------------------------------------

def filter_events_after(
    events: "list[Event]",
    since_epoch: str | None,
    since_seq: int | None,
) -> "list[Event]":
    """Return the events that follow the ``(since_epoch, since_seq)`` cursor.

    ``events`` is in chronological (file) order. The cursor is epoch-aware
    because ``seq`` resets per host epoch:

    * No cursor → return everything.
    * ``since_seq`` only (legacy) → plain ``seq > since_seq`` filter; correct only
      within a single epoch.
    * ``(epoch, seq)`` cursor → walk in order: skip events before the cursor's
      epoch block, keep ``> since_seq`` events within that epoch, and keep every
      event in a later epoch block. If the cursor epoch is absent from the file
      (rotated out, or unknown), replay everything rather than risk dropping new
      events — duplicates are idempotent for consumers, dropped events are not.
    """
    if since_epoch is None:
        if since_seq is None:
            return list(events)
        return [e for e in events if (e.seq or 0) > since_seq]

    if not any(e.epoch == since_epoch for e in events):
        # Cursor epoch is gone or unknown → safest is a full replay.
        return list(events)

    out: "list[Event]" = []
    seen_cursor_epoch = False
    for e in events:
        if e.epoch == since_epoch:
            seen_cursor_epoch = True
            if since_seq is None or e.seq is None or e.seq > since_seq:
                out.append(e)
        elif seen_cursor_epoch:
            # A different epoch appearing after the cursor's block is newer.
            out.append(e)
        # else: an earlier epoch (before the cursor) — already consumed, skip.
    return out


# --- 4. payload bounds ----------------------------------------------------------

def send_payload_violation(text: str, limit: int | None = None) -> tuple[int, int] | None:
    """Return ``(size, limit)`` when ``text`` exceeds the managed paste-channel
    payload bound, else ``None``. ``limit=None`` uses the live process setting
    (``ccx.constants.SEND_TEXT_MAX_BYTES``), which is unlimited by default; a
    non-positive limit disables the check. Shared by the CLI send, the client,
    and the scripted client so an explicitly configured bound behaves the same
    on every surface."""
    if limit is None:
        limit = _constants.SEND_TEXT_MAX_BYTES
    if limit <= 0:
        return None
    size = len(text.encode("utf-8", errors="ignore"))
    return (size, limit) if size > limit else None


def payload_guidance(short: str, size: int, limit: int) -> str:
    """The canonical oversize-prompt guidance string carried by
    :class:`ccx.errors.PayloadTooLarge` and the CLI error path."""
    return (f"send: prompt is {size} bytes; the configured managed paste channel "
            f"limit is {limit} bytes. Write the content to a file in the worker's "
            f"cwd and send a short pointer message instead, e.g. "
            f"`ccx send {short} \"Read task.md and do what it says\"`. "
            f"Adjust or disable the bound with CCX_SEND_MAX_BYTES.")


__all__ = [
    "QUESTION_TOOL",
    "KIND_QUESTION",
    "KIND_PERMISSION",
    "KIND_ELICITATION",
    "ELICITATION_ACTIONS",
    "REQUEST_ID_MISMATCH",
    "pending_input_kind",
    "wrong_verb_hint",
    "answer_illegality",
    "elicitation_illegality",
    "elicitation_response_problem",
    "elicitation_field_content",
    "answer_request_fence",
    "parse_question_index",
    "question_answer_map",
    "question_answer_message",
    "filter_events_after",
    "send_payload_violation",
    "payload_guidance",
]
