from __future__ import annotations

import copy
import hashlib
from contextlib import ExitStack, contextmanager
import io
import json
import os
import pathlib
import re
import stat
import time

from ccx.system import oslayer
from ccx.claude.context import compaction_unknown_context, compute_context_usage
from ccx.store.record_cache import StampedCache
from ccx.store.records import read_record


class PinnedNativeSource:
    """Retained original native FD, read with independent logical offsets.

    Darwin /dev/fd/N and dup(N) share the original file description's seek
    position. pread leaves that offset unchanged while preserving inode
    identity even if the provider path is replaced. The owner closes the FD.
    """

    def __init__(self, fd: int):
        self.fd = fd

    def size(self) -> int:
        return os.fstat(self.fd).st_size

    def open_reader(self) -> io.BufferedReader:
        held = self

        class _PositionedReader(io.RawIOBase):
            def __init__(self):
                self.position = 0

            def readable(self):
                return True

            def seekable(self):
                return True

            def tell(self):
                return self.position

            def seek(self, offset, whence=os.SEEK_SET):
                if whence == os.SEEK_CUR:
                    offset += self.position
                elif whence == os.SEEK_END:
                    offset += held.size()
                elif whence != os.SEEK_SET:
                    raise ValueError("invalid positioned source seek mode")
                if offset < 0:
                    raise ValueError("negative positioned source seek")
                self.position = offset
                return offset

            def readinto(self, target):
                data = os.pread(held.fd, len(target), self.position)
                target[:len(data)] = data
                self.position += len(data)
                return len(data)

        return io.BufferedReader(_PositionedReader())

def read_ai_title(path: str | pathlib.Path, *, max_lines: int = 100) -> str | None:
    """Return the first ai-title from the early lines of a JSONL transcript, or None.

    Claude Code writes the ``ai-title`` event before the first assistant
    response — typically around line 6 — so scanning only the first
    ``max_lines`` lines is both sufficient and cheap.  Accepts both the
    current ``aiTitle`` key and the older ``title`` key for compatibility.
    """
    path = pathlib.Path(path)
    try:
        with path.open() as f:
            for i, raw in enumerate(f):
                if i >= max_lines:
                    break
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    evt = json.loads(raw)
                except ValueError:
                    continue
                if evt.get("type") != "ai-title" or evt.get("isSidechain") is True:
                    continue
                title = evt.get("aiTitle") or evt.get("title")
                if isinstance(title, str) and title:
                    return title
    except OSError:
        pass
    return None


def _iter_transcript(
    path: str | pathlib.Path,
    stats: dict | None = None,
    *,
    after_offset: int = 0,
    before_offset: int | None = None,
    include_sidechain: bool = False,
):
    """Yield parsed JSON events from a transcript JSONL, skipping malformed lines.

    If a mutable `stats` dict is passed, each dropped malformed line increments
    `stats["malformedLines"]` so callers can surface lossy parsing. Counting is
    opt-in; existing callers that pass nothing see no behavior change.

    Subagent (`isSidechain: true`) rows are never the session's own: they are
    skipped unless `include_sidechain` is set by a reader that deliberately
    reads a subagent transcript. Current Claude writes them only to
    `<session>/subagents/` files, so a main transcript normally has none."""
    try:
        reader = (path.open_reader() if isinstance(path, PinnedNativeSource)
                  else open(path, "rb"))
        with reader as f:
            offset = max(0, int(after_offset or 0))
            try:
                size = (path.size() if isinstance(path, PinnedNativeSource)
                        else pathlib.Path(path).stat().st_size)
            except OSError:
                size = 0
            # A replaced/truncated transcript invalidates the byte cursor.
            # Never restart at zero for a fenced operation: that could
            # misattribute an older boundary to a newer compact.
            if offset > size:
                return
            f.seek(offset)
            while True:
                start_offset = f.tell()
                line = f.readline()
                if not line:
                    break
                end_offset = f.tell()
                if before_offset is not None and end_offset > before_offset:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    if (
                        not include_sidechain
                        and isinstance(event, dict)
                        and event.get("isSidechain") is True
                    ):
                        continue
                    if isinstance(event, dict):
                        event["_ccx_start_offset"] = start_offset
                        event["_ccx_end_offset"] = end_offset
                    yield event
                except (ValueError, UnicodeDecodeError):
                    if stats is not None:
                        stats["malformedLines"] = stats.get("malformedLines", 0) + 1
                    continue
    except FileNotFoundError:
        return

def _parse_event_ts_ms(evt: dict) -> int | None:
    """Pull a millisecond timestamp from a transcript event if present."""
    ts = evt.get("timestamp")
    if isinstance(ts, str):
        # ISO 8601
        try:
            import datetime as _dt
            return int(_dt.datetime.fromisoformat(ts.replace("Z","+00:00")).timestamp() * 1000)
        except Exception:
            return None
    if isinstance(ts, (int, float)):
        return int(ts if ts > 10**12 else ts * 1000)
    return None


def paste_normalized(text: str) -> str:
    """The text Claude records for a bracketed paste of ``text``.

    Claude's paste handler turns CRLF/CR into LF and expands each tab to four
    spaces before inserting a paste (2.1.294), so the prompt CCX wrote and the
    prompt Claude journals differ whenever either appears. Idempotent, so it is
    safe on both sides of a comparison.
    """
    return text.replace("\r\n", "\n").replace("\r", "\n").replace("\t", "    ")


def queued_prompt_accepted(
    transcript_path: str | pathlib.Path,
    text: str,
    *,
    after_ms: int = 0,
    after_offset: int = 0,
) -> bool:
    """Return True once Claude has natively accepted a queued prompt.

    Claude Code records text pasted while a turn is between tool steps as a raw
    JSONL `queue-operation` event before surfacing it to the model as a
    `queued_command` attachment. ccx uses this as the acknowledgement that a
    PostToolUse/SubagentStop steering paste was actually accepted, not merely
    written to the PTY.

    Only the exact normalized prompt acknowledges this delivery. Callers pass
    the transcript size from before the paste as ``after_offset``: this is
    polled while a hook holds the turn, so it must not rescan a long history.
    """
    # Claude trims a submitted prompt; an inline paste has no envelope that
    # would keep the surrounding whitespace.
    text = normalize_semantic_prompt_text(text).strip()
    if not text:
        return False
    for evt in _iter_transcript(transcript_path, after_offset=after_offset):
        if evt.get("type") != "queue-operation":
            continue
        if evt.get("operation") != "enqueue":
            continue
        content = evt.get("content")
        if not isinstance(content, str) or not prompt_matches(text, content):
            continue
        ts_ms = _parse_event_ts_ms(evt)
        if after_ms and ts_ms is not None and ts_ms < after_ms:
            continue
        return True
    return False


# --- steering delivery evidence ------------------------------------------------
#
# A steered prompt leaves several distinguishable native traces. Ranked from
# weakest to strongest placement proof:
#
#   enqueued  Claude recorded a `queue-operation`/`enqueue` for the text. The
#             TUI owns the prompt: accepted, but not yet placed in a turn.
#   started   The text appears as an ordinary user event (or a `dequeue`),
#             i.e. it will run as the next turn.
#   consumed  The text reached the model as a `queued_command` attachment on
#             the running turn — it affected the current turn.
#
# Matching is normalized equality within one event, fenced by transcript byte
# offset. Offsets (not timestamps) are the correlation key: Claude journals
# some events out of clock order, and identical prompt text may legitimately
# occur more than once in a conversation.
STEER_EVIDENCE_RANK = {"enqueued": 1, "started": 2, "consumed": 3}


def _block_text(content) -> str:
    """Flatten a message content value (string or block list) to plain text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                for key in ("text", "content"):
                    value = block.get(key)
                    if isinstance(value, str):
                        parts.append(value)
                    elif isinstance(value, list):
                        parts.append(_block_text(value))
        return "\n".join(parts)
    return ""


def _has_queued_command_marker(evt: dict) -> bool:
    """True when a user event carries Claude's queued-command attachment.

    Claude surfaces a prompt that was enqueued mid-turn to the model as a
    `queued_command` attachment rather than a fresh user turn, so this marker
    is the proof that the steering affected the turn already running.
    """
    content = (evt.get("message") or {}).get("content")
    blocks = content if isinstance(content, list) else []
    for block in blocks:
        if isinstance(block, dict) and block.get("type") == "queued_command":
            return True
    for attachment in evt.get("attachments") or []:
        if isinstance(attachment, dict) and attachment.get("type") == "queued_command":
            return True
    return False


def steer_evidence_events(
    transcript_path: str | pathlib.Path,
    text: str,
    *,
    after_offset: int = 0,
    before_offset: int | None = None,
    stats: dict | None = None,
):
    """Yield native placement evidence for one steered prompt.

    Each item is ``{"kind", "offset", "uuid", "rank"}`` in file order. Callers
    correlate by ``offset`` so two deliveries with identical text can never
    claim the same event.
    """
    # Claude trims a submitted prompt; an inline paste has no envelope that
    # would keep the surrounding whitespace.
    text = normalize_semantic_prompt_text(text).strip()
    if not text:
        return
    for evt in _iter_transcript(transcript_path, stats=stats,
                                after_offset=after_offset,
                                before_offset=before_offset):
        kind = None
        if evt.get("type") == "queue-operation":
            content = evt.get("content")
            if not isinstance(content, str) or not prompt_matches(text, content):
                continue
            operation = evt.get("operation")
            if operation == "enqueue":
                kind = "enqueued"
            elif operation == "dequeue":
                kind = "started"
            else:
                continue
        elif evt.get("type") == "user":
            body = _block_text((evt.get("message") or {}).get("content"))
            if not prompt_matches(text, body):
                continue
            kind = "consumed" if _has_queued_command_marker(evt) else "started"
        elif evt.get("type") == "attachment":
            attachment = evt.get("attachment") or {}
            if attachment.get("type") != "queued_command":
                continue
            body = _block_text(attachment.get("prompt") or attachment.get("content"))
            if not prompt_matches(text, body):
                continue
            # live Claude Code shape: a mid-turn queued message is persisted as
            # its own attachment event, not necessarily inside a user event.
            kind = "consumed"
        else:
            continue
        yield {
            "kind": kind,
            "rank": STEER_EVIDENCE_RANK[kind],
            "offset": evt.get("_ccx_end_offset"),
            "uuid": evt.get("uuid"),
        }


_PASTED_CONTENT_RE = re.compile(
    r"""^\s*<pasted_content id="([^"]+)">\n(.*)\n</pasted_content id="\1">\s*$""",
    re.DOTALL,
)


# Native queued attachments also persist the envelope's newline delimiters
# as literal escape sequences. Only unwrap a complete, same-id envelope.
_ESCAPED_PASTED_CONTENT_RE = re.compile(
    r'''^\s*<pasted_content id="([^"]+)">\\n(.*)\\n</pasted_content id="\1">\s*$''',
    re.DOTALL,
)


# Claude stores a typed `<pasted_content>` tag escaped as `<\\pasted_content>`.
_ESCAPED_TAG_RE = re.compile(r"<\\(/?pasted_content)")
# Claude turns a prompt that is only an image path into an `[Image #N]` row.
_IMAGE_PATH_RE = re.compile(r"^[/~][^\n]*\.(?:png|jpe?g|gif|webp)$", re.IGNORECASE)
_IMAGE_PLACEHOLDER_RE = re.compile(r"^\[Image #\d+\]$")


def normalize_semantic_prompt_text(value: str | None) -> str:
    """Normalize Claude's empirically observed bracketed-paste envelope and
    the edits its paste handler makes (see ``paste_normalized``)."""
    if not isinstance(value, str):
        return ""
    match = (_PASTED_CONTENT_RE.match(value)
             or _ESCAPED_PASTED_CONTENT_RE.match(value))
    if match is not None:
        value = match.group(2)
    return _ESCAPED_TAG_RE.sub(r"<\1", paste_normalized(value))


def prompt_matches(expected: str, actual: str | None) -> bool:
    """Whether native prompt text ``actual`` is the sent prompt ``expected``
    (already normalized and stripped). A bare image path also matches the
    ``[Image #N]`` placeholder Claude stores in its place."""
    actual = normalize_semantic_prompt_text(actual).strip()
    return bool(expected) and (actual == expected or bool(
        _IMAGE_PATH_RE.match(expected) and _IMAGE_PLACEHOLDER_RE.match(actual)))


def exact_user_prompt_event(
    transcript_path: str | pathlib.Path,
    text: str,
    *,
    after_offset: int = 0,
) -> dict | None:
    """Return exact ordinary-user start evidence for one semantic prompt.

    Prompts are single-entry and must not claim an unrelated prompt merely
    because it contains the same substring.
    """
    expected = normalize_semantic_prompt_text(text).strip()
    if not expected:
        return None
    for evt in _iter_transcript(transcript_path, after_offset=after_offset):
        if evt.get("type") != "user" or evt.get("isMeta"):
            continue
        content = (evt.get("message") or {}).get("content")
        if not prompt_matches(expected, _block_text(content)):
            continue
        if _has_queued_command_marker(evt):
            continue
        return {
            "uuid": evt.get("uuid"),
            "offset": evt.get("_ccx_end_offset"),
            "timestamp_ms": _parse_event_ts_ms(evt),
        }
    return None


def original_started_user_event(
    transcript_path: str | pathlib.Path,
    text: str,
    *,
    after_offset: int,
    evidence_uuid: str | None,
    evidence_offset: int | None,
) -> dict | None:
    """Reverify one hot `started` claim as the original native USER row.

    Native paste envelopes are unwrapped before exact comparison. A dequeue or an attachment
    can also be `started`/related evidence; neither is an ordinary user turn.
    Require that the journal's *same UUID and end offset* refer to a real
    non-meta, non-queued user row after the original paste fence. The caller
    must separately prove the host epoch, owner and selected native source.
    """
    if (not isinstance(evidence_uuid, str) or not evidence_uuid
            or type(evidence_offset) is not int or evidence_offset <= 0
            or type(after_offset) is not int or after_offset < 0):
        return None
    normalized = normalize_semantic_prompt_text(text).strip()
    if not normalized:
        return None
    for event in _iter_transcript(transcript_path, after_offset=after_offset):
        offset = event.get("_ccx_end_offset")
        if type(offset) is not int:
            continue
        if offset > evidence_offset:
            break
        if (offset == evidence_offset and event.get("uuid") == evidence_uuid
                and event.get("type") == "user" and not event.get("isMeta")
                and not _has_queued_command_marker(event)
                and prompt_matches(normalized, _block_text(
                    (event.get("message") or {}).get("content")))):
            return {"uuid": evidence_uuid, "offset": evidence_offset,
                    "kind": "native-started-user"}
    return None


def _is_compact_command_content(content) -> bool:
    return (
        isinstance(content, str)
        and (
            content.strip() == "/compact"
            or "<command-name>/compact<" in content
        )
    )


#: Wrappers Claude uses to journal a slash command's own output as a user event.
#: They are the command's result, never a prompt somebody submitted.
_LOCAL_COMMAND_OUTPUT_TAGS = ("<local-command-stdout>", "<local-command-stderr>")


def _is_local_command_output(content) -> bool:
    return (
        isinstance(content, str)
        and content.lstrip().startswith(_LOCAL_COMMAND_OUTPUT_TAGS)
    )


def is_compact_command_text(text) -> bool:
    """Public: does this prompt text represent a submitted ``/compact``?

    Claude records a submitted ``/compact`` as an ordinary user event, so a
    ``UserPromptSubmit`` hook fires for it exactly as it does for a real
    prompt.  Callers that must tell "our own compact command came back" from
    "somebody else's turn took the input surface" discriminate on content.
    """
    return _is_compact_command_content(text)


def foreign_prompt_since(
    transcript_path: str | pathlib.Path,
    *,
    after_offset: int = 0,
) -> bool:
    """Return True when a real, non-``/compact`` user prompt follows the fence.

    Positive proof that Ink handed its input surface to an ordinary turn.  Only
    genuine prompts count: tool results are also ``type="user"`` but carry block
    lists rather than a plain string, and meta/synthetic records are skipped.

    This is the transcript-side twin of the ``UserPromptSubmit`` discriminator.
    A host that missed the hook (restart, hook relay failure) can still reach
    the same conclusion by reading the file.

    Compaction's own bookkeeping is excluded, because callers also fence past a
    finished compact boundary to ask "did a new turn start while we were
    settling?".  Three records land there that are ``type="user"`` with plain
    string content but are not prompts: the generated summary
    (``isCompactSummary``), the ``<command-name>/compact`` echo, and the
    ``<local-command-stdout>Compacted…`` result.  Counting any of them as a
    foreign turn would report a busy worker after every single compaction.
    """
    for evt in _iter_transcript(transcript_path, after_offset=after_offset):
        if evt.get("type") != "user" or evt.get("isMeta"):
            continue
        if evt.get("isCompactSummary"):
            continue
        content = (evt.get("message") or {}).get("content")
        if not isinstance(content, str) or not content.strip():
            continue
        if _is_compact_command_content(content):
            continue
        if _is_local_command_output(content):
            continue
        return True
    return False


def compact_command_seen_since(
    transcript_path: str | pathlib.Path,
    *,
    after_ms: int = 0,
    after_offset: int = 0,
) -> bool:
    """Return True once the transcript records a submitted ``/compact`` command.

    Submission acknowledgement only — the *result* (a ``compact_boundary`` or a
    "Not enough messages to compact" stdout) can arrive minutes later; see
    :func:`compact_result_since` for that. Claude Code logs a submitted slash
    command as a user event whose content is the literal command text and/or a
    ``<command-name>`` local-command record.
    """
    for evt in _iter_transcript(transcript_path, after_offset=after_offset):
        content = None
        if evt.get("type") == "system" and evt.get("subtype") == "local_command":
            content = evt.get("content")
        elif evt.get("type") == "user":
            content = (evt.get("message") or {}).get("content")
        if not _is_compact_command_content(content):
            continue
        ts_ms = _parse_event_ts_ms(evt)
        if after_ms and ts_ms is not None and ts_ms < after_ms:
            continue
        return True
    return False


def pending_compact_request(
    transcript_path: str | pathlib.Path, *, before_offset: int | None = None,
) -> dict | None:
    """Return the latest submitted ``/compact`` that has no terminal result.

    File order is authoritative. Compaction timestamps can run slightly
    backwards around the boundary because Claude journals the generated summary
    and preserved messages together. A successful boundary or either
    local-command wrapper settles the pending request: stdout covers command
    replay and harmless no-ops, stderr covers failures. Both are matched on the
    wrapper alone, without parsing the human text inside it — settling here only
    means "the command finished", and :func:`compact_result_since` is what reads
    the verdict.
    """
    pending = None
    for evt in _iter_transcript(transcript_path, before_offset=before_offset):
        if evt.get("type") == "system" and evt.get("subtype") == "compact_boundary":
            pending = None
            continue

        content = None
        if evt.get("type") == "system" and evt.get("subtype") == "local_command":
            content = evt.get("content")
        elif evt.get("type") == "user":
            content = (evt.get("message") or {}).get("content")

        if _is_compact_command_content(content):
            pending = {"as_of_ms": _parse_event_ts_ms(evt)}
            continue
        if (
            pending is not None
            and isinstance(content, str)
            and (
                "<local-command-stdout>" in content
                or "<local-command-stderr>" in content
            )
        ):
            pending = None
    return pending


# A failed ``/compact`` is reported as local-command stderr. Two shapes reach it:
# Claude Code's own compaction errors ("Error during compaction: <detail>",
# "Compaction failed \xb7 <why>"), and a rethrow whose String(Error) adds a
# leading "Error: ". Both are matched on the compaction markers rather than on
# the stderr wrapper alone, so an unrelated slash command's stderr inside the
# fence window can never be mistaken for a compact verdict.
_COMPACT_STDERR_RE = re.compile(
    r"<local-command-stderr>(.*?)</local-command-stderr>", re.DOTALL,
)
# anchored at the start of the stderr payload, never merely contained in it.
# `failed` is monotonic, so a false positive is unrepairable: it would terminally
# fail an operation that may still be running, and no later boundary could undo
# it. A message that merely mentions compaction in passing must not qualify.
_COMPACT_FAILURE_RE = re.compile(
    r"^(?:Error:\s*)?(?:Error during compaction:|Compaction failed(?:\s|$))",
)

# When compaction fails because of an API error, Claude Code embeds the already
# formatted message rather than structured fields, so everything must be read
# back out of the prose — unlike the turn path, which reads `apiErrorStatus` and
# `error` directly.
_API_ERROR_RE = re.compile(r"\bAPI Error(?:\s*\([^)]*\))?:")
_API_ERROR_STATUS_RE = re.compile(r"API Error(?:\s*\([^)]*\))?:\s*(\d{3})\b")
_REPEATED_OVERLOAD_RE = re.compile(r"Repeated (\d{3}) Overloaded errors")

# Claude Code writes this phrase on exactly the branches its own rule calls
# transient (5xx, repeated 529, lost connection), so it is evidence rather than
# a guess — which matters for the failures that carry no status at all, such as
# a stream dying after a 200.
_TRANSIENT_PHRASE = "usually temporary"


def _compact_failure_from_content(content: str) -> dict | None:
    """Return the failure verdict carried by a local-command stderr record."""
    match = _COMPACT_STDERR_RE.search(content)
    if match is None:
        return None
    message = match.group(1).strip()
    if _COMPACT_FAILURE_RE.match(message) is None:
        return None
    # `String(err)` on the rethrow path prefixes the class name; the message
    # underneath is the part a human needs.
    if message.startswith("Error: "):
        message = message[len("Error: "):]

    if _API_ERROR_RE.search(message) is None:
        # Compaction failed on its own terms — the conversation could not be
        # reduced, media was unstrippable. Nothing to attribute to the API, and
        # retrying will fail the same way.
        return {"message": message, "api_error": None}

    status = None
    status_match = _API_ERROR_STATUS_RE.search(message) or \
        _REPEATED_OVERLOAD_RE.search(message)
    if status_match is not None:
        status = int(status_match.group(1))
    # The API cause is recorded even when no status could be recovered; dropping
    # it there would silently downgrade a retryable server fault to "compaction
    # is impossible".
    return {
        "message": message,
        "api_error": {
            "status": status,
            "error": None,
            "request_id": None,
            "text": message,
            "ts": None,
            "transient": (
                (status is not None and status >= 500)
                or _TRANSIENT_PHRASE in message.lower()
            ),
            "rate_limited": status == 429,
            "reset_text": _reset_text_from(message) if status == 429 else None,
        },
    }


def compact_result_since(
    transcript_path: str | pathlib.Path,
    *,
    after_ms: int = 0,
    after_offset: int = 0,
    before_offset: int | None = None,
    bounded_manual: bool = False,
    require_command: bool = False,
) -> dict | None:
    """Return a native ``/compact`` result after a time or byte fence.

    Claude Code records ``/compact`` as a local slash-command, not as a normal
    user prompt/assistant turn, so all three outcomes are local-command records
    rather than turn events:

    * success — a ``system`` event with ``subtype="compact_boundary"``;
    * harmless no-op ("Not enough messages to compact") — local-command stdout;
    * failure — local-command **stderr**, which is the only trace a failed
      compaction leaves. Claude Code does not retry compaction, so a failure here
      is terminal for the operation even though the worker itself is healthy.

    ``after_offset`` is the production correlation fence; file order remains
    authoritative even when Claude journals timestamps out of order.
    """
    command_seen = False
    stats: dict = {}
    for evt in _iter_transcript(transcript_path, stats, after_offset=after_offset,
                                before_offset=before_offset):
        if bounded_manual and (stats.get("malformedLines") or not isinstance(evt, dict)):
            return None
        if bounded_manual:
            content = (evt.get("message") or {}).get("content") if evt.get("type") == "user" else evt.get("content")
            if _is_compact_command_content(content):
                # Claude 2.1.312 journals a pasted `/compact` twice: the bare
                # prompt text, then its `<command-name>` wrapper. That pair is
                # one submission; any further command record is another one.
                wrapper = "<command-name>/compact<" in content
                if command_seen == "wrapper" or (command_seen == "bare" and not wrapper):
                    break
                command_seen = "wrapper" if wrapper else "bare"
                continue
            if (
                evt.get("type") == "user" and not evt.get("isMeta")
                and not evt.get("isCompactSummary")
                and isinstance(content, str) and content.strip()
                and not _is_local_command_output(content)
            ):
                break
            if require_command and not command_seen:
                continue
        ts_ms = _parse_event_ts_ms(evt)
        if after_ms and ts_ms is not None and ts_ms < after_ms:
            continue

        if evt.get("type") == "system" and evt.get("subtype") == "compact_boundary":
            meta = evt.get("compactMetadata") or {}
            if bounded_manual and meta.get("trigger") != "manual":
                # A different compaction closes this attribution window.
                return None
            return {
                "ok": True,
                "compacted": True,
                "message": evt.get("content") or "Conversation compacted",
                "api_error": None,
                "pre_tokens": meta.get("preTokens"),
                "post_tokens": meta.get("postTokens"),
                "duration_ms": meta.get("durationMs"),
                "trigger": meta.get("trigger"),
                "timestamp": ts_ms,
                "event_uuid": evt.get("uuid"),
                "end_offset": evt.get("_ccx_end_offset"),
            }

        content = None
        if evt.get("type") == "system" and evt.get("subtype") == "local_command":
            content = evt.get("content")
        elif evt.get("type") == "user":
            content = (evt.get("message") or {}).get("content")
        if not isinstance(content, str):
            continue
        if bounded_manual and not (
            (evt.get("type") == "system" and evt.get("subtype") == "local_command")
            or _is_local_command_output(content)
        ):
            continue
        if "Not enough messages to compact" in content:
            return {
                "ok": True,
                "compacted": False,
                "message": "Not enough messages to compact.",
                "api_error": None,
                "pre_tokens": None,
                "post_tokens": None,
                "duration_ms": None,
                "trigger": "manual",
                "timestamp": ts_ms,
                "event_uuid": evt.get("uuid"),
                "end_offset": evt.get("_ccx_end_offset"),
            }
        failure = _compact_failure_from_content(content)
        if failure is not None:
            return {
                "ok": False,
                "compacted": False,
                "message": failure["message"],
                "api_error": failure["api_error"],
                "pre_tokens": None,
                "post_tokens": None,
                "duration_ms": None,
                "trigger": "manual",
                "timestamp": ts_ms,
                "event_uuid": evt.get("uuid"),
                "end_offset": evt.get("_ccx_end_offset"),
            }
    return None


# Claude Code's own transience rule, mirrored from the 2.1.x bundle:
#
#     isTransient = apiErrorIsTransient === true
#                   || error === "overloaded" || error === "server_error"
#
# "Transient" means the failure is a property of the server or the connection at
# that instant rather than of the request, so the identical call could succeed
# later. It is not a retry instruction. Claude Code has already spent its own
# retry budget before any error reaches the transcript (roughly ten attempts over
# several minutes, honoring retry-after), so CCX never retries on top of that; it
# propagates the classification and lets the caller decide whether coming back
# later is meaningful at all.
TRANSIENT_API_ERROR_KINDS = frozenset({"overloaded", "server_error"})

# Rate limiting is deliberately not folded into "transient". Retrying is
# meaningful, but only after a reset the caller does not control, and it is an
# account condition rather than a server fault — the two need different handling,
# so callers must be able to tell them apart without matching on prose.
#
# The flags are independent, not exclusive: Claude Code marks a 429 that is a
# capacity throttle (rather than a quota wall) with apiErrorIsTransient, so
# `transient and rate_limited` together means "throttled, worth retrying soon",
# while `rate_limited` alone means "quota exhausted until reset".
RATE_LIMIT_API_ERROR_KIND = "rate_limit"

# The reset wording inside a rate-limit message, e.g.
#   "You've hit your session limit \xb7 resets 8:40pm (Europe/Madrid)"
# Captured verbatim and never parsed into a timestamp: this is human prose whose
# format is not a contract, and a parser would fail silently when it drifts.
_RESET_TEXT_RE = re.compile(r"\bresets\b[^·\n]*")


def _reset_text_from(text: str) -> str | None:
    match = _RESET_TEXT_RE.search(text)
    return match.group(0).strip() if match else None


# Prefixes Claude Code has already applied to its own message. Stripping them
# keeps the composed reason from reading "…overloaded (529): API Error: 529
# Overloaded…", which says the same thing three times.
_REDUNDANT_MESSAGE_PREFIXES = ("Error during compaction: ", "API Error: ")


def api_error_reason(api_error: dict) -> str:
    """One human line describing an API failure, for logs and lifecycle state.

    Shape: ``Claude API <label>[ (<status>)]: <Claude's message>``. CCX writes
    only the head; the sentence after it is Claude's own, so the wording cannot
    drift out of step with what Claude actually reports.

    The one edit is dropping prefixes Claude has *already* applied — the head
    restates them, and left in place the line reads "…overloaded (529): API
    Error: 529 Overloaded…". This composes a display string only: the untouched
    original always remains available as the classification's ``text``.
    """
    kind = api_error.get("error")
    status = api_error.get("status")
    if api_error.get("rate_limited"):
        label = "rate limit"
    elif status == 529 or kind == "overloaded":
        label = "overloaded"
    elif kind:
        label = str(kind).replace("_", " ")
    else:
        label = "error"

    message = (api_error.get("text") or "").strip()
    changed = True
    while changed:
        changed = False
        for prefix in _REDUNDANT_MESSAGE_PREFIXES:
            if message.startswith(prefix):
                message = message[len(prefix):].lstrip()
                changed = True

    head = f"Claude API {label}"
    if isinstance(status, int):
        head = f"{head} ({status})"
    return f"{head}: {message}" if message else head


def api_error_from_event(evt: dict) -> dict | None:
    """Classify Claude Code's synthetic API-error message for one raw event.

    Claude may write a terminal API/server error to the raw transcript without
    firing the Stop hook. The raw event carries explicit markers; the cleaned
    transcript intentionally drops them, so lifecycle code needs this raw-event
    helper.

    ``error`` is Claude Code's own error class (``server_error``, ``rate_limit``,
    ``authentication_failed``, ...) and ``status`` the HTTP status — absent for
    failures that have none, such as a stream that dies after a 200. ``text`` is
    Claude's message verbatim: it is already written for a human, so CCX reuses
    it rather than inventing parallel prose that would drift out of date.
    """
    if evt.get("type") != "assistant" or evt.get("isApiErrorMessage") is not True:
        return None
    msg = evt.get("message") or {}
    text_parts = []
    for block in msg.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text")
            if isinstance(text, str) and text:
                text_parts.append(text)
    text = "\n".join(text_parts).strip()
    kind = evt.get("error")
    rate_limited = kind == RATE_LIMIT_API_ERROR_KIND
    return {
        "status": evt.get("apiErrorStatus"),
        "error": kind,
        "request_id": evt.get("requestId"),
        "text": text,
        "ts": _parse_event_ts_ms(evt),
        "transient": (
            evt.get("apiErrorIsTransient") is True
            or kind in TRANSIENT_API_ERROR_KINDS
        ),
        "rate_limited": rate_limited,
        "reset_text": _reset_text_from(text) if rate_limited else None,
    }


# Background-task launch result content, e.g.
#   "Command running in background with ID: b2e5smis8. Output is being
#    written to: /tmp/.../tasks/b2e5smis8.output. You will be notified ..."
_BG_LAUNCH_ID_RE = re.compile(r"running in background with ID:\s*([A-Za-z0-9_-]+)")
_BG_AUTO_LAUNCH_ID_RE = re.compile(
    r"moved to the background \(ID:\s*([A-Za-z0-9_-]+)\)",
)
_BG_OUTPUT_FILE_RE = re.compile(r"written to:\s*(\S+)")
_MONITOR_LAUNCH_ID_RE = re.compile(r"Monitor started \(task\s+([A-Za-z0-9_-]+)")
# Async Agent launch result content, e.g.
#   "Async agent launched successfully. ...\nagentId: aba8230838a028bad (internal
#    ID ...)\n...\noutput_file: /tmp/.../tasks/aba8230838a028bad.output\n..."
_ASYNC_AGENT_LAUNCH_PREFIX = "Async agent launched successfully"
_ASYNC_AGENT_ID_RE = re.compile(r"agentId:\s*([A-Za-z0-9_-]+)")
_ASYNC_AGENT_OUTPUT_FILE_RE = re.compile(r"output_file:\s*(\S+)")
# Exit code embedded in a completion summary, e.g. "(exit code 0)".
_BG_EXIT_CODE_RE = re.compile(r"exit code:?\s*(\d+)")
# Notification metadata precedes the first free-text field; an agent <result>
# may quote another notification, so task ids are only read before it.
_TASK_NOTIFICATION_TEXT_TAG_RE = re.compile(r"<(?:summary|note|result|event)>")
_TASK_ID_TAG_RE = re.compile(r"<task-id>([^<]*)</task-id>")
_TASK_LAST_EVENT_LIMIT = 1000


def _xml_tag(content: str, name: str) -> str | None:
    """Return the inner text of the first <name>…</name> in `content`, or None."""
    m = re.search(rf"<{name}>(.*?)</{name}>", content, re.DOTALL)
    return m.group(1).strip() if m else None


def _parse_task_notification(content) -> dict | None:
    """Parse a Claude Code `<task-notification>` enqueue payload.

    The harness writes one when a background task or async agent finishes (as
    a `queue-operation`/enqueue whose content the worker later auto-continues
    on). `task_ids` lists every header `<task-id>`: a resumed session's
    "previous session" sweep names several agents in one notification. Returns
    the matched fields, or None when `content` is not a task notification.
    `exit_code` is parsed from the summary when present."""
    if not isinstance(content, str) or "<task-notification>" not in content:
        return None
    head = content[:content.index("<task-notification>")]
    rest = content[len(head):]
    m = _TASK_NOTIFICATION_TEXT_TAG_RE.search(rest)
    header = rest[:m.start()] if m else rest
    task_ids = [t.strip() for t in _TASK_ID_TAG_RE.findall(header) if t.strip()]
    summary = _xml_tag(content, "summary")
    exit_code = None
    if summary:
        m = _BG_EXIT_CODE_RE.search(summary)
        if m:
            exit_code = int(m.group(1))
    status = _xml_tag(content, "status")
    return {
        "task_ids": task_ids,
        "tool_use_id": _xml_tag(content, "tool-use-id"),
        "status": status.lower() if status else None,
        "summary": summary,
        "event": _xml_tag(content, "event"),
        "output_file": _xml_tag(content, "output-file"),
        "exit_code": exit_code,
    }


# Background-task statuses ccx classifies a task into. Anything a notification
# reports that isn't clearly terminal becomes "unknown"; a launched task with
# no notification yet is "running" (not a new lifecycle state — an open
# background task does not mean the worker is unfinished).
_BG_STATUS_RUNNING = "running"
_BG_STATUS_COMPLETED = "completed"
_BG_STATUS_FAILED = "failed"
_BG_STATUS_STOPPED = "stopped"
_BG_STATUS_UNKNOWN = "unknown"
# Claude's raw terminal notification statuses. `killed` (TaskStop, session
# exit) is reported as `stopped`, matching the successful TaskStop result that
# accompanies it, so the normalized vocabulary stays closed.
_BG_RAW_TERMINAL_STATUS = {
    "completed": _BG_STATUS_COMPLETED,
    "failed": _BG_STATUS_FAILED,
    "stopped": _BG_STATUS_STOPPED,
    "killed": _BG_STATUS_STOPPED,
}


def _task_kind(tool_name: str | None) -> str:
    if tool_name in ("Agent", "Task"):
        return "subagent"
    if tool_name == "Monitor":
        return "monitor"
    return "bash"


def _task_terminal_status(raw_status: str | None, exit_code: int | None) -> str | None:
    if raw_status in _BG_RAW_TERMINAL_STATUS:
        return _BG_RAW_TERMINAL_STATUS[raw_status]
    if exit_code is not None:
        return _BG_STATUS_COMPLETED if exit_code == 0 else _BG_STATUS_FAILED
    return None


def _async_agent_launch(result: dict, tur, body) -> dict | None:
    """Launch facts when an Agent/Task tool_result started async work.

    Claude's launch input flag is unreliable (absent, the string "true", or
    True), so only the result counts. Structured `toolUseResult` decides when
    present — `status:"async_launched"`, or `isAsync` with no status; any other
    status is a synchronous run. Otherwise fall back to the result text."""
    if result.get("is_error") is True:
        return None
    if isinstance(tur, dict) and ("status" in tur or "isAsync" in tur):
        status = tur.get("status")
        agent_id = tur.get("agentId")
        is_async = status == "async_launched" or (
            status is None and tur.get("isAsync") is True)
        if not (is_async and isinstance(agent_id, str) and agent_id):
            return None
        output_file = tur.get("outputFile")
        description = tur.get("description")
        return {
            "task_id": agent_id,
            "output_file": output_file if isinstance(output_file, str) else None,
            "description": description if isinstance(description, str) else None,
        }
    if not isinstance(body, str) or not body.lstrip().startswith(_ASYNC_AGENT_LAUNCH_PREFIX):
        return None
    m = _ASYNC_AGENT_ID_RE.search(body)
    if not m:
        return None
    mo = _ASYNC_AGENT_OUTPUT_FILE_RE.search(body)
    return {
        "task_id": m.group(1),
        "output_file": mo.group(1) if mo else None,
        "description": None,
    }


def _clip_task_event(text: str | None) -> str | None:
    if text is None:
        return None
    return text if len(text) <= _TASK_LAST_EVENT_LIMIT else text[:_TASK_LAST_EVENT_LIMIT] + "..."


def extract_background_tasks(transcript_path: str | pathlib.Path) -> list[dict]:
    """Return transcript-derived background tasks for a session, oldest first.

    A background task is launched explicitly by a `Bash` tool_use with
    `run_in_background: true`, implicitly when Claude moves a foreground Bash
    command to the background after its timeout, by a `Monitor`, or by an
    `Agent`/`Task` call whose result reports an async launch. Its launching
    tool_result carries a stable task id (`backgroundTaskId`, Monitor
    `taskId`, or `agentId`). When the work ends the harness enqueues a
    `<task-notification>` keyed by that id (and usually a tool-use id). A
    successful `TaskStop` result is separate terminal evidence because Claude
    does not always enqueue a notification for it. A successful `SendMessage`
    that resumes an agent (`resumedAgentId`) reopens its task.
    An eager Bash/Monitor launch whose result has no task id is stopped;
    without a result it remains pending/running. Monitor expiry/timeout event
    notices also stop the task, including persistent Monitors.

    Evidence pairs with a task by its task id; the tool-use id is a fallback
    only when the evidence names no known task. Evidence is folded in
    transcript order, so a terminal notification settles a task and a later
    resume reopens it. A resumed agent's notifications carry the resuming
    SendMessage's tool-use id; a terminal record naming a superseded
    invocation of that agent is ignored.

    Each task dict: `task_id`, `tool_use_id`, `kind` (bash/monitor/subagent),
    `command`, `description`, `output_file`, `status`
    (running/completed/failed/stopped/unknown), `exit_code`, `summary`,
    `launched_at`, `completed_at` (ms timestamps when available). Claude's
    `killed` status is reported as `stopped`.

    Pure-transcript and read-only — never blocks, and an open task is reported
    as `running`, never as a worker-level lifecycle state. Only work evidenced
    in this transcript counts: background work started inside a subagent is
    not visible here. Claude Code appends resume catch-up notifications to the
    same raw transcript today; if a future version splits resumed sessions
    across raw files, an open prior task may remain transcript-running until
    higher-level liveness marks it stale.
    Returns [] when no background tasks (or no transcript) are present."""
    reducer = BackgroundTaskReducer()
    for event in _iter_transcript(transcript_path):
        reducer.feed(event)
    return reducer.tasks()


class BackgroundTaskReducer:
    """Transcript-only task fold: ingest appended rows, snapshot current tasks.

    Both historical extraction and the host incremental tail use this one
    parser. Candidate/results/evidence maps persist; materializing a snapshot
    folds only task evidence, never unrelated transcript history.
    """

    def __init__(self):
        self.candidates: dict[str, dict] = {}    # possible launch tool_use_id -> facts
        self.launches: dict[str, dict] = {}      # proven launch tool_use_id -> facts
        self.results: dict[str, dict] = {}       # launch tool_use_id -> task result facts
        self.stop_requests: dict[str, str] = {}  # TaskStop tool_use_id -> requested task id
        self.resume_requests: set[str] = set()   # SendMessage tool_use_ids
        # Terminal, Monitor-event and reopen evidence, appended in transcript order.
        self.evidence: list[dict] = []

    def feed(self, evt: dict) -> bool:
        """Ingest one row; tell the owner whether background facts might change."""
        candidates = self.candidates
        launches = self.launches
        results = self.results
        stop_requests = self.stop_requests
        resume_requests = self.resume_requests
        evidence = self.evidence
        t = evt.get("type")
        ts = _parse_event_ts_ms(evt)
        if t == "assistant":
            msg = evt.get("message") or {}
            for c in (msg.get("content") or []):
                if not isinstance(c, dict) or c.get("type") != "tool_use":
                    continue
                inp = c.get("input")
                if not isinstance(inp, dict):
                    continue
                name = c.get("name")
                tid = c.get("id")
                if not tid:
                    continue
                if name == "TaskStop":
                    requested_id = inp.get("task_id")
                    if isinstance(requested_id, str) and requested_id:
                        stop_requests[tid] = requested_id
                    continue
                if name == "SendMessage":
                    resume_requests.add(tid)
                    continue
                is_agent = name in ("Agent", "Task")
                is_monitor = name == "Monitor"
                # Agent launches are proven by their result, never the flag.
                is_bg = inp.get("run_in_background") is True and not is_agent
                is_bash = name == "Bash"
                if not (is_bash or is_bg or is_monitor or is_agent):
                    continue
                candidate = {
                    "tool_use_id": tid,
                    "kind": _task_kind(name),
                    "command": inp.get("command") if isinstance(inp.get("command"), str) else None,
                    "description": inp.get("description") if isinstance(inp.get("description"), str) else None,
                    "persistent": inp.get("persistent") if is_monitor else None,
                    "timeout_ms": inp.get("timeout_ms") if is_monitor else None,
                    "launched_at": ts,
                    "last_open_offset": evt.get("_ccx_start_offset"),
                }
                candidates[tid] = candidate
                if is_bg or is_monitor:
                    launches[tid] = candidate
        elif t == "user":
            msg = evt.get("message") or {}
            content = msg.get("content")
            if not isinstance(content, list):
                return False
            tool_results = [c for c in content
                            if isinstance(c, dict) and c.get("type") == "tool_result"]
            tur = evt.get("toolUseResult")
            # toolUseResult is a per-event sibling; only trust it to label a
            # tool_result when this event carries exactly one (the common shape).
            single_tur = tur if len(tool_results) == 1 else None
            bg_id = None
            monitor_id = monitor_timeout_ms = monitor_persistent = None
            if isinstance(single_tur, dict):
                bg_id = single_tur.get("backgroundTaskId")
                monitor_id = single_tur.get("taskId")
                monitor_timeout_ms = single_tur.get("timeoutMs")
                monitor_persistent = single_tur.get("persistent")
            for c in tool_results:
                tid = c.get("tool_use_id")
                if not tid:
                    continue
                body = _format_tool_result_content(c.get("content"))

                requested_stop_id = stop_requests.get(tid)
                if requested_stop_id is not None:
                    stop_result = single_tur if isinstance(single_tur, dict) else None
                    stopped_id = stop_result.get("task_id") if stop_result else None
                    stop_message = stop_result.get("message") if stop_result else None
                    stopped_command = stop_result.get("command") if stop_result else None
                    if (
                        c.get("is_error") is not True
                        and stopped_id == requested_stop_id
                        and isinstance(stop_message, str)
                        and stop_message.startswith("Successfully stopped task:")
                    ):
                        evidence.append({
                            "task_id": stopped_id,
                            "tool_use_id": tid,
                            "status": _BG_STATUS_STOPPED,
                            "summary": stop_message,
                            "event": None,
                            "output_file": None,
                            "exit_code": None,
                            "command": (
                                stopped_command
                                if isinstance(stopped_command, str)
                                else None
                            ),
                            "completed_at": ts,
                        })
                    continue

                if tid in resume_requests:
                    resume = single_tur if isinstance(single_tur, dict) else {}
                    resumed_id = resume.get("resumedAgentId")
                    if (
                        c.get("is_error") is not True
                        and resume.get("success") is True
                        and isinstance(resumed_id, str)
                        and resumed_id
                    ):
                        evidence.append({
                            "task_id": resumed_id,
                            "tool_use_id": tid,
                            "reopen": True,
                            "reopened_at": ts,
                            "last_open_offset": evt.get("_ccx_start_offset"),
                        })
                    continue

                if (candidates.get(tid) or {}).get("kind") == "subagent":
                    agent = _async_agent_launch(c, single_tur, body)
                    if agent is not None:
                        launches[tid] = candidates[tid]
                        results[tid] = {
                            "task_id": agent["task_id"],
                            "output_file": agent["output_file"],
                            "description": agent["description"],
                            "result_at": ts,
                            "last_open_offset": evt.get("_ccx_start_offset"),
                        }
                    continue

                parsed_id = output_file = None
                if isinstance(body, str):
                    m = (
                        _BG_LAUNCH_ID_RE.search(body)
                        or _BG_AUTO_LAUNCH_ID_RE.search(body)
                    )
                    if m:
                        parsed_id = m.group(1)
                    mo = _BG_OUTPUT_FILE_RE.search(body)
                    if mo:
                        output_file = mo.group(1).rstrip(".")
                parsed_monitor_id = None
                if isinstance(body, str) and body.startswith("Monitor started (task "):
                    mm = _MONITOR_LAUNCH_ID_RE.search(body)
                    if mm:
                        parsed_monitor_id = mm.group(1)
                launch = launches.get(tid) or {}
                is_monitor_result = launch.get("kind") == "monitor" or parsed_monitor_id is not None
                task_id = bg_id or parsed_id
                if is_monitor_result:
                    task_id = monitor_id or parsed_monitor_id or task_id
                # Only record results that are genuinely background launches.
                if task_id is None:
                    if launch.get("kind") in ("bash", "monitor"):
                        evidence.append({
                            "tool_use_id": tid,
                            "status": _BG_STATUS_STOPPED,
                            "completed_at": ts,
                            "summary": _clip_task_event(body),
                        })
                    continue
                if tid in candidates:
                    launches[tid] = candidates[tid]
                res = {
                    "task_id": task_id,
                    "output_file": output_file,
                    "result_at": ts,
                    "last_open_offset": evt.get("_ccx_start_offset"),
                }
                if is_monitor_result:
                    if monitor_timeout_ms is not None:
                        res["timeout_ms"] = monitor_timeout_ms
                    if monitor_persistent is not None:
                        res["persistent"] = monitor_persistent
                results[tid] = res
        elif t == "queue-operation" and evt.get("operation") == "enqueue":
            parsed = _parse_task_notification(evt.get("content"))
            if parsed:
                task_ids = parsed.pop("task_ids")
                # A multi-id sweep names no single launching tool use.
                if len(task_ids) > 1:
                    parsed["tool_use_id"] = None
                for task_id in task_ids or [None]:
                    evidence.append({
                        **parsed, "task_id": task_id, "completed_at": ts,
                        "last_open_offset": evt.get("_ccx_start_offset"),
                    })
            return parsed is not None
        if t == "user":
            return bool(tool_results)
        if t == "assistant":
            return any(
                isinstance(part, dict) and part.get("type") == "tool_use"
                and part.get("name") in {"Bash", "Monitor", "Agent", "Task",
                                        "TaskStop", "SendMessage"}
                for part in (evt.get("message") or {}).get("content") or []
            )
        return False

    def tasks(self) -> list[dict]:
        candidates = self.candidates
        launches = self.launches
        results = self.results
        stop_requests = self.stop_requests
        resume_requests = self.resume_requests
        evidence = self.evidence
        # Task identity: launch tool_use_id, with its task id as the pairing key.
        # A resume of an agent never launched in this transcript is still live
        # work; it becomes a task keyed by the resuming SendMessage.
        task_keys = set(launches) | set(results)
        launches_by_task: dict[str, list[str]] = {}
        for tid in sorted(task_keys):
            task_id = (results.get(tid) or {}).get("task_id")
            if task_id:
                launches_by_task.setdefault(task_id, []).append(tid)
        for rec in evidence:
            if rec.get("reopen") and rec["task_id"] not in launches_by_task:
                tid = rec["tool_use_id"]
                launches[tid] = {
                    "tool_use_id": tid,
                    "kind": "subagent",
                    "launched_at": rec.get("reopened_at"),
                    "last_open_offset": rec.get("last_open_offset"),
                }
                results[tid] = {"task_id": rec["task_id"]}
                launches_by_task[rec["task_id"]] = [tid]
                task_keys.add(tid)

        def _evidence_targets(rec: dict) -> list[str]:
            task_id = rec.get("task_id")
            if task_id and task_id in launches_by_task:
                return launches_by_task[task_id]
            tool_use_id = rec.get("tool_use_id")
            if tool_use_id in task_keys:
                launched_id = (results.get(tool_use_id) or {}).get("task_id")
                # Never let a tool-use id contradict the task id it launched.
                if not (task_id and launched_id and task_id != launched_id):
                    return [tool_use_id]
            return []

        evidence_by_launch: dict[str, list[dict]] = {}
        for rec in evidence:
            for target in _evidence_targets(rec):
                evidence_by_launch.setdefault(target, []).append(rec)

        tasks: list[dict] = []
        for tid in sorted(task_keys):
            launch = launches.get(tid) or {}
            res = results.get(tid) or {}
            kind = launch.get("kind") or "bash"
            task_id = res.get("task_id")

            output_file = res.get("output_file")
            last_open_offset = res.get("last_open_offset", launch.get("last_open_offset"))
            terminal = None
            event_count = 0
            last_event = last_event_at = None
            superseded: set[str] = set()  # earlier invocations of a resumed agent
            invocation = tid
            for rec in evidence_by_launch.get(tid, []):
                if rec.get("reopen"):
                    if kind == "subagent":
                        superseded.add(invocation)
                        invocation = rec["tool_use_id"]
                        terminal = None
                        last_open_offset = rec.get("last_open_offset", last_open_offset)
                    continue
                ev_text = rec.get("event")
                if ev_text is not None:
                    event_count += 1
                    if kind == "monitor" and terminal is None:
                        last_open_offset = rec.get("last_open_offset", last_open_offset)
                    last_event = _clip_task_event(ev_text)
                    last_event_at = rec.get("completed_at")
                    if kind == "monitor" and ev_text.startswith(
                        ("[Monitor expired", "[Monitor timed out")
                    ):
                        terminal = {**rec, "status": _BG_STATUS_STOPPED,
                                    "summary": _clip_task_event(ev_text)}
                    if rec.get("status") is None:
                        continue
                if rec.get("tool_use_id") in superseded:
                    continue
                if _task_terminal_status(rec.get("status"), rec.get("exit_code")):
                    terminal = rec

            exit_code = completed_at = summary = None
            if terminal:
                exit_code = terminal.get("exit_code")
                status = _task_terminal_status(terminal.get("status"), exit_code) or _BG_STATUS_UNKNOWN
                completed_at = terminal.get("completed_at")
                summary = terminal.get("summary")
                output_file = output_file or terminal.get("output_file")
                task_id = task_id or terminal.get("task_id")
            else:
                status = _BG_STATUS_RUNNING

            task = {
                "task_id": task_id,
                "tool_use_id": tid,
                "kind": kind,
                "command": launch.get("command") or (terminal or {}).get("command"),
                "description": launch.get("description") or res.get("description"),
                "output_file": output_file,
                "status": status,
                "exit_code": exit_code,
                "summary": summary,
                "launched_at": launch.get("launched_at"),
                "completed_at": completed_at,
                "last_open_offset": last_open_offset,
            }
            if kind == "monitor":
                task["persistent"] = (
                    res.get("persistent")
                    if res.get("persistent") is not None
                    else launch.get("persistent")
                )
                task["timeout_ms"] = (
                    res.get("timeout_ms")
                    if res.get("timeout_ms") is not None
                    else launch.get("timeout_ms")
                )
                task["event_count"] = event_count
                task["last_event"] = last_event
                task["last_event_at"] = last_event_at
            tasks.append(task)

        tasks.sort(key=lambda t: (t.get("launched_at") or 0, t.get("tool_use_id") or ""))
        return tasks


def read_background_append(
    path: str | pathlib.Path | PinnedNativeSource,
    offset: int,
    *,
    max_bytes: int = 2 * 1024 * 1024,
) -> tuple[list[dict], int]:
    """Read only complete newly appended JSONL rows, returning the safe cursor.

    The final partial line is left for the next host tick. PinnedNativeSource
    uses pread, so this never disturbs any other reader's transcript offset.
    """
    size = path.size() if isinstance(path, PinnedNativeSource) else pathlib.Path(path).stat().st_size
    if offset < 0 or offset > size:
        raise ValueError("background transcript changed beneath append cursor")
    if offset == size:
        return [], offset
    reader = (path.open_reader() if isinstance(path, PinnedNativeSource)
              else open(path, "rb"))
    with reader as stream:
        stream.seek(offset)
        chunk = stream.read(min(max_bytes, size - offset))
    end = chunk.rfind(b"\n")
    if end < 0:
        return [], offset
    rows: list[dict] = []
    start = offset
    for line in chunk[:end + 1].splitlines(keepends=True):
        next_offset = start + len(line)
        try:
            event = json.loads(line)
        except (ValueError, UnicodeDecodeError):
            event = None
        if isinstance(event, dict) and event.get("isSidechain") is not True:
            event["_ccx_start_offset"] = start
            event["_ccx_end_offset"] = next_offset
            rows.append(event)
        start = next_offset
    return rows, offset + end + 1


def background_task_summary(tasks: list[dict]) -> dict:
    """Compact counts for a list from :func:`extract_background_tasks`.

    Returns `background_task_count` (all tasks ever launched in the transcript)
    and `running_background_task_count` (those with no terminal completion or
    stop evidence yet). Safe on an empty list."""
    return {
        "background_task_count": len(tasks),
        "running_background_task_count": sum(
            1 for t in tasks if t.get("status") == _BG_STATUS_RUNNING),
    }


def native_transcript_start(value, path) -> int | None:
    """Return a valid native start byte offset within this same JSONL file."""
    if type(value) is not int or value < 0:
        return None
    try:
        size = (path.size() if isinstance(path, PinnedNativeSource)
                else os.stat(path).st_size)
        return value if value <= size else None
    except (OSError, TypeError, ValueError):
        return None


def background_task_current(task: dict, native_start: int | None) -> bool:
    """An open task has meaningful activity from this native Claude process."""
    if task.get("status") != _BG_STATUS_RUNNING:
        return False
    opened = task.get("last_open_offset")
    return (type(native_start) is not int or native_start < 0
            or type(opened) is not int or opened < 0
            or opened >= native_start)


def _cache_miss_reason(evt: dict, usage: dict) -> dict | str | None:
    """Claude has moved cache diagnostics across versions; preserve either shape."""
    server_diag = ((usage.get("server_tool_use") or {}).get("diagnostics") or {})
    if server_diag.get("cache_miss_reason") is not None:
        return server_diag.get("cache_miss_reason")
    msg = evt.get("message") or {}
    diag = msg.get("diagnostics") or evt.get("diagnostics") or {}
    if isinstance(diag, dict):
        return diag.get("cache_miss_reason")
    return None


def _usage_tokens(evt: dict, usage: dict) -> dict:
    tokens = {
        "input": usage.get("input_tokens"),
        "output": usage.get("output_tokens"),
    }
    if usage.get("cache_read_input_tokens") is not None:
        tokens["cache_read_input"] = usage.get("cache_read_input_tokens")
    if usage.get("cache_creation_input_tokens") is not None:
        tokens["cache_creation_input"] = usage.get("cache_creation_input_tokens")
    cache_creation = usage.get("cache_creation")
    if isinstance(cache_creation, dict):
        tokens["cache_creation"] = cache_creation
    miss_reason = _cache_miss_reason(evt, usage)
    if miss_reason is not None:
        tokens["cache_miss_reason"] = miss_reason
    return tokens


def _merge_usage_objects(
    prior: dict | None, newer: dict | None
) -> dict:
    """Merge repeated usage snapshots for one Claude API message.

    Later informative fields win; omitted/None fields do not erase facts already
    observed on an earlier content-block record for the same message id.
    """
    merged = dict(prior or {})
    for key, value in (newer or {}).items():
        if value is not None:
            merged[key] = value
    return merged


def _format_tool_call(name: str, inp: dict) -> tuple[str, str | None]:
    """Render a tool_use input as (header, body).

    Ported from claude-history's src/tool_format.rs.
    header is one short line ("Bash: ls -la", "Read: /path:100-150"); body
    is optional extra detail (Edit diff, Task prompt) or None.
    """
    if not isinstance(inp, dict):
        inp = {}
    g = lambda k, d="": inp.get(k) if isinstance(inp.get(k), str) else d
    if name == "Bash":
        return f"Bash: {g('command')}", None
    if name == "Read":
        p = g("file_path")
        off = inp.get("offset"); lim = inp.get("limit")
        if isinstance(off, int) and isinstance(lim, int):
            return f"Read: {p}:{off}-{off+lim}", None
        if isinstance(off, int):
            return f"Read: {p}:{off}", None
        return f"Read: {p}", None
    if name == "Grep":
        path, glob = g("path"), g("glob")
        loc = (f"{path}/{glob}" if path and glob else path or glob or ".")
        return f'Grep: "{g("pattern")}" in {loc}', None
    if name == "Glob":
        pat = g("pattern"); path = g("path")
        return (f"Glob: {pat} in {path}" if path else f"Glob: {pat}"), None
    if name == "Edit":
        p = g("file_path"); old = g("old_string"); new = g("new_string")
        body = None
        if old or new:
            body = "\n".join(f"- {l}" for l in old.splitlines()) + ("\n" if old and new else "") + "\n".join(f"+ {l}" for l in new.splitlines())
        return f"Edit: {p}", body
    if name == "Write":
        return f"Write: {g('file_path')}", None
    if name == "Task":
        return f"Task ({g('subagent_type','agent')}): {g('description')}", (g("prompt") or None)
    if name == "WebFetch":
        return f"Fetch: {g('url')}", (g("prompt") or None)
    if name == "WebSearch":
        return f'Search: "{g("query")}"', None
    # Fallback: dump JSON
    try:
        body = json.dumps(inp, indent=2)
    except Exception:
        body = None
    return f"{name}:", body

def _process_command_message(text: str) -> str | None:
    """Filter and unwrap <local-command-*> / <command-name> tags.

    Returns None if the message should be dropped, otherwise the cleaned text.
    Ported from claude-history's display.rs::process_command_message.
    """
    t = text.strip()
    # local-command-caveat → drop entirely
    if t.startswith("<local-command-caveat>") and t.endswith("</local-command-caveat>"):
        return None
    # local-command-stdout → unwrap; drop if empty
    if t.startswith("<local-command-stdout>") and t.endswith("</local-command-stdout>"):
        inner = t[len("<local-command-stdout>"):-len("</local-command-stdout>")].strip()
        return inner or None
    # <command-name>...</command-name> [<command-args>...</command-args>]
    if "<command-name>" in t and "</command-name>" in t:
        i, j = t.find("<command-name>") + len("<command-name>"), t.find("</command-name>")
        if i < j:
            cmd = t[i:j]
            if cmd == "/clear":
                return None
            if "<command-args>" in t and "</command-args>" in t:
                ai, aj = t.find("<command-args>") + len("<command-args>"), t.find("</command-args>")
                args = t[ai:aj].strip() if ai < aj else ""
                if args:
                    return f"{cmd} {args}"
            return cmd
    return text

def _format_tool_result_content(content) -> str:
    """Stringify a tool_result content field. Strings pass through; everything
    else gets pretty JSON. Empty/missing → '<no content>'."""
    if content is None:
        return "<no content>"
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        # The common case: [{type:"text", text:"..."}]
        parts = []
        for c in content:
            if isinstance(c, dict) and c.get("type") == "text":
                parts.append(c.get("text", ""))
            elif isinstance(c, dict):
                try:
                    parts.append(json.dumps(c, indent=2))
                except Exception:
                    parts.append(str(c))
            else:
                parts.append(str(c))
        return "".join(parts) if parts else "<no content>"
    try:
        return json.dumps(content, indent=2)
    except Exception:
        return str(content)

_TOOL_RESULT_TRUNC_JSON = 4000  # per-block char cap for tool_result.content

# Version of the cleaned-transcript JSON contract. Bump only on a
# backward-incompatible change to the rendered shape (renamed/removed fields,
# changed semantics). Purely additive fields do not require a bump.
TRANSCRIPT_SCHEMA_VERSION = 2

def _iter_clean_turns(
    src: str | pathlib.Path,
    stats: dict | None = None,
    *,
    after_offset: int = 0,
    before_offset: int | None = None,
):
    """Yield cleaned turn dicts from a native JSONL transcript.

    Schema (per yielded dict):
      role: "user" | "assistant" | "system"
      ts: int ms (or None)
      text: str (the human-visible message text; command tags filtered)
        — user/assistant turns only.
      queued: True — user turns only, present only on prompts delivered mid-turn
        as a `queued_command` attachment (steering a busy session, or a
        task-notification landing while a turn runs). Absent on ordinary prompts.
        On these turns `ts` is the *delivery* moment (when the model ingested
        the prompt) and `queued_at` is when it was queued. Claude Code stamps
        the attachment with the enqueue time even though it writes the line at
        delivery, so a raw `ts` would run backwards vs the surrounding turns
        and break timestamp-ordered consumers (e.g. tail splicing).
      tools: list of {name, summary, input, result?} — for assistant turns only
        result has shape {is_error: bool, content: str}; content truncated to
        _TOOL_RESULT_TRUNC_JSON chars with a "<truncated>" sentinel field if so.

    System turns surface Claude Code "system events" as a general channel. They
    carry no `text`/`tools`; instead a `kind` discriminator and a `data` payload:
      {role: "system", kind: <str>, ts: int ms (or None), data: {...}}
    Currently only the compaction boundary is wired up; unknown system subtypes
    are dropped (no behavior change). Shape for kind == "compact_boundary":
      data: {summary: str, pre_tokens: int|None, post_tokens: int|None,
             duration_ms: int|None, trigger: "manual"|"auto"}

    If a mutable `stats` dict is passed, lossy cleaning is tallied into it:
    `malformedLines` (dropped unparseable JSONL lines) and
    `truncatedToolResults` (tool_result blocks clipped to the char cap)."""
    events = list(
        _iter_transcript(
            src,
            stats,
            after_offset=after_offset,
            before_offset=before_offset,
        )
    )

    # Build {tool_use_id: {is_error, content}} from all subsequent user messages
    tool_results: dict[str, dict] = {}
    for evt in events:
        if evt.get("type") != "user":
            continue
        content = evt.get("message", {}).get("content", "")
        if not isinstance(content, list):
            continue
        for c in content:
            if isinstance(c, dict) and c.get("type") == "tool_result":
                tid = c.get("tool_use_id")
                if not tid:
                    continue
                body = _format_tool_result_content(c.get("content"))
                truncated = False
                if len(body) > _TOOL_RESULT_TRUNC_JSON:
                    body = body[:_TOOL_RESULT_TRUNC_JSON]
                    truncated = True
                    if stats is not None:
                        stats["truncatedToolResults"] = stats.get("truncatedToolResults", 0) + 1
                tool_results[tid] = {
                    "is_error": bool(c.get("is_error")),
                    "content": body,
                    **({"truncated": True} if truncated else {}),
                }

    # Claude Code writes each content block of a single model response (thinking /
    # text / each tool_use) as its own JSONL line, all sharing one message.id. They
    # are one logical assistant turn, so we accumulate consecutive same-id lines
    # here and flush them as a single turn (text joined, tools concatenated in
    # order). `pending` is the in-flight accumulator, or None between responses:
    #   {"mid","ts","text_parts","tools","model","usage","usage_evt"}
    pending: dict | None = None
    # Timestamp of the latest event seen so far — the delivery clock for queued
    # attachments (whose own stamp is the earlier enqueue time; see docstring).
    last_ts: int | None = None

    def _flush_pending():
        """Yield the accumulated assistant turn (0 or 1 dict), then reset. Mirrors
        the original per-line emit: a turn with neither text nor tools is dropped."""
        nonlocal pending
        if pending is None:
            return
        p = pending
        pending = None
        text = "\n".join(p["text_parts"]).strip()
        if not text and not p["tools"]:
            return
        turn = {"role": "assistant", "ts": p["ts"], "text": text, "tools": p["tools"]}
        if p["mid"]:
            turn["id"] = p["mid"]
        if p["model"]:
            turn["model"] = p["model"]
        if p["usage"]:
            turn["tokens"] = _usage_tokens(p["usage_evt"], p["usage"])
        yield turn

    for evt in events:
        t = evt.get("type")
        ts = _parse_event_ts_ms(evt)
        prev_ts = last_ts
        if ts is not None:
            last_ts = ts if last_ts is None else max(last_ts, ts)
        if t == "user":
            content = evt.get("message", {}).get("content", "")
            user_text_parts = []
            has_tool_results = False
            if isinstance(content, str):
                processed = _process_command_message(content)
                if processed is None:
                    continue
                user_text_parts.append(processed)
            elif isinstance(content, list):
                for c in content:
                    if not isinstance(c, dict):
                        continue
                    ct = c.get("type")
                    if ct == "text":
                        processed = _process_command_message(c.get("text", ""))
                        if processed is not None:
                            user_text_parts.append(processed)
                    elif ct == "tool_result":
                        has_tool_results = True
            # Skip pure tool-result echo messages — they're paired into the prior assistant turn
            if has_tool_results and not user_text_parts:
                continue
            yield from _flush_pending()
            text = "\n".join(p for p in user_text_parts if p).strip()
            if text:
                turn = {"role": "user", "ts": ts, "text": text}
                if evt.get("uuid"):
                    turn["id"] = evt["uuid"]
                yield turn
        elif t == "assistant":
            msg = evt.get("message", {}) or {}
            mid = msg.get("id")
            # Boundary between two model responses: flush the prior one. An id-less
            # line never groups (flush + start its own) so unrelated turns can't be
            # merged on a missing id.
            if pending is not None and (not mid or pending["mid"] != mid):
                yield from _flush_pending()
            content = msg.get("content", [])
            text_parts = []
            tools = []
            for c in content:
                if not isinstance(c, dict):
                    continue
                ct = c.get("type")
                if ct == "text":
                    text_parts.append(c.get("text", ""))
                elif ct == "tool_use":
                    name = c.get("name", "?")
                    inp = c.get("input", {}) or {}
                    summary, _body = _format_tool_call(name, inp)
                    tool_entry = {
                        "name": name,
                        "summary": summary,
                        "input": inp,
                    }
                    tid = c.get("id")
                    if tid:
                        tool_entry["id"] = tid
                    if tid and tid in tool_results:
                        tool_entry["result"] = tool_results[tid]
                    tools.append(tool_entry)
            if pending is None:
                pending = {
                    "mid": mid,
                    "ts": ts,  # first line's ts = when the response began
                    "text_parts": [],
                    "tools": [],
                    "model": None,
                    "usage": None,
                    "usage_evt": None,
                }
            pending["text_parts"].extend(text_parts)
            pending["tools"].extend(tools)
            # model/usage live on the response's final line (the most informative),
            # so let later lines overwrite earlier ones.
            if msg.get("model"):
                pending["model"] = msg["model"]
            usage = msg.get("usage", {}) or {}
            if usage:
                pending["usage"] = usage
                pending["usage_evt"] = evt
        elif t == "attachment":
            # A prompt delivered while a turn was already running (queued
            # steering, or a task-notification landing mid-turn). Claude Code
            # hands it to the model as a `queued_command` attachment — never as
            # a `user` event — so without this branch the message is invisible
            # in the rendered transcript even though the model read it. The
            # attachment (not the `queue-operation` enqueue) is the proof of
            # delivery: an enqueue can be removed without ever being consumed.
            att = evt.get("attachment") or {}
            if att.get("type") != "queued_command":
                continue
            processed = _process_command_message(_block_text(att.get("prompt")))
            text = (processed or "").strip()
            if not text:
                continue
            yield from _flush_pending()
            delivered_ts = ts if prev_ts is None else max(ts or 0, prev_ts) or None
            turn = {"role": "user", "ts": delivered_ts, "text": text, "queued": True}
            if evt.get("uuid"):
                turn["id"] = evt["uuid"]
            if ts is not None:
                turn["queued_at"] = ts
            yield turn
        elif t == "system":
            yield from _flush_pending()
            # General system-event channel: dispatch on subtype so future kinds
            # can be added with a single handler. Unhandled subtypes drop
            # silently (no behavior change from when system events were ignored).
            data = _clean_system_event(evt.get("subtype"), evt)
            if data is None:
                continue
            kind, payload = data
            turn = {"role": "system", "kind": kind, "ts": ts, "data": payload}
            if evt.get("uuid"):
                turn["id"] = evt["uuid"]
            yield turn
    # A response still accumulating at EOF (file ends mid-turn, e.g. paused on a
    # pending tool) is real content — emit it.
    yield from _flush_pending()


def exact_assistant_text(
    transcript_path: str | pathlib.Path,
    *,
    after_offset: int,
    before_offset: int | None = None,
) -> str:
    """Return the last non-empty assistant text in one exact byte-bounded window.

    The caller owns the turn-to-byte mapping. This helper deliberately does no
    timestamp, prompt-text, or ordinal correlation and never looks outside the
    supplied raw transcript range.
    """
    text = ""
    for turn in _iter_clean_turns(
        transcript_path,
        after_offset=after_offset,
        before_offset=before_offset,
    ):
        if turn.get("role") != "assistant":
            continue
        candidate = turn.get("text")
        if isinstance(candidate, str) and candidate.strip():
            text = candidate.strip()
    return text

def _clean_system_event(subtype, evt: dict):
    """Map a raw system event to a (kind, data) pair for a cleaned turn, or None
    to drop it. Add new (subtype -> kind, data) handlers here as they are wired
    up; currently only ``compact_boundary`` is surfaced."""
    if subtype == "compact_boundary":
        meta = evt.get("compactMetadata") or {}
        return "compact_boundary", {
            "summary": evt.get("content") or "",
            "pre_tokens": meta.get("preTokens"),
            "post_tokens": meta.get("postTokens"),
            "duration_ms": meta.get("durationMs"),
            "trigger": meta.get("trigger"),
        }
    return None

def pending_ask_user_question(transcript_path: str | pathlib.Path) -> dict | None:
    """Return the last AskUserQuestion tool_use with no matching tool_result, or
    None. A dangling AskUserQuestion is an unanswered question dialog — useful as
    a soft `wait_kind=question` corroborator and for rendering options in `peek`.
    Read-only; shape: {"toolUseId": str, "questions": [...]}.

    Answering this reads the whole raw JSONL (only lines that can matter are
    decoded), and `list` / `peek` ask it of external rows on every pass —
    megabytes of re-read for a file that usually hasn't moved. The stamp-keyed
    cache makes an unchanged
    transcript free while keeping the answer provably current: any append
    changes (mtime_ns, size) and forces a rescan. Result is copied out so a
    caller mutating the returned dict cannot poison the entry."""
    result = _pending_question_cache.get(transcript_path, _pending_ask_user_question_uncached)
    return copy.deepcopy(result) if result is not None else None


def clear_pending_question_cache() -> None:
    """Drop every cached transcript scan (tests)."""
    _pending_question_cache.clear()


_pending_question_cache = StampedCache()


_ASK_USER_QUESTION = b"AskUserQuestion"


def _lines_containing(data: bytes, needles):
    """Yield, in file order, each stripped JSONL line of ``data`` holding at
    least one of the byte strings in the live mapping ``needles`` (its values).

    The mapping may change between yields; later lines are matched against its
    current contents. Each needle's next occurrence is found with one
    ``bytes.find`` and remembered until the scan passes it, so the cost is a few
    C-speed searches per matching line instead of a Python step per line."""
    next_at: dict[bytes, int] = {}
    pos = 0
    while True:
        hit = -1
        for needle in list(needles.values()):
            at = next_at.get(needle)
            if at is None or (0 <= at < pos):
                at = data.find(needle, pos)
                next_at[needle] = at
            if at >= 0 and (hit < 0 or at < hit):
                hit = at
        if hit < 0:
            return
        start = data.rfind(b"\n", 0, hit) + 1
        end = data.find(b"\n", hit)
        if end < 0:
            end = len(data)
        pos = end + 1
        yield data[start:end].strip()


def _pending_ask_user_question_uncached(transcript_path: str | pathlib.Path) -> dict | None:
    asked: dict[str, dict] = {}
    answered: set[str] = set()
    try:
        data = pathlib.Path(transcript_path).read_bytes()
    except FileNotFoundError:
        return None
    # Only two kinds of line can change the answer: one naming the tool, and a
    # tool_result for a question still unanswered (an answered id stays
    # answered). Both carry a literal the raw bytes must contain, so only those
    # lines are decoded; the rest of a long transcript is never parsed. Decoding
    # mirrors _iter_transcript: malformed and sidechain lines are skipped.
    needles: dict[object, bytes] = {None: _ASK_USER_QUESTION}
    for line in _lines_containing(data, needles):
        try:
            evt = json.loads(line)
        except (ValueError, UnicodeDecodeError):
            continue
        if not isinstance(evt, dict) or evt.get("isSidechain") is True:
            continue
        content = (evt.get("message") or {}).get("content")
        if not isinstance(content, list):
            continue
        for b in content:
            if not isinstance(b, dict):
                continue
            if b.get("type") == "tool_use" and b.get("name") == "AskUserQuestion":
                tid = b.get("id")
                asked[tid] = b.get("input") or {}
                if isinstance(tid, str) and tid and tid not in answered:
                    # The id as JSON writes it inside a string literal.
                    needles[tid] = json.dumps(tid, ensure_ascii=False)[1:-1].encode()
            elif b.get("type") == "tool_result" and b.get("tool_use_id") in asked:
                answered.add(b.get("tool_use_id"))
                if b.get("tool_use_id") is not None:
                    needles.pop(b.get("tool_use_id"), None)
    pend = [(tid, inp) for tid, inp in asked.items() if tid and tid not in answered]
    if not pend:
        return None
    tid, inp = pend[-1]
    return {"toolUseId": tid, "questions": inp.get("questions", [])}


def _private_native_source_group(gid: int) -> bool:
    """Prove an explicitly named user-private primary group via NSS.

    A group's ``gr_mem`` omits users whose *primary* group is that gid. Check
    both supplementary and primary memberships, and refuse any lookup or
    enumeration ambiguity. A named shared group must not gain write authority
    over the provider's original JSONL merely by sharing our effective gid.
    """
    import grp
    import pwd

    try:
        account = pwd.getpwuid(os.getuid())
        group = grp.getgrgid(gid)
        accounts = pwd.getpwall()
    except (KeyError, OSError):
        return False
    if (account.pw_uid != os.getuid()
            or gid != account.pw_gid or gid != os.getgid()
            or group.gr_gid != gid or group.gr_name != account.pw_name
            or not isinstance(accounts, list) or not accounts
            or not isinstance(group.gr_mem, (list, tuple))
            or any(member != account.pw_name for member in group.gr_mem)):
        return False
    group_primaries = [entry for entry in accounts if entry.pw_gid == gid]
    return (bool(group_primaries)
            and all(entry.pw_name == account.pw_name
                    and entry.pw_uid == account.pw_uid
                    for entry in group_primaries))


def _native_source_directory_safe(info) -> bool:
    """Require our owner; group write only under a proven private group."""
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or info.st_mode & 0o002):
        return False
    return not (info.st_mode & 0o020) or _private_native_source_group(info.st_gid)


def _complete_line_end(fd: int, size: int) -> int:
    """Last complete JSONL byte boundary in a pinned native file."""
    pos = size
    while pos:
        start = max(0, pos - 65536)
        part = os.pread(fd, pos - start, start)
        index = part.rfind(b"\n")
        if index >= 0:
            return start + index + 1
        pos = start
    return 0


@contextmanager
def open_original_claude_source(store, owner, rec):
    """Pin one immutable-original Claude JSONL to its owner and native create claim.

    Callers read the held original fd with offset-independent pread, not the
    mutable provider path or a Darwin /dev/fd alias sharing the seek offset.
    A missing upstream file remains distinct from an unsafe original.
    Yield the pinned source/stat, last complete line, derived original path and
    directory fd (renderer uses the latter to verify identity before publish).
    """
    from ccx.store import paths
    from ccx.store.root import StorageIntegrityError, _DIR_FLAGS, _FILE_FLAGS
    from ccx.store.identity import InvalidTarget, canonical_worker_id

    native = rec.get("native")
    native_sid = native.get("sessionId") if isinstance(native, dict) else None
    try:
        canonical_worker_id(native_sid)
    except InvalidTarget as exc:
        raise StorageIntegrityError("Claude owner has no original native session ID") from exc
    fingerprint = rec.get("createSha256")
    if not isinstance(fingerprint, str) or not fingerprint:
        raise StorageIntegrityError("Claude original creation fingerprint is missing")
    original = store.creation_binding(
        owner.worker, engine="claude", fingerprint=fingerprint,
    )
    if not isinstance(original, dict) or original.get("sessionId") != native_sid:
        raise StorageIntegrityError("Claude JSONL source differs from original native binding")
    cwd = rec.get("cwd")
    if not isinstance(cwd, str) or not os.path.isabs(cwd):
        raise StorageIntegrityError("Claude original working directory is not absolute")
    source_path = paths.transcript_path_for(cwd, native_sid)
    if rec.get("transcript") != str(source_path):
        raise StorageIntegrityError("Claude transcript source is not original native project JSONL")
    # A provider JSONL is outside CCX_ROOT, but has a derivable native owner:
    # open its original <native UUID>.jsonl through no-follow parent fds. The
    # retained native fd also prevents pathname replacement from switching
    # the transcript between turn cleanup and compact-boundary detection.
    with ExitStack() as held:
        try:
            current = os.open(paths.CLAUDE_HOME, _DIR_FLAGS)
            held.callback(os.close, current)
            for component in ("projects", source_path.parent.name):
                directory = os.fstat(current)
                if not _native_source_directory_safe(directory):
                    raise StorageIntegrityError("unsafe native Claude source directory")
                next_fd = os.open(component, _DIR_FLAGS, dir_fd=current)
                held.callback(os.close, next_fd)
                current = next_fd
            directory = os.fstat(current)
            if not _native_source_directory_safe(directory):
                raise StorageIntegrityError("unsafe native Claude source directory")
            native_fd = os.open(
                native_sid + ".jsonl", _FILE_FLAGS | os.O_NONBLOCK, dir_fd=current,
            )
            held.callback(os.close, native_fd)
        except FileNotFoundError:
            # No upstream JSONL yet. Leave the renderer a no-output sentinel;
            # compact callers must classify this as missing terminal evidence.
            yield None
            return
        except OSError as exc:
            raise StorageIntegrityError("unsafe Claude native transcript source") from exc
        source_stat = os.fstat(native_fd)
        if (not stat.S_ISREG(source_stat.st_mode)
                or source_stat.st_uid != os.getuid()
                or source_stat.st_mode & 0o022):
            raise StorageIntegrityError("Claude native transcript must be an owned regular file")
        # A held original inode; each parse gets an independent pread cursor.
        pinned_source = PinnedNativeSource(native_fd)
        end_offset = _complete_line_end(native_fd, source_stat.st_size)
        yield pinned_source, source_stat, end_offset, source_path, current



def render_transcript(short: str, *, store=None) -> dict | None:
    """Render one Claude worker's cleaned transcript from its original JSONL.

    Read-only and on demand (``logs``): parses the whole native transcript, so
    nothing on a hot path may call it. Missing upstream JSONL returns None.
    """
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.store.identity import EngineMismatch

    if store is None:
        store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(short)
    if owner.engine != "claude":
        raise EngineMismatch("transcript rendering requires a Claude full-UUID owner")
    rec = store.record(owner.worker)
    with open_original_claude_source(store, owner, rec) as source:
        if source is None:
            return None
        pinned_source, _source_stat, end_offset, source_path, _current = source
        stats = {"malformedLines": 0, "truncatedToolResults": 0}
        turns = list(_iter_clean_turns(
            pinned_source, stats, before_offset=end_offset,
        ))
        context = compute_context_usage(turns)
        pending_compact = pending_compact_request(
            pinned_source, before_offset=end_offset,
        )
    if pending_compact is not None:
        context = compaction_unknown_context(
            "compacting", as_of_ms=pending_compact.get("as_of_ms"),
        )
    return {
        "schemaVersion": TRANSCRIPT_SCHEMA_VERSION,
        "generatedAt": int(time.time() * 1000),
        "worker": owner.worker,
        "sessionId": rec["native"]["sessionId"],
        "name": rec.get("name", owner.short),
        "cwd": rec["cwd"],
        "transcriptJsonl": str(source_path),
        "turns": turns,
        "context": context,
        "stats": stats,
    }


def _valid_observed_model(model) -> bool:
    return (isinstance(model, str) and 0 < len(model) <= 256
            and model != "<synthetic>")


class _ObservationReducer:
    """The context, model and pending-compact facts of a cleaned transcript,
    fed one raw event at a time so the host can resume where it stopped.

    It mirrors exactly the parts of ``_iter_clean_turns`` those facts depend
    on: which events emit a turn, how one response's lines group by message
    id, and which line's model and usage win. ``render_transcript`` stays the
    full renderer; tests pin the two together at every line boundary. The
    state is small and JSON-serializable.
    """

    def __init__(self, state: dict | None = None):
        state = state or {}
        self.any_turn = state.get("anyTurn", False)
        # Latest compact boundary or usable assistant usage, as a minimal
        # cleaned turn that compute_context_usage reads.
        self.anchor = state.get("anchor")
        self.model = state.get("model")
        self.pending_compact = state.get("pendingCompact")
        # The response still accumulating: {mid, ts, content, model, tokens}.
        self.open = state.get("open")

    @staticmethod
    def valid_state(state) -> bool:
        if not isinstance(state, dict) or not isinstance(state.get("anyTurn"), bool):
            return False
        anchor, model = state.get("anchor"), state.get("model")
        pending, open_ = state.get("pendingCompact"), state.get("open")
        return ((anchor is None or isinstance(anchor, dict))
                and (model is None or _valid_observed_model(model))
                and (pending is None or isinstance(pending, dict))
                and (open_ is None or (isinstance(open_, dict)
                                       and isinstance(open_.get("content"), bool))))

    def state(self) -> dict:
        return {"anyTurn": self.any_turn, "anchor": self.anchor, "model": self.model,
                "pendingCompact": self.pending_compact, "open": self.open}

    def _flush(self) -> None:
        response, self.open = self.open, None
        if response is None or not response["content"]:
            return
        self.any_turn = True
        if _valid_observed_model(response.get("model")):
            self.model = response["model"]
        tokens = response.get("tokens")
        if tokens is not None:
            turn = {"role": "assistant", "ts": response.get("ts"), "tokens": tokens}
            if (compute_context_usage([turn]) or {}).get("state") == "known":
                self.anchor = turn

    def _feed_compact(self, evt: dict, kind, ts) -> None:
        # Same rules as pending_compact_request.
        if kind == "system" and evt.get("subtype") == "compact_boundary":
            self.pending_compact = None
            return
        content = None
        if kind == "system" and evt.get("subtype") == "local_command":
            content = evt.get("content")
        elif kind == "user":
            content = (evt.get("message") or {}).get("content")
        if _is_compact_command_content(content):
            self.pending_compact = {"as_of_ms": ts}
        elif (self.pending_compact is not None and isinstance(content, str)
                and ("<local-command-stdout>" in content
                     or "<local-command-stderr>" in content)):
            self.pending_compact = None

    def feed(self, evt: dict) -> None:
        kind = evt.get("type")
        ts = _parse_event_ts_ms(evt)
        self._feed_compact(evt, kind, ts)
        if kind == "user":
            content = (evt.get("message") or {}).get("content", "")
            parts: list[str] = []
            has_tool_results = False
            if isinstance(content, str):
                processed = _process_command_message(content)
                if processed is None:
                    return
                parts.append(processed)
            elif isinstance(content, list):
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    if block.get("type") == "text":
                        processed = _process_command_message(block.get("text", ""))
                        if processed is not None:
                            parts.append(processed)
                    elif block.get("type") == "tool_result":
                        has_tool_results = True
            if has_tool_results and not parts:
                return
            self._flush()
            if "\n".join(p for p in parts if p).strip():
                self.any_turn = True
        elif kind == "assistant":
            msg = evt.get("message") or {}
            mid = msg.get("id")
            if self.open is not None and (not mid or self.open["mid"] != mid):
                self._flush()
            if self.open is None:
                self.open = {"mid": mid, "ts": ts, "content": False,
                             "model": None, "tokens": None}
            content = msg.get("content", [])
            for block in content if isinstance(content, list) else ():
                if not isinstance(block, dict):
                    continue
                text = block.get("text", "")
                if ((block.get("type") == "text" and isinstance(text, str) and text.strip())
                        or block.get("type") == "tool_use"):
                    self.open["content"] = True
            if msg.get("model"):
                self.open["model"] = msg["model"]
            usage = msg.get("usage", {}) or {}
            if usage:
                tokens = _usage_tokens(evt, usage)
                self.open["tokens"] = {
                    key: tokens[key] for key in
                    ("input", "cache_read_input", "cache_creation_input")
                    if key in tokens
                }
        elif kind == "attachment":
            attachment = evt.get("attachment") or {}
            if attachment.get("type") != "queued_command":
                return
            processed = _process_command_message(_block_text(attachment.get("prompt")))
            if (processed or "").strip():
                self._flush()
                self.any_turn = True
        elif kind == "system":
            self._flush()
            data = _clean_system_event(evt.get("subtype"), evt)
            if data is not None:
                self.any_turn = True
                if data[0] == "compact_boundary":
                    self.anchor = {"role": "system", "kind": "compact_boundary", "ts": ts}

    def observe(self) -> tuple[dict | None, str | None]:
        """(context, observed model) as the full render would report them now:
        the open response counts as finished, as it does at end of file."""
        final = _ObservationReducer(copy.deepcopy(self.state()))
        final._flush()
        turns = [final.anchor or {"role": "user"}] if final.any_turn else []
        context = compute_context_usage(turns)
        if final.pending_compact is not None:
            context = compaction_unknown_context(
                "compacting", as_of_ms=final.pending_compact.get("as_of_ms"),
            )
        return context, final.model


OBSERVATION_CURSOR_VERSION = 1
# Bytes just before the resume offset whose hash proves the file was only
# appended to since the last refresh (not truncated and rewritten in place).
_OBSERVATION_FENCE_BYTES = 4096


def _observation_fence_sha(source: PinnedNativeSource, offset: int) -> str:
    start = max(0, offset - _OBSERVATION_FENCE_BYTES)
    return hashlib.sha256(os.pread(source.fd, offset - start, start)).hexdigest()


def _resumable_observation_state(store, owner, native_sid, source, source_stat, end_offset):
    """The prior observation's (offset, reducer state) when it still describes
    a prefix of this exact source, else None (start again from the top)."""
    try:
        doc = store.read_worker_sidecar(owner.worker, "context-observation.json")
    except Exception:
        return None
    if not isinstance(doc, dict):
        return None
    observed, cursor = doc.get("source"), doc.get("cursor")
    if (doc.get("schemaVersion") != 1 or doc.get("workerId") != owner.worker_id
            or doc.get("nativeSessionId") != native_sid
            or doc.get("generation") != store.generation
            or not isinstance(observed, dict) or not isinstance(cursor, dict)
            or observed.get("dev") != source_stat.st_dev
            or observed.get("ino") != source_stat.st_ino
            or type(observed.get("endOffset")) is not int
            or not 0 <= observed["endOffset"] <= end_offset
            or cursor.get("version") != OBSERVATION_CURSOR_VERSION
            or not _ObservationReducer.valid_state(cursor.get("state"))
            or cursor.get("fenceSha256") != _observation_fence_sha(source, observed["endOffset"])):
        return None
    return observed["endOffset"], cursor["state"]


def refresh_context_observation(short: str, *, store=None) -> dict | None:
    """Update one Claude worker's context/model observation from its JSONL.

    Incremental: parses only what Claude appended since the previous
    observation of the same source file (same inode, same generation), so a
    long-lived worker's refresh costs what its last turn wrote, with constant
    memory. Anything else starts again from the top, streaming. Publishes only
    through the ACTIVE generation's exact full-UUID worker directory; missing
    upstream JSONL returns None.
    """
    from ccx import constants
    from ccx.store.root import StorageIntegrityError, _write_json, require_ccx_active
    from ccx.store.identity import EngineMismatch

    if store is None:
        store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(short)
    if owner.engine != "claude":
        raise EngineMismatch("context observation requires a Claude full-UUID owner")
    rec = store.record(owner.worker)
    with open_original_claude_source(store, owner, rec) as source:
        if source is None:
            return None
        pinned_source, source_stat, end_offset, source_path, current = source
        native_sid = rec["native"]["sessionId"]
        cwd = rec["cwd"]
        fingerprint = rec["createSha256"]
        prior = _resumable_observation_state(
            store, owner, native_sid, pinned_source, source_stat, end_offset)
        start, state = prior if prior is not None else (0, None)
        reducer = _ObservationReducer(state)
        for event in _iter_transcript(
            pinned_source, after_offset=start, before_offset=end_offset,
        ):
            reducer.feed(event)
        context, observed_model = reducer.observe()
        observation = {
            "schemaVersion": 1,
            "workerId": owner.worker_id, "nativeSessionId": native_sid,
            "generation": store.generation,
            "source": {"dev": source_stat.st_dev, "ino": source_stat.st_ino,
                       "endOffset": end_offset,
                       "observedAtMs": int(time.time() * 1000)},
            "context": context, "observedModel": observed_model,
            "cursor": {"version": OBSERVATION_CURSOR_VERSION,
                       "fenceSha256": _observation_fence_sha(pinned_source, end_offset),
                       "state": reducer.state()},
        }
        # If the upstream pathname was replaced during the read, the pinned
        # old inode no longer proves the current provider source.
        try:
            current_source = os.stat(
                native_sid + ".jsonl", dir_fd=current, follow_symlinks=False,
            )
        except OSError as exc:
            raise StorageIntegrityError("original Claude native JSONL disappeared during refresh") from exc
        if (current_source.st_dev != source_stat.st_dev
                or current_source.st_ino != source_stat.st_ino
                or not stat.S_ISREG(current_source.st_mode)
                or current_source.st_uid != os.getuid()
                or current_source.st_mode & 0o022
                or current_source.st_size < end_offset):
            raise StorageIntegrityError("original Claude native JSONL changed identity during refresh")
        # Revalidate global ownership after the read; a generation rotation or
        # changed original source must not publish into a new owner's directory.
        with store.lease():
            latest_owner, latest = store._owned(owner.worker)
            latest_native = latest.get("native")
            if (latest_owner != owner or not isinstance(latest_native, dict)
                    or latest_native.get("sessionId") != native_sid
                    or latest.get("cwd") != cwd
                    or latest.get("transcript") != str(source_path)
                    or latest.get("createSha256") != fingerprint):
                raise StorageIntegrityError("Claude original JSONL ownership changed during refresh")
            with store._gen("workers", "claude", owner.worker_id) as worker_dir_fd:
                _write_json(worker_dir_fd, "context-observation.json", observation)
        return observation
