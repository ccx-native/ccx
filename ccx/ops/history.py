"""Read and search worker histories as text: ``ccx history``.

A projection of the normalized conversation document
(:mod:`ccx.ops.conversation`), never a second parser: every message keeps the
document's turn ``sequence`` and item ``ordinal``, so an address ``#TURN.N``
means the same item in ``ccx history``, ``ccx logs`` and ``peek --since-turn``.

The projection maps normalized items to a small message model shared by both
engines: prompts (``U``), steers (``U+``), notifications and events
(``N[kind]``), assistant text (``A``, final ``A*``, a running turn's latest
``A…``), tool calls (``T``), slash commands (``/``), interrupts (``!``) and
compactions (``══``). Reasoning is never shown. Everything here is read-only
text rendering over documents the caller already read.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import shlex
import time
from dataclasses import dataclass, field
from typing import Callable, TextIO

from ccx.claude.transcript import _parse_task_notification
from ccx.errors import InvalidRequest

# --- message model -------------------------------------------------------------

ROLES = ("user", "notify", "assistant", "final", "tool", "output", "compact")
VIEW_ROLES = {"chat": {"user", "notify", "final"},
              "trace": {"user", "notify", "assistant", "tool"}}
SEARCH_ROLES = {"user", "assistant", "notify", "tool"}
VIEWS = ("outline", "chat", "trace", "map")

DEFAULT_WINDOW = 30          # turns shown with no selector (one worker)
DEFAULT_MANY_WINDOW = 10     # per worker when several are named
DEFAULT_LIMIT = 20           # hit lines per worker
DEFAULT_MAX_CHARS = 3000
MAP_GAP_HOURS = 6.0


@dataclass
class Message:
    turn: int
    n: int
    role: str                 # user steer notify assistant tool compact command interrupt
    text: str = ""
    ts: float | None = None   # epoch seconds, when the source records it
    kind: str | None = None   # notify kind: bg monitor agent cron system error <namespace>
    final: str | None = None  # "final" (A*) or "latest" (A…, turn still running)
    tool: str | None = None
    cmd: str | None = None
    desc: str | None = None
    out: str = ""             # tool output, notification body, compaction summary
    err: bool = False
    dur: float | None = None


@dataclass
class Turn:
    seq: int
    start: float | None
    end: float | None
    status: str | None
    messages: list[Message] = field(default_factory=list)


@dataclass
class History:
    engine: str
    short: str
    name: str | None
    cwd: str | None
    turns: list[Turn]
    notes: list[str] = field(default_factory=list)

    def messages(self) -> list[Message]:
        return [m for t in self.turns for m in t.messages]


def _epoch_iso(value) -> float | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=dt.timezone.utc).timestamp()
    except ValueError:
        return None


def _epoch_ms(value) -> float | None:
    return value / 1000.0 if isinstance(value, int) and not isinstance(value, bool) else None


_COMPACT_HEAD = re.compile(r"\AThis session is being continued from a previous conversation.*?\n\n"
                           r"(?:<system-reminder>.*?</system-reminder>\s*)?", re.S)
_COMPACT_TAIL = re.compile(r"\n+If you need specific details from before compaction.*\Z", re.S)


def strip_compact_wrapper(text: str) -> str:
    """Drop Claude Code's resume boilerplate around a compaction summary."""
    return _COMPACT_TAIL.sub("", _COMPACT_HEAD.sub("", text or "")).strip()


_XML_TAG = re.compile(r"<(result|event|note)>(.*?)</\1>", re.S)


def _notification(text: str) -> tuple[str, str, str] | None:
    """``(kind, one-line summary, body)`` for a Claude ``<task-notification>``."""
    parsed = _parse_task_notification(text)
    if parsed is None:
        return None
    summary = (parsed.get("summary") or "").strip()
    kind = ("monitor" if summary.startswith("Monitor") or parsed.get("event")
            else "agent" if summary.startswith("Agent") else "bg")
    body = "\n".join(v.strip() for _k, v in _XML_TAG.findall(text) if v.strip())
    return kind, summary or (parsed.get("status") or "") or text[:200], body


_JSON_FIELD = re.compile(r'"(\w+)"\s*:\s*("(?:[^"\\]|\\.)*("|$)|-?\d+(?:\.\d+)?|true|false|null)')


def _json_fields(raw: str) -> dict:
    """The fields of a JSON object, also from one cut short by a result bound.

    A cut payload keeps its leading string and number fields; an unterminated
    last string keeps what arrived."""
    try:
        body = json.loads(raw)
        return body if isinstance(body, dict) else {}
    except ValueError:
        pass
    fields: dict = {}
    for key, value, closed in _JSON_FIELD.findall(raw or ""):
        if key in fields:
            continue
        if value.startswith('"') and not closed:
            value = value.rstrip("\\") + '"'
        try:
            fields[key] = json.loads(value)
        except ValueError:
            continue
    return fields


def _injected_output(item: dict) -> str:
    """The text of a Codex namespaced wake-up output: who, phase, status, report."""
    raw = (item.get("result") or {}).get("content") or ""
    if not isinstance(raw, str):
        raw = json.dumps(raw)
    body = _json_fields(raw)
    if not body:
        return raw
    report = next((str(body[k]) for k in ("report", "message", "summary", "activity")
                   if isinstance(body.get(k), str) and body[k]), "")
    who = next((str(body[k]) for k in ("worker", "sessionId", "work_id", "workId")
                if isinstance(body.get(k), str) and body[k]), "")
    status = body.get("status") or body.get("state")
    head = " ".join(str(x) for x in (who, body.get("phase"), status) if x)
    return f"{head}: {report}" if head else report


def _shell_command(item: dict) -> str:
    """The command a shell call ran; Codex already unwraps ``bash -lc``."""
    actions = ((item.get("data") or {}).get("commandActions")) or []
    commands = [a.get("command") for a in actions
                if isinstance(a, dict) and isinstance(a.get("command"), str) and a["command"]]
    if commands:
        return "\n".join(commands)
    command = (item.get("arguments") or {}).get("command")
    if isinstance(command, list):
        command = command[-1] if len(command) == 3 else " ".join(map(str, command))
    command = command if isinstance(command, str) else ""
    m = re.match(r"^\S*(?:bash|sh|zsh) -l?c '(.*)'$", command, re.S)
    return m.group(1).replace("'\\''", "'") if m else command


def _tool_message(item: dict, base: dict) -> Message:
    name = item.get("name") or item.get("nativeType") or "tool"
    summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
    if name in ("Bash", "shell"):
        cmd = _shell_command(item)
    elif summary.get("target"):
        cmd = summary["target"]
    else:
        args = item.get("arguments")
        cmd = json.dumps(args, ensure_ascii=False)[:300] if args else ""
    result = item.get("result") if isinstance(item.get("result"), dict) else {}
    started, completed = item.get("startedAtMs"), item.get("completedAtMs")
    dur = ((completed - started) / 1000.0 if isinstance(started, int)
           and isinstance(completed, int) and completed >= started else None)
    out = result.get("content")
    return Message(**base, role="tool", tool=name, cmd=cmd, desc=summary.get("description"),
                   out=out if isinstance(out, str) else ("" if out is None else json.dumps(out)),
                   err=bool(result.get("isError")) or item.get("status") in ("failed", "declined"),
                   dur=dur)


def _project_item(item: dict, base: dict, cron: dict | None) -> Message | None:
    kind, native_type = item.get("kind"), item.get("nativeType")
    text = item.get("text") if isinstance(item.get("text"), str) else ""
    if kind == "user-text":
        if cron is not None:
            return Message(**base, role="notify", kind="cron", text=text)
        if item.get("meta"):
            if text.lstrip().startswith("<local-command-caveat>"):
                return None
            return Message(**base, role="notify", kind="system", text=text)
        note = _notification(text)
        if note is not None:
            k, summary, body = note
            return Message(**base, role="notify", kind=k, text=summary, out=body)
        if text.startswith("[Request interrupted by user"):
            return Message(**base, role="interrupt", text=text)
        if text.lstrip().startswith(("<local-command-stdout>", "<local-command-stderr>")):
            return None
        return Message(**base, role="user", text=text)
    if kind == "command":
        args = (item.get("arguments") or {}).get("args")
        return Message(**base, role="command",
                       text=f"{item.get('name') or ''}{' ' + args if args else ''}")
    if kind in ("assistant-text", "assistant-plan"):
        return Message(**base, role="assistant", text=text, kind=item.get("phase"))
    if kind == "tool-result" and item.get("namespace"):
        return Message(**base, role="notify", kind=item["namespace"], text=_injected_output(item))
    if kind in ("tool-call", "tool-result", "file-change", "subagent-action",
                "subagent-activity", "action"):
        return _tool_message(item, base)
    if kind == "system-text" and native_type in ("compact_boundary", "contextCompaction"):
        data = item.get("data") if isinstance(item.get("data"), dict) else {}
        bits = [b for b in (data.get("trigger"),
                            f"{data['preTokens'] // 1000}k tokens"
                            if isinstance(data.get("preTokens"), int) else None) if b]
        summary = strip_compact_wrapper(data.get("summary") or "")
        return Message(**base, role="compact", text=" · ".join(bits), out=summary)
    if kind == "system-text":
        return Message(**base, role="notify", kind="system", text=text)
    if kind == "error":
        return Message(**base, role="notify", kind="error", text=str(item.get("message") or ""))
    if kind == "other" and native_type == "system":
        native = item.get("native") if isinstance(item.get("native"), dict) else {}
        if native.get("subtype") == "informational":
            return Message(**base, role="notify", kind="system",
                           text=str(native.get("content") or ""))
    return None  # attachments, reasoning summaries, bookkeeping: not shown


def project(doc: dict, *, name: str | None = None) -> History:
    """Map one normalized document to the history message model."""
    worker = doc.get("worker") or {}
    busy = doc.get("state") == "busy"
    turns: list[Turn] = []
    cron: dict | None = None
    for t in doc.get("turns") or []:
        seq = t.get("sequence")
        turn = Turn(seq, _epoch_iso(t.get("startedAt")), _epoch_iso(t.get("completedAt")),
                    t.get("status"))
        for item in sorted(t.get("input", []) + t.get("output", []),
                           key=lambda i: i.get("ordinal") or 0):
            native = item.get("native") if isinstance(item.get("native"), dict) else {}
            if item.get("kind") == "other" and native.get("subtype") == "scheduled_task_fire":
                cron = native        # its prompt is the next user row
                continue
            base = {"turn": seq, "n": item.get("ordinal"), "ts": _epoch_ms(item.get("startedAtMs"))}
            message = _project_item(item, base, cron)
            if item.get("kind") == "user-text":
                cron = None
            if message is not None:
                turn.messages.append(message)
        _label_turn(turn, worker.get("engine"))
        turns.append(turn)
    if turns and (busy or turns[-1].status == "in-progress"):
        for m in reversed(turns[-1].messages):
            if m.role == "assistant":
                for other in turns[-1].messages:
                    if other.final:
                        other.final = None
                m.final = "latest"
                break
    completeness = doc.get("completeness") or {}
    notes = list(completeness.get("warnings") or []) if completeness.get("status") in (
        "partial", "source-limited") else []
    return History(engine=worker.get("engine") or "?", short=worker.get("short") or "?",
                   name=name, cwd=worker.get("cwd"), turns=turns, notes=notes)


def _label_turn(turn: Turn, engine: str | None) -> None:
    """Prompt vs steer, and the final answer, from each turn's own messages."""
    prompted = False
    for m in turn.messages:
        if m.role in ("user", "notify", "command"):
            if prompted and m.role == "user":
                m.role = "steer"
            prompted = True
    texts = [m for m in turn.messages if m.role == "assistant"]
    if not texts:
        return
    if engine == "codex" and any(m.kind for m in texts):
        finals = [m for m in texts if m.kind == "final_answer"]
        if finals:
            finals[-1].final = "final"
        return
    texts[-1].final = "final"


# --- shell command words -------------------------------------------------------

_HEREDOC = re.compile(r"<<-?\s*(['\"]?)(\w+)\1[^\n]*\n.*?\n\s*\2\s*(?=\n|$)", re.S)
# wrapper -> its options that take a separate argument
_WRAPPERS = {
    "sudo": {"-u", "-g", "-C", "-D"}, "env": {"-u", "-C", "-S", "--unset", "--chdir"},
    "timeout": {"-s", "-k", "--signal", "--kill-after"}, "nice": {"-n"},
    "xargs": {"-I", "-n", "-P", "-L", "-d", "-a", "-E", "-s"},
    "uv": {"--with", "--python", "-p", "--project", "--directory", "--group", "--extra"},
    "run": {"--with", "--python", "-p", "--project", "--directory", "--group", "--extra"},
    "uvx": {"--from", "--with"}, "npx": {"-p", "--package"}, "ionice": {"-c", "-n"},
    "time": set(), "nohup": set(), "exec": set(), "command": set(), "stdbuf": set(),
    "caffeinate": set(),
    # shell keywords that precede a command
    "if": set(), "then": set(), "elif": set(), "else": set(), "while": set(),
    "until": set(), "do": set(), "!": set(), "{": set(),
}
_KEYWORDS = {"fi", "done", "for", "in", "esac", "case", "function", "}", "]]", "[["}
_SHELLS = {"bash", "sh", "zsh"}


def _segments(cmd: str) -> list[str]:
    """Split on unquoted ``; & | && || newline $( ` ( )``; heredoc bodies and
    ``$(( ))`` arithmetic dropped, ``\\``-newline continuations joined, and
    redirections such as ``2>&1`` kept whole."""
    cmd = _HEREDOC.sub("<<HEREDOC", (cmd or "").replace("\\\n", " "))
    segs, cur, quote, i = [], [], None, 0
    while i < len(cmd):
        c = cmd[i]
        if quote:
            if c == "\\" and quote == '"':
                cur.append(cmd[i:i + 2])
                i += 2
                continue
            if c == quote:
                quote = None
            cur.append(c)
        elif c in "'\"":
            quote = c
            cur.append(c)
        elif c == "\\":
            cur.append(cmd[i:i + 2])
            i += 2
            continue
        elif cmd.startswith("$((", i):
            end = cmd.find("))", i + 3)
            i = len(cmd) if end < 0 else end + 2
            cur.append("0")                 # arithmetic is a value, not a command
            continue
        elif c == "&" and ((cur and cur[-1] in "<>") or cmd[i + 1:i + 2] == ">"):
            cur.append(c)                   # 2>&1, >&2, &>file, &>>file
        elif c == "|" and cur and cur[-1] == ">":
            cur.append(c)                   # >| (noclobber override)
        elif c in ";&|\n`()":
            segs.append("".join(cur))
            cur = []
        elif c == "$" and cmd[i + 1:i + 2] == "(":
            segs.append("".join(cur))
            cur = []
            i += 2
            continue
        elif c == "#" and (not cur or cur[-1] in " \t"):
            while i < len(cmd) and cmd[i] != "\n":
                i += 1
            continue
        else:
            cur.append(c)
        i += 1
    segs.append("".join(cur))
    return segs


def command_words(cmd: str, _depth: int = 0) -> list[str]:
    """The command name of each pipeline/list segment, after env assignments,
    wrappers (with their option arguments) and leading shell keywords;
    ``bash -c '…'`` is unwrapped and ``python3.x`` reads as ``python``."""
    words: list[str] = []
    for seg in _segments(cmd):
        toks = seg.strip().split()
        wrapper = None
        while toks:
            t = toks[0]
            if re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", t):
                toks.pop(0)
            elif t in _WRAPPERS:
                wrapper = t
                toks.pop(0)
            elif wrapper is not None and t.startswith("-"):
                toks.pop(0)
                if t in _WRAPPERS[wrapper] and toks:
                    toks.pop(0)             # the option's argument
            elif wrapper == "timeout" and re.match(r"^\d+(\.\d+)?[smhd]?$", t):
                toks.pop(0)                 # the duration
            else:
                break
        if not toks:
            continue
        word = os.path.basename(toks[0].strip("'\"{}"))
        word = re.sub(r"^python3(\.\d+)?$", "python", word)
        if not word or not re.match(r"^[\w.+-]+$", word) or word in _KEYWORDS:
            continue
        if word in _SHELLS and _depth < 2:
            inner = _shell_c_argument(seg)
            if inner is not None:
                words.extend(command_words(inner, _depth + 1))
                continue
        words.append(word)
    return words


def _shell_c_argument(segment: str) -> str | None:
    """The script of ``bash -c '…'`` / ``sh -lc "…"``, or None."""
    try:
        toks = shlex.split(segment)
    except ValueError:
        return None
    for i, tok in enumerate(toks[1:], 1):
        if re.fullmatch(r"-[a-z]*c[a-z]*", tok):
            return toks[i + 1] if i + 1 < len(toks) else None
        if not tok.startswith("-"):
            return None
    return None


# --- time ---------------------------------------------------------------------

_TIME_FORMS = "2026-11-02, 2026-11-02T14:00, '11-02 14:00', 14:00 (today), or 90m/6h/2d (ago)"


def parse_time(value: str | None, *, end: bool = False, now: float | None = None) -> float | None:
    """Epoch seconds for ``--since``/``--until``; a bare ``--until`` date includes that day."""
    if value is None:
        return None
    v = value.strip()
    now_s = time.time() if now is None else now
    m = re.fullmatch(r"(\d+)([mhd])", v)
    if m:
        return now_s - int(m.group(1)) * {"m": 60, "h": 3600, "d": 86400}[m.group(2)]
    today = dt.datetime.fromtimestamp(now_s)
    if re.fullmatch(r"\d{1,2}:\d{2}", v):
        v = f"{today:%Y-%m-%d} {v}"
    elif re.fullmatch(r"\d{2}-\d{2}( .*)?", v):
        v = f"{today.year}-{v}"
    try:
        moment = dt.datetime.fromisoformat(v.replace(" ", "T"))
    except ValueError:
        raise InvalidRequest(f"cannot read time {value!r}; use {_TIME_FORMS}") from None
    if moment.tzinfo is not None:
        return moment.timestamp()
    if end and re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        moment += dt.timedelta(days=1)
    return moment.timestamp()


_YEAR = dt.datetime.now().year


def _clock(ts: float | None, full: bool = False) -> str:
    if not ts:
        return "--"
    d = dt.datetime.fromtimestamp(ts)
    if not full:
        return d.strftime("%H:%M")
    return d.strftime("%m-%d %H:%M") if d.year == _YEAR else d.strftime("%Y-%m-%d %H:%M")


def _msg_clock(ts: float | None, turn_ts: float | None) -> str:
    """HH:MM inside a turn; the date too when the turn crossed midnight."""
    if not ts:
        return ""
    if turn_ts and dt.date.fromtimestamp(ts) != dt.date.fromtimestamp(turn_ts):
        return " " + _clock(ts, True)
    return " " + _clock(ts)


def _dur(s: float | None) -> str:
    if s is None:
        return "?"
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}"
    return f"{s // 3600}h{(s % 3600) // 60:02d}"


def zone_name() -> str:
    tz = os.environ.get("TZ")
    if tz:
        return tz.lstrip(":")
    try:
        link = os.path.realpath("/etc/localtime")
    except OSError:
        link = ""
    return link.split("zoneinfo/")[-1] if "zoneinfo/" in link else (time.tzname[0] or "local")


# --- selection ----------------------------------------------------------------

@dataclass
class Selection:
    turns: set[int] | None = None          # None: no --turns
    messages: set[tuple[int, int]] = field(default_factory=set)


_TURN_PART = re.compile(r"\d+(\.\d+)?|\d+-\d*|-\d+")


def check_turns(spec: str | None) -> None:
    for part in (spec or "").replace("#", "").split(","):
        if spec is not None and not _TURN_PART.fullmatch(part.strip()):
            raise InvalidRequest(
                f"cannot read --turns part {part!r}; use 12, 10-14, 120-, -5 (the last five) "
                "or 184.0 (one message), comma-separated")


def parse_turns(spec: str | None, last: int) -> Selection:
    if spec is None:
        return Selection()
    check_turns(spec)
    sel = Selection(turns=set())
    for part in spec.replace("#", "").split(","):
        part = part.strip()
        if "." in part:
            t, k = map(int, part.split("."))
            sel.messages.add((t, k))
        elif part.startswith("-"):
            sel.turns |= set(range(max(1, last - int(part[1:]) + 1), last + 1))
        elif "-" in part:
            a, b = part.split("-")
            sel.turns |= set(range(int(a), (int(b) if b else last) + 1))
        else:
            sel.turns.add(int(part))
    return sel


def smart_regex(pattern: str | None, what: str = "--grep") -> re.Pattern | None:
    """ripgrep-style smart case: an all-lowercase pattern is case-insensitive."""
    if pattern is None:
        return None
    flags = 0 if re.search(r"[A-Z]", re.sub(r"\\.", "", pattern)) else re.I
    try:
        return re.compile(pattern, flags)
    except re.error as error:
        raise InvalidRequest(f"{what}: invalid regular expression: {error}") from None


def roles_of(m: Message) -> set[str]:
    if m.role in ("user", "steer", "command", "interrupt"):
        return {"user"}
    if m.role == "assistant":
        return {"assistant", "final"} if m.final else {"assistant"}
    if m.role == "tool":
        return {"tool", "output"}
    return {m.role}


@dataclass
class Options:
    view: str | None = None
    turns: str | None = None
    grep: str | None = None
    cmd: str | None = None
    errors: bool = False
    roles: set[str] | None = None
    since: float | None = None
    until: float | None = None
    context: int | None = None
    limit: int = DEFAULT_LIMIT
    max_chars: int | None = None
    full: bool = False

    @property
    def search(self) -> bool:
        return bool(self.grep or self.cmd or self.errors)

    @property
    def hits_mode(self) -> bool:
        return self.search and self.view is None and self.context is None

    @property
    def addresses_only(self) -> bool:
        return self.turns is not None and all(
            "." in p for p in self.turns.replace("#", "").split(","))

    def needs_full_results(self) -> bool:
        """Tool results unbounded only where they are printed whole."""
        return self.addresses_only or (self.full and bool(self.roles) and "output" in self.roles)


def options(*, view=None, turns=None, grep=None, cmd=None, errors=False, role=None,
            since=None, until=None, context=None, limit=DEFAULT_LIMIT, max_chars=None,
            full=False, now: float | None = None) -> Options:
    """Validate CLI arguments before any worker is read."""
    if view is not None and view not in VIEWS:
        raise InvalidRequest(f"--view must be one of {', '.join(VIEWS)}")
    roles = None
    if role is not None:
        roles = {r.strip() for r in role.split(",") if r.strip()}
        bad = roles - set(ROLES)
        if bad or not roles:
            raise InvalidRequest(f"--role: unknown role {','.join(sorted(bad)) or '(none given)'} "
                                 f"(use {','.join(ROLES)})")
    check_turns(turns)
    smart_regex(grep)
    smart_regex(cmd, "--cmd")
    for name, value in (("--limit", limit), ("--max-chars", max_chars)):
        if value is not None and value < 1:
            raise InvalidRequest(f"{name} must be at least 1")
    if context is not None and context < 0:
        raise InvalidRequest("-C must be zero or more")
    return Options(view=view, turns=turns, grep=grep, cmd=cmd, errors=errors, roles=roles,
                   since=parse_time(since, now=now), until=parse_time(until, end=True, now=now),
                   context=context, limit=limit, max_chars=max_chars, full=full)


def matcher(opts: Options, turn_span: Callable[[Message], tuple]) -> Callable[[Message], bool] | None:
    """Message predicate for the search and time filters, or None when unfiltered."""
    if not (opts.search or opts.since or opts.until):
        return None
    rx = smart_regex(opts.grep)
    cw = re.compile(opts.cmd, re.I) if opts.cmd else None
    scopes = opts.roles or SEARCH_ROLES

    def in_time(m: Message) -> bool:
        if m.ts is not None:
            return not ((opts.since and m.ts < opts.since) or (opts.until and m.ts >= opts.until))
        start, end = turn_span(m)          # no per-message time: the turn's span
        if opts.since and (end or start or 0) < opts.since:
            return False
        return not (opts.until and (start or 0) >= opts.until)

    def keep(m: Message) -> bool:
        if opts.errors and not (m.role == "tool" and m.err):
            return False
        if (opts.since or opts.until) and not in_time(m):
            return False
        if cw is not None and (m.role != "tool" or not any(
                cw.fullmatch(w) for w in command_words(m.cmd or ""))):
            return False
        if rx is None:
            return True
        fields: list[str] = []
        if m.role in ("user", "steer", "command", "interrupt") and "user" in scopes:
            fields.append(m.text)
        if m.role == "notify" and "notify" in scopes:
            fields += [m.text, m.out]
        if m.role == "assistant" and ("assistant" in scopes or ("final" in scopes and m.final)):
            fields.append(m.text)
        if m.role == "tool":
            if "tool" in scopes:
                fields += [m.cmd or "", m.desc or ""]
            if "output" in scopes:
                fields.append(m.out)
        if m.role == "compact" and "compact" in scopes:
            fields.append(m.out)
        return any(rx.search(f) for f in fields if f)

    return keep


# --- rendering ----------------------------------------------------------------

_TAGS = {"user": "U", "steer": "U+", "notify": "N", "assistant": "A", "tool": "T",
         "compact": "══", "command": "/", "interrupt": "!"}


def tag(m: Message) -> str:
    if m.final == "final":
        return "A*"
    if m.final == "latest":
        return "A…"
    if m.role == "notify" and m.kind:
        return f"N[{m.kind}]"
    return _TAGS[m.role]


def _one(text: str, n: int) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= n else text[: n - 1] + "…"


def cut(text: str, n: int | None, *, keep_end: bool = False) -> str:
    # Don't cut for a remainder shorter than the marker itself.
    if n is None or len(text) <= n + 40:
        return text
    if keep_end:                     # compaction summaries end with current work / next step
        return f"[… {len(text) - n} earlier chars]\n" + text[-n:]
    return text[:n] + f"\n[… {len(text) - n} more chars]"


def tool_label(m: Message, commands: bool, limit: int | None = 160) -> str:
    """One line; with ``limit=None`` the whole command, newlines kept."""
    body = (m.cmd or "") if commands or not m.desc else m.desc
    mark = " ✗" if m.err else ""
    d = f" {_dur(m.dur)}" if m.dur and m.dur >= 5 else ""
    flat = " ".join(body.split())
    if limit is None:
        text = body
    elif len(flat) <= limit:
        text = flat
    else:
        text = flat[:limit] + f" [+{len(flat) - limit} chars]"
    return f"{m.tool}{mark}{d}: {text}"


def compact_line(m: Message) -> str:
    extra = f" · {len(m.out) // 1000}k-char summary" if m.out else ""
    text = f" · {m.text}" if m.text else ""
    return f"══ #{m.turn}.{m.n} compaction{text}{extra}"


_NOISE_WORDS = {"cd", "echo", "head", "tail", "sed", "wc", "sort", "ls", "cat", "true",
                "printf", "sleep"}


def main_word(m: Message) -> str:
    if m.tool not in ("Bash", "shell"):
        return m.tool or "?"
    words = command_words(m.cmd or "")
    useful = [w for w in words if w not in _NOISE_WORDS] or words
    return useful[0] if useful else "sh"


def tool_digest(turn: Turn) -> str:
    tools = [m for m in turn.messages if m.role == "tool"]
    if not tools:
        return ""
    counts: dict[str, int] = {}
    for m in tools:
        counts[main_word(m)] = counts.get(main_word(m), 0) + 1
    top = sorted(counts.items(), key=lambda kv: -kv[1])[:5]
    stamps = [t for m in tools for t in (m.ts, (m.ts or 0) + (m.dur or 0) if m.ts else None) if t]
    span = f" · {_dur(max(stamps) - min(stamps))}" if len(stamps) > 1 else ""
    errs = sum(1 for m in tools if m.err)
    return (f"[{len(tools)} tools{span}" + (f" · {errs}✗" if errs else "") + " · "
            + ", ".join(f"{k}×{v}" if v > 1 else k for k, v in top) + "]")


def idle_gaps(turns: list[Turn]) -> dict[int, float]:
    """Idle time before each turn, on the whole history (not a selection)."""
    gaps, last = {}, None
    for t in turns:
        if last and t.start:
            gaps[t.seq] = t.start - last
        last = t.end or t.start or last
    return gaps


def render_turns(turns: list[Turn], view: str, out: TextIO, *, max_chars: int | None,
                 gaps: dict[int, float], roles: set[str] | None = None,
                 keep: Callable[[Message], bool] | None = None, commands: bool = False) -> None:
    tool_limit = None if max_chars is None else 160
    shown = roles or VIEW_ROLES.get(view, set())
    for t in turns:
        idle = gaps.get(t.seq, 0)
        gap = f"  (+{_dur(idle)} idle)" if idle > 3600 else ""
        if view == "outline":
            _render_outline_turn(t, gap, out)
            continue
        print(f"\n━━ #{t.seq} {_clock(t.start, True)}{gap}", file=out)
        for m in t.messages:
            if m.role == "compact":
                print(f"[.{m.n}] {compact_line(m)}", file=out)
                continue
            if not roles_of(m) & shown:
                continue
            if m.role == "tool":
                if keep is None or keep(m):
                    print(f"    .{m.n}{_msg_clock(m.ts, t.start)} "
                          f"{tool_label(m, commands, tool_limit)}", file=out)
                    if "output" in shown and m.out:
                        print("      " + cut(m.out, max_chars).replace("\n", "\n      "), file=out)
                continue
            body = m.text + (f"\n{m.out}" if m.role == "notify" and m.out else "")
            print(f"[.{m.n} {tag(m)}{_msg_clock(m.ts, t.start)}] {cut(body, max_chars)}", file=out)


def _render_outline_turn(t: Turn, gap: str, out: TextIO) -> None:
    if not t.messages:
        return                  # e.g. a turn of command output only: its number stays a gap
    compacts = [m for m in t.messages if m.role == "compact"]
    heads = [m for m in t.messages if m.role in ("user", "notify", "steer", "interrupt")
             or (m.role == "command" and not (compacts and m.text.startswith("/compact")))]
    final = next((m for m in reversed(t.messages) if m.final), None)
    if heads:
        h = heads[0]
        print(f"#{t.seq}.{h.n} {_clock(t.start, True)}{gap} {tag(h)}: {_one(h.text, 150)}", file=out)
        for m in heads[1:3]:
            print(f"    .{m.n} {tag(m)}: {_one(m.text, 120)}", file=out)
        if len(heads) > 3:
            print(f"    (+{len(heads) - 3} more prompts)", file=out)
    elif not compacts:
        print(f"#{t.seq} {_clock(t.start, True)}{gap} (no prompt)", file=out)
    if final:
        print(f"    .{final.n} {tag(final)}: {_one(final.text, 200)}", file=out)
    digest = tool_digest(t)
    if digest:
        print(f"    {digest}", file=out)
    for c in compacts:
        print(f"   {compact_line(c)} · {_clock(c.ts or t.end, True)}", file=out)


def _section(summary: str, name: str, n: int) -> str:
    m = re.search(rf"^\s*\d+\. {name}[^\n]*:\s*\n(.*?)(?=^\s*\d+\. [A-Z][^\n]*:\s*$|\Z)",
                  summary, re.S | re.M)
    return _one(m.group(1), n) if m else ""


def render_map(turns: list[Turn], out: TextIO) -> None:
    """Segments between compactions and long idle gaps; one title line each."""
    segs, cur, last_ts, why = [], None, None, "start"
    for t in turns:
        if cur and t.start and last_ts and t.start - last_ts > MAP_GAP_HOURS * 3600:
            segs.append((cur, None))
            cur, why = None, f"idle {_dur(t.start - last_ts)}"
        if cur is None:
            cur = {"turns": [], "why": why}
        cur["turns"].append(t)
        last_ts = t.end or t.start or last_ts
        compact = next((m for m in reversed(t.messages) if m.role == "compact"), None)
        if compact is not None:              # the compacting turn closes its segment
            segs.append((cur, compact))
            cur, why = None, f"after compaction #{compact.turn}.{compact.n}"
    if cur:
        segs.append((cur, None))
    msgs = [m for t in turns for m in t.messages]
    tools = [m for m in msgs if m.role == "tool"]
    words: dict[str, int] = {}
    for m in tools:
        words[main_word(m)] = words.get(main_word(m), 0) + 1
    starts = [t.start for t in turns if t.start]
    ends = [t.end or t.start for t in turns if t.end or t.start]
    print(f"{len(turns)} turns · {sum(1 for m in msgs if m.role in ('user', 'steer'))} user msgs · "
          f"{sum(1 for m in msgs if m.role == 'notify')} notifications · "
          f"{sum(1 for m in msgs if m.role == 'compact')} compactions · {len(tools)} tool calls "
          f"({sum(1 for m in tools if m.err)}✗) · {_clock(min(starts) if starts else None, True)} → "
          f"{_clock(max(ends) if ends else None, True)}", file=out)
    if words:
        print("   top: " + ", ".join(f"{k}×{v}" for k, v in
                                    sorted(words.items(), key=lambda kv: -kv[1])[:12]), file=out)
    for seg, closer in segs:
        ts_ = seg["turns"]
        sm = [m for t in ts_ for m in t.messages]
        users = [m for m in sm if m.role in ("user", "steer")]
        notes = sum(1 for t in ts_ if t.messages and t.messages[0].role == "notify")
        st = [t.start for t in ts_ if t.start]
        en = [t.end or t.start for t in ts_ if t.end or t.start]
        print(f"── #{ts_[0].seq}-{ts_[-1].seq}  {_clock(min(st) if st else None, True)} → "
              f"{_clock(max(en) if en else None, True)}  {seg['why']} · {len(users)} user · "
              f"{notes} notify-led turns", file=out)
        title = ""
        if closer is not None and closer.out:
            work = _section(closer.out, "Current Work", 170)
            title = f"#{closer.turn}.{closer.n} compaction, current work: {work}" if work else ""
        if not title and users:
            title = f"#{users[0].turn}.{users[0].n} {tag(users[0])}: {_one(users[0].text, 160)}"
        if not title:
            fin = next((m for m in sm if m.final), None)
            title = f"#{fin.turn}.{fin.n} {tag(fin)}: {_one(fin.text, 160)}" if fin else ""
        if title:
            print(f"   ⤷ {title}", file=out)


def snippet(m: Message, rx: re.Pattern | None, width: int = 220) -> str:
    if m.role == "tool" and rx is None:
        return tool_label(m, True)
    for text in (m.text, m.cmd or "", m.desc or "", m.out):
        found = rx.search(text) if (rx and text) else None
        if found:
            a = max(0, found.start() - width // 2)
            return ("…" if a else "") + _one(text[a: a + width], width)
    return tool_label(m, True) if m.role == "tool" else _one(m.text, width)


def header(h: History, zone: str) -> str:
    starts = [t.start for t in h.turns if t.start]
    ends = [t.end or t.start for t in h.turns if t.end or t.start]
    span = f"{_clock(min(starts), True)} → {_clock(max(ends), True)}" if starts else "no times"
    compacts = [m for m in h.messages() if m.role == "compact"]
    latest = f" · latest compaction #{compacts[-1].turn}.{compacts[-1].n}" if compacts else ""
    cwd = (h.cwd or "").replace(os.path.expanduser("~"), "~", 1)
    name = f" {h.name!r}" if h.name else ""
    return (f"{h.engine}:{h.short}{name} {cwd} · {len(h.turns)} turns · {span}{latest} · "
            f"local times ({zone})")


@dataclass
class Hint:
    short: str
    turn: int
    n: int


def render_history(h: History, opts: Options, out: TextIO, *, many: bool,
                   zone: str) -> Hint | None:
    """Print one worker's part of ``ccx history``; return a next-step hint."""
    if many:
        print(file=out)
    print(header(h, zone), file=out)
    for note in h.notes:
        print(f"  note: {note}", file=out)
    last = h.turns[-1].seq if h.turns else 0
    sel = parse_turns(opts.turns, last)
    if sel.messages:
        _render_addressed(h, sel, opts, out)
        if not sel.turns:
            return None
    span = {t.seq: (t.start, t.end) for t in h.turns}
    keep = matcher(opts, lambda m: span.get(m.turn, (None, None)))
    view = opts.view or ("chat" if (opts.turns or opts.roles or opts.context is not None)
                         else "outline")
    gaps = idle_gaps(h.turns)
    turns = [t for t in h.turns if sel.turns is None or t.seq in sel.turns]
    if view == "map":
        if keep is not None:
            turns = [t for t in turns if any(keep(m) for m in t.messages)]
        render_map(turns, out)
        return None
    if opts.hits_mode:
        return _render_hits(h, turns, keep, opts, out)
    if keep is not None:
        index = [i for i, t in enumerate(turns) if any(keep(m) for m in t.messages)
                 or (not opts.search and not t.messages and _turn_in_range(t, opts))]
        ctx = opts.context or 0
        show = {j for i in index for j in range(i - ctx, i + ctx + 1)}
        turns = [t for i, t in enumerate(turns) if i in show]
    elif sel.turns is None:
        window = DEFAULT_MANY_WINDOW if many else DEFAULT_WINDOW
        if len(turns) > window:
            print(f"(last {window} of {len(turns)} turns; --turns 1- for all, "
                  "--view map for an overview)", file=out)
            turns = turns[-window:]
    if not turns:
        print("  (no matching turns)" if opts.search else "  (no turns in range)", file=out)
    max_chars = None if opts.full else (opts.max_chars or DEFAULT_MAX_CHARS)
    render_turns(turns, view, out, max_chars=max_chars, gaps=gaps, roles=opts.roles,
                 keep=keep if opts.search else None,
                 commands=bool(opts.cmd or (opts.roles and "tool" in opts.roles)))
    return None


def _turn_in_range(t: Turn, opts: Options) -> bool:
    start, end = t.start, t.end or t.start
    return not ((opts.since and (end or 0) < opts.since) or (opts.until and (start or 0) >= opts.until))


def _render_addressed(h: History, sel: Selection, opts: Options, out: TextIO) -> None:
    found = {(m.turn, m.n): m for m in h.messages()}
    for address in sorted(sel.messages):
        m = found.get(address)
        if m is None:
            print(f"(no message #{address[0]}.{address[1]}: it doesn't exist or isn't shown "
                  "by history, e.g. reasoning or bookkeeping)", file=out)
            continue
        if m.role == "tool":
            body = (m.cmd or "") + ("\n--- output ---\n" + m.out if m.out else "")
        elif m.role == "compact":
            body = m.out or ("(no summary stored locally: Codex encrypts compaction summaries)"
                             if h.engine == "codex" else "(no summary text)")
        else:
            body = m.text + (f"\n{m.out}" if m.role == "notify" and m.out else "")
        turn_start = next((t.start for t in h.turns if t.seq == m.turn), None)
        print(f"#{m.turn}.{m.n} {tag(m)} {_clock(m.ts or turn_start, True)}"
              + (f" {m.tool}" if m.tool else ""), file=out)
        print(cut(body, None if opts.full else opts.max_chars, keep_end=m.role == "compact"),
              file=out)


def _render_hits(h: History, turns: list[Turn], keep, opts: Options, out: TextIO) -> Hint | None:
    rx = smart_regex(opts.grep)
    starts = {t.seq: t.start for t in turns}
    hits = [m for t in turns for m in t.messages if keep(m)]
    shown = hits[-opts.limit:]
    if len(hits) > len(shown):
        older = f"--turns 1-{shown[0].turn - 1} or " if shown[0].turn > 1 else ""
        print(f"  {len(hits)} hits, newest {len(shown)} shown (older: {older}--limit {len(hits)})",
              file=out)
    else:
        print(f"  {len(hits)} hit{'s' * (len(hits) != 1)}", file=out)
    with_output = bool(opts.roles and "output" in opts.roles)
    for m in shown:
        print(f"  #{m.turn}.{m.n} {_clock(m.ts or starts.get(m.turn), True)} {tag(m)}: "
              f"{snippet(m, rx)}", file=out)
        if with_output and m.role == "tool" and m.out:
            print(f"      ⤷ {_one(m.out[-200:], 200)}", file=out)     # how it ended
    return Hint(h.short, shown[-1].turn, shown[-1].n) if shown else None


def hint_line(hint: Hint) -> str:
    return (f"next: ccx history {hint.short} --turns {hint.turn}.{hint.n} prints that message "
            f"whole; --turns {max(1, hint.turn - 1)}-{hint.turn + 1} reads the exchange around it")


__all__ = [
    "History", "Message", "Options", "Turn", "command_words", "hint_line", "options",
    "parse_time", "parse_turns", "project", "render_history", "zone_name",
]
