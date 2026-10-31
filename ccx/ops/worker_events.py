"""Owned worker event feed reader with generation-bound nofollow cursors.

Uses the original engine-specific CCX lifecycle projection helpers, not native
provider access or a second name/short address resolver. The cursor binds
the original worker/generation and retained source position; append resumes
by prefix digest and journal rotation uses its original epoch/sequence.
"""
from __future__ import annotations

import base64
import binascii
import errno
import hashlib
import json
import os
import stat
import math
import time
from dataclasses import dataclass
from typing import Mapping, Iterator

from ccx.ops.event_feed import (
    HEADER_METHOD, KINDS, _claude_position, _claude_project,
    _codex_position, _codex_project, header_instance_id, _place,
    POLL_S, HEARTBEAT_DEFAULT_S,
)
from ccx.errors import InvalidRequest, SessionNotFound, worker_not_found
from ccx.store.identity import UnknownWorker

from ccx.store.store import GenerationStore
from ccx.store.root import StorageIntegrityError

CURSOR_PREFIX = "ev2."
# A bounded event page, not a cap on an append-only journal lifetime.
MAX_PAGE_BYTES = 2 * 1024 * 1024
# Claude rotates around 5,000 events. Its retained sidecar is practical to
# search farther back for a sparse kind; keep a byte ceiling for unusually
# large previews. Codex evidence is unbounded and uses forward continuation.
CLAUDE_FILTERED_TAIL_BYTES = 16 * MAX_PAGE_BYTES
CURSOR_ANCHOR_BYTES = 1024
TAIL_STEP_BYTES = 64 * 1024
MAX_PAGE_EVENTS = 1000
MAX_OVERSIZED_SKIPS = 16  # continuation instead of scanning unlimited bad rows


class EventCursorInvalid(StorageIntegrityError):
    """Cursor no longer belongs to this exact generation/worker/source bytes."""


@dataclass(frozen=True)
class _Positioned:
    offset: int
    obj: dict
    epoch: str | None
    seq: int


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _cursor_payload(store: GenerationStore, engine: str, worker_id: str,
                    fd: int | None, offset: int, inode: int | None,
                    epoch: str | None = None, seq: int = 0) -> dict:
    # Bind the root, owner, generation and byte boundary with a bounded anchor
    # so polling does not hash the entire journal.
    anchor = min(offset, CURSOR_ANCHOR_BYTES)
    prefix = os.pread(fd, anchor, offset-anchor) if fd is not None else b""
    return {
        "v": 2, "root": _hash(os.fsencode(store.root)),
        "generation": store.generation, "engine": engine, "workerId": worker_id,
        "inode": inode, "offset": offset, "anchorBytes": anchor,
        "prefixSha256": _hash(prefix), "p": epoch, "q": seq,
    }


def _encode_cursor(payload: Mapping) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return CURSOR_PREFIX + base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _decode_cursor(value: str, expected: Mapping, fd: int | None,
                   size: int, engine: str
                   ) -> tuple[int, bool, str | None, int]:
    if (not isinstance(value, str) or not value.startswith(CURSOR_PREFIX)
            or len(value) > 2048):
        raise EventCursorInvalid("stale legacy/invalid cursor: full-UUID ev2 required")
    try:
        part = value[len(CURSOR_PREFIX):]
        supplied = json.loads(base64.urlsafe_b64decode(
            part + "=" * (-len(part) % 4),
        ))
    except (ValueError, TypeError, UnicodeError, binascii.Error) as exc:
        raise EventCursorInvalid("invalid event feed cursor payload") from exc
    if not isinstance(supplied, dict):
        raise EventCursorInvalid("event feed cursor must be an object")
    for key in ("v", "root", "generation", "engine", "workerId"):
        if supplied.get(key) != expected[key]:
            raise EventCursorInvalid("event cursor from other root/generation/full worker")
    offset, epoch, seq = supplied.get("offset"), supplied.get("p"), supplied.get("q")
    if (type(offset) is not int or offset < 0
            or (epoch is not None and (not isinstance(epoch, str) or not epoch))
            or type(seq) is not int or seq < 0):
        raise EventCursorInvalid("invalid event cursor original byte/epoch/seq position")
    anchor = supplied.get("anchorBytes")
    if type(anchor) is not int or not 0 <= anchor <= min(offset, CURSOR_ANCHOR_BYTES):
        raise EventCursorInvalid("invalid event cursor anchorBytes")
    valid = offset <= size and (offset == 0 or (
        fd is not None and os.pread(fd, 1, offset-1) == b"\n"))
    if valid:
        window = os.pread(fd, anchor, offset-anchor) if fd is not None else b""
        valid = supplied.get("prefixSha256") == _hash(window)
    return offset, valid, epoch, seq


def _lines(engine: str, raw: bytes, *, start: int = 0,
           epoch: str | None = None) -> tuple[list[_Positioned], int]:
    lines: list[_Positioned] = []
    offset = start
    for part in raw[start:].splitlines(keepends=True):
        if not part.endswith(b"\n"):
            break  # in-flight partial final line is neither published nor cursored
        offset += len(part)
        try:
            value = json.loads(part)
        except (ValueError, UnicodeError):
            continue  # historical malformed evidence is not a worker alias
        if not isinstance(value, dict):
            continue
        if engine == "claude":
            line_epoch, seq = _claude_position(value)
        else:
            if value.get("method") == HEADER_METHOD:
                epoch = header_instance_id(value)
            line_epoch, seq = epoch, _codex_position(value)
        if seq is not None:
            lines.append(_Positioned(offset, value, line_epoch, seq))
    return lines, offset


def _pinned_epoch(fd: int | None, size: int) -> str | None:
    if fd is None or not size:
        return None
    prefix = os.pread(fd, min(TAIL_STEP_BYTES, size), 0)
    for line in prefix.splitlines():
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError):
            continue
        if isinstance(event, dict) and event.get("method") == HEADER_METHOD:
            return header_instance_id(event)
    return None


def _last_complete(fd: int | None, size: int) -> int:
    if fd is None or not size:
        return 0
    # A producer may be partway through one large JSONL event at EOF. Keep
    # walking backward to the last complete newline, even when that partial
    # row spans several chunks. Never mistake unfinished bytes for a journal
    # integrity failure, and never search before this pinned file's start.
    end = size
    while end > 0:
        window = min(TAIL_STEP_BYTES, end)
        begin = end - window
        chunk = os.pread(fd, window, begin)
        cut = chunk.rfind(b"\n")
        if cut >= 0:
            return begin + cut + 1
        end = begin
    return 0


def _bounded_lines(engine: str, fd: int | None, start: int, size: int,
                   *, epoch: str | None = None
                   ) -> tuple[list[_Positioned], int, bool, dict | None]:
    if fd is None or start >= size:
        return [], start, False, None
    pos, skipped, skipped_bytes = start, 0, 0

    def gap():
        return ({"reason": "oversized_event_line", "count": skipped,
                 "skipped_bytes": skipped_bytes,
                 "note": "oversized journal event(s) skipped"} if skipped else None)

    while pos < size:
        length = min(MAX_PAGE_BYTES, size-pos)
        raw = os.pread(fd, length, pos)
        parsed, consumed = _lines(engine, raw, epoch=epoch)
        if consumed:
            absolute = [_Positioned(pos+x.offset, x.obj, x.epoch, x.seq)
                        for x in parsed]
            end = pos+consumed
            return absolute, end, end < size and length == MAX_PAGE_BYTES, gap()
        if len(raw) < MAX_PAGE_BYTES:
            break  # only an unfinished final line remains
        # A complete oversized line is advisory, not bad worker identity.
        # Search for its newline in fixed-size preads, never buffering it.
        scan, boundary = pos+len(raw), None
        while scan < size:
            chunk = os.pread(fd, min(TAIL_STEP_BYTES, size-scan), scan)
            if not chunk:
                break
            newline = chunk.find(b"\n")
            if newline >= 0:
                boundary = scan+newline+1
                break
            scan += len(chunk)
        if boundary is None:
            break  # unfinished event: do not cursor partial bytes
        skipped += 1
        skipped_bytes += boundary-pos
        pos = boundary  # exact newline boundary remains cursor-verifiable
        if skipped >= MAX_OVERSIZED_SKIPS:
            return [], pos, pos < size, gap()
    return [], pos, False, gap()


def read_full_uuid_event_page(
    store: GenerationStore,
    target: str,
    *, after: str | None = None,
    tail: int | None = None,
    limit: int = MAX_PAGE_EVENTS,
    kinds: list[str] | None = None,
    native: bool = False,
) -> dict:
    """Pin a private inode, then inspect complete lines up to the fstat size.

    Concurrent append is normal: never require size/mtime to stay unchanged
    while parsing. Reading a page is bounded independent of journal age.
    """
    if (type(limit) is not int or not 1 <= limit <= MAX_PAGE_EVENTS
            or tail is not None and (type(tail) is not int or not 0 <= tail <= MAX_PAGE_EVENTS)
            or tail is not None and after is not None):
        raise InvalidRequest("invalid event page limit/tail/after combination")
    wanted = None
    if kinds is not None:
        if (not isinstance(kinds, list) or any(not isinstance(k, str) for k in kinds)
                or not set(kinds).issubset(KINDS)):
            raise InvalidRequest("unknown event feed kind filter")
        wanted = frozenset(kinds)
    with store.lease():
        owner, _record = store._owned(target)
        filename = "events.ndjson" if owner.engine == "claude" else "evidence.jsonl"
        fd = None
        try:
            with store._gen("workers", owner.engine, owner.worker_id) as directory:
                try:
                    fd = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                 dir_fd=directory)
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                        raise StorageIntegrityError(
                            "unsafe full-ID event journal inode"
                        ) from exc
                    raise
                if fd is not None:
                    actual = os.fstat(fd)
                    named = os.stat(filename, dir_fd=directory, follow_symlinks=False)
                    if (not stat.S_ISREG(actual.st_mode)
                            or actual.st_uid != os.getuid() or actual.st_mode & 0o077
                            or (actual.st_dev, actual.st_ino)
                            != (named.st_dev, named.st_ino)):
                        raise StorageIntegrityError("unsafe full-ID event journal inode")
                    size, inode = actual.st_size, actual.st_ino
                else:
                    size, inode = 0, None
            reference = _cursor_payload(store, owner.engine, owner.worker_id, fd, 0, inode)
            offset, valid, epoch, seq = (
                _decode_cursor(after, reference, fd, size, owner.engine)
                if after else (0, True, None, 0)
            )
            gap = None
            if not valid:
                # File rotation/replacement/truncation: keep existing epoch/seq
                # placement and gap semantics for retained records.
                replay, _, _, skipped_gap = _bounded_lines(owner.engine, fd, 0, size)
                from ccx.ops.event_feed import _Line
                placed, gap = _place(
                    owner.engine,
                    [_Line(x.epoch, x.seq, x.offset, x.obj) for x in replay],
                    {"p": epoch, "q": seq},
                )
                gap = gap or skipped_gap
                if placed:
                    first = placed[0].end
                    before = os.pread(fd, min(first, TAIL_STEP_BYTES), max(0,first-TAIL_STEP_BYTES))
                    cut = before[:-1].rfind(b"\n")
                    offset = first-len(before)+cut+1 if cut >= 0 else 0
                else:
                    offset = 0 if gap else size
                epoch, seq = None, 0

            if tail is not None:
                if tail == 0:
                    parsed, end_offset, unread = [], _last_complete(fd, size), False
                    if fd is not None and end_offset:
                        # From-now must carry the last actual epoch/sequence.
                        # Otherwise a same-inode replacement of a nonempty
                        # journal looks like an empty-source cursor and hides a
                        # real rotation gap.
                        length = min(end_offset, TAIL_STEP_BYTES)
                        start = end_offset - length
                        chunk = os.pread(fd, length, start)
                        skip = 0 if start == 0 else chunk.find(b"\n") + 1
                        if skip or start == 0:
                            last_rows, _ = _lines(
                                owner.engine, chunk[skip:],
                                epoch=(_pinned_epoch(fd, size)
                                       if owner.engine == "codex" else None),
                            )
                            if last_rows:
                                epoch, seq = last_rows[-1].epoch, last_rows[-1].seq
                else:
                    # Claude rotates after ~5,000 lines: for a kind-filtered
                    # tail search its retained events farther back. An
                    # unbounded Codex journal (or an oversized Claude file)
                    # uses a truthful forward continuation instead of claiming
                    # that no older matches exist.
                    total = 0
                    parsed = []
                    raw = b""
                    eligible = 0
                    end_offset, unread = _last_complete(fd, size), False
                    scan_budget = min(end_offset, (
                        CLAUDE_FILTERED_TAIL_BYTES
                        if owner.engine == "claude" and wanted is not None and not native
                        else MAX_PAGE_BYTES
                    ))
                    # Grow reverse windows geometrically: never reread backing
                    # bytes, and avoid reparsing an entire growing prefix at
                    # every 64 KiB step on a large retained sidecar.
                    while total < scan_budget:
                        step = min(max(TAIL_STEP_BYTES, total),
                                   end_offset-total, scan_budget-total)
                        begin = end_offset-total-step
                        raw = os.pread(fd, step, begin) + raw
                        total += step
                        skip = 0 if begin == 0 else raw.find(b"\n")+1
                        if begin and skip == 0:
                            continue
                        source, _consumed = _lines(
                            owner.engine, raw[skip:],
                            epoch=(_pinned_epoch(fd, size)
                                   if owner.engine == "codex" else None),
                        )
                        parsed = [
                            _Positioned(begin+skip+x.offset,x.obj,x.epoch,x.seq)
                            for x in source
                        ]
                        # Project only the records in this bounded window.
                        # The producer is responsible for compact identity,
                        # admission and exactly-once terminal emission.
                        eligible = 0
                        for candidate in parsed:
                            projection = (
                                candidate.obj if native else
                                (_claude_project(candidate.obj)
                                 if owner.engine == "claude"
                                 else _codex_project(candidate.obj))
                            )
                            if projection is not None and (
                                native or wanted is None
                                or projection.get("kind") in wanted
                            ):
                                eligible += 1
                        if eligible >= tail or begin == 0:
                            unread = begin > 0
                            break
                    else:
                        unread = total < end_offset
                    if owner.engine == "codex":
                        seed = _pinned_epoch(fd, size)
                        parsed = [_Positioned(x.offset,x.obj,x.epoch or seed,x.seq)
                                  for x in parsed]
            else:
                if owner.engine == "codex" and epoch is None:
                    epoch = _pinned_epoch(fd, size)
                parsed, end_offset, unread, skipped_gap = _bounded_lines(
                    owner.engine, fd, offset, size, epoch=epoch,
                )
                gap = gap or skipped_gap
            selected: list[tuple[_Positioned, dict]] = []
            for line in parsed:
                projection = (line.obj if native else
                              (_claude_project(line.obj) if owner.engine == "claude"
                               else _codex_project(line.obj)))
                if projection is None:
                    continue
                if not native and wanted is not None and projection.get("kind") not in wanted:
                    continue
                selected.append((line, projection))
            if tail is not None:
                selected = selected[-tail:] if tail else []
                # If the budget ended before finding N matches, the source
                # still has unscanned RETAINED bytes. The ordinary forward
                # reader can recover them from the source-head cursor.
                more = bool(tail and unread and len(selected) < tail)
            else:
                more = len(selected) > limit or unread
                selected = selected[:limit]
            events = []
            for line, projection in selected:
                if native:
                    events.append(projection)
                else:
                    boundary = _cursor_payload(store, owner.engine, owner.worker_id,
                                               fd, line.offset, inode, line.epoch, line.seq)
                    events.append({
                        **projection, "worker": owner.worker, "worker_id": owner.worker_id,
                        "engine": owner.engine, "short": owner.short,
                        "epoch": line.epoch, "seq": line.seq,
                        "cursor": _encode_cursor(boundary),
                    })
            if more and tail is not None:
                # Tail goes newest-to-oldest, forward pagination goes oldest-to-
                # newest. A source-head continuation (not EOF) ensures a
                # caller can actually recover retained matches we did not scan.
                resume, boundary_epoch, boundary_seq = 0, None, 0
            else:
                boundary = selected[-1][0] if more and selected else (parsed[-1] if parsed else None)
                resume = selected[-1][0].offset if more and selected else end_offset
                boundary_epoch = boundary.epoch if boundary else epoch
                boundary_seq = boundary.seq if boundary else seq
            final_cursor = _encode_cursor(_cursor_payload(
                store, owner.engine, owner.worker_id, fd, resume, inode,
                boundary_epoch, boundary_seq,
            ))
            if fd is not None:
                final_stat = os.fstat(fd)
                if final_stat.st_size < max(size, resume):
                    # The pinned inode shrank. Appends are fine; truncation
                    # invalidates a previously complete native journal view.
                    raise EventCursorInvalid("event journal truncated during read")
                with store._gen("workers", owner.engine, owner.worker_id) as directory:
                    try:
                        named = os.stat(filename, dir_fd=directory,
                                        follow_symlinks=False)
                    except FileNotFoundError as exc:
                        raise EventCursorInvalid(
                            "event journal removed during read"
                        ) from exc
                    if ((named.st_dev, named.st_ino) !=
                        (final_stat.st_dev, final_stat.st_ino)):
                        raise EventCursorInvalid(
                            "event journal inode replaced during read"
                        )
                    if (not stat.S_ISREG(named.st_mode)
                            or named.st_uid != os.getuid() or named.st_mode & 0o077):
                        raise StorageIntegrityError(
                            "unsafe full-ID event journal inode"
                        )
            store.revalidate()
            return {
                "worker": owner.worker, "worker_id": owner.worker_id,
                "engine": owner.engine, "short": owner.short,
                "events": events, "next_cursor": final_cursor,
                "has_more": more, "gap": gap, "source": "owned-ccx-journal",
            }
        finally:
            if fd is not None:
                os.close(fd)


def _source_marker(store: GenerationStore, target: str) -> tuple | None:
    """Stat only the original owned journal, without hashing or holding a wait lease."""
    with store.lease():
        owner, _record = store._owned(target)
        filename = "events.ndjson" if owner.engine == "claude" else "evidence.jsonl"
        try:
            with store._gen("workers", owner.engine, owner.worker_id) as fd:
                metadata = os.stat(filename, dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        if (not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid() or metadata.st_mode & 0o077):
            raise StorageIntegrityError("unsafe full-ID event journal inode")
        return (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns)


def _recorded_state(store: GenerationStore, target: str) -> dict:
    """Original record's state; a follower never probes native host or daemon."""
    record = store.record(target)
    from ccx.ops.event_feed import state_of
    return {**state_of(record), "state_source": "recorded"}


def wait_full_uuid_event_page(
    store: GenerationStore, target: str, *, after: str | None = None,
    tail: int | None = None, limit: int = 200, kinds: list[str] | None = None,
    wait: float = 0.0, native: bool = False,
) -> dict:
    """One same-generation held page; stat the owned journal before re-hashing.

    Every page carries ``runtime`` (``live``/``gone``/``unknown``, see
    :mod:`runtime_presence`), observed read-only while the read is held. An
    unheld page is one sample, so it is never ``gone``. A held read that sees
    its live runtime go returns at once, empty; one that starts with the
    runtime already gone holds as usual and reports ``gone`` at the end.
    """
    from ccx.store.runtime_presence import GONE, RuntimeObserver
    if (type(wait) not in (int, float) or not math.isfinite(wait) or wait < 0):
        raise InvalidRequest("event wait must be nonnegative finite seconds")
    marker = _source_marker(store, target) if wait else None
    page = read_full_uuid_event_page(
        store, target, after=after, tail=tail, limit=limit, kinds=kinds,
        native=native,
    )
    observer = RuntimeObserver(store, target)
    observer.sample()

    def done(current: dict, gap, timed_out: bool) -> dict:
        return {**current, **_recorded_state(store, target),
                "gap": gap, "timed_out": timed_out, "runtime": observer.value}

    if not wait or page["events"] or page["has_more"] or page["gap"]:
        return done(page, page["gap"], False)
    deadline = time.monotonic() + wait
    gap = page["gap"]
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            observer.sample()
            return done(page, gap, True)
        time.sleep(min(POLL_S, remaining))
        # The source is validated (and any refusal raised) before presence:
        # a runtime going away never stands in for a changed journal.
        current = _source_marker(store, target)
        if current != marker:
            marker = current
            page = read_full_uuid_event_page(
                store, target, after=page["next_cursor"], limit=limit, kinds=kinds,
                native=native,
            )
            gap = gap or page["gap"]
            if page["events"] or page["has_more"] or gap:
                return done(page, gap, False)
        if observer.sample() == GONE and observer.seen_live:
            return done(page, gap, False)


def follow_full_uuid_events(
    store: GenerationStore, target: str, *, after: str | None = None,
    tail: int | None = None, kinds: list[str] | None = None,
    timeout: float | None = None,
    heartbeat: float | None = HEARTBEAT_DEFAULT_S,
    native: bool = False,
) -> Iterator[dict]:
    """Replay then follow the original owner's event page, not a native socket."""
    for name, value in (("timeout", timeout), ("heartbeat", heartbeat)):
        if value is not None and (
            type(value) not in (int, float) or not math.isfinite(value) or value < 0
        ):
            raise InvalidRequest(f"event follow {name} must be nonnegative finite or None")
    deadline = None if timeout is None else time.monotonic() + timeout
    try:
        # Take the end-of-journal cursor first: a short tail page continues
        # from the journal head, so follow from here instead (an event landing
        # between the two reads may show twice, never zero times).
        start = wait_full_uuid_event_page(
            store, target, tail=0, kinds=kinds, limit=MAX_PAGE_EVENTS,
            native=native,
        ) if tail else None
        page = wait_full_uuid_event_page(
            store, target, after=after, tail=tail, kinds=kinds, limit=MAX_PAGE_EVENTS,
            native=native,
        )

        def beat(current: dict, gap: dict | None = None) -> dict:
            return {
                "kind": "heartbeat", "type": "heartbeat",
                "worker": current["worker"], "worker_id": current["worker_id"],
                "engine": current["engine"], "short": current["short"],
                "cursor": current["next_cursor"], "gap": gap,
                "ts": time.time(),
                "state": current["state"], "native_state": current["native_state"],
                "state_source": "recorded", "runtime": current["runtime"],
            }

        gap = page["gap"]  # A later replay page must not erase the first gap.
        if start is not None and page["has_more"]:
            yield from page["events"]
            page = start
        while True:
            yield from page["events"]
            if not page["has_more"]:
                break
            page = wait_full_uuid_event_page(
                store, target, after=page["next_cursor"], kinds=kinds,
                limit=MAX_PAGE_EVENTS, native=native,
            )
            gap = gap or page["gap"]
        cursor = page["next_cursor"]
        yield beat(page, gap)
        runtime = page["runtime"]
        last_output = time.monotonic()
        while deadline is None or time.monotonic() < deadline:
            remaining = (None if deadline is None
                         else max(0.0, deadline - time.monotonic()))
            period = heartbeat if heartbeat else HEARTBEAT_DEFAULT_S
            duration = period if remaining is None else min(period, remaining)
            page = wait_full_uuid_event_page(
                store, target, after=cursor, kinds=kinds,
                limit=MAX_PAGE_EVENTS, wait=duration, native=native,
            )
            # Presence comes from the held read only; an unheld page is one sample.
            changed, runtime = page["runtime"] != runtime, page["runtime"]
            cursor = page["next_cursor"]
            if page["gap"]:
                yield beat(page, page["gap"])
                last_output = time.monotonic()
            if page["events"]:
                yield from page["events"]
                last_output = time.monotonic()
            while page["has_more"]:
                page = wait_full_uuid_event_page(
                    store, target, after=cursor, kinds=kinds, limit=MAX_PAGE_EVENTS,
                    native=native,
                )
                cursor = page["next_cursor"]
                if page["gap"]:
                    yield beat(page, page["gap"])
                if page["events"]:
                    yield from page["events"]
                    last_output = time.monotonic()
            # A runtime presence change is a beat at once, even after events.
            if heartbeat and (changed or (
                    not page["events"]
                    and time.monotonic() - last_output >= heartbeat)):
                yield beat({**page, "runtime": runtime})
                last_output = time.monotonic()
    except UnknownWorker as exc:
        raise worker_not_found(target) from exc
