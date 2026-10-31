"""Persistent stdio transport for Protocol V1 (`ccx rpc --serve`).

The one-shot transport pays interpreter start-up and the full CCX import on
every request. Here a warm parent reads newline-delimited request envelopes
and forks one child per request. The child runs exactly the one-shot
`respond` path, so each request keeps today's process-per-request semantics:
its own locks, caches and failure domain. Only the import is shared.

The parent never dispatches. It owns the streams, correlates each child's
single response line with its request, and enforces a hard deadline so every
request is answered within its budget even if a child wedges.
"""

from __future__ import annotations

import json
import os
import selectors
import signal
import stat
import sys
import time
from dataclasses import dataclass, field
from typing import Any

from ccx.system import oslayer
from ccx.rpc.local_rpc import (
    CANCEL_OP,
    DEFAULT_TIMEOUT_S,
    MAX_MESSAGE_BYTES,
    PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    _error,
    _error_from_exception,
    _params,
    _request_id,
    _required_text,
    _serialized,
    _success,
    _timeout_seconds,
    _validate_operation_params,
    parse_request,
    respond,
)

# Requests a single serve process runs at once; beyond it, refused as data.
MAX_IN_FLIGHT = 256
# How long past its own budget a child may run before the parent kills it.
DEADLINE_GRACE_S = 5.0
# Requests that only observe. A child running one dies with the parent: its
# answer has nowhere to go, and a held wait could otherwise outlive the server
# by hours. A mutating request is left to finish, as a one-shot `ccx rpc`
# would be when its caller dies; it has its own bounded timeout.
READ_ONLY_OPS = frozenset({
    "system.info", "worker.inspect", "worker.wait", "turn.reconcile",
    "worker.report", "worker.telemetry", "worker.events",
    "worker.compact.status",
    "transcript.read", "conversation.page", "account.usage.read",
    "notification.inspect",
})
_REAP_INTERVAL_S = 1.0


@dataclass
class _Child:
    pid: int
    fd: int
    request_id: str | None
    op: str | None
    deadline: float
    buf: bytearray = field(default_factory=bytearray)
    # Reaped already: its pid may belong to another process now.
    exited: bool = False


def _envelope_field(raw: bytes, name: str) -> str | None:
    """Best-effort text field of a request envelope; never raises."""
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError):
        return None
    field_value = value.get(name) if isinstance(value, dict) else None
    return field_value if isinstance(field_value, str) else None


def _deadline_of(raw: bytes) -> float:
    try:
        value = json.loads(raw)
        budget = _timeout_seconds(value, DEFAULT_TIMEOUT_S) if isinstance(value, dict) else DEFAULT_TIMEOUT_S
    except Exception:
        budget = DEFAULT_TIMEOUT_S
    return time.monotonic() + budget + DEADLINE_GRACE_S


def _max_fd() -> int:
    try:
        return int(os.sysconf("SC_OPEN_MAX"))
    except (OSError, ValueError):
        return 65536


def _run_child(raw: bytes, write_fd: int, parent: int, generation: str) -> None:
    """Child side: answer one request on ``write_fd``, then exit."""
    try:
        if _envelope_field(raw, "op") in READ_ONLY_OPS:
            oslayer.arm_parent_death(parent)
        # Keep the parent's streams out of reach: stray prints go to stderr and
        # nothing a request spawns can read the request stream.
        devnull = os.open(os.devnull, os.O_RDONLY)
        os.dup2(devnull, 0)
        os.close(devnull)
        os.dup2(2, 1)
        # Nothing else the parent holds (its streams, the selector, sibling
        # pipes) may outlive this request in a process it spawns.
        os.closerange(3, write_fd)
        os.closerange(write_fd + 1, _max_fd())
        payload = respond(lambda: parse_request(raw),
                          expected_generation=generation)
        view = memoryview(payload)
        while view:
            view = view[os.write(write_fd, view):]
    finally:
        os._exit(0)


class _Server:
    def __init__(self, in_fd: int, out_fd: int, generation: str) -> None:
        if not isinstance(generation, str) or not generation:
            raise ValueError("serve requires the original ACTIVE generation")
        self.generation = generation
        self.in_fd = in_fd
        self.out_fd = out_fd
        # epoll refuses a regular file and kqueue never reports its EOF;
        # select() treats it as always readable, which is what it is.
        self.selector = (selectors.SelectSelector()
                         if stat.S_ISREG(os.fstat(in_fd).st_mode)
                         else selectors.DefaultSelector())
        self.children: dict[int, _Child] = {}
        self.pending = bytearray()
        self.discarding = False
        self.eof = False

    def emit(self, payload: bytes) -> None:
        view = memoryview(payload)
        while view:
            view = view[os.write(self.out_fd, view):]

    def refuse(self, request_id: str | None, code: str, message: str, **details: Any) -> None:
        self.emit(_serialized(_error(request_id, code, message, details=details or None)))

    def start(self, raw: bytes) -> None:
        request_id = _envelope_field(raw, "request_id")
        op = _envelope_field(raw, "op")
        if op == CANCEL_OP:
            # Answered here: the request it names lives in this process.
            self.cancel(raw, request_id)
            return
        if len(self.children) >= MAX_IN_FLIGHT:
            self.refuse(request_id, "runtime_unavailable",
                        "serve concurrency limit reached", limit=MAX_IN_FLIGHT)
            return
        if request_id is not None and any(
            child.request_id == request_id for child in self.children.values()
        ):
            # Responses are matched by request_id, so two in flight would be
            # indistinguishable to the caller.
            self.refuse(request_id, "conflict",
                        "a request with this request_id is already in flight")
            return
        read_fd, write_fd = os.pipe()
        parent = os.getpid()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            _run_child(raw, write_fd, parent, self.generation)
        os.close(write_fd)
        os.set_blocking(read_fd, False)
        child = _Child(pid, read_fd, request_id, op, _deadline_of(raw))
        self.children[read_fd] = child
        self.selector.register(read_fd, selectors.EVENT_READ, child)

    def cancel(self, raw: bytes, request_id: str | None) -> None:
        """Answer ``request.cancel``: stop one in-flight observation.

        Only a request in ``READ_ONLY_OPS`` is stopped; it is answered with
        ``cancelled`` before the cancel's own result. A mutation is left to
        finish, since killing it would only turn its answer into a lost reply.
        """
        try:
            request = parse_request(raw)
            request_id = _request_id(request.get("request_id"))
            if request.get("protocol_version") != PROTOCOL_VERSION:
                self.refuse(
                    request_id, "unsupported_protocol_version",
                    f"unsupported protocol_version {request.get('protocol_version')!r}",
                    supported_protocol_versions=list(SUPPORTED_PROTOCOL_VERSIONS),
                )
                return
            params = _params(request)
            _validate_operation_params(CANCEL_OP, params)
            target = _required_text(params, "cancel_request_id")
        except Exception as error:
            self.emit(_serialized(_error_from_exception(request_id, error)))
            return
        if any(child.request_id == request_id for child in self.children.values()):
            self.refuse(request_id, "conflict",
                        "a request with this request_id is already in flight")
            return
        child = next(
            (c for c in self.children.values() if c.request_id == target), None
        )
        if child is not None:
            # An answer already in the pipe wins over the cancellation.
            self.read_child(child)
            if child.fd not in self.children:
                child = None
        if child is None:
            reason = "not_in_flight"
        elif child.op not in READ_ONLY_OPS:
            reason = "not_cancellable"
        else:
            self.drop(child, "cancelled", "request cancelled by request.cancel")
            reason = None
        self.emit(_serialized(_success(request_id, {
            "cancel_request_id": target,
            "cancelled": reason is None,
            "reason": reason,
        })))

    def drop(self, child: _Child, code: str, message: str) -> None:
        """Kill an unanswered child and answer its request with ``code``."""
        if not child.exited:
            try:
                os.kill(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.selector.unregister(child.fd)
        os.close(child.fd)
        del self.children[child.fd]
        self.refuse(child.request_id, code, message)
        if not child.exited:
            try:
                os.waitpid(child.pid, 0)
            except ChildProcessError:
                pass

    def finish(self, child: _Child, payload: bytes | None) -> None:
        self.selector.unregister(child.fd)
        os.close(child.fd)
        del self.children[child.fd]
        if payload is None:
            self.refuse(child.request_id, "internal_error",
                        "request process exited without a response")
        else:
            self.emit(payload)
        # The response is complete; reap without waiting on anything the
        # request may have spawned.
        if not child.exited:
            try:
                os.waitpid(child.pid, os.WNOHANG)
            except ChildProcessError:
                pass

    def read_child(self, child: _Child) -> None:
        try:
            chunk = os.read(child.fd, 65536)
        except BlockingIOError:
            if child.exited:
                # Dead, its output drained, and a process it spawned keeps the
                # pipe open: no line is coming.
                self.finish(child, None)
            return
        if chunk:
            child.buf.extend(chunk)
            # A response is exactly one line; stop at it rather than waiting
            # for EOF, which a process the request spawned could hold open.
            end = child.buf.find(b"\n")
            if end >= 0:
                self.finish(child, bytes(child.buf[: end + 1]))
            elif child.exited:
                self.read_child(child)
            return
        self.finish(child, None)

    def read_input(self) -> None:
        chunk = os.read(self.in_fd, 65536)
        if not chunk:
            self.eof = True
            self.selector.unregister(self.in_fd)
            if self.pending.strip() and not self.discarding:
                self.start(bytes(self.pending))
            self.pending.clear()
            return
        self.pending.extend(chunk)
        while True:
            end = self.pending.find(b"\n")
            if end < 0:
                if len(self.pending) > MAX_MESSAGE_BYTES and not self.discarding:
                    self.discarding = True
                    self.refuse(None, "invalid_request",
                                "protocol request exceeds the 4 MiB V1 limit",
                                limit_bytes=MAX_MESSAGE_BYTES)
                if self.discarding:
                    self.pending.clear()
                return
            line = bytes(self.pending[:end])
            del self.pending[: end + 1]
            if self.discarding:
                self.discarding = False
                continue
            if len(line) > MAX_MESSAGE_BYTES:
                self.refuse(None, "invalid_request",
                            "protocol request exceeds the 4 MiB V1 limit",
                            limit_bytes=MAX_MESSAGE_BYTES)
                continue
            if line.strip():
                self.start(line)

    def expire(self) -> None:
        now = time.monotonic()
        for child in [c for c in self.children.values() if c.deadline <= now]:
            self.drop(child, "timeout",
                      "request exceeded its timeout_ms budget in serve mode")

    def reap(self) -> None:
        # Children whose response was already forwarded may exit later. A
        # child still tracked is answered from what it wrote: its pipe is
        # drained now, since a process it spawned may hold it open forever.
        while True:
            try:
                pid, _status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if pid == 0:
                return
            for child in list(self.children.values()):
                if child.pid == pid:
                    child.exited = True
                    self.read_child(child)

    def run(self) -> int:
        self.selector.register(self.in_fd, selectors.EVENT_READ, None)
        while not (self.eof and not self.children):
            soonest = min((c.deadline for c in self.children.values()), default=None)
            # While requests run, wake at least every second to reap a child
            # that died without a response (its pipe may never close).
            wait = None if soonest is None else min(
                _REAP_INTERVAL_S, max(0.0, soonest - time.monotonic()))
            for key, _events in self.selector.select(wait):
                if key.data is None:
                    self.read_input()
                elif key.fd in self.children:
                    self.read_child(key.data)
            self.expire()
            self.reap()
        return 0


def serve(in_fd: int | None = None, out_fd: int | None = None) -> int:
    """Run the original per-request process server on one pinned ACTIVE generation."""
    from ccx.constants import CCX_ROOT
    from ccx.store.root import GenerationError, open_or_initialize
    try:
        generation = open_or_initialize(CCX_ROOT)[0].generation
    except (GenerationError, OSError) as exc:
        print(f"ccx rpc --serve unavailable: {exc}", file=sys.stderr)
        return 3
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    in_fd = sys.stdin.fileno() if in_fd is None else in_fd
    out_fd = sys.stdout.fileno() if out_fd is None else out_fd
    return _Server(in_fd, out_fd, generation).run()


__all__ = ["DEADLINE_GRACE_S", "DEFAULT_TIMEOUT_S", "MAX_IN_FLIGHT", "serve"]
