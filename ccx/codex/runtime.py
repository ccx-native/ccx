"""Shared, non-presentational operations for CCX-owned Codex workers.

Both ``ccx codex`` and :class:`ccx.CodexClient` use these primitives. Nothing
here prints, parses command output, or exits the importing process.
"""

from __future__ import annotations

from ccx.version import ccx_version

import fcntl
import json
import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from ccx.codex.paths import (
    codexd_lifetime_lock_path,
    codexd_log_path,
    codexd_sock_path,
    codexd_spawn_lock_path,
    ensure_dirs,
    root as codex_state_root,
)
from ccx.constants import (
    CODEXD_RECOVERY_OPS,
    CODEXD_RPC_GENERATION,
)
from ccx.codex.protocol import AppServerClient, ProtocolError
from ccx.codex.reports import report_from_thread
from ccx.codex.records import read_record
from ccx.errors import (
    CcxError,
    InvalidRequest,
    SessionNotFound,
    SessionUnavailable,
    SpawnFailed,
)
from ccx.store.worker_state import (
    RUNTIME_ABSENT,
    RUNTIME_INCONCLUSIVE,
    RUNTIME_SERVED,
)

STABLE_STATES = {"idle", "waiting", "idle-killed", "crashed", "dead"}
# App-server boot; a load deadline still overrides this.
CODEXD_BOOT_TIMEOUT_S = 60
# How long a caller without a deadline waits for a stopping codexd to exit. Its
# shutdown closes the app-server (up to three 3s waits) before it lets go.
CODEXD_STOP_WAIT_S = 15
# One presence probe's reply budget; a slower one is retried, not refused.
PRESENCE_PROBE_S = 5


@contextmanager
def _short_lived_app_server(
    *,
    cwd: str,
    title: str,
    timeout: float,
    rejection_prefix: str,
) -> Iterator[AppServerClient]:
    """Open one stable app-server connection for a read-only operation."""
    from ccx.system.engine_binary import resolve_engine_binary
    codex_bin = resolve_engine_binary("codex")
    client: AppServerClient | None = None

    def reject_request(
        request_id: Any,
        method: str,
        params: dict[str, Any],
    ) -> None:
        if client is not None:
            client.reject_request(request_id, f"{rejection_prefix}: {method}")

    try:
        client = AppServerClient(
            [codex_bin, "app-server", "--stdio"],
            cwd=cwd,
            on_notification=lambda method, params: None,
            on_server_request=reject_request,
            stderr=subprocess.DEVNULL,
        )
        client.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "ccx",
                    "title": title,
                    "version": ccx_version(),
                },
                "capabilities": {"experimentalApi": False},
            },
            timeout=timeout,
        )
        client.notify("initialized")
        yield client
    finally:
        if client is not None:
            client.close()


def list_models(*, cwd: str, timeout: float = 30) -> list[str]:
    """Return every model ID advertised by Codex's stable ``model/list`` API."""
    try:
        with _short_lived_app_server(
            cwd=cwd,
            title="CCX Codex model validation",
            timeout=timeout,
            rejection_prefix="unsupported request during model list read",
        ) as client:
            models: list[str] = []
            cursor: str | None = None
            seen_cursors: set[str] = set()
            while True:
                params = {"cursor": cursor} if cursor is not None else {}
                page = client.request("model/list", params, timeout=timeout)
                for item in page.get("data") or []:
                    if not isinstance(item, dict):
                        continue
                    model_id = item.get("model") or item.get("id")
                    if isinstance(model_id, str) and model_id and model_id not in models:
                        models.append(model_id)
                next_cursor = page.get("nextCursor")
                if not isinstance(next_cursor, str) or not next_cursor:
                    break
                if next_cursor in seen_cursors:
                    raise RuntimeError("model/list returned a repeated cursor")
                seen_cursors.add(next_cursor)
                cursor = next_cursor
            if not models:
                raise RuntimeError("model/list returned no model IDs")
            return models
    except (OSError, RuntimeError) as error:
        raise SessionUnavailable(
            f"Codex model list unavailable: {error}"
        ) from error


def validate_model(model: str, *, cwd: str, timeout: float = 30) -> list[str]:
    """Validate one explicit model and return the complete advertised ID list."""
    if not isinstance(model, str) or not model:
        raise InvalidRequest("Codex model must be a non-empty string")
    valid_models = list_models(cwd=cwd, timeout=timeout)
    if model in valid_models:
        return valid_models
    suffix_matches = [
        value for value in valid_models if value.endswith(f"-{model}")
    ]
    suggestion = (
        f" Did you mean {suffix_matches[0]!r}?"
        if len(suffix_matches) == 1
        else ""
    )
    raise InvalidRequest(
        f"unsupported Codex model {model!r}.{suggestion} "
        f"Valid models: {', '.join(valid_models)}"
    )


def read_account_usage(
    *,
    tokens: bool = False,
    timeout: float = 30,
) -> dict[str, Any]:
    """Read Codex account quota or token activity without creating a worker.

    A running codexd answers from its already-open app-server; only when no
    daemon owns the runtime slot is a short-lived stable connection used. The
    read never starts the daemon.
    """
    from ccx.system.engine_binary import resolve_engine_binary
    resolve_engine_binary("codex")
    deadline = time.monotonic() + max(0.0, timeout)
    if observation_daemon(deadline):
        reply = daemon_call(
            {"op": "account-read", "tokens": tokens},
            timeout=remaining(deadline, timeout),
        )
        if reply.get("ok"):
            data = reply.get("data")
            if isinstance(data, dict):
                return data
            raise SessionUnavailable(
                "codexd returned no Codex account usage data", short="codexd"
            )
        # The daemon that owns the app-server answered, and its failure is the
        # answer. Starting a second app-server beside it to get a different one
        # would launder a daemon-side failure into a fresh read.
        raise SessionUnavailable(
            _served_failure(reply, "Codex usage unavailable"), short="codexd"
        )
    try:
        with _short_lived_app_server(
            cwd=os.getcwd(),
            title="CCX Codex usage",
            timeout=timeout,
            rejection_prefix="unsupported account request during usage read",
        ) as client:
            method = "account/usage/read" if tokens else "account/rateLimits/read"
            return client.request(method, timeout=timeout)
    except (OSError, RuntimeError) as error:
        raise SessionUnavailable(
            f"Codex usage unavailable: {error}"
        ) from error


def observation_daemon(
    deadline: float | None,
    *,
    short: str | None = None,
) -> bool:
    """Does the shared daemon own the runtime slot for this observation?

    There is one app-server per CCX root, and codexd owns it. So if codexd is
    answering at all, an observation goes through it: starting a second
    app-server beside a daemon that is merely still booting would double the
    runtime and route the read around the compatibility boundary.

    Returns ``True`` when the daemon should serve the read and ``False`` only
    when absence is positively established and a short-lived app-server is safe. A daemon that is
    booting is waited for strictly inside the caller's own deadline — never
    past it — and one of another generation is refused rather than bypassed.
    """
    while True:
        probe = settled_daemon_status(deadline)
        transport = probe.get("transport")
        if transport == RUNTIME_ABSENT:
            return False
        if transport != RUNTIME_SERVED:
            raise SessionUnavailable(
                inconclusive_presence(probe, "no second app-server was started"),
                short=short or "codexd",
            )
        require_compatible_daemon(probe, short=short)
        if probe.get("state") == "ready":
            return True
        if remaining(deadline, 1) <= 0:
            raise SessionUnavailable(
                "codexd is still booting and the read budget expired; it owns "
                "the shared app-server, so no second one was started",
                short=short or "codexd",
            )
        time.sleep(min(0.05, remaining(deadline, 0.05)))


def remaining(deadline: float | None, maximum: float) -> float:
    if deadline is None:
        return maximum
    return max(0.0, min(maximum, deadline - time.monotonic()))


def _lifetime_lock_held() -> bool | None:
    """Read-only positive singleton lease check, no implicit lock creation."""
    from ccx import constants
    from ccx.store.root import require_ccx_active
    import stat
    try:
        store = require_ccx_active(constants.CCX_ROOT)
        with store._gen("runtime-metadata", "pid") as directory:
            try:
                fd = os.open(
                    "codexd.lease.lock",
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=directory,
                )
            except FileNotFoundError:
                return False
            try:
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode)
                        or info.st_uid != os.getuid()
                        or info.st_mode & 0o077):
                    return None
                try:
                    fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    return True
                fcntl.flock(fd, fcntl.LOCK_UN)
                return False
            finally:
                os.close(fd)
    except FileNotFoundError:
        return False
    except (OSError, ValueError):
        return None


def _transport_failure(error: BaseException) -> dict[str, Any]:
    """Classify a failed socket observation without confusing timeout with absence."""
    if isinstance(error, (TimeoutError, socket.timeout, EOFError)):
        # A busy machine or a daemon mid-exit: unknown, but worth asking again
        # inside the caller's budget. Never evidence of absence.
        return {
            "ok": False,
            "error": f"codexd unavailable: {error.__class__.__name__}",
            "transport": RUNTIME_INCONCLUSIVE,
            "retry": True,
        }
    elif isinstance(error, OSError) and getattr(error, "errno", None) in {
        2,  # ENOENT: no socket file
        32,  # EPIPE: the listener closed under the request
        54,  # ECONNRESET (BSD/macOS)
        61,  # ECONNREFUSED (BSD/macOS)
        104,  # ECONNRESET: a backlog connection dropped as the listener closed
        111,  # ECONNREFUSED
    }:
        held = _lifetime_lock_held()
        presence = RUNTIME_ABSENT if held is False else RUNTIME_INCONCLUSIVE
        if held is True:
            # Nothing listens, yet a codexd still owns the runtime: one that is
            # binding its socket or shutting down. Both resolve in seconds.
            return {
                "ok": False,
                "error": f"codexd unavailable: {error.__class__.__name__}",
                "transport": presence,
                "transitional": True,
            }
    else:
        presence = RUNTIME_INCONCLUSIVE
    return {
        "ok": False,
        "error": f"codexd unavailable: {error.__class__.__name__}",
        "transport": presence,
    }


HISTORY_REPLY_OPS = frozenset({"thread-read", "thread-history", "thread-items-list", "thread-turns-list"})


def daemon_rpc(message: dict[str, Any], timeout: float = 10, *,
               keep_open: bool = False) -> dict[str, Any]:
    """Real daemon RPC: verify singleton lease and SO_PEERCRED on same socket.

    Codex provider native IDs never identify the managed CCX worker, and a
    socket pathname or prior daemon status is not evidence of this daemon's
    PID, generation or still-held runtime role lock.
    """
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.rpc.transport import (
        handshake_payload, plan_transport, verify_handshake,
    )
    if timeout <= 0:
        return {
            "ok": False, "error": "codexd unavailable: TimeoutError",
            "transport": RUNTIME_INCONCLUSIVE,
        }
    try:
        store = require_ccx_active(constants.CCX_ROOT)
        plan = plan_transport(store, role="codexd")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(timeout)
            client.connect(str(plan.final))
            client.sendall((json.dumps({
                "op": "handshake",
                "identity": {"protocolVersion": 2, **plan.evidence()},
            }) + "\n").encode())
            # The native daemon must prove its own ACTIVE lease nonce in its
            # reply. The request supplies only the expected root/role fields:
            # a client cannot know a daemon nonce until the daemon presents
            # it, and knowledge of the nonce alone never authorizes a peer.
            data = bytearray()
            # thread/read and thread/items/list responses are caller-requested
            # native history, not bounded control envelopes. A large rollout or
            # one page of big tool outputs can exceed the normal 1 MiB RPC reply
            # guard; rejecting it here masqueraded as "codexd unavailable:
            # ValueError".
            def read_one(*, bounded: bool = True) -> dict:
                while b"\n" not in data:
                    chunk = client.recv(65536)
                    if not chunk:
                        raise EOFError("Codex daemon closed socket mid-RPC")
                    data.extend(chunk)
                    if bounded and len(data) > 1024 * 1024:
                        raise ValueError("Codex daemon reply exceeds budget")
                line, rest = data.split(b"\n", 1)
                data[:] = rest
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError("Codex daemon replied with non-object")
                return value
            hello = read_one()
            if not hello.get("ok"):
                raise ValueError(
                    "Codex daemon handshake refused: "
                    + str(hello.get("error"))
                )
            verify_handshake(
                store, plan, received=hello.get("identity"),
                connected_socket=client,
            )
            store.revalidate()
            client.sendall((json.dumps(message) + "\n").encode())
            result = read_one(bounded=message.get("op") not in HISTORY_REPLY_OPS)
            if keep_open and result.get("ok") is True:
                return {**result, "transport": RUNTIME_SERVED,
                        "_held_fd": client.detach()}
            return {**result, "transport": RUNTIME_SERVED}
    except (OSError, ValueError, EOFError) as error:
        return _transport_failure(error)


@contextmanager
def attached_worker(target: str, *, timeout: float = 90) -> Iterator[dict[str, Any]]:
    """Hold one authenticated codexd connection while the native TUI runs."""
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.store.identity import IdentityError

    store = require_ccx_active(constants.CCX_ROOT)
    try:
        owner = store.resolve(target)
        if owner.engine != "codex":
            raise InvalidRequest("attach requires a Codex worker")
        store.authorize_native_action(owner.worker, "resume")
        record = store.record(owner.worker)
        require_workspace(record)
    except IdentityError as error:
        raise InvalidRequest(f"Codex attach worker unavailable: {error}") from error

    deadline = time.monotonic() + timeout
    ensure_daemon(deadline=deadline)
    reply = daemon_rpc({
        "op": "attach", "short": owner.worker_id,
        "rpcGeneration": CODEXD_RPC_GENERATION,
    }, timeout=max(.1, deadline - time.monotonic()), keep_open=True)
    fd = reply.pop("_held_fd", None)
    if fd is None:
        if reply.get("transport") == RUNTIME_SERVED:
            require_served_generation(reply, short=owner.short)
        raise SessionUnavailable(
            f"Codex attach unavailable: {reply.get('error') or reply.get('state') or reply.get('transport')}",
            short=owner.short)
    with socket.socket(fileno=fd):
        require_served_generation(reply, short=owner.short)
        store.revalidate()
        args = record.get("createArgs") or {}
        yield {**reply, "workerId": owner.worker_id,
               "model": args.get("model"), "effort": args.get("effort"),
               "hasTurn": bool(record.get("lastTurn"))}


# Inspection and teardown stay available on an incompatible daemon: they are
# how an operator sees it and gets rid of it. Everything else is refused.
RECOVERY_OPS = CODEXD_RECOVERY_OPS


def daemon_call(
    message: dict[str, Any],
    timeout: float = 10,
    *,
    short: str | None = None,
) -> dict[str, Any]:
    """Send a non-recovery request that declares this client's RPC generation.

    The declaration is what lets a current daemon refuse an old client before
    running the handler, so it belongs on every request that can change
    anything — never on the recovery operations that must keep answering an
    incompatible peer.

    Whatever comes back is held to the same standard in reverse. Once a request
    has actually been written, a served envelope must declare this generation
    whether it reports success or failure: a daemon replaced between the
    preflight and the write could otherwise return an old ``ok: false`` and
    have its application error read as a current one.
    """
    reply = daemon_rpc(
        {**message, "rpcGeneration": CODEXD_RPC_GENERATION}, timeout
    )
    if reply.get("transport") != RUNTIME_SERVED:
        return reply
    return require_served_generation(reply, short=short)


def daemon_generation(reply: dict[str, Any]) -> int | None:
    """The RPC generation an answering daemon declared, if it declared one."""
    generation = reply.get("rpcGeneration")
    return generation if isinstance(generation, int) else None


def require_served_generation(
    reply: dict[str, Any],
    *,
    short: str | None = None,
) -> dict[str, Any]:
    """Refuse an envelope a codexd of another RPC generation served us.

    A daemon that predates the generation field declares nothing and is treated
    as incompatible: its replies are missing members current callers rely on,
    and using them anyway is how a successful turn gets reported as a failure —
    or an old rejection as a current one. Success is irrelevant here; only who
    answered is.
    """
    generation = daemon_generation(reply)
    if generation != CODEXD_RPC_GENERATION:
        raise SessionUnavailable(
            "the running codexd speaks RPC generation "
            f"{generation if generation is not None else 'unknown (legacy)'}, "
            f"but this CCX speaks {CODEXD_RPC_GENERATION}. Finish or kill its "
            "workers and run `ccx shutdown --engine codex` to replace it.",
            short=short or "codexd",
        )
    return reply


def require_compatible_daemon(
    reply: dict[str, Any],
    *,
    short: str | None = None,
) -> dict[str, Any]:
    """Hold a raw availability probe to this RPC generation.

    Only a reply that reports success proves a daemon is there to be
    incompatible with: a not-ok probe mutated nothing and may simply mean no
    daemon, so those paths stay free to fall through to :func:`ensure_daemon`,
    which knows how to replace an idle one. Requests that were actually written
    are stricter — see :func:`require_served_generation`.
    """
    if reply.get("transport") != RUNTIME_SERVED:
        return reply
    return require_served_generation(reply, short=short)


def preflight_daemon(
    deadline: float | None,
    *,
    short: str | None = None,
) -> dict[str, Any] | None:
    """Establish daemon compatibility before a mutating operation is written.

    Returns ``None`` once the answering daemon has positively declared this
    RPC generation and the positive-absence reply when no daemon owns the
    runtime. An inconclusive probe raises: it must never authorize mutation or
    daemon replacement.
    """
    probe = settled_daemon_status(deadline)
    if probe.get("transport") == RUNTIME_ABSENT:
        return probe
    if probe.get("transport") != RUNTIME_SERVED:
        raise SessionUnavailable(
            inconclusive_presence(probe, "nothing was sent"),
            short=short or "codexd",
        )
    require_compatible_daemon(probe, short=short)
    return None


def rpc(
    short: str,
    message: dict[str, Any],
    timeout: float = 10,
) -> dict[str, Any]:
    """Send a worker-addressed operation to the shared codexd daemon.

    Every operation that is not a recovery operation is fenced on the daemon's
    RPC generation before it is written, so an incompatible daemon never
    processes a steer, an approval, or a turn and leaves the caller to notice
    afterwards. ``timeout`` is the end-to-end budget for the preflight and the
    operation together; the reply-side check stays as a guard against a daemon
    that is replaced between the two.
    """
    if message.get("op") in RECOVERY_OPS:
        return daemon_rpc({**message, "short": short}, timeout)
    deadline = time.monotonic() + max(0.0, timeout)
    unreachable = preflight_daemon(deadline, short=short)
    if unreachable is not None:
        return unreachable
    return daemon_call(
        {**message, "short": short},
        timeout=remaining(deadline, timeout),
        short=short,
    )


def daemon_status(timeout: float = 2, *, presence_only: bool = False) -> dict[str, Any]:
    """Probe the daemon. Deliberately unchecked: status is a recovery tool.

    ``presence_only`` skips the per-worker states, which take every worker's
    lock; a daemon that predates the flag ignores it and lists them anyway.
    """
    message: dict[str, Any] = {"op": "daemon-status"}
    if presence_only:
        message["workers"] = False
    return daemon_rpc(message, timeout=timeout)


def _stopping(probe: dict[str, Any]) -> bool:
    """A codexd is between owning the runtime and having let go of it."""
    if probe.get("transport") == RUNTIME_SERVED:
        return probe.get("state") == "stopping"
    return probe.get("transport") == RUNTIME_INCONCLUSIVE and bool(
        probe.get("transitional")
    )


def settled_daemon_status(deadline: float | None) -> dict[str, Any]:
    """Probe the daemon, waiting out one that is slow, binding or shutting down.

    A stopping codexd refuses new loads and is about to release the runtime,
    so neither "use it" nor "start another" is right yet; a probe that timed
    out says nothing either way. Both are asked again until the caller's
    deadline (or ``CODEXD_STOP_WAIT_S``); what is still unknown then is
    returned as the inconclusive probe it is, with how long it was waited for.
    """
    started = time.monotonic()
    limit = deadline if deadline is not None else started + CODEXD_STOP_WAIT_S
    attempts = 0
    while True:
        attempts += 1
        probe = daemon_status(timeout=remaining(limit, PRESENCE_PROBE_S),
                              presence_only=True)
        now = time.monotonic()
        if not (_stopping(probe) or probe.get("retry")) or now >= limit:
            if probe.get("transport") == RUNTIME_INCONCLUSIVE:
                probe = {**probe, "attempts": attempts,
                         "waitedSeconds": round(now - started, 1)}
            return probe
        time.sleep(min(0.1, max(0.0, limit - now)))


def inconclusive_presence(probe: dict[str, Any], outcome: str) -> str:
    """The refusal for a presence probe that never settled, with its evidence."""
    error = str(probe.get("error") or "no reply").removeprefix("codexd unavailable: ")
    attempts = probe.get("attempts") or 1
    tried = (f" after {attempts} probes over {probe.get('waitedSeconds')}s"
             if attempts > 1 else "")
    return f"codexd presence is inconclusive ({error}{tried}); {outcome}"


def observe_worker(
    short: str,
    *,
    timeout: float = 1.0,
) -> dict[str, Any]:
    """Canonical live observation for one Codex worker."""
    reply = daemon_rpc({"op": "status", "short": short}, timeout=max(0.0, timeout))
    transport = reply.get("transport")
    if transport == RUNTIME_ABSENT:
        return {
            "presence": RUNTIME_ABSENT,
            "daemon_served": False,
            "reply": reply,
        }
    if transport != RUNTIME_SERVED:
        return {
            "presence": RUNTIME_INCONCLUSIVE,
            "daemon_served": False,
            "reply": reply,
        }
    if daemon_generation(reply) != CODEXD_RPC_GENERATION:
        return {
            "presence": RUNTIME_INCONCLUSIVE,
            "daemon_served": True,
            "reply": reply,
        }
    if reply.get("ok"):
        return {
            "presence": RUNTIME_SERVED,
            "daemon_served": True,
            "reply": reply,
        }
    if reply.get("loaded") is False:
        return {
            "presence": RUNTIME_ABSENT,
            "daemon_served": True,
            "reply": reply,
        }
    return {
        "presence": RUNTIME_INCONCLUSIVE,
        "daemon_served": True,
        "reply": reply,
    }


def observe_registry(*, timeout: float = 1.0) -> dict[str, Any]:
    """Observe the shared daemon registry once for list-style projection."""
    reply = daemon_status(timeout=timeout)
    transport = reply.get("transport")
    if transport == RUNTIME_ABSENT:
        return {"presence": RUNTIME_ABSENT, "workers": {}, "reply": reply}
    if (
        transport == RUNTIME_SERVED
        and daemon_generation(reply) == CODEXD_RPC_GENERATION
        and reply.get("ok")
    ):
        workers = reply.get("workers")
        return {
            "presence": RUNTIME_SERVED,
            "workers": workers if isinstance(workers, dict) else {},
            "reply": reply,
        }
    return {"presence": RUNTIME_INCONCLUSIVE, "workers": {}, "reply": reply}


def explain_native_failure(message: str) -> str:
    """Put a plain-language cause in front of known native Codex failures."""
    if "missing source rollout" in message:
        return ("the Codex thread this worker was forked or imported from was "
                f"deleted, so its history can't be read ({message})")
    return message


def _served_failure(reply: dict[str, Any], fallback: str) -> str:
    """The most specific message a failed daemon reply carries.

    A current daemon attaches the native ``ProtocolError`` as ``protocolError``;
    an envelope without one only carries the flattened string.
    """
    structured = reply.get("protocolError")
    if isinstance(structured, dict) and isinstance(structured.get("message"), str):
        return explain_native_failure(structured["message"])
    return explain_native_failure(str(reply.get("error") or fallback))


def _history_failure(error: BaseException, thread_id: str) -> CcxError:
    message = explain_native_failure(getattr(error, "message", str(error)))
    if _is_missing_history(message):
        return SessionNotFound(message, short=thread_id)
    return SessionUnavailable(message, short=thread_id)


def _is_missing_history(error: Any) -> bool:
    """Recognize native missing-thread errors by message, never by code.

    Codex 0.150.1 answers both a missing thread ("thread not loaded: <id>")
    and a malformed one with the same broad ``-32600``, so the code proves
    nothing. These phrases are specific enough that a model, config, or
    resource failure cannot match them; anything else stays unavailable.
    """
    message = getattr(error, "message", str(error)).lower()
    return (
        "thread not loaded:" in message
        or "unknown thread" in message
        or "no rollout found" in message
    )


def observation_cwd(cwd: Any) -> str:
    """The directory a read-only app-server may be launched in.

    ``thread/read`` is addressed by native thread id and never uses the launch
    directory, so a deleted worktree must not also destroy access to persisted
    history. Only observation falls back, and only to CCX's own state root; the
    fallback is never written back to the worker record, the native thread's
    own cwd, or conversation metadata. Starting project work is the opposite
    policy and lives in :func:`require_workspace`.
    """
    if isinstance(cwd, str) and cwd and os.path.isdir(cwd):
        return cwd
    neutral = codex_state_root()
    if not neutral.is_dir():
        raise SessionUnavailable(
            "cannot read Codex history: the worker workspace is gone and the "
            f"CCX state directory {neutral} does not exist"
        )
    return str(neutral)


def require_workspace(value: dict[str, Any]) -> str:
    """The worker's recorded project directory, which must still exist.

    Native ``thread/resume`` will happily reattach a persisted thread whose
    ``cwd`` is gone, but CCX will not start or restart project work in a
    directory that no longer exists: the turn would run somewhere the record
    does not describe, and its shell commands would land in whatever the
    app-server falls back to. This is a deliberate CCX policy, stricter than
    the native one, and it gates every operation that starts native work on
    the thread — ``resume``, ``send``, and ``compact``. Creation is
    independently strict about its own ``--cwd``.

    Read-only observation keeps working without the workspace (see
    :func:`observation_cwd`): report, conversation, export, ``thread/read``,
    and status. So does controlling a turn that is already running — steer,
    approve, reject, interrupt, stop, kill — because a workspace that
    disappears mid-turn must not trap the turn.
    """
    cwd = value.get("cwd")
    if isinstance(cwd, str) and cwd and os.path.isdir(cwd):
        return cwd
    raise SessionUnavailable(
        f"the Codex worker workspace is gone: {cwd!r}. History stays readable "
        f"(`ccx report`, `conversation`, `export`), but resuming or "
        f"sending needs the recorded directory to exist.",
        short=value.get("short"),
    )


def read_thread(
    thread_id: str,
    *,
    cwd: str,
    include_turns: bool = True,
    timeout: float = 30,
    worker_id: str | None = None,
    expected_source: str | None = None,
    limit: int | None = None,
    newest_first: bool = False,
    exact_turn_id: str | None = None,
) -> dict[str, Any]:
    """Read source-verified Codex metadata, optionally assembling paged turns."""
    if not isinstance(thread_id, str) or not thread_id:
        raise SessionNotFound("Codex conversation is unavailable; check worker status")
    if timeout <= 0:
        raise InvalidRequest("history read timeout must be greater than zero")
    if worker_id is not None and (not isinstance(expected_source, str) or not expected_source):
        raise InvalidRequest("Codex conversation source is not verified; inspect the worker")
    deadline = time.monotonic() + timeout
    message = {"threadId": thread_id, "includeTurns": False}
    from ccx.codex.history import fill_thread
    # Never start a second app-server beside a daemon that owns the runtime
    # slot — including one still booting — nor to skip a compatibility problem
    # the operator needs to see.
    if observation_daemon(deadline, short=thread_id):
        budget = remaining(deadline, timeout)
        reply = daemon_call({
            "op": "thread-history" if include_turns else "thread-read",
            **message,
            **({"workerId": worker_id} if worker_id is not None else {}),
            **({"limit": limit} if limit is not None else {}),
            **({"newestFirst": True} if newest_first else {}),
            **({"turnId": exact_turn_id} if exact_turn_id else {}),
            "observeSeconds": budget,
        }, timeout=budget, short=thread_id)
        if not reply.get("ok"):
            native = _served_failure(reply, "Codex history read failed")
            if _is_missing_history(native):
                raise SessionNotFound(native, short=thread_id)
            raise SessionUnavailable(native, short=thread_id)
        thread = reply.get("thread")
        if not isinstance(thread, dict):
            raise SessionNotFound("no Codex thread available", short=thread_id)
        if worker_id is not None and (
            reply.get("workerId") != worker_id
            or reply.get("threadId") != thread_id
            or thread.get("id") != thread_id
            or thread.get("threadSource") != expected_source
        ):
            raise SessionUnavailable(
                "Codex history belongs to another worker; inspect the handle", short=thread_id,
            )
        return thread

    try:
        with _short_lived_app_server(
            cwd=observation_cwd(cwd),
            title="CCX Codex history read",
            timeout=remaining(deadline, timeout),
            rejection_prefix="unsupported request during history read",
        ) as client:
            result = client.request(
                "thread/read", message, timeout=remaining(deadline, timeout)
            )
            thread = result.get("thread")
            if not isinstance(thread, dict):
                raise SessionNotFound("no Codex thread available", short=thread_id)
            if worker_id is not None and (
                thread.get("id") != thread_id
                or thread.get("threadSource") != expected_source
            ):
                raise SessionUnavailable(
                    "Codex history belongs to another worker; inspect the handle", short=thread_id,
                )
            if not include_turns:
                return thread
            return fill_thread(thread, client.request, deadline=deadline,
                               limit=limit, newest_first=newest_first,
                               exact_turn_id=exact_turn_id)
    except (ProtocolError, OSError) as error:
        raise _history_failure(error, thread_id) from error


def read_import_source(thread_id: str, *, timeout: float = 20.) -> dict:
    """Read one external native thread's last completed-point candidate.

    No stored owner is required, and this operation never resumes the source.
    A running CCX app-server must serve it; an absent one permits one short
    lived observer without access to the machine control socket.
    """
    import os
    from ccx.codex.history import read_fork_source_point
    if not isinstance(thread_id, str):
        raise InvalidRequest("Codex import requires a full native thread UUID")
    deadline = time.monotonic() + timeout
    if observation_daemon(deadline, short=thread_id):
        reply = daemon_call({
            "op": "import-source-point", "threadId": thread_id,
            "observeSeconds": remaining(deadline, timeout),
        }, timeout=remaining(deadline, timeout), short=thread_id)
        if not reply.get("ok"):
            error = _served_failure(reply, "Codex import source unavailable")
            if _is_missing_history(error):
                raise SessionNotFound(error, short=thread_id)
            raise SessionUnavailable(error, short=thread_id)
        return {"thread": reply["thread"], "lastTurn": reply["lastTurn"]}
    try:
        with _short_lived_app_server(
            cwd=os.getcwd(), title="CCX Codex native import source read",
            timeout=remaining(deadline, timeout),
            rejection_prefix="unsupported request during Codex native import",
        ) as client:
            return read_fork_source_point(
                client.request, thread_id, deadline=deadline,
            )
    except ProtocolError as error:
        raise _history_failure(error, thread_id) from error


def list_thread_turns(
    thread_id: str,
    *,
    cwd: str,
    cursor: str | None = None,
    limit: int = 100,
    sort_direction: str = "asc",
    items_view: str = "full",
    timeout: float = 30,
    worker_id: str | None = None,
    expected_source: str | None = None,
) -> dict[str, Any]:
    """One owner-fenced native turn page, including original statuses."""
    from ccx.store.identity import InvalidTarget, canonical_worker_id

    try:
        canonical_worker_id(worker_id)
    except InvalidTarget as exc:
        raise InvalidRequest("Codex history needs a complete worker handle") from exc
    if not isinstance(expected_source, str) or not expected_source:
        raise InvalidRequest("Codex history source is not verified; restart paging")
    if not isinstance(thread_id, str) or not thread_id:
        raise SessionNotFound("Codex conversation is unavailable; check worker status")
    if type(limit) is not int or not 1 <= limit <= 100:
        raise InvalidRequest("thread turn limit must be between 1 and 100")
    if (not isinstance(sort_direction, str) or sort_direction not in {"asc", "desc"}
            or not isinstance(items_view, str) or items_view not in {"notLoaded", "summary", "full"}):
        raise InvalidRequest("invalid Codex turn page direction or items view")
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise InvalidRequest("native turn cursor must be non-empty")
    params: dict[str, Any] = {
        "threadId": thread_id, "limit": limit,
        "sortDirection": sort_direction, "itemsView": items_view,
    }
    if cursor is not None:
        params["cursor"] = cursor
    return _paged_history_request(
        method="thread/turns/list", daemon_op="thread-turns-list", params=params,
        cwd=cwd, timeout=timeout, short=thread_id, worker_id=worker_id,
        expected_source=expected_source,
    )


def list_thread_items(
    thread_id: str,
    *,
    cwd: str,
    cursor: str | None = None,
    limit: int = 25,
    sort_direction: str = "desc",
    timeout: float = 30,
    turn_id: str | None = None,
    worker_id: str | None = None,
    expected_source: str | None = None,
) -> dict[str, Any]:
    """Read original full-worker Codex items without loading or resuming.

    A native thread ID alone cannot authorize a page. The running daemon
    independently proves the CCX owner; a cold read proves threadSource first
    on the same observer connection, without materializing native turns.
    """
    from ccx.store.identity import InvalidTarget, canonical_worker_id

    try:
        canonical_worker_id(worker_id)
    except InvalidTarget as exc:
        raise InvalidRequest("Codex history needs a complete worker handle") from exc
    if not isinstance(expected_source, str) or not expected_source:
        raise InvalidRequest("Codex history source is not verified; restart paging")
    if not isinstance(thread_id, str) or not thread_id:
        raise SessionNotFound("Codex conversation is unavailable; check worker status")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
        raise InvalidRequest("history read timeout must be greater than zero")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise InvalidRequest("thread item limit must be between 1 and 100")
    if not isinstance(sort_direction, str) or sort_direction not in {"asc", "desc"}:
        raise InvalidRequest("thread item sort direction must be asc or desc")
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise InvalidRequest("native item cursor must be a non-empty string")

    params: dict[str, Any] = {
        "threadId": thread_id,
        "limit": limit,
        "sortDirection": sort_direction,
    }
    if cursor is not None:
        params["cursor"] = cursor
    if turn_id is not None:
        if not isinstance(turn_id, str) or not turn_id:
            raise InvalidRequest("native item turn id must be non-empty")
        params["turnId"] = turn_id
    return _paged_history_request(
        method="thread/items/list", daemon_op="thread-items-list", params=params,
        cwd=cwd, timeout=timeout, short=thread_id, worker_id=worker_id,
        expected_source=expected_source,
    )


def _paged_history_request(
    *,
    method: str,
    daemon_op: str,
    params: dict[str, Any],
    cwd: str,
    timeout: float,
    short: str | None,
    worker_id: str,
    expected_source: str,
) -> dict[str, Any]:
    """Read one original source-attested page within the caller's deadline."""
    from ccx.errors import OperationTimeout

    if (method, daemon_op) not in {
        ("thread/items/list", "thread-items-list"),
        ("thread/turns/list", "thread-turns-list"),
    }:
        raise InvalidRequest("Only CCX-managed Codex workers can be read; use a worker handle")
    deadline = time.monotonic() + timeout
    thread_id = params["threadId"]
    source = expected_source
    if observation_daemon(deadline, short=short):
        remaining_time = remaining(deadline, timeout)
        if remaining_time <= 0:
            raise OperationTimeout("Codex item-page deadline expired", short=short)
        reply = daemon_call(
            {"op": daemon_op, "workerId": worker_id, "params": params},
            timeout=remaining_time, short=short,
        )
        if not reply.get("ok"):
            native = _served_failure(reply, f"{method} failed")
            if reply.get("reason") == "worker-not-found" or _is_missing_history(native):
                raise SessionNotFound(native, short=short)
            raise SessionUnavailable(native, short=short)
        if (reply.get("workerId") != worker_id
                or reply.get("threadId") != thread_id
                or reply.get("threadSource") != source):
            raise SessionUnavailable(
                "Codex history page belongs to another worker; restart paging", short=short,
            )
        page = reply.get("data")
    else:
        try:
            remaining_time = remaining(deadline, timeout)
            if remaining_time <= 0:
                raise OperationTimeout("Codex item-page deadline expired", short=short)
            with _short_lived_app_server(
                cwd=observation_cwd(cwd),
                title="CCX Codex paginated history read",
                timeout=remaining_time,
                rejection_prefix="unsupported request during paginated history read",
            ) as client:
                remaining_time = remaining(deadline, timeout)
                if remaining_time <= 0:
                    raise OperationTimeout("Codex item-page deadline expired", short=short)
                observed = client.request(
                    "thread/read", {"threadId": thread_id, "includeTurns": False},
                    timeout=remaining_time,
                )
                thread = observed.get("thread") if isinstance(observed, dict) else None
                if not isinstance(thread, dict):
                    raise SessionNotFound("no Codex thread available", short=short)
                if thread.get("id") != thread_id or thread.get("threadSource") != source:
                    raise SessionUnavailable(
                        "Codex history page belongs to another worker; restart paging", short=short,
                    )
                remaining_time = remaining(deadline, timeout)
                if remaining_time <= 0:
                    raise OperationTimeout("Codex item-page deadline expired", short=short)
                page = client.request(method, params, timeout=remaining_time)
        except (ProtocolError, OSError) as error:
            raise _history_failure(error, thread_id) from error
    if (not isinstance(page, dict)
            or not isinstance(page.get("data"), list)
            or (page.get("nextCursor") is not None
                and not isinstance(page.get("nextCursor"), str))):
        raise SessionUnavailable(f"{method} returned malformed native page", short=short)
    return page


def daemon_stop(*, force: bool = False, timeout: float = 10) -> dict[str, Any]:
    return daemon_rpc({"op": "daemon-stop", "force": force}, timeout=timeout)


CALLER_PYTHONPATH_ENV = "CCX_CALLER_PYTHONPATH"


def package_env() -> dict[str, str]:
    """Make the currently-running CCX package importable to a child process."""
    env = os.environ.copy()
    package_parent = str(Path(__file__).resolve().parents[2])
    inherited = env.get("PYTHONPATH")
    # Remember the caller's own value so grandchildren that run user commands
    # (codex app-server) get it back instead of CCX's install path.
    env.setdefault(CALLER_PYTHONPATH_ENV, inherited or "")
    env["PYTHONPATH"] = (
        package_parent
        if not inherited
        else package_parent + os.pathsep + inherited
    )
    return env


def daemon_env() -> dict[str, str]:
    """Environment for the shared daemon: importable CCX, no caller identity.

    The singleton cannot inherit the initiating worker's identity, but its
    user environment (auth, proxies, PATH and tool config) must survive.
    Each native thread gets its own attested full-UUID stamp separately.
    """
    from ccx.codex.worker_environment import scrub_shared_codex_identity
    return scrub_shared_codex_identity(package_env())


def _acquire_spawn_lock(handle, deadline: float | None) -> None:
    while True:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if deadline is not None and time.monotonic() >= deadline:
                raise SessionUnavailable("timed out waiting for Codex spawn lock")
            time.sleep(0.02)


@contextmanager
def daemon_spawn_lock(deadline: float | None) -> Iterator[None]:
    """Existing singleton spawn fence on stable owner-private generation inode.

    Create its lock only after ACTIVE, then hold that inode across daemon boot,
    without holding the activation lock during subprocess/provider startup.
    """
    from ccx import constants
    from ccx.store.root import GenerationBusy, require_ccx_active, _flock_at
    store = require_ccx_active(constants.CCX_ROOT)
    with store.lease():
        with store._gen("runtime-metadata", "pid", create=True):
            pass
    # Storage lock acquisitions are individually bounded to three seconds, but
    # a cold app-server boot may take longer. Wait for that same daemon through
    # the caller's deadline, then probe again while holding the existing fence.
    until = deadline if deadline is not None else time.monotonic() + CODEXD_BOOT_TIMEOUT_S
    while True:
        left = until - time.monotonic()
        if left <= 0:
            raise SessionUnavailable("timed out waiting for Codex spawn lock")
        acquired = False
        try:
            with store._gen("runtime-metadata", "pid") as fd:
                with _flock_at(fd, "codexd.spawn.lock", exclusive=True,
                               create=True, timeout=min(3.0, left)):
                    acquired = True
                    store.revalidate()
                    yield
                    return
        except GenerationBusy:
            # A failure in the protected body must propagate; never re-enter
            # it and run a native operation a second time.
            if acquired:
                raise
            if time.monotonic() >= until:
                raise SessionUnavailable("timed out waiting for Codex spawn lock")


def _open_daemon_log():
    """Descriptor-pinned, nofollow, uid-private daemon stdout/stderr sidecar."""
    from ccx import constants
    from ccx.store.root import require_ccx_active
    import stat
    store = require_ccx_active(constants.CCX_ROOT)
    with store.lease():
        with store._gen("runtime-metadata", create=True) as fd:
            for attempt in range(2):
                log_fd = os.open(
                    "codexd.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND |
                    os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=fd,
                )
                info = os.fstat(log_fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                        or info.st_mode & 0o077):
                    os.close(log_fd)
                    raise PermissionError("untrusted Codex daemon log inode")
                if attempt == 0 and info.st_size > 10 * 1024 * 1024:
                    os.close(log_fd)
                    os.replace("codexd.log", "codexd.log.1", src_dir_fd=fd,
                               dst_dir_fd=fd)
                    continue
                break
    return os.fdopen(log_fd, "a", encoding="utf-8", buffering=1)




def _codexd_log_tail() -> str:
    try:
        lines = codexd_log_path().read_text().splitlines()
    except OSError:
        return ""
    return " | ".join(line.strip() for line in lines[-3:] if line.strip())


def ensure_daemon(deadline: float | None = None) -> dict[str, Any]:
    """Start the shared codexd daemon if it is not already serving.

    The daemon gets its own process session and a reaper thread waits for it,
    so it survives the importing process while never becoming a zombie in
    long-lived callers. The spawn lock is held for the whole boot; a daemon
    that is already booting is waited for rather than duplicated.
    """
    ensure_dirs()
    probe = settled_daemon_status(deadline)
    if probe.get("transport") == RUNTIME_SERVED:
        require_compatible_daemon(probe)
        if probe.get("state") == "ready":
            return probe
    elif probe.get("transport") != RUNTIME_ABSENT and not (
        probe.get("transport") == RUNTIME_INCONCLUSIVE and probe.get("transitional")
    ):
        raise SessionUnavailable(
            inconclusive_presence(probe, "refusing to start a second daemon"),
            short="codexd",
        )
    with daemon_spawn_lock(deadline):
        probe = settled_daemon_status(deadline)
        if probe.get("transport") == RUNTIME_SERVED:
            require_compatible_daemon(probe)
            if probe.get("state") == "ready":
                return probe
        elif probe.get("transport") == RUNTIME_INCONCLUSIVE and not probe.get(
            "transitional"
        ):
            raise SessionUnavailable(
                inconclusive_presence(probe, "refusing to start a second daemon"),
                short="codexd",
            )
        process = None
        if probe.get("transport") == RUNTIME_ABSENT:
            if deadline is not None and time.monotonic() >= deadline:
                raise SessionUnavailable(
                    "timed out before codexd could be started", short="codexd"
                )
            from ccx.system.engine_binary import resolve_engine_binary
            resolve_engine_binary("codex")  # No daemon start for absent CLI.
            try:
                from ccx import constants
                from ccx.store.root import require_ccx_active
                from ccx.rpc.transport import plan_transport, ensure_transport_parent
                ctx = require_ccx_active(constants.CCX_ROOT)
                plan = plan_transport(ctx, role="codexd")
                ensure_transport_parent(ctx, plan)
                log = _open_daemon_log()
            except OSError as error:
                raise SpawnFailed(
                    f"cannot prepare codexd: {error}",
                    short="codexd",
                    phase="settings-io",
                    detail=str(error),
                ) from error
            try:
                process = subprocess.Popen(
                    [sys.executable, "-m", 'ccx.codex.daemon'],
                    cwd=str(codexd_sock_path().parent),
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                    close_fds=True,
                    env=daemon_env(),
                )
            except OSError as error:
                raise SpawnFailed(
                    f"could not start codexd: {error}",
                    short="codexd",
                    phase="launcher-failed",
                    detail=str(error),
                ) from error
            finally:
                if not log.closed:
                    log.close()
            threading.Thread(
                target=process.wait,
                name="ccx-codexd-reaper",
                daemon=True,
            ).start()

        # The caller's deadline governs the boot wait.
        boot_deadline = (
            deadline if deadline is not None
            else time.monotonic() + CODEXD_BOOT_TIMEOUT_S
        )
        while time.monotonic() < boot_deadline:
            probe = daemon_status(timeout=1)
            if probe.get("transport") == RUNTIME_SERVED:
                require_compatible_daemon(probe)
                if probe.get("state") == "ready":
                    return probe
            if process is not None and process.poll() is not None:
                # The daemon we started died before serving (e.g. no codex
                # binary); waiting out the boot budget would only hide that.
                detail = _codexd_log_tail()
                raise SpawnFailed(
                    "codexd exited during startup"
                    + (f": {detail}" if detail else "")
                    + f" (set CCX_CODEX_BIN or put codex on PATH; log: {codexd_log_path()})",
                    short="codexd",
                    phase="launcher-failed",
                    detail=detail or None,
                )
            time.sleep(0.05)
        detail = _codexd_log_tail()
        raise SessionUnavailable(
            "codexd did not become ready" + (f": {detail}" if detail else "")
            + f" (log: {codexd_log_path()})",
            short="codexd",
        )


def record(target: str) -> dict[str, Any]:
    """Read only an exact owned Codex full UUID, bare or Codex-qualified."""
    if isinstance(target, str):
        engine, sep, suffix = target.partition(":")
        if sep and engine == "claude" and suffix:
            raise InvalidRequest(
                f"this command is scoped to codex; got {target!r} (a Claude handle)"
            )
    # Only error labels use a bare UUID; owner lookup receives the original target.
    label = (target[6:] if isinstance(target, str) and target.startswith("codex:")
             and target[6:] else target)
    try:
        value = read_record(target)
    except OSError as error:
        raise SessionUnavailable(
            f"cannot read Codex worker records: {error}",
            short=label,
        ) from error
    if not value:
        raise SessionNotFound(f"no such Codex worker: {label}", short=label)
    return value




def identity(value: dict[str, Any]) -> dict[str, Any]:
    lifecycle = value.get("lifecycle") or {}
    return {
        "engine": "codex",
        "short": value["short"],
        "worker_id": value.get("workerId"),
        "thread_id": value.get("threadId"),
        "name": value.get("name"),
        "cwd": value.get("cwd"),
        "state": lifecycle.get("state"),
        "active_turn_id": lifecycle.get("activeTurnId"),
    }


def wait(short: str, timeout: float | None) -> dict[str, Any]:
    deadline = time.monotonic() + timeout if timeout is not None else None
    polled = False
    while not polled or deadline is None or time.monotonic() < deadline:
        polled = True
        observation = observe_worker(short, timeout=remaining(deadline, 2))
        reply = observation["reply"]
        if (
            observation["presence"] == RUNTIME_SERVED
            and reply.get("state") in STABLE_STATES
        ):
            return reply
        current = read_record(short) or {}
        if observation["presence"] == RUNTIME_ABSENT:
            recorded_state = (current.get("lifecycle") or {}).get("state")
            if recorded_state in {"busy", "waiting", "resuming"}:
                return {
                    **identity(current),
                    "lastTurn": current.get("lastTurn"),
                    "ok": False,
                    "loaded": False,
                    "normalized_state": "failed",
                    "error": "Codex worker is not loaded; recorded active state is orphaned",
                }
            return {
                **identity(current),
                "lastTurn": current.get("lastTurn"),
                "ok": True,
                "loaded": False,
                "normalized_state": (
                    "stopped" if recorded_state == "idle" else None
                ),
            }
        if deadline is not None and time.monotonic() >= deadline:
            break
        time.sleep(min(0.1, remaining(deadline, 0.1)))
    current = read_record(short) or {"short": short, "lifecycle": {}}
    return {
        **identity(current),
        "lastTurn": current.get("lastTurn"),
        "ok": False,
        "error": "wait timed out",
    }


__all__ = [
    "STABLE_STATES",
    "daemon_call",
    "daemon_env",
    "daemon_rpc",
    "daemon_status",
    "daemon_stop",
    "ensure_daemon",
    "identity",
    "list_models",
    "daemon_generation",
    "daemon_spawn_lock",
    "observe_registry",
    "observe_worker",
    "package_env",
    "preflight_daemon",
    "read_account_usage",
    "record",
    "require_compatible_daemon",
    "require_served_generation",
    "require_workspace",
    "remaining",
    "observation_cwd",
    "observation_daemon",
    "read_thread",
    "report_from_thread",
    "rpc",
    "validate_model",
    "wait",
]
