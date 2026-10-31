"""Identify the harness currently driving the CCX CLI.

This is deliberately about the *caller*, not the worker engine. A Codex turn can
launch either a Claude or Codex worker, and the continuation policy follows the
parent harness that invoked CCX.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

from ccx.errors import InvalidRequest


def _external_codex_unavailable(thread_id: str) -> str | None:
    """Check the *owning* app-server, not merely a thread saved on disk.

    A plain Codex TUI has its own in-process app-server. A separately running
    daemon can read that TUI's rollout, but cannot deliver into the TUI turn.
    Only server-local loaded-thread membership permits an external return arm.
    """
    from ccx.codex import control as codex_control

    path = codex_control.control_socket_path()
    try:
        is_socket = path.is_socket()
    except OSError as error:  # e.g. a sandbox denying access to ~/.codex
        return f"Codex app-server control socket at {path} is not accessible: {error}"
    if not is_socket:
        return f"no Codex app-server control socket at {path} (plain Codex TUI runs in-process)"
    client = None
    try:
        client = codex_control.CodexControlClient(handshake_timeout=2)
        client.initialize(timeout=2)
        result = client.request("thread/loaded/list", {}, timeout=2)
    except Exception as error:
        return f"Codex app-server control socket at {path} is unreachable: {error}"
    finally:
        if client is not None:
            client.close()
    loaded = result.get("data")
    if not isinstance(loaded, list) or not all(isinstance(item, str) for item in loaded):
        return f"Codex app-server at {path} did not confirm its loaded threads"
    if thread_id not in loaded:
        return (f"Codex parent thread {thread_id} is not loaded in the app-server at {path}; "
                "a separate TUI process cannot receive its notifications there")
    return None


def _owned_codex_thread(store, thread_id: str):
    """Complete read-only native return-address census, never a worker alias."""
    from ccx.errors import SessionUnavailable

    if not isinstance(thread_id, str) or not thread_id:
        raise InvalidRequest("Notification needs a Codex recipient; call it from Codex")
    matched = None
    for row in store.list_workers(engine="codex", limit=None):
        native = store.record(row["worker"]).get("native")
        if not isinstance(native, dict):
            raise SessionUnavailable("Codex worker list is unavailable; check CCX_ROOT")
        if native.get("threadId") == thread_id:
            if matched is not None:
                raise SessionUnavailable("Cannot identify the Codex caller; use a worker handle")
            matched = row["worker"]
    store.revalidate()
    return store.resolve(matched) if matched is not None else None


@dataclass(frozen=True)
class CallerContext:
    """Full-UUID owned caller or explicitly unverified native origin."""
    kind: str  # codex | claude | unknown
    native_id: str | None = None
    worker_id: str | None = None
    managed: bool = False

    def parent_notify_unavailable(self) -> str | None:
        """The single capability rule for wait policy and notification arms."""
        if (self.kind != "codex" or not self.native_id
                or (not self.worker_id if self.managed else self.worker_id is not None)):
            return ("parent notification needs a managed Codex caller or its own "
                    "CODEX_THREAD_ID; Claude and mixed origins are not supported")
        if self.managed:
            return None  # CCX-owned parents keep their original codexd path.
        return _external_codex_unavailable(self.native_id)

    @property
    def supports_parent_notify(self) -> bool:
        return self.parent_notify_unavailable() is None

    def parent_ref(self) -> dict[str, Any]:
        unavailable = self.parent_notify_unavailable()
        if unavailable is not None:
            raise InvalidRequest(f"{unavailable}; use --wait / ccx wait TARGET instead")
        from ccx.constants import CCX_ROOT
        from ccx.errors import SessionUnavailable
        from ccx.store.root import GenerationError, require_ccx_active
        from ccx.store.identity import IdentityError

        if not self.managed:
            # The caller's own CODEX_THREAD_ID is a return address, not a
            # worker target. A missing managed stamp must never cause the
            # interactive app-server to dual-subscribe a CCX-owned rollout.
            try:
                store = require_ccx_active(CCX_ROOT)
                if _owned_codex_thread(store, self.native_id) is not None:
                    raise InvalidRequest(
                        "Notification recipient is already a CCX worker; use that handle"
                    )
            except (GenerationError, IdentityError, OSError) as exc:
                raise SessionUnavailable(
                    f"Cannot list Codex workers; check CCX_ROOT: {exc}"
                ) from exc
            return {"kind": "codex-native-thread", "threadId": self.native_id}

        try:
            store = require_ccx_active(CCX_ROOT)
            owner = store.resolve(f"codex:{self.worker_id}")
            store.authorize_native_action(owner.worker, "send")
            native = store.record(owner.worker).get("native")
            if not isinstance(native, dict):
                raise SessionUnavailable("Codex worker record is missing; inspect its handle")
            if (native.get("threadId") != self.native_id
                    or native.get("threadSource")
                    != store.codex_source(owner.worker)):
                raise SessionUnavailable(
                    "Codex caller does not match the worker; verify the handle"
                )
            store.revalidate()
        except (GenerationError, IdentityError, OSError) as exc:
            raise SessionUnavailable(
                f"Cannot verify the Codex parent worker; inspect status: {exc}"
            ) from exc
        return {"kind": "ccx-codex", "workerId": owner.worker_id,
                "threadId": native["threadId"], "generation": store.generation}


def current_caller() -> CallerContext:
    """Classify owner with full-ID engine stamp; never infer from old short.

    The managed full-ID stamp is independently checked against current
    ACTIVE global owner record and provider-native thread/session identity.
    A native-only stamp is NOT a CCX worker association.
    """
    from ccx.store.records import spawned_by_from_env
    from ccx.constants import CCX_ROOT
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.errors import SessionUnavailable

    origin = spawned_by_from_env()
    if origin is None:
        return CallerContext("unknown")
    if origin.get("kind") == "ccx-worker":
        wid, engine = origin["workerId"], origin["engine"]
        try:
            rec = require_ccx_active(CCX_ROOT).record(f"{engine}:{wid}")
        except (GenerationError, OSError) as exc:
            raise SessionUnavailable(
                f"Caller worker changed; retry the notification: {exc}"
            ) from exc
        native = rec.get("native") if isinstance(rec.get("native"), dict) else {}
        key = "sessionId" if engine == "claude" else "threadId"
        env_key = ("CLAUDE_CODE_SESSION_ID" if engine == "claude"
                   else "CODEX_THREAD_ID")
        supplied = os.environ.get(env_key, "").strip()
        if supplied and supplied != native.get(key):
            raise SessionUnavailable(
                "Codex caller does not match this worker; verify the handle"
            )
        return CallerContext(
            engine, native_id=supplied or native.get(key),
            worker_id=wid, managed=True,
        )
    if origin.get("kind") == "provider-native-unknown":
        engine = origin.get("engine")
        if engine in ("codex", "claude"):
            return CallerContext(engine, native_id=origin.get("nativeId"))
        return CallerContext("unknown")
    raise SessionUnavailable("Notification caller could not be identified; use a CCX Codex worker")


def require_notify_parent() -> dict[str, Any]:
    """Return the machine-continuation parent reference or raise before effects."""
    return current_caller().parent_ref()


def codex_blocking_override_enabled() -> bool:
    """Hidden developer escape hatch; intentionally absent from the public CLI."""
    return os.environ.get("CCX_ALLOW_CODEX_BLOCKING_WAIT", "").strip().lower() in {
        "1", "true", "yes", "on"
    }
