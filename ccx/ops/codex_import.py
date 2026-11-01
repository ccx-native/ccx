"""Fork a user-owned native Codex thread into a separate ordinary CCX worker."""
from __future__ import annotations

import time
from uuid import UUID

from ccx.errors import InvalidRequest, SessionUnavailable
from ccx.ops.creation_steps import validated_create_spec
from ccx.ops.creation import create
from ccx.store.identity import mint_worker_id
from ccx.constants import DEFAULT_IDLE_TIMEOUT_S


def import_thread(
    thread_id: str, *, source: dict | None = None,
    model: str | None = None, effort: str | None = None,
) -> dict:
    """Only the child is native-mutated; the original may remain open in Codex.

    Importing a second time intentionally forks a second independent child.
    """
    if not isinstance(thread_id, str):
        raise InvalidRequest("Codex import needs an exact native thread UUID")
    try:
        if str(UUID(thread_id)) != thread_id:
            raise ValueError("noncanonical")
    except (ValueError, TypeError, AttributeError) as exc:
        raise InvalidRequest("Codex import needs an exact native thread UUID") from exc
    if any(value is not None and (not isinstance(value, str) or not value.strip())
           for value in (model, effort)):
        raise InvalidRequest("import model/effort overrides must be nonempty strings")
    if source is None:
        from ccx.codex.runtime import read_import_source
        source = read_import_source(thread_id)
    thread = source.get("thread") if isinstance(source, dict) else None
    last = source.get("lastTurn") if isinstance(source, dict) else None
    if not isinstance(thread, dict) or thread.get("id") != thread_id:
        raise SessionUnavailable("Codex import native thread ID was not confirmed")
    status = thread.get("status")
    status = status.get("type") if isinstance(status, dict) else status
    if status not in (None, "idle", "notLoaded"):
        raise SessionUnavailable("Codex source has an active turn; wait for it to finish")
    if (not isinstance(last, dict) or not isinstance(last.get("id"), str)
            or not last["id"] or last.get("status") != "completed"):
        raise SessionUnavailable(
            "Codex import needs a latest completed native turn; no worker created"
        )
    cwd = thread.get("cwd")
    if not isinstance(cwd, str) or not cwd:
        raise SessionUnavailable("Codex source has no recorded workspace")
    # Native metadata may omit the latest model context. Never infer it from
    # prose/rollout files; leave omitted settings at the normal CCX defaults.
    native_model = last.get("model") or thread.get("model")
    native_effort = (last.get("reasoningEffort") or last.get("effort")
                     or thread.get("reasoningEffort"))
    chosen_model = model if model is not None else (
        native_model if isinstance(native_model, str) and native_model.strip() else None
    )
    chosen_effort = effort if effort is not None else (
        native_effort if isinstance(native_effort, str) and native_effort.strip() else None
    )
    directory, args = validated_create_spec(
        engine="codex", cwd=cwd, model=chosen_model, effort=chosen_effort,
        idle_timeout=DEFAULT_IDLE_TIMEOUT_S, permissions="full",
        sandbox=None, approval_policy=None, agent=None,
        append_system_prompt=None, allowed_tools=None, disallowed_tools=None,
        add_dir=None, permission_mode=None,
    )
    from ccx.codex.runtime import validate_model
    if model is not None:
        validate_model(model, cwd=directory)
    name = thread.get("name") or thread.get("title")
    if not isinstance(name, str) or not name.strip():
        name = None
    completed_at = last.get("completedAt")
    last_at = (int(completed_at * 1000)
               if type(completed_at) in (float, int) and completed_at > 0 else None)
    worker_id = mint_worker_id()
    anchor = {
        "engine": "codex", "sourceKind": "codex-thread",
        "sourceWorkerId": None, "sourceNativeThreadId": thread_id,
        "lastTurnId": last["id"], "cwd": directory,
        "createArgs": args,
        # The newest page cannot attest the historical total; zero here
        # counts managed submissions, NOT all source turns.
        "seed": {"stats": {"turns": 0}, "lastTurnAt": last_at,
                 "lastTurn": {"turnId": last["id"], "status": "completed"}},
        "overrides": {"model": model, "effort": effort},
        "forkedAtMs": int(time.time() * 1000),
    }
    return create(
        engine="codex", cwd=directory, worker_id=worker_id, name=name,
        model=chosen_model, effort=chosen_effort,
        permissions="full", idle_timeout=DEFAULT_IDLE_TIMEOUT_S,
        created_via="import", _fork_anchor=anchor, start_timeout=90,
    )
