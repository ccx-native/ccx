from __future__ import annotations
import base64
import hashlib
import json
import os
from typing import Any
from ccx.store.liveness import recorded_host_loaded
from ccx.client import CcxClient
from ccx.codex.client import CodexClient
from ccx.errors import (
    InvalidRequest,
    SessionUnavailable,
    worker_not_found,
)
from ccx.workers import WorkerClient
from ccx.store.identity import canonical_worker_id


_TRANSCRIPT_LOCATOR_PREFIX = "ccxt2."


_TRANSCRIPT_CURSOR_PREFIX = "ccxtc2."


_DEFAULT_TRANSCRIPT_PAGE = 20


_DEFAULT_TRANSCRIPT_TOOL_BYTES = 64 * 1024


_DEFAULT_REPORT_CHARS = 64 * 1024


MAX_CALLER_METADATA_BYTES = 4 * 1024


_ENGINES = frozenset({"claude", "codex"})


_PERMISSIONS = frozenset({"full", "restricted"})


class _LazyClient:
    """Keep observational protocol imports free of directory initialization."""

    def __init__(self, factory):
        self.factory = factory
        self.value = None

    def __getattr__(self, name):
        if self.value is None:
            self.value = self.factory()
        return getattr(self.value, name)


_CLIENT = _LazyClient(CcxClient)


_CODEX_CLIENT = _LazyClient(CodexClient)


_WORKERS = _LazyClient(WorkerClient)


def _normalize_caller_metadata(
    value: dict[str, Any] | None,
    *,
    notify_parent: bool,
) -> dict[str, Any] | None:
    if value is None:
        return None
    if not notify_parent:
        raise InvalidRequest(
            "send: caller_metadata requires notify_parent=true"
        )
    if not isinstance(value, dict):
        raise InvalidRequest("send: caller_metadata must be a JSON object")
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as error:
        raise InvalidRequest(
            "send: caller_metadata must contain only valid JSON values"
        ) from error
    if len(encoded) > MAX_CALLER_METADATA_BYTES:
        raise InvalidRequest(
            "send: caller_metadata exceeds the 4096-byte serialized limit"
        )
    # Return a detached, JSON-normalized object so later caller mutation cannot
    # change the durable semantic value between receipt and notification writes.
    return json.loads(encoded.decode("utf-8"))


def split_target(target: str) -> tuple[str, str, str]:
    """Exact globally owned full UUID; never short/name/provider aliases.

    Direct protocol helper now requires the same sealed ACTIVE generation as
    CLI, one-shot and direct Python. It returns (full handle, engine, full ID),
    not the previous short. No native discovery or legacy record fallback.
    """
    from ccx.constants import CCX_ROOT
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.identity import UnknownWorker, IdentityError
    try:
        store = require_ccx_active(CCX_ROOT)
        owner = store.resolve(target)
        return owner.worker, owner.engine, owner.worker_id
    except UnknownWorker as exc:
        raise worker_not_found(target) from exc
    except IdentityError as exc:
        raise InvalidRequest(f"target must be a canonical managed full UUID: {exc}") from exc
    except (GenerationError, OSError) as exc:
        raise SessionUnavailable(
            f"CCX worker records are unavailable; check CCX_ROOT: {exc}"
        ) from exc


def _encode_opaque(prefix: str, payload: dict[str, Any]) -> str:
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return prefix + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_opaque(prefix: str, value: str, *, label: str) -> dict[str, Any]:
    if not isinstance(value, str) or not value.startswith(prefix):
        raise InvalidRequest(f"invalid {label}")
    body = value[len(prefix):]
    padding = "=" * ((4 - len(body) % 4) % 4)
    try:
        decoded = base64.urlsafe_b64decode((body + padding).encode("ascii"))
        payload = json.loads(decoded)
    except (ValueError, UnicodeError, TypeError) as error:
        raise InvalidRequest(f"invalid {label}") from error
    if not isinstance(payload, dict):
        raise InvalidRequest(f"invalid {label}")
    return payload


def _transcript_locator_owned(store, owner, rec: dict[str, Any]) -> str | None:
    """Total, I/O-free token projection of an already-loaded original binding.

    Inspect is also embedded in *completed* native-effect receipts. An absent
    or contradictory binding must never turn such a receipt into a failure.
    The original source is independently re-proven by conversation() on read.
    """
    try:
        if (not isinstance(rec, dict) or rec.get("workerId") != owner.worker_id
                or rec.get("engine") != owner.engine
                or rec.get("fingerprintVersion") != 2
                or not isinstance(rec.get("createSha256"), str)
                or len(rec["createSha256"]) != 64
                or any(ch not in "0123456789abcdef" for ch in rec["createSha256"])
                or rec.get("terminalOnly")):
            return None
        canonical_worker_id(owner.worker_id)
        native = rec.get("native")
        if not isinstance(native, dict):
            return None
        if owner.engine == "claude":
            canonical_worker_id(native.get("sessionId"))
            if (native.get("nativeState") != "session-bound"
                    or not isinstance(rec.get("transcript"), str)
                    or not rec["transcript"]):
                return None
        elif owner.engine == "codex":
            if (native.get("nativeState") != "thread-bound"
                    or not isinstance(native.get("threadId"), str)
                    or not native["threadId"]
                    or native.get("threadSource") != store.codex_source(owner.worker)):
                return None
        else:
            return None
        return _encode_opaque(_TRANSCRIPT_LOCATOR_PREFIX, {
            "v": 2, "engine": owner.engine, "worker_id": owner.worker_id,
            "root": hashlib.sha256(os.fsencode(store.root)).hexdigest(),
            "generation": store.generation,
        })
    except Exception:
        return None


def transcript_locator(target: str) -> str | None:
    """Locator for one exact owned full UUID; no legacy short/name aliases."""
    from ccx.constants import CCX_ROOT
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.identity import IdentityError, UnknownWorker

    try:
        store = require_ccx_active(CCX_ROOT)
        owner = store.resolve(target)
        rec = store.record(owner.worker)
        locator = _transcript_locator_owned(store, owner, rec)
        store.revalidate()
        return locator
    except UnknownWorker as error:
        raise worker_not_found(target) from error
    except IdentityError as error:
        raise InvalidRequest(f"Invalid worker for conversation read; use a worker handle: {error}") from error
    except (GenerationError, OSError) as error:
        raise SessionUnavailable(f"Cannot locate this worker conversation; check the worker: {error}") from error


def recorded_inspector(store, target: str) -> dict[str, Any]:
    from ccx.ops.forking import public_lineage

    owner = store.resolve(target)
    rec = store.record(owner.worker)
    snapshot = rec.get("backgroundTaskSummary")
    if isinstance(snapshot, dict):
        facts = {
            "background_task_summary": snapshot,
            "running_background_task_count": snapshot.get("running_count",
                                                     snapshot.get("runningCount")),
        }
        background = background_task_block(facts, supported=True)
    else:
        # Historical Claude inspection can still use the explicit diagnostic
        # extractor; the normal recorded owner snapshot is preferred.
        facts = (CcxClient._uuid_background_task_facts_owned(store, owner, rec)
                 if owner.engine == "claude" else None)
        background = background_task_block(facts, supported=owner.engine == "claude")
    lifecycle = rec.get("lifecycle")
    lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
    native = rec.get("native")
    native = native if isinstance(native, dict) else {}
    args = rec.get("createArgs")
    args = args if isinstance(args, dict) else {}
    stats = rec.get("stats")
    stats = stats if isinstance(stats, dict) else {}
    return {
        "worker": owner.worker,
        "worker_id": owner.worker_id,
        "engine": owner.engine,
        "short": owner.short,  # display only
        "name": rec.get("name"),
        "cwd": rec.get("cwd"),
        "forked_from": public_lineage(rec),
        "state": {
            "state": lifecycle.get("state"),
            "native_state": native.get("nativeState"),
            "source": "recorded",
        },
        "host": "unobserved",
        "loaded": recorded_host_loaded(owner.engine, lifecycle),
        "native": {
            "session_id": native.get("sessionId") if owner.engine == "claude" else None,
            "thread_id": native.get("threadId") if owner.engine == "codex" else None,
            "thread_source": native.get("threadSource") if owner.engine == "codex" else None,
        },
        "pending_input": None,
        "pending_input_observed": False,
        "created_at": rec.get("createdAt"),
        "last_turn_at": rec.get("lastTurnAt"),
        "managed_turns": stats.get("turns"),
        "model": args.get("model"),
        "effort": args.get("effort"),
        "terminal_only": bool(rec.get("terminalOnly")),
        "record_revision": rec.get("recordRevision"),
        "context": None,
        "transcript_locator": _transcript_locator_owned(store, owner, rec),
        "active_turn_ref": None,
        "active_turn_ref_source": "unobserved",
        **background,
    }


def _v1_postnative_inspect(
    info: dict, expected_generation: str | None, *, unproven: str, incomplete: str,
) -> dict[str, Any]:
    from ccx import constants
    from ccx.errors import CreatePartialFailure
    from ccx.store.root import require_ccx_active

    if (info.get("native_ready") is not True
            or info.get("activation_state") != "confirmed"):
        raise CreatePartialFailure(
            unproven, info=info, phase="native-activation",
        )
    try:
        store = require_ccx_active(
            constants.CCX_ROOT, expected_generation=expected_generation,
        )
        inspected = recorded_inspector(store, info["worker"])
        store.revalidate()
        return {
            **inspected, "worker_id": info["worker_id"],
            "created": info["created"], "reconciled": info["reconciled"],
        }
    except Exception as error:
        raise CreatePartialFailure(
            incomplete, info=info, phase="inspect", cause=error,
        ) from error


_BACKGROUND_BLOCK_MAX_BYTES = 64 * 1024


_BACKGROUND_TEXT_MAX_CHARS = 1024


_BACKGROUND_TEXT_FIELDS = (
    "command", "description", "output_file", "summary", "last_event",
)


_BACKGROUND_FACT_KEYS = (
    "background_tasks",
    "background_task_count",
    "running_background_task_count",
    "open_background_task_count",
    "active_background_task_count",
    "stale_background_task_count",
    "background_tasks_truncated",
    "background_task_limit",
    "background_task_hint",
    "background_task_summary",
)


def _background_block_size(block: dict[str, Any]) -> int:
    return len(json.dumps(
        block, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        default=str,
    ).encode("utf-8"))


def background_task_block(
    facts: dict[str, Any] | None, *, supported: bool
) -> dict[str, Any]:
    """Bound one ``_background_task_facts`` projection for local protocol.

    Unsupported engines get only ``background_tasks_supported=false``: absent
    counts are not an observed empty inventory. Identities and counts are kept
    exactly; ``background_task_details_truncated`` reports text clipping or
    detail omission, while ``background_tasks_truncated`` keeps meaning "the
    list is not the whole inventory".
    """
    if not supported:
        return {"background_tasks_supported": False}
    facts = facts or {}
    block: dict[str, Any] = {"background_tasks_supported": True}
    for key in _BACKGROUND_FACT_KEYS:
        if key in facts:
            block[key] = facts[key]
    details_truncated = False
    tasks = []
    for task in block.get("background_tasks") or []:
        if not isinstance(task, dict):
            continue
        task = dict(task)
        for field_name in _BACKGROUND_TEXT_FIELDS:
            value = task.get(field_name)
            if isinstance(value, str) and len(value) > _BACKGROUND_TEXT_MAX_CHARS:
                task[field_name] = value[:_BACKGROUND_TEXT_MAX_CHARS]
                details_truncated = True
        tasks.append(task)
    block["background_tasks"] = tasks
    hint = block.get("background_task_hint")
    if isinstance(hint, str) and len(hint) > _BACKGROUND_TEXT_MAX_CHARS:
        block["background_task_hint"] = hint[:_BACKGROUND_TEXT_MAX_CHARS]
        details_truncated = True
    block["background_task_details_truncated"] = details_truncated
    # Over budget: drop text details oldest-first, then oldest entries.
    for task in tasks:
        if _background_block_size(block) <= _BACKGROUND_BLOCK_MAX_BYTES:
            break
        for field_name in _BACKGROUND_TEXT_FIELDS:
            if task.get(field_name) is not None:
                task[field_name] = None
                block["background_task_details_truncated"] = True
    while tasks and _background_block_size(block) > _BACKGROUND_BLOCK_MAX_BYTES:
        tasks.pop(0)
        block["background_tasks_truncated"] = True
    return block


MAX_RESPONSE_ID = 128
