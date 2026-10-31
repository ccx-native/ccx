"""Small maintenance helpers for generated Codex app-server schemas.

The generated JSON files are developer/test artifacts, not a runtime protocol
implementation. The checker reads the retained aggregate schemas directly and
protects only the protocol facts CCX currently depends on.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SCHEMA_ROOT = Path(__file__).resolve().parents[2] / "tests" / "protocol" / "codex"
REFERENCE_CODEX_SCHEMA_VERSION = "0.147.0"
REFERENCE_SCHEMA_DIR = SCHEMA_ROOT / REFERENCE_CODEX_SCHEMA_VERSION
REFERENCE_SCHEMA_FILES = (
    "ClientNotification.json",
    "ClientRequest.json",
    "ServerNotification.json",
    "ServerRequest.json",
)

# These are maintainer assertions about the stable app-server surface CCX
# actively uses. They are intentionally not imported by production code.
CRITICAL_CLIENT_METHODS = (
    "account/rateLimits/read",
    "account/usage/read",
    "initialize",
    "model/list",
    "thread/archive",
    "thread/compact/start",
    "thread/list",
    "thread/read",
    "thread/resume",
    "thread/start",
    "thread/unarchive",
    "thread/unsubscribe",
    "turn/interrupt",
    "turn/start",
    "turn/steer",
)

CRITICAL_CLIENT_NOTIFICATIONS = ("initialized",)

# Native compaction in current Codex is represented by this item type inside an
# internal turn. The older `thread/compacted` notification remains a runtime
# compatibility fallback but is deprecated upstream and is not a hard contract.
CRITICAL_THREAD_ITEM_TYPES = ("contextCompaction",)

CRITICAL_SERVER_NOTIFICATIONS = (
    "account/rateLimits/updated",
    "error",
    "item/completed",
    "item/started",
    "serverRequest/resolved",
    "thread/status/changed",
    "thread/tokenUsage/updated",
    "turn/completed",
    "turn/started",
)

# (schema filename, definition, field, optional ThreadItem variant)
CRITICAL_FIELDS = (
    ("ClientRequest.json", "TurnStartParams", "clientUserMessageId", None),
    ("ClientRequest.json", "TurnSteerParams", "clientUserMessageId", None),
    ("ClientRequest.json", "TurnSteerParams", "expectedTurnId", None),
    ("ServerNotification.json", "ThreadItem", "clientId", "userMessage"),
    (
        "ServerNotification.json",
        "ItemStartedNotification",
        "startedAtMs",
        None,
    ),
    (
        "ServerNotification.json",
        "ItemCompletedNotification",
        "completedAtMs",
        None,
    ),
    ("ServerNotification.json", "ErrorNotification", "willRetry", None),
    (
        "ServerNotification.json",
        "AccountRateLimitsUpdatedNotification",
        "rateLimits",
        None,
    ),
)


def _load_schema(snapshot_dir: Path, filename: str) -> dict[str, Any]:
    path = snapshot_dir / filename
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ValueError(f"cannot read Codex schema {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"Codex schema {path} must be a JSON object")
    return value


def extract_method_names(snapshot_dir: Path, filename: str) -> set[str]:
    value = _load_schema(snapshot_dir, filename)
    methods: set[str] = set()
    for branch in value.get("oneOf") or []:
        if not isinstance(branch, dict):
            continue
        enum = (
            ((branch.get("properties") or {}).get("method") or {}).get("enum")
            or []
        )
        if enum and isinstance(enum[0], str):
            methods.add(enum[0])
    return methods


def extract_thread_item_types(snapshot_dir: Path) -> set[str]:
    value = _load_schema(snapshot_dir, "ServerNotification.json")
    definition = (value.get("definitions") or {}).get("ThreadItem") or {}
    types: set[str] = set()
    for branch in definition.get("oneOf") or []:
        if not isinstance(branch, dict):
            continue
        enum = (
            ((branch.get("properties") or {}).get("type") or {}).get("enum")
            or []
        )
        if enum and isinstance(enum[0], str):
            types.add(enum[0])
    return types


def protocol_summary(snapshot_dir: Path) -> dict[str, list[str]]:
    return {
        "threadItemTypes": sorted(extract_thread_item_types(snapshot_dir)),
        "clientNotificationMethods": sorted(
            extract_method_names(snapshot_dir, "ClientNotification.json")
        ),
        "notificationMethods": sorted(
            extract_method_names(snapshot_dir, "ServerNotification.json")
        ),
        "serverRequestMethods": sorted(
            extract_method_names(snapshot_dir, "ServerRequest.json")
        ),
    }


def _definition(snapshot_dir: Path, filename: str, name: str) -> dict[str, Any]:
    value = _load_schema(snapshot_dir, filename)
    definition = (value.get("definitions") or {}).get(name)
    if not isinstance(definition, dict):
        raise ValueError(f"{filename} has no definition {name!r}")
    return definition


def _field_present(
    snapshot_dir: Path, spec: tuple[str, str, str, str | None]
) -> bool:
    filename, definition_name, field, variant = spec
    definition = _definition(snapshot_dir, filename, definition_name)
    if variant is not None:
        definition = next(
            (
                branch
                for branch in definition.get("oneOf") or []
                if isinstance(branch, dict)
                and variant
                in (
                    ((branch.get("properties") or {}).get("type") or {}).get(
                        "enum"
                    )
                    or []
                )
            ),
            None,
        )
        if not isinstance(definition, dict):
            return False
    return field in (definition.get("properties") or {})


def critical_schema_projection(snapshot_dir: Path) -> dict[str, Any]:
    """Extract the deliberately small set of facts CCX depends on."""
    return {
        "threadItemTypes": {
            item_type: item_type in extract_thread_item_types(snapshot_dir)
            for item_type in CRITICAL_THREAD_ITEM_TYPES
        },
        "clientMethods": {
            method: method in extract_method_names(snapshot_dir, "ClientRequest.json")
            for method in CRITICAL_CLIENT_METHODS
        },
        "clientNotificationMethods": {
            method: method
            in extract_method_names(snapshot_dir, "ClientNotification.json")
            for method in CRITICAL_CLIENT_NOTIFICATIONS
        },
        "notificationMethods": {
            method: method
            in extract_method_names(snapshot_dir, "ServerNotification.json")
            for method in CRITICAL_SERVER_NOTIFICATIONS
        },
        "fields": {
            _critical_field_key(spec): _field_present(snapshot_dir, spec)
            for spec in CRITICAL_FIELDS
        },
    }


def _critical_field_key(spec: tuple[str, str, str, str | None]) -> str:
    filename, definition, field, variant = spec
    return f"{filename}:{definition}:{field}:{variant or '-'}"


def missing_critical_schema_facts(snapshot_dir: Path) -> list[str]:
    projection = critical_schema_projection(snapshot_dir)
    missing = [
        f"thread item type {item_type}"
        for item_type, present in projection["threadItemTypes"].items()
        if not present
    ]
    missing.extend(
        f"client method {method}"
        for method, present in projection["clientMethods"].items()
        if not present
    )
    missing.extend(
        f"client notification {method}"
        for method, present in projection["clientNotificationMethods"].items()
        if not present
    )
    missing.extend(
        f"server notification {method}"
        for method, present in projection["notificationMethods"].items()
        if not present
    )
    missing.extend(
        f"field {key}"
        for key, present in projection["fields"].items()
        if not present
    )
    return missing


def validate_reference_schema(
    snapshot_dir: Path = REFERENCE_SCHEMA_DIR,
) -> dict[str, list[str]]:
    """Parse the reference aggregates and verify CCX-critical facts."""
    if not snapshot_dir.is_dir():
        raise ValueError(
            f"Codex reference schema directory does not exist: {snapshot_dir}"
        )
    for filename in REFERENCE_SCHEMA_FILES:
        _load_schema(snapshot_dir, filename)
    summary = protocol_summary(snapshot_dir)
    missing = missing_critical_schema_facts(snapshot_dir)
    if missing:
        raise ValueError(
            "reference schema is missing compatibility-critical facts: "
            + ", ".join(missing)
        )
    return summary


def semantic_schema_diff(
    reference_dir: Path,
    candidate_dir: Path,
) -> dict[str, Any]:
    """Compare protocol unions and CCX-critical facts between aggregates."""
    reference = protocol_summary(reference_dir)
    candidate = protocol_summary(candidate_dir)
    diff: dict[str, Any] = {}
    for key in (
        "threadItemTypes",
        "clientNotificationMethods",
        "notificationMethods",
        "serverRequestMethods",
    ):
        diff[key] = {
            "added": sorted(set(candidate[key]) - set(reference[key])),
            "removed": sorted(set(reference[key]) - set(candidate[key])),
        }
    diff["criticalMissing"] = missing_critical_schema_facts(candidate_dir)
    return diff


__all__ = [
    "CRITICAL_CLIENT_METHODS",
    "CRITICAL_CLIENT_NOTIFICATIONS",
    "CRITICAL_FIELDS",
    "CRITICAL_SERVER_NOTIFICATIONS",
    "CRITICAL_THREAD_ITEM_TYPES",
    "REFERENCE_CODEX_SCHEMA_VERSION",
    "REFERENCE_SCHEMA_DIR",
    "REFERENCE_SCHEMA_FILES",
    "SCHEMA_ROOT",
    "critical_schema_projection",
    "extract_method_names",
    "extract_thread_item_types",
    "missing_critical_schema_facts",
    "protocol_summary",
    "semantic_schema_diff",
    "validate_reference_schema",
]
