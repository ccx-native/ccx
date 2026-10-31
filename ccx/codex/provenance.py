"""CCX-owned client ids for the user messages CCX sends into Codex.

Native Codex accepts an optional ``clientUserMessageId`` on ``turn/start`` and
``turn/steer`` and persists it on the resulting ``userMessage`` item as
``clientId``. That is the only durable, first-class way to tell — reading a
thread back cold, possibly on another machine — which user messages CCX itself
put there, and whether one started a turn or steered a running one.

The id names the semantic user input, not a JSON-RPC attempt: an explicit
overload retry re-sends the identical request and therefore reuses the same id,
while a genuinely new input always gets a fresh one.

Parsing is deliberately conservative. Only the exact format this module emits
is recognized; an id written by another client, a malformed one, a legacy
``null``, and anything CCX cannot prove it authored all stay unknown rather
than being guessed at from position, text, or turn shape.
"""

from __future__ import annotations

import re
import uuid

# Historical v1 native client message IDs remain immutable forensic facts.
# Mint v2 only for new full-UUID owners; no rewrite of original operation IDs.
FORMAT_VERSION = "v2"

START = "start"
STEER = "steer"
KINDS = (START, STEER)

_ORIGINS = {START: "ccx-start", STEER: "ccx-steer"}

# Original short-keyed v1 native client IDs remain parseable but not
# managed-worker addressing authority; v2 uses the complete owner UUID.
_PATTERN = re.compile(
    r"^ccx:"
    r"v1:"
    r"(?P<short>[0-9a-f]{8}):"
    rf"(?P<kind>{START}|{STEER}):"
    r"(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$"
)

# Historical v1 IDs in existing native Codex threads remain forensic
# provenance and are never rewritten. New full-UUID identities use v2
# with one independent canonical full owner (no truncated alias).
_PATTERN_V2 = re.compile(
    r"^ccx:v2:"
    r"(?P<workerId>[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}):"
    rf"(?P<kind>{START}|{STEER}):"
    r"(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$"
)


def new_client_message_id(short: str, kind: str) -> str:
    """Mint one id for a single semantic user input.

    Carries no prompt text — an id ends up in a native thread CCX does not own
    the retention of, so it must be pure provenance.
    """
    if kind not in KINDS:
        raise ValueError(f"unknown client message kind: {kind!r}")
    from ccx.store.identity import InvalidTarget, canonical_worker_id
    try:
        owner = canonical_worker_id(short)
    except InvalidTarget as exc:
        raise ValueError(
            "new Codex clientUserMessageId requires canonical full CCX UUIDv4"
        ) from exc
    return f"ccx:{FORMAT_VERSION}:{owner}:{kind}:{uuid.uuid4()}"


def parse_client_message_id(value: object) -> dict[str, str] | None:
    """Decompose an id CCX can prove it authored, else ``None``."""
    if not isinstance(value, str):
        return None
    match = _PATTERN_V2.match(value) or _PATTERN.match(value)
    if match is None:
        return None
    parsed = match.groupdict()
    # `uuid4()` produces the canonical lowercase form the pattern requires, so
    # a value that parses but does not round-trip was not written by us.
    if str(uuid.UUID(parsed["uuid"])) != parsed["uuid"]:
        return None
    return parsed


def origin_of(value: object) -> str | None:
    """``"ccx-start"``/``"ccx-steer"`` for a CCX id, ``None`` for anything else.

    ``None`` is a real answer: the message exists and its origin is unknown.
    """
    parsed = parse_client_message_id(value)
    return _ORIGINS[parsed["kind"]] if parsed else None


__all__ = [
    "FORMAT_VERSION",
    "KINDS",
    "START",
    "STEER",
    "new_client_message_id",
    "origin_of",
    "parse_client_message_id",
]
