"""One sanitizer for every native payload CCX preserves or persists.

Hidden reasoning is the thing CCX must never write down. Both places that keep
native payloads — the normalized conversation document and the Codex evidence
journal — have to strip it exactly the same way, so the rule lives here, in a
module low enough that either can import it without importing the other.

The rule is structural rather than a list of known types: any mapping whose
``type`` mentions reasoning loses its raw ``content``, at every depth. A future
item type that nests a reasoning-shaped payload inside itself is covered
without being anticipated, while the engine's own surfaced ``summary`` — which
is not hidden — survives.
"""

from __future__ import annotations

from typing import Any


def is_reasoning_shaped(value: Any) -> bool:
    """Does this mapping look like it carries chain-of-thought?"""
    return (
        isinstance(value, dict)
        and "reasoning" in str(value.get("type", "")).lower()
    )


def safe_native(value: Any) -> Any:
    """A copy of one native payload that is safe to persist verbatim."""
    if isinstance(value, list):
        return [safe_native(entry) for entry in value]
    if not isinstance(value, dict):
        return value
    hide = is_reasoning_shaped(value)
    return {
        key: safe_native(entry)
        for key, entry in value.items()
        if not key.startswith("_ccx_")
        and not (hide and key == "content")
    }


__all__ = ["is_reasoning_shaped", "safe_native"]
