"""Pure Claude plain context phrases over flat typed context fields.

The real CLI JSON/lifecycle/error behaviors live in file-backed full-UUID tests.
"""

from __future__ import annotations
import ccx.cli.commands.read as commands_read

import pytest

from ccx.cli.commands import _common as C

WORKER = "claude:aaaaaaaa-1234-4000-8000-000000000001"


def _known_ctx(**over):
    ctx = {
        "state": "idle",
        "tokens_used": 124_530,
        "as_of_turn_ref": None,
        "as_of_ms": None,
        "reason": None,
    }
    ctx.update(over)
    return ctx


# --- _format_context_line ----------------------------------------------------

def test_format_none_is_no_signal():
    assert commands_read._format_context_line(WORKER, None) == \
        f"{WORKER}  context: no signal yet (none recorded; updates after the next turn)"


def test_format_pending():
    line = commands_read._format_context_line(WORKER, {"state": "idle", "tokens_used": None, "reason": "pending"})
    assert "pending" in line
    assert line.startswith(f"{WORKER}  context:")


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        ("compacting", f"{WORKER}  context: unknown (compacting)"),
        ("just_compacted", f"{WORKER}  context: unknown (just compacted)"),
        ("future_reason", f"{WORKER}  context: unknown"),
    ],
)
def test_format_unknown_context(reason, expected):
    ctx = {"state": "dead", "tokens_used": None, "reason": reason}
    assert commands_read._format_context_line(WORKER, ctx) == expected


def test_format_known_shows_only_carried_tokens():
    line = commands_read._format_context_line(WORKER, _known_ctx())
    assert line == f"{WORKER}  context: 124,530 tokens"
