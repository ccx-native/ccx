"""Harness-specific CLI policy for blocking versus continuation modes."""

from __future__ import annotations

import sys

from ccx.constants import EXIT_ERROR
from ccx.controllers.orchestrator import codex_blocking_override_enabled, current_caller


def blocking_turn_family(argv: list[str]) -> str | None:
    """Classify calls whose blocking completion has a durable continuation sibling.

    This includes model turns and native compaction. Operational waits such as
    kill confirmation and report reads stay synchronous because notification is
    not a semantic replacement for them.
    """
    if not argv or "--help" in argv or "-h" in argv:
        return None
    command, flags = argv[0], argv[1:]
    if command in {"new", "fork"} and "--wait" in flags:
        return command
    # Every managed root send is a strict new turn and waits by default,
    # regardless of the child engine. No target/native probe is needed here.
    if command == "send" and "--no-wait" not in flags and "--notify-parent" not in flags:
        return "send"
    if command == "wait":
        return "wait.next-turn" if "--next-turn" in flags else "wait"
    if command in {"answer", "approve", "reject", "respond"} and "--wait" in flags:
        return command
    if command == "compact" and "--no-wait" not in flags and "--notify-parent" not in flags:
        return "compact"
    return None


def _guidance(family: str) -> str:
    if family == "wait.next-turn":
        return "use `ccx notify TARGET --next-turn` to arm a durable future-turn continuation"
    if family in {"wait"}:
        return "use `ccx notify TARGET` to arm a durable parent continuation"
    if family == "compact":
        return "use `--notify-parent` so Codex is resumed when the exact compact operation settles"
    return "use `--notify-parent` instead of blocking for the worker turn"


def reject_codex_blocking_call(argv: list[str]) -> None:
    family = blocking_turn_family(argv)
    if family is None or codex_blocking_override_enabled():
        return
    if not current_caller().supports_parent_notify:
        return
    sys.stderr.write(
        "[ccx] blocking CCX waits are disabled for Codex orchestrators when a durable continuation exists; "
        + _guidance(family)
        + ". The notification returns immediately and resumes Codex when the awaited operation settles.\n"
    )
    raise SystemExit(EXIT_ERROR)
