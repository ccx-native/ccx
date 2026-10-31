#!/usr/bin/env python3
"""Validate CCX's Codex reference schema and compare later candidates.

Offline reference check::

    python scripts/check_codex_protocol.py

Candidate schema check::

    codex app-server generate-json-schema --out /tmp/codex-schema
    python scripts/check_codex_protocol.py --generated /tmp/codex-schema

The candidate directory is compared semantically with the checked-in
reference.  Additive protocol surface is reported for review; removal of a
compatibility-critical fact fails the check.  The installed Codex version is
not consulted by this tool.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ccx.codex.schema import (  # noqa: E402
    REFERENCE_SCHEMA_DIR,
    semantic_schema_diff,
    validate_reference_schema,
)


def check(
    *,
    generated: Path | None = None,
) -> tuple[Path, dict[str, list[str]], dict[str, Any]]:
    """Return ``(reference, summary, report)`` or raise on a real mismatch."""
    reference = REFERENCE_SCHEMA_DIR
    summary = validate_reference_schema(reference)
    report: dict[str, Any] = {"mode": "offline", "diff": None}
    if generated is None:
        return reference, summary, report
    if not generated.is_dir():
        raise RuntimeError(f"generated schema directory does not exist: {generated}")

    diff = semantic_schema_diff(reference, generated)
    report["mode"] = "candidate"
    report["diff"] = diff
    if diff["criticalMissing"]:
        raise RuntimeError(
            "candidate schema is missing compatibility-critical facts: "
            + ", ".join(diff["criticalMissing"])
        )
    return reference, summary, report


def _print_diff(diff: dict[str, Any]) -> None:
    labels = (
        ("threadItemTypes", "ThreadItem variants"),
        ("clientNotificationMethods", "client notifications"),
        ("notificationMethods", "server notifications"),
        ("serverRequestMethods", "server requests"),
    )
    for key, label in labels:
        values = diff[key]
        added = values["added"] or []
        removed = values["removed"] or []
        print(f"  {label} added: {', '.join(added) or '-'}")
        print(f"  {label} removed: {', '.join(removed) or '-'}")
    print("  compatibility-critical facts: present")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--generated",
        type=Path,
        help="directory produced by codex app-server generate-json-schema",
    )
    args = parser.parse_args(argv)

    try:
        checked, summary, report = check(
            generated=args.generated,
        )
    except (OSError, RuntimeError, ValueError) as error:
        print(f"codex protocol check: {error}", file=sys.stderr)
        return 1

    print(
        f"Codex reference {checked} is valid; "
        f"{len(summary['threadItemTypes'])} ThreadItem types, "
        f"{len(summary['clientNotificationMethods'])} client notifications, "
        f"{len(summary['notificationMethods'])} notifications, "
        f"{len(summary['serverRequestMethods'])} server requests"
    )
    if report["mode"] == "candidate":
        print(f"Candidate schema compared with reference {checked.name}:")
        _print_diff(report["diff"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
