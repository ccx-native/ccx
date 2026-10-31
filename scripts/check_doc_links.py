#!/usr/bin/env python3
"""Fail if a Markdown file links to a repository file that doesn't exist.

Checks relative links in README.md, CONTRIBUTING.md, SECURITY.md, CHANGELOG.md, llms.txt
and every .md under docs/, skills/ and examples/. External URLs and pure
#anchors are skipped.
"""
from __future__ import annotations

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent.parent
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")


def files() -> list[Path]:
    found = [ROOT / name for name in ("README.md", "CONTRIBUTING.md", "SECURITY.md", "CHANGELOG.md", "llms.txt")]
    for folder in ("docs", "skills", "examples"):
        found += sorted((ROOT / folder).rglob("*.md"))
    return [path for path in found if path.is_file()]


def main() -> int:
    broken = []
    for path in files():
        text = re.sub(r"```.*?```", "", path.read_text(encoding="utf-8"), flags=re.S)
        for target in LINK.findall(text):
            if re.match(r"[a-z][a-z0-9+.-]*:", target) or target.startswith("#"):
                continue
            if not (path.parent / target.split("#", 1)[0]).exists():
                broken.append(f"{path.relative_to(ROOT)}: {target}")
    for line in broken:
        print(line)
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
