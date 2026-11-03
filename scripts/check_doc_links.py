#!/usr/bin/env python3
"""Fail if a Markdown link points to a missing repository file or heading.

Checks relative links in README.md, CONTRIBUTING.md, SECURITY.md, CHANGELOG.md, llms.txt
and every .md under docs/, skills/ and examples/. External URLs are skipped.
A #fragment must match a heading in the target Markdown file (GitHub slugs).
"""
from __future__ import annotations

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent.parent
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*#*\s*$", re.M)


def files() -> list[Path]:
    found = [ROOT / name for name in ("README.md", "CONTRIBUTING.md", "SECURITY.md", "CHANGELOG.md", "llms.txt")]
    for folder in ("docs", "skills", "examples"):
        found += sorted((ROOT / folder).rglob("*.md"))
    return [path for path in found if path.is_file()]


def prose(path: Path) -> str:
    return re.sub(r"```.*?```", "", path.read_text(encoding="utf-8"), flags=re.S)


def anchors(path: Path) -> set[str]:
    """GitHub heading slugs: lowercase, drop punctuation, spaces to hyphens."""
    seen: dict[str, int] = {}
    out = set()
    for title in HEADING.findall(prose(path)):
        title = re.sub(r"`|\*|\[([^\]]*)\]\([^)]*\)", r"\1", title)
        slug = re.sub(r"[^\w\- ]", "", title.lower()).replace(" ", "-")
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        out.add(slug if count == 0 else f"{slug}-{count}")
    return out


def main() -> int:
    broken = []
    for path in files():
        for target in LINK.findall(prose(path)):
            if re.match(r"[a-z][a-z0-9+.-]*:", target):
                continue
            name, _, fragment = target.partition("#")
            dest = (path.parent / name) if name else path
            if not dest.exists():
                broken.append(f"{path.relative_to(ROOT)}: {target}")
            elif fragment and dest.suffix == ".md" and fragment not in anchors(dest):
                broken.append(f"{path.relative_to(ROOT)}: {target} (no such heading)")
    for line in broken:
        print(line)
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
