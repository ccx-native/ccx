"""Recognize standalone local image paths without changing prompt APIs."""
from __future__ import annotations

import os
import re

_IMAGE_LINE_RE = re.compile(r"/[^\r\n]*\.(?:png|jpe?g|gif|webp)", re.IGNORECASE)


def looks_like_image_line(line: str) -> bool:
    """Syntax only: a stripped, absolute POSIX path with an image suffix."""
    return _IMAGE_LINE_RE.fullmatch(line.strip()) is not None


def split_prompt_images(text: str) -> tuple[list[str], str]:
    """Extract existing image files in line order, preserving other text.

    Blank separators touching removed image lines collapse to one internal
    blank line (none at the edges). Unrelated lines keep their original bytes,
    including CRLF and trailing spaces. Files are checked at delivery time.
    """
    lines = text.splitlines(keepends=True)
    images = [line.strip() for line in lines
              if looks_like_image_line(line) and os.path.isfile(line.strip())]
    if not images:
        return [], text
    paths = set(images)
    remaining: list[str] = []
    i = 0
    while i < len(lines):
        if lines[i].strip() and lines[i].strip() not in paths:
            remaining.append(lines[i])
            i += 1
            continue
        start = i
        while i < len(lines) and (not lines[i].strip() or lines[i].strip() in paths):
            i += 1
        region = lines[start:i]
        if any(line.strip() in paths for line in region):
            if start > 0 and i < len(lines):
                blank = next((line for line in region if not line.strip()), None)
                if blank is not None:
                    remaining.append(blank)
        else:
            remaining.extend(region)
    return images, "".join(remaining)
