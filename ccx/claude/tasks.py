"""Read-only projection of Claude Code's native structured task list.

Claude Code 2.1.260 stores ordinary session tasks below
``~/.claude/tasks/<session-id>/`` as one JSON file per task. This module is the
compatibility boundary for that private upstream layout: callers receive typed
normalized tasks and never touch Claude's lock/high-watermark files.

Reads are deliberately tolerant. Claude may replace files while we inspect the
directory, future versions may add fields/statuses, and a task directory may be
created or removed between syscalls. None of those conditions should make a CCX
worker unreadable.
"""

from __future__ import annotations

import json
from pathlib import Path

from ccx.store.paths import task_list_dir_for
from ccx.types import ClaudeTask


def _sort_key(task: ClaudeTask) -> tuple[int, int | str]:
    try:
        return (0, int(task.id))
    except (TypeError, ValueError):
        return (1, task.id)


def read_task_list(session_id: str) -> list[ClaudeTask]:
    """Return the current native task list for ``session_id``.

    Missing/unreadable/partially-written task state is an empty or partial list,
    never an exception. Internal Claude tasks (``metadata._internal``) are omitted
    just like Claude Code's own task-list UI. No lock is acquired and no file is
    ever mutated.
    """
    root = task_list_dir_for(session_id)
    if root is None:
        return []
    try:
        entries = list(root.iterdir())
    except OSError:
        return []

    tasks: list[ClaudeTask] = []
    for path in entries:
        if path.suffix != ".json" or path.name.startswith("."):
            continue
        try:
            # Native task files are ordinary files. Refuse symlinks so a corrupt
            # or adversarial task directory cannot turn this read-only helper into
            # an arbitrary-file reader.
            if path.is_symlink() or not path.is_file():
                continue
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            continue
        if not isinstance(raw, dict):
            continue
        metadata = raw.get("metadata")
        if isinstance(metadata, dict) and metadata.get("_internal"):
            continue
        task = ClaudeTask.from_dict(raw)
        if not task.id:
            # A nameless task cannot be updated/addressed coherently and is most
            # likely a transient/corrupt file. Ignore it rather than invent an id.
            continue
        tasks.append(task)

    tasks.sort(key=_sort_key)
    return tasks
