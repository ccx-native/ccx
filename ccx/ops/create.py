from __future__ import annotations
import time
from typing import Any
from ccx.constants import DEFAULT_IDLE_TIMEOUT_S
from ccx.ops.common import _v1_postnative_inspect


def create_worker(
    *, name: str | None, cwd: str, engine: str, worker_id: str | None = None,
    model: str | None = None, effort: str | None = None,
    idle_timeout: int = DEFAULT_IDLE_TIMEOUT_S,
    permissions: str | None = None, sandbox: str | None = None,
    approval_policy: str | None = None, timeout: float | None = None,
    created_via: str = "local-json-v1", expected_generation: str | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Verified original native create, then the public recorded inspect shape."""
    from ccx.ops.creation import create

    info = create(
        engine=engine, cwd=cwd, worker_id=worker_id, name=name,
        model=model, effort=effort, idle_timeout=idle_timeout,
        permissions=permissions, sandbox=sandbox,
        approval_policy=approval_policy, created_via=created_via,
        start_timeout=min(90., timeout) if timeout is not None else None,
        expected_generation=expected_generation, deadline=deadline,
    )
    return _v1_postnative_inspect(
        info, expected_generation,
        unproven="worker create has no proven original native activation",
        incomplete="worker creation committed but inspection is incomplete",
    )


def fork_worker(
    *, target: str, worker_id: str | None = None, name: str | None = None,
    model: str | None = None, effort: str | None = None,
    timeout: float | None = None, created_via: str = "local-json-v1",
    expected_generation: str | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Promptless original full-UUID fork, then main's recorded inspect shape."""
    from ccx.ops.forking import fork

    if deadline is None and timeout is not None:
        deadline = time.monotonic() + timeout
    info = fork(
        target, worker_id=worker_id, name=name, model=model, effort=effort,
        created_via=created_via, expected_generation=expected_generation,
        deadline=deadline,
    )
    return _v1_postnative_inspect(
        info, expected_generation,
        unproven="worker fork has no proven original native activation",
        incomplete="worker fork committed but inspection is incomplete",
    )
