"""Explicit full-UUID worker inventory discovery; never runtime addressing.

The CLI resolves only selected user worker-reference fields before dispatch.
Ordinary client, V1, native owner and store APIs remain exact-ID-only.
"""
from __future__ import annotations

import re
from typing import Any

from ccx.errors import AmbiguousWorker, InvalidRequest, SessionUnavailable, worker_not_found
from ccx.store.identity import (
    ENGINES, EngineMismatch, IdentityError, UnknownWorker, canonical_worker_id,
)
from ccx.store.root import GenerationError, require_ccx_active

_PREFIX = re.compile(r"[0-9a-f]{8}[0-9a-f-]{0,27}\Z", re.ASCII)


def _query(value: str) -> tuple[str | None, str, bool]:
    """(explicit engine, textual UUID query, is_incomplete_prefix)."""
    if not isinstance(value, str) or not value:
        raise InvalidRequest("worker query must be a nonempty canonical UUID or prefix")
    if ":" in value:
        engine, sep, uuid_text = value.partition(":")
        if sep != ":" or engine not in ENGINES or ":" in uuid_text:
            raise InvalidRequest("worker query must be bare or claude:/codex: qualified")
    else:
        engine, uuid_text = None, value
    if len(uuid_text) == 36:
        try:
            canonical_worker_id(uuid_text)
        except IdentityError as exc:
            raise InvalidRequest(
                "complete worker UUID must be canonical lowercase UUIDv4"
            ) from exc
        return engine, uuid_text, False
    if not _PREFIX.fullmatch(uuid_text):
        raise InvalidRequest(
            "worker prefix must start with 8 lowercase hex characters, "
            "continue with lowercase hex/hyphens, and be at most 35 characters; "
            "no names, casefolding or compact-UUID aliases"
        )
    return engine, uuid_text, True


def _candidates(store, engine: str | None, prefix: str) -> list[str]:
    """Validated active workers whose full UUID starts with ``prefix``.

    Only owner files whose NAME matches are read and validated, so a lookup
    costs O(matches), not a validation of the whole fleet.
    """
    matches = []
    try:
        for worker_id in store.owner_ids(prefix=prefix):
            try:
                owner = store.resolve(f"{engine}:{worker_id}" if engine else worker_id)
            except (UnknownWorker, EngineMismatch):
                continue  # reserved/quarantined, or the other engine
            matches.append(owner.worker)
    except (GenerationError, IdentityError, OSError) as exc:
        raise SessionUnavailable(f"managed worker inventory unavailable: {exc}") from exc
    return sorted(matches)


def _revalidate(store) -> None:
    try:
        store.revalidate()
    except (GenerationError, OSError) as exc:
        raise SessionUnavailable(f"managed worker generation changed: {exc}") from exc


def _match(query: str, engine: str | None, prefix: str, store) -> str:
    matches = _candidates(store, engine, prefix)
    if not matches and engine is not None:
        other = _candidates(store, None, prefix)
        if len(other) == 1:
            found, _, worker_id = other[0].partition(":")
            raise InvalidRequest(f"{query} is a {found.capitalize()} worker; use "
                                 f"{found}:{worker_id[:8]} or the bare prefix")
    if not matches:
        raise worker_not_found(query)
    if len(matches) > 1:
        raise AmbiguousWorker(query, matches)
    return matches[0]


def resolve_worker(query: str) -> str:
    """Explicitly resolve a full UUID or unique textual prefix to engine:UUID.

    This is a current read-only inventory query, NOT permission to pass the
    prefix to ordinary Python methods, V1, host/daemon or persisted references.
    """
    from ccx import constants

    engine, body, incomplete = _query(query)
    try:
        store = require_ccx_active(constants.CCX_ROOT)
    except (GenerationError, OSError) as exc:
        raise SessionUnavailable(f"ACTIVE managed worker inventory unavailable: {exc}") from exc
    if not incomplete:
        try:
            owner = store.resolve(query)
        except UnknownWorker as exc:
            raise worker_not_found(query) from exc
        except EngineMismatch as exc:
            raise InvalidRequest(str(exc)) from exc
        except (GenerationError, OSError) as exc:
            raise SessionUnavailable(f"managed worker lookup unavailable: {exc}") from exc
        resolved = owner.worker
    else:
        resolved = _match(query, engine, body, store)
    _revalidate(store)
    return resolved


def resolve_cli_fields(namespace: Any, store, fields: tuple[str, ...], *,
                       check_exact: bool = False) -> None:
    """All-or-nothing namespace preflight; prefixes read only matching owners.

    Complete exact UUIDs stay on the handler's fast path unless ``check_exact``
    requests a missing-owner preflight. Multi-target report batches preserve
    their existing per-row error behavior.
    """
    prepared: dict[str, list[tuple[str, str | None, str, bool]]] = {}
    any_prefix = False
    for field in fields:
        value = getattr(namespace, field, None)
        if value is None:
            continue
        values = value if isinstance(value, list) else [value]
        parsed = []
        for item in values:
            engine, uuid_text, incomplete = _query(item)
            parsed.append((item, engine, uuid_text, incomplete))
            any_prefix = any_prefix or incomplete
        prepared[field] = parsed
    if check_exact:
        for specs in prepared.values():
            for original, _engine, _uuid_text, incomplete in specs:
                if incomplete:
                    continue
                try:
                    store.resolve(original)
                except UnknownWorker as exc:
                    if str(exc) == "full UUID is not present in global owner index":
                        raise worker_not_found(original) from exc
                    # Reserved/quarantined identities retain the verb's own
                    # refusal; they are not missing global owner entries.
                except EngineMismatch as exc:
                    raise InvalidRequest(str(exc)) from exc
                except (GenerationError, OSError):
                    # Other refusal reasons retain the handler's existing type,
                    # message and exit code rather than being normalized here.
                    pass
    if not any_prefix:
        return

    rewritten: dict[str, str | list[str]] = {}
    for field, specs in prepared.items():
        resolved = [
            _match(original, engine, uuid_text, store) if incomplete else original
            for original, engine, uuid_text, incomplete in specs
        ]
        rewritten[field] = (resolved if isinstance(getattr(namespace, field), list)
                            else resolved[0])
    _revalidate(store)
    # Nothing from any field was applied until every prefix succeeded.
    for field, resolved in rewritten.items():
        setattr(namespace, field, resolved)
