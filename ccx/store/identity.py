"""Exact CCX worker identity and engine-qualified target validation.

Pure canonical UUID and owner contracts; no file I/O, provider discovery,
environment lookup, or runtime compatibility aliases.
"""
from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

IDENTITY_SCHEMA_VERSION = 2
MANIFEST_SCHEMA_VERSION = 1
ENGINES = frozenset({"claude", "codex"})
OWNER_STATES = frozenset({"worker", "indeterminate-reserved", "quarantined"})


class IdentityError(ValueError):
    """Unusable or contradictory CCX-managed identity."""


class InvalidTarget(IdentityError):
    """Not a complete canonical managed UUID or qualified UUID."""


class UnknownWorker(IdentityError):
    """Canonical UUID has no CCX-owned record or is reserved."""


class EngineMismatch(IdentityError):
    """An explicit engine disagrees with the immutable CCX owner."""


class OwnerConflict(IdentityError):
    """One full UUID has two inconsistent owners/reservations."""


def canonical_worker_id(value: str) -> str:
    """Require lowercase, hyphenated RFC 4122 UUIDv4 (including variant)."""
    if not isinstance(value, str):
        raise InvalidTarget("worker id must be a canonical lowercase UUIDv4")
    try:
        value_uuid = UUID(value)
    except (ValueError, TypeError, AttributeError) as exc:
        raise InvalidTarget("worker id must be a canonical lowercase UUIDv4") from exc
    if value_uuid.version != 4 or value_uuid.variant != "specified in RFC 4122" or str(value_uuid) != value:
        raise InvalidTarget("worker id must be a canonical lowercase UUIDv4")
    return value


def mint_worker_id() -> str:
    """Independently random UUIDv4; NEVER preserve or derive an old short."""
    return str(uuid4())


@dataclass(frozen=True)
class ManagedTarget:
    worker_id: str
    qualified_engine: str | None = None

    def __post_init__(self) -> None:
        canonical_worker_id(self.worker_id)
        if self.qualified_engine is not None and self.qualified_engine not in ENGINES:
            raise InvalidTarget("unsupported managed engine")

    def qualified(self, owner_engine: str) -> str:
        if owner_engine not in ENGINES:
            raise IdentityError("invalid CCX engine")
        if self.qualified_engine is not None and self.qualified_engine != owner_engine:
            raise EngineMismatch("managed target engine does not match CCX ownership")
        return f"{owner_engine}:{self.worker_id}"


def parse_managed_target(target: str) -> ManagedTarget:
    """Only a canonical full UUID or engine:full UUID; never a display alias."""
    if not isinstance(target, str):
        raise InvalidTarget("managed target must be a canonical full worker UUID")
    if ":" not in target:
        return ManagedTarget(canonical_worker_id(target))
    engine, separator, worker_id = target.partition(":")
    if separator != ":" or engine not in ENGINES:
        raise InvalidTarget("managed target must be a bare or engine-qualified full UUID")
    return ManagedTarget(canonical_worker_id(worker_id), engine)


@dataclass(frozen=True)
class WorkerOwner:
    worker_id: str
    engine: str
    state: str = "worker"

    def __post_init__(self) -> None:
        canonical_worker_id(self.worker_id)
        if self.engine not in ENGINES:
            raise IdentityError("unsupported CCX worker engine")
        if self.state not in OWNER_STATES:
            raise IdentityError("invalid CCX owner reservation state")

    @property
    def worker(self) -> str:
        return f"{self.engine}:{self.worker_id}"

    @property
    def short(self) -> str:
        """Nonunique display field, never a key for routing."""
        return self.worker_id[:8]
