from __future__ import annotations

import time
from typing import Any


def now_ms() -> int:
    return int(time.time() * 1000)


def _uuid_codex_store():
    """ACTIVE full-ID bootstrap before any Codex record filesystem operation."""
    from ccx.constants import CCX_ROOT
    from ccx.store.root import require_ccx_active
    return require_ccx_active(CCX_ROOT)


def read_record(target: str) -> dict[str, Any]:
    """Read exact managed Codex full UUID, never short/name/thread alias."""
    from ccx.store.identity import EngineMismatch
    store = _uuid_codex_store()
    owner = store.resolve(target)
    if owner.engine != "codex":
        raise EngineMismatch("Codex record lookup requires Codex full worker UUID")
    return store.record(owner.worker)


def write_record(record: dict[str, Any]) -> None:
    """Require original exact whole-record revision, or refuse stale overwrite."""
    from ccx.store.root import RecordConflict, StorageIntegrityError
    if not isinstance(record, dict) or record.get("engine") != "codex":
        raise StorageIntegrityError("Codex full-UUID record must preserve engine")
    original_revision = record.get("recordRevision")
    if type(original_revision) is not int or original_revision < 0:
        raise RecordConflict(
            "Codex whole-record snapshot needs current original recordRevision"
        )
    store = _uuid_codex_store()
    updated = store.replace_record_if_revision(
        f"codex:{record.get('workerId')}", record,
        expected_revision=original_revision,
    )
    record.clear()
    record.update(updated)


def iter_records():
    """Only full-UUID Codex global owners, not old codex/sessions short path."""
    store = _uuid_codex_store()
    for row in store.list_workers(engine="codex"):
        yield store.record(row["worker"])
