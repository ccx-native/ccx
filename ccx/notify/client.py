"""Public observation and cancellation of durable parent continuations.

The notification supervisor owns persistence and delivery. This module exposes a
small supported surface so dashboards and orchestrators can inspect armed
registrations and cancel one explicitly without importing parent_notify internals
or reading its state files.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from ccx.errors import InvalidRequest
from ccx.notify import registry as parent_notify


def _event_id(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise InvalidRequest("notification event_id must be a canonical UUID")
    try:
        if str(UUID(value)) != value:
            raise ValueError("noncanonical event id")
    except (ValueError, AttributeError) as error:
        raise InvalidRequest("notification event_id must be a canonical UUID") from error
    return value


class ParentNotificationClient:
    """Inspect durable parent notifications and explicitly cancel one."""

    @staticmethod
    def _public(value: dict[str, Any]) -> dict[str, Any]:
        return parent_notify.public_registration(value)

    def get(self, event_id: str) -> dict[str, Any] | None:
        event_id = _event_id(event_id)
        value = parent_notify.read_registration(event_id)
        if not isinstance(value, dict):
            return None
        return self._public(value)

    def cancel(self, event_id: str, *, reason: str = "cancelled by operator") -> dict[str, Any] | None:
        event_id = _event_id(event_id)
        if not isinstance(reason, str) or not reason.strip():
            raise InvalidRequest("notification cancel reason must be a non-empty string")
        value = parent_notify.read_registration(event_id)
        if not isinstance(value, dict):
            return None
        parent_notify.cancel_registration(event_id, reason.strip())
        updated = parent_notify.read_registration(event_id)
        return self._public(updated) if isinstance(updated, dict) else None

    def list(
        self,
        *,
        parent: str | None = None,
        worker: str | None = None,
        source: str | None = None,
        state: str | None = None,
        include_terminal: bool = False,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit < 1
        ):
            raise InvalidRequest("notification limit must be a positive integer")

        from ccx.store.identity import canonical_worker_id, IdentityError
        filters = []
        for candidate, kinds in ((parent, {"codex"}),
                                 (worker, {"codex", "claude", "controller"})):
            if candidate is None:
                filters.append(None)
                continue
            try:
                kind, separator, identity = candidate.partition(":")
                if separator:
                    if kind not in kinds:
                        raise ValueError("unsupported notification subject kind")
                else:
                    identity, kind = candidate, None
                filters.append((kind, canonical_worker_id(identity)))
            except (IdentityError, ValueError, AttributeError) as error:
                raise InvalidRequest("notification filter needs FULL_UUID or KIND:FULL_UUID with a supported kind") from error
        parent_filter, worker_filter = filters
        rows: list[dict[str, Any]] = []
        for value in parent_notify.iter_registrations(
            include_terminal=include_terminal
        ):
            row = self._public(value)
            if any(
                selected is not None and (
                    not isinstance(row.get(field), str)
                    or row[field].split(":", 1)[-1] != selected[1]
                    or (selected[0] is not None and row[field] != f"{selected[0]}:{selected[1]}")
                )
                for field, selected in (("parent", parent_filter), ("worker", worker_filter))
            ):
                continue
            if source is not None and value.get("source") != source:
                continue
            if state is not None and row.get("state") != state:
                continue
            rows.append(row)

        rows.sort(
            key=lambda row: (
                int(row.get("created_at_ms") or 0),
                str(row.get("event_id") or ""),
            ),
            reverse=True,
        )
        if limit is not None:
            rows = rows[:limit]
        return {"count": len(rows), "notifications": rows}
