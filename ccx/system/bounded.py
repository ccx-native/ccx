"""Deterministic UTF-8/JSON previews for bounded public projections."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from typing import Any


_DEFAULT_BREADTH = 64
_DEFAULT_DEPTH = 8
_MAX_PATH_SAMPLES = 32
_MAX_PATH_BYTES = 256
_OMIT = object()


@dataclass(frozen=True)
class BoundedValue:
    value: Any
    truncated: bool
    original_byte_count: int
    returned_byte_count: int
    sha256: str | None
    omitted: bool = False
    changed_paths: tuple[str, ...] = ()
    changed_path_count: int = 0
    omitted_count: int = 0


class _PreviewState:
    def __init__(self) -> None:
        self.paths: list[str] = []
        self.changed_path_count = 0
        self.omitted_count = 0

    def snapshot(self) -> tuple[int, int, int]:
        return (len(self.paths), self.changed_path_count, self.omitted_count)

    def restore(self, snapshot: tuple[int, int, int]) -> None:
        length, changed, omitted = snapshot
        del self.paths[length:]
        self.changed_path_count = changed
        self.omitted_count = omitted

    def add(self, path: str, *, omitted: bool = False, count: int = 1) -> None:
        count = max(1, int(count))
        self.changed_path_count += count
        if omitted:
            self.omitted_count += count
        if len(self.paths) < _MAX_PATH_SAMPLES:
            sample = _utf8_prefix(path or "$", _MAX_PATH_BYTES)
            if sample not in self.paths:
                self.paths.append(sample)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _value_bytes(value: Any) -> bytes:
    # String metadata is raw UTF-8. Structured metadata is canonical JSON.
    if isinstance(value, str):
        return value.encode("utf-8")
    return _canonical_json(value)


def _encoded_size(value: Any) -> int:
    return len(_canonical_json(value))


def _utf8_prefix(value: str, max_bytes: int) -> str:
    raw = value.encode("utf-8")
    if len(raw) <= max_bytes:
        return value
    return raw[:max(0, max_bytes)].decode("utf-8", "ignore")


def _json_string_prefix(value: str, max_encoded_bytes: int) -> str | object:
    if max_encoded_bytes < 2:  # even "" is two encoded bytes
        return _OMIT
    if _encoded_size(value) <= max_encoded_bytes:
        return value
    low, high = 0, len(value)
    while low < high:
        mid = (low + high + 1) // 2
        if _encoded_size(value[:mid]) <= max_encoded_bytes:
            low = mid
        else:
            high = mid - 1
    return value[:low]


def _dict_path(base: str, key: Any) -> str:
    key_text = str(key)
    if key_text.isidentifier():
        return f"{base}.{key_text}" if base else key_text
    encoded = json.dumps(key_text, ensure_ascii=False)
    return f"{base}[{encoded}]" if base else f"[{encoded}]"


def _list_path(base: str, index: int | str) -> str:
    return f"{base}[{index}]" if base else f"[{index}]"


def _preview_json(
    value: Any,
    max_bytes: int,
    *,
    state: _PreviewState,
    path: str = "",
    depth: int = 0,
    breadth: int = _DEFAULT_BREADTH,
    max_depth: int = _DEFAULT_DEPTH,
) -> Any:
    """Return a deterministic same-shape preview or ``_OMIT``.

    Scalars are never substituted. If a scalar cannot fit the allotted encoded
    JSON bytes, its containing key/list suffix is omitted and the path is
    reported through ``state``.
    """
    max_bytes = max(0, int(max_bytes))
    if _encoded_size(value) <= max_bytes:
        return copy.deepcopy(value)

    if isinstance(value, str):
        clipped = _json_string_prefix(value, max_bytes)
        if clipped is _OMIT:
            state.add(path, omitted=True)
            return _OMIT
        state.add(path)
        return clipped

    if isinstance(value, list):
        if max_bytes < 2:
            state.add(path, omitted=True, count=max(1, len(value)))
            return _OMIT
        if depth >= max_depth:
            if value:
                state.add(_list_path(path, "0:"), omitted=True, count=len(value))
            return []
        out: list[Any] = []
        entries = value[:breadth]
        for index, entry in enumerate(entries):
            remaining = max_bytes - _encoded_size(out) - 2
            if remaining <= 0:
                state.add(_list_path(path, f"{index}:"), omitted=True, count=len(value) - index)
                break
            remaining_slots = len(entries) - index
            share = max(1, remaining // max(1, remaining_slots))
            snapshot = state.snapshot()
            candidate_entry = _preview_json(
                entry,
                share,
                state=state,
                path=_list_path(path, index),
                depth=depth + 1,
                breadth=breadth,
                max_depth=max_depth,
            )
            if candidate_entry is _OMIT:
                state.restore(snapshot)
                state.add(_list_path(path, f"{index}:"), omitted=True, count=len(value) - index)
                break
            candidate = [*out, candidate_entry]
            if _encoded_size(candidate) > max_bytes:
                state.restore(snapshot)
                state.add(_list_path(path, f"{index}:"), omitted=True, count=len(value) - index)
                break
            out = candidate
        if len(value) > breadth and len(out) >= breadth:
            state.add(_list_path(path, f"{breadth}:"), omitted=True, count=len(value) - breadth)
        return out

    if isinstance(value, dict):
        if max_bytes < 2:
            state.add(path, omitted=True, count=max(1, len(value)))
            return _OMIT
        if depth >= max_depth:
            if value:
                state.add(f"{path}.*" if path else "*", omitted=True, count=len(value))
            return {}
        out: dict[Any, Any] = {}
        entries = list(value.items())[:breadth]
        for index, (key, entry) in enumerate(entries):
            child_path = _dict_path(path, key)
            key_probe = {key: None}
            key_bytes = _encoded_size(key_probe)
            remaining = max_bytes - _encoded_size(out) - 2
            if key_bytes >= remaining:
                state.add(child_path, omitted=True)
                continue
            remaining_slots = len(entries) - index
            share = max(1, remaining // max(1, remaining_slots) - key_bytes)
            snapshot = state.snapshot()
            candidate_entry = _preview_json(
                entry,
                share,
                state=state,
                path=child_path,
                depth=depth + 1,
                breadth=breadth,
                max_depth=max_depth,
            )
            if candidate_entry is _OMIT:
                # Child already recorded the exact omitted path.
                continue
            candidate = {**out, key: candidate_entry}
            if _encoded_size(candidate) > max_bytes:
                state.restore(snapshot)
                state.add(child_path, omitted=True)
                continue
            out = candidate
        if len(value) > breadth:
            state.add(f"{path}.*" if path else "*", omitted=True, count=len(value) - breadth)
        return out

    # JSON scalars are either represented exactly or omitted. Substituting a
    # false/zero value would fabricate provider data.
    state.add(path, omitted=True)
    return _OMIT


def omit_value(value: Any) -> BoundedValue:
    """Describe a field deliberately omitted from a bounded projection."""
    original = _value_bytes(value)
    return BoundedValue(
        None,
        True,
        len(original),
        0,
        hashlib.sha256(original).hexdigest(),
        omitted=True,
        changed_paths=(),
        changed_path_count=1,
        omitted_count=1,
    )


def bound_value(value: Any, max_bytes: int | None) -> BoundedValue:
    """Bound one public value and return explicit full-value metadata.

    Strings use raw UTF-8 byte counts/digests. Structured values use canonical
    JSON: UTF-8, sorted keys, compact `,`/`:` separators, `ensure_ascii=False`,
    and `allow_nan=False`.
    """
    original = _value_bytes(value)
    if max_bytes is None or len(original) <= max_bytes:
        return BoundedValue(copy.deepcopy(value), False, len(original), len(original), None)

    if isinstance(value, str):
        projected = _utf8_prefix(value, max(0, max_bytes))
        returned = _value_bytes(projected)
        return BoundedValue(
            projected,
            True,
            len(original),
            len(returned),
            hashlib.sha256(original).hexdigest(),
        )

    state = _PreviewState()
    projected = _preview_json(value, max(0, max_bytes), state=state)
    if projected is _OMIT:
        return BoundedValue(
            None,
            True,
            len(original),
            0,
            hashlib.sha256(original).hexdigest(),
            omitted=True,
            changed_paths=tuple(state.paths),
            changed_path_count=state.changed_path_count,
            omitted_count=max(1, state.omitted_count),
        )
    returned = _value_bytes(projected)
    return BoundedValue(
        projected,
        True,
        len(original),
        len(returned),
        hashlib.sha256(original).hexdigest(),
        changed_paths=tuple(state.paths),
        changed_path_count=state.changed_path_count,
        omitted_count=state.omitted_count,
    )


__all__ = ["BoundedValue", "bound_value", "omit_value"]
