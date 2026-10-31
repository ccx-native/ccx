from __future__ import annotations

import hashlib
import json

from ccx.system.bounded import bound_value


def test_bounded_string_is_utf8_safe_and_reports_full_digest():
    value = "é" * 100
    bounded = bound_value(value, 25)
    assert bounded.truncated is True
    assert bounded.returned_byte_count <= 25
    bounded.value.encode("utf-8")
    assert bounded.original_byte_count == len(value.encode("utf-8"))
    assert len(bounded.sha256 or "") == 64


def test_bounded_nested_json_is_deterministic_and_keeps_json_types():
    value = {
        "path": "/synthetic/file.txt",
        "content": "x" * 10_000,
        "rows": [{"name": f"row-{i}", "value": "y" * 500} for i in range(100)],
    }
    first = bound_value(value, 2_048)
    second = bound_value(value, 2_048)
    assert first == second
    assert first.truncated is True
    assert isinstance(first.value, dict)
    assert isinstance(first.value.get("rows"), list)
    assert len(json.dumps(first.value, ensure_ascii=False, separators=(",", ":")).encode()) <= 2_048


def test_bounded_string_digest_is_of_full_utf8_value_not_preview():
    value = "é" * 100
    bounded = bound_value(value, 25)
    assert bounded.sha256 == hashlib.sha256(value.encode("utf-8")).hexdigest()
    assert bounded.sha256 != hashlib.sha256(bounded.value.encode("utf-8")).hexdigest()


def test_tight_nested_preview_omits_scalars_instead_of_fabricating_values():
    value = {
        "rows": [{"id": 1727000000, "enabled": True, "label": "x" * 50}],
    }
    bounded = bound_value(value, 24)
    assert bounded.truncated is True
    assert bounded.value == {"rows": [{}]}
    assert "rows[0].id" in bounded.changed_paths
    assert "rows[0].enabled" in bounded.changed_paths
    assert bounded.omitted_count >= 2
    assert "0" not in str(bounded.value)
    assert "false" not in json.dumps(bounded.value)


def test_structured_digest_uses_documented_canonical_json():
    value = {"z": [3, 2, 1], "a": {"text": "é" * 200}}
    bounded = bound_value(value, 64)
    canonical = json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    assert bounded.sha256 == hashlib.sha256(canonical).hexdigest()
    assert bounded.original_byte_count == len(canonical)


def test_tight_list_preview_omits_over_budget_bool_instead_of_preserving_it():
    bounded = bound_value([True, "xxxxxxxxxx"], 8)
    assert bounded.truncated is True
    assert bounded.value == []
    assert bounded.omitted_count == 2
    assert "[0:]" in bounded.changed_paths
