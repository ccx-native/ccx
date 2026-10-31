"""Hermetic tests for engine-neutral bounded conversation paging."""

from __future__ import annotations

import json
from pathlib import Path

from ccx.ops import conversation_paging as P

def _write(path, events):
    path.write_bytes(
        b"".join(
            (json.dumps(event, separators=(",", ":")) + "\n").encode()
            for event in events
        )
    )
    return path


def _user(uid, text):
    return {
        "type": "user",
        "uuid": uid,
        "timestamp": "2026-09-27T10:00:00Z",
        "message": {"content": text},
    }


def _assistant(uid, mid, text):
    return {
        "type": "assistant",
        "uuid": uid,
        "timestamp": "2026-09-27T10:00:01Z",
        "message": {
            "id": mid,
            "model": "claude-haiku",
            "content": [{"type": "text", "text": text}],
        },
    }


def test_structural_anchor_rejects_late_assistant_boundary(tmp_path):
    path = tmp_path / "anchors.jsonl"
    _write(path, [
        _user("u0", "one"),
        _assistant("a0", "m0", "one"),
        _user("u1", "two"),
    ])
    events = list(P._iter_transcript(path))
    user = events[0]
    assistant = events[1]
    assert P._validate_anchor(
        str(path), user["_ccx_start_offset"], before_offset=path.stat().st_size
    )["source_record_id"] == "u0"
    assert P._validate_anchor(
        str(path), assistant["_ccx_start_offset"], before_offset=path.stat().st_size
    ) is None


def _public_bearing(records):
    out = []
    for record in records:
        if not record.get("_ccx_page_bearing"):
            continue
        public = P._public_record(record)
        if public is not None:
            out.append(public)
    return out


def test_every_safe_boundary_matches_full_normalization_at_partial_eofs():
    path = Path(__file__).parent / "fixtures" / "claude_conversation.jsonl"
    all_events = list(P._iter_transcript(path))
    safe = [
        event["_ccx_start_offset"]
        for event in all_events
        if P._ordinary_user_boundary(event)
    ]
    assert safe
    for boundary in safe:
        for event in all_events:
            eof = event["_ccx_end_offset"]
            if eof <= boundary:
                continue
            full, _ = P._normalize_claude_window(
                str(path),
                after_offset=0,
                before_offset=eof,
                max_tool_result_bytes=16_384,
            )
            bounded, _ = P._normalize_claude_window(
                str(path),
                after_offset=boundary,
                before_offset=eof,
                max_tool_result_bytes=16_384,
            )
            expected = [
                record
                for record in full
                if record.get("_ccx_page_bearing")
                and record.get("_ccx_start_offset", -1) >= boundary
            ]
            assert _public_bearing(bounded) == _public_bearing(expected), (
                boundary,
                eof,
            )








def test_page_tool_result_second_stage_clip_preserves_full_digest_metadata():
    counters = P.conv._Counters()
    full = "x" * (200 * 1024)
    result = P.conv._result_payload(full, False, 100 * 1024, counters)
    assert result["truncated"] is True
    full_digest = result["contentSha256"]
    record = P._public_record({
        "turn_id": "t",
        "source_record_id": "tool",
        "timestamp_ms": 1,
        "duration_ms": None,
        "usage": None,
        "items": [{
            "kind": "tool-call", "id": "tool", "name": "shell", "status": "completed",
            "result": result,
        }],
    })
    item = record["items"][0]
    assert len(item["result"]["content"].encode()) <= P._PAGE_FIELD_MAX_BYTES
    assert item["result"]["originalByteCount"] == len(full.encode())
    assert item["result"]["contentSha256"] == full_digest
    by_source = {entry["source"]: entry for entry in item["truncations"]}
    assert by_source["max_tool_result_bytes"]["returned_byte_count"] == 100 * 1024
    assert by_source["ccx-page"]["returned_byte_count"] <= P._PAGE_FIELD_MAX_BYTES
    assert by_source["ccx-page"]["sha256"] == full_digest


def test_protected_strings_and_usage_are_bounded_without_record_failure():
    huge = "q" * (5 * 1024 * 1024)
    counters = P.conv._Counters()
    record = P._public_record({
        "turn_id": "t",
        "source_record_id": "s",
        "timestamp_ms": 1,
        "duration_ms": None,
        "usage": {"provider_blob": huge},
        "items": [{
            "kind": "tool-call",
            "id": "tool",
            "nativeType": "dynamicToolCall",
            "name": huge,
            "status": "completed",
        }],
    }, counters)
    assert record is not None
    item = record["items"][0]
    assert len(item["name"].encode("utf-8")) <= P._PROTECTED_STRING_MAX_BYTES
    assert any(t["path"] == "name" for t in item["truncations"])
    assert any(t["path"] == "usage" for t in record["truncations"])
    assert P._encoded_size(record) <= P._PAGE_SINGLE_RECORD_MAX_BYTES




def test_record_item_budget_shrink_preserves_all_items_before_stub_degradation():
    counters = P.conv._Counters()
    items = [
        {
            "kind": "assistant-text",
            "id": f"i{i}",
            "name": "n" * P._PROTECTED_STRING_MAX_BYTES,
            "text": "x" * 10_000,
        }
        for i in range(500)
    ]
    record = P._public_record({
        "turn_id": "t",
        "source_record_id": "record",
        "timestamp_ms": 1,
        "duration_ms": None,
        "usage": None,
        "items": items,
    }, counters)
    assert record is not None
    assert len(record["items"]) == 500
    assert counters.omitted == 0
    assert P._encoded_size(record) <= P._PAGE_SINGLE_RECORD_MAX_BYTES


def test_minimal_result_omission_reuses_full_result_digest_and_reports_zero_bytes():
    counters = P.conv._Counters()
    full = "r" * (200 * 1024)
    result = P.conv._result_payload(full, False, 100 * 1024, counters)
    item = P._minimal_public_item({
        "kind": "tool-call",
        "id": "tool",
        "name": "shell",
        "status": "completed",
        "result": result,
    })
    by_source = {entry["source"]: entry for entry in item["truncations"]}
    assert by_source["max_tool_result_bytes"]["original_byte_count"] == len(full.encode())
    omitted = by_source["ccx-page"]
    assert omitted["original_byte_count"] == len(full.encode())
    assert omitted["sha256"] == result["contentSha256"]
    assert omitted["returned_byte_count"] == 0
    assert omitted["omitted"] is True




def test_nested_projection_metadata_names_omitted_paths_without_fabrication():
    item = P._project_item_once({
        "kind": "tool-call",
        "id": "tool",
        "arguments": {
            "rows": [{"id": 1727000000, "enabled": True, "label": "x" * 50}],
        },
    }, 24)
    truncation = next(t for t in item["truncations"] if t["path"] == "arguments")
    assert "arguments.rows[0].id" in truncation["paths"]
    assert "arguments.rows[0].enabled" in truncation["paths"]
    assert truncation["omitted_path_count"] >= 2
    assert item["arguments"] == {"rows": [{}]}










def test_action_summary_target_is_capped_in_shared_normalization():
    huge_command = "é" * 400 + "\nsecond line that must not enter the display summary"
    native_item = {
        "type": "commandExecution",
        "id": "command-summary",
        "command": huge_command,
        "cwd": "/synthetic",
        "aggregatedOutput": "ok",
        "exitCode": 0,
        "status": "completed",
    }
    full = P.conv._codex_item(
        native_item, P.conv._Counters(), None, "gpt-5.6-luna"
    )[0]
    assert full["arguments"]["command"] == huge_command
    assert full["summary"]["verb"] == "Ran"
    assert full["summary"]["target_truncated"] is True
    assert "\n" not in full["summary"]["target"]
    assert len(full["summary"]["target"].encode("utf-8")) <= 512



def test_summary_description_is_individually_protected_in_page_projection():
    huge_description = "é" * (P._PROTECTED_STRING_MAX_BYTES * 2)
    item = P._public_item({
        "kind": "tool-call",
        "id": "description-summary",
        "summary": {
            "verb": "Ran",
            "target": "python3 gate.py",
            "description": huge_description,
        },
    })

    assert item["summary"]["verb"] == "Ran"
    assert item["summary"]["target"] == "python3 gate.py"
    assert item["summary"]["description"] != huge_description
    assert len(item["summary"]["description"].encode("utf-8")) <= P._PROTECTED_STRING_MAX_BYTES
    truncation = next(t for t in item["truncations"] if t["path"] == "summary.description")
    assert truncation["original_byte_count"] > truncation["returned_byte_count"]
    assert not any(t["path"] == "summary" for t in item["truncations"])
