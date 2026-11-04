"""Deterministic tests for the normalized conversation contract.

No sockets, no hosts, no engines: both converters are pure functions of a
record plus a native source, so everything here runs off the two fixtures in
``tests/fixtures`` (plus payloads synthesized in-test, which are too big to
commit).
"""

from __future__ import annotations

import hashlib
import json
import pathlib

import pytest

from ccx.ops import conversation as conv
from ccx.errors import InvalidRequest, SessionNotFound

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
CLAUDE_JSONL = FIXTURES / "claude_conversation.jsonl"
CODEX_THREAD = FIXTURES / "codex_thread_read.json"


def claude_record(transcript, **over):
    rec = {
        "short": "966a8a1b",
        "sessionId": "sess-1",
        "cwd": "/workspace",
        "transcript": str(transcript),
        "createArgs": {"model": "opus"},
        "stats": {"turns": 4},
    }
    rec.update(over)
    return rec


def codex_record(**over):
    rec = {
        "short": "c0de1234",
        "threadId": "th_fixture_1",
        "cwd": "/workspace",
        "createArgs": {"model": "gpt-5.6-sol"},
        "resolvedModel": "gpt-5.6-sol-2026",
        "stats": {"turns": 3},
    }
    rec.update(over)
    return rec


def build_claude(transcript=CLAUDE_JSONL, **kwargs):
    return conv.build_claude(
        claude_record(transcript), state="idle", worker="claude:966a8a1b", **kwargs
    )


def build_codex(thread=None, **kwargs):
    if thread is None:
        thread = json.loads(CODEX_THREAD.read_text())
    return conv.build_codex(
        codex_record(), thread, state="idle", worker="codex:c0de1234", **kwargs
    )


def all_items(doc):
    for turn in doc["turns"]:
        yield from turn["input"]
        yield from turn["output"]


# --- shared schema -----------------------------------------------------------

def test_both_engines_produce_the_same_document_keys():
    a, b = build_claude(), build_codex()
    assert set(a) == set(b)
    assert a["schemaVersion"] == b["schemaVersion"] == conv.SCHEMA_VERSION == 5
    assert set(a["worker"]) == set(b["worker"])
    assert set(a["completeness"]) == set(b["completeness"])
    assert set(a["stats"]) == set(b["stats"])
    for doc in (a, b):
        for turn in doc["turns"]:
            assert set(turn) == {
                "turnId", "sequence", "status", "startedAt", "completedAt",
                "input", "output", "usage",
            }
            assert set(turn["usage"]) == {
                "inputTokens", "outputTokens", "cachedInputTokens",
                "cacheCreationTokens", "reasoningTokens",
            }
            for item in turn["input"] + turn["output"]:
                assert {"kind", "id", "ordinal", "nativeType"} <= set(item)


def test_result_payloads_always_carry_every_key():
    for doc in (build_claude(), build_codex()):
        for item in all_items(doc):
            if item["kind"] in ("tool-call", "tool-result"):
                assert set(item["result"]) == {
                    "content", "isError", "truncated", "originalByteCount",
                    "returnedByteCount", "contentSha256",
                }


def test_turn_sequence_is_dense_and_ordered():
    for doc in (build_claude(), build_codex()):
        assert [t["sequence"] for t in doc["turns"]] == list(
            range(1, len(doc["turns"]) + 1)
        )


# --- Claude conversion -------------------------------------------------------

def test_claude_turn_segmentation_and_ordering():
    doc = build_claude()
    kinds = [[i["kind"] for i in t["input"]] for t in doc["turns"]]
    assert kinds[0] == ["user-text"]
    # A prompt delivered mid-turn as a queued_command attachment opens a turn.
    assert kinds[1] == ["user-text"]
    assert doc["turns"][1]["input"][0]["nativeType"] == "queued_command"
    assert kinds[2] == ["command"]
    assert kinds[3] == ["user-text", "attachment"]
    first = doc["turns"][0]
    assert [i["kind"] for i in first["output"]] == [
        "assistant-text", "tool-call", "assistant-text",
    ]
    assert first["startedAt"] == "2026-07-27T09:00:00Z"
    assert first["completedAt"] == "2026-07-27T09:00:03Z"


def test_claude_command_item_carries_name_and_args():
    command = build_claude()["turns"][2]["input"][0]
    assert command["name"] == "/compact"
    assert command["arguments"] == {"args": "keep the tests"}


def test_claude_tool_call_result_pairing():
    call = build_claude()["turns"][0]["output"][1]
    assert call["kind"] == "tool-call"
    assert call["callId"] == "toolu_1"
    assert call["name"] == "Bash"
    assert call["summary"] == {"verb": "Ran", "target": "echo hi"}
    assert call["arguments"] == {"command": "echo hi"}
    assert call["status"] == "completed"
    assert call["result"]["content"] == "hi\n"
    assert call["result"]["isError"] is False
    # Paired results are not also emitted standalone.
    assert not [
        i for t in build_claude()["turns"] for i in t["output"]
        if i["kind"] == "tool-result" and i["callId"] == "toolu_1"
    ]


def test_claude_unmatched_tool_result_is_preserved_standalone():
    doc = build_claude()
    orphans = [i for i in all_items(doc) if i["kind"] == "tool-result"]
    assert [o["callId"] for o in orphans] == ["toolu_orphan"]
    assert orphans[0]["result"]["content"] == "orphaned result"
    assert any("toolu_orphan" in w for w in doc["completeness"]["warnings"])


def test_claude_queued_command_with_image_is_a_block_list(tmp_path):
    # A message queued mid-turn that carries an image is persisted with
    # `prompt` as a content-block list, not a string.
    lines = CLAUDE_JSONL.read_text().splitlines()
    queued = json.dumps({
        "type": "attachment", "uuid": "atimg",
        "timestamp": "2026-07-27T09:00:12.000Z",
        "attachment": {"type": "queued_command", "prompt": [
            {"type": "text", "text": "[Image #1] look at this"},
            {"type": "image", "source": {
                "type": "base64", "media_type": "image/png", "data": "iVBORw0KGgo=",
            }},
            {"type": "future_block", "value": 1},
        ]},
    })
    src = tmp_path / "queued-image.jsonl"
    src.write_text("\n".join([*lines, queued]) + "\n")
    doc = build_claude(src)
    last = doc["turns"][-1]["input"]
    assert [i["kind"] for i in last] == ["user-text", "attachment"]
    assert last[0]["nativeType"] == "queued_command"
    assert last[0]["text"] == "[Image #1] look at this"
    assert last[1]["mediaType"] == "image/png"
    assert "data" not in last[1]["native"]["source"]
    # An unknown block is kept, as in a user message, not dropped.
    other = doc["turns"][-1]["output"]
    assert [(i["kind"], i["nativeType"]) for i in other] == [("other", "future_block")]
    assert doc["completeness"]["omittedItemCount"] == 0


def test_claude_duplicate_tool_result_is_preserved_not_dropped(tmp_path):
    lines = CLAUDE_JSONL.read_text().splitlines()
    duplicate = json.dumps({
        "type": "user", "uuid": "dup",
        "timestamp": "2026-07-27T09:00:12.000Z",
        "message": {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "toolu_1", "content": "again"},
        ]},
    })
    src = tmp_path / "dup.jsonl"
    src.write_text("\n".join([*lines, duplicate]) + "\n")
    doc = build_claude(src)
    # The first result wins the pairing...
    assert doc["turns"][0]["output"][1]["result"]["content"] == "hi\n"
    # ...and the second survives as a standalone item rather than vanishing.
    extras = [i for i in all_items(doc)
              if i["kind"] == "tool-result" and i["callId"] == "toolu_1"]
    assert [e["result"]["content"] for e in extras] == ["again"]
    assert extras[0]["unpaired"] is True
    assert any("duplicate" in w for w in doc["completeness"]["warnings"])


def test_claude_error_is_not_flattened_into_text():
    doc = build_claude()
    errors = [i for i in all_items(doc) if i["kind"] == "error"]
    assert len(errors) == 1
    assert errors[0]["message"] == "API Error: overloaded"
    assert errors[0]["data"] == {"status": 529, "requestId": "req_1"}
    assert doc["turns"][3]["status"] == "failed"


def test_claude_compact_boundary_is_a_system_item():
    system = [i for i in all_items(build_claude()) if i["kind"] == "system-text"]
    assert len(system) == 1
    assert system[0]["text"] == "conversation summary"
    assert system[0]["data"] == {
        "preTokens": 1000, "postTokens": 200, "durationMs": 50,
        "trigger": "manual",
    }


def test_claude_attachment_metadata_survives_without_inline_bytes():
    attachment = [i for i in all_items(build_claude()) if i["kind"] == "attachment"][0]
    assert attachment["mediaType"] == "image/png"
    assert "data" not in attachment["native"]["source"]
    assert attachment["native"]["source"]["type"] == "base64"


def test_claude_unknown_event_types_are_preserved_as_other():
    others = [i for i in all_items(build_claude()) if i["kind"] == "other"]
    assert "queue-operation" in [o["nativeType"] for o in others]


def test_claude_usage_is_mapped_and_unknowns_stay_null():
    usage = build_claude()["turns"][0]["usage"]
    assert usage == {
        "inputTokens": 10, "outputTokens": 5, "cachedInputTokens": 2,
        "cacheCreationTokens": 3, "reasoningTokens": None,
    }
    assert build_claude()["turns"][2]["usage"]["inputTokens"] is None


def test_claude_observed_model_is_never_fabricated(tmp_path):
    doc = build_claude()
    assert doc["worker"]["requestedModel"] == "opus"
    assert doc["worker"]["observedModel"] == "claude-opus-5"

    src = tmp_path / "nomodel.jsonl"
    src.write_text(json.dumps({
        "type": "assistant", "uuid": "x",
        "message": {"id": "m", "role": "assistant",
                    "content": [{"type": "text", "text": "hi"}]},
    }) + "\n")
    assert build_claude(src)["worker"]["observedModel"] is None


def test_claude_synthetic_model_is_not_an_observed_model(tmp_path):
    src = tmp_path / "synthetic.jsonl"
    src.write_text(json.dumps({
        "type": "assistant", "uuid": "x", "isApiErrorMessage": True,
        "message": {"id": "m", "role": "assistant", "model": "<synthetic>",
                    "content": "API Error"},
    }) + "\n")
    assert build_claude(src)["worker"]["observedModel"] is None


def test_claude_missing_transcript_raises_session_not_found(tmp_path):
    with pytest.raises(SessionNotFound):
        build_claude(tmp_path / "absent.jsonl")


def test_claude_malformed_line_makes_completeness_partial(tmp_path):
    src = tmp_path / "broken.jsonl"
    src.write_text(CLAUDE_JSONL.read_text() + "{not json\n")
    doc = build_claude(src)
    assert doc["stats"]["malformedNativeRecords"] == 1
    assert doc["completeness"]["status"] == "partial"


def test_claude_referenced_artifact_when_not_exported():
    artifact = build_claude()["nativeArtifacts"][0]
    assert artifact["kind"] == "claude-transcript-jsonl"
    assert artifact["availability"] == "referenced"
    assert artifact["byteCount"] == CLAUDE_JSONL.stat().st_size
    assert artifact["sha256"] is None


# --- hidden reasoning --------------------------------------------------------

def test_claude_thinking_is_excluded_and_counted():
    doc = build_claude()
    assert doc["stats"]["excludedReasoningBlocks"] == 1
    blob = json.dumps(doc)
    assert "private chain of thought" not in blob
    assert not [i for i in all_items(doc) if i["kind"] == "reasoning-summary"]


def test_claude_narration_thinking_renders_as_visible_text(tmp_path):
    import base64

    def sig(tag: bytes) -> str:
        return base64.b64encode(
            b"\x08\x04\x12\xd8\x06\n\x11\x08\x12\x18\x028\x01B"
            + bytes([len(tag)]) + tag + b"\x12\x0c" + b"\x00" * 40
        ).decode()

    src = tmp_path / "narration.jsonl"
    src.write_text("\n".join(json.dumps({
        "type": "assistant", "uuid": uuid,
        "message": {"id": "m", "role": "assistant", "model": "claude-opus-5",
                    "content": [block]},
    }) for uuid, block in [
        ("n1", {"type": "thinking", "thinking": "Status: checking the tests",
                "signature": sig(b"narration")}),
        ("t1", {"type": "thinking", "thinking": "SECRET REASONING",
                "signature": sig(b"thinking")}),
        ("t2", {"type": "thinking", "thinking": "", "signature": sig(b"narration")}),
    ]) + "\n")
    doc = build_claude(src)
    texts = [i for i in all_items(doc) if i["kind"] == "assistant-text"]
    assert [(i["text"], i.get("narration")) for i in texts] == [
        ("Status: checking the tests", True),
    ]
    assert "SECRET REASONING" not in json.dumps(doc)
    assert doc["stats"]["excludedReasoningBlocks"] == 1


def test_codex_reasoning_keeps_only_the_engine_summary():
    doc = build_codex()
    blob = json.dumps(doc)
    assert "HIDDEN CHAIN OF THOUGHT" not in blob
    assert "HIDDEN FUTURE THOUGHT" not in blob
    summaries = [i for i in all_items(doc) if i["kind"] == "reasoning-summary"]
    assert [s["text"] for s in summaries] == ["considered the options"]
    assert doc["stats"]["excludedReasoningBlocks"] == 1


def test_codex_future_reasoning_variant_falls_through_to_other_without_content():
    doc = build_codex()
    future = [i for i in all_items(doc) if i["nativeType"] == "reasoningV2"]
    assert len(future) == 1
    assert future[0]["kind"] == "other"
    assert "content" not in future[0]["native"]
    assert future[0]["native"]["summary"] == ["future variant"]


# --- Codex conversion --------------------------------------------------------

def test_codex_item_mapping():
    doc = build_codex()
    first = doc["turns"][0]
    assert [i["kind"] for i in first["input"]] == ["user-text", "attachment"]
    assert first["input"][1]["locator"] == "/tmp/shot.png"
    assert [i["kind"] for i in first["output"]] == [
        "reasoning-summary", "assistant-text", "tool-call", "file-change",
        "tool-call", "tool-call", "system-text", "assistant-plan", "other",
    ]
    shell = first["output"][2]
    assert shell["name"] == "shell"
    assert shell["arguments"] == {"command": "echo hi", "cwd": "/workspace"}
    assert shell["result"]["content"] == "hi\n"
    assert first["output"][3]["changes"] == [
        {"path": "/workspace/a.py", "kind": "update", "diff": "@@ -1 +1 @@"},
    ]
    assert first["output"][4]["name"] == "srv/do"
    assert first["output"][5]["name"] == "web_search"
    # Codex stamps turns in unix SECONDS; the document normalizes to ISO-8601 Z.
    assert first["startedAt"] == "2026-05-28T20:26:40Z"
    assert first["completedAt"] == "2026-05-28T20:27:40Z"


def test_codex_reference_thread_items_have_deliberate_normalized_kinds():
    items = [
        {"type": "userMessage", "id": "u", "content": [
            {"type": "text", "text": "prompt"},
        ]},
        {"type": "hookPrompt", "id": "h", "fragments": [
            {"hookRunId": "hook-1", "text": "hook context"},
        ]},
        {"type": "agentMessage", "id": "a", "text": "answer"},
        {"type": "plan", "id": "p", "text": "steps"},
        {"type": "reasoning", "id": "r", "summary": ["summary"]},
        {"type": "commandExecution", "id": "c", "command": "echo hi",
         "commandActions": [], "cwd": "/workspace", "status": "completed"},
        {"type": "fileChange", "id": "f", "changes": [],
         "status": "completed"},
        {"type": "mcpToolCall", "id": "m", "arguments": {},
         "server": "srv", "status": "completed", "tool": "do"},
        {"type": "dynamicToolCall", "id": "d", "arguments": {},
         "status": "completed", "tool": "lookup"},
        {"type": "collabAgentToolCall", "id": "ca", "agentsStates": {},
         "receiverThreadIds": [], "senderThreadId": "parent",
         "status": "completed", "tool": "wait"},
        {"type": "subAgentActivity", "id": "sa", "agentPath": "worker",
         "agentThreadId": "child", "kind": "started"},
        {"type": "webSearch", "id": "w", "query": "ccx"},
        {"type": "imageView", "id": "iv", "path": "/tmp/view.png"},
        {"type": "sleep", "id": "sl", "durationMs": 10},
        {"type": "imageGeneration", "id": "ig", "result": "image-ref",
         "status": "completed"},
        {"type": "enteredReviewMode", "id": "er", "review": "review"},
        {"type": "exitedReviewMode", "id": "xr", "review": "review"},
        {"type": "contextCompaction", "id": "cc"},
    ]
    expected = {
        "u": "user-text", "h": "system-text", "a": "assistant-text",
        "p": "assistant-plan", "r": "reasoning-summary", "c": "tool-call",
        "f": "file-change", "m": "tool-call", "d": "tool-call",
        "ca": "subagent-action", "sa": "subagent-activity",
        "w": "tool-call", "iv": "attachment", "sl": "action",
        "ig": "action", "er": "system-text", "xr": "system-text",
        "cc": "system-text",
    }
    doc = build_codex(_codex_turn(items))
    normalized = {item["id"]: item for item in all_items(doc)}

    assert {item["nativeType"] for item in normalized.values()} == {
        item["type"] for item in items
    }
    assert {item_id: normalized[item_id]["kind"]
            for item_id in expected} == expected
    assert sorted(item["ordinal"] for item in normalized.values()) == list(
        range(len(items))
    )
    assert not any(
        "unmapped Codex thread item" in warning
        for warning in doc["completeness"]["warnings"]
    )
    assert normalized["h"]["data"]["fragments"][0]["hookRunId"] == "hook-1"
    assert normalized["p"]["text"] == "steps"
    assert normalized["sa"]["data"] == {
        "agentPath": "worker", "agentThreadId": "child", "kind": "started",
    }
    assert normalized["iv"]["locator"] == "/tmp/view.png"
    assert normalized["sl"]["data"] == {"durationMs": 10}
    assert normalized["er"]["data"]["transition"] == "entered"
    assert normalized["xr"]["data"]["transition"] == "exited"


def test_codex_command_execution_preserves_exit_and_execution_metadata():
    common = {
        "type": "commandExecution", "command": "run", "cwd": "/workspace",
        "status": "failed", "durationMs": 341, "source": "agent",
        "processId": "pty-1", "pluginId": "plugin-x",
        "scriptPath": "bin/run", "commandActions": [{"type": "search"}],
        "aggregatedOutput": "failed",
    }
    docs = []
    for exit_code in (127, 1, 0):
        item = {**common, "id": f"command-{exit_code}", "exitCode": exit_code,
                "status": "completed" if exit_code == 0 else "failed"}
        docs.append(build_codex(_codex_turn([item])))

    calls = [doc["turns"][0]["output"][0] for doc in docs]
    assert [call["data"]["exitCode"] for call in calls] == [127, 1, 0]
    assert [call["result"]["isError"] for call in calls] == [True, True, False]
    assert calls[0]["summary"] == {"verb": "Ran", "target": "run"}
    assert calls[0]["data"] == {
        "exitCode": 127, "durationMs": 341, "source": "agent",
        "processId": "pty-1", "pluginId": "plugin-x", "scriptPath": "bin/run",
        "commandActions": [{"type": "search"}],
    }
    no_exit = build_codex(_codex_turn([{
        "type": "commandExecution", "id": "no-exit", "command": "run",
        "cwd": "/workspace", "status": "declined", "commandActions": [],
    }]))["turns"][0]["output"][0]
    assert no_exit["status"] == "declined"
    assert "exitCode" not in no_exit.get("data", {})


def test_codex_dynamic_mcp_and_web_items_keep_native_metadata():
    items = [
        {"type": "dynamicToolCall", "id": "dynamic", "namespace": "ns",
         "tool": "lookup", "arguments": {"q": "x"}, "status": "completed",
         "success": True, "durationMs": 12,
         "contentItems": [{"type": "inputText", "text": "found"}]},
        {"type": "mcpToolCall", "id": "mcp", "server": "srv", "tool": "do",
         "arguments": {"a": 1}, "status": "failed", "durationMs": 23,
         "appContext": {"connectorId": "con", "appName": "app"},
         "pluginId": "plugin-y", "readOnlyHint": True,
         "error": {"message": "nope"}},
        {"type": "webSearch", "id": "web", "query": "ccx",
         "action": {"type": "openPage", "url": "https://example.test"},
         "results": [{"title": "CCX", "url": "https://example.test"}]},
    ]
    output = build_codex(_codex_turn(items))["turns"][0]["output"]
    dynamic, mcp, web = output
    assert dynamic["name"] == "ns/lookup"
    assert dynamic["summary"] == {"verb": "Called", "target": "ns/lookup"}
    assert dynamic["arguments"] == {"q": "x"}
    assert dynamic["data"] == {
        "namespace": "ns", "success": True, "durationMs": 12,
    }
    assert "found" in dynamic["result"]["content"]
    assert mcp["summary"] == {"verb": "Called", "target": "srv/do"}
    assert mcp["data"] == {
        "appContext": {"connectorId": "con", "appName": "app"},
        "durationMs": 23, "pluginId": "plugin-y", "readOnlyHint": True,
    }
    assert mcp["result"]["isError"] is True
    assert web["summary"] == {"verb": "Searched", "target": "ccx"}
    assert "CCX" in web["result"]["content"]
    assert "data" not in web
    assert web["arguments"]["action"]["type"] == "openPage"


def test_codex_image_generation_does_not_persist_generated_payload():
    sentinel = "cG5n_SCOPE10_IMAGE_PAYLOAD_SENTINEL"
    doc = build_codex(_codex_turn([{
        "type": "imageGeneration", "id": "image", "result": sentinel,
        "status": "completed", "revisedPrompt": "a safe prompt",
        "savedPath": "/tmp/generated.png", "transparentBackground": True,
    }]))
    item = doc["turns"][0]["output"][0]

    assert sentinel not in json.dumps(doc)
    assert item["status"] == "completed"
    assert item["data"] == {
        "revisedPrompt": "a safe prompt", "savedPath": "/tmp/generated.png",
        "transparentBackground": True,
    }


def test_codex_tool_outputs_are_bounded_without_data_duplicates():
    cases = [
        (
            "dynamicToolCall", "DYNAMIC_SCOPE10_OUTPUT_SENTINEL",
            {"type": "dynamicToolCall", "id": "dynamic", "namespace": "ns",
             "tool": "lookup", "arguments": {}, "status": "completed",
             "success": True, "durationMs": 4,
             "contentItems": [{"type": "inputText",
                               "text": "DYNAMIC_SCOPE10_OUTPUT_SENTINEL"}]},
        ),
        (
            "mcpToolCall", "MCP_SCOPE10_OUTPUT_SENTINEL",
            {"type": "mcpToolCall", "id": "mcp", "server": "srv",
             "tool": "lookup", "arguments": {}, "status": "completed",
             "result": {"message": "MCP_SCOPE10_OUTPUT_SENTINEL"},
             "durationMs": 5, "readOnlyHint": True},
        ),
        (
            "webSearch", "WEB_SCOPE10_OUTPUT_SENTINEL",
            {"type": "webSearch", "id": "web", "query": "ccx",
             "action": {"type": "search"},
             "results": [{"title": "WEB_SCOPE10_OUTPUT_SENTINEL"}]},
        ),
    ]

    for item_type, sentinel, native in cases:
        source = (
            native.get("contentItems") if item_type == "dynamicToolCall"
            else native.get("result") if item_type == "mcpToolCall"
            else native.get("results")
        )
        expected = conv._format_tool_result_content(source)
        expected_bytes = expected.encode("utf-8")
        expected_hash = hashlib.sha256(expected_bytes).hexdigest()

        zero = build_codex(
            _codex_turn([native]), max_tool_result_bytes=0,
        )["turns"][0]["output"][0]
        small = build_codex(
            _codex_turn([native]), max_tool_result_bytes=7,
        )["turns"][0]["output"][0]
        full = build_codex(_codex_turn([native]))["turns"][0]["output"][0]

        for bounded in (zero, small):
            result = bounded["result"]
            assert result["truncated"] is True
            assert result["originalByteCount"] == len(expected_bytes)
            assert result["returnedByteCount"] == len(
                result["content"].encode("utf-8")
            )
            assert result["returnedByteCount"] <= 7
            assert result["contentSha256"] == expected_hash
            assert sentinel not in json.dumps(
                {key: value for key, value in bounded.items() if key != "result"}
            )

        assert zero["result"]["content"] == ""
        assert sentinel not in json.dumps(zero)
        assert sentinel not in json.dumps(small)
        assert full["result"]["truncated"] is False
        assert full["result"]["content"] == expected
        assert sentinel in full["result"]["content"]

        if item_type == "dynamicToolCall":
            assert set(zero["data"]) == {"namespace", "success", "durationMs"}
            assert "contentItems" not in zero["data"]
        elif item_type == "mcpToolCall":
            assert set(zero.get("data", {})) == {"durationMs", "readOnlyHint"}
            assert "result" not in zero.get("data", {})
            assert "error" not in zero.get("data", {})
        else:
            assert "data" not in zero


def test_codex_agent_metadata_preserves_phase_and_memory_citation():
    thread = {
        "id": "thread-metadata",
        "turns": [{
            "id": "turn-metadata",
            "status": "completed",
            "startedAt": 1780000000,
            "completedAt": 1780000001,
            "items": [
                {"type": "agentMessage", "id": "item-commentary",
                 "text": "progress", "phase": "commentary",
                 "memoryCitation": None},
                {"type": "agentMessage", "id": "item-final",
                 "text": "answer", "phase": "final_answer",
                 "memoryCitation": {"id": "memory-1", "quote": "answer"}},
                {"type": "reasoning", "id": "item-reasoning",
                 "summary": ["visible summary"],
                 "content": ["HIDDEN CHAIN OF THOUGHT"]},
            ],
        }],
    }

    doc = build_codex(thread)
    messages = [
        item for item in all_items(doc) if item["kind"] == "assistant-text"
    ]
    assert [(item["id"], item["phase"]) for item in messages] == [
        ("item-commentary", "commentary"),
        ("item-final", "final_answer"),
    ]
    assert messages[1]["memoryCitation"] == {
        "id": "memory-1", "quote": "answer"
    }
    assert "HIDDEN CHAIN OF THOUGHT" not in json.dumps(doc)


def test_codex_turn_status_and_error_are_preserved():
    doc = build_codex()
    assert [t["status"] for t in doc["turns"]] == [
        "completed", "completed", "failed",
    ]
    errors = [i for i in doc["turns"][2]["output"] if i["kind"] == "error"]
    assert errors and errors[0]["message"] == "boom"


def test_codex_source_limitations_are_visible():
    doc = build_codex()
    completeness = doc["completeness"]
    assert completeness["sourceLimited"] is True
    assert completeness["status"] == "source-limited"
    warnings = " ".join(completeness["warnings"])
    assert "itemsView" in warnings
    assert "aggregatedOutput" not in warnings
    # An unknown item count is stated, never invented.
    assert completeness["omittedItemCount"] == 0


def test_codex_silent_completed_command_is_complete():
    doc = build_codex(_codex_turn([{
        "type": "commandExecution", "id": "silent", "command": "true",
        "cwd": "/workspace", "status": "completed",
        "aggregatedOutput": None, "exitCode": 0, "commandActions": [],
    }]))
    completeness = doc["completeness"]
    command = doc["turns"][0]["output"][0]

    assert completeness["sourceLimited"] is False
    assert completeness["status"] == "complete"
    assert not any("aggregatedOutput" in warning
                   for warning in completeness["warnings"])
    assert command["result"]["content"] == ""
    assert command["result"]["isError"] is False
    assert command["status"] == "completed"
    assert command["data"]["exitCode"] == 0


@pytest.mark.parametrize("native", [
    {"type": "webSearch", "id": "web-started", "query": "",
     "action": None, "results": None},
    {"type": "webSearch", "id": "web-completed", "query": "ccx",
     "action": {"type": "search"}, "results": [{"title": "CCX"}]},
])
def test_codex_web_search_does_not_fabricate_completion_status(native):
    item = build_codex(_codex_turn([native]))["turns"][0]["output"][0]

    assert item["status"] is None


def test_codex_empty_turns_with_recorded_turns_is_source_limited():
    doc = build_codex({"id": "th", "turns": []})
    assert doc["completeness"]["status"] == "source-limited"
    assert any("no turns" in w for w in doc["completeness"]["warnings"])


def test_codex_missing_thread_raises_session_not_found():
    with pytest.raises(SessionNotFound):
        conv.build_codex(codex_record(), None, state="idle",
                         worker="codex:c0de1234")


def test_codex_observed_model_comes_from_the_host_resolution():
    assert build_codex()["worker"]["observedModel"] == "gpt-5.6-sol-2026"
    doc = conv.build_codex(
        {**codex_record(), "resolvedModel": None},
        json.loads(CODEX_THREAD.read_text()),
        state="idle", worker="codex:c0de1234",
    )
    assert doc["worker"]["observedModel"] is None


def test_codex_turn_usage_stays_null_rather_than_inferred():
    for turn in build_codex()["turns"]:
        assert set(turn["usage"].values()) == {None}


# --- full vs bounded ---------------------------------------------------------

def _big_transcript(path, size=200_000):
    payload = "x" * size
    lines = [
        {"type": "user", "uuid": "u", "timestamp": "2026-07-27T09:00:00.000Z",
         "message": {"role": "user", "content": "go"}},
        {"type": "assistant", "uuid": "a", "timestamp": "2026-07-27T09:00:01.000Z",
         "message": {"id": "m", "role": "assistant", "model": "claude-opus-5",
                     "content": [{"type": "tool_use", "id": "t1", "name": "Bash",
                                  "input": {"command": "cat big"}}]}},
        {"type": "user", "uuid": "u2", "timestamp": "2026-07-27T09:00:02.000Z",
         "message": {"role": "user", "content": [
             {"type": "tool_result", "tool_use_id": "t1", "content": payload}]}},
    ]
    path.write_text("\n".join(json.dumps(line) for line in lines) + "\n")
    return payload


def test_full_mode_preserves_a_200kb_tool_result_byte_exact(tmp_path):
    src = tmp_path / "big.jsonl"
    payload = _big_transcript(src)
    doc = build_claude(src)
    result = doc["turns"][0]["output"][0]["result"]
    assert result["content"] == payload
    assert result["truncated"] is False
    assert doc["completeness"]["status"] == "complete"
    assert doc["completeness"]["truncatedItemCount"] == 0


def test_bounded_mode_reports_honest_byte_counts_and_hash(tmp_path):
    src = tmp_path / "big.jsonl"
    payload = _big_transcript(src)
    doc = build_claude(src, max_tool_result_bytes=4096)
    result = doc["turns"][0]["output"][0]["result"]
    assert result["truncated"] is True
    assert result["originalByteCount"] == len(payload.encode())
    assert result["returnedByteCount"] == 4096
    assert result["contentSha256"] == hashlib.sha256(payload.encode()).hexdigest()
    assert result["content"] == payload[:4096]
    assert doc["completeness"]["requested"] == "bounded"
    assert doc["completeness"]["status"] == "bounded"
    assert doc["completeness"]["truncatedItemCount"] == 1


def test_bounded_truncation_lands_on_a_codepoint_boundary(tmp_path):
    src = tmp_path / "utf8.jsonl"
    text = "é" * 100  # two bytes each
    src.write_text("\n".join(json.dumps(line) for line in [
        {"type": "assistant", "uuid": "a",
         "message": {"id": "m", "role": "assistant", "model": "claude-opus-5",
                     "content": [{"type": "tool_use", "id": "t1", "name": "Read",
                                  "input": {}}]}},
        {"type": "user", "uuid": "u",
         "message": {"role": "user", "content": [
             {"type": "tool_result", "tool_use_id": "t1", "content": text}]}},
    ]) + "\n")
    result = build_claude(src, max_tool_result_bytes=5)["turns"][0]["output"][0]["result"]
    assert result["content"] == "éé"
    assert result["returnedByteCount"] == 4 <= 5
    result["content"].encode("utf-8")  # valid UTF-8 by construction


def test_bounded_mode_applies_to_codex_command_output():
    thread = json.loads(CODEX_THREAD.read_text())
    thread["turns"][0]["items"][3]["aggregatedOutput"] = "y" * 10_000
    doc = build_codex(thread, max_tool_result_bytes=1024)
    result = doc["turns"][0]["output"][2]["result"]
    assert result["truncated"] is True
    assert result["originalByteCount"] == 10_000
    assert result["returnedByteCount"] == 1024
    # source-limited outranks bounded.
    assert doc["completeness"]["status"] == "source-limited"
    assert doc["completeness"]["truncatedItemCount"] == 1


def test_bound_validation():
    for bad in (-1, 1.5, True, "4096"):
        with pytest.raises(InvalidRequest):
            build_claude(max_tool_result_bytes=bad)


def test_zero_bound_yields_metadata_only_but_truthful_counts(tmp_path):
    src = tmp_path / "big.jsonl"
    payload = _big_transcript(src, size=1000)
    doc = build_claude(src, max_tool_result_bytes=0)
    result = doc["turns"][0]["output"][0]["result"]
    assert result["content"] == ""
    assert result["truncated"] is True
    assert result["returnedByteCount"] == 0
    assert result["originalByteCount"] == 1000
    assert result["contentSha256"] == hashlib.sha256(payload.encode()).hexdigest()
    assert doc["completeness"]["status"] == "bounded"


def test_unrepresentable_structural_data_makes_completeness_partial(tmp_path):
    src = tmp_path / "odd.jsonl"
    src.write_text(json.dumps({
        "type": "assistant", "uuid": "a",
        "message": {"id": "m", "role": "assistant", "model": "claude-opus-5",
                    # A non-dict content block cannot be represented as an item.
                    "content": [{"type": "text", "text": "hi"}, "bare string"]},
    }) + "\n")
    doc = build_claude(src)
    assert doc["completeness"]["omittedItemCount"] == 1
    assert doc["completeness"]["status"] == "partial"


def test_codex_unrepresentable_items_make_completeness_partial():
    thread = json.loads(CODEX_THREAD.read_text())
    thread["turns"] = [{"id": "t", "status": "completed", "itemsView": "full",
                        "items": ["bare string"]}]
    doc = build_codex(thread)
    assert doc["completeness"]["omittedItemCount"] == 1
    assert doc["completeness"]["status"] == "partial"


def test_safe_native_strips_nested_reasoning_content():
    thread = json.loads(CODEX_THREAD.read_text())
    thread["turns"][0]["items"] = [{
        "type": "someFutureWrapper", "id": "w1",
        "steps": [{"type": "reasoningDelta",
                   "content": [{"text": "NESTED HIDDEN THOUGHT"}],
                   "summary": ["visible"]}],
        "nested": {"inner": {"type": "reasoning",
                             "content": "ALSO HIDDEN"}},
    }]
    doc = build_codex(thread)
    blob = json.dumps(doc)
    assert "NESTED HIDDEN THOUGHT" not in blob
    assert "ALSO HIDDEN" not in blob
    # Everything that is not reasoning content still survives.
    assert "visible" in blob
    assert conv.safe_native([{"type": "reasoning", "content": "x", "a": 1}]) == [
        {"type": "reasoning", "a": 1},
    ]


def test_warnings_are_capped_with_a_suppression_note(tmp_path):
    lines = []
    for index in range(60):
        lines.append(json.dumps({
            "type": "user", "uuid": f"u{index}",
            "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": f"missing{index}",
                 "content": "x"}]},
        }))
    src = tmp_path / "many.jsonl"
    src.write_text("\n".join(lines) + "\n")
    warnings = build_claude(src)["completeness"]["warnings"]
    assert len(warnings) == 51
    assert warnings[-1] == "10 more warnings suppressed"


# --- export ------------------------------------------------------------------

def test_claude_export_writes_conversation_and_hashed_native(tmp_path):
    doc = build_claude()
    out = conv.write_export(doc, tmp_path / "_execution", engine="claude",
                            native_source=str(CLAUDE_JSONL))
    written = json.loads(pathlib.Path(out["conversation_path"]).read_text())
    artifact = written["nativeArtifacts"][0]
    assert artifact["kind"] == "claude-transcript-jsonl"
    assert artifact["path"] == "native/claude-transcript.jsonl"
    assert artifact["availability"] == "exported"
    raw = CLAUDE_JSONL.read_bytes()
    assert artifact["byteCount"] == len(raw)
    assert artifact["sha256"] == hashlib.sha256(raw).hexdigest()
    assert pathlib.Path(out["native_path"]).read_bytes() == raw


def test_codex_export_names_the_artifact_thread_read(tmp_path):
    thread = json.loads(CODEX_THREAD.read_text())
    out = conv.write_export(build_codex(thread), tmp_path / "out",
                            engine="codex", native_source=thread)
    written = json.loads(pathlib.Path(out["conversation_path"]).read_text())
    artifact = written["nativeArtifacts"][0]
    assert artifact["kind"] == "codex-thread-read"
    assert artifact["path"] == "native/codex-thread.json"
    payload = pathlib.Path(out["native_path"]).read_bytes()
    assert artifact["sha256"] == hashlib.sha256(payload).hexdigest()
    assert json.loads(payload) == thread


def test_export_without_native_keeps_only_a_referenced_pointer(tmp_path):
    out = conv.write_export(build_claude(), tmp_path / "out", engine="claude",
                            native_source=str(CLAUDE_JSONL),
                            include_native=False)
    written = json.loads(pathlib.Path(out["conversation_path"]).read_text())
    artifact = written["nativeArtifacts"][0]
    assert artifact["availability"] == "referenced"
    assert artifact["sha256"] is None
    assert out["native_path"] is None
    assert not (tmp_path / "out" / "native").exists()


def test_codex_export_without_native_has_no_artifacts(tmp_path):
    thread = json.loads(CODEX_THREAD.read_text())
    out = conv.write_export(build_codex(thread), tmp_path / "out",
                            engine="codex", native_source=thread,
                            include_native=False)
    written = json.loads(pathlib.Path(out["conversation_path"]).read_text())
    assert written["nativeArtifacts"] == []
    assert out["native_path"] is None


# --- Scope 07: source order and message provenance ---------------------------

def _codex_turn(items, turn_id="t1"):
    return {"id": "th_order", "cwd": "/workspace", "turns": [{
        "id": turn_id, "status": "completed",
        "startedAt": 1780000000, "completedAt": 1780000060,
        "itemsView": "full", "items": items,
    }]}


def _user_message(item_id, text, client_id=None):
    return {"type": "userMessage", "id": item_id, "clientId": client_id,
            "content": [{"type": "text", "text": text}]}


def ordered(turn):
    return sorted(turn["input"] + turn["output"], key=lambda i: i["ordinal"])


def test_ordinal_reconstructs_interleaved_codex_input_and_output():
    """A steer lands mid-turn, and only the ordinal can put it back there."""
    start = conv_start_id = "ccx:v1:c0de1234:start:11111111-1111-4111-8111-111111111111"
    steer = "ccx:v1:c0de1234:steer:22222222-2222-4222-8222-222222222222"
    doc = build_codex(_codex_turn([
        _user_message("i1", "build it", start),
        {"type": "agentMessage", "id": "i2", "text": "starting"},
        {"type": "commandExecution", "id": "i3", "command": "make",
         "status": "completed", "exitCode": 0, "aggregatedOutput": "ok"},
        _user_message("i4", "also run the tests", steer),
        {"type": "agentMessage", "id": "i5", "text": "done"},
    ]))
    turn = doc["turns"][0]

    assert [i["id"] for i in ordered(turn)] == ["i1", "i2", "i3", "i4", "i5"]
    # Bucketing alone would file the steer beside the opening prompt...
    assert [i["id"] for i in turn["input"]] == ["i1", "i4"]
    # ...but its ordinal is its real position in the native sequence.
    steered = next(i for i in turn["input"] if i["id"] == "i4")
    assert steered["ordinal"] == 3
    assert steered["origin"] == "ccx-steer"
    assert next(i for i in turn["input"] if i["id"] == "i1")["ordinal"] == 0
    assert conv_start_id  # the start id is what the first message carries


def test_codex_multipart_input_gets_adjacent_dense_ordinals():
    """One native item expanding into children keeps them contiguous."""
    client_id = "ccx:v1:c0de1234:start:33333333-3333-4333-8333-333333333333"
    doc = build_codex(_codex_turn([
        {"type": "userMessage", "id": "i1", "clientId": client_id, "content": [
            {"type": "text", "text": "look at this"},
            {"type": "localImage", "path": "/tmp/a.png"},
            {"type": "text", "text": "and this"},
        ]},
        {"type": "agentMessage", "id": "i2", "text": "ok"},
    ]))
    turn = doc["turns"][0]

    parts = turn["input"]
    assert [i["ordinal"] for i in parts] == [0, 1, 2]
    assert [i["kind"] for i in parts] == ["user-text", "attachment", "user-text"]
    # Every part of one message shares that message's client id and origin.
    assert {i["clientId"] for i in parts} == {client_id}
    assert {i["origin"] for i in parts} == {"ccx-start"}
    assert turn["output"][0]["ordinal"] == 3


def test_ordinals_are_dense_and_unique_within_every_turn():
    for doc in (build_claude(), build_codex()):
        for turn in doc["turns"]:
            ordinals = [i["ordinal"] for i in turn["input"] + turn["output"]]
            assert sorted(ordinals) == list(range(len(ordinals)))


def test_codex_foreign_and_missing_client_ids_stay_unknown():
    """Origin is proof, not a guess: only CCX's own ids classify."""
    doc = build_codex(_codex_turn([
        _user_message("i1", "from another client", "user_msg_42"),
        _user_message("i2", "no id at all", None),
        _user_message("i3", "shaped like ours", "ccx:v1:c0de1234:start:"
                      "44444444-4444-1444-8444-444444444444"),
    ]))
    items = {i["id"]: i for i in doc["turns"][0]["input"]}

    assert items["i1"]["clientId"] == "user_msg_42"
    assert items["i1"]["origin"] is None
    assert items["i2"]["clientId"] is None and items["i2"]["origin"] is None
    # The third id is not a v4 uuid, so it is not one CCX minted either.
    assert items["i3"]["clientId"].startswith("ccx:v1:")
    assert items["i3"]["origin"] is None


def test_claude_ordinals_follow_source_order_without_a_paired_result():
    """Updating an already-emitted item must not consume a second ordinal."""
    doc = build_claude()
    for turn in doc["turns"]:
        assert ordered(turn) == sorted(
            turn["input"] + turn["output"], key=lambda i: i["ordinal"]
        )
    paired = [
        i for i in all_items(doc)
        if i["kind"] == "tool-call" and i["result"]["content"] is not None
    ]
    assert paired, "fixture should contain a tool call with its result"
    for turn in doc["turns"]:
        ordinals = [i["ordinal"] for i in turn["input"] + turn["output"]]
        assert sorted(ordinals) == list(range(len(ordinals)))
    # Input precedes output in the fixture's turns, which the ordinals show
    # without relying on the buckets.
    first = doc["turns"][0]
    assert min(i["ordinal"] for i in first["input"]) == 0


def test_safe_native_strips_internal_ccx_parser_metadata_recursively():
    value = {
        "_ccx_start_offset": 1,
        "visible": {
            "_ccx_end_offset": 2,
            "nested": [{"_ccx_private": "x", "keep": True}],
        },
    }
    assert conv.safe_native(value) == {
        "visible": {"nested": [{"keep": True}]}
    }


def test_claude_full_conversation_never_leaks_internal_ccx_offsets():
    doc = build_claude()
    assert "_ccx_" not in json.dumps(doc)


def test_claude_action_summary_mapping_and_unknown_tool_fallback(tmp_path):
    path = tmp_path / "claude-action-summary.jsonl"
    tools = [
        ("bash", "Bash", {"command": "echo hi"}, {"verb": "Ran", "target": "echo hi"}),
        ("read", "Read", {"file_path": "/work/a.txt"}, {"verb": "Read", "target": "/work/a.txt"}),
        ("edit", "Edit", {"file_path": "/work/b.txt"}, {"verb": "Edited", "target": "/work/b.txt"}),
        ("write", "Write", {"file_path": "/work/c.txt"}, {"verb": "Wrote", "target": "/work/c.txt"}),
        ("grep", "Grep", {"pattern": "needle"}, {"verb": "Searched", "target": "needle"}),
        ("glob", "Glob", {"pattern": "**/*.py"}, {"verb": "Searched", "target": "**/*.py"}),
        ("web", "WebSearch", {"query": "ccx paging"}, {"verb": "Searched", "target": "ccx paging"}),
        (
            "unknown", "mcp__files__read",
            {"path": "/must-not-guess", "name": "wrong", "target": "wrong"},
            {"verb": "Called", "target": "mcp__files__read"},
        ),
    ]
    events = [
        {
            "type": "user", "uuid": "u0", "timestamp": "2026-09-27T10:00:00Z",
            "message": {"role": "user", "content": "go"},
        },
        {
            "type": "assistant", "uuid": "a0", "timestamp": "2026-09-27T10:00:01Z",
            "message": {
                "id": "m0", "role": "assistant", "model": "claude-opus-5",
                "content": [
                    {"type": "tool_use", "id": tool_id, "name": name, "input": arguments}
                    for tool_id, name, arguments, _expected in tools
                ],
            },
        },
    ]
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n")
    output = build_claude(path)["turns"][0]["output"]
    by_id = {item["id"]: item for item in output}
    for tool_id, _name, _arguments, expected in tools:
        assert by_id[tool_id]["summary"] == expected


def test_claude_model_written_description_is_exposed_only_for_bash_task_agent(tmp_path):
    path = tmp_path / "claude-summary-description.jsonl"
    long_description = "é" * 400 + "\nsecond line"
    tools = [
        ("bash-described", "Bash", {"command": "echo hi", "description": "Record CX1c gate result"}),
        ("bash-undescribed", "Bash", {"command": "pwd"}),
        ("bash-multiline", "Bash", {"command": "echo multi", "description": "First line\nsecond line"}),
        ("bash-long", "Bash", {"command": "echo long", "description": long_description}),
        ("bash-leading-newline", "Bash", {"command": "echo lead", "description": "\nDo it"}),
        ("bash-whitespace", "Bash", {"command": "echo space", "description": "   "}),
        ("bash-newlines", "Bash", {"command": "echo blank", "description": "\n\n"}),
        ("bash-trimmed-multiline", "Bash", {"command": "echo trim", "description": "  Run tests  \nmore"}),
        ("task", "Task", {"description": "Delegate focused review"}),
        ("agent", "Agent", {"description": "Inspect migration edge cases"}),
        ("read", "Read", {"file_path": "/work/a.txt", "description": "Must be ignored"}),
    ]
    events = [
        {
            "type": "user", "uuid": "u0", "timestamp": "2026-09-28T10:00:00Z",
            "message": {"role": "user", "content": "go"},
        },
        {
            "type": "assistant", "uuid": "a0", "timestamp": "2026-09-28T10:00:01Z",
            "message": {
                "id": "m0", "role": "assistant", "model": "claude-opus-5",
                "content": [
                    {"type": "tool_use", "id": tool_id, "name": name, "input": arguments}
                    for tool_id, name, arguments in tools
                ],
            },
        },
    ]
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n")
    output = build_claude(path)["turns"][0]["output"]
    by_id = {item["id"]: item for item in output}

    assert by_id["bash-described"]["summary"] == {
        "verb": "Ran", "target": "echo hi", "description": "Record CX1c gate result",
    }
    assert by_id["bash-undescribed"]["summary"] == {"verb": "Ran", "target": "pwd"}
    assert by_id["bash-multiline"]["summary"] == {
        "verb": "Ran",
        "target": "echo multi",
        "description": "First line",
        "description_truncated": True,
    }
    long_summary = by_id["bash-long"]["summary"]
    assert long_summary["verb"] == "Ran"
    assert long_summary["target"] == "echo long"
    assert long_summary["description_truncated"] is True
    assert "\n" not in long_summary["description"]
    assert len(long_summary["description"].encode("utf-8")) <= 512
    long_summary["description"].encode("utf-8")
    assert by_id["bash-leading-newline"]["summary"] == {
        "verb": "Ran", "target": "echo lead", "description": "Do it",
    }
    assert by_id["bash-whitespace"]["summary"] == {
        "verb": "Ran", "target": "echo space",
    }
    assert by_id["bash-newlines"]["summary"] == {
        "verb": "Ran", "target": "echo blank",
    }
    assert by_id["bash-trimmed-multiline"]["summary"] == {
        "verb": "Ran",
        "target": "echo trim",
        "description": "Run tests",
        "description_truncated": True,
    }
    assert by_id["task"]["summary"] == {
        "verb": "Called", "target": "Task", "description": "Delegate focused review",
    }
    assert by_id["agent"]["summary"] == {
        "verb": "Called", "target": "Agent", "description": "Inspect migration edge cases",
    }
    assert by_id["read"]["summary"] == {"verb": "Read", "target": "/work/a.txt"}


def test_codex_action_summary_mapping_and_mcp_fallback_ignores_argument_names():
    items = [
        {
            "type": "commandExecution", "id": "command", "command": "make test",
            "cwd": "/work", "status": "completed", "exitCode": 0,
            "aggregatedOutput": "ok", "commandActions": [{"type": "search"}],
        },
        {
            "type": "fileChange", "id": "one-file", "status": "completed",
            "changes": [{"path": "/work/a.py", "kind": "update", "diff": "@@"}],
        },
        {
            "type": "fileChange", "id": "two-files", "status": "completed",
            "changes": [
                {"path": "/work/a.py", "kind": "update", "diff": "@@"},
                {"path": "/work/b.py", "kind": "create", "diff": "@@"},
            ],
        },
        {"type": "webSearch", "id": "web", "query": "ccx paging"},
        {
            "type": "mcpToolCall", "id": "mcp", "server": "srv", "tool": "do",
            "arguments": {"path": "/must-not-guess", "name": "wrong", "target": "wrong"},
            "status": "completed",
        },
        {
            "type": "dynamicToolCall", "id": "dynamic", "namespace": "ns",
            "tool": "lookup",
            "arguments": {"path": "/must-not-guess", "name": "wrong", "target": "wrong"},
            "status": "completed",
        },
    ]
    normalized = {
        item["id"]: item
        for item in build_codex(_codex_turn(items))["turns"][0]["output"]
    }
    assert normalized["command"]["summary"] == {"verb": "Ran", "target": "make test"}
    assert "description" not in normalized["command"]["summary"]
    assert normalized["one-file"]["summary"] == {"verb": "Changed", "target": "/work/a.py"}
    assert normalized["two-files"]["summary"] == {"verb": "Changed", "target": "2 files"}
    assert normalized["web"]["summary"] == {"verb": "Searched", "target": "ccx paging"}
    assert normalized["mcp"]["summary"] == {"verb": "Called", "target": "srv/do"}
    assert normalized["dynamic"]["summary"] == {"verb": "Called", "target": "ns/lookup"}


def test_claude_intermediate_text_blocks_between_tools_are_preserved_in_source_order(tmp_path):
    path = tmp_path / "claude-intermediate-text.jsonl"
    events = [
        {
            "type": "user", "uuid": "u0", "timestamp": "2026-09-27T10:00:00Z",
            "message": {"role": "user", "content": "go"},
        },
        {
            "type": "assistant", "uuid": "a0", "timestamp": "2026-09-27T10:00:01Z",
            "message": {
                "id": "m0", "role": "assistant", "model": "claude-opus-5",
                "content": [
                    {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "one"}},
                    {"type": "text", "text": "between tools"},
                    {"type": "tool_use", "id": "t2", "name": "Read", "input": {"file_path": "/work/a"}},
                    {"type": "text", "text": "after tools"},
                ],
            },
        },
    ]
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n")
    turn = build_claude(path)["turns"][0]
    ordered_items = sorted(turn["input"] + turn["output"], key=lambda item: item["ordinal"] )
    assert [(item["kind"], item.get("text"), item.get("name")) for item in ordered_items] == [
        ("user-text", "go", None),
        ("tool-call", None, "Bash"),
        ("assistant-text", "between tools", None),
        ("tool-call", None, "Read"),
        ("assistant-text", "after tools", None),
    ]


def test_codex_intermediate_agent_messages_between_tools_keep_phase_and_source_order():
    items = [
        {
            "type": "commandExecution", "id": "tool-1", "command": "one",
            "status": "completed", "exitCode": 0, "aggregatedOutput": "ok",
        },
        {
            "type": "agentMessage", "id": "commentary", "text": "between tools",
            "phase": "commentary",
        },
        {
            "type": "commandExecution", "id": "tool-2", "command": "two",
            "status": "completed", "exitCode": 0, "aggregatedOutput": "ok",
        },
        {
            "type": "agentMessage", "id": "final", "text": "done",
            "phase": "final_answer",
        },
    ]
    turn = build_codex(_codex_turn(items))["turns"][0]
    ordered_items = sorted(turn["input"] + turn["output"], key=lambda item: item["ordinal"] )
    assert [item["id"] for item in ordered_items] == ["tool-1", "commentary", "tool-2", "final"]
    assert [(item["id"], item.get("phase")) for item in ordered_items if item["kind"] == "assistant-text"] == [
        ("commentary", "commentary"),
        ("final", "final_answer"),
    ]


def test_action_summary_target_keeps_first_line_and_caps_utf8_to_512_bytes():
    command = "é" * 400 + "\r\nsecond line"
    item = build_codex(_codex_turn([{
        "type": "commandExecution", "id": "long-command", "command": command,
        "status": "completed", "exitCode": 0, "aggregatedOutput": "ok",
    }]))["turns"][0]["output"][0]
    assert item["arguments"]["command"] == command
    assert item["summary"]["verb"] == "Ran"
    assert item["summary"]["target_truncated"] is True
    assert "\r" not in item["summary"]["target"]
    assert "\n" not in item["summary"]["target"]
    assert len(item["summary"]["target"].encode("utf-8")) <= 512
    item["summary"]["target"].encode("utf-8")


def test_action_summary_marks_multiline_target_truncated_even_when_first_line_is_short():
    item = build_codex(_codex_turn([{
        "type": "commandExecution", "id": "multiline-command",
        "command": "echo hi\nsecond line",
        "status": "completed", "exitCode": 0, "aggregatedOutput": "ok",
    }]))["turns"][0]["output"][0]
    assert item["summary"] == {
        "verb": "Ran", "target": "echo hi", "target_truncated": True,
    }


def test_codex_file_change_summary_uses_distinct_native_paths_and_omits_missing_data():
    items = [
        {"type": "fileChange", "id": "missing", "status": "completed"},
        {"type": "fileChange", "id": "empty", "status": "completed", "changes": []},
        {
            "type": "fileChange", "id": "dupe", "status": "completed",
            "changes": [
                {"path": "/work/a.py", "kind": "update", "diff": "@@1"},
                {"path": "/work/a.py", "kind": "update", "diff": "@@2"},
            ],
        },
        {
            "type": "fileChange", "id": "distinct", "status": "completed",
            "changes": [
                {"path": "/work/a.py", "kind": "update", "diff": "@@1"},
                {"path": "/work/a.py", "kind": "update", "diff": "@@2"},
                {"path": "/work/b.py", "kind": "create", "diff": "@@3"},
                {"kind": "update", "diff": "@@4"},
            ],
        },
    ]
    normalized = {
        item["id"]: item
        for item in build_codex(_codex_turn(items))["turns"][0]["output"]
    }
    assert "summary" not in normalized["missing"]
    assert "summary" not in normalized["empty"]
    assert normalized["dupe"]["summary"] == {
        "verb": "Changed", "target": "/work/a.py"
    }
    assert normalized["distinct"]["summary"] == {
        "verb": "Changed", "target": "2 files"
    }


def test_codex_mcp_summary_requires_non_empty_server_and_tool():
    items = [
        {"type": "mcpToolCall", "id": "missing-both", "arguments": {}, "status": "completed"},
        {"type": "mcpToolCall", "id": "missing-tool", "server": "srv", "arguments": {}, "status": "completed"},
        {"type": "mcpToolCall", "id": "missing-server", "tool": "do", "arguments": {}, "status": "completed"},
        {"type": "mcpToolCall", "id": "empty", "server": "", "tool": "", "arguments": {}, "status": "completed"},
        {"type": "mcpToolCall", "id": "good", "server": "srv", "tool": "do", "arguments": {}, "status": "completed"},
    ]
    normalized = {
        item["id"]: item
        for item in build_codex(_codex_turn(items))["turns"][0]["output"]
    }
    for item_id in ("missing-both", "missing-tool", "missing-server", "empty"):
        assert normalized[item_id]["name"] is None
        assert "summary" not in normalized[item_id]
    assert normalized["good"]["name"] == "srv/do"
    assert normalized["good"]["summary"] == {
        "verb": "Called", "target": "srv/do"
    }


def test_claude_observed_compaction_shape_pairs_summary_by_parent_identity():
    src = FIXTURES / "claude_compaction_observed_2026_09.jsonl"
    doc = build_claude(src)
    items = list(all_items(doc))
    compact = next(item for item in items if item.get("nativeType") == "compact_boundary")
    assert compact["data"]["summary"].startswith(
        "This session is being continued from a previous conversation"
    )
    assert compact["data"]["preTokens"] == 312865
    assert compact["data"]["postTokens"] == 14659
    assert compact["data"]["trigger"] == "manual"
    # File-order / parent identity is authoritative even though Claude timestamps
    # the generated summary slightly before the preceding compact boundary.
    assert not any(
        item.get("id") == "compact-summary" and item.get("kind") == "user-text"
        for item in items
    )
    # The local-command echo shares prompt identity + semantic command with the
    # real submission and is suppressed; the real /compact remains visible.
    visible_compact_inputs = [
        item for item in items
        if item.get("kind") in {"user-text", "command"}
        and (
            item.get("text") == "/compact"
            or item.get("name") == "/compact"
        )
    ]
    assert len(visible_compact_inputs) == 1
    assert visible_compact_inputs[0]["id"] == "compact-prompt"


def test_claude_unlinked_compact_summary_is_preserved_instead_of_dropped(tmp_path):
    path = tmp_path / "unlinked-compact-summary.jsonl"
    path.write_text(json.dumps({
        "type": "user",
        "uuid": "orphan-summary",
        "timestamp": "2026-09-27T10:00:00Z",
        "parentUuid": "missing-boundary",
        "isCompactSummary": True,
        "isVisibleInTranscriptOnly": True,
        "message": {"role": "user", "content": "ORPHAN SUMMARY MUST SURVIVE"},
    }) + "\n")
    doc = build_claude(path)
    assert any(
        item.get("kind") == "user-text"
        and item.get("id") == "orphan-summary"
        and item.get("text") == "ORPHAN SUMMARY MUST SURVIVE"
        for item in all_items(doc)
    )


def test_claude_ambiguous_compact_summaries_are_not_guessed(tmp_path):
    events = [
        {
            "type": "system", "subtype": "compact_boundary", "uuid": "boundary",
            "timestamp": "2026-09-27T10:00:00Z", "content": "Conversation compacted",
            "compactMetadata": {"preTokens": 10, "postTokens": 5},
        },
        {
            "type": "user", "uuid": "summary-a", "parentUuid": "boundary",
            "isCompactSummary": True, "message": {"role": "user", "content": "A"},
        },
        {
            "type": "user", "uuid": "summary-b", "parentUuid": "boundary",
            "isCompactSummary": True, "message": {"role": "user", "content": "B"},
        },
    ]
    path = tmp_path / "ambiguous-compact-summary.jsonl"
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n")
    doc = build_claude(path)
    items = list(all_items(doc))
    compact = next(item for item in items if item.get("nativeType") == "compact_boundary")
    assert "summary" not in compact["data"]
    assert {item.get("text") for item in items if item.get("kind") == "user-text"} >= {"A", "B"}


def test_claude_unmatched_local_command_wrapper_remains_visible(tmp_path):
    path = tmp_path / "standalone-command-wrapper.jsonl"
    path.write_text(json.dumps({
        "type": "user",
        "uuid": "wrapper",
        "promptId": "prompt-only-wrapper",
        "message": {
            "role": "user",
            "content": "<command-name>/compact</command-name><command-args>keep this</command-args>",
        },
    }) + "\n")
    doc = build_claude(path)
    command = next(item for item in all_items(doc) if item.get("kind") == "command")
    assert command["id"] == "wrapper"
    assert command["name"] == "/compact"
    assert command["arguments"] == {"args": "keep this"}


def test_codex_context_compaction_never_exposes_opaque_summary_payload():
    native = {
        "type": "contextCompaction",
        "id": "compact-opaque",
        "encryptedSummary": "ciphertext-that-must-stay-opaque",
        "summary": "PLAINTEXT-LIKE-FIELD-MUST-NOT-BE-TRUSTED",
    }
    item = build_codex(_codex_turn([native]))["turns"][0]["output"][0]
    assert item["kind"] == "system-text"
    assert item["nativeType"] == "contextCompaction"
    assert item.get("text") is None
    assert item.get("data") is None
    assert "summary" not in item
    assert "ciphertext-that-must-stay-opaque" not in json.dumps(item)
    assert "PLAINTEXT-LIKE-FIELD-MUST-NOT-BE-TRUSTED" not in json.dumps(item)


def test_claude_session_bookkeeping_is_not_conversation(tmp_path):
    """Titles, modes, hook outcomes, injected context and API-request snapshots
    are dropped and counted; an unknown record type still surfaces as `other`."""
    def line(**fields):
        return json.dumps(fields)

    src = tmp_path / "t.jsonl"
    src.write_text("\n".join([
        line(type="custom-title", customTitle="demo", sessionId="s"),
        line(type="permission-mode", permissionMode="default", sessionId="s"),
        line(type="attachment", uuid="a1", attachment={"type": "session_context",
             "context": {"userEmail": "someone@example.com"}}),
        line(type="attachment", uuid="a2", attachment={"type": "prompt_snapshot",
             "systemPrompt": ["You are an agent"]}),
        line(type="user", uuid="u1", timestamp="2026-10-16T10:00:00Z",
             message={"role": "user", "content": "say hi"}),
        line(type="attachment", uuid="a3", attachment={"type": "hook_success",
             "hookName": "Stop"}),
        line(type="api-request-blob", hash="h", message={"role": "system", "content": []}),
        line(type="assistant", uuid="m1", timestamp="2026-10-16T10:00:01Z",
             message={"id": "msg1", "role": "assistant", "model": "claude-haiku-5-5",
                      "content": [{"type": "text", "text": "hi"}]}),
        line(type="system", uuid="s1", subtype="turn_duration", durationMs=5),
        line(type="future-record", uuid="f1", detail="kept"),
    ]) + "\n")
    doc = build_claude(src)
    kinds = [(item["kind"], item.get("nativeType")) for item in all_items(doc)]
    assert kinds == [("user-text", "user"), ("assistant-text", "assistant"),
                     ("other", "future-record")]
    assert len(doc["turns"]) == 1
    assert doc["stats"]["excludedBookkeepingRecords"] == 7
    assert "someone@example.com" not in json.dumps(doc)
