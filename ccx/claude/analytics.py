from __future__ import annotations

import pathlib
import time
from typing import Any

from ccx.store.paths import iter_subagent_transcripts
from ccx.claude.transcript import (
    _iter_transcript,
    _merge_usage_objects,
    _parse_event_ts_ms,
    _process_command_message,
)

ANALYTICS_SCHEMA_VERSION = 3
TOKEN_FIELDS = (
    ("input", "input_tokens"),
    ("output", "output_tokens"),
    ("cacheRead", "cache_read_input_tokens"),
    ("cacheCreation", "cache_creation_input_tokens"),
)


def _zero_tokens() -> dict[str, int]:
    return {"input": 0, "output": 0, "cacheRead": 0, "cacheCreation": 0}


def _add_tokens(dst: dict[str, int], src: dict[str, int]) -> None:
    for key in _zero_tokens():
        dst[key] = int(dst.get(key) or 0) + int(src.get(key) or 0)


def _tokens_from_usage(usage: dict[str, Any]) -> dict[str, int]:
    out = _zero_tokens()
    for out_key, usage_key in TOKEN_FIELDS:
        val = usage.get(usage_key)
        if isinstance(val, int):
            out[out_key] += val
    return out


def _message_id(message: dict[str, Any]) -> str | None:
    value = message.get("id")
    return value if isinstance(value, str) and value else None


def _add_model_usage(
    prompt: dict[str, Any],
    totals: dict[str, Any],
    model: str | None,
    tokens: dict[str, int],
    *,
    level: str = "main",
    events: int = 1,
) -> None:
    if model == "<synthetic>":
        return
    model = model or "unknown"
    prompt_model = prompt.setdefault("byModel", {}).setdefault(
        model, {"events": 0, "tokens": _zero_tokens()}
    )
    prompt_model["events"] += int(events)
    _add_tokens(prompt_model["tokens"], tokens)

    bucket = _model_bucket(totals, model)
    bucket["events"] += int(events)
    bucket["level"] = level if bucket.get("level") in (None, level) else "mixed"
    _add_tokens(bucket["tokens"], tokens)


def _content_blocks(content: Any) -> list[dict[str, Any]]:
    if isinstance(content, list):
        return [c for c in content if isinstance(c, dict)]
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    return []


def _text_from_user_content(content: Any) -> str:
    parts = []
    for block in _content_blocks(content):
        if block.get("type") == "text" and isinstance(block.get("text"), str):
            processed = _process_command_message(block["text"])
            if processed is not None:
                parts.append(processed)
    return "\n".join(parts).strip()


def _is_synthetic_user_text(text: str) -> bool:
    stripped = text.strip()
    if stripped == "Continue from where you left off.":
        return True
    return stripped.startswith(("<bash-input>", "<bash-stdout>", "<bash-stderr>"))


def _is_pure_tool_result(content: Any) -> bool:
    blocks = _content_blocks(content)
    return bool(blocks) and all(b.get("type") == "tool_result" for b in blocks)


def _contains_queued_command(value: Any) -> int:
    if isinstance(value, dict):
        count = 1 if value.get("type") == "queued_command" else 0
        return count + sum(_contains_queued_command(v) for v in value.values())
    if isinstance(value, list):
        return sum(_contains_queued_command(v) for v in value)
    return 0


def _model_bucket(totals: dict[str, Any], model: str) -> dict[str, Any]:
    by_model = totals.setdefault("byModel", {})
    bucket = by_model.setdefault(model, {"events": 0, "tokens": _zero_tokens()})
    return bucket


def _new_prompt(prompt_id: str, ts_ms: int | None, turn_index: int) -> dict[str, Any]:
    return {
        "promptId": prompt_id,
        "turnIndex": turn_index,
        "tsMs": ts_ms,
        "durationMs": 0,
        "tokens": _zero_tokens(),
        "byModel": {},
        "tools": {},
        "toolErrors": 0,
        "askUserQuestions": 0,
        "steering": {"enqueued": 0, "boundary": 0, "idle": 0},
        "subagents": {"count": 0, "tokens": _zero_tokens(), "byAgent": {}},
    }


def _add_model_tokens(
    prompt: dict[str, Any],
    totals: dict[str, Any],
    model: str | None,
    tokens: dict[str, int],
    *,
    level: str = "main",
    events: int = 1,
) -> None:
    _add_model_usage(
        prompt, totals, model, tokens, level=level, events=events
    )


def _add_tool(prompt: dict[str, Any], totals: dict[str, Any], name: str) -> None:
    prompt_tools = prompt.setdefault("tools", {})
    prompt_tools[name] = int(prompt_tools.get(name) or 0) + 1
    totals_tools = totals.setdefault("tools", {})
    totals_tools[name] = int(totals_tools.get(name) or 0) + 1
    if name == "AskUserQuestion":
        prompt["askUserQuestions"] = int(prompt.get("askUserQuestions") or 0) + 1
        totals["askUserQuestions"] = int(totals.get("askUserQuestions") or 0) + 1


def _prompt_by_id(prompts: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {p["promptId"]: p for p in prompts}


def _agent_label(evt: dict[str, Any], path: pathlib.Path) -> str:
    return (
        evt.get("attributionAgent")
        or evt.get("attributionSkill")
        or evt.get("entrypoint")
        or evt.get("agentId")
        or path.stem
    )


def _read_subagent_rollup(
    path: pathlib.Path, stats: dict[str, int]
) -> dict[str, Any]:
    rollup = {
        "path": str(path),
        "agent": path.stem,
        "promptId": None,
        "tokens": _zero_tokens(),
        "byModel": {},
    }
    local_stats: dict[str, int] = {}
    usage_by_message: dict[str, dict[str, Any]] = {}
    idless_usage_index = 0
    try:
        for evt in _iter_transcript(path, local_stats, include_sidechain=True):
            if rollup["promptId"] is None and evt.get("promptId"):
                rollup["promptId"] = evt.get("promptId")
            rollup["agent"] = _agent_label(evt, path)
            msg = evt.get("message") or {}
            if evt.get("type") != "assistant" or not isinstance(msg, dict):
                continue
            usage = msg.get("usage") or {}
            if not isinstance(usage, dict) or not usage:
                continue
            mid = _message_id(msg)
            if mid is None:
                idless_usage_index += 1
                key = f"<idless>:{idless_usage_index}"
            else:
                key = mid
            model = msg.get("model")
            model = model if isinstance(model, str) and model else None
            prior = usage_by_message.get(key)
            if prior is None:
                usage_by_message[key] = {
                    "usage": _merge_usage_objects(None, usage),
                    "model": model,
                }
            else:
                prior["usage"] = _merge_usage_objects(
                    prior.get("usage"), usage
                )
                if model is not None:
                    prior["model"] = model
    except OSError:
        stats["subagentFilesUnreadable"] = (
            stats.get("subagentFilesUnreadable", 0) + 1
        )

    for item in usage_by_message.values():
        tokens = _tokens_from_usage(item.get("usage") or {})
        _add_tokens(rollup["tokens"], tokens)
        model = item.get("model")
        if model and model != "<synthetic>":
            bucket = rollup["byModel"].setdefault(
                model, {"events": 0, "tokens": _zero_tokens()}
            )
            bucket["events"] += 1
            _add_tokens(bucket["tokens"], tokens)

    stats["malformedLines"] = (
        stats.get("malformedLines", 0)
        + int(local_stats.get("malformedLines") or 0)
    )
    return rollup


def _add_subagent_rollups(rec: dict[str, Any], prompts: list[dict[str, Any]],
                          totals: dict[str, Any], stats: dict[str, int]) -> None:
    cwd = rec.get("cwd") or ""
    session_id = rec.get("sessionId") or ""
    if not cwd or not session_id:
        return
    prompt_map = _prompt_by_id(prompts)
    fallback_prompt = prompts[-1] if prompts else None
    sub_total = totals.setdefault("subagents", {"count": 0, "tokens": _zero_tokens(), "byAgent": {}})
    for path in iter_subagent_transcripts(cwd, session_id) or []:
        rollup = _read_subagent_rollup(path, stats)
        prompt = prompt_map.get(rollup.get("promptId")) or fallback_prompt
        if prompt is None:
            continue
        sub_total["count"] = int(sub_total.get("count") or 0) + 1
        _add_tokens(sub_total["tokens"], rollup["tokens"])
        _add_tokens(totals["tokens"], rollup["tokens"])
        agent_bucket = sub_total.setdefault("byAgent", {}).setdefault(
            rollup["agent"], {"count": 0, "tokens": _zero_tokens(), "byModel": {}}
        )
        agent_bucket["count"] += 1
        _add_tokens(agent_bucket["tokens"], rollup["tokens"])
        for model, model_data in rollup["byModel"].items():
            abm = agent_bucket["byModel"].setdefault(model, {"events": 0, "tokens": _zero_tokens()})
            abm["events"] += int(model_data.get("events") or 0)
            _add_tokens(abm["tokens"], model_data.get("tokens") or {})
            _add_model_tokens(
                prompt,
                totals,
                model,
                model_data.get("tokens") or {},
                level="subagent",
                events=int(model_data.get("events") or 0),
            )

        prompt_sub = prompt.setdefault("subagents", {"count": 0, "tokens": _zero_tokens(), "byAgent": {}})
        prompt_sub["count"] = int(prompt_sub.get("count") or 0) + 1
        _add_tokens(prompt_sub["tokens"], rollup["tokens"])
        _add_tokens(prompt["tokens"], rollup["tokens"])
        ps_agent = prompt_sub.setdefault("byAgent", {}).setdefault(
            rollup["agent"], {"count": 0, "tokens": _zero_tokens()}
        )
        ps_agent["count"] += 1
        _add_tokens(ps_agent["tokens"], rollup["tokens"])


def compute_session_analytics(
    rec: dict[str, Any], *, before_offset: int | None = None,
) -> dict[str, Any]:
    stats = {"malformedLines": 0, "subagentFilesUnreadable": 0}
    totals: dict[str, Any] = {
        "turns": 0,
        "startedAt": None,
        "endedAt": None,
        "activeMs": 0,
        "transcriptSpanMs": 0,
        "tokens": _zero_tokens(),
        "byModel": {},
        "tools": {},
        "toolErrors": 0,
        "askUserQuestions": 0,
        "subagents": {"count": 0, "tokens": _zero_tokens(), "byAgent": {}},
        "steering": {"enqueued": 0, "boundary": 0, "idle": 0},
    }
    prompts: list[dict[str, Any]] = []
    prompts_by_id: dict[str, dict[str, Any]] = {}
    current: dict[str, Any] | None = None
    first_ts: int | None = None
    last_ts: int | None = None
    generated_prompt_index = 0
    queued_pending = 0
    usage_by_message: dict[str, dict[str, Any]] = {}
    idless_usage_index = 0
    seen_tool_blocks: set[tuple[str, str]] = set()

    transcript = rec.get("transcript") or ""
    # A fork's transcript opens with its source's copied history; usage counts
    # only what this worker incurred after the fork point.
    from ccx.ops.forking import inherited_offset
    for evt in _iter_transcript(
        transcript, stats, after_offset=inherited_offset(rec),
        before_offset=before_offset,
    ):
        ts_ms = _parse_event_ts_ms(evt)
        if ts_ms is not None:
            first_ts = ts_ms if first_ts is None else min(first_ts, ts_ms)
            last_ts = ts_ms if last_ts is None else max(last_ts, ts_ms)

        etype = evt.get("type")
        if etype == "queue-operation":
            op = evt.get("operation")
            if op == "enqueue":
                queued_pending += 1
                totals["steering"]["enqueued"] += 1
                if current is not None:
                    current["steering"]["enqueued"] += 1
            elif op in ("dequeue", "remove"):
                queued_pending = max(0, queued_pending - 1)
            if current is not None and ts_ms is not None:
                current["_lastTsMs"] = ts_ms
            continue

        queued_attachments = _contains_queued_command(evt)
        if queued_attachments:
            totals["steering"]["boundary"] += queued_attachments
            if current is not None:
                current["steering"]["boundary"] += queued_attachments
                if ts_ms is not None:
                    current["_lastTsMs"] = ts_ms
            queued_pending = max(0, queued_pending - queued_attachments)

        msg = evt.get("message") or {}
        content = msg.get("content") if isinstance(msg, dict) else None
        if etype == "user":
            if _is_pure_tool_result(content):
                for block in _content_blocks(content):
                    if block.get("type") == "tool_result" and block.get("is_error"):
                        totals["toolErrors"] += 1
                        if current is not None:
                            current["toolErrors"] += 1
                    if current is not None and ts_ms is not None:
                        current["_lastTsMs"] = ts_ms
                continue
            text = _text_from_user_content(content)
            if text and not _is_synthetic_user_text(text):
                prompt_id = str(evt.get("promptId") or f"turn-{generated_prompt_index + 1}")
                if prompt_id in prompts_by_id:
                    current = prompts_by_id[prompt_id]
                else:
                    generated_prompt_index += 1
                    current = _new_prompt(prompt_id, ts_ms, generated_prompt_index)
                    prompts_by_id[prompt_id] = current
                    prompts.append(current)
                if queued_pending:
                    current["steering"]["idle"] += queued_pending
                    totals["steering"]["idle"] += queued_pending
                    queued_pending = 0
            continue

        if etype != "assistant" or current is None or not isinstance(msg, dict):
            if current is not None and ts_ms is not None:
                current["_lastTsMs"] = ts_ms
            continue
        if msg.get("model") == "<synthetic>":
            continue
        usage = msg.get("usage") or {}
        if isinstance(usage, dict) and usage:
            model = msg.get("model")
            model = model if isinstance(model, str) and model else None
            mid = _message_id(msg)
            if mid is None:
                idless_usage_index += 1
                key = f"<idless>:{idless_usage_index}"
            else:
                key = mid
            prior = usage_by_message.get(key)
            if prior is None:
                usage_by_message[key] = {
                    "usage": _merge_usage_objects(None, usage),
                    "prompt": current,
                    "model": model,
                }
            else:
                prior["usage"] = _merge_usage_objects(
                    prior.get("usage"), usage
                )
                if model is not None:
                    prior["model"] = model
                # First prompt attribution wins. A repeated API message id
                # appearing after another user boundary is still one message,
                # not fresh usage for the later prompt.
        for block in _content_blocks(content):
            if block.get("type") == "tool_use":
                mid = _message_id(msg)
                block_id = block.get("id")
                if (
                    isinstance(mid, str)
                    and mid
                    and isinstance(block_id, str)
                    and block_id
                ):
                    tool_key = (mid, block_id)
                    if tool_key in seen_tool_blocks:
                        continue
                    seen_tool_blocks.add(tool_key)
                _add_tool(current, totals, block.get("name") or "?")
        if ts_ms is not None:
            current["_lastTsMs"] = ts_ms

    for item in usage_by_message.values():
        target_prompt = item.get("prompt")
        if not isinstance(target_prompt, dict):
            continue
        tokens = _tokens_from_usage(item.get("usage") or {})
        _add_tokens(target_prompt["tokens"], tokens)
        _add_tokens(totals["tokens"], tokens)
        _add_model_usage(
            target_prompt,
            totals,
            item.get("model"),
            tokens,
            level="main",
            events=1,
        )

    totals["turns"] = len(prompts)
    for prompt in prompts:
        last_prompt_ts = prompt.pop("_lastTsMs", None)
        if prompt.get("tsMs") is not None and last_prompt_ts is not None:
            prompt["durationMs"] = max(0, last_prompt_ts - prompt["tsMs"])
        totals["activeMs"] += int(prompt.get("durationMs") or 0)
    if first_ts is not None and last_ts is not None:
        totals["startedAt"] = first_ts
        totals["endedAt"] = last_ts
        totals["transcriptSpanMs"] = max(0, last_ts - first_ts)

    _add_subagent_rollups(rec, prompts, totals, stats)
    totals["subagents"]["tokens"] = dict(totals["subagents"]["tokens"])

    return {
        "analyticsSchemaVersion": ANALYTICS_SCHEMA_VERSION,
        "generatedAt": int(time.time() * 1000),
        "short": rec.get("short"),
        "sessionId": rec.get("sessionId"),
        "name": rec.get("name") or rec.get("short"),
        "cwd": rec.get("cwd"),
        "totals": totals,
        "prompts": prompts,
        "stats": stats,
    }
