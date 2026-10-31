"""Promptless Claude Code usage control protocol; no credential or transcript reads."""

from __future__ import annotations

import json
import os
import select
import subprocess
import time
import uuid

from ccx.constants import CLAUDE_BIN

# None of these flags is --bare: --bare disables native OAuth/Keychain auth.
_PROBE_ARGS = (
    "-p", "--input-format", "stream-json", "--output-format", "stream-json",
    "--verbose", "--safe-mode", "--strict-mcp-config",
    "--no-session-persistence", "--disable-slash-commands", "--no-chrome",
    "--tools", "",
)
_MAX_OUTPUT_BYTES = 2 * 1024 * 1024
_MAX_LINE_BYTES = 1 * 1024 * 1024


def request_usage(timeout: float = 8.0) -> dict:
    """Return {kind, body?, raw?, error?} from one bounded get_usage exchange.

    The native CLI owns auth. The only input consists of initialize/get_usage
    control requests; no model prompt, hooks, MCP server or transcript scan.
    """
    timeout = max(0.1, float(timeout))
    start = time.monotonic()
    deadline = start + timeout
    init_id, usage_id = uuid.uuid4().hex, uuid.uuid4().hex
    child = None
    reply = None
    invalid_line = None
    total_bytes = 0
    buffer = b""
    problem = None
    try:
        child = subprocess.Popen(
            [CLAUDE_BIN, *_PROBE_ARGS], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        for rid, request in (
            (init_id, {"subtype": "initialize"}),
            (usage_id, {"subtype": "get_usage", "skip_behaviors": True}),
        ):
            message = {"type": "control_request", "request_id": rid, "request": request}
            child.stdin.write((json.dumps(message, separators=(",", ":")) + "\n").encode())
        child.stdin.flush()

        while time.monotonic() < deadline and reply is None and problem is None:
            readable, _, _ = select.select(
                [child.stdout], [], [], min(0.2, max(0, deadline - time.monotonic()))
            )
            if not readable:
                if child.poll() is not None:
                    break
                continue
            chunk = os.read(child.stdout.fileno(), 32768)
            if not chunk:
                break
            total_bytes += len(chunk)
            if total_bytes > _MAX_OUTPUT_BYTES:
                problem = "Claude usage control output too large"
                break
            buffer += chunk
            if len(buffer) > _MAX_LINE_BYTES and b"\n" not in buffer:
                problem = "Claude usage control line too large"
                break
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                if len(line) > _MAX_LINE_BYTES:
                    problem = "Claude usage control line too large"
                    break
                try:
                    message = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    invalid_line = line[:1000].decode("utf-8", "replace")
                    continue
                if not isinstance(message, dict) or message.get("type") != "control_response":
                    continue
                response = message.get("response")
                if not isinstance(response, dict):
                    continue
                if response.get("request_id") == init_id and response.get("subtype") == "error":
                    problem = "Claude usage initialize control request failed"
                    break
                if response.get("request_id") == usage_id:
                    reply = message
                    break
        # Always close stdin after the matching response, or on a bounded failure.
        child.stdin.close()
        child.stdin = None
        remaining = max(0.0, deadline - time.monotonic())
        try:
            exit_code = child.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            problem = problem or "Claude usage control timed out"
            exit_code = None
        if reply is None:
            if problem is None:
                problem = "Claude usage control returned no matching response"
            if invalid_line is not None:
                return {"kind": "unparsed", "raw": {"stdout_text": invalid_line},
                        "error": problem}
            return {"kind": "error", "error": problem}
        if problem is not None or exit_code not in (0, None):
            return {"kind": "error", "error": problem or "Claude usage control exited unsuccessfully"}
        response = reply["response"]
        if response.get("subtype") == "error":
            return {"kind": "error", "error": "Claude usage control response was an error"}
        if response.get("subtype") != "success":
            return {"kind": "unparsed", "raw": reply,
                    "error": "Claude usage control returned an unknown response subtype"}
        body = response.get("response")
        if not isinstance(body, dict):
            return {"kind": "unparsed", "raw": reply,
                    "error": "Claude usage control returned an unknown JSON body"}
        if body.get("rate_limits_available") is False and body.get("rate_limits") is None:
            return {"kind": "unavailable", "error": "Claude plan rate limits unavailable"}
        if body.get("rate_limits_available") is True and body.get("rate_limits") is None:
            return {"kind": "held", "error": "Claude plan usage is held by the provider"}
        rate_limits = body.get("rate_limits")
        if isinstance(rate_limits, dict) and any(k in rate_limits for k in (
            "five_hour", "seven_day", "limits", "model_scoped", "extra_usage",
            "seven_day_opus", "seven_day_sonnet",
        )):
            return {"kind": "success", "body": rate_limits}
        return {"kind": "unparsed", "raw": reply,
                "error": "Claude usage control returned an unrecognized rate_limits field"}
    except (OSError, ValueError, BrokenPipeError) as exc:
        return {"kind": "error", "error": f"Claude usage control unavailable: {type(exc).__name__}"}
    finally:
        if child is not None:
            if child.stdin is not None and not child.stdin.closed:
                child.stdin.close()
            if child.poll() is None:
                # No early SIGKILL: wait for the bounded deadline to expire first.
                try:
                    child.wait(timeout=max(0.0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    child.terminate()
                    try:
                        child.wait(timeout=1.0)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=1.0)
            if child.stdout is not None:
                child.stdout.close()
