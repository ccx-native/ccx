"""Promptless Claude Code usage and model control protocol; no credential reads."""

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


def _control_exchange(requests: list[dict], timeout: float, cwd: str | None = None) -> dict:
    """Send a list of requests through one bounded promptless child.

    Initialize replies are projected immediately: the full response can also
    contain personal account data, which must never leave this transport.
    """
    timeout = max(0.1, float(timeout))
    deadline = time.monotonic() + timeout
    ids = [(uuid.uuid4().hex, request) for request in requests]
    target_id = ids[-1][0]
    subtypes = {rid: request["subtype"] for rid, request in ids}
    child = None
    replies: dict[str, dict] = {}
    seen_target = False
    invalid_line = None
    total_bytes = 0
    buffer = b""
    problem = None
    try:
        child = subprocess.Popen(
            [CLAUDE_BIN, *_PROBE_ARGS], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, cwd=cwd,
        )
        for rid, request in ids:
            message = {"type": "control_request", "request_id": rid, "request": request}
            child.stdin.write((json.dumps(message, separators=(",", ":")) + "\n").encode())
        child.stdin.flush()

        while time.monotonic() < deadline and not seen_target and problem is None:
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
                rid = response.get("request_id")
                if rid not in subtypes:
                    continue
                subtype = subtypes[rid]
                if subtype == "initialize" and response.get("subtype") == "error":
                    problem = "Claude usage initialize control request failed"
                    break
                if subtype == "initialize":
                    body = response.get("response")
                    # Do not retain the rest of initialize (notably 'account').
                    safe_body = ({
                        "models": body.get("models"),
                        "claude_code_version": body.get("claude_code_version"),
                    } if isinstance(body, dict) else None)
                    message = {
                        "type": "control_response",
                        "response": {
                            "request_id": rid,
                            "subtype": response.get("subtype"),
                            "response": safe_body,
                        },
                    }
                replies[subtype] = message
                if rid == target_id:
                    seen_target = True
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
        if not seen_target:
            if problem is None:
                problem = "Claude usage control returned no matching response"
            if invalid_line is not None:
                return {"kind": "unparsed", "raw": {"stdout_text": invalid_line},
                        "error": problem}
            return {"kind": "error", "error": problem}
        if problem is not None or exit_code not in (0, None):
            return {"kind": "error", "error": problem or "Claude usage control exited unsuccessfully"}
        return {"kind": "success", "responses": replies}
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


def request_usage(timeout: float = 8.0) -> dict:
    """Return {kind, body?, raw?, error?} from one bounded get_usage exchange.

    The native CLI owns auth. The only input consists of initialize/get_usage
    control requests; no model prompt, hooks, MCP server or transcript scan.
    """
    result = _control_exchange([
        {"subtype": "initialize"},
        {"subtype": "get_usage", "skip_behaviors": True},
    ], timeout)
    if result["kind"] != "success":
        return result
    reply = result["responses"]["get_usage"]
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


def request_models(timeout: float = 8.0) -> dict:
    """Return only Claude's model entries and version from initialize."""
    # From home, so one project's settings can't shape the shared catalog.
    result = _control_exchange([{"subtype": "initialize"}], timeout,
                               cwd=os.path.expanduser("~"))
    if result["kind"] != "success":
        # Do not return raw stdout; initialize may include private account data.
        return {"kind": "error", "error": result.get("error") or "Claude model probe failed"}
    response = result["responses"]["initialize"]["response"]
    if response.get("subtype") != "success":
        return {"kind": "error", "error": "Claude model initialize failed"}
    body = response.get("response")
    if not isinstance(body, dict) or not isinstance(body.get("models"), list):
        return {"kind": "error", "error": "Claude model initialize returned no models"}
    return {"kind": "success", "body": body}
