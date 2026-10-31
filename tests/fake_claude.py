"""Minimal stand-in for ``claude`` under a real ``ccx host`` PTY.

It fires parent hooks straight at the host socket the way the hooks relay
would, so host-loop tests exercise the real select loop, journals and PTY
writes without a model. Behaviour knobs (``FAKE_MODE``, comma-separated):

* ``nostop``      never send Stop after a submitted prompt;
* ``render_hold`` keep the initial idle PTY rendering until FAKE_RENDER_GATE;
* ``ignore_term`` ignore SIGTERM/SIGHUP and outlive a closed PTY.
* ``FAKE_HOOKS_VIA_SETTINGS=1`` execute original generated hook commands,
  instead of sending directly to the host socket (opt-in integration tests).
* ``compact_hooks`` (generated settings only) emit real /compact UPS and Stop;
  ``compact_late_stop`` delays Stop until a test release file appears, and
  ``compact_queued_prompt`` starts an ordinary turn before compact settles.

Every byte the host writes to the PTY is appended to ``FAKE_LOG``.
"""

import json
import os
from pathlib import Path
from datetime import datetime, timezone
import re
import select
import shlex
import subprocess
import signal
import socket
import sys
import time
import tty

# The old harness uses CCX_SELF_SHORT/sock. Managed hosts bind an exact
# full-UUID socket, which can be relocated to a short private runtime base
# when the real CCX_ROOT is too deep for Darwin's 104-byte sun_path.
# This fake asks CCX's READ-ONLY transport planner for the same bound endpoint;
# it never discovers an unrelated worker or launches a real provider.
WORKER_ID = os.environ.get("CCX_SELF_WORKER_ID")
if WORKER_ID:
    from ccx.store.root import require_ccx_active
    from ccx.rpc.transport import plan_transport

    store = require_ccx_active(Path(os.environ["CCX_ROOT"]))
    SOCK = str(plan_transport(store, target="claude:" + WORKER_ID).final)
else:
    SHORT = os.environ["CCX_SELF_SHORT"]
    SOCK = os.path.join(os.environ["CCX_ROOT"], "sock", SHORT + ".sock")
MODES = set(filter(None, os.environ.get("FAKE_MODE", "").split(",")))
LOG = os.environ.get("FAKE_LOG")
# The fake provider copies the exact native session argv issued by the real
# host. This is provider evidence, not an alias for CCX's full worker UUID.
SESSION_ID = None
for opt in ("--session-id", "--resume"):
    if opt in sys.argv[1:]:
        index = sys.argv.index(opt)
        if index + 1 < len(sys.argv):
            SESSION_ID = sys.argv[index + 1]
        break


VIA_SETTINGS = os.environ.get("FAKE_HOOKS_VIA_SETTINGS") == "1"
SETTINGS = None
NATIVE_JSONL = None


def fake_pid_domain() -> str:
    """Provider-shaped OS tag, not an assertion about this fake's machine ID.

    Real Linux Claude includes the machine and PID-namespace details; the
    OS-layer reader intentionally checks only the first (OS) component.
    """
    if sys.platform == "darwin":
        return "darwin"
    return "linux:9c1c63c55c164e25b6cab9ecf2d91080:pid:[4026532223]"


if os.environ.get("FAKE_NATIVE_SESSION_METADATA") == "1":
    # Explicit opt-in: never synthesize a pid file in a real provider home.
    # Only write inside a named, disposable test CLAUDE_CONFIG_DIR.
    home_text = os.environ.get("CLAUDE_CONFIG_DIR")
    if not home_text or not SESSION_ID:
        raise RuntimeError("fake native session metadata needs disposable home and session ID")
    from ccx.system import oslayer

    observed = oslayer.process_info(os.getpid())
    if observed.presence != "alive" or not observed.start_token:
        raise RuntimeError("fake native session lacks its own kernel start")
    start = observed.start_token
    if sys.platform == "darwin":
        seconds = int(start.split(":")[1])
        start = datetime.fromtimestamp(
            seconds, timezone.utc).strftime("%a %b %d %H:%M:%S %Y")
    sessions = Path(home_text) / "sessions"
    sessions.mkdir(mode=0o700, parents=True, exist_ok=True)
    (sessions / f"{os.getpid()}.json").write_text(json.dumps({
        "pid": os.getpid(),
        "sessionId": SESSION_ID,
        "kind": "interactive",
        "status": "idle",
        "statusUpdatedAt": int(time.time() * 1000),
        "procStart": start,
        "pidDomain": fake_pid_domain(),
    }))
if VIA_SETTINGS:
    idx = sys.argv.index("--settings")
    SETTINGS = json.loads(Path(sys.argv[idx + 1]).read_text(encoding="utf-8"))
    explicit_home = os.environ.get("CLAUDE_CONFIG_DIR")
    if not explicit_home:
        raise RuntimeError("fake generated hooks require a disposable CLAUDE_CONFIG_DIR")
    home = Path(explicit_home)
    slug = re.sub(r"[^a-zA-Z0-9]", "-", os.getcwd())
    parent = home / "projects" / slug
    for directory in (home, home / "projects", parent):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    NATIVE_JSONL = parent / (SESSION_ID + ".jsonl")


def native_row(event):
    if not VIA_SETTINGS:
        return
    # Fake native source is owner-private from CREATE, even under umask 0002.
    fd = os.open(NATIVE_JSONL, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as out:
        out.write(json.dumps(event) + "\n")


def hook(event, data=None):
    payload = dict(data or {})
    if VIA_SETTINGS:
        payload.setdefault("session_id", SESSION_ID)
        payload.setdefault("transcript_path", str(NATIVE_JSONL))
        command = SETTINGS["hooks"][event][0]["hooks"][0]["command"]
        result = subprocess.run(
            shlex.split(command), input=json.dumps(payload), text=True,
            capture_output=True, timeout=20,
        )
        if log := os.environ.get("FAKE_HOOK_RESULT_LOG"):
            with open(log, "a", encoding="utf-8") as results:
                results.write(json.dumps({"event": event, "rc": result.returncode,
                                          "stdout": result.stdout,
                                          "stderr": result.stderr[-500:]}) + "\n")
        if event == "UserPromptSubmit" and "ups_blocked" in MODES:
            time.sleep(12)  # original UPS acknowledged but provider makes no user/Stop row
        return result
    msg = {"op": "hook-event", "event": event, "data": payload}
    reply_log = os.environ.get("FAKE_HOOK_RESULT_LOG")
    last_error = None
    for _ in range(400):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(10)
            try:
                s.connect(SOCK)
            except OSError as exc:
                # Only a connection failure is safe to retry. Once the fake
                # submits a hook, a missing response must never replay it.
                last_error = exc.__class__.__name__
                time.sleep(0.025)
                continue
            s.sendall((json.dumps(msg) + "\n").encode())
            # Keep the synthetic hook connection open until the host has
            # authenticated its original PID/start and returned a reply.
            # Otherwise Darwin may observe an already-closed relay as no
            # longer descended from the recorded worker process.
            if event in ("SessionStart", "UserPromptSubmit", "Stop",
                         "PostToolUse", "SubagentStop"):
                response = s.recv(4096).split(b"\n", 1)[0]
                if reply_log:
                    with open(reply_log, "a", encoding="utf-8") as result:
                        result.write(json.dumps({
                            "event": event,
                            "reply": response.decode("utf-8", "replace")[:600],
                        }) + "\n")
                try:
                    accepted = json.loads(response).get("ok") is True
                except (ValueError, AttributeError):
                    accepted = False
                if not accepted:
                    raise RuntimeError("synthetic native hook was not acknowledged")
            return
    if reply_log:
        with open(reply_log, "a", encoding="utf-8") as result:
            result.write(json.dumps({
                "event": event, "unreachable": last_error,
            }) + "\n")


def linger():
    while True:
        time.sleep(3600)


if "ignore_term" in MODES:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
try:
    tty.setraw(0)
except Exception:
    pass
os.write(1, b"> ready\r\n")
hook("SessionStart", {"session_id": SESSION_ID} if SESSION_ID else {})
if "render_hold" in MODES:
    # Keep Ink unready while the test submits a second strict delivery against
    # the first one's owned-but-unwritten input surface.
    gate = Path(os.environ["FAKE_RENDER_GATE"])
    until = time.monotonic() + 30
    while not gate.exists() and time.monotonic() < until:
        os.write(1, b"\r> rendering\r")
        time.sleep(.1)
    if not gate.exists():
        raise RuntimeError("fake initial render gate never released")
buf = b""
while True:
    try:
        ready, _, _ = select.select([0], [], [], 0.5)
    except InterruptedError:
        continue
    if not ready:
        continue
    try:
        data = os.read(0, 65536)
    except OSError:
        data = b""
    if not data:
        if "ignore_term" in MODES:
            linger()
        sys.exit(0)
    if LOG:
        with open(LOG, "ab") as f:
            f.write(data)
    if "echo_attached_probe" in MODES and b"CCX_SETTLE_SERVICE_" in data:
        # Proves both the owned host's PTY write AND normal PTY read/fanout
        # continue while a native-idle cancellation observer is armed.
        os.write(1, b"FANOUT_SETTLE_SERVICE_OK\r\n")
    if "manual_interrupt_marker" in MODES and data == b"\x1b":
        # The test fake models the documented native marker after a human
        # Escape; a delayed Stop must not settle the same turn twice.
        native_row({"type": "user", "promptId": "fake-active-prompt",
                    "interruptedMessageId": "fake-active-assistant",
                    "message": {"content": "[Request interrupted by user]"}})
        data = data.replace(b"\x1b", b"")
        if "manual_interrupt_late_stop" in MODES:
            time.sleep(2.3)  # wait for the passive transcript polling tick
            hook("Stop", {"prompt_id": "fake-active-prompt",
                          "last_assistant_message": "stale Stop"})
    buf += data
    while b"\r" in buf:
        raw, buf = buf.split(b"\r", 1)
        text = raw.replace(b"\x1b[200~", b"").replace(b"\x1b[201~", b"")
        text = text.replace(b"\x03", b"").replace(b"\x15", b"").decode("utf-8", "replace")
        if not text.strip():
            continue
        if text.strip() == "/compact":
            if VIA_SETTINGS and "compact_hooks" in MODES:
                hook("UserPromptSubmit", {"prompt": text})
                if "compact_late_stop" not in MODES:
                    hook("Stop", {"last_assistant_message": "compact hook output"})
                queued = "compact_queued_prompt" in MODES
                native_row({"type": "user", "message": {
                    "content": "<command-name>/compact</command-name>"}})
                if queued:
                    queue_gate = Path(os.environ["FAKE_COMPACT_HOOK_QUEUE"])
                    deadline = time.monotonic() + 20
                    while not queue_gate.exists() and time.monotonic() < deadline:
                        time.sleep(0.05)
                    if not queue_gate.exists():
                        raise RuntimeError("compact submitted gate not received")
                    hook("UserPromptSubmit", {"prompt": "queued model after compact"})
                native_row({"type": "system", "subtype": "compact_boundary",
                            "uuid": "fake-compact-generated-hook-boundary",
                            "content": "Conversation compacted",
                            "compactMetadata": {"trigger": "manual", "preTokens": 1234,
                                                "postTokens": 100, "durationMs": 43}})
                if queued:
                    # Native model user history may lag its UPS; keep the
                    # command and terminal compact boundary adjacent.
                    native_row({"type": "user", "message": {"content": "queued model after compact"}})
                if queued or "compact_late_stop" in MODES:
                    release = os.environ["FAKE_COMPACT_HOOK_RELEASE"]
                    deadline = time.monotonic() + 20
                    while not Path(release).exists() and time.monotonic() < deadline:
                        time.sleep(0.05)
                    if not Path(release).exists():
                        raise RuntimeError("compact hook test release not received")
                    answer = "Fake queued model completed" if queued else "late compact Stop"
                    if queued:
                        native_row({"type": "assistant", "message": {
                            "model": "fake-claude-generated-hooks-model",
                            "content": [{"type": "text", "text": answer}]}})
                    hook("Stop", {"last_assistant_message": answer})
            continue
        if text.strip() == "successor UPS during observed idle grace":
            # The original A is an accepted CCX send, not an unrelated fake
            # user turn: preserve the original prompt for receipt attribution.
            hook("UserPromptSubmit", {"prompt": text, "prompt_id": "prompt-A"})
            native_row({"type": "user", "message": {"content": text}})
            hook("PreToolUse", {"tool_name": "Read", "tool_use_id": "observed-A",
                                "prompt_id": "prompt-A"})
            ups_gate = Path(os.environ["FAKE_SUCCESSOR_UPS_GATE"])
            until = time.monotonic() + 15
            while not ups_gate.exists() and time.monotonic() < until:
                time.sleep(.04)
            if not ups_gate.exists():
                raise RuntimeError("new successor UPS was never released")
            hook("UserPromptSubmit", {"prompt": "observed successor B", "prompt_id": "prompt-B"})
            native_row({"type": "user", "message": {"content": "observed successor B"}})
            stop_gate = Path(os.environ["FAKE_SUCCESSOR_STOP_GATE"])
            until = time.monotonic() + 15
            while not stop_gate.exists() and time.monotonic() < until:
                time.sleep(.04)
            if not stop_gate.exists():
                raise RuntimeError("new successor Stop was never released")
            native_row({"type": "assistant", "message": {"content": [
                {"type": "text", "text": "native B done"}]}})
            hook("Stop", {"prompt_id": "prompt-B", "last_assistant_message": "native B done"})
            continue
        if text.strip() == "UPS successor before any native idle is observable":
            # An overwritten idle edge is not evidence. Two UPS identities may
            # remain on one active ref until a native terminal hook appears.
            hook("UserPromptSubmit", {"prompt": "unobserved original A", "prompt_id": "prompt-A"})
            hook("UserPromptSubmit", {"prompt": "unobserved successor B", "prompt_id": "prompt-B"})
            gate = Path(os.environ["FAKE_SUCCESSOR_STOP_GATE"])
            until = time.monotonic() + 16
            while not gate.exists() and time.monotonic() < until:
                time.sleep(.04)
            if not gate.exists():
                raise RuntimeError("unobserved-successor Stop gate never released")
            hook("Stop", {"prompt_id": "prompt-B", "last_assistant_message": "B after no native idle"})
            continue
        if text.strip() == "old prompt Stop arrives after successor UPS":
            # Source-authenticated parent hooks exercise marker(A), UPS(B),
            # Stop(A), Stop(B). A new Stop must not be stolen by A's guard.
            from ccx.rpc.rpc import _rpc
            hook("UserPromptSubmit", {"prompt": "original A", "prompt_id": "prompt-A"})
            native_row({"type": "user", "message": {"content": "original A"}})
            hook("PreToolUse", {"tool_name": "Read", "tool_use_id": "A-tool",
                                "prompt_id": "prompt-A"})
            native_row({"type": "user", "promptId": "prompt-A",
                        "interruptedMessageId": "assistant-A",
                        "message": {"content": "[Request interrupted by user]"}})
            end = time.monotonic() + 12
            while time.monotonic() < end:
                snap = _rpc("claude:" + WORKER_ID, {"op": "status"}, timeout=.8)
                if snap.get("turns") == 1 and snap.get("state") == "idle":
                    break
                time.sleep(.04)
            else:
                raise RuntimeError("original marker did not settle before successor")
            hook("UserPromptSubmit", {"prompt": "successor B", "prompt_id": "prompt-B"})
            native_row({"type": "user", "message": {"content": "successor B"}})
            hook("PreToolUse", {"tool_name": "Read", "tool_use_id": "B-tool",
                                "prompt_id": "prompt-B"})
            hook("Stop", {"prompt_id": "prompt-A", "last_assistant_message": "late A"})
            gate = Path(os.environ["FAKE_SUCCESSOR_STOP_GATE"])
            deadline = time.monotonic() + 16
            while not gate.exists() and time.monotonic() < deadline:
                time.sleep(.04)
            if not gate.exists():
                raise RuntimeError("successor B Stop gate never released")
            native_row({"type": "assistant", "message": {"content": [
                {"type": "text", "text": "actual B reply"}]}})
            hook("Stop", {"prompt_id": "prompt-B", "last_assistant_message": "actual B reply"})
            continue
        hook("UserPromptSubmit", {"prompt": text})
        native_row({"type": "user", "message": {"content": text}})
        if VIA_SETTINGS:
            if "late_stop_gate" in MODES:
                import threading
                def delayed_stop():
                    gate = Path(os.environ["FAKE_LATE_STOP_GATE"])
                    deadline = time.monotonic() + 15
                    while not gate.exists() and time.monotonic() < deadline:
                        time.sleep(.03)
                    if gate.exists():
                        hook("Stop", {"prompt_id": "fake-active-prompt",
                                      "last_assistant_message": "late original Stop"})
                threading.Thread(target=delayed_stop, daemon=True).start()
            hook("PreToolUse", {"tool_name": "Read", "tool_use_id": "generated-tool-1",
                                **({"prompt_id": "fake-active-prompt"}
                                   if "manual_interrupt_marker" in MODES else {})})
            if text.strip() == "ask generated question":
                hook("PermissionRequest", {
                    "tool_name": "AskUserQuestion",
                    "tool_input": {"questions": [{"question": "Which option?", "header": "Choice",
                                                 "options": [{"label": "Yes", "description": "Continue"},
                                                             {"label": "No", "description": "Decline"}],
                                                 "multiSelect": False}]},
                })
            elif text.strip() == "request generated permission":
                hook("PermissionRequest", {
                    "tool_name": "Bash", "tool_input": {"command": "pwd"},
                    "permission_message": "Allow one fake command?",
                })
            elif text.strip() in ("native permission completion", "native permission failure"):
                # Simulate a native terminal answer completing while the
                # actual PermissionRequest relay is still blocked. The parent
                # fake Claude (not the test process) emits both real hooks.
                import threading
                from ccx.rpc.rpc import _rpc
                request = {"tool_name": "Bash",
                           "tool_input": {"command": "pwd", "full": {"nested": "original"}},
                           "permission_message": "Native decision or external ccx approval?"}
                capture = []
                def held_permission():
                    capture.append(hook("PermissionRequest", request))
                hold = threading.Thread(target=held_permission, daemon=True)
                hold.start()
                until = time.monotonic() + 10
                while time.monotonic() < until:
                    st = _rpc("claude:" + WORKER_ID, {"op": "status"}, timeout=.5)
                    if (st.get("waitingFor") or {}).get("tool") == "Bash":
                        break
                    time.sleep(.05)
                else:
                    raise RuntimeError("fake native permission never became pending")
                failing = text.strip() == "native permission failure"
                hook("PostToolUseFailure" if failing else "PostToolUse", {
                    "tool_name": "Bash", "tool_input": request["tool_input"],
                    "tool_response": {"is_error": failing},
                })
                hold.join(timeout=10)
                if hold.is_alive() or not capture:
                    raise RuntimeError("native completion never released relay")
                if result_log := os.environ.get("FAKE_NATIVE_RECONCILE_LOG"):
                    with open(result_log, "a", encoding="utf8") as dest:
                        dest.write(json.dumps({"failure": failing,
                                               "stdout": capture[0].stdout,
                                               "rc": capture[0].returncode}) + "\n")
            elif text.strip() == "terminal-only permission prompt":
                # Claude's own safety checks prompt in the terminal with no
                # PermissionRequest hook; only the Notification reaches CCX.
                hook("Notification", {"notification_type": "permission_prompt",
                                      "message": "Claude needs your permission to use Bash"})
                gate = Path(os.environ["FAKE_LOG"]).parent / "terminal-prompt-gate"
                deadline = time.monotonic() + 15
                while not gate.exists() and time.monotonic() < deadline:
                    time.sleep(.04)
                hook("PreToolUse", {"tool_name": "Bash", "tool_use_id": "after-terminal-prompt"})
                hook("PostToolUse", {"tool_name": "Bash", "tool_use_id": "after-terminal-prompt",
                                     "tool_response": {}})
            elif text.strip() == "request generated elicitation":
                hook("Elicitation", {
                    "mcp_server_name": "fake-mcp", "mode": "form",
                    "message": "Choose a title",
                    "requested_schema": {"type": "object",
                                         "properties": {"title": {"type": "string"}},
                                         "required": ["title"]},
                })
        if "nostop" not in MODES:
            # Emulate the native Stop hook's original provider session source
            # and final assistant message. No provider-owned transcript is
            # written outside the disposable fake's own filesystem.
            answer = "Fake Claude completed: " + text
            os.write(1, (answer + "\r\n").encode())
            native_row({"type": "assistant", "message": {
                "model": "fake-claude-generated-hooks-model",
                "content": [{"type": "text", "text": answer}],
                "usage": {"input_tokens": 1234, "output_tokens": 16},
            }})
            time.sleep(0.3)
            hook("Stop", {
                "session_id": (
                    "ffffffff-ffff-4fff-8fff-ffffffffffff"
                    if "wrong_session" in MODES else SESSION_ID
                ),
                "last_assistant_message": (
                    answer if "missing_assistant" not in MODES else None
                ),
            } if SESSION_ID else {})
