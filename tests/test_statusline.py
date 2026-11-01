"""Managed Claude statusLine bridge: exact full-UUID owner and advisory reads."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import stat
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from ccx import constants
from ccx.cli import statusline as S
from ccx.constants import SELF_ENGINE_ENV, SELF_WORKER_ID_ENV
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import _write_json, require_ccx_active
from ccx.workers import WorkerClient
from test_generation_store import A, B, fixture_root


@pytest.fixture
def active_relay(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    # The bridge now invokes the effective user statusline. Tests must NEVER
    # read/execute a real operator profile's configured statusline command.
    profile = tmp_path / "isolated-empty-claude-profile"
    profile.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CLAUDE_HOME", profile)
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(profile))
    store = require_ccx_active(root)
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    monkeypatch.setenv(SELF_WORKER_ID_ENV, A)
    monkeypatch.setenv(SELF_ENGINE_ENV, "claude")
    return store


def _payload(store, *, tokens=85000, model="claude-opus-statusline"):
    return {
        "session_id": store.record(A)["native"]["sessionId"],
        "version": "2.1.132",
        "model": {"id": model, "display_name": "Opus"},
        "context_window": {
            "used_percentage": 42.5,
            "total_input_tokens": tokens,
            "total_output_tokens": 100,
            "context_window_size": 200000,
        },
        "exceeds_200k_tokens": False,
        "cwd": "/private/large-payload-not-persisted",
    }


def _invoke_relay(monkeypatch, payload, target=A):
    monkeypatch.setattr(S.sys, "stdin", SimpleNamespace(
        read=lambda limit: json.dumps(payload) if isinstance(payload, dict) else payload,
    ))
    S.cmd_statusline_relay(SimpleNamespace(short=target))


def test_statusline_relay_writes_normalized_sidecar(active_relay, monkeypatch, capsys):
    store = active_relay
    _invoke_relay(monkeypatch, _payload(store))
    assert capsys.readouterr().out == ""
    doc = store.read_worker_sidecar(A, "statusline.json")
    assert doc["short"] == A
    assert doc["sessionId"] == store.record(A)["native"]["sessionId"]
    assert doc["model"]["id"] == "claude-opus-statusline"
    assert doc["contextWindow"]["used_percentage"] == 42.5
    assert doc["exceeds200k"] is False
    assert "cwd" not in doc
    path = store.worker_path(A, "statusline.json")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert not list(path.parent.glob(".new-*"))
    assert not (constants.CCX_ROOT / "run").exists()
    # No completed native assistant turn: valid bound statusline is still useful.
    before = (path.read_bytes(), path.stat().st_mtime_ns)
    client = WorkerClient()
    summary = client.summaries([A])["workers"][0]
    assert summary["context"]["state"] == "known"
    assert summary["context"]["tokens_used"] == 85000
    assert summary["observed_model"] == "claude-opus-statusline"
    context = client.context(A)
    assert context["tokens_used"] == 85000 and context["reason"] is None
    assert context["window_size"] is None
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


@pytest.mark.parametrize("raw", ["{not json", " " * 33000, "[]"])
def test_statusline_relay_swallows_malformed_stdin(active_relay, monkeypatch, capsys, raw):
    _invoke_relay(monkeypatch, raw)
    assert capsys.readouterr().out == ""
    assert active_relay.read_worker_sidecar(A, "statusline.json") is None


@pytest.mark.parametrize("reason", ["wrong-native", "wrong-engine", "wrong-worker", "prefix", "cross-engine"])
def test_statusline_refuses_wrong_original_identity(active_relay, monkeypatch, capsys, reason):
    doc = _payload(active_relay)
    target = A
    if reason == "wrong-native":
        doc["session_id"] = B
    elif reason == "wrong-engine":
        monkeypatch.setenv(SELF_ENGINE_ENV, "codex")
    elif reason == "wrong-worker":
        monkeypatch.setenv(SELF_WORKER_ID_ENV, B)
    elif reason == "prefix":
        target = A[:8]
    else:
        target = f"codex:{A}"
    _invoke_relay(monkeypatch, doc, target=target)
    assert capsys.readouterr().out == ""
    assert active_relay.read_worker_sidecar(A, "statusline.json") is None
    assert not (constants.CCX_ROOT / "run").exists()


def _publish_observation(store, context, model="assistant-real-model"):
    doc = {
        "schemaVersion": 1, "workerId": A,
        "nativeSessionId": store.record(A)["native"]["sessionId"],
        "generation": store.generation,
        "source": {"dev": 1, "ino": 2, "endOffset": 10, "observedAtMs": 10},
        "context": context, "observedModel": model,
    }
    with store.lease():
        with store._gen("workers", "claude", A) as fd:
            _write_json(fd, "context-observation.json", doc)


@pytest.mark.parametrize("reason", ["compacting", "just_compacted"])
def test_statusline_context_obeys_compact_fence_and_observed_model(active_relay, monkeypatch, reason):
    store = active_relay
    _publish_observation(store, {
        "state": "unknown", "reason": reason, "tokens_used": None,
        "as_of_turn_ref": None, "as_of_ms": 10,
    })
    _invoke_relay(monkeypatch, _payload(store, tokens=99000))
    client = WorkerClient()
    row = client.summaries([A])["workers"][0]
    assert row["context"]["state"] == "unknown"
    assert row["context"]["reason"] == reason
    assert row["context"]["tokens_used"] is None
    assert row["observed_model"] == "assistant-real-model"
    assert client.context(A)["reason"] == reason
    assert client.context(A)["tokens_used"] is None


@pytest.mark.parametrize("age_ms", [-(90_000 + 500), 10_000])
def test_statusline_expired_or_future_does_not_override_native(active_relay, monkeypatch, age_ms):
    store = active_relay
    _publish_observation(store, {
        "state": "known", "tokens_used": 35, "as_of_turn_ref": None,
        "as_of_ms": 30,
    }, model="genuine-assistant")
    _invoke_relay(monkeypatch, _payload(store, tokens=99000))
    doc = store.read_worker_sidecar(A, "statusline.json")
    doc["writtenAtMs"] = int(time.time() * 1000) + age_ms
    with store.lease():
        with store._gen("workers", "claude", A) as fd:
            _write_json(fd, "statusline.json", doc)
    assert WorkerClient().summaries([A])["workers"][0]["context"]["tokens_used"] == 35
    assert WorkerClient().context(A)["tokens_used"] == 35


def test_statusline_cli_full_uuid_active_only(active_relay, tmp_path):
    root = constants.CCX_ROOT
    payload = _payload(active_relay, tokens=77221)
    env = {**os.environ, "CCX_ROOT": str(root),
           SELF_WORKER_ID_ENV: A, SELF_ENGINE_ENV: "claude"}
    cli = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "statusline-relay", A],
        input=json.dumps(payload), capture_output=True, text=True, env=env,
        timeout=15, cwd=str(Path(__file__).resolve().parent.parent),
    )
    assert cli.returncode == 0, (cli.stdout, cli.stderr)
    assert cli.stdout == ""
    assert WorkerClient().context(A)["tokens_used"] == 77221
    for bad_root in (tmp_path / "missing", fixture_root(tmp_path / "verify", phase="VERIFY_ONLY")):
        env["CCX_ROOT"] = str(bad_root)
        denied = subprocess.run(
            [sys.executable, "-B", "-m", "ccx", "statusline-relay", A],
            input=json.dumps(payload), capture_output=True, text=True,
            env=env, timeout=15, cwd=str(Path(__file__).resolve().parent.parent),
        )
        assert denied.returncode == 3
        assert not (bad_root / "run").exists()
    assert not (root / "sessions").exists()


def _install_statusline_command(path: Path, command: str | None):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"statusLine": (
        {"type": "command", "command": command} if command is not None else None
    )}), encoding="utf-8")


def test_statusline_passes_exact_stdin_user_project_local_precedence(
    active_relay, tmp_path, monkeypatch, capsys,
):
    monkeypatch.setattr(constants, "CLAUDE_HOME", tmp_path / "test-claude-profile")
    cwd = Path(active_relay.record(A)["cwd"])
    user = constants.CLAUDE_HOME / "settings.json"
    project = cwd / ".claude" / "settings.json"
    local = cwd / ".claude" / "settings.local.json"
    capture = tmp_path / "statusline-stdin.json"
    runner = tmp_path / "render-statusline.py"
    runner.write_text("import sys\nfrom pathlib import Path\n"
                      "Path(sys.argv[1]).write_text(sys.stdin.read())\n"
                      "print(sys.argv[2])\n", encoding="utf-8")
    command = lambda marker: (f"{shlex.quote(sys.executable)} {shlex.quote(str(runner))} "
                              f"{shlex.quote(str(capture))} {shlex.quote(marker)}")
    _install_statusline_command(user, command("USER-LINE"))
    _invoke_relay(monkeypatch, _payload(active_relay))
    assert capsys.readouterr().out == "USER-LINE\n"
    assert json.loads(capture.read_text()) == _payload(active_relay)
    _install_statusline_command(project, command("PROJECT-LINE"))
    _invoke_relay(monkeypatch, _payload(active_relay))
    assert capsys.readouterr().out == "PROJECT-LINE\n"
    _install_statusline_command(local, command("LOCAL-LINE"))
    _invoke_relay(monkeypatch, _payload(active_relay))
    assert capsys.readouterr().out == "LOCAL-LINE\n"
    # A higher-priority explicit disable overrides the lower settings layer.
    _install_statusline_command(local, None)
    _invoke_relay(monkeypatch, _payload(active_relay))
    assert capsys.readouterr().out == ""
    assert active_relay.read_worker_sidecar(A, "statusline.json")["model"]["id"] == "claude-opus-statusline"


def test_statusline_missing_malformed_and_noncommand_config_is_blank(
    active_relay, tmp_path, monkeypatch, capsys,
):
    monkeypatch.setattr(constants, "CLAUDE_HOME", tmp_path / "no-user-profile")
    config = Path(active_relay.record(A)["cwd"]) / ".claude" / "settings.json"
    _invoke_relay(monkeypatch, _payload(active_relay))
    assert capsys.readouterr().out == ""
    config.parent.mkdir(exist_ok=True)
    for value in ("{malformed", '{"statusLine": {"type":"none"}}',
                  '{"statusLine": {"type":"command", "command":""}}'):
        config.write_text(value)
        _invoke_relay(monkeypatch, _payload(active_relay))
        assert capsys.readouterr().out == ""


def test_statusline_timeout_and_failed_command_never_break_sidecar(
    active_relay, tmp_path, monkeypatch, capsys,
):
    monkeypatch.setattr(constants, "CLAUDE_HOME", tmp_path / "no-user-profile")
    config = Path(active_relay.record(A)["cwd"]) / ".claude" / "settings.json"
    for cmd in (f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(10)'",
                f"{shlex.quote(sys.executable)} -c 'import sys; print(123); sys.exit(2)'"):
        _install_statusline_command(config, cmd)
        t0 = time.monotonic()
        _invoke_relay(monkeypatch, _payload(active_relay))
        elapsed = time.monotonic() - t0
        assert capsys.readouterr().out == ""
        assert elapsed < 4.0
    assert active_relay.read_worker_sidecar(A, "statusline.json")["sessionId"] == _payload(active_relay)["session_id"]


def test_statusline_wrong_owner_must_not_invoke_configured_command(
    active_relay, tmp_path, monkeypatch, capsys,
):
    monkeypatch.setattr(constants, "CLAUDE_HOME", tmp_path / "no-user-profile")
    marker = tmp_path / "statusline-untrusted-command-ran"
    _install_statusline_command(
        Path(active_relay.record(A)["cwd"]) / ".claude" / "settings.json",
        f"{shlex.quote(sys.executable)} -c 'from pathlib import Path; Path({str(marker)!r}).touch()'",
    )
    _invoke_relay(monkeypatch, {**_payload(active_relay), "session_id": B})
    assert capsys.readouterr().out == ""
    assert not marker.exists()


def test_statusline_timeout_kills_entire_owned_process_group(
    active_relay, tmp_path, monkeypatch, capsys,
):
    """A timed-out shell cannot leave an owned child for the next refresh."""
    import shlex
    import signal

    pid_path = tmp_path / "owned-statusline-child.pid"
    cfg = Path(active_relay.record(A)["cwd"]) / ".claude" / "settings.json"
    cfg.parent.mkdir(parents=True, exist_ok=True)
    cfg.write_text(json.dumps({"statusLine": {
        "type": "command",
        "command": f"sleep 30 & echo $! > {shlex.quote(str(pid_path))}; wait",
    }}))
    child_pid = None
    try:
        before = time.monotonic()
        _invoke_relay(monkeypatch, _payload(active_relay))
        elapsed = time.monotonic() - before
        assert elapsed < 4.0
        assert capsys.readouterr().out == ""
        assert pid_path.exists()
        child_pid = int(pid_path.read_text())
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            # An orphan briefly visible as a zombie is *not running* and
            # cannot consume resources or survive as an active statusline job.
            status = subprocess.run(["ps", "-p", str(child_pid), "-o", "stat="],
                                    capture_output=True, text=True, timeout=1)
            if status.returncode != 0 or status.stdout.strip().startswith("Z"):
                break
            time.sleep(.05)
        else:
            pytest.fail("statusline shell timeout left its child running")
        assert active_relay.read_worker_sidecar(A, "statusline.json")["sessionId"] == _payload(active_relay)["session_id"]
    finally:
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGKILL)  # test-owned PID only
            except ProcessLookupError:
                pass


def test_statusline_nudge_is_nonblocking_no_reply_and_bound_to_written_sidecar(
    active_relay, monkeypatch, capsys,
):
    """The native relay sends no usage itself; it only nudges the host socket."""
    import socket
    from ccx.store.paths import sock_path
    endpoint = sock_path(A)
    endpoint.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(str(endpoint))
        server.listen(1)
        server.settimeout(1)
        _invoke_relay(monkeypatch, _payload(active_relay, tokens=32100))
        assert capsys.readouterr().out == ""
        with server.accept()[0] as peer:
            peer.settimeout(1)
            notice = json.loads(peer.recv(4096).decode().strip())
        assert notice == {"op": "statusline-updated", "worker_id": A,
                          "session_id": _payload(active_relay)["session_id"]}
        doc = active_relay.read_worker_sidecar(A, "statusline.json")
        assert doc["contextWindow"]["total_input_tokens"] == 32100
    finally:
        server.close()
        endpoint.unlink(missing_ok=True)


def test_statusline_nudge_unreachable_does_not_affect_render(
    active_relay, monkeypatch, tmp_path, capsys,
):
    """No socket reply or host is ever required to render user's statusline."""
    config = Path(active_relay.record(A)["cwd"]) / ".claude" / "settings.json"
    _install_statusline_command(config, "printf 'OWNED-STATUS\\n'")
    _invoke_relay(monkeypatch, _payload(active_relay, tokens=12345))
    assert capsys.readouterr().out == "OWNED-STATUS\n"
    assert active_relay.read_worker_sidecar(A, "statusline.json")["contextWindow"]["total_input_tokens"] == 12345
