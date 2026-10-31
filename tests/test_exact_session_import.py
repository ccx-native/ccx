"""Original exact-native Claude import: one CCX ownership and true --resume."""
from __future__ import annotations
import ccx.cli.commands.create as commands_create

import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import time
from uuid import uuid4

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx import constants
from ccx.ops import creation
from ccx.claude import external
from ccx.system import oslayer
from ccx.store import paths
from ccx.errors import CreatePartialFailure, SessionUnavailable
from ccx.store.liveness import owned_pid_alive
from ccx.store.store import fingerprint_v2
from ccx.store.root import require_ccx_active
from ccx.store.identity import mint_worker_id
from test_generation_store import A, B, G1, put, short_root


def _original(native_home: Path, cwd: Path, sid: str, *, nonempty=True):
    slug = re.sub(r"[^a-zA-Z0-9]", "-", str(cwd))
    source = native_home / "projects" / slug / f"{sid}.jsonl"
    source.parent.mkdir(parents=True, exist_ok=True)
    original = (json.dumps({"type": "user", "cwd": str(cwd),
                            "message": {"role": "user", "content": "OLD NATIVE HISTORY"}})
                + "\n").encode() if nonempty else b""
    source.write_bytes(original)
    return source, original


def _environment(short_root: Path, tmp_path: Path):
    repo = Path(__file__).resolve().parent.parent
    native_home = tmp_path / "isolated-claude-home"
    work = tmp_path / "native-project"
    work.mkdir()
    fake = repo / "tests" / "fake_claude.py"
    wrapper = tmp_path / "only-fake-claude"
    argv_log = tmp_path / "native-argv.txt"
    wrapper.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> " + shlex.quote(str(argv_log))
        + "\nexec " + shlex.quote(sys.executable) + " " + shlex.quote(str(fake))
        + ' "$@"\n', encoding="utf-8")
    wrapper.chmod(0o700)
    env = {**os.environ,
           "CCX_ROOT": str(short_root), "CLAUDE_CONFIG_DIR": str(native_home),
           "CCX_CLAUDE_BIN": str(wrapper), "CCX_SPAWN_SCOPE": "0", "CCX_BOOT_TIMEOUT_S": "15",
           "FAKE_HOOKS_VIA_SETTINGS": "1", "FAKE_MODE": "",
           "FAKE_LOG": str(tmp_path / "native-stdin.txt"),
           "FAKE_HOOK_RESULT_LOG": str(tmp_path / "hook-results.txt"),
           "PYTHONPATH": str(repo), "PYTHONDONTWRITEBYTECODE": "1",
           "CCX_REAL_PARITY": "0"}
    for field in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(field, None)

    def call(*args, timeout=26):
        return subprocess.run([sys.executable, "-B", "-m", "ccx", *args],
                              env=env, cwd=str(repo), capture_output=True,
                              text=True, timeout=timeout)

    return native_home, work, argv_log, env, call


def _stop_only_fake(short_root: Path, worker_id: str):
    try:
        rec = require_ccx_active(short_root).record(f"claude:{worker_id}")
    except Exception:
        return
    life = rec.get("lifecycle") or {}
    for pid_field, token in (("hostPid", "hostStart"), ("workerPid", "workerStart")):
        pid, start = life.get(pid_field), life.get(token)
        if owned_pid_alive(pid, start):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    until = time.monotonic() + 3
    while time.monotonic() < until and any(
            owned_pid_alive(life.get(p), life.get(k))
            for p, k in (("hostPid", "hostStart"), ("workerPid", "workerStart"))):
        time.sleep(.05)
    for pid_field, token in (("hostPid", "hostStart"), ("workerPid", "workerStart")):
        pid, start = life.get(pid_field), life.get(token)
        if owned_pid_alive(pid, start):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_exact_import_real_generated_hook_resume_and_same_confirmed_owner(short_root, tmp_path):
    native_home, cwd, argv_log, _env, call = _environment(short_root, tmp_path)
    sid = str(uuid4())
    source, original_bytes = _original(native_home, cwd, sid)
    old_stat = source.stat()
    original_wid = None
    try:
        first = call("import", sid)
        assert first.returncode in (0, 3), (first.stdout, first.stderr)
        assert "claude:" in first.stdout, (first.stdout, first.stderr)
        handle = first.stdout.strip().splitlines()[-1]
        original_wid = handle.split(":", 1)[1]
        assert original_wid != sid
        store = require_ccx_active(short_root)
        record = store.record(handle)
        assert record["native"]["sessionId"] == sid
        assert record["origin"] == "ccx-import"
        assert record["createdVia"] == "import"
        assert record["createArgs"]["skip_permissions"] is True
        assert all(record["createArgs"].get(k) is None for k in (
            "model", "effort", "agent", "append_system_prompt",
            "allowed_tools", "disallowed_tools", "permission_mode"))
        provenance = record["importedFrom"]
        assert provenance["sessionId"] == sid
        assert "mode" not in provenance  # An import is not an external worker mode.
        assert provenance["wasLive"] is False
        assert provenance["transcriptJsonl"] == str(source)
        assert provenance["preImportTranscriptSize"] == len(original_bytes)
        assert provenance["preImportTranscriptMtime"] == int(old_stat.st_mtime * 1000)
        assert record["transcript"] == str(source)
        assert store._owner(original_wid)["provenance"] == "ccx-import"
        assert store._owner(original_wid)["createIntent"]["importedFrom"] == provenance
        assert store._claim(original_wid)["nativeBinding"] == {"sessionId": sid}
        until = time.monotonic() + 12
        while time.monotonic() < until:
            if store.native_activation(handle)["state"] == "confirmed":
                break
            time.sleep(.06)
        host_log = (short_root / "identity" / "generations" / G1 / "workers" /
                    "claude" / original_wid / "host.log")
        assert store.native_activation(handle)["state"] == "confirmed", (
            first.stderr, store.record(handle).get("lifecycle"),
            host_log.read_text(errors="replace")[-8000:] if host_log.exists() else "no host log")
        proof = store.record(handle)["lifecycle"]["nativeStartProof"]
        assert proof["workerId"] == original_wid
        assert proof["sessionId"] == sid
        assert proof["nativeSource"] == "claude-session:" + sid
        assert proof["generation"] == store.generation
        assert store.record(handle)["native"]["nativeState"] == "session-bound"
        native_args = argv_log.read_text().splitlines()
        assert len(native_args) == 1, native_args
        assert "--resume " + sid in native_args[0]
        assert "--settings" in native_args[0]
        assert "--dangerously-skip-permissions" in native_args[0]
        for forbidden in ("--session-id", "--model", "--agent", "--append-system-prompt",
                          "--allowed-tools", "--disallowed-tools", "--permission-mode"):
            assert forbidden not in native_args[0], (forbidden, native_args)
        assert source.read_bytes().startswith(original_bytes)
        second = call("import", sid)
        assert second.returncode == 0, (second.stdout, second.stderr)
        assert second.stdout.strip() == handle
        assert argv_log.read_text().splitlines() == native_args
        sent = call("send", handle, "first managed post-import input",
                    "--no-wait", "--delivery-id", "import-managed-first", "--json")
        assert sent.returncode == 0, (sent.stdout, sent.stderr)
        assert json.loads(sent.stdout)["worker"] == handle
    finally:
        if original_wid:
            _stop_only_fake(short_root, original_wid)


def test_import_original_history_readable_before_and_after_managed_turn(
    short_root, tmp_path, monkeypatch,
):
    from ccx.store.root import CreationConflict
    from ccx.workers import WorkerClient

    native_home, cwd, argv_log, _env, call = _environment(short_root, tmp_path)
    sid = str(uuid4())
    source, _ = _original(native_home, cwd, sid)
    with source.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({
            "type": "assistant", "sessionId": sid,
            "message": {"role": "assistant", "stop_reason": "end_turn",
                        "content": [{"type": "text", "text": "NATIVE ORIGINAL FINAL"}]},
        }) + "\n")
    source.chmod(0o600)
    native_before = source.stat()
    original_bytes = source.read_bytes()
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setattr(paths, "CLAUDE_HOME", native_home)
    wid = None
    try:
        imported = call("import", sid)
        assert imported.returncode == 0, (imported.stdout, imported.stderr)
        handle = imported.stdout.strip()
        wid = handle.split(":", 1)[1]
        assert handle == "claude:" + wid and wid != sid
        store = require_ccx_active(short_root)
        rec = store.record(handle)
        assert rec["origin"] == "ccx-import" and rec["createdVia"] == "import"
        assert rec["native"]["sessionId"] == sid
        assert rec["importedFrom"]["preImportTranscriptSize"] == len(original_bytes)
        assert rec["importedFrom"]["preImportTranscriptMtime"] == int(
            native_before.st_mtime * 1000)
        root = store.pointer.generation_root
        owner = root / "owners" / (wid + ".json")
        claim = root / "claims" / (wid + ".json")
        before_proof = (owner.read_bytes(), claim.read_bytes())
        before_source = (source.read_bytes(), source.stat().st_mtime_ns)
        native_args = argv_log.read_text().splitlines()
        assert len(native_args) == 1 and "--resume " + sid in native_args[0]

        client = WorkerClient()
        full = client.conversation(handle)
        page = client.conversation_page(handle)
        logs = call("logs", handle, "--tail", "2")
        peek = call("peek", handle, "--json")
        assert full["schemaVersion"] == 5
        assert "NATIVE ORIGINAL FINAL" in json.dumps(full)
        assert "NATIVE ORIGINAL FINAL" in json.dumps(page)
        assert logs.returncode == 0, (logs.stdout, logs.stderr)
        assert "NATIVE ORIGINAL FINAL" in logs.stdout
        assert peek.returncode == 0, (peek.stdout, peek.stderr)
        assert "NATIVE ORIGINAL FINAL" in peek.stdout
        assert json.loads(peek.stdout)["worker"]["handle"] == handle
        with pytest.raises(CreationConflict):
            store.creation_binding(
                handle, engine="claude", fingerprint=rec["createSha256"],
                provenance="ccx-new",
            )
        with pytest.raises(CreationConflict):
            store.creation_binding(
                handle, engine="claude", fingerprint=rec["createSha256"],
                provenance="ccx-import",
                imported_from={**rec["importedFrom"], "preImportTranscriptSize": -1},
            )
        assert (owner.read_bytes(), claim.read_bytes()) == before_proof
        assert (source.read_bytes(), source.stat().st_mtime_ns) == before_source
        assert argv_log.read_text().splitlines() == native_args

        prompt = "F01 managed continuation"
        sent = call("send", handle, prompt, "--delivery-id", "f01-reader-one",
                    "--timeout", "20", "--json", timeout=35)
        assert sent.returncode == 0, (sent.stdout, sent.stderr)
        receipt = json.loads(sent.stdout)
        expected = "Fake Claude completed: " + prompt
        assert receipt["settled"] is True and receipt["report"]["text"] == expected
        assert receipt["report"]["delivery_id"] == "f01-reader-one"
        after = client.conversation(handle)
        after_page = client.conversation_page(handle)
        for view in (after, after_page):
            text = json.dumps(view)
            assert "NATIVE ORIGINAL FINAL" in text
            assert prompt in text and expected in text
        assert source.read_bytes().startswith(original_bytes)
        assert store.record(handle)["native"]["sessionId"] == sid
        assert argv_log.read_text().splitlines() == native_args
        reported = call("report", handle, "--json")
        assert reported.returncode == 0, (reported.stdout, reported.stderr)
        assert json.loads(reported.stdout)["text"] == expected
    finally:
        if wid:
            _stop_only_fake(short_root, wid)


@pytest.mark.parametrize("reason", ["malformed", "missing", "ambiguous", "wrong-cwd",
                                    "native-bg", "native-bg-no-id", "missing-cwd", "empty", "live-busy"])
def test_ineligible_exact_import_has_zero_original_owner_or_native_effect(short_root, tmp_path,
                                                                         reason):
    native_home, cwd, argv_log, _env, call = _environment(short_root, tmp_path)
    sid = str(uuid4())
    if reason not in {"malformed", "missing"}:
        source, _history = _original(native_home, cwd, sid, nonempty=reason != "empty")
        if reason == "ambiguous":
            duplicate = native_home / "projects" / "different-project" / f"{sid}.jsonl"
            duplicate.parent.mkdir(parents=True)
            duplicate.write_bytes(source.read_bytes())
        elif reason in {"wrong-cwd", "missing-cwd"}:
            other = str(tmp_path / "different-project")
            if reason == "wrong-cwd":
                Path(other).mkdir()
            source.write_text(json.dumps({"type": "user", "cwd": other,
                                          "message": {"role": "user", "content": "old"}}) + "\n")
        elif reason in {"native-bg", "native-bg-no-id"}:
            job = native_home / "jobs" / sid[:8] / "state.json"
            job.parent.mkdir(parents=True)
            job.write_text(json.dumps({"sessionId": sid} if reason == "native-bg" else {}))
        elif reason == "live-busy":
            # A positive identity-matched native process is intentionally
            # simulated at the liveness boundary; no provider is launched.
            pass
    if reason == "live-busy":
        # Test the unchanged main eligibility contract independently from
        # incidental PID creation rather than spawn a busy native TUI.
        eligible, code, _ = external.import_eligibility({
            "sessionId": sid, "cwd": str(cwd), "transcript": str(source),
            "live": True, "status": "busy", "waitKind": "none"})
        assert not eligible and code == "busy"
        return
    target = "short-or-name" if reason == "malformed" else sid
    result = call("import", target)
    assert result.returncode != 0, (result.stdout, result.stderr)
    assert not argv_log.exists()
    owners = short_root / "identity" / "generations" / G1 / "owners"
    assert not owners.exists() or not list(owners.glob("*.json"))


def test_post_claim_source_loss_keeps_original_uuid_and_no_native_fallback(
    short_root, tmp_path, monkeypatch,
):
    native_home, cwd, _argv_log, _env, _call = _environment(short_root, tmp_path)
    sid = str(uuid4())
    source, history = _original(native_home, cwd, sid)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setattr(external, "CLAUDE_HOME", native_home)
    monkeypatch.setattr(paths, "CLAUDE_HOME", native_home)
    imported = {"sessionId": sid, "cwd": str(cwd),
                "transcriptJsonl": str(source), "importedAt": 12345,
                "preImportTranscriptSize": len(history),
                "preImportTranscriptMtime": int(source.stat().st_mtime * 1000),
                "wasLive": False}
    original = creation._complete_claude_create

    def drop_after_reserve(*args, **kwargs):
        source.unlink()
        return original(*args, **kwargs)

    monkeypatch.setattr(creation, "_complete_claude_create", drop_after_reserve)
    with pytest.raises(CreatePartialFailure) as caught:
        creation.create(prompt=None, engine="claude", cwd=str(cwd),
                        worker_id=B, created_via="import", permissions="full",
                        import_session_id=sid, imported_from=imported)
    error = caught.value
    assert error.worker == "claude:" + B and error.phase == "native-activation"
    store = require_ccx_active(short_root)
    owner = store._owner(B)
    assert owner["provenance"] == "ccx-import"
    assert owner["createIntent"]["importedFrom"] == imported
    assert store.record(B)["native"]["sessionId"] == sid
    assert store.native_activation(B)["state"] == "unprepared"
    assert store.import_native_owner(sid)["worker"] == "claude:" + B


def test_original_sid_uniqueness_pending_ordinary_and_converted(short_root):
    store = require_ccx_active(short_root)
    sid = str(uuid4())
    provenance = {"sessionId": sid, "cwd": "/tmp", "transcriptJsonl": "/tmp/native.jsonl",
                  "preImportTranscriptSize": 42, "preImportTranscriptMtime": 123}
    store.reserve_import_creation(A, session_id=sid,
                                  fingerprint="f" * 64,
                                  native_binding={"sessionId": sid},
                                  imported_from=provenance)
    assert store.import_native_owner(sid)["worker"] == "claude:" + A
    assert not store.import_native_owner(sid)["confirmed"]
    with pytest.raises(Exception, match="already owned by claude:" + A):
        store.reserve_import_creation(B, session_id=sid,
                                      fingerprint="f" * 64,
                                      native_binding={"sessionId": sid},
                                      imported_from=provenance)
    another = str(uuid4())
    store.reserve_creation(B, engine="claude", fingerprint="e" * 64,
                           native_binding={"sessionId": another})
    assert store.import_native_owner(another)["worker"] == "claude:" + B
    # Clean converted managed owner without a modern createIntent still owns
    # its exact original native SID, so a fresh import cannot claim it again.
    native = str(uuid4())
    wid = mint_worker_id()
    with store.lease():
        store._write(("owners",), wid + ".json", {
            "schemaVersion": 2, "generation": store.generation,
            "workerId": wid, "engine": "claude", "status": "worker",
            "provenance": "migration-imported-claude"})
        store._write(("workers", "claude", wid), "record.json", {
            "workerId": wid, "engine": "claude",
            "origin": "ccx-import", "importedFrom": {"sessionId": native},
            "native": {"sessionId": native, "nativeState": "session-bound"}})
    collision = store.import_native_owner(native)
    assert collision["worker"] == "claude:" + wid
    assert collision["confirmed"] is False
    assert collision["provenance"] != "ccx-import"
    same_prefix_sid = sid[:8] + str(uuid4())[8:]
    assert same_prefix_sid != sid and same_prefix_sid[:8] == sid[:8]
    other_worker = mint_worker_id()
    store.reserve_import_creation(other_worker, session_id=same_prefix_sid,
                                  fingerprint="d" * 64,
                                  native_binding={"sessionId": same_prefix_sid},
                                  imported_from={**provenance, "sessionId": same_prefix_sid})
    assert store.import_native_owner(same_prefix_sid)["worker_id"] == other_worker
    assert store.import_native_owner(sid)["worker_id"] == A


def test_pending_same_native_sid_cli_refusal_names_original_owner(
    short_root, monkeypatch, capsys,
):
    from argparse import Namespace
    from ccx.cli.commands import _common as commands
    from ccx import constants
    store = require_ccx_active(short_root)
    sid = str(uuid4())
    store.reserve_import_creation(A, session_id=sid, fingerprint="b" * 64,
                                  native_binding={"sessionId": sid},
                                  imported_from={"sessionId": sid})
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    with pytest.raises(SystemExit) as caught:
        commands_create.cmd_import(Namespace(session=sid))
    assert caught.value.code != 0
    assert "claude:" + A in capsys.readouterr().err
    assert store.import_native_owner(sid)["worker_id"] == A


@pytest.mark.parametrize("provider_start,presence,observed_start", [
    ("123456", "alive", "123456"),  # proved live idle; argv optional
    (None, "alive", "123456"), ("0", "alive", "123456"),
    ("123456", "alive", "543210"), ("123456", "unprovable", None),
    (True, "alive", "1"), ("123456", "dead", None),  # stopped history
])
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Linux numeric start-tick ABI; Darwin C-locale UTC sample covered separately",
)
def test_exact_native_import_procstart_requires_live_identity(
    tmp_path, monkeypatch, provider_start, presence, observed_start,
):
    """A live candidate needs matching provider and kernel PID/start facts.

    Unreadable argv cannot downgrade an unprovable live process to a stopped
    import. Stopped sessions still use the existing transcript resume policy.
    """
    native_home = tmp_path / "claude-home"
    cwd = tmp_path / "native-project"
    cwd.mkdir()
    sid = str(uuid4())
    _original(native_home, cwd, sid)
    sessions = native_home / "sessions"
    sessions.mkdir()
    meta = {"sessionId": sid, "pid": 4711, "cwd": str(cwd),
            "kind": "interactive", "status": "idle", "waitingFor": None}
    if provider_start is not None:
        meta["procStart"] = provider_start
        meta["pidDomain"] = "linux:9c1c63c55c164e25b6cab9ecf2d91080:pid:[4026532223]"
    (sessions / "4711.json").write_text(json.dumps(meta))
    monkeypatch.setattr(external, "CLAUDE_HOME", native_home)
    monkeypatch.setattr(paths, "CLAUDE_HOME", native_home)
    monkeypatch.setattr(oslayer, "read_argv", lambda pid: pytest.fail(
        "argv cannot authorize or veto a native import"))
    monkeypatch.setattr(oslayer, "process_info", lambda pid: oslayer.ProcessInfo(
        pid, presence, None, "S" if presence == "alive" else None, observed_start))
    allowed = presence == "dead" or (presence == "alive" and provider_start == observed_start)
    if not allowed:
        with pytest.raises(SessionUnavailable, match="PID/procStart"):
            external.exact_import_candidate(sid)
    else:
        candidate = external.exact_import_candidate(sid)
        assert candidate["live"] is (presence == "alive")
        assert external.import_eligibility(candidate)[:2] == (True, "ok")


@pytest.mark.parametrize('domain,kernel_start,allowed', [
    ('darwin', 'darwin:1792965748:000001', True),
    ('darwin', 'darwin:1792965748:999999', True),
    ('darwin', 'darwin:1792965749:000000', False),
    ('linux:9c1c63c55c164e25b6cab9ecf2d91080:pid:[4026532223]',
     'darwin:1792965748:000001', False),
    (None, None, False),
])
def test_exact_native_darwin_procstart_requires_same_original_kernel_second(
    tmp_path, monkeypatch, domain, kernel_start, allowed,
):
    from ccx.system import oslayer
    monkeypatch.setattr(oslayer.sys, 'platform', 'darwin')
    native_home = tmp_path / 'claude-home'
    cwd = tmp_path / 'native-project'
    cwd.mkdir()
    sid = str(uuid4())
    _original(native_home, cwd, sid)
    sessions = native_home / 'sessions'
    sessions.mkdir()
    meta = {'sessionId': sid, 'pid': 4711, 'cwd': str(cwd),
            'kind': 'interactive', 'status': 'idle',
            'procStart': 'Sun Oct 25 22:02:28 2026'}
    if domain is not None:
        meta['pidDomain'] = domain
    (sessions / '4711.json').write_text(json.dumps(meta))
    monkeypatch.setattr(external, 'CLAUDE_HOME', native_home)
    monkeypatch.setattr(paths, 'CLAUDE_HOME', native_home)
    monkeypatch.setattr(oslayer, 'read_argv', lambda pid:
                        pytest.fail('argv must not authorize provider import'))
    monkeypatch.setattr(oslayer, 'process_info', lambda pid:
                        oslayer.ProcessInfo(pid, 'alive' if kernel_start else 'unprovable',
                                            1, 'R', kernel_start))
    if not allowed:
        with pytest.raises(SessionUnavailable, match='PID/procStart'):
            external.exact_import_candidate(sid)
    else:
        candidate = external.exact_import_candidate(sid)
        assert candidate['live'] is True
        assert external.import_eligibility(candidate)[0] is True
