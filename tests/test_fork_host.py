"""F1b: frozen full-UUID Claude fork, one original host copy and separate hook proof."""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
import signal
import sys
import time

import pytest

from ccx import constants
from ccx.ops import creation
from ccx.ops import forking
from ccx.store import paths
from ccx.errors import CreatePartialFailure, SessionUnavailable
from ccx.store.liveness import owned_pid_alive
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import CreationIndeterminate, NativeEffectIndeterminate, require_ccx_active
from ccx.store.store import fingerprint_v2
from test_generation_store import A, C, short_root as short_root
from test_fork_source import sources as sources


def _launch(plan, worker_id=C, **kwargs):
    options = dict(
        engine="claude", cwd=plan["cwd"], worker_id=worker_id,
        name=plan["name"], _fork_anchor=plan["fork_anchor"],
        **plan["launch"],
    )
    options.update(kwargs)
    return options


def test_f1b_frozen_child_claim_first_record_and_mutable_same_id_retry(
    sources, tmp_path, monkeypatch,
):
    from ccx.ops import creation
    store, original, observed = sources
    binary = tmp_path / "present-fake-claude"
    binary.write_text("#!/bin/sh\nexit 1\n")
    binary.chmod(0o700)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(binary))
    def fail_activation(*_args, **_kwargs):
        raise NativeEffectIndeterminate("fake original SessionStart unproven")
    monkeypatch.setattr(creation, "_complete_claude_create", fail_activation)
    plan = forking.plan(A, model="opus")
    # A malformed inherited seed is not a reason to mint or reserve a child.
    malformed_id = "dddddddd-1234-4000-8000-000000000004"
    malformed_anchor = json.loads(json.dumps(plan["fork_anchor"]))
    malformed_anchor["seed"]["stats"] = []
    options = _launch(plan, worker_id=malformed_id)
    options.pop("_fork_anchor")
    with pytest.raises(CreationIndeterminate, match="fork settings changed"):
        prepare_worker(store, **options, fork_anchor=malformed_anchor)
    assert store._owner(malformed_id) is None

    with pytest.raises(CreatePartialFailure) as first:
        creation.create(prompt=None, **_launch(plan))
    assert first.value.phase == "native-activation"
    assert isinstance(first.value.cause, NativeEffectIndeterminate)
    assert "fake original SessionStart unproven" in first.value.info["native_start_error"]
    record = store.record(C)
    anchor = plan["fork_anchor"]
    child_sid = record["native"]["sessionId"]
    assert len({A, C, anchor["sourceNativeSessionId"], child_sid}) == 4
    assert record["forkAnchor"] == store._claim(C)["forkAnchor"] == anchor
    assert store._owner(C)["createIntent"]["forkAnchor"] == anchor
    assert record["forkedFrom"] == {
        key: anchor[key] for key in ("engine", "sourceWorkerId", "sourceNativeSessionId",
                                  "resumeAt", "forkedAtMs")
    }
    assert record["createSha256"] == fingerprint_v2(
        engine="claude", cwd=anchor["cwd"], create_args=anchor["createArgs"],
        fork_anchor=anchor,
    )
    assert record["stats"]["turns"] == 1
    assert "inputTokens" not in record["stats"] and "outputTokens" not in record["stats"]
    assert record["lastTurnAt"] == anchor["seed"]["lastTurnAt"]
    assert record["transcript"] == str(paths.transcript_path_for(plan["cwd"], child_sid))
    assert store.native_activation(C)["state"] == "unprepared"
    store.mutate_record(C, patches={"name": "manual child", "createArgs.model": "later model"})
    store.mutate_record(A, patches={"lifecycle.state": "dead", "createArgs.model": "new source model"})
    assert original.exists()
    frozen_again = forking.plan(A, worker_id=C, model="opus")
    assert frozen_again["existing"] and frozen_again["fork_anchor"] == anchor
    assert frozen_again["launch"]["model"] == "opus"
    with pytest.raises(CreatePartialFailure) as again:
        creation.create(prompt=None, **_launch(frozen_again, name="should not overwrite"))
    assert again.value.worker_id == C
    # The child was deliberately reconfigured after the first claim; never
    # overwrite that mutable record or reinterpret its original fingerprint.
    assert again.value.phase == "record"
    assert isinstance(again.value.cause, CreationIndeterminate)
    assert "original first-record immutable full-UUID proof differs" in str(again.value.cause)
    assert len(list(store.pointer.generation_root.joinpath("owners").glob("*.json"))) == 3
    assert store.record(C)["native"]["sessionId"] == child_sid
    assert store.record(C)["forkAnchor"] == anchor
    assert store.record(C)["name"] == "manual child"
    assert store.record(C)["createArgs"]["model"] == "later model"
    assert observed == []


def _fake_copy_and_pty_wrapper(tmp_path: Path):
    # One fake binary entrypoint. Print-mode materializer copies a native JSONL
    # without any hook/model. Interactive mode delegates to existing fake_claude
    # so the native child really emits SessionStart through generated hooks.
    wrapper = tmp_path / "fork-native-fake"
    wrapper.write_text("#!" + sys.executable + "\n" + '\nimport json, os, runpy, sys\nfrom pathlib import Path\nfrom ccx.store import paths\nfrom ccx.store.root import require_ccx_active\n\nargv = sys.argv[1:]\nwid = os.environ["CCX_SELF_WORKER_ID"]\nstore = require_ccx_active(Path(os.environ["CCX_ROOT"]))\nrec = store.record(wid)\nclaim = store.native_activation(wid)\nlog = Path(os.environ["FORK_NATIVE_ARGV_LOG"])\nif "-p" in argv:\n    assert paths.sock_path(wid).exists(), "copy must run AFTER socket.listen"\n    assert claim["state"] == "submitted-unknown"\n    assert rec["lifecycle"]["nativeForkCopyAttempt"] == claim["submissionId"]\n    assert rec["lifecycle"].get("nativeForkProof") is None\n    assert rec["lifecycle"].get("nativeStartProof") is None\n    assert "--fork-session" in argv and "--session-id" in argv\n    sid = argv[argv.index("--session-id") + 1]\n    anchor = rec["forkAnchor"]\n    assert sid == rec["native"]["sessionId"]\n    assert argv[argv.index("--resume") + 1] == anchor["transcript"]\n    assert argv[argv.index("--resume-session-at") + 1] == anchor["resumeAt"]\n    assert json.loads(argv[argv.index("--settings") + 1]) == {"disableAllHooks": True}\n    assert "--strict-mcp-config" in argv\n    assert argv[-1] == "/rename " + rec["name"]\n    assert not any(flag in argv for flag in ("--model", "--effort", "--agent"))\n    with log.open("a") as output:\n        output.write(json.dumps({"kind": "copy", "argv": argv, "listener": True}) + "\\n")\n    source_rows = [json.loads(line) for line in Path(anchor["transcript"]).read_text().splitlines()]\n    copied = []\n    for row in source_rows:\n        row["sessionId"] = sid\n        copied.append(json.dumps(row))\n    child = paths.transcript_path_for(anchor["cwd"], sid)\n    child.parent.mkdir(mode=0o700, parents=True, exist_ok=True)\n    child.write_text("\\n".join(copied) + "\\n")\n    child.chmod(0o600)\n    print(json.dumps({"type": "result", "is_error": False, "session_id": sid,\n                      "num_turns": 0, "local_command": "rename"}))\nelse:\n    assert "--resume" in argv and "--session-id" not in argv\n    assert "--fork-session" not in argv\n    sid = argv[argv.index("--resume") + 1]\n    assert sid == rec["native"]["sessionId"]\n    assert rec["lifecycle"]["nativeForkProof"]["nativeSessionId"] == sid\n    assert rec["lifecycle"].get("nativeStartProof") is None\n    with log.open("a") as output:\n        output.write(json.dumps({"kind": "pty", "argv": argv, "copy_proven": True}) + "\\n")\n    sys.argv = [os.environ["FORK_FAKE_PTY"], *argv]\n    runpy.run_path(os.environ["FORK_FAKE_PTY"], run_name="__main__")\n', encoding="utf-8")
    wrapper.chmod(0o700)
    return wrapper


def _stop_fake(root, wid):
    """Only ever signal this test's pinned disposable host/native PID+start."""
    try:
        record = require_ccx_active(root).record(wid)
    except Exception:
        return
    state = record.get("lifecycle") or {}
    pairs = [(state.get(p), state.get(t)) for p, t in (
        ("hostPid", "hostStart"), ("workerPid", "workerStart"))]
    for signal_to_send in (signal.SIGTERM, signal.SIGKILL):
        for pid, start in pairs:
            if owned_pid_alive(pid, start):
                try:
                    os.kill(pid, signal_to_send)
                except ProcessLookupError:
                    pass
        until = time.monotonic() + 2
        while time.monotonic() < until and any(
                owned_pid_alive(pid, start) for pid, start in pairs):
            time.sleep(.05)


def test_f1b_one_original_detached_host_listen_copy_proof_then_native_sessionstart(
    sources, tmp_path, monkeypatch,
):
    store, original, observed = sources
    plan = forking.plan(A, model="opus")
    wrapper = _fake_copy_and_pty_wrapper(tmp_path)
    native_home = tmp_path / "native-home"
    log = tmp_path / "fork-native-argv.ndjson"
    fake = Path(__file__).resolve().parent / "fake_claude.py"
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(wrapper))
    for name, value in {
        "CCX_ROOT": str(constants.CCX_ROOT),
        "CCX_CLAUDE_BIN": str(wrapper),
        "CLAUDE_CONFIG_DIR": str(native_home),
        "FORK_NATIVE_ARGV_LOG": str(log),
        "FORK_FAKE_PTY": str(fake),
        "CCX_SPAWN_SCOPE": "0",
        "CCX_BOOT_TIMEOUT_S": "15", "FAKE_HOOKS_VIA_SETTINGS": "1",
        "FAKE_LOG": str(tmp_path / "fake-pty-input.log"),
        "FAKE_HOOK_RESULT_LOG": str(tmp_path / "fake-hook-result.log"),
        "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
        "PYTHONDONTWRITEBYTECODE": "1",
    }.items():
        monkeypatch.setenv(name, value)
    try:
        try:
            result = creation.create(**_launch(plan, start_timeout=18))
        except CreatePartialFailure as partial:
            assert partial.worker_id == C
            print("INITIAL FORK PARTIAL:", partial.info.get("cause_type"),
                  partial.info.get("native_start_error"))
            # A slow but still-live original host can finish after the initial
            # caller budget; its same original submission is observed only.
            until = time.monotonic() + 12
            while time.monotonic() < until:
                if store.native_activation(C)["state"] == "confirmed":
                    break
                time.sleep(.05)
            result = creation.create(**_launch(plan, start_timeout=8))
        assert result["worker_id"] == C and result["native_ready"] is True
        record = store.record(C)
        activation = store.native_activation(C)
        copy = record["lifecycle"]["nativeForkProof"]
        native = record["lifecycle"]["nativeStartProof"]
        sid = record["native"]["sessionId"]
        assert copy["workerId"] == C and copy["nativeSessionId"] == sid
        assert copy["forkAnchor"] == plan["fork_anchor"]
        assert copy["inheritedBytes"] > 0 and copy["inheritedOpenTaskIds"] == []
        assert record["lifecycle"]["nativeTranscriptStart"] >= copy["inheritedBytes"]
        assert record["lifecycle"]["nativeForkCopyAttempt"] == activation["submissionId"]
        assert native["sessionId"] == sid and native["workerId"] == C
        assert native["submissionId"] == activation["submissionId"]
        assert activation["state"] == "confirmed"
        assert not forking.claude_fork_pending(record)
        assert forking.inherited_offset(record) == copy["inheritedBytes"]
        actions = [json.loads(line) for line in log.read_text().splitlines()]
        assert [item["kind"] for item in actions] == ["copy", "pty"]
        assert "--resume" in actions[1]["argv"]
        assert "--session-id" not in actions[1]["argv"]
        assert "--model" in actions[1]["argv"]
        assert actions[1]["argv"][actions[1]["argv"].index("--model") + 1] == "opus"
        # Fork-copy is headless (-p); the actual managed forked PTY worker
        # uses its own generated --settings, overriding any user fullscreen TUI.
        fork_argv = actions[1]["argv"]
        assert "--settings" in fork_argv
        settings_file = Path(fork_argv[fork_argv.index("--settings") + 1])
        assert settings_file == paths.settings_path(C)
        assert json.loads(settings_file.read_text())["tui"] == "default"
        assert original.read_text().count("original") == 1
        assert observed == []

        # A legitimate later host lifetime replaces nativeStartProof (same
        # original submission/source/SID, new PID/start, new self-digest) while
        # the claim keeps its first digest. Same-child retry must stay ready
        # and launch/copy nothing. Only the proof changes: lifecycle host/worker
        # PIDs stay pinned to the real test host for cleanup.
        from ccx.store.store import _digest
        resumed = {key: value for key, value in native.items()
                   if key != "nativeEvidenceSha256"}
        resumed.update(hostPid=native["hostPid"] + 100_000, hostStart="999",
                       workerPid=native["workerPid"] + 100_000, workerStart="998",
                       peerPid=native["workerPid"] + 100_000, peerStart="998")
        resumed["nativeEvidenceSha256"] = _digest(resumed)
        assert resumed["nativeEvidenceSha256"] != activation["nativeEvidenceSha256"]
        store.mutate_record(C, patches={"lifecycle.nativeStartProof": resumed},
                            expect={"lifecycle.nativeStartProof": native})
        retried = creation.create(**_launch(plan, start_timeout=8))
        assert retried["worker_id"] == C and retried["native_ready"] is True
        assert store.native_activation(C) == activation
        assert store.record(C)["lifecycle"]["nativeStartProof"] == resumed
        assert [json.loads(line)["kind"]
                for line in log.read_text().splitlines()] == ["copy", "pty"]
    finally:
        try:
            current = store.record(C)
            (tmp_path / "fork-last-record.json").write_text(json.dumps(current, indent=2))
            host_log = store.worker_path(C).parent / "host.log"
            if host_log.exists():
                shutil.copyfile(host_log, tmp_path / "fork-host-log.txt")
        except Exception:
            pass  # diagnostic copy must never suppress the original failure
        _stop_fake(constants.CCX_ROOT, C)


@pytest.mark.parametrize("failure", ["lost", "wrong-native-reply"])
def test_f1b_copy_unknown_refuses_reentry_and_both_proofs_gate_ready(
    sources, monkeypatch, failure,
):
    store, _source, _observed = sources
    plan = forking.plan(A)
    anchor = plan["fork_anchor"]
    initial = prepare_worker(store, **{k: v for k, v in _launch(plan).items()
                                       if k != "_fork_anchor"}, fork_anchor=anchor)
    reservation = store.reserve_creation(
        C, engine="claude", fingerprint=initial.record["createSha256"],
        native_binding={"sessionId": initial.record["native"]["sessionId"]},
        fork_anchor=anchor,
    )
    prepared = store.prepare_native_activation(reservation, native_source=initial.native_source)
    store.native_activation_dispatch(prepared, bounded_submit=lambda _secs: {"ok": True})
    attempt = store.native_activation(C)["submissionId"]
    store.mutate_record(C, patches={"lifecycle.hostPid": 987654,
                                    "lifecycle.hostStart": "fake-first-lifetime"})
    child_path = Path(initial.record["transcript"])
    child_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    child_path.write_bytes(b"partial old child")
    with pytest.raises(CreationIndeterminate, match="unproven native bytes"):
        forking.copy_original_claude_fork(
            store, initial.record, submission_id=attempt,
            host_pid=987654, host_start="fake-first-lifetime")
    assert store.record(C)["lifecycle"].get("nativeForkCopyAttempt") is None
    child_path.unlink()  # this disposable test removes its own fake preexisting data
    calls = []
    if failure == "lost":
        def lost_copy(*_args, **_kwargs):
            calls.append("copy")
            raise SessionUnavailable("copy result lost")
        monkeypatch.setattr(forking, "_run_materializer", lost_copy)
    else:
        class WrongNativeResult:
            pid = 999999
            returncode = 0
            def communicate(self, timeout=None):
                return json.dumps({
                    "type": "result", "is_error": False,
                    "session_id": initial.record["native"]["sessionId"],
                    "num_turns": 1, "local_command": "rename",
                }), ""
        def wrong_popen(argv, **_kwargs):
            calls.append(argv)
            return WrongNativeResult()
        monkeypatch.setattr(forking.subprocess, "Popen", wrong_popen)
    with pytest.raises(SessionUnavailable, match="copy result lost|copy failed"):
        forking.copy_original_claude_fork(
            store, initial.record, submission_id=attempt,
            host_pid=987654, host_start="fake-first-lifetime")
    assert len(calls) == 1
    record = store.record(C)
    assert record["lifecycle"]["nativeForkCopyAttempt"] == attempt
    assert record["lifecycle"].get("nativeForkProof") is None
    assert store.native_activation(C)["state"] == "submitted-unknown"
    with pytest.raises(CreationIndeterminate, match="never recopy"):
        forking.copy_original_claude_fork(
            store, record, submission_id=attempt,
            host_pid=987654, host_start="fake-first-lifetime")
    assert len(calls) == 1
    # A positive *claim alone* or even a forged independent native-start fact
    # cannot establish copied history; conversely copy without SessionStart
    # never completes native activation.
    store.confirm_native_activation(
        prepared, native_evidence_sha256="a" * 64,
        native_binding={"sessionId": initial.record["native"]["sessionId"]},
    )
    confirmed = store.bind_confirmed_native(C)
    with pytest.raises(NativeEffectIndeterminate, match="both|copy and native"):
        creation._require_claude_fork_ready(store, initial, confirmed)
    proof = {"workerId": C, "nativeSessionId": initial.record["native"]["sessionId"],
             "forkAnchor": anchor, "copiedAtMs": 1, "inheritedBytes": 50,
             "inheritedOpenTaskIds": []}
    store.mutate_record(C, patches={"lifecycle.nativeForkProof": proof})
    assert not forking.claude_fork_pending(store.record(C))
    with pytest.raises(NativeEffectIndeterminate, match="copy and native"):
        creation._require_claude_fork_ready(store, initial, store.record(C))
