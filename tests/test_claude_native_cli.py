"""Normal ccx new -> detached host -> genuine PTY fake Claude -> full-ID turn."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import time

import pytest

from test_generation_store import A, G1, short_root
from ccx import constants
from ccx.store.root import require_ccx_active
from ccx.store.liveness import proc_start_token, owned_pid_alive


def _stop_disposable_original(root):
    """Only signal positive PID/start pairs from this disposable generation."""
    try:
        record = require_ccx_active(root).record(A)
    except Exception:
        return
    live = record.get("lifecycle") or {}
    for key, token in (("hostPid", "hostStart"), ("workerPid", "workerStart")):
        pid = live.get(key)
        start = live.get(token)
        if owned_pid_alive(pid, start):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if not any(owned_pid_alive(live.get(key),live.get(token)) for key,token
                   in (("hostPid", "hostStart"), ("workerPid", "workerStart"))):
            break
        time.sleep(.05)
    for key, token in (("hostPid", "hostStart"), ("workerPid", "workerStart")):
        pid=live.get(key)
        if owned_pid_alive(pid, live.get(token)):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_normal_cli_new_uses_one_real_detached_host_and_fake_claude_turn(
    short_root, tmp_path, monkeypatch,
):
    """No native provider. Test actual CLI/launcher/daemonized host/PTY/socket."""
    root=short_root
    repo=Path(__file__).resolve().parent.parent
    fake=repo/"tests"/"fake_claude.py"
    wrapper=tmp_path/"fake-claude-wrapper"
    wrapper.write_text(
        # Exceed the old 2s admission + .5s observation budget before
        # the original fake provider can emit its SessionStart proof.
        "#!/bin/sh\nsleep 4\nexec "+shlex.quote(sys.executable)+" "
        +shlex.quote(str(fake))+' "$@"\n',encoding="utf-8",
    )
    wrapper.chmod(0o700)
    native_input=tmp_path/"fake-input.log"
    fake_hook_trace=tmp_path/"fake-hook-replies.jsonl"
    fake_claude_home=tmp_path/"fake-claude-home"
    env={**os.environ,
         "CCX_ROOT":str(root),
         "CCX_CLAUDE_BIN":str(wrapper),
         "CCX_SPAWN_SCOPE":"0",
         "CCX_BOOT_TIMEOUT_S":"15",
         "FAKE_MODE":"",
         "FAKE_LOG":str(native_input),
         "FAKE_HOOK_RESULT_LOG":str(fake_hook_trace),
         "CLAUDE_CONFIG_DIR":str(fake_claude_home),
         "FAKE_NATIVE_SESSION_METADATA":"1",
         "PYTHONPATH":str(repo),
    }
    for field in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(field,None)
    monkeypatch.setattr(constants,"CCX_ROOT",root)
    from ccx.store import paths as native_paths
    monkeypatch.setattr(native_paths, "CLAUDE_HOME", fake_claude_home)
    def new():
        try:
            return subprocess.run(
                [sys.executable,"-B","-m","ccx","new","--engine","claude","--cwd",str(tmp_path),
                 "--worker-id",A,"--no-wait","--json"],
                env=env,cwd=str(repo),capture_output=True,text=True,timeout=15,
            )
        except subprocess.TimeoutExpired as exc:
            # These are this fixture's private, disposable host artifacts;
            # never read the real provider home, auth data or native history.
            from ccx.store.paths import host_log_path, sock_path, events_ndjson_path
            from ccx.system import oslayer
            path = host_log_path(A)
            host_tail = path.read_text(errors="replace")[-5000:] if path.exists() else ""
            socket_path = sock_path(A)
            store = require_ccx_active(root)
            lifecycle = store.record(A).get("lifecycle") or {}
            summary = {
                key: lifecycle.get(key) for key in (
                    "state", "hostPid", "hostStart", "workerPid",
                    "workerStart", "lastError", "nativeStartProof",
                )
            }
            for role in ("host", "worker"):
                pid = summary[role + "Pid"]
                summary[role + "Kernel"] = oslayer.process_info(pid) if pid else None
            events = events_ndjson_path(A)
            events_tail = events.read_text(errors="replace")[-1200:] if events.exists() else ""
            hook_replies = (fake_hook_trace.read_text(errors="replace")[-1500:]
                            if fake_hook_trace.exists() else "")
            raise AssertionError(
                f"synthetic CLI startup stalled; host_log_tail={host_tail!r}; "
                f"socket={str(socket_path)!r} socket_exists={socket_path.exists()}; "
                f"fake_input_exists={native_input.exists()}; "
                f"activation={store.native_activation(A).get('state')!r}; "
                f"lifecycle={summary!r}; synthetic_events={events_tail!r}; "
                f"hook_replies={hook_replies!r}"
            ) from exc
    try:
        initial=new()
        assert initial.returncode==0, (initial.stdout,initial.stderr)
        value=json.loads(initial.stdout)
        assert value["worker_id"]==A and value["worker"]=="claude:"+A
        assert value["native_ready"] is True
        store=require_ccx_active(root)
        original=store.record(A)
        session=original["native"]["sessionId"]
        assert session!=A
        # Successful startup proves the original claimant's SessionStart,
        # with no new process/native call and no remint.
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            original=store.record(A)
            if isinstance(original.get("lifecycle",{}).get("nativeStartProof"),dict):
                break
            time.sleep(.05)
        proof=original["lifecycle"].get("nativeStartProof")
        assert proof is not None, (
            "CLI=" + repr(value) + " stdout=" +repr(initial.stdout)
            +" stderr="+repr(initial.stderr)
            +" lifecycle="+repr(original["lifecycle"])
            +" files="+repr(sorted(str(p.relative_to(root))
                                  for p in root.rglob("*") if p.is_file())[-40:])
            +" hostlog="+repr((root/"identity"/"generations"/G1/"workers"/"claude"/A/"host.log").read_text(errors="replace")
                             if (root/"identity"/"generations"/G1/"workers"/"claude"/A/"host.log").exists() else "")
        )
        assert proof["sessionId"]==session
        assert proof["workerId"]==A
        assert proof["generation"]==store.generation
        assert proof["nativeSource"]=="claude-session:"+session
        assert owned_pid_alive(proof["hostPid"],proof["hostStart"])
        assert owned_pid_alive(proof["workerPid"],proof["workerStart"])
        assert store.native_activation(A)["state"]=="confirmed"
        assert store.record(A)["native"]["nativeState"]=="session-bound"
        original_host=store.record(A)["lifecycle"]["hostPid"]
        original_child=store.record(A)["lifecycle"]["workerPid"]
        # The fake writes upstream-shaped, PID-keyed metadata into its
        # disposable provider home. The identity claim still comes from
        # the original kernel PID/start, not its machine or namespace suffix.
        native_meta=json.loads(
            (fake_claude_home/"sessions"/f"{original_child}.json").read_text())
        from ccx.system import oslayer
        from ccx.store.liveness import read_managed_claude_native_state
        expected_domain=("darwin" if oslayer.UNIX_SUN_PATH_BYTES==104
                         else "linux:9c1c63c55c164e25b6cab9ecf2d91080:pid:[4026532223]")
        assert native_meta["pidDomain"]==expected_domain
        assert oslayer.provider_start_matches(
            native_meta["procStart"],proof["workerStart"],native_meta["pidDomain"])
        assert read_managed_claude_native_state(
            original_child,session,claude_home=fake_claude_home) is not None
        # A confirmed same-spec CLI retry returns the existing exact worker
        # and does not attempt another native start/daemon instance.
        same=new()
        assert same.returncode==0,(same.stdout,same.stderr)
        assert json.loads(same.stdout)["worker_id"]==A
        assert store.record(A)["lifecycle"]["hostPid"]==original_host
        assert store.record(A)["lifecycle"]["workerPid"]==original_child

        # Drive the same TRUE CLI surface, not a Python injected callback:
        # first send is original host-owned pending, Stop makes it settled,
        # report comes from the exact completed receipt, same ID never resends.
        def call(*argv):
            return subprocess.run(
                [sys.executable, "-B", "-m", "ccx", *argv],
                env=env, cwd=str(repo),
                capture_output=True, text=True, timeout=15,
            )
        delivery_id = "full-real-cli-original-turn"
        send_args = (
            "send", A, "hello from true CLI worker",
            "--no-wait", "--delivery-id", delivery_id, "--json",
        )
        first_send = call(*send_args)
        assert first_send.returncode == 0, (
            first_send.stdout, first_send.stderr,
        )
        sent = json.loads(first_send.stdout)
        assert sent["worker_id"] == A
        assert sent["delivery_id"] == delivery_id
        assert sent["status"] in ("owned", "accepted")
        deadline = time.monotonic() + 12
        report = None
        while time.monotonic() < deadline:
            observed = call("report", A, "--json")
            if observed.returncode == 0:
                report = json.loads(observed.stdout)
                break
            time.sleep(.10)
        assert report is not None, (
            "actual CLI report never observed native Stop settlement",
            store.record(A),
            (root / "identity" / "generations" / G1 / "workers" /
             "claude" / A / "host.log").read_text(errors="replace")[-4000:]
            if (root / "identity" / "generations" / G1 / "workers" /
                "claude" / A / "host.log").exists() else "host log absent",
        )
        assert report["delivery_id"] == delivery_id
        assert report["text"] == (
            "Fake Claude completed: hello from true CLI worker"
        )
        assert b"hello from true CLI worker" in native_input.read_bytes()
        prior_len = len(native_input.read_bytes())
        repeated = call(*send_args)
        assert repeated.returncode == 0, (
            repeated.stdout, repeated.stderr,
        )
        again = json.loads(repeated.stdout)
        assert again["settled"] is True
        assert again["delivery_id"] == delivery_id
        assert len(native_input.read_bytes()) == prior_len
        assert store.read_receipt(A, "claude-delivery", delivery_id)[
            "state"
        ] == "accepted"
        # Same native ID with a different original request must conflict,
        # never paste or mutate a previously completed provider turn.
        different = call(
            "send", A, "NOT the original prompt", "--no-wait",
            "--delivery-id", delivery_id, "--json",
        )
        assert different.returncode != 0
        assert len(native_input.read_bytes()) == prior_len
        # An invalid worker query (shorter than the 8-hex prefix minimum) is
        # refused before any native effect.
        alias = call("send", A[:7], "wrong target", "--no-wait", "--json")
        assert alias.returncode != 0
        assert len(native_input.read_bytes()) == prior_len
        assert store.record(A)["lifecycle"]["hostPid"] == original_host
        assert store.record(A)["lifecycle"]["workerPid"] == original_child

        # Same ORIGINAL physical fake-Claude host; now the three public Python
        # clients must use it directly, not shell out or call legacy short IPC.
        from ccx.client import CcxClient
        from ccx.workers import WorkerClient
        native_client=CcxClient()
        facade=WorkerClient()
        assert facade.capabilities(A)["engine"] == "claude"
        native_token="direct-claude-original-token"
        typed=native_client.send(
            f"claude:{A}","direct Python Claude delivery",
            wait=False,delivery_id=native_token,
        )
        assert typed.worker_id==A and typed.short==A[:8]
        assert typed.as_dict()["worker"]==f"claude:{A}"
        assert typed.delivery_id==native_token
        deadline=time.monotonic()+10
        settled_status=None
        while time.monotonic()<deadline:
            settled_status=native_client.delivery_status(A,native_token)
            if settled_status.settled:
                break
            time.sleep(.05)
        assert settled_status is not None and settled_status.settled
        assert settled_status.as_dict()["worker"]==f"claude:{A}"
        typed_report=native_client.report(A)
        assert typed_report.worker_id==A
        assert typed_report.delivery_id==native_token
        assert typed_report.text==(
            "Fake Claude completed: direct Python Claude delivery"
        )
        original_bytes=native_input.read_bytes()
        duplicate=native_client.send(
            A,"direct Python Claude delivery",
            wait=False,delivery_id=native_token,
        )
        assert duplicate.settled and duplicate.delivery_id==native_token
        assert native_input.read_bytes()==original_bytes
        facade_token="facade-claude-original-token"
        normalized=facade.send(
            A,"from WorkerClient Claude",
            wait=True,timeout=10,delivery_id=facade_token,
        )
        assert normalized["worker"]==f"claude:{A}"
        assert normalized["delivery_id"]==facade_token
        assert normalized["settled"] is True
        assert normalized["reply"]==(
            "Fake Claude completed: from WorkerClient Claude"
        )
        summary=facade.report(A,turn_ref=normalized["turn_ref"])
        assert summary["worker_id"]==A
        assert summary["text"]==normalized["reply"]
        assert facade.send(
            A,"from WorkerClient Claude",
            wait=False,delivery_id=facade_token,
        )["settled"] is True
        assert store.record(A)["lifecycle"]["hostPid"]==original_host
        assert store.record(A)["lifecycle"]["workerPid"]==original_child

        # ORIGINAL one-shot V1 envelope, normal CLI -> same native host
        # process used above. Positive delivery/turn/report are all bound to
        # one root/generation/full UUID and ORIGINAL caller-owned token.
        def v1(op, **params):
            envelope={"protocol_version":2,"request_id":"real-v1-claude",
                      "op":op,"params":params}
            process=subprocess.run(
                [sys.executable,"-B","-m","ccx","rpc"],
                input=json.dumps(envelope),env=env,cwd=str(repo),
                capture_output=True,text=True,timeout=15,
            )
            assert process.returncode==0,(process.stdout,process.stderr)
            return json.loads(process.stdout)
        v1_token="v1-claude-real-native-token"
        begun=v1("turn.send",target=A,text="from V1 real Claude",
                 delivery_id=v1_token)
        assert begun["ok"] is True,begun
        assert begun["result"]["worker"]==f"claude:{A}"
        deadline=time.monotonic()+10
        completed=None
        while time.monotonic()<deadline:
            probe=v1("turn.reconcile",target=A,delivery_id=v1_token)
            assert probe["ok"] is True,probe
            if probe["result"]["delivery"]["status"]=="settled":
                completed=probe["result"]
                break
            time.sleep(.05)
        assert completed is not None
        native=v1("worker.report",target=A,
                  turn_ref=completed["delivery"]["turn_ref"])
        assert native["ok"] is True,native
        assert native["result"]["text"]==(
            "Fake Claude completed: from V1 real Claude"
        )
        original_data=native_input.read_bytes()
        repeated=v1("turn.send",target=A,text="from V1 real Claude",
                    delivery_id=v1_token)
        assert repeated["ok"] is True,repeated
        assert isinstance(repeated["result"]["delivery"]["settlement"], dict)
        assert native_input.read_bytes()==original_data
        invalid=v1("turn.send",target=A,text="changed original",
                   delivery_id=v1_token)
        assert invalid["ok"] is False
        assert invalid["error"]["code"]=="refused"
        assert native_input.read_bytes()==original_data
        assert store.record(A)["lifecycle"]["hostPid"]==original_host
        assert store.record(A)["lifecycle"]["workerPid"]==original_child
        assert not (root/"sessions").exists()
        assert not (root/"codex").exists()
    finally:
        _stop_disposable_original(root)


@pytest.mark.parametrize("requested_name", [None, "Operator Defined Title"])
def test_host_a_real_detached_fake_claude_default_custom_and_stop_title(
    short_root, tmp_path, monkeypatch, requested_name,
):
    """A real detached host+PTY omits -n for ccx-<8>; Stop adopts AI title."""
    repo = Path(__file__).resolve().parent.parent
    fake = repo / "tests" / "fake_claude.py"
    wrapper = tmp_path / "host-a-fake-claude"
    argv_log = tmp_path / "native-argv.log"
    wrapper.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$@" > "$FAKE_ARGV_LOG"\nexec '
        + shlex.quote(sys.executable) + ' ' + shlex.quote(str(fake)) + ' "$@"\n',
        encoding="utf-8",
    )
    wrapper.chmod(0o700)
    env = {**os.environ, "CCX_ROOT": str(short_root),
           "CCX_CLAUDE_BIN": str(wrapper), "CCX_SPAWN_SCOPE": "0", "CCX_BOOT_TIMEOUT_S": "15",
           "FAKE_MODE": "", "FAKE_ARGV_LOG": str(argv_log),
           "FAKE_LOG": str(tmp_path / "native-stdin.log"),
           "PYTHONPATH": str(repo)}
    for name in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(name, None)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)

    def call(*args, timeout=20):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", *args], env=env,
            cwd=str(repo), capture_output=True, text=True, timeout=timeout,
        )

    new_args = ["new", "--engine", "claude", "--cwd", str(tmp_path), "--worker-id", A, "--json"]
    if requested_name is not None:
        new_args.extend(("--name", requested_name))
    try:
        created = call(*new_args)
        assert created.returncode in (0, 3), (created.stdout, created.stderr)
        store = require_ccx_active(short_root)
        until = time.monotonic() + 10
        while time.monotonic() < until:
            rec = store.record(A)
            if rec.get("lifecycle", {}).get("nativeStartProof"):
                break
            time.sleep(.04)
        assert store.record(A)["lifecycle"].get("nativeStartProof"), created.stderr
        if store.native_activation(A)["state"] != "confirmed":
            again = call(*new_args)
            assert again.returncode == 0, (again.stdout, again.stderr)
        assert argv_log.exists()
        args = argv_log.read_text().splitlines()
        if requested_name is None:
            assert "-n" not in args, args
            assert store.record(A)["name"] == "ccx-aaaaaaaa"
        else:
            assert "-n" in args and args[args.index("-n") + 1] == requested_name

        # The fake provider does not generate its own title; inject only its
        # native JSONL ai-title event, AFTER host start, so Stop tests the
        # production read_ai_title path rather than a stubbed unit callback.
        transcript = Path(store.record(A)["transcript"])
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text(json.dumps({
            "type": "ai-title", "aiTitle": "Generated Project Summary",
        }) + "\n", encoding="utf-8")
        token = "host-a-title-stop"
        sent = call("send", A, "title this real fake session", "--no-wait",
                    "--delivery-id", token, "--json")
        assert sent.returncode == 0, (sent.stdout, sent.stderr)
        until = time.monotonic() + 14
        settled = None
        while time.monotonic() < until:
            rec = store.record(A)
            if rec.get("lastSettledTurn", {}).get("turnRef"):
                settled = rec
                break
            time.sleep(.08)
        assert settled is not None, (
            store.record(A).get("lifecycle"),
            list(short_root.rglob("host.log")),
        )
        assert settled["stats"]["turns"] == settled["lastSettledTurn"]["turn"] == 1
        assert settled["lastSettledTurn"]["outcome"] == "completed"
        assert settled["name"] == (requested_name or "Generated Project Summary")
        assert "-n" not in args if requested_name is None else "-n" in args
    finally:
        _stop_disposable_original(short_root)


def test_host_a_real_fake_stop_after_external_terminal_write_never_resurrects(
    short_root, tmp_path, monkeypatch,
):
    """A native Stop can settle history after CLI terminal kill, never IDLE."""
    repo = Path(__file__).resolve().parent.parent
    provider = tmp_path / "gated-fake-claude.py"
    original = (repo / "tests" / "fake_claude.py").read_text(encoding="utf-8")
    needle = '            time.sleep(0.3)\n            hook("Stop", {'
    assert original.count(needle) == 1
    replacement = '''            deadline = time.monotonic() + 14
            while not os.path.exists(os.environ["FAKE_STOP_GATE"]) and time.monotonic() < deadline:
                time.sleep(.015)
            hook("Stop", {'''
    provider.write_text(original.replace(needle, replacement), encoding="utf-8")
    gate = tmp_path / "permit-native-stop"
    wrapper = tmp_path / "gated-claude-wrapper"
    wrapper.write_text(
        "#!/bin/sh\nexec " + shlex.quote(sys.executable) + " "
        + shlex.quote(str(provider)) + ' "$@"\n', encoding="utf-8",
    )
    wrapper.chmod(0o700)
    env = {**os.environ, "CCX_ROOT": str(short_root),
           "CCX_CLAUDE_BIN": str(wrapper), "CCX_SPAWN_SCOPE": "0", "CCX_BOOT_TIMEOUT_S": "15",
           "FAKE_STOP_GATE": str(gate), "FAKE_MODE": "",
           "FAKE_LOG": str(tmp_path / "native-stdin.log"),
           "PYTHONPATH": str(repo)}
    for name in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(name, None)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)

    def call(*args):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", *args], env=env,
            cwd=str(repo), capture_output=True, text=True, timeout=20,
        )

    try:
        creation = call("new", "--engine", "claude", "--cwd", str(tmp_path), "--worker-id", A, "--json")
        assert creation.returncode in (0, 3), (creation.stdout, creation.stderr)
        store = require_ccx_active(short_root)
        until = time.monotonic() + 10
        while time.monotonic() < until:
            if store.record(A)["lifecycle"].get("nativeStartProof"):
                break
            time.sleep(.05)
        assert store.record(A)["lifecycle"].get("nativeStartProof")
        if store.native_activation(A)["state"] != "confirmed":
            same = call("new", "--engine", "claude", "--cwd", str(tmp_path), "--worker-id", A, "--json")
            assert same.returncode == 0, (same.stdout, same.stderr)
        send = call("send", A, "stop should not revive", "--no-wait",
                    "--delivery-id", "host-a-terminal-fence", "--json")
        assert send.returncode == 0, (send.stdout, send.stderr)
        events = (short_root / "identity" / "generations" / G1 / "workers"
                  / "claude" / A / "events.ndjson")
        until = time.monotonic() + 12
        while time.monotonic() < until:
            if events.exists() and '"type": "turn_start"' in events.read_text():
                break
            time.sleep(.05)
        assert events.exists() and '"type": "turn_start"' in events.read_text()
        # The host broadcasts turn_start BEFORE it durably commits BUSY. An
        # external DEAD inside that gap correctly makes the host stop before
        # Stop (terminal wins), so wait for the durable busy owner first.
        until = time.monotonic() + 8
        while time.monotonic() < until:
            lifecycle = store.record(A)["lifecycle"]
            if lifecycle.get("state") == "busy" and lifecycle.get("hostPid"):
                break
            time.sleep(.02)
        assert lifecycle.get("state") == "busy" and lifecycle.get("hostPid"), lifecycle
        # Competing CLI/Python changes DURING a provider turn and before the
        # native Stop hook. All three should survive the host's settlement.
        store.rename_worker(A, engine="claude", name="Operator rename at Stop")
        store.mutate_record(A, patches={"createArgs.model": "manual-config"})
        store.patch_lifecycle(A, changes={"state": "dead", "lastError": "explicit CLI kill"})
        gate.touch()
        until = time.monotonic() + 8
        latest = None
        while time.monotonic() < until:
            latest = store.record(A)
            if latest.get("lastSettledTurn", {}).get("turnRef"):
                break
            time.sleep(.05)
        assert latest is not None and latest.get("lastSettledTurn", {}).get("turnRef"), latest
        assert latest["lifecycle"]["state"] == "dead"
        assert latest["lifecycle"]["lastError"] == "explicit CLI kill"
        assert latest["name"] == "Operator rename at Stop"
        assert latest["createArgs"]["model"] == "manual-config"
        assert latest["stats"]["turns"] == latest["lastSettledTurn"]["turn"] == 1
        assert latest["lastSettledTurn"]["outcome"] == "completed"
        time.sleep(.25)
        assert store.record(A)["lifecycle"]["state"] == "dead"
    finally:
        gate.touch()  # never strand the disposable provider in test cleanup
        _stop_disposable_original(short_root)


def test_r1_real_detached_fake_provider_0775_native_tree_renders_without_chmod0700(
    short_root, tmp_path, monkeypatch,
):
    """Real detached host/PTY refresh reads the fake provider's native JSONL.

    The fake itself creates its profile, projects and native project with
    umask 0002/0775. No test fixture repairs those modes to 0700 before the
    host refresh; the ORIGINAL provider ID, never the CCX UUID, names JSONL.
    No real provider binary is touched.
    """
    import re
    import stat

    repo = Path(__file__).resolve().parent.parent
    fake_original = (repo / "tests" / "fake_claude.py").read_text(encoding="utf-8")
    anchor = '            time.sleep(0.3)\n            hook("Stop", {'
    assert fake_original.count(anchor) == 1
    instrumented = '''            # Native Claude JSONL provider source, written by this fake's
            # actual detached PTY child before emitting its native Stop hook.
            import pathlib as _p
            import re as _re
            os.umask(0o002)
            _home = _p.Path(os.environ["CLAUDE_CONFIG_DIR"])
            _slug = _re.sub(r"[^a-zA-Z0-9]", "-", os.getcwd())
            _native_parent = _home / "projects" / _slug
            _native_parent.mkdir(parents=True, exist_ok=True)
            for _part in (_home, _home / "projects", _native_parent):
                _part.chmod(0o775)  # actual provider tree under private UPG
            _source = _native_parent / (SESSION_ID + ".jsonl")
            with _source.open("a", encoding="utf-8") as _native:
                _native.write(json.dumps({
                    "type": "user", "message": {"content": text},
                }) + "\\n")
                _native.write(json.dumps({
                    "type": "assistant", "message": {
                        "model": "fake-claude-recorded-native-model",
                        "content": [{"type": "text", "text": answer}],
                        "usage": {"input_tokens": 1200, "output_tokens": 22},
                    },
                }) + "\\n")
            _source.chmod(0o600)
'''
    fake = tmp_path / "fake-provider-0775.py"
    fake.write_text(fake_original.replace(anchor, instrumented + anchor, 1), encoding="utf-8")
    wrapper = tmp_path / "provider-0775-wrapper"
    wrapper.write_text(
        "#!/bin/sh\nexec " + shlex.quote(sys.executable) + " "
        + shlex.quote(str(fake)) + ' "$@"\n', encoding="utf-8",
    )
    wrapper.chmod(0o700)
    provider_home = tmp_path / "native-provider-home"
    env = {**os.environ,
           "CCX_ROOT": str(short_root), "CLAUDE_CONFIG_DIR": str(provider_home),
           "CCX_CLAUDE_BIN": str(wrapper), "CCX_SPAWN_SCOPE": "0", "CCX_BOOT_TIMEOUT_S": "15",
           "FAKE_MODE": "", "FAKE_LOG": str(tmp_path / "provider-stdin.log"),
           "PYTHONPATH": str(repo)}
    for field in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(field, None)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)

    def run(*args):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", *args],
            cwd=str(repo), env=env, capture_output=True, text=True, timeout=25,
        )

    new_args = ("new", "--engine", "claude", "--cwd", str(tmp_path), "--worker-id", A, "--json")
    try:
        started = run(*new_args)
        assert started.returncode in (0, 3), (started.stdout, started.stderr)
        store = require_ccx_active(short_root)
        until = time.monotonic() + 12
        while time.monotonic() < until:
            if store.record(A).get("lifecycle", {}).get("nativeStartProof"):
                break
            time.sleep(.05)
        assert store.record(A)["lifecycle"].get("nativeStartProof"), started.stderr
        if store.native_activation(A)["state"] != "confirmed":
            reconciled = run(*new_args)
            assert reconciled.returncode == 0, (reconciled.stdout, reconciled.stderr)
        sent = run("send", A, "prove 0775 provider render", "--no-wait",
                   "--delivery-id", "r1-fake-original", "--json")
        assert sent.returncode == 0, (sent.stdout, sent.stderr)
        rendered = None
        until = time.monotonic() + 18
        while time.monotonic() < until:
            rendered = store.read_worker_sidecar(A, "transcript.json")
            if (rendered and isinstance(rendered.get("context"), dict)
                    and rendered["context"].get("tokens_used") == 1200):
                break
            time.sleep(.08)
        native_record = store.record(A)
        original_sid = native_record["native"]["sessionId"]
        synthetic_slug = re.sub(r"[^a-zA-Z0-9]", "-", str(tmp_path))
        fake_jsonl = provider_home / "projects" / synthetic_slug / (original_sid + ".jsonl")
        host_log = (short_root / "identity" / "generations" / G1 / "workers"
                    / "claude" / A / "host.log")
        from ccx.claude.transcript import _native_source_directory_safe
        native_dirs = [provider_home, provider_home / "projects", fake_jsonl.parent]
        unsafe_group_dirs = [
            part for part in native_dirs
            if part.exists() and not _native_source_directory_safe(part.stat())
        ]
        if unsafe_group_dirs:
            # A Linux user-private UPG can safely own 0775 provider paths.
            # macOS's shared 'staff' group (gid 20) cannot. The same pinned
            # native original must be REFUSED rather than parsed or published;
            # never chmod a provider-owned directory to change the result.
            assert fake_jsonl.is_file() and fake_jsonl.stat().st_size > 0
            assert rendered is None
            from ccx.store import paths as native_paths
            from ccx.claude.transcript import open_original_claude_source
            from ccx.store.root import StorageIntegrityError
            with monkeypatch.context() as patch:
                patch.setattr(native_paths, "CLAUDE_HOME", provider_home)
                with pytest.raises(StorageIntegrityError, match="unsafe native Claude source directory"):
                    with open_original_claude_source(
                        store, store.resolve(f"claude:{A}"), native_record,
                    ):
                        pytest.fail("opened unsafe shared-group provider source")
            return
        if (rendered is None or
                (rendered.get("context") or {}).get("tokens_used") != 1200):
            # This probe reads ONLY the fake provider's own disposable files.
            # Mac's shared 'staff' group may not qualify as a private UPG;
            # 0775 must never silently bypass the native inode trust gate.
            print("CCX_SYNTHETIC_NATIVE_DIRS", [
                (str(part), oct(stat.S_IMODE(part.stat().st_mode)),
                 part.stat().st_gid, _native_source_directory_safe(part.stat()))
                for part in native_dirs if part.exists()
            ], flush=True)
            print("CCX_SYNTHETIC_SOURCE", str(fake_jsonl),
                  "exists", fake_jsonl.exists(),
                  "bytes", fake_jsonl.stat().st_size if fake_jsonl.exists() else None,
                  "record_transcript", native_record.get("transcript"), flush=True)
            print("CCX_SYNTHETIC_HOST_LOG",
                  host_log.read_text(errors="replace")[-2800:]
                  if host_log.exists() else "<missing>", flush=True)
        assert rendered is not None and rendered["context"]["tokens_used"] == 1200, {
            "record": native_record,
            "synthetic_provider_source": str(fake_jsonl),
            "source_exists": fake_jsonl.exists(),
            "source_bytes": fake_jsonl.stat().st_size if fake_jsonl.exists() else None,
            "host_log_tail": host_log.read_text(errors="replace")[-4000:]
                             if host_log.exists() else "absent",
            "rendered": rendered,
        }
        rec = store.record(A)
        assert rec["native"]["sessionId"] != A
        assert rendered["worker"] == f"claude:{A}"
        assert rendered["sessionId"] == rec["native"]["sessionId"]
        assert rendered["turns"][-1]["model"] == "fake-claude-recorded-native-model"
        assert rendered["turns"][-1]["text"] == "Fake Claude completed: prove 0775 provider render"
        observed = store.read_worker_sidecar(A, "context-observation.json")
        assert observed["workerId"] == A
        assert observed["nativeSessionId"] == rec["native"]["sessionId"]
        assert observed["context"]["tokens_used"] == 1200
        assert observed["observedModel"] == "fake-claude-recorded-native-model"
        from ccx.workers import WorkerClient
        facade = WorkerClient()
        summary = facade.summaries([A])["workers"][0]
        assert summary["observed_model"] == "fake-claude-recorded-native-model"
        assert summary["context"]["tokens_used"] == 1200
        assert facade.context(A)["tokens_used"] == 1200
        native = Path(rec["transcript"])
        assert native.exists()
        assert native.name == rec["native"]["sessionId"] + ".jsonl"
        assert stat.S_IMODE(native.stat().st_mode) == 0o600
        assert [stat.S_IMODE(path.stat().st_mode) for path in
                (provider_home, provider_home / "projects", native.parent)] == [0o775] * 3
        published = store.worker_path(A, "transcript.json")
        assert stat.S_IMODE(published.stat().st_mode) == 0o600
        assert not list(published.parent.glob(".new-*"))
    finally:
        _stop_disposable_original(short_root)


@pytest.fixture
def generated_hook_host(short_root, tmp_path, monkeypatch, request):
    """Opt-in native fake executes the REAL per-worker generated hook commands."""
    repo = Path(__file__).resolve().parent.parent
    fake = repo / "tests" / "fake_claude.py"
    wrapper = tmp_path / "generated-hook-fake-claude"
    wrapper.write_text(
        "#!/bin/sh\nexec " + shlex.quote(sys.executable) + " "
        + shlex.quote(str(fake)) + ' "$@"\n', encoding="utf-8",
    )
    wrapper.chmod(0o700)
    hook_log = tmp_path / "generated-hook-results.jsonl"
    env = {**os.environ,
           "CCX_ROOT": str(short_root),
           "CLAUDE_CONFIG_DIR": str(tmp_path / "native-claude-home"),
           "CCX_CLAUDE_BIN": str(wrapper), "CCX_SPAWN_SCOPE": "0", "CCX_BOOT_TIMEOUT_S": "15",
           "FAKE_HOOKS_VIA_SETTINGS": "1",
           "FAKE_HOOK_RESULT_LOG": str(hook_log),
           "FAKE_LOG": str(tmp_path / "native-stdin.log"),
           "PYTHONPATH": str(repo)}
    for name in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(name, None)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setenv("CCX_ROOT", str(short_root))

    def call(*args, timeout=25, env_override=None):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", *args],
            env=env if env_override is None else env_override,
            cwd=str(repo), capture_output=True, text=True, timeout=timeout,
        )

    try:
        create_args = ("new", "--engine", "claude", "--cwd", str(tmp_path), "--worker-id", A,
                       *(("--permissions", "restricted") if getattr(request, "param", None) == "restricted" else ()),
                       "--json")
        started = call(*create_args)
        assert started.returncode in (0, 3), (started.stdout, started.stderr)
        store = require_ccx_active(short_root)
        until = time.monotonic() + 12
        while time.monotonic() < until:
            rec = store.record(A)
            if rec.get("lifecycle", {}).get("nativeStartProof"):
                break
            time.sleep(.05)
        assert store.record(A)["lifecycle"].get("nativeStartProof"), (
            started.stdout, started.stderr,
            (short_root / "identity" / "generations" / G1 /
             "workers" / "claude" / A / "host.log").read_text(errors="replace")[-5000:]
            if (short_root / "identity" / "generations" / G1 /
                "workers" / "claude" / A / "host.log").exists() else "host log absent",
        )
        if store.native_activation(A)["state"] != "confirmed":
            retry = call(*create_args)
            assert retry.returncode == 0, (retry.stdout, retry.stderr)
        yield store, call, env, hook_log
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("original_kind", ["new", "send"])
def test_unsettled_original_refuses_immediate_strict_send_without_pending_receipt(
    generated_hook_host, tmp_path, original_kind,
):
    """A second strict send is definitely refused, not left submitted-unknown."""
    from uuid import uuid4
    from ccx.ops import forking
    from ccx.claude.delivery import iter_receipts
    from ccx.ops.wait import reconcile_turn
    from ccx.rpc.rpc import _rpc
    from test_fork_host import _fake_copy_and_pty_wrapper

    store, call, env, _hooks = generated_hook_host
    worker_id = str(uuid4())
    gate = tmp_path / f"{original_kind}-release-render"
    fake_env = {**env, "FAKE_MODE": "render_hold", "FAKE_RENDER_GATE": str(gate)}
    original_delivery = "create:initial" if original_kind == "new" else "first-ordinary"
    try:
        created = call(
            "new", "--engine", "claude", "--cwd", str(tmp_path),
            "--worker-id", worker_id, "--json",
            *(("FIRST FROM NEW",) if original_kind == "new" else ()),
            env_override=fake_env,
        )
        assert created.returncode == 0, (created.stdout, created.stderr)
        if original_kind == "new":
            assert json.loads(created.stdout)["delivery_status"] == "owned"
            assert "waited" not in json.loads(created.stdout)
        else:
            first = call(
                "send", worker_id, "FIRST ORDINARY SEND", "--no-wait",
                "--delivery-id", original_delivery, "--json",
            )
            assert first.returncode == 0, (first.stdout, first.stderr)
            assert json.loads(first.stdout)["status"] == "owned"

        # Native Ink is still rendering, so the first ORIGINAL delivery owns
        # the input surface but has not written to the PTY. Host is genuinely
        # idle; a stale preflight cannot be our explanation for the refusal.
        status = _rpc(worker_id, {"op": "status"}, timeout=1)
        assert status["state"] == "idle", status
        second = call(
            "send", worker_id, "SECOND STRICT SEND", "--wait",
            "--timeout", "8", "--json", timeout=16,
        )
        assert second.returncode == 5, (second.stdout, second.stderr)
        assert "busy" in second.stderr.lower()
        refused = json.loads(second.stdout)
        assert refused["status"] == "rejected", refused
        delivery_id = refused["delivery_id"]
        assert refused["delivery"]["rejection_code"] == "busy"
        assert store.read_receipt(worker_id, "claude-delivery", delivery_id)["state"] == "rejected"
        assert store.record(worker_id)["lifecycle"]["state"] == "idle"
        assert not any(
            row.get("deliveryId") == delivery_id
            for row in iter_receipts(worker_id)
            if row["state"] in {"prepared", "owned"}
        )

        gate.touch()
        deadline = time.monotonic() + 22
        while time.monotonic() < deadline:
            if reconcile_turn(worker_id, delivery_id=original_delivery)["status"] == "settled":
                break
            time.sleep(.1)
        else:
            pytest.fail("the original first turn did not settle after releasing fake Ink")

        # The same prompt, with a new independent delivery ID, now succeeds.
        retried = call(
            "send", worker_id, "SECOND STRICT SEND", "--wait", "--timeout",
            "30", "--json", timeout=40,
        )
        assert retried.returncode == 0, (retried.stdout, retried.stderr)
        sent = json.loads(retried.stdout)
        assert sent["settled"] is True
        assert sent["report"]["text"] == "Fake Claude completed: SECOND STRICT SEND"
        # This is fork's real source-admission guard. A false submitted-unknown
        # effect/semantic receipt would remain pending and refuse the fork.
        record = store.record(worker_id)
        forking._settled(record)
        forking._no_pending_effects(store.resolve(worker_id), record)

        # Give the disposable fake's transcript a complete native UUID-bound
        # assistant row, then drive the real fork CLI and native copy/PTY fake.
        # The ordinary fake's minimal rows deliberately omit native UUIDs.
        with Path(record["transcript"]).open("a", encoding="utf-8") as source:
            source.write(json.dumps({
                "type": "assistant", "uuid": str(uuid4()),
                "sessionId": record["native"]["sessionId"],
                "message": {
                    "stop_reason": "end_turn",
                    "content": [{"type": "text", "text": "fake fork anchor"}],
                },
            }) + "\n")
        fork_trace = tmp_path / f"{original_kind}-fork-trace"
        fork_env = {
            **env, "CCX_CLAUDE_BIN": str(_fake_copy_and_pty_wrapper(tmp_path)),
            "FORK_NATIVE_ARGV_LOG": str(fork_trace),
            "FORK_FAKE_PTY": str(Path(__file__).with_name("fake_claude.py")),
        }
        fork = call(
            "fork", worker_id, "--name", f"{original_kind}-settled-fork",
            "--timeout", "45", "--json", env_override=fork_env, timeout=55,
        )
        assert fork.returncode == 0, (fork.stdout, fork.stderr)
        assert json.loads(fork.stdout)["native_ready"] is True
    finally:
        gate.touch()
        call("shutdown", "--timeout", "20", "--json", timeout=30)


def _generated_hook_results(log):
    return [json.loads(row) for row in log.read_text().splitlines()] if log.exists() else []


def test_generated_full_uuid_hooks_reach_real_host_and_settle_original_native_turn(
    generated_hook_host,
):
    store, call, _env, hook_log = generated_hook_host
    original = store.record(A)
    sid = original["native"]["sessionId"]
    assert sid != A
    proof = original["lifecycle"]["nativeStartProof"]
    assert proof["sessionId"] == sid and proof["workerId"] == A
    assert proof["peerPid"] == proof["workerPid"]
    assert proof["relayPeerPid"] != proof["workerPid"]
    assert proof["relayPeerStart"]
    start = next((e for e in _generated_hook_results(hook_log)
                  if e["event"] == "SessionStart"), None)
    assert start and start["rc"] == 0 and start["stdout"] == "{}\n", start

    delivery = "generated-hooks-original-turn"
    sent = call("send", A, "generated hook lifecycle", "--no-wait",
                "--delivery-id", delivery, "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    until = time.monotonic() + 15
    observation = None
    while time.monotonic() < until:
        rec = store.record(A)
        observation = store.read_worker_sidecar(A, "context-observation.json")
        if (rec.get("lastSettledTurn", {}).get("outcome") == "completed"
                and observation and observation.get("context", {}).get("tokens_used") == 1234):
            break
        time.sleep(.08)
    assert rec["lastSettledTurn"]["outcome"] == "completed", rec
    assert observation is not None and observation["context"]["tokens_used"] == 1234
    assert observation["nativeSessionId"] == sid
    assert observation["observedModel"] == "fake-claude-generated-hooks-model"
    # Internal delivery acceptance is asynchronous and may remain
    # submitted-unknown after a positively completed host turn.
    assert store.read_receipt(A, "claude-delivery", delivery)["state"] in (
        "accepted", "submitted-unknown",
    )
    report = call("report", A, "--json")
    assert report.returncode == 0, (report.stdout, report.stderr)
    assert json.loads(report.stdout)["delivery_id"] == delivery
    results = _generated_hook_results(hook_log)
    for event in ("SessionStart", "UserPromptSubmit", "PreToolUse", "Stop"):
        assert any(r["event"] == event and r["rc"] == 0
                   and r["stdout"] == "{}\n" for r in results), results
    events = (store.pointer.generation_root / "workers" / "claude" / A / "events.ndjson")
    event_lines = [json.loads(line) for line in events.read_text().splitlines()]
    if not any(event.get("type") == "tool_started" for event in event_lines):
        # Preserve the exact event assertion; make Mac asynchronous hook
        # ordering visible without running any real provider or opening auth.
        print("CCX_SYNTHETIC_EVENT_TYPES",
              [event.get("type") for event in event_lines], flush=True)
        print("CCX_SYNTHETIC_HOOK_RESULTS",
              [(r.get("event"), r.get("rc"), r.get("stderr"))
               for r in results], flush=True)
    assert any(event.get("type") == "tool_started" for event in event_lines), {
        "event_types": [event.get("type") for event in event_lines],
        "hook_events": results,
        "record_lifecycle": rec.get("lifecycle"),
    }
    native = Path(rec["transcript"])
    assert native.name == sid + ".jsonl" and native.exists()
    assert not (constants.CCX_ROOT / "sessions").exists()


def test_generated_full_uuid_question_reaches_broker_and_returns_real_answer(
    generated_hook_host,
):
    from ccx.client import CcxClient
    from ccx.errors import DeliveryRejected, SessionUnavailable

    store, call, _env, hook_log = generated_hook_host
    sent = call("send", A, "ask generated question", "--no-wait",
                "--delivery-id", "generated-hook-question", "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    until = time.monotonic() + 12
    waiting = None
    while time.monotonic() < until:
        waiting = store.record(A).get("waitingFor")
        if isinstance(waiting, dict) and waiting.get("id"):
            break
        time.sleep(.04)
    assert waiting is not None and waiting.get("id"), store.record(A)
    assert waiting["tool"] == "AskUserQuestion" or waiting.get("tool_name") == "AskUserQuestion", waiting
    client = CcxClient()
    assert client.peek(A).waiting_for.request_id == waiting["id"]
    status = call("peek", A, "--status", "--json")
    assert status.returncode == 0, (status.stdout, status.stderr)
    visible = json.loads(status.stdout)["statuses"][0]
    assert visible["pending_input"]["request_id"] == waiting["id"]
    assert visible["pending_input"]["kind"] == "question"
    with pytest.raises(DeliveryRejected, match="request id mismatch"):
        client.answer(A, "Yes", request_id="stale-gate-id")
    with pytest.raises(SessionUnavailable):
        client.approve(A, request_id=waiting["id"])
    cli_answer = call("answer", A, "Yes", "--request-id", waiting["id"], "--json")
    assert cli_answer.returncode == 0, (cli_answer.stdout, cli_answer.stderr)
    assert json.loads(cli_answer.stdout)["request_id"] == waiting["id"]
    until = time.monotonic() + 15
    results = []
    while time.monotonic() < until:
        results = _generated_hook_results(hook_log)
        if any(r["event"] == "PermissionRequest" for r in results):
            break
        time.sleep(.06)
    reply = next(r for r in results if r["event"] == "PermissionRequest")
    assert reply["rc"] == 0 and not reply["stderr"], reply
    decision = json.loads(reply["stdout"])["hookSpecificOutput"]["decision"]
    assert decision["behavior"] == "allow"
    assert decision["updatedInput"]["answers"] == {"Which option?": "Yes"}
    # After the first answer, the same native child may ask again. The old
    # request ID must never answer that distinct successor.
    until = time.monotonic() + 12
    while time.monotonic() < until:
        rec = store.record(A)
        if (rec.get("lastSettledTurn", {}).get("outcome") == "completed"
                and (rec.get("lifecycle") or {}).get("state") == "idle"):
            break
        time.sleep(.04)
    assert rec["lastSettledTurn"]["outcome"] == "completed", rec
    again = call("send", A, "ask generated question", "--no-wait",
                 "--delivery-id", "generated-hook-question-again", "--json")
    assert again.returncode == 0, (again.stdout, again.stderr)
    until = time.monotonic() + 12
    successor = None
    while time.monotonic() < until:
        successor = store.record(A).get("waitingFor")
        if isinstance(successor, dict) and successor.get("id") != waiting["id"]:
            break
        time.sleep(.04)
    assert successor and successor["id"] != waiting["id"], successor
    with pytest.raises(DeliveryRejected, match="request id mismatch"):
        client.answer(A, "Yes", request_id=waiting["id"])
    assert store.record(A)["waitingFor"]["id"] == successor["id"]
    answered = client.answer(A, "Yes", request_id=successor["id"])
    assert answered.worker_id == A and answered.queued
    assert not (constants.CCX_ROOT / "sessions").exists()


@pytest.mark.parametrize("target_kind", ["wrong-native", "unreadable-record"])
def test_generated_relay_rejects_invalid_original_without_implicit_permission(
    generated_hook_host, target_kind,
):
    store, call, env, _hook_log = generated_hook_host
    original = store.record(A)
    if target_kind == "wrong-native":
        worker = A
        payload_sid = "ffffffff-ffff-4fff-8fff-ffffffffffff"
    else:
        from test_generation_store import B
        worker = B
        payload_sid = original["native"]["sessionId"]
    relay_env = {**env, "CCX_SELF_WORKER_ID": worker, "CCX_SELF_ENGINE": "claude"}
    payload = {"session_id": payload_sid, "tool_name": "Bash",
               "tool_input": {"command": "must never run"}}
    proc = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "hooks-relay", worker, "PermissionRequest"],
        env=relay_env, input=json.dumps(payload), text=True, capture_output=True,
        cwd=str(Path(__file__).resolve().parent.parent), timeout=15,
    )
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert json.loads(proc.stdout)["hookSpecificOutput"]["decision"]["behavior"] == "deny"
    # The existing MCP elicitation failure contract is cancel, never an
    # interactive native fallback or an unstructured permission reply.
    cancel = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "hooks-relay", worker, "Elicitation"],
        env=relay_env, input=json.dumps({"session_id": payload_sid}), text=True,
        capture_output=True, cwd=str(Path(__file__).resolve().parent.parent), timeout=15,
    )
    assert cancel.returncode == 0, (cancel.stdout, cancel.stderr)
    assert json.loads(cancel.stdout)["hookSpecificOutput"]["action"] == "cancel"
    assert not store.record(A).get("waitingFor")
    assert not (constants.CCX_ROOT / "sessions").exists()


@pytest.mark.parametrize("generated_hook_host", ["restricted"], indirect=True)
def test_generated_full_uuid_restricted_permission_worker_and_cli_decisions(
    generated_hook_host,
):
    """The original restricted native input is resumed by exact bound responses."""
    from ccx.client import CcxClient
    from ccx.workers import WorkerClient

    store, call, _env, hook_log = generated_hook_host
    assert store.record(A)["createArgs"]["skip_permissions"] is False
    sent = call("send", A, "request generated permission", "--no-wait",
                "--delivery-id", "restricted-permission-first", "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    until = time.monotonic() + 12
    pending = None
    while time.monotonic() < until:
        pending = store.record(A).get("waitingFor")
        if isinstance(pending, dict) and pending.get("id"):
            break
        time.sleep(.04)
    assert pending and pending["tool"] == "Bash", pending
    assert CcxClient().peek(A).waiting_for.request_id == pending["id"]
    worker = WorkerClient()
    assert worker.capabilities("claude")["answer_question"] is True
    # The cross-engine facade alone discovers the ID its approve requires.
    projected = worker.status(A)["pending_input"]
    assert projected["kind"] == "permission" and projected["tool"] == "Bash"
    assert projected["request_id"] == pending["id"]
    confirmed = worker.approve(A, request_id=projected["request_id"])
    assert confirmed["worker_id"] == A and confirmed["decision"] == "allow"
    until = time.monotonic() + 15
    replies = []
    while time.monotonic() < until:
        replies = [r for r in _generated_hook_results(hook_log)
                   if r["event"] == "PermissionRequest"]
        if replies:
            break
        time.sleep(.04)
    assert replies and replies[-1]["rc"] == 0, replies
    granted = json.loads(replies[-1]["stdout"])["hookSpecificOutput"]["decision"]
    assert granted["behavior"] == "allow"

    until = time.monotonic() + 12
    while time.monotonic() < until:
        rec = store.record(A)
        if ((rec.get("lifecycle") or {}).get("state") == "idle"
                and rec.get("lastSettledTurn", {}).get("outcome") == "completed"):
            break
        time.sleep(.04)
    assert (rec.get("lifecycle") or {}).get("state") == "idle", rec
    sent_again = call("send", A, "request generated permission", "--no-wait",
                      "--delivery-id", "restricted-permission-second", "--json")
    assert sent_again.returncode == 0, (sent_again.stdout, sent_again.stderr)
    until = time.monotonic() + 12
    successor = None
    while time.monotonic() < until:
        successor = store.record(A).get("waitingFor")
        if isinstance(successor, dict) and successor.get("id") != pending["id"]:
            break
        time.sleep(.04)
    assert successor and successor["id"] != pending["id"], successor
    old = call("reject", A, "--request-id", pending["id"], "--json")
    assert old.returncode == 5, (old.stdout, old.stderr)
    assert store.record(A)["waitingFor"]["id"] == successor["id"]
    assert worker.status(A)["pending_input"]["request_id"] == successor["id"]
    denied = call("reject", A, "--request-id", successor["id"],
                  "--reason", "fake test denial", "--json")
    assert denied.returncode == 0, (denied.stdout, denied.stderr)
    assert json.loads(denied.stdout)["request_id"] == successor["id"]
    until = time.monotonic() + 15
    while time.monotonic() < until:
        replies = [r for r in _generated_hook_results(hook_log)
                   if r["event"] == "PermissionRequest"]
        if len(replies) == 2:
            break
        time.sleep(.04)
    assert len(replies) == 2
    rejected = json.loads(replies[-1]["stdout"])["hookSpecificOutput"]["decision"]
    assert rejected["behavior"] == "deny"
    assert not (constants.CCX_ROOT / "sessions").exists()


def test_generated_full_uuid_mcp_elicitation_typed_schema_and_cli_response(
    generated_hook_host,
):
    from ccx.client import CcxClient
    from ccx.errors import DeliveryRejected, InvalidRequest, SessionUnavailable

    store, call, _env, hook_log = generated_hook_host
    sent = call("send", A, "request generated elicitation", "--no-wait",
                "--delivery-id", "mcp-elicitation-1", "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    until = time.monotonic() + 12
    pending = None
    while time.monotonic() < until:
        pending = store.record(A).get("waitingFor")
        if isinstance(pending, dict) and pending.get("id") and pending.get("type") == "elicitation":
            break
        time.sleep(.04)
    assert pending and pending.get("type") == "elicitation", pending
    assert pending["server"] == "fake-mcp"
    client = CcxClient()
    assert client.peek(A).waiting_for.request_id == pending["id"]
    with pytest.raises(InvalidRequest, match="unknown field"):
        client.respond_elicitation(A, "accept", {"unknown": "bad"},
                                  request_id=pending["id"])
    with pytest.raises(DeliveryRejected, match="request id mismatch"):
        client.respond_elicitation(A, "cancel", request_id="stale-mcp-id")
    with pytest.raises(SessionUnavailable):
        client.reject(A, request_id=pending["id"])
    response = call("respond", A, "--accept", "--field", "title=Text",
                    "--request-id", pending["id"], "--json")
    assert response.returncode == 0, (response.stdout, response.stderr)
    assert json.loads(response.stdout)["request_id"] == pending["id"]
    until = time.monotonic() + 15
    replies = []
    while time.monotonic() < until:
        replies = [r for r in _generated_hook_results(hook_log)
                   if r["event"] == "Elicitation"]
        if replies:
            break
        time.sleep(.04)
    assert replies and replies[-1]["rc"] == 0, replies
    elicited = json.loads(replies[-1]["stdout"])["hookSpecificOutput"]
    assert elicited["action"] == "accept" and elicited["content"] == {"title": "Text"}
    assert not (constants.CCX_ROOT / "sessions").exists()
