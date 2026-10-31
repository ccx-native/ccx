from __future__ import annotations

import base64
import json
from pathlib import Path
import socket
import subprocess
import sys
import sysconfig
import threading
import time

import pytest

from ccx.controllers import live_vars
from test_generation_store import A, B, add_worker, fixture_root


@pytest.fixture
def variable_root(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    monkeypatch.setattr(live_vars.constants, "CCX_ROOT", root)
    monkeypatch.setenv("CCX_ROOT", str(root))
    add_worker(root, A, "claude")
    add_worker(root, B, "codex")
    return root


def _request(owner: live_vars.LiveOwner, engine: str, short: str, code: str):
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(owner.socket_path))
    req = {
        "schemaVersion": 1,
        "op": "exec",
        "token": owner.token,
        "engine": engine,
        "short": short,
        "args": ["-c", code],
        "cwd": str(Path.cwd()),
        "stdinBase64": "",
        "nativePython": {
            "version": list(sys.version_info[:3]),
            "cacheTag": sys.implementation.cache_tag,
            "platform": sysconfig.get_platform(),
            "executable": sys.executable,
            "prefix": sys.prefix,
            "execPrefix": sys.exec_prefix,
            "sysPath": list(sys.path),
        },
    }
    client.sendall((json.dumps(req) + "\n").encode())
    data = bytearray()
    while b"\n" not in data:
        chunk = client.recv(65536)
        if not chunk:
            break
        data.extend(chunk)
    client.close()
    return json.loads(bytes(data).split(b"\n", 1)[0])


def test_cli_var_parsing_and_validation():
    assert live_vars.parse_cli_vars(["count=2", 'evidence={"ok":true}']) == {
        "count": 2,
        "evidence": {"ok": True},
    }
    with pytest.raises(Exception, match="duplicate"):
        live_vars.parse_cli_vars(["count=1", "count=2"])
    with pytest.raises(Exception, match="identifiers"):
        live_vars.parse_cli_vars(["not-valid=1"])
    with pytest.raises(Exception, match="valid JSON"):
        live_vars.parse_cli_vars(["count=nope"])

def test_durable_state_can_be_shared_across_engines(variable_root):
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"count": 1, "evidence": []}),
        "claude",
        A,
    )
    source = live_vars.prepare_shared_durable_attachment("claude", A)
    live_vars.publish_attachment(source, "codex", B)

    claude_info = live_vars.variables_info("claude", A)
    codex_info = live_vars.variables_info("codex", B)
    assert claude_info["state_id"] == codex_info["state_id"]
    assert live_vars.variables_info(
        "codex", B, name="count"
    )["value"] == 1

    # A normal later durable attachment on either worker mutates the shared
    # state rather than silently forking a worker-local copy.
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"count": 2}),
        "codex",
        B,
    )
    assert live_vars.variables_info(
        "claude", A, name="count"
    )["value"] == 2


def test_top_level_rebind_updates_retained_bindings_not_stale_local_alias(
    variable_root,
):
    original = {"route": None}
    bindings = {"result": original}
    owner = live_vars.LiveOwner()
    try:
        plan = live_vars.PreparedAttachment(
            mode="live",
            names=["result"],
            mapping=bindings,
            owner=owner,
        )
        live_vars.publish_attachment(plan, "claude", A)
        reply = _request(
            owner,
            "claude",
            A,
            "result = {'route': 'repair', 'finding_ids': ['F1']}",
        )
        assert reply["ok"] is True
        assert bindings["result"] == {
            "route": "repair",
            "finding_ids": ["F1"],
        }
        # A separate local alias still points at the object that was originally
        # bound. Controller authors must read final top-level bindings from the
        # retained mapping they passed through variables=.
        assert original == {"route": None}
    finally:
        owner.close()


def test_live_owner_mutation_rebind_delete_and_error_writeback(variable_root):
    class Box:
        def __init__(self):
            self.count = 1

    mapping = {"box": Box(), "result": None}
    owner = live_vars.LiveOwner()
    try:
        plan = live_vars.PreparedAttachment(
            mode="live",
            names=["box", "result"],
            mapping=mapping,
            owner=owner,
        )
        live_vars.publish_attachment(plan, "codex", B)

        reply = _request(
            owner,
            "codex",
            B,
            "box.count += 2; result = 'bugs_found'; scratch = 4",
        )
        assert reply["ok"] is True
        assert mapping["box"].count == 3
        assert mapping["result"] == "bugs_found"

        reply = _request(
            owner,
            "codex",
            B,
            "assert 'scratch' not in globals(); del result",
        )
        assert reply["ok"] is True
        assert "result" not in mapping
        assert sorted(mapping) == ["box"]

        mapping["result"] = {"items": []}
        plan = live_vars.PreparedAttachment(
            mode="live",
            names=["result"],
            mapping=mapping,
            owner=owner,
        )
        live_vars.publish_attachment(plan, "codex", B)
        reply = _request(
            owner,
            "codex",
            B,
            "result['items'].append(1); result={'status':'changed'}; "
            "raise RuntimeError('boom')",
        )
        assert reply["ok"] is True
        assert reply["exitCode"] == 1
        assert mapping["result"] == {"status": "changed"}
        stderr = base64.b64decode(reply["stderrBase64"]).decode()
        assert "RuntimeError: boom" in stderr
    finally:
        owner.close()


def test_live_owner_calls_queue(variable_root):
    mapping = {"count": 0}
    owner = live_vars.LiveOwner()
    try:
        live_vars.publish_attachment(live_vars.PreparedAttachment(
            mode="live", names=["count"], mapping=mapping, owner=owner,
        ), "claude", A)
        result = {}

        def slow():
            result["slow"] = _request(
                owner, "claude", A, "import time; time.sleep(2.1); count += 1",
            )

        thread = threading.Thread(target=slow)
        thread.start()
        time.sleep(.1)
        second = _request(owner, "claude", A, "count += 100")
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert result["slow"]["ok"] is True
        assert second["ok"] is True
        assert second["exitCode"] == 0
        assert mapping["count"] == 101
    finally:
        owner.close()


def test_durable_runner_serializes_and_rejects_partial_complex_write(variable_root):
    plan = live_vars.prepare_durable_attachment(
        {"counter": 0, "evidence": {"findings": []}}
    )
    live_vars.publish_attachment(plan, "claude", A)
    state_id = live_vars.durable_state_id("claude", A)
    state = live_vars.state_path(state_id)
    lock = live_vars.state_lock_path(state_id)

    def run(code):
        return subprocess.run(
            live_vars._durable_runner_argv(
                sys.executable,
                state=state,
                lock=lock,
                cwd=str(Path.cwd()),
                args=["-c", code],
            ),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )

    assert run("counter=2; evidence['ok']=True").returncode == 0
    assert live_vars.variables_info(
        "claude", A, name="counter"
    )["value"] == 2
    assert live_vars.variables_info(
        "claude", A, name="evidence"
    )["value"] == {"findings": [], "ok": True}

    before = state.read_text()
    bad = run("counter=99; evidence={'bad': object()}")
    assert bad.returncode != 0
    assert "CcxStateValueUnsupported" in bad.stderr
    assert state.read_text() == before

    document = json.loads(state.read_text())
    document["variables"]["counter"] = 0
    state.write_text(json.dumps(document))

    code = "import time; value=counter; time.sleep(.12); counter=value+1"
    argv = live_vars._durable_runner_argv(
        sys.executable,
        state=state,
        lock=lock,
        cwd=str(Path.cwd()),
        args=["-c", code],
    )
    p1 = subprocess.Popen(argv)
    p2 = subprocess.Popen(argv)
    assert p1.wait(timeout=5) == 0
    assert p2.wait(timeout=5) == 0
    assert live_vars.variables_info(
        "claude", A, name="counter"
    )["value"] == 2

def test_workerclient_variables_require_full_permissions(variable_root, tmp_path):
    from ccx import CapabilityUnsupported, WorkerClient

    client = WorkerClient()
    with pytest.raises(CapabilityUnsupported, match="full permissions"):
        client.create(
            engine="claude",
            cwd=str(tmp_path),
            permissions="restricted",
            variables={"result": None},
        )


def test_durable_runner_argv_avoids_package_script_startup(variable_root):
    argv = live_vars._durable_runner_argv(
        "/usr/bin/python3",
        state=live_vars.state_path("0" * 32),
        lock=live_vars.state_lock_path("0" * 32),
        cwd="/tmp",
        args=["-c", "print(1)"],
    )
    assert argv[:2] == ["/usr/bin/python3", "-c"]
    assert "runpy.run_path" in argv[2]
    assert "live_vars_runner.py" in argv[2]
    assert argv[3:5] == ["--state", str(live_vars.state_path("0" * 32))]


def test_live_owner_close_during_active_invocation_settles_cleanly(variable_root):
    mapping = {"count": 0}
    owner = live_vars.LiveOwner()
    plan = live_vars.PreparedAttachment(
        mode="live", names=["count"], mapping=mapping, owner=owner
    )
    live_vars.publish_attachment(plan, "claude", A)

    result = {}

    def run():
        result["reply"] = _request(
            owner,
            "claude",
            A,
            "import time; time.sleep(.1); count += 1",
        )

    thread = threading.Thread(target=run)
    thread.start()
    time.sleep(.02)
    owner.close()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert result["reply"]["ok"] is True
    assert mapping["count"] == 1

    deadline = time.time() + 1
    while (
        sys.stdout is owner._stdout_proxy
        or sys.stderr is owner._stderr_proxy
        or sys.stdin is owner._stdin_proxy
    ):
        if time.time() >= deadline:
            raise AssertionError("owner stream proxies were not restored")
        time.sleep(.01)


def test_durable_success_only_transaction_and_limit(variable_root):
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"counter": 0, "evidence": {"saved": False}}),
        "claude", A,
    )
    state_id = live_vars.durable_state_id("claude", A)
    state = live_vars.state_path(state_id)

    def run(code):
        return subprocess.run(
            live_vars._durable_runner_argv(
                sys.executable, state=state,
                lock=live_vars.state_lock_path(state_id),
                cwd=str(Path.cwd()), args=["-c", code],
            ),
            stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=10,
        )

    before = state.read_bytes()
    failed = run("counter=4; evidence['saved']=True; raise RuntimeError('boom')")
    assert failed.returncode == 1 and "RuntimeError: boom" in failed.stderr
    assert "live_vars_runner.py" not in failed.stderr
    assert state.read_bytes() == before

    exited = run("import sys; counter=10; sys.exit(7)")
    assert exited.returncode == 7 and state.read_bytes() == before
    killed = run("counter=10; raise KeyboardInterrupt")
    assert killed.returncode == 130 and state.read_bytes() == before

    assert run("counter=3; import sys; sys.exit(0)").returncode == 0
    assert live_vars.variables_info("claude", A, "counter")["value"] == 3
    before = state.read_bytes()
    unsupported = run("evidence={'bad': {1,2}}")
    assert unsupported.returncode == 1
    assert "evidence" in unsupported.stderr and "set" in unsupported.stderr
    assert state.read_bytes() == before

    too_large = run("evidence='x' * 5_000_000")
    assert too_large.returncode == 1 and "4 MiB" in too_large.stderr
    assert state.read_bytes() == before

    assert run("evidence={'a': 1, 3: 'b'}").returncode == 0
    assert live_vars.variables_info("claude", A, "evidence")["value"] == {
        "a": 1, "3": "b",
    }


def test_oversized_legacy_state_can_be_shrunk_through_worker_bridge(variable_root):
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"large": "ok"}), "claude", A,
    )
    state_id = live_vars.durable_state_id("claude", A)
    state = live_vars.state_path(state_id)
    doc = json.loads(state.read_text())
    doc["variables"]["large"] = "z" * 5_000_000
    state.write_text(json.dumps(doc))
    with pytest.raises(Exception, match="bounded read size"):
        live_vars.variables_info("claude", A, "large")
    backend = __import__("test_worker_variable_storage")
    result = backend._exec_python(variable_root, "claude", A, "-c", "large='recovered'")
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert live_vars.variables_info("claude", A, "large")["value"] == "recovered"
    assert state.stat().st_size <= 4 * 1024 * 1024


def test_durable_inherited_stdin_not_drained_by_c_code(variable_root):
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"counter": 0}), "claude", A,
    )
    state_id = live_vars.durable_state_id("claude", A)
    cmd = live_vars._durable_runner_argv(
        sys.executable, state=live_vars.state_path(state_id),
        lock=live_vars.state_lock_path(state_id),
        cwd=str(Path.cwd()), args=["-c", "counter+=1; print('done')"],
    )
    proc = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
    )
    try:
        proc.stdin.write("still open\n")
        proc.stdin.flush()
        assert proc.wait(timeout=5) == 0
        assert proc.stdout.read().strip() == "done"
        assert live_vars.variables_info("claude", A, "counter")["value"] == 1
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            stream.close()


def test_live_stdin_only_program_from_stdin(variable_root):
    values = {"count": 0}
    owner = live_vars.LiveOwner()
    try:
        live_vars.publish_attachment(
            live_vars.PreparedAttachment("live", ["count"], mapping=values, owner=owner),
            "claude", A,
        )
        backend = __import__("test_worker_variable_storage")
        cmd = backend._python_argv("claude", A, "-c", "count=1; print('done')")
        proc = subprocess.Popen(
            cmd, env=backend._python_env(variable_root), cwd=variable_root,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True,
        )
        try:
            proc.stdin.write("left for shell\n")
            proc.stdin.flush()
            assert proc.wait(timeout=5) == 0
            assert proc.stdout.read() == "done\n"
            assert values["count"] == 1
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                stream.close()

        reply = subprocess.run(
            backend._python_argv("claude", A, "-c",
                                 "import sys; count=(sys.stdin.read() == '')"),
            env=backend._python_env(variable_root), cwd=variable_root,
            input="unavailable to live -c\n", capture_output=True, text=True,
            timeout=5,
        )
        assert reply.returncode == 0, reply.stderr
        assert values["count"] is True
        source = subprocess.run(
            backend._python_argv("claude", A, "-"),
            env=backend._python_env(variable_root), cwd=variable_root,
            input="count=9\n", capture_output=True, text=True, timeout=5,
        )
        assert source.returncode == 0, source.stderr
        assert values["count"] == 9
    finally:
        owner.close()


def test_live_wire_serializer_survives_worker_json_monkeypatch(variable_root):
    import json as json_module
    owner = live_vars.LiveOwner()
    values = {"count": 0}
    live_vars.publish_attachment(
        live_vars.PreparedAttachment("live", ["count"], mapping=values, owner=owner),
        "claude", A,
    )
    original = json_module.dumps
    try:
        reply = _request(owner, "claude", A, "import json; json.dumps = None; count=2")
        assert reply["ok"] is False and reply["errorKind"] == "owner_error"
        assert owner._executor_thread.is_alive()
    finally:
        json_module.dumps = original
    try:
        result = _request(owner, "claude", A, "count += 1")
        assert result["ok"] is True
        assert values["count"] == 3
    finally:
        owner.close()


def test_live_lost_socket_native_fallback_and_new_binding(variable_root):
    owner = live_vars.LiveOwner()
    values = {"counter": 4}
    try:
        live_vars.publish_attachment(
            live_vars.PreparedAttachment("live", ["counter"], mapping=values, owner=owner),
            "claude", A,
        )
        owner.socket_path.unlink()
        backend = __import__("test_worker_variable_storage")
        process = backend._exec_python(variable_root, "claude", A,
                                       "-c", "print('native')")
        assert process.returncode == 0, process.stderr
        assert process.stdout == "native\n"
        assert live_vars.read_sidecar("claude", A) is None
        live_vars.publish_attachment(
            live_vars.prepare_durable_attachment({"counter": 7}), "claude", A,
        )
        assert live_vars.variables_info("claude", A, "counter")["value"] == 7
    finally:
        owner.close()
    assert live_vars.variables_info("claude", A, "counter")["value"] == 7


def test_double_underscore_names_refused_but_builtins_allowed(variable_root):
    for name in ("__builtins__", "__name__", "__file__", "__anything"):
        with pytest.raises(Exception, match="identifier"):
            live_vars.parse_cli_vars([f"{name}=1"])
        with pytest.raises(Exception, match="identifier"):
            live_vars.prepare_attachment({name: 2})
    assert live_vars.parse_cli_vars(["_private=1", "print=2", "exit=3"]) == {
        "_private": 1, "print": 2, "exit": 3,
    }
    with pytest.raises(Exception, match="4 MiB"):
        live_vars.prepare_durable_attachment({"large": "a" * 5_000_000})


@pytest.mark.parametrize("state", [
    "owned", "submitted", "accepted", "rejected", "delivered", "abandoned",
])
def test_original_delivery_token_never_republishes_state(monkeypatch, state):
    from types import SimpleNamespace
    store = SimpleNamespace(record=lambda *_: {"lifecycle": {"state": "idle"}})
    owner = SimpleNamespace(worker="claude:" + A, short=A[:8], engine="claude")
    monkeypatch.setattr(
        "ccx.claude.delivery.read_receipt",
        lambda *_args: {"state": state},
    )
    assert not live_vars.should_publish_send_attachment(
        store, owner, "original-delivery", caps=None,
    )
