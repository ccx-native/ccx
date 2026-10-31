"""Private AF_UNIX plan, daemon lease and original peer-identity checks."""
from __future__ import annotations
import json
import multiprocessing
import os
import socket
import tempfile
from contextlib import contextmanager
from pathlib import Path
import pytest
from ccx.system import oslayer
from ccx.store.root import GenerationBusy, StaleGeneration, require_ccx_active
from ccx.rpc.transport import (
    TransportError, ensure_transport_parent, handshake_payload, plan_transport,
    verify_handshake, daemon_process_lease, _ensure_private_runtime_directory,
)
from test_generation_store import (
    A, B, C, G1, G2, add_worker, fixture_root, privdir, put, record, spec,
    switch_pointer, short_root,
)


@contextmanager
def socket_pair():
    left, right = socket.socketpair()
    try:
        yield left, right
    finally:
        left.close()
        right.close()


@pytest.fixture
def short_runtime(socket_tmp_path):
    if oslayer.UNIX_SUN_PATH_BYTES == 104:
        # Only the approved test-owned /private/tmp UID leaf leaves space for
        # the hashed parent and exact 36-char worker socket on Monterey.
        # Its pinned creation gate is the same one used by the real runtime.
        base = Path("/private/tmp") / f"ccx-u{os.getuid()}"
        choice = oslayer.RuntimeChoice(base, may_create=not base.exists())
        _ensure_private_runtime_directory(base, choice)
        yield base
        return
    runtime = socket_tmp_path / "rt"
    runtime.mkdir(mode=0o700)
    yield runtime


def test_transport_default_owned_socket_plan_and_private_parent(short_root):
    root = short_root
    add_worker(root, A)
    ctx = require_ccx_active(root)
    planned = plan_transport(ctx, target="claude:" + A)
    assert planned.final.name == A + ".sock"
    assert ctx.generation not in str(planned.final)
    assert planned.worker_id == A and planned.engine == "claude"
    assert len(os.fsencode(planned.final)) + 1 <= oslayer.UNIX_SUN_PATH_BYTES
    ensure_transport_parent(ctx, planned)
    assert planned.parent.is_dir() and planned.parent.stat().st_mode & 0o077 == 0


def test_transport_deep_root_uses_private_fallback_and_no_worker_alias(
        tmp_path, short_runtime, monkeypatch):
    root = tmp_path / ("long-" * 30) / "disposable"
    privdir(root)
    # Fixture root needs identity at this exact deep address, not relocated.
    manifest_root = fixture_root(tmp_path / ("long-" * 30))
    add_worker(manifest_root, A)
    runtime = short_runtime
    ctx = require_ccx_active(manifest_root)
    plan = plan_transport(ctx, target=A, trusted_runtime=runtime)
    assert plan.fallback and plan.parent.parent == runtime
    assert ctx.generation not in str(plan.final)
    assert A in plan.final.name
    assert len(os.fsencode(plan.final)) + 1 <= oslayer.UNIX_SUN_PATH_BYTES
    ensure_transport_parent(ctx, plan, trusted_runtime=runtime)
    assert plan.parent.is_dir()
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    if oslayer.UNIX_SUN_PATH_BYTES == 104:
        assert plan_transport(ctx, target=A).parent.parent == runtime
    else:
        # Linux now uses the same guarded sticky-tmp UID fallback as Darwin
        # when there is no private XDG base. Planning must remain read-only;
        # the Linux bind/connect and hostile leaf cases run separately.
        selected = plan_transport(ctx, target=A)
        assert selected.fallback
        assert selected.parent.parent == Path("/tmp") / f"ccx-u{os.getuid()}"
        assert oslayer.fits_unix_path(selected.final)
    # A private XDG_RUNTIME_DIR is the default fallback when none is passed.
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(runtime))
    default = plan_transport(ctx, target=A)
    assert default.final == plan.final
    # Callers (host, codexd) pass no trusted_runtime: creation must agree.
    ensure_transport_parent(ctx, default)


def test_transport_multibyte_root_budget_counts_utf8_not_characters(tmp_path, short_runtime):
    root = fixture_root(tmp_path / ("é" * 45))
    add_worker(root, A)
    runtime = short_runtime
    ctx = require_ccx_active(root)
    plan = plan_transport(ctx, target=A, trusted_runtime=runtime)
    assert plan.fallback
    assert len(os.fsencode(plan.final)) + 1 <= oslayer.UNIX_SUN_PATH_BYTES
    assert len(str(root)) < len(os.fsencode(root))


def test_transport_handshake_checks_same_uid_full_root_generation_owner(short_root):
    root = short_root
    add_worker(root, A)
    ctx = require_ccx_active(root)
    plan = plan_transport(ctx, target=A)
    assert plan.worker_id == A
    from ccx.store.liveness import proc_start_token
    ctx.mutate_record(A, patches={"lifecycle.hostPid": os.getpid(),
                                  "lifecycle.hostStart": proc_start_token(os.getpid())})
    payload = handshake_payload(plan)
    with socket_pair() as (first, second):
        verify_handshake(ctx, plan, received=payload, connected_socket=first)
        with pytest.raises(TransportError):
            verify_handshake(ctx, plan, received={**payload, "workerId": B},
                             connected_socket=first)
        with pytest.raises(TransportError):
            verify_handshake(ctx, plan, received=payload, connected_socket=first,
                             peer_uid=os.getuid() + 1)
    switch_pointer(root)
    with socket_pair() as (first, second):
        with pytest.raises(StaleGeneration):
            verify_handshake(ctx, plan, received=payload, connected_socket=first)


def test_singleton_transport_has_daemon_role_not_native_worker_alias(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    plan = plan_transport(ctx, role="codexd")
    assert plan.final.name == "codexd.sock" and plan.worker_id is None
    with pytest.raises(TransportError):
        plan_transport(ctx, role="codexd", target=A)
    with daemon_process_lease(ctx, plan) as lease:
        with socket_pair() as (a, b):
            verify_handshake(ctx, plan,
                             received=handshake_payload(plan, daemon_nonce=lease.nonce),
                             connected_socket=a)
        with pytest.raises(TransportError):
            handshake_payload(plan)
    with socket_pair() as (a, b):
        with pytest.raises(TransportError):
            verify_handshake(ctx, plan,
                             received=handshake_payload(plan, daemon_nonce=lease.nonce),
                             connected_socket=a)


def test_runtime_transport_parent_not_created_by_inspection(short_root):
    root = short_root
    add_worker(root, A)
    ctx = require_ccx_active(root)
    plan = plan_transport(ctx, target=A)
    assert not plan.parent.exists()
    assert not (root / "identity" / "generations" / G1 / "runtime" / "sock").exists()


def test_transport_rejects_parent_symlink_before_creating_anything(short_root):
    root = short_root
    add_worker(root, A)
    (root / "run").symlink_to(root.parent, target_is_directory=True)
    ctx = require_ccx_active(root)
    if oslayer.UNIX_SUN_PATH_BYTES == 104:
        # A Mac root this deep cannot host the full UUID socket; the planted
        # primary-run symlink must remain untouched while the trusted short
        # runtime is chosen.
        fallback = plan_transport(ctx, target=A)
        assert fallback.fallback and fallback.parent != root / "run"
        assert (root / "run").is_symlink()
    else:
        with pytest.raises(TransportError):
            plan_transport(ctx, target=A)


def _child_daemon_socket_lease(args):
    """Actual disposable AF_UNIX connect: Linux kernel reports child's peer PID."""
    root, socket_path, start_signal, stop_signal, role = args
    import json
    import socket
    from pathlib import Path
    from ccx.store.root import require_ccx_active
    from ccx.rpc.transport import plan_transport, daemon_process_lease, handshake_payload
    ctx = require_ccx_active(Path(root))
    plan = plan_transport(ctx, role=role)
    with daemon_process_lease(ctx, plan) as lease:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(socket_path)
            client.sendall(json.dumps(handshake_payload(plan, daemon_nonce=lease.nonce)).encode() + b"\n")
            start_signal.set()
            stop_signal.wait(10)


def test_worker_transport_rejects_same_uid_wrong_pid_or_host_start(short_root):
    from ccx.store.liveness import proc_start_token
    root = short_root
    add_worker(root, A)
    ctx = require_ccx_active(root)
    plan = plan_transport(ctx, target=A)
    packet = handshake_payload(plan)
    with socket_pair() as (a, b):
        with pytest.raises(TransportError, match="not current PID"):
            verify_handshake(ctx, plan, received=packet, connected_socket=a)
    ctx.mutate_record(A, patches={
        "lifecycle.hostPid": os.getpid(),
        "lifecycle.hostStart": "stale-start-token",
    })
    with socket_pair() as (a, b):
        with pytest.raises(TransportError, match="not current PID"):
            verify_handshake(ctx, plan, received=packet, connected_socket=a)
    ctx.mutate_record(A, patches={
        "lifecycle.hostStart": proc_start_token(os.getpid()),
    })
    with socket_pair() as (a, b):
        verify_handshake(ctx, plan, received=packet, connected_socket=a)
        with pytest.raises(TransportError, match="unexpected unowned"):
            verify_handshake(ctx, plan, received={
                **packet, "daemonLeaseNonce": "a" * 32
            }, connected_socket=a)


def test_daemon_transport_requires_real_held_lease_and_correct_peer_pid(short_root):
    import multiprocessing
    import socket
    import json
    from ccx.store.root import GenerationBusy

    root = short_root
    ctx = require_ccx_active(root)
    plan = plan_transport(ctx, role="codexd")
    sock_path = root / "test-only-peer.sock"
    mp = multiprocessing.get_context("fork")
    started = mp.Event()
    finish = mp.Event()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(sock_path))
        server.listen(1)
        child = mp.Process(
            target=_child_daemon_socket_lease,
            args=((str(root), str(sock_path), started, finish, "codexd"),),
        )
        child.start()
        try:
            server.settimeout(10)
            connection, _address = server.accept()
            with connection:
                payload = b""
                while not payload.endswith(b"\n"):
                    block = connection.recv(2048)
                    if not block:
                        break
                    payload += block
                assert payload.endswith(b"\n")
                packet = json.loads(payload)
                assert started.wait(3)
                assert packet["daemonLeaseNonce"]
                verify_handshake(ctx, plan, received=packet, connected_socket=connection)
                with pytest.raises(TransportError, match="runtime PID/lease"):
                    verify_handshake(ctx, plan, received={
                        **packet, "daemonLeaseNonce": "0" * 32
                    }, connected_socket=connection)
                with pytest.raises(GenerationBusy):
                    with daemon_process_lease(ctx, plan):
                        pytest.fail("second daemon stole exclusive role lock")
                # Forged same UID socket connection from the parent process,
                # even with the correct child nonce, fails PID/start proof.
                with socket_pair() as (ours, theirs):
                    with pytest.raises(TransportError, match="PID/lease"):
                        verify_handshake(ctx, plan, received=packet, connected_socket=ours)
            finish.set()
            child.join(10)
            assert child.exitcode == 0
            with socket_pair() as (ours, theirs):
                with pytest.raises(TransportError):
                    verify_handshake(ctx, plan, received=packet, connected_socket=ours)
        finally:
            finish.set()
            if child.is_alive():
                child.terminate()
            child.join(5)


def test_daemon_lease_generation_change_during_lifetime_no_restart_approval(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    plan = plan_transport(ctx, role="notifyd")
    with daemon_process_lease(ctx, plan) as lease:
        with socket_pair() as (a, b):
            verify_handshake(ctx, plan, received=handshake_payload(
                plan, daemon_nonce=lease.nonce,
            ), connected_socket=a)
        switch_pointer(root)
        with socket_pair() as (a, b):
            with pytest.raises(StaleGeneration):
                verify_handshake(ctx, plan, received=handshake_payload(
                    plan, daemon_nonce=lease.nonce,
                ), connected_socket=a)
    metadata = root / "identity" / "generations" / G1 / "runtime-metadata" / "pid" / "notifyd.json"
    assert json.loads(metadata.read_text())["generation"] == G1
    assert json.loads(metadata.read_text())["instanceNonce"] == lease.nonce
    # Even if the local close cannot write a stale generation's released
    # marker, kernel flock release and pointer revalidation disallow adoption.



def test_transport_plan_forgery_even_correct_same_uid_payload_refuses(short_root):
    from dataclasses import replace
    from ccx.store.liveness import proc_start_token
    root = short_root
    add_worker(root, A)
    ctx = require_ccx_active(root)
    ctx.mutate_record(A, patches={
        "lifecycle.hostPid": os.getpid(),
        "lifecycle.hostStart": proc_start_token(os.getpid()),
    })
    plan = plan_transport(ctx, target=A)
    bad_root = replace(plan, root=root.parent)
    bad_gen = replace(plan, generation=G2)
    bad_address = replace(plan, final=root / "run" / B / "s")
    with socket_pair() as (a, b):
        for forged in (bad_root, bad_gen, bad_address):
            with pytest.raises(TransportError, match="plan"):
                verify_handshake(
                    ctx, forged, received=handshake_payload(forged),
                    connected_socket=a,
                )
    assert plan.final.name == A + ".sock"


def test_daemon_process_lease_refuses_wrong_transport_plan(tmp_path):
    from dataclasses import replace
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    plan = plan_transport(ctx, role="notifyd")
    wrong = replace(plan, role="codexd", final=plan.final)
    with pytest.raises(TransportError, match="plan"):
        with daemon_process_lease(ctx, wrong):
            pytest.fail("unreviewed role address was adopted")
    assert not (root / "identity" / "generations" / G1 / "runtime-metadata").exists()
