"""Detached spawning of managed session hosts.

A managed worker's host must be a first-class, init-parented process that
outlives whatever launched it — and the *launch itself* must be safe to call
from any thread of a long-running Python orchestrator. ``os.fork()`` in the
caller's process is neither: a forked child of a threaded process can inherit a
lock held by another thread and deadlock before ``exec``, and it drags every
open fd of the caller into a host that lives for hours.

So spawning is split in two:

* :func:`spawn_host_detached` — the caller side. Runs ``ccx host <short>
  --detach`` as a plain subprocess (fork+exec happens at the C level with no
  Python in between, safe in threaded processes) and parses the launcher's
  one-line JSON verdict. The subprocess is reaped by ``subprocess.run`` — no
  zombies, no stray ``waitpid`` racing the caller's own child management.
  With an explicit caller deadline the caller stops *observing* at it but
  never kills the launcher; a still-running launcher is left to the stdlib's
  own deferred reaping.
* :func:`run_detached_launcher` — the launcher side, entered by ``cmd_host``
  when ``--detach`` is passed. It runs in a fresh, single-threaded process that
  owns no caller state, so the classic double-fork/setsid daemonization is safe
  *here*. The grandchild redirects stdio to the host log and execs the real
  host (via :func:`_host_exec_argv`, so ``CCX_SPAWN_SCOPE`` systemd scoping
  applies uniformly to fresh spawns and revives); the launcher waits for the
  host socket and reports the verdict.

The environment is inherited unchanged from the caller in both hops: Claude
auth, proxy, and subscription behavior depend on it, so ccx deliberately does
not curate an env allowlist. fd hygiene comes from ``subprocess``'s default
``close_fds=True`` plus the launcher's stdio redirection, not from env
filtering.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time

from ccx.system import oslayer
from ccx.constants import (
    HOST_SOCK_WAIT_NEW_S,
    SPAWN_LAUNCHER_GRACE_S,
)
from ccx.system.executable import current_executable_path
from ccx.store.paths import host_log_path, sock_path

# Launcher verdict phases (the "phase" field when ok is False):
#   launcher-failed  the launcher subprocess could not run / crashed / timed out
#                    — no host was handed off; treat as a hard spawn failure.
#   bad-verdict      the launcher ran but printed no parseable verdict — assume
#                    no host; hard failure.
#   host-exited      the host process exited before its socket appeared; the
#                    error carries the host log's last line. Hard failure.
#   sock-timeout     the host was forked and is still running, but its socket
#                    did not appear within the launcher's wait — it may still
#                    be booting slowly, so callers may treat this as soft
#                    (legacy semantics). Also returned when an explicit caller
#                    launch deadline expires while the launcher still runs:
#                    the host may not be forked yet, but it may still start.
PHASE_LAUNCHER_FAILED = "launcher-failed"
PHASE_BAD_VERDICT = "bad-verdict"
PHASE_HOST_EXITED = "host-exited"
PHASE_SOCK_TIMEOUT = "sock-timeout"


def _scope_enabled(value: str | None) -> bool:
    """Whether CCX_SPAWN_SCOPE asks for systemd user-scope isolation.

    Opt-in only: truthy means the host (and the `claude` child it spawns) should
    run in their own transient systemd user scope so they don't inherit — and
    outlive — the cgroup of whatever service launched ccx (e.g. a dashboard
    systemd unit). Unset/empty/falsey leaves the historic direct-exec path.
    """
    if not value:
        return False
    return value.strip().lower() in ("1", "true", "yes", "on")


def _user_bus_available() -> bool:
    """Whether ``systemd-run --user`` can reach a per-user manager bus."""
    if os.environ.get("DBUS_SESSION_BUS_ADDRESS"):
        return True
    runtime = os.environ.get("XDG_RUNTIME_DIR")
    return bool(runtime) and os.path.exists(os.path.join(runtime, "bus"))


def _scope_unit_name(short: str, instance: str) -> str:
    """A safe, per-host systemd scope name: ``ccx-<short>-<instance>``.

    A session can intentionally leave a background process alive after its CCX
    host exits.  Such a process keeps the transient scope active, so reusing a
    deterministic ``ccx-<short>`` name makes every later revive fail with
    "unit already loaded".  Each host lifetime therefore gets a unique suffix;
    the short remains in the prefix for operator visibility.

    ``systemd-run`` appends ``.scope`` for scope runs, so this is the bare name.
    Sanitize both parts defensively so neither can produce an invalid unit name.
    """
    safe = re.sub(r"[^A-Za-z0-9-]", "-", short)
    safe_instance = re.sub(r"[^A-Za-z0-9-]", "-", instance)
    return f"ccx-{safe}-{safe_instance}"


def _host_exec_argv(short: str, *, executable: str | None = None,
                    script: str | None = None, scope_env: str | None = None,
                    systemd_run: str | None = None,
                    scope_instance: str | None = None) -> tuple[str, list[str]]:
    """Build the (program, argv) used to re-exec ourselves as `ccx host <short>`.

    The default is a direct ``python <ccx-entrypoint> host <short>`` exec — the
    historic behavior. When ``CCX_SPAWN_SCOPE`` is truthy and ``systemd-run`` is
    available, the exec is wrapped in a transient systemd user scope so the host
    lands in its own ``ccx-<short>-<instance>.scope`` cgroup instead of the
    launcher's. This is purely opt-in: with the env unset or systemd-run absent,
    the wrapping is
    skipped and the direct exec is returned unchanged, so CLI users and
    non-systemd environments see no behavior change.

    Pure and side-effect free (the params are injectable) so command
    construction can be unit-tested without forking.
    """
    executable = executable or sys.executable
    script = script or current_executable_path()
    direct = [executable, script, "host", short]

    if scope_env is None:
        scope_env = os.environ.get("CCX_SPAWN_SCOPE")
    if not _scope_enabled(scope_env):
        return executable, direct

    if systemd_run is None:
        systemd_run = shutil.which("systemd-run")
        if systemd_run and not _user_bus_available():
            # Binary present but no user manager to talk to (container,
            # `su`/cron/SSH without a session): `systemd-run --user` would fail.
            systemd_run = None
    if not systemd_run:
        # Opt-in requested but unavailable: fall back to the direct exec rather
        # than failing the spawn. Keeps non-systemd hosts working.
        return executable, direct

    # PID alone can eventually be reused while an older scope is still held by
    # a long-lived background child.  Pair it with a monotonic nanosecond stamp
    # to keep the unit unique across every revive in this boot.
    instance = scope_instance or f"{os.getpid()}-{time.monotonic_ns()}"
    unit = _scope_unit_name(short, instance)
    wrapped = [
        systemd_run, "--user", "--scope", "--quiet", "--collect",
        f"--unit={unit}", "--",
        *direct,
    ]
    return systemd_run, wrapped


def _verified_full_uuid_host_start(worker_id: str) -> str:
    """Verify first native submission or a confirmed original host revival.

    First creation needs submitted-unknown; later host lifetimes require the
    same confirmed native session/source, resuming state and native history.
    Mere record existence never authorizes a provider start. Full ID only.
    """
    from ccx import constants
    from ccx.store.root import require_ccx_active, CreationIndeterminate
    from ccx.store.identity import EngineMismatch
    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(worker_id)
    if owner.engine != "claude":
        raise EngineMismatch("Claude startup requires a worker handle")
    record = store.record(owner.worker)
    native = record.get("native")
    if not isinstance(native, dict) or not isinstance(native.get("sessionId"), str):
        raise CreationIndeterminate("Claude session is missing; inspect worker status")
    act = store.native_activation(owner.worker)
    original_source = f"claude-session:{native['sessionId']}"
    if (act.get("state") == "submitted-unknown"
            and act.get("source") == original_source
            and isinstance(act.get("submissionId"), str)
            and act["submissionId"]):
        pass  # unchanged first, fsynced original-native submission
    elif (act.get("state") == "confirmed"
            and act.get("source") == original_source
            and (record.get("lifecycle") or {}).get("state") == "resuming"):
        # This is a *host lifetime* restart, not a second native activation.
        # Only original source-bound nonempty history may enter --resume.
        from pathlib import Path
        import stat
        store.authorize_native_action(owner.worker, "resume")
        transcript = record.get("transcript")
        if not isinstance(transcript, str):
            raise CreationIndeterminate("Claude history is missing; check worker status")
        try:
            st = Path(transcript).stat()
        except OSError as exc:
            raise CreationIndeterminate(
                "Claude history is missing; inspect the worker before resuming"
            ) from exc
        if not stat.S_ISREG(st.st_mode) or st.st_size == 0:
            raise CreationIndeterminate(
                "Claude history is empty; check the source session before resuming"
            )
    else:
        raise CreationIndeterminate(
            "no originally submitted or confirmed/resuming source-verified "
            "Claude host lifetime; never start a replacement session"
        )
    store.revalidate()
    return owner.worker_id


def verified_original_claude_start(store, prepared, *, deadline: float | None = None):
    """Observe one original host/provider SessionStart; never start or retry it.

    Host startup is positively bound to the immutable full CCX owner and
    separately minted native session, original submitted activation token,
    Linux host/worker PID+start, and the server's exact socket handshake on
    the same kernel-authenticated connection. This is the existing host and
    rpc module, not an alternate provider transport.
    """
    from ccx.store.liveness import owned_pid_alive
    from ccx.rpc.rpc import _rpc
    from ccx.store.store import _digest
    from ccx.store.root import NativeEffectIndeterminate
    def remaining_status_time():
        remaining = (.25 if deadline is None else
                     min(.25, deadline - time.monotonic()))
        if remaining <= 0:
            raise NativeEffectIndeterminate(
                "Claude startup timed out; inspect the worker before retrying"
            )
        return remaining

    wid = prepared.worker_id
    remaining_status_time()
    store.revalidate()
    claim = store.native_activation(prepared.worker)
    if (claim.get("state") not in ("submitted-unknown", "confirmed")
            or claim.get("source") != prepared.native_source
            or not isinstance(claim.get("submissionId"), str)):
        return None
    record = store.record(prepared.worker)
    native = record.get("native") or {}
    lifecycle = record.get("lifecycle") or {}
    proof = lifecycle.get("nativeStartProof")
    if (not isinstance(proof, dict)
            or proof.get("workerId") != wid
            or proof.get("engine") != "claude"
            or proof.get("generation") != store.generation
            or proof.get("nativeSource") != prepared.native_source
            or proof.get("sessionId") != native.get("sessionId")
            or proof.get("submissionId") != claim["submissionId"]
            or proof.get("hostPid") != lifecycle.get("hostPid")
            or proof.get("hostStart") != str(lifecycle.get("hostStart"))
            or proof.get("workerPid") != lifecycle.get("workerPid")
            or proof.get("workerStart") != str(lifecycle.get("workerStart"))
            or not owned_pid_alive(proof.get("hostPid"), proof.get("hostStart"))
            or not owned_pid_alive(proof.get("workerPid"), proof.get("workerStart"))
            or not owned_pid_alive(proof.get("peerPid"), proof.get("peerStart"))
            or not isinstance(proof.get("nativeEvidenceSha256"), str)):
        return None
    digest = proof["nativeEvidenceSha256"]
    if digest != _digest({
        k: value for k, value in proof.items()
        if k != "nativeEvidenceSha256"
    }):
        return None
    # _rpc verifies real SO_PEERCRED against this exact host PID/start,
    # returned root/generation/full-ID and current pinned store *before*
    # transmitting this read-only status request on the same socket.
    observed = _rpc(
        wid, {"op": "startup-status"}, timeout=remaining_status_time()
    )
    remaining_status_time()  # A late positive reply is not timely success.
    if (observed.get("ok") is not True
            or observed.get("workerId") != wid
            or observed.get("engine") != "claude"
            or observed.get("generation") != store.generation
            or observed.get("nativeStartProof") != proof):
        return None
    store.revalidate()
    remaining_status_time()
    return {
        "accepted": True,
        "workerId": wid, "engine": "claude",
        "source": prepared.native_source,
        "submissionId": claim["submissionId"],
        "nativeEvidenceSha256": digest,
        "nativeBinding": {"sessionId": native["sessionId"]},
    }


def submit_original_claude_start(store, prepared, budget: float, *,
                                 deadline: float | None = None):
    """one existing detached host spawn under fsynced native submitted claim.

    Socket readiness alone is not native success. Only the native child
    SessionStart proof above can turn a claimed worker into sendable state.
    Any lost/delayed native result stays submitted-unknown: the caller later
    reconciles the same original claim, never launches another host.
    """
    from ccx.store.root import NativeEffectIndeterminate
    until = min(time.monotonic() + budget,
                deadline if deadline is not None else float("inf"))
    remaining = until - time.monotonic()
    if remaining <= 0:
        raise NativeEffectIndeterminate(
            "Claude startup timed out before it began; retry with more time"
        )
    verdict = spawn_host_detached(
        prepared.worker_id,
        sock_wait_s=min(.6, remaining),
        launch_timeout_s=min(max(.1, budget - .05), remaining),
    )
    if time.monotonic() >= until:
        raise NativeEffectIndeterminate(
            "Claude startup timed out; inspect the worker before retrying"
        )
    if not verdict.get("ok"):
        raise NativeEffectIndeterminate(
            "Claude startup could not be confirmed; check worker status: "
            + str(verdict.get("phase") or verdict.get("error"))
        )
    while time.monotonic() < until:
        answer = verified_original_claude_start(store, prepared, deadline=until)
        if answer:
            if time.monotonic() >= until:
                raise NativeEffectIndeterminate(
                    "Claude startup timed out; check worker status before retrying"
                )
            return answer
        time.sleep(min(.025, max(0.0, until - time.monotonic())))
    raise NativeEffectIndeterminate(
        "original native SessionStart not positively attested within bounded "
        "submission; submitted-unknown, reconcile original later"
    )


def spawn_host_detached(short: str, *,
                        sock_wait_s: float = HOST_SOCK_WAIT_NEW_S,
                        launch_timeout_s: float | None = None) -> dict:
    """Launch the detached host for `short` and return the launcher's verdict.

    Runs ``ccx host <short> --detach`` as a normal subprocess: the only fork in
    the caller's process is subprocess's own C-level fork+exec, so this is safe
    from threads and never runs Python in a forked copy of the caller. The
    launcher daemonizes the real host (init-parented, own session — it survives
    the caller exiting), waits up to ``sock_wait_s`` for the host socket, and
    reports one JSON line on stdout.

    Returns the verdict dict. ``{"ok": True, ...}`` means the host is up
    (socket bound). ``ok: False`` carries a ``phase``:
    :data:`PHASE_SOCK_TIMEOUT` (host or launcher still running, socket late —
    possibly still booting), or :data:`PHASE_LAUNCHER_FAILED` /
    :data:`PHASE_BAD_VERDICT` (hard failure — assume no host). Never raises.

    An explicit ``launch_timeout_s`` bounds only the caller's observation: the
    one launcher is never killed mid-import, so a fenced submission can still
    produce its host after the caller has returned.

    ``start_new_session=True`` keeps the short-lived launcher out of the
    caller's process group so an orchestrator Ctrl-C can't kill it mid-detach.
    The caller's environment is inherited unchanged (see module docstring).
    """
    short = _verified_full_uuid_host_start(short)
    argv = [sys.executable, current_executable_path(), "host", short,
            "--detach", "--sock-wait", f"{sock_wait_s:g}"]
    try:
        if launch_timeout_s is None:
            proc = subprocess.run(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
                timeout=(sock_wait_s + SPAWN_LAUNCHER_GRACE_S
                         if launch_timeout_s is None else launch_timeout_s),
            )
        else:
            launcher = subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            try:
                stdout, stderr = launcher.communicate(timeout=launch_timeout_s)
            except subprocess.TimeoutExpired:
                return {"ok": False, "short": short, "phase": PHASE_SOCK_TIMEOUT,
                        "error": f"launcher still running after {launch_timeout_s:g}s; "
                                 "host may still start (launcher not killed)"}
            proc = subprocess.CompletedProcess(argv, launcher.returncode, stdout, stderr)
    except (OSError, subprocess.SubprocessError) as e:
        return {"ok": False, "short": short, "phase": PHASE_LAUNCHER_FAILED,
                "error": f"{e.__class__.__name__}: {e}"}

    verdict = _parse_verdict(proc.stdout)
    if verdict is None:
        err = (proc.stderr or b"").decode("utf-8", "replace").strip()
        return {"ok": False, "short": short, "phase": PHASE_BAD_VERDICT,
                "error": err or f"launcher exited rc={proc.returncode} "
                                "with no verdict"}
    verdict.setdefault("short", short)
    return verdict


def _parse_verdict(stdout: bytes | None) -> dict | None:
    """Last parseable JSON object line of the launcher's stdout, or None."""
    if not stdout:
        return None
    for line in reversed(stdout.decode("utf-8", "replace").splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if isinstance(obj, dict):
            return obj
    return None


def _open_private_host_log(short: str):
    """Open the Claude host's stdio log owner-only despite permissive umask.

    fchmod also tightens a log an older host created group/world-readable.
    """
    fd = os.open(host_log_path(short), os.O_WRONLY | os.O_APPEND | os.O_CREAT,
                 0o600)
    os.fchmod(fd, 0o600)
    return os.fdopen(fd, "ab", buffering=0)


def run_detached_launcher(short: str, sock_wait_s: float) -> int:
    """Launcher side of a detached spawn (``ccx host <short> --detach``).

    Runs in a fresh process, so the double-fork is safe: no caller threads, no
    caller locks, no caller fds beyond stdio. Daemonizes the host (setsid +
    second fork → grandchild reparents to init), redirects the grandchild's
    stdio to the host log, and execs the real host via :func:`_host_exec_argv`
    (so CCX_SPAWN_SCOPE scoping applies to spawns and revives alike). The
    surviving launcher then waits up to ``sock_wait_s`` for the host socket and
    prints a one-line JSON verdict for :func:`spawn_host_detached`.

    Returns the launcher's exit code: 0 when the socket came up, 1 otherwise
    (host exited, or still booting past the wait).
    """
    short = _verified_full_uuid_host_start(short)
    pid_read, pid_write = os.pipe()
    pid = os.fork()
    if pid == 0:
        # First child: detach from the launcher's session, then fork the real
        # host so it reparents to init when we exit.
        os.close(pid_read)
        os.setsid()
        pid2 = os.fork()
        if pid2 > 0:
            # Tell the launcher which process is the host, so it can stop
            # waiting (and say why) if the host dies before binding.
            os.write(pid_write, str(pid2).encode())
            os._exit(0)
        os.close(pid_write)
        # Grandchild: become the host. Point stdio at the host log (this also
        # drops the launcher's stdout/stderr pipes so the caller's read of the
        # verdict is never held open by the long-lived host).
        log = _open_private_host_log(short)
        devnull = os.open(os.devnull, os.O_RDONLY)
        os.dup2(devnull, 0)
        os.close(devnull)
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
        try:
            log.close()
        except Exception:
            pass
        prog, argv = _host_exec_argv(short)
        try:
            os.execv(prog, argv)
        except BaseException as e:
            # stderr is the host log by now; an exec failure would otherwise
            # leave it empty.
            os.write(2, f"[host {short}] could not exec {prog}: {e}\n".encode())
        finally:
            os._exit(127)

    # Launcher: reap the intermediate child, then wait for the host socket.
    os.close(pid_write)
    os.waitpid(pid, 0)
    with os.fdopen(pid_read, "rb") as handle:
        raw = handle.read().decode("ascii", "replace").strip()
    host_pid = int(raw) if raw.isdigit() else None
    deadline = time.time() + sock_wait_s
    host_gone = False
    while time.time() < deadline and not sock_path(short).exists():
        if host_pid is not None and not _process_running(host_pid):
            host_gone = not sock_path(short).exists()
            break
        time.sleep(0.05)
    ok = sock_path(short).exists()
    verdict = {"ok": ok, "short": short}
    if not ok and host_gone:
        verdict["phase"] = PHASE_HOST_EXITED
        tail = _log_tail(short)
        verdict["error"] = ("host exited before opening its socket"
                            + (f": {tail}" if tail else "")
                            + f" (log: {host_log_path(short)})")
    elif not ok:
        verdict["phase"] = PHASE_SOCK_TIMEOUT
        verdict["error"] = (f"host socket did not appear within "
                            f"{sock_wait_s:g}s; the host is still running "
                            f"(log: {host_log_path(short)})")
    sys.stdout.write(json.dumps(verdict) + "\n")
    sys.stdout.flush()
    return 0 if ok else 1


def _process_running(pid: int) -> bool:
    """Whether ``pid`` exists and is not a zombie awaiting its reaper."""
    probe = oslayer.probe_pid(pid)
    if probe == "missing":
        return False
    if probe == "denied":
        return True
    # Old launcher considers every state but Z running; missing/unreadable
    # stat after a successful kill(0) is also still considered running.
    return oslayer.process_info(pid).state != "Z"


def _log_tail(short: str) -> str:
    """The host log's last non-empty line, bounded, or ''."""
    try:
        with open(host_log_path(short), "rb") as handle:
            handle.seek(0, os.SEEK_END)
            handle.seek(max(0, handle.tell() - 4096))
            lines = handle.read().decode("utf-8", "replace").splitlines()
    except OSError:
        return ""
    for line in reversed(lines):
        if line.strip():
            return line.strip()[:500]
    return ""


__all__ = [
    "PHASE_BAD_VERDICT",
    "PHASE_HOST_EXITED",
    "PHASE_LAUNCHER_FAILED",
    "PHASE_SOCK_TIMEOUT",
    "run_detached_launcher",
    "spawn_host_detached",
]
