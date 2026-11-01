"""Shared/durable Python variables for CCX-managed workers."""

from __future__ import annotations

import base64
from collections.abc import Mapping, MutableMapping
from contextlib import contextmanager
from dataclasses import dataclass
import importlib
import io
import json
import keyword
import os
from pathlib import Path
import queue
import shlex
import shutil
import socket
import stat
import subprocess
import sys
import sysconfig
import threading
import time
import uuid
from typing import Any

from ccx import constants
from ccx.system import oslayer
from ccx.errors import CapabilityUnsupported, InvalidRequest, SessionUnavailable
from ccx.system.executable import current_executable_path
from ccx.controllers.live_vars_runner import execute_target, strict_json, STATE_FILE_LIMIT
from ccx.store.paths import accept_same_user, bind_private_unix_socket, _require_unix_socket_budget


VARIABLE_SCHEMA_VERSION = 2
VARIABLE_STATE_SCHEMA_VERSION = 1
_WIRE_DUMPS = json.dumps
_WIRE_LOADS = json.loads
_NATIVE_FALLTHROUGH_MODULES = frozenset({"pip", "venv", "ensurepip"})


def _root() -> Path:
    from ccx.store.root import require_ccx_active
    return require_ccx_active(constants.CCX_ROOT).pointer.generation_root / "variables"


def sidecar_path(engine: str, short: str) -> Path:
    store, owner = _env_worker(engine, short)
    return store.pointer.generation_root / "variables" / engine / f"{owner.worker_id}.json"


def sidecar_lock_path(engine: str, short: str) -> Path:
    store, owner = _env_worker(engine, short)
    return store.pointer.generation_root / "variables" / engine / f"{owner.worker_id}.lock"


def states_dir() -> Path:
    return _root() / "states"


def state_path(state_id: str) -> Path:
    return states_dir() / f"{_valid_state_id(state_id)}.json"


def state_lock_path(state_id: str) -> Path:
    return states_dir() / f"{_valid_state_id(state_id)}.lock"


def _env_worker(engine: str, worker_id: str):
    from ccx.store.root import require_ccx_active
    from ccx.store.identity import EngineMismatch
    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(worker_id)
    if owner.engine != engine:
        raise EngineMismatch("Worker uses a different engine; check the handle")
    return store, owner


def shell_env_path(engine: str, short: str) -> Path:
    store, owner = _env_worker(engine, short)
    return (store.pointer.generation_root / "variables" / "env" /
            engine / f"{owner.worker_id}.sh")


def claude_bridge_path(short: str) -> Path:
    store, owner = _env_worker("claude", short)
    return (store.pointer.generation_root / "variables" / "env" /
            "claude" / f"{owner.worker_id}.bridge.json")


def _owner_runtime(
    basename: str | None = None,
) -> tuple[Path, oslayer.RuntimeChoice]:
    """Plan the full live-owner socket suffix before any filesystem writes."""
    dirname = f"ccx-live-vars-{os.getuid()}"
    suffix = (max(
        len(os.fsencode(f"/{dirname}/{basename}")) + 1,
        len(os.fsencode(f"/{dirname}/.XXXXXXXX/s")) + 1,
    ) if basename else 0)
    choice = oslayer.runtime_base(purpose="live_vars", suffix_bytes=suffix)
    if choice is None:
        raise PermissionError("no private owner-socket runtime base available")
    return choice.path / dirname, choice


def _owners_path() -> Path:
    return _owner_runtime()[0]


def owners_dir(
    path: Path | None = None, *, runtime: oslayer.RuntimeChoice | None = None,
) -> Path:
    if path is None:
        path, runtime = _owner_runtime()
    if runtime is None:
        runtime = oslayer.RuntimeChoice(path.parent)
    from ccx.rpc.transport import _ensure_private_runtime_directory
    _ensure_private_runtime_directory(path, runtime)
    st = os.lstat(path)
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid():
        raise PermissionError(
            f"live-vars owner directory is not private and user-owned: {path}"
        )
    os.chmod(path, 0o700)
    return path


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    """Main's atomic state replacement inside the validated private generation."""
    text = strict_json(value)
    if len(text.encode("utf-8")) > STATE_FILE_LIMIT:
        raise InvalidRequest("worker variables exceed the 4 MiB state limit")
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


@contextmanager
def _worker_lock(engine: str, short: str):
    from ccx.store.root import _flock_at
    store, owner = _env_worker(engine, short)
    with store.lease():
        with store._gen("variables", engine, create=True) as directory:
            with _flock_at(directory, owner.worker_id + ".lock",
                           exclusive=True, create=True):
                yield


@contextmanager
def _state_lock(state_id: str, *, create: bool = False):
    from ccx.store.root import _flock_at, require_ccx_active
    store = require_ccx_active(constants.CCX_ROOT)
    with store.lease():
        with store._gen("variables", "states", create=create) as directory:
            with _flock_at(directory, _valid_state_id(state_id) + ".lock",
                           exclusive=True, create=create):
                yield


def _valid_state_id(state_id: Any) -> str:
    value = str(state_id or "")
    if len(value) != 32 or any(ch not in "0123456789abcdef" for ch in value):
        raise SessionUnavailable(f"invalid durable variable state id: {value!r}")
    return value


def _new_state_id() -> str:
    return uuid.uuid4().hex


def read_sidecar(engine: str, short: str) -> dict[str, Any] | None:
    """Read a binding only after resolving its exact global full-UUID owner."""
    from ccx.store.root import _read_json, StorageIntegrityError
    store, owner = _env_worker(engine, short)
    with store.lease():
        try:
            with store._gen("variables", engine) as directory:
                value = _read_json(directory, owner.worker_id + ".json")
        except FileNotFoundError:
            return None
    if value is not None and (
        value.get("schemaVersion") != VARIABLE_SCHEMA_VERSION
        or value.get("engine") != owner.engine
        or value.get("workerId") != owner.worker_id
        or value.get("mode") not in {"live", "durable"}
    ):
        raise StorageIntegrityError("Worker variable reference is incomplete; use the full handle")
    return value


def read_state(state_id: str) -> dict[str, Any]:
    from ccx.store.root import _read_json, require_ccx_active, StorageIntegrityError
    store = require_ccx_active(constants.CCX_ROOT)
    with store.lease():
        try:
            with store._gen("variables", "states") as directory:
                value = _read_json(directory, _valid_state_id(state_id) + ".json")
        except FileNotFoundError:
            value = None
    if (value is None or value.get("schemaVersion") != VARIABLE_STATE_SCHEMA_VERSION
            or value.get("stateId") != state_id
            or not isinstance(value.get("names"), list)
            or not isinstance(value.get("variables"), dict)):
        raise StorageIntegrityError("Worker variables are unavailable; inspect the worker")
    names, variables = value["names"], value["variables"]
    if (any(not isinstance(name, str) or not name.isidentifier()
            or keyword.iskeyword(name) for name in names)
            or len(set(names)) != len(names)
            or set(names) != set(variables)):
        raise StorageIntegrityError("Worker variable names do not match saved values; inspect them")
    return value


def _new_state_document(
    state_id: str,
    *,
    names: list[str],
    variables: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schemaVersion": VARIABLE_STATE_SCHEMA_VERSION,
        "stateId": _valid_state_id(state_id),
        "names": sorted(names),
        "variables": _json_clone(variables, "durable variables"),
    }


def durable_state_id(
    engine: str,
    short: str,
    value: dict[str, Any] | None = None,
) -> str:
    from ccx.store.root import StorageIntegrityError
    sidecar = value if value is not None else read_sidecar(engine, short)
    if not sidecar or sidecar.get("mode") != "durable":
        raise SessionUnavailable(f"{engine}:{short} has no shared variables; attach variables when creating it")
    if not sidecar.get("stateId"):
        raise StorageIntegrityError("Shared variable state is missing; inspect the worker")
    return _valid_state_id(sidecar["stateId"])


def _valid_name(name: Any) -> str:
    if (not isinstance(name, str) or not name.isidentifier()
            or keyword.iskeyword(name) or name.startswith("__")):
        raise InvalidRequest(
            f"variable names must be valid non-keyword Python identifiers, got {name!r}"
        )
    return name


def _names(values: Mapping[str, Any]) -> list[str]:
    return sorted(_valid_name(name) for name in values.keys())


def _json_clone(value: Any, label: str) -> Any:
    try:
        return json.loads(strict_json(value))
    except (TypeError, ValueError) as error:
        raise InvalidRequest(f"{label} must be JSON-compatible: {error}") from error


def parse_cli_vars(items: list[str] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for raw in items or []:
        if "=" not in raw:
            raise InvalidRequest(
                f"--var expects NAME=JSON, got {raw!r}"
            )
        name, encoded = raw.split("=", 1)
        name = _valid_name(name)
        if name in result:
            raise InvalidRequest(f"duplicate --var binding {name!r}")
        try:
            value = json.loads(encoded)
        except ValueError as error:
            raise InvalidRequest(
                f"--var {name}=... must contain valid JSON: {error}"
            ) from error
        result[name] = _json_clone(value, f"variable {name!r}")
    return result


def _active_controller_runtime():
    try:
        module = importlib.import_module('ccx.controllers.controller')
        return module._active_runtime.get()
    except Exception:
        return None


@dataclass
class PreparedAttachment:
    mode: str
    names: list[str]
    mapping: MutableMapping[str, Any] | None = None
    values: dict[str, Any] | None = None
    state_id: str | None = None
    owner: "LiveOwner | None" = None


def _check_initial_durable_size(names: list[str], values: dict[str, Any]) -> None:
    # A fresh state has a fixed-length state ID; reject before worker creation.
    document = {"schemaVersion": VARIABLE_STATE_SCHEMA_VERSION,
                "stateId": "0" * 32, "names": names, "variables": values}
    if len(strict_json(document).encode("utf-8")) > STATE_FILE_LIMIT:
        raise InvalidRequest("worker variables exceed the 4 MiB state limit")


def prepare_attachment(values: Mapping[str, Any] | None) -> PreparedAttachment | None:
    if values is None:
        return None
    if not isinstance(values, Mapping):
        raise InvalidRequest("variables must be a mapping of Python names to values")
    names = _names(values)
    runtime = _active_controller_runtime()
    if runtime is not None:
        if not isinstance(values, MutableMapping):
            raise InvalidRequest(
                "controller-backed variables require a mutable mapping so worker rebinding can write back"
            )
        owner = runtime.live_vars_owner()
        return PreparedAttachment(
            mode="live",
            names=names,
            mapping=values,
            owner=owner,
        )
    durable = _json_clone(dict(values), "variables")
    _check_initial_durable_size(names, durable)
    return PreparedAttachment(mode="durable", names=names, values=durable)


def prepare_durable_attachment(values: Mapping[str, Any] | None) -> PreparedAttachment | None:
    if values is None:
        return None
    if not isinstance(values, Mapping):
        raise InvalidRequest("variables must be a mapping of Python names to values")
    names = _names(values)
    durable = _json_clone(dict(values), "variables")
    _check_initial_durable_size(names, durable)
    return PreparedAttachment(mode="durable", names=names, values=durable)


def prepare_shared_durable_attachment(
    engine: str,
    short: str,
) -> PreparedAttachment:
    store, owner = _env_worker(engine, short)
    info = store.variables_info(owner.worker)
    if info["mode"] != "durable":
        raise InvalidRequest(f"{owner.worker} has no durable variables to share")
    if not info["available"]:
        raise SessionUnavailable(f"{owner.worker} durable variables are unavailable")
    return PreparedAttachment(
        mode="durable",
        names=info["names"],
        state_id=info["state_id"],
    )


def _owner_gone(owner: Mapping[str, Any]) -> bool:
    socket_path = owner.get("socket")
    if not isinstance(socket_path, str) or not socket_path:
        return False
    try:
        os.lstat(socket_path)
    except FileNotFoundError:
        return True
    except OSError:
        return False
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.settimeout(0.25)
        client.connect(socket_path)
        return False
    except (FileNotFoundError, ConnectionRefusedError):
        return True
    except OSError:
        return False
    finally:
        client.close()


def _unlink_sidecar(store, engine: str, worker_id: str) -> None:
    with store.lease():
        with store._gen("variables", engine) as directory:
            os.unlink(worker_id + ".json", dir_fd=directory)
            os.fsync(directory)


def _clear_dead_binding(engine: str, worker_id: str, token: str) -> None:
    store, owner = _env_worker(engine, worker_id)
    with _worker_lock(engine, owner.worker_id):
        value = read_sidecar(engine, owner.worker_id)
        if (value and value.get("mode") == "live"
                and (value.get("owner") or {}).get("token") == token):
            # Recheck after acquiring the writer lock.
            if _owner_gone(value["owner"]):
                _unlink_sidecar(store, engine, owner.worker_id)



def publish_attachment(
    attachment: PreparedAttachment | None,
    engine: str,
    short: str,
) -> None:
    if attachment is None:
        return
    if not attachment.names and attachment.state_id is None:
        return

    store, owner = _env_worker(engine, short)
    with _worker_lock(engine, owner.worker_id):
        existing = read_sidecar(engine, owner.worker_id)
        if attachment.mode == "live":
            assert attachment.owner is not None
            assert attachment.mapping is not None
            if existing and existing["mode"] == "durable":
                raise InvalidRequest(
                    f"{owner.worker} already has shared variables; choose one variable mode"
                )
            if existing:
                old_owner = existing.get("owner")
                if isinstance(old_owner, dict):
                    if old_owner.get("token") != attachment.owner.token:
                        if _owner_gone(old_owner):
                            _unlink_sidecar(store, engine, owner.worker_id)
                            existing = None
                        else:
                            raise InvalidRequest(
                                f"{owner.worker} already has a reachable live variable owner"
                            )
            attachment.owner.register_worker(
                engine, owner.worker_id, attachment.mapping, attachment.names
            )
            _atomic_json(sidecar_path(engine, owner.worker_id), {
                "schemaVersion": VARIABLE_SCHEMA_VERSION,
                "engine": engine,
                "workerId": owner.worker_id,
                "mode": "live",
                "names": attachment.owner.worker_names(engine, owner.worker_id),
                "owner": attachment.owner.public_identity(),
            })
            return

        if attachment.mode != "durable":
            raise InvalidRequest(f"unknown variable mode {attachment.mode!r}")
        if existing and existing["mode"] == "live":
            old_owner = existing.get("owner")
            if isinstance(old_owner, dict) and _owner_gone(old_owner):
                _unlink_sidecar(store, engine, owner.worker_id)
                existing = None
            else:
                raise InvalidRequest(
                    f"{owner.worker} already owns live variables; cannot mix modes"
                )
        if attachment.state_id is not None:
            state_id = _valid_state_id(attachment.state_id)
            with _state_lock(state_id):
                read_state(state_id)
            _atomic_json(sidecar_path(engine, owner.worker_id), {
                "schemaVersion": VARIABLE_SCHEMA_VERSION,
                "engine": engine,
                "workerId": owner.worker_id,
                "mode": "durable",
                "stateId": state_id,
            })
            return

        assert attachment.values is not None
        if existing:
            state_id = durable_state_id(engine, owner.worker_id, existing)
        else:
            state_id = _new_state_id()
        with _state_lock(state_id, create=existing is None):
            if existing:
                document = read_state(state_id)
                values = dict(document["variables"])
                names = set(document["names"])
            else:
                values, names = {}, set()
            values.update(attachment.values)
            names.update(attachment.names)
            _atomic_json(state_path(state_id), _new_state_document(
                state_id, names=sorted(names), variables=values,
            ))
        _atomic_json(sidecar_path(engine, owner.worker_id), {
            "schemaVersion": VARIABLE_SCHEMA_VERSION,
            "engine": engine,
            "workerId": owner.worker_id,
            "mode": "durable",
            "stateId": state_id,
        })


def variables_info(engine: str, short: str, name: str | None = None) -> dict[str, Any]:
    """Use the already-accepted single full-UUID variable read authority."""
    store, owner = _env_worker(engine, short)
    return store.variables_info(owner.worker, name=name)


def _runtime_state(owner) -> str | None:
    try:
        if owner.engine == "claude":
            from ccx.rpc.rpc import _rpc
            reply = _rpc(owner.worker, {"op": "status"}, timeout=0.5)
            return reply.get("state") if reply.get("ok") is True else None
        from ccx.codex.runtime import observe_worker
        observation = observe_worker(owner.worker_id, timeout=0.5)
        if not observation["daemon_served"]:
            return None
        return observation["reply"].get("state")
    except Exception:  # an unreadable runtime never refuses; the send decides
        return None


def should_publish_send_attachment(store, owner, delivery_id: str | None,
                                   *, caps: dict | None = None) -> bool:
    """Preflight likely native refusal before touching attached variables.

    Native admission may race after this check. A previously admitted delivery
    token never reapplies its original variable values.
    """
    if delivery_id:
        if owner.engine == "claude":
            from ccx.claude.delivery import read_receipt
        else:
            from ccx.codex.delivery import read_receipt
        prior = read_receipt(owner.worker, delivery_id)
        if prior and prior.get("state") in (
            "owned", "submitted", "accepted", "rejected", "delivered", "abandoned",
        ):
            return False

    from ccx.errors import BudgetStopped, DeliveryRejected
    record = store.record(owner.worker)
    state = (record.get("lifecycle") or {}).get("state")
    # A host or daemon that died mid-turn leaves a stale busy record that the
    # send itself repairs, so only a live runtime's own state refuses here.
    if state in ("busy", "waiting"):
        state = _runtime_state(owner)
    if state in ("busy", "waiting"):
        raise DeliveryRejected(
            f"worker is {state}; wait until idle before sending variables",
            short=owner.short, state=state,
            refusal="busy" if state == "busy" else "state_unavailable",
            delivery_status="rejected",
        )
    if caps:
        if owner.engine == "claude":
            from ccx.claude.usage import cap_tripped, read_usage_cached
            from ccx.constants import USAGE_STALE_LIMIT_S
            data = read_usage_cached(stale_limit=USAGE_STALE_LIMIT_S)
            trip = cap_tripped(data, caps)
        else:
            from ccx.codex.types import codex_cap_tripped
            # A cap preflight may observe the existing daemon, never launch
            # a second provider solely for a speculative send.
            from ccx.codex.runtime import observation_daemon, daemon_call
            deadline = time.monotonic() + 0.5
            try:
                data = (daemon_call({"op": "account-read", "tokens": False}, timeout=0.5)
                        if observation_daemon(deadline) else None)
                if isinstance(data, dict) and "data" in data:
                    data = data["data"]
            except (OSError, ValueError, SessionUnavailable):
                data = None
            trip = codex_cap_tripped(data, caps)
        if trip:
            key, used, limit = trip
            raise BudgetStopped(
                f"budget stop: usage cap {key} already exceeded ({used:g}% >= {limit:g}%)",
                short=owner.short, reason=f"usage cap {key} exceeded", phase="preflight",
            )
    return True


def with_variable_hint(text: str, names: list[str]) -> str:
    if not names:
        return text
    joined = ", ".join(names)
    return (
        text.rstrip()
        + "\n\n[CCX variables available in direct python/python3: "
        + joined
        + ". Rebinding or deleting these names writes back to shared state. "
        + "command/env/explicit Python paths are ordinary native Python.]"
    )


def _managed_shell_env_file(value: str | None) -> bool:
    if not value:
        return False
    candidate = Path(value).expanduser()
    # Identify a CCX bridge from this, a retired, or a foreign generation.
    # No ACTIVE-pointer lookup or foreign shell-file read is needed.
    if _generated_shell_file_shape(candidate):
        return True
    try:
        candidate_resolved = candidate.resolve()
        if _generated_shell_file_shape(candidate_resolved):
            return True
        from ccx.store.root import require_ccx_active
        managed_root = (
            require_ccx_active(constants.CCX_ROOT).pointer.generation_root
            / "variables" / "env"
        ).resolve()
        return candidate_resolved.is_relative_to(managed_root)
    except (OSError, RuntimeError):
        return False


def _generated_shell_file_shape(path: Path) -> bool:
    if (path.suffix != ".sh" or path.parent.name not in {"claude", "codex"}
            or path.parent.parent.name != "env"
            or path.parent.parent.parent.name != "variables"):
        return False
    from ccx.store.identity import canonical_worker_id, IdentityError
    try:
        canonical_worker_id(path.stem)
    except IdentityError:
        return False
    return True


def _managed_zdotdir(value: str | None) -> bool:
    """Recognize this or a foreign generation's Codex zsh trampoline directory.

    A user directory with the same shape is deliberately treated as managed:
    never read or source a foreign worker's .zshenv.
    """
    if not value:
        return False

    def matches(path: Path) -> bool:
        return path.parts[-5:-1] == ("variables", "env", "codex", "zsh")

    candidate = Path(value).expanduser()
    if matches(candidate):
        return True
    try:
        return matches(candidate.resolve())
    except (OSError, RuntimeError):
        return False


_BRIDGE_ORIGINALS = {
    "BASH_ENV": "CCX_BRIDGE_ORIGINAL_BASH_ENV",
    "ZDOTDIR": "CCX_BRIDGE_ORIGINAL_ZDOTDIR",
    "CLAUDE_ENV_FILE": "CCX_BRIDGE_ORIGINAL_CLAUDE_ENV_FILE",
}


def sanitize_inherited_shell_bridge_env(
    env: dict[str, str] | None = None,
) -> dict[str, str]:
    """Restore native user startup paths, never a parent's CCX Python bridge.

    Each original worker-owned shell shim exports one-use snapshots of the
    user's startup paths. A nested worker consumes those snapshots only if
    the inherited value is itself a managed CCX path. Explicit user overrides
    and unset-versus-empty original values are preserved.
    """
    target = os.environ if env is None else env
    for key, marker in _BRIDGE_ORIGINALS.items():
        snapshot_text = target.pop(marker, None)
        value = target.get(key)
        managed = (_managed_zdotdir(value) if key == "ZDOTDIR"
                   else _managed_shell_env_file(value))
        if not managed:
            continue
        target.pop(key, None)
        try:
            original = json.loads(snapshot_text) if isinstance(snapshot_text, str) else None
        except ValueError:
            original = None
        if (isinstance(original, dict) and original.get("set") is True
                and isinstance(original.get("value"), str)):
            restored = original["value"]
            # Refuse a pointer to any other worker, even when forged as a
            # user-original path. Do not source foreign CCX-owned scripts.
            if not (_managed_zdotdir(restored) if key == "ZDOTDIR"
                    else _managed_shell_env_file(restored)):
                target[key] = restored
    return target


def _shell_original_exports(env: Mapping[str, str]) -> list[str]:
    """Emit exact user startup-path snapshots for a managed tool shell."""
    return [
        f"export {marker}={shlex.quote(json.dumps({'set': key in env, 'value': env.get(key)}))}"
        for key, marker in _BRIDGE_ORIGINALS.items()
    ]


def _external_shell_env_file(value: str | None) -> str | None:
    if not value:
        return None
    if _managed_shell_env_file(value):
        return None
    # A bridge from a different CCX_ROOT/generation must not override this
    # worker's python/python3 functions. Do not source or read foreign scripts.
    try:
        path = Path(value).expanduser().resolve()
        if _generated_shell_file_shape(path):
            return None
    except (OSError, RuntimeError):
        return None
    return str(Path(value).expanduser())


def _python_bridge_functions(engine: str, short: str, *, root: Path, zsh: bool) -> list[str]:
    """Render the same Python bridge command for both shell startup files."""
    python = shlex.quote(sys.executable)
    executable = shlex.quote(current_executable_path())
    engine_q = shlex.quote(engine)
    short_q = shlex.quote(short)
    root_q = shlex.quote(str(root))
    lines: list[str] = []
    for name in ("python3", "python"):
        name_q = shlex.quote(name)
        command = (
            f"{python} {executable} _live-vars-exec "
            f"--root {root_q} --engine {engine_q} --short {short_q} "
            f"--invoked-as {name_q} -- \"$@\""
        )
        # macOS may supply python3 without a native ``python`` executable.
        # Do not publish a python function that can only fail with rc 127;
        # do not silently remap python to python3, either.
        native_python = (("whence -p python" if zsh else "type -P python")
                         + " >/dev/null 2>&1 && ") if name == "python" else ""
        lines.extend([
            f"if {native_python}! typeset -f {name} >/dev/null 2>&1 && ! alias {name} >/dev/null 2>&1; then",
            # Quoted zsh function names cannot expand a user-defined alias
            # while the startup script is parsed.
            (f"  function '{name}' {{ {command}; }}" if zsh
             else f"  {name}() {{ {command}; }}"),
            "fi",
        ])
    return lines


def _publish_shell_script(
    store, parts: tuple[str, ...], name: str, data: bytes, *,
    exact_dir_mode: bool = False,
) -> None:
    """Reuse matching private script bytes; publish changed bytes atomically.

    Every comparison occurs against a nofollow, user-owned regular file inside
    the generation's pinned directory. A warm call needs no replace or fsync.
    """
    from ccx.store.root import StorageIntegrityError

    with store.lease():
        with store._gen(*parts, create=True) as directory:
            if exact_dir_mode:
                directory_stat = os.fstat(directory)
                if (not stat.S_ISDIR(directory_stat.st_mode)
                        or directory_stat.st_uid != os.getuid()
                        or stat.S_IMODE(directory_stat.st_mode) != 0o700):
                    raise StorageIntegrityError(
                        "Codex zsh directory must be owner-owned mode 0700"
                    )
            try:
                old = os.stat(name, dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                if (not stat.S_ISREG(old.st_mode) or old.st_uid != os.getuid()
                        or stat.S_IMODE(old.st_mode) != 0o600 or old.st_nlink != 1):
                    raise StorageIntegrityError(
                        "managed shell script must be a private regular file"
                    )
                source_fd = os.open(
                    name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC |
                    os.O_NONBLOCK, dir_fd=directory,
                )
                with os.fdopen(source_fd, "rb") as source:
                    opened = os.fstat(source.fileno())
                    if (not stat.S_ISREG(opened.st_mode)
                            or opened.st_uid != os.getuid()
                            or stat.S_IMODE(opened.st_mode) != 0o600
                            or opened.st_nlink != 1):
                        raise StorageIntegrityError(
                            "managed shell script changed ownership or mode"
                        )
                    existing = source.read(len(data) + 1)
                    finished = os.fstat(source.fileno())
                now = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if (old.st_dev == opened.st_dev == finished.st_dev == now.st_dev
                        and old.st_ino == opened.st_ino == finished.st_ino == now.st_ino
                        and old.st_mtime_ns == finished.st_mtime_ns == now.st_mtime_ns
                        and old.st_size == finished.st_size == now.st_size
                        and existing == data):
                    return

            temp_name = "." + name + "." + uuid.uuid4().hex + ".tmp"
            script_fd = os.open(
                temp_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory,
            )
            try:
                with os.fdopen(script_fd, "wb") as sink:
                    sink.write(data)
                    sink.flush()
                    os.fsync(sink.fileno())
                os.replace(
                    temp_name, name, src_dir_fd=directory, dst_dir_fd=directory,
                )
                os.fsync(directory)
            except BaseException:
                try:
                    os.unlink(temp_name, dir_fd=directory)
                except FileNotFoundError:
                    pass
                raise


def ensure_shell_env(
    engine: str,
    short: str,
    *,
    inherited_env_file: str | None = None,
) -> Path:
    store, owner = _env_worker(engine, short)
    return _ensure_shell_env_owned(
        store, engine, owner.worker_id, inherited_env_file=inherited_env_file,
    )


def _ensure_shell_env_owned(
    store, engine: str, worker_id: str, *,
    inherited_env_file: str | None,
    native_env: Mapping[str, str] | None = None,
) -> Path:
    """Use an already-attested full UUID; the generation lease revalidates it."""
    path = (store.pointer.generation_root / "variables" / "env" /
            engine / f"{worker_id}.sh")
    inherited_env_file = _external_shell_env_file(inherited_env_file)

    lines = ["# generated by ccx; sourced for managed worker shell commands"]
    if inherited_env_file:
        inherited = Path(inherited_env_file).expanduser()
        try:
            same = inherited.resolve() == path.resolve()
        except OSError:
            same = False
        if not same:
            lines.extend(
                [
                    f"if [ -f {shlex.quote(str(inherited))} ]; then",
                    f"  . {shlex.quote(str(inherited))}",
                    "fi",
                ]
            )
    lines.extend(_shell_original_exports(os.environ if native_env is None else native_env))
    lines.extend(_python_bridge_functions(engine, worker_id, root=store.root, zsh=False))
    text = "\n".join(lines) + "\n"
    # Only the sealed ACTIVE generation's uid-private owner directory may
    # receive this shell script. Never resurrect CCX_ROOT/variables/<short>.
    from ccx.store.root import _FILE_LIMIT
    data = text.encode("utf-8")
    if len(data) > _FILE_LIMIT:
        raise SessionUnavailable("managed full-UUID shell environment script too large")
    _publish_shell_script(store, ("variables", "env", engine), path.name, data)
    return path


def codex_zdotdir_path(worker_id: str) -> Path:
    store, owner = _env_worker("codex", worker_id)
    return (store.pointer.generation_root / "variables" / "env" / "codex" /
            "zsh" / owner.worker_id)


def ensure_codex_zshenv(worker_id: str, *, original_zdotdir: str | None) -> Path:
    """Install the restore-first .zshenv in the sealed, full-UUID generation."""
    store, owner = _env_worker("codex", worker_id)
    return _ensure_codex_zshenv_owned(
        store, owner.worker_id, original_zdotdir=original_zdotdir,
    )


def _zsh_user_startup_lines(stage: str, managed: Path, *,
                            original_zdotdir: str | None = None) -> list[str]:
    """Replay native user startup stages while keeping CCX ZDOTDIR effective.

    A nested zsh is a new shell of the *same worker*, so its own .zshenv must
    see the managed directory. The original user's directory is remembered
    only for startup-file sourcing; the previous user's .zshenv (or any later
    startup stage) may update its subsequent native lookup directory.
    """
    lines = [f"# generated by ccx; native user {stage} once, CCX bridge retained"]
    if stage == ".zshenv":
        lines.extend([
            f"typeset -g __ccx_managed_zdotdir={shlex.quote(str(managed))}",
            f"typeset -g __ccx_user_zdotdir_set={'0' if original_zdotdir is None else '1'}",
            f"typeset -g __ccx_user_zdotdir_value={shlex.quote(original_zdotdir or '')}",
        ])
    lines.extend([
        'if [[ $__ccx_user_zdotdir_set == 0 ]]; then',
        '  unset ZDOTDIR',
        f'  __ccx_user_startup="$HOME/{stage}"',
        'elif [[ -z $__ccx_user_zdotdir_value ]]; then',
        "  export ZDOTDIR=''",
        '  __ccx_user_startup=""',
        'else',
        '  export ZDOTDIR="$__ccx_user_zdotdir_value"',
        f'  __ccx_user_startup="${{__ccx_user_zdotdir_value}}/{stage}"',
        'fi',
        'if [[ -n $__ccx_user_startup && -f $__ccx_user_startup ]]; then',
        '  case "${__ccx_user_startup:A}" in',
        # A retired/foreign CCX-generated shim is not a user startup file.
        f'    {shlex.quote(str(managed))}/.*|*/variables/env/codex/zsh/*/.*) ;;',
        '    *) builtin source "$__ccx_user_startup" ;;',
        '  esac',
        'fi',
        'if [[ -z ${ZDOTDIR+x} ]]; then',
        '  __ccx_user_zdotdir_set=0',
        '  __ccx_user_zdotdir_value=""',
        'else',
        '  __ccx_user_zdotdir_set=1',
        '  __ccx_user_zdotdir_value="$ZDOTDIR"',
        'fi',
        'export ZDOTDIR="$__ccx_managed_zdotdir"',
        'unset __ccx_user_startup',
    ])
    return lines


def _ensure_codex_zshenv_owned(
    store, worker_id: str, *, original_zdotdir: str | None,
    native_env: Mapping[str, str] | None = None,
) -> Path:
    from ccx.store.root import _FILE_LIMIT

    directory = (store.pointer.generation_root / "variables" / "env" /
                 "codex" / "zsh" / worker_id)
    if _managed_zdotdir(original_zdotdir):
        original_zdotdir = None
    for stage in (".zshenv", ".zprofile", ".zshrc", ".zlogin", ".zlogout"):
        lines = _zsh_user_startup_lines(
            stage, directory, original_zdotdir=original_zdotdir,
        )
        if stage == ".zshenv":
            lines.extend(_shell_original_exports(
                os.environ if native_env is None else native_env,
            ))
            lines.extend(_python_bridge_functions("codex", worker_id, root=store.root, zsh=True))
        data = ("\n".join(lines) + "\n").encode("utf-8")
        if len(data) > _FILE_LIMIT:
            raise SessionUnavailable("managed full-UUID zsh startup script too large")
        _publish_shell_script(
            store, ("variables", "env", "codex", "zsh", worker_id),
            stage, data, exact_dir_mode=True,
        )
    return directory


def configure_claude_worker_env(
    env: dict[str, str], short: str, *, host_pid: int,
) -> dict[str, str]:
    sanitize_inherited_shell_bridge_env(env)
    inherited = env.get("CLAUDE_ENV_FILE")
    store, owner = _env_worker("claude", short)
    path = _ensure_shell_env_owned(
        store, "claude", owner.worker_id, inherited_env_file=inherited,
        native_env=env,
    )
    env["CLAUDE_ENV_FILE"] = str(path)
    mark_claude_bridge(short, host_pid)
    return env


def codex_thread_config(worker_id: str) -> dict[str, Any]:
    """Attested full-UUID Bash/zsh bridges for one original native Codex thread.

    The daemon first validates the native claim/source/dispatch; this function
    only prepares the shell config and cannot start/adopt a native thread.
    Preserve Codex's user/default inheritance policy.
    """
    store, owner = _env_worker("codex", worker_id)
    # The per-thread policy may be constructed by a nested CCX caller as
    # well as by the shared (already scrubbed) codexd. Never capture that
    # caller's bridge as this worker's genuine user-shell baseline.
    native_env = sanitize_inherited_shell_bridge_env(dict(os.environ))
    bash_env = _ensure_shell_env_owned(
        store, "codex", owner.worker_id,
        inherited_env_file=native_env.get("BASH_ENV"), native_env=native_env,
    )
    zdotdir = _ensure_codex_zshenv_owned(
        store, owner.worker_id, original_zdotdir=native_env.get("ZDOTDIR"),
        native_env=native_env,
    )
    return {"shell_environment_policy": {"set": {
        "CCX_SELF_WORKER_ID": owner.worker_id,
        "CCX_SELF_ENGINE": "codex",
        "CCX_ROOT": str(constants.CCX_ROOT),
        "BASH_ENV": str(bash_env),
        "ZDOTDIR": str(zdotdir),
    }}}


def mark_claude_bridge(short: str, host_pid: int) -> None:
    """Store exact full owner + current host pid as an atomic owned sidecar."""
    from ccx.store.root import _write_json
    store, owner = _env_worker("claude", short)
    with store.lease():
        with store._gen("variables", "env", "claude", create=True) as fd:
            _write_json(fd, owner.worker_id + ".bridge.json", {
                "workerId": owner.worker_id,
                "engine": "claude",
                "generation": store.generation,
                "hostPid": int(host_pid),
            })


def claude_bridge_ready(short: str, host_pid: int | None) -> bool:
    if not host_pid:
        return False
    from ccx.store.root import _read_json
    store, owner = _env_worker("claude", short)
    with store.lease():
        try:
            with store._gen("variables", "env", "claude") as fd:
                value = _read_json(fd, owner.worker_id + ".bridge.json")
        except FileNotFoundError:
            return False
    return (
        isinstance(value, dict)
        and value.get("workerId") == owner.worker_id
        and value.get("engine") == "claude"
        and value.get("generation") == store.generation
        and value.get("hostPid") == int(host_pid)
    )


def require_send_variable_permission(owner, record: dict) -> None:
    """Refuse a later-SEND variable attachment the original worker cannot take."""
    create_args = record.get("createArgs") or {}
    if owner.engine == "claude":
        if not bool(create_args.get("skip_permissions", True)):
            raise CapabilityUnsupported(
                'Shared variables require full permissions (permissions="full")',
                short=owner.short, engine=owner.engine,
                capability="shared_python_variables",
            )
        lifecycle = record.get("lifecycle") or {}
        state = lifecycle.get("state")
        host_pid = lifecycle.get("hostPid")
        if (state not in {"dead", "crashed", "idle-killed", "budget-stopped"}
                and host_pid
                and not claude_bridge_ready(owner.worker_id, host_pid)):
            raise SessionUnavailable(
                "this live Claude host predates the CCX shared-Python bridge; "
                "kill/revive it or create a new worker before attaching variables",
                short=owner.short,
            )
    elif not (create_args.get("sandbox") == "danger-full-access"
              and create_args.get("approvalPolicy") == "never"):
        raise CapabilityUnsupported(
            'Shared variables require full permissions (permissions="full")',
            short=owner.short, engine=owner.engine,
            capability="shared_python_variables",
        )


class _ThreadStreamProxy:
    def __init__(self, original):
        self.original = original
        self._routes: dict[int, Any] = {}
        self._lock = threading.Lock()

    def _target(self):
        with self._lock:
            return self._routes.get(threading.get_ident(), self.original)

    @contextmanager
    def route(self, target):
        ident = threading.get_ident()
        with self._lock:
            self._routes[ident] = target
        try:
            yield
        finally:
            with self._lock:
                self._routes.pop(ident, None)

    def write(self, value):
        return self._target().write(value)

    def read(self, *args):
        return self._target().read(*args)

    def readline(self, *args):
        return self._target().readline(*args)

    def flush(self):
        return self._target().flush()

    def isatty(self):
        return self._target().isatty()

    def fileno(self):
        return self._target().fileno()

    @property
    def buffer(self):
        return self._target().buffer

    @property
    def encoding(self):
        return getattr(self._target(), "encoding", None)

    @property
    def errors(self):
        return getattr(self._target(), "errors", None)

    def __iter__(self):
        return iter(self._target())

    def __getattr__(self, name):
        return getattr(self._target(), name)


@dataclass
class _Binding:
    mapping: MutableMapping[str, Any]
    key: str


class LiveOwner:
    def __init__(self):
        self.pid = os.getpid()
        self.token = uuid.uuid4().hex
        basename = f"{self.pid}-{self.token}.sock"
        owner_dir, choice = _owner_runtime(basename)
        candidate = owner_dir / basename
        _require_unix_socket_budget(candidate)  # before any parent writes
        self.socket_path = owners_dir(owner_dir, runtime=choice) / basename
        self._registry_lock = threading.RLock()
        self._exec_lock = threading.Lock()
        self._workers: dict[tuple[str, str], dict[str, _Binding]] = {}
        self._queue: queue.Queue[Any] = queue.Queue()
        self._closed = threading.Event()
        self._current: dict[str, Any] | None = None

        self._old_stdin = sys.stdin
        self._old_stdout = sys.stdout
        self._old_stderr = sys.stderr
        self._stdin_proxy = _ThreadStreamProxy(sys.stdin)
        self._stdout_proxy = _ThreadStreamProxy(sys.stdout)
        self._stderr_proxy = _ThreadStreamProxy(sys.stderr)
        sys.stdin = self._stdin_proxy
        sys.stdout = self._stdout_proxy
        sys.stderr = self._stderr_proxy

        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass
            bind_private_unix_socket(self._server, self.socket_path)
            self._server.listen()
            # Closing a listening fd from another thread does not reliably
            # interrupt a blocked accept() on Linux. Poll for shutdown.
            self._server.settimeout(0.1)
        except BaseException:
            try:
                self._server.close()
            except OSError:
                pass
            if sys.stdin is self._stdin_proxy:
                sys.stdin = self._old_stdin
            if sys.stdout is self._stdout_proxy:
                sys.stdout = self._old_stdout
            if sys.stderr is self._stderr_proxy:
                sys.stderr = self._old_stderr
            raise

        self._executor_thread = threading.Thread(
            target=self._executor_loop,
            name=f"ccx-live-vars-{self.pid}",
            daemon=True,
        )
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name=f"ccx-live-vars-accept-{self.pid}",
            daemon=True,
        )
        self._executor_thread.start()
        self._accept_thread.start()

    def public_identity(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "token": self.token,
            "socket": str(self.socket_path),
            "python": {
                "major": sys.version_info.major,
                "minor": sys.version_info.minor,
                "cacheTag": sys.implementation.cache_tag,
                "platform": sysconfig.get_platform(),
            },
        }

    def register_worker(
        self,
        engine: str,
        short: str,
        mapping: MutableMapping[str, Any],
        names: list[str],
    ) -> None:
        self._exec_lock.acquire()
        try:
            with self._registry_lock:
                bindings = self._workers.setdefault((engine, short), {})
                for name in names:
                    bindings[name] = _Binding(mapping=mapping, key=name)
        finally:
            self._exec_lock.release()

    def worker_names(self, engine: str, short: str) -> list[str]:
        with self._registry_lock:
            return sorted(self._workers.get((engine, short), {}))

    def _accept_loop(self) -> None:
        while not self._closed.is_set():
            try:
                conn = accept_same_user(self._server)
            except socket.timeout:
                continue
            except OSError:
                return
            if conn is None:
                continue
            threading.Thread(
                target=self._handle_connection,
                args=(conn,),
                daemon=True,
                name="ccx-live-vars-client",
            ).start()

    @staticmethod
    def _read_line(conn: socket.socket) -> bytes:
        data = bytearray()
        while b"\n" not in data:
            chunk = conn.recv(65536)
            if not chunk:
                break
            data.extend(chunk)
        return bytes(data).split(b"\n", 1)[0]

    @staticmethod
    def _send(conn: socket.socket, value: dict[str, Any]) -> None:
        conn.sendall((_WIRE_DUMPS(value, separators=(",", ":")) + "\n").encode("utf-8"))

    def _handle_connection(self, conn: socket.socket) -> None:
        try:
            if self._closed.is_set():
                self._send(conn, {"ok": False, "errorKind": "state_unavailable"})
                conn.close()
                return
            raw = self._read_line(conn)
            request = _WIRE_LOADS(raw) if raw else {}
            if request.get("token") != self.token:
                self._send(conn, {"ok": False, "errorKind": "state_unavailable"})
                conn.close()
                return
            if request.get("op") == "ping":
                self._send(conn, {"ok": True})
                conn.close()
                return
            if request.get("op") != "exec":
                self._send(conn, {"ok": False, "errorKind": "invalid_request"})
                conn.close()
                return
            self._exec_lock.acquire()
            if self._closed.is_set():
                self._exec_lock.release()
                self._send(conn, {"ok": False, "errorKind": "state_unavailable"})
                conn.close()
                return
            self._current = {
                "worker": f"{request.get('engine')}:{request.get('short')}",
                "startedAt": time.time(),
            }
            self._queue.put((conn, request))
        except Exception as error:
            try:
                self._send(
                    conn,
                    {
                        "ok": False,
                        "errorKind": "protocol_error",
                        "message": f"{type(error).__name__}: {error}",
                    },
                )
            except OSError:
                pass
            conn.close()

    def _restore_streams(self) -> None:
        if sys.stdin is self._stdin_proxy:
            sys.stdin = self._old_stdin
        if sys.stdout is self._stdout_proxy:
            sys.stdout = self._old_stdout
        if sys.stderr is self._stderr_proxy:
            sys.stderr = self._old_stderr

    def _executor_loop(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                self._restore_streams()
                return
            conn, request = item
            try:
                response = self._execute(request)
                self._send(conn, response)
            except BaseException as error:
                try:
                    self._send(
                        conn,
                        {
                            "ok": False,
                            "errorKind": "owner_error",
                            "message": f"{type(error).__name__}: {error}",
                        },
                    )
                except BaseException:
                    pass
            finally:
                try:
                    conn.close()
                except OSError:
                    pass
                self._current = None
                self._exec_lock.release()

    def _execute(self, request: dict[str, Any]) -> dict[str, Any]:
        engine = str(request.get("engine") or "")
        short = str(request.get("short") or "")
        with self._registry_lock:
            bindings = dict(self._workers.get((engine, short), {}))
        if not bindings:
            return {"ok": False, "errorKind": "state_unavailable"}

        seed: dict[str, Any] = {}
        for name, binding in bindings.items():
            if name in binding.mapping:
                seed[name] = binding.mapping[name]

        native = request.get("nativePython")
        if not isinstance(native, dict):
            native = {}
        stdin_bytes = base64.b64decode(request.get("stdinBase64") or "")
        stdin_buffer = io.BytesIO(stdin_bytes)
        stdout_buffer = io.BytesIO()
        stderr_buffer = io.BytesIO()
        stdin_text = io.TextIOWrapper(
            stdin_buffer,
            encoding=getattr(self._old_stdin, "encoding", None) or "utf-8",
            errors=getattr(self._old_stdin, "errors", None) or "strict",
        )
        stdout_text = io.TextIOWrapper(
            stdout_buffer,
            encoding=getattr(self._old_stdout, "encoding", None) or "utf-8",
            errors=getattr(self._old_stdout, "errors", None) or "replace",
            write_through=True,
        )
        stderr_text = io.TextIOWrapper(
            stderr_buffer,
            encoding=getattr(self._old_stderr, "encoding", None) or "utf-8",
            errors=getattr(self._old_stderr, "errors", None) or "replace",
            write_through=True,
        )

        old_cwd = os.getcwd()
        old_argv = sys.argv[:]
        old_path = sys.path[:]
        try:
            cwd = str(request.get("cwd") or old_cwd)
            os.chdir(cwd)
            native_path = native.get("sysPath")
            if isinstance(native_path, list) and all(
                isinstance(value, str) for value in native_path
            ):
                sys.path[:] = native_path
            args = request.get("args")
            if not isinstance(args, list) or not all(isinstance(v, str) for v in args):
                return {"ok": False, "errorKind": "invalid_request"}
            with (
                self._stdin_proxy.route(stdin_text),
                self._stdout_proxy.route(stdout_text),
                self._stderr_proxy.route(stderr_text),
            ):
                exit_code, final_ns = execute_target(args, seed)
                stdout_text.flush()
                stderr_text.flush()
        finally:
            os.chdir(old_cwd)
            sys.argv = old_argv
            sys.path[:] = old_path

        deleted: list[str] = []
        with self._registry_lock:
            current = self._workers.get((engine, short), {})
            for name, binding in bindings.items():
                if name in final_ns:
                    binding.mapping[name] = final_ns[name]
                else:
                    binding.mapping.pop(name, None)
                    if current.get(name) is binding:
                        current.pop(name, None)
                    deleted.append(name)
            names = sorted(current)

        self._publish_live_names(engine, short, names)
        return {
            "ok": True,
            "exitCode": int(exit_code),
            "stdoutBase64": base64.b64encode(stdout_buffer.getvalue()).decode("ascii"),
            "stderrBase64": base64.b64encode(stderr_buffer.getvalue()).decode("ascii"),
            "deleted": deleted,
        }

    def _publish_live_names(self, engine: str, short: str, names: list[str]) -> None:
        with _worker_lock(engine, short):
            value = read_sidecar(engine, short)
            if (
                not value
                or value.get("mode") != "live"
                or (value.get("owner") or {}).get("token") != self.token
            ):
                return
            value["names"] = names
            _atomic_json(sidecar_path(engine, short), value)

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        try:
            self._server.close()
        except OSError:
            pass
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass
        self._queue.put(None)
        # Wait briefly for both daemon loops to finish. In particular, the
        # executor restores sys.stdin/stdout/stderr; without this join a new
        # LiveOwner can wrap a proxy from a closed owner and strand it forever.
        for worker in (self._accept_thread, self._executor_thread):
            if threading.current_thread() is not worker:
                worker.join(timeout=1.0)
        # Never remove a sidecar that a newer controller or durable send owns.
        for engine, worker_id in list(self._workers):
            try:
                store, owned = _env_worker(engine, worker_id)
                with _worker_lock(engine, owned.worker_id):
                    value = read_sidecar(engine, owned.worker_id)
                    if (value and value.get("mode") == "live"
                            and (value.get("owner") or {}).get("token") == self.token):
                        _unlink_sidecar(store, engine, owned.worker_id)
            except (OSError, SessionUnavailable):
                pass


def _native_python_metadata(executable: str) -> dict[str, Any] | None:
    code = (
        "import json,sys,sysconfig;"
        "print(json.dumps({'version':list(sys.version_info[:3]),"
        "'cacheTag':sys.implementation.cache_tag,"
        "'platform':sysconfig.get_platform(),"
        "'executable':sys.executable,'prefix':sys.prefix,"
        "'execPrefix':sys.exec_prefix,'sysPath':sys.path}))"
    )
    try:
        output = subprocess.check_output(
            [executable, "-c", code],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        value = json.loads(output)
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _compatible(owner: Mapping[str, Any], native: Mapping[str, Any]) -> bool:
    version = native.get("version")
    return bool(
        isinstance(version, list)
        and len(version) >= 2
        and version[0] == owner.get("major")
        and version[1] == owner.get("minor")
        and native.get("platform") == owner.get("platform")
        and native.get("cacheTag") == owner.get("cacheTag")
    )


def _durable_runner_argv(
    executable: str,
    *,
    state: Path,
    lock: Path,
    cwd: str,
    args: list[str],
) -> list[str]:
    runner = Path(__file__).with_name("live_vars_runner.py")
    bootstrap = (
        "import runpy; "
        f"runpy.run_path({str(runner)!r}, run_name='__main__')"
    )
    return [
        executable,
        "-c",
        bootstrap,
        "--state",
        str(state),
        "--lock",
        str(lock),
        "--cwd",
        cwd,
        "--real-python",
        executable,
        "--",
        *args,
    ]


def _exec_native(executable: str, args: list[str], *, env: dict[str, str] | None = None) -> int:
    os.execvpe(executable, [executable, *args], os.environ.copy() if env is None else env)
    return 127


def worker_exec(engine: str, short: str, invoked_as: str, args: list[str], *,
                native_env: dict[str, str] | None = None) -> int:
    from ccx.store.root import GenerationError
    from ccx.store.identity import IdentityError

    real_python = shutil.which(invoked_as)
    if not real_python:
        print(f"ccx: cannot resolve native {invoked_as}", file=sys.stderr)
        return 127

    # Stage0's exact ACTIVE owner/read gate stays before any native execution.
    try:
        store, owned = _env_worker(engine, short)
        binding = read_sidecar(engine, owned.worker_id)
    except (IdentityError, GenerationError, OSError) as error:
        print(f"CcxStateUnavailable: managed worker variables unavailable: {error}",
              file=sys.stderr)
        return 69
    if binding is None:
        return _exec_native(real_python, args, env=native_env)

    if len(args) >= 2 and args[0] == "-m" and args[1] in _NATIVE_FALLTHROUGH_MODULES:
        return _exec_native(real_python, args, env=native_env)
    # Shared variables cover `-c`, `-m`, stdin and scripts; any other leading
    # interpreter option (--version, -V, -u, -I, ...) runs native Python.
    if args and args[0].startswith("-") and args[0] not in ("-", "-c", "-m"):
        return _exec_native(real_python, args, env=native_env)

    short = owned.worker_id
    if binding["mode"] == "durable":
        try:
            state_id = _valid_state_id(binding.get("stateId"))
            with store.lease():
                state = state_path(state_id)
                lock = state_lock_path(state_id)
        except (GenerationError, SessionUnavailable, OSError) as error:
            print(f"CcxStateUnavailable: durable variable state unavailable: {error}",
                  file=sys.stderr)
            return 69
        os.execvpe(
            real_python,
            _durable_runner_argv(
                real_python, state=state, lock=lock, cwd=os.getcwd(), args=args,
            ),
            os.environ.copy() if native_env is None else native_env,
        )
        return 127

    if binding["mode"] != "live":
        print("CcxStateUnavailable: invalid worker variable mode", file=sys.stderr)
        return 69
    try:
        value = read_sidecar(engine, short)
    except (IdentityError, GenerationError, OSError) as error:
        print(f"CcxStateUnavailable: live variable binding unavailable: {error}",
              file=sys.stderr)
        return 69
    if not value or value.get("mode") != "live":
        print("CcxStateUnavailable: live variable binding changed", file=sys.stderr)
        return 69

    owner = value.get("owner")
    if not isinstance(owner, dict):
        print("CcxStateUnavailable: live variable owner metadata is missing", file=sys.stderr)
        return 69
    if _owner_gone(owner):
        _clear_dead_binding(engine, short, str(owner.get("token") or ""))
        return _exec_native(real_python, args, env=native_env)
    owner_python = owner.get("python")
    native = _native_python_metadata(real_python)
    if not isinstance(owner_python, dict) or native is None:
        print(
            "ccx: cannot verify Python compatibility for shared variables; running native Python",
            file=sys.stderr,
        )
        return _exec_native(real_python, args, env=native_env)
    if not _compatible(owner_python, native):
        print(
            "ccx: selected Python is incompatible with the live variable owner; running native Python",
            file=sys.stderr,
        )
        return _exec_native(real_python, args, env=native_env)

    request = {
        "schemaVersion": VARIABLE_SCHEMA_VERSION,
        "op": "exec",
        "token": owner.get("token"),
        "engine": engine,
        "short": short,
        "args": args,
        "cwd": os.getcwd(),
        "stdinBase64": base64.b64encode(
            sys.stdin.buffer.read() if not args or args[0] == "-" else b""
        ).decode("ascii"),
        "nativePython": native,
    }
    sock = owner.get("socket")
    if not isinstance(sock, str):
        print("CcxStateUnavailable: live variable owner socket is missing", file=sys.stderr)
        return 69

    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.connect(sock)
        client.sendall((json.dumps(request, separators=(",", ":")) + "\n").encode("utf-8"))
        raw = LiveOwner._read_line(client)
    except OSError as error:
        print(
            f"CcxStateUnavailable: shared variable owner is unavailable: {error}",
            file=sys.stderr,
        )
        return 69
    finally:
        client.close()

    try:
        response = json.loads(raw)
    except ValueError:
        print("CcxStateUnavailable: invalid response from shared variable owner", file=sys.stderr)
        return 69

    if not response.get("ok"):
        kind = response.get("errorKind")
        if kind == "owner_busy":
            print(
                f"CcxStateBusy: shared Python owner is busy: {response.get('current')}",
                file=sys.stderr,
            )
            return 75
        print(
            "CcxStateUnavailable: shared variable execution is unavailable"
            + (f": {response.get('message')}" if response.get("message") else ""),
            file=sys.stderr,
        )
        return 69

    stdout = base64.b64decode(response.get("stdoutBase64") or "")
    stderr = base64.b64decode(response.get("stderrBase64") or "")
    if stdout:
        sys.stdout.buffer.write(stdout)
        sys.stdout.buffer.flush()
    if stderr:
        sys.stderr.buffer.write(stderr)
        sys.stderr.buffer.flush()
    return int(response.get("exitCode") or 0)


def cmd_worker_exec(args) -> None:
    target_args = list(getattr(args, "target_args", []) or [])
    if target_args and target_args[0] == "--":
        target_args = target_args[1:]
    raise SystemExit(
        worker_exec(
            str(args.engine),
            str(args.short),
            str(args.invoked_as),
            target_args,
            native_env=getattr(args, "_bridge_native_env", None),
        )
    )
