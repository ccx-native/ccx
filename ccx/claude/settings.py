from __future__ import annotations

import contextlib
import fcntl
import json
import os
import pathlib
import shlex
import sys
import tempfile
import time
import unicodedata

from ccx.system import oslayer
from ccx.constants import CLAUDE_DOTFILE, PERMISSION_HOOK_TIMEOUT_S
from ccx.system.executable import current_executable_path
from ccx.store.paths import private_mkdir, settings_path, sock_path, trust_ledger_path

_TRUST_KEY = "hasTrustDialogAccepted"
# Ledger "change" values: what a ccx trust write did to one projects entry.
TRUST_CREATED_ENTRY = "created-entry"
TRUST_ADDED_KEY = "added-key"
TRUST_REPLACED_VALUE = "replaced-value"
_CAS_ATTEMPTS = 5

def _write_claude_config(data) -> None:
    """Atomically write the selected native profile's global metadata.

    The default stays HOME/.claude.json; an explicit CLAUDE_CONFIG_DIR uses
    that native profile's .claude.json. Never write trust to the wrong profile.
    """
    CLAUDE_DOTFILE.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = CLAUDE_DOTFILE.with_suffix(".json.ccx.tmp")
    tmp.write_text(json.dumps(data, indent=2))
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    tmp.replace(CLAUDE_DOTFILE)


def ensure_trusted(cwd: str) -> bool:
    return ensure_trusted_paths([cwd])


def ensure_trusted_paths(paths: list[str]) -> bool:
    """Trust the cwd and added directories in one native-profile rewrite.

    Each directory needs its own exact-path entry: ancestor trust does not
    override a child's explicit false value. Missing trust can block Claude
    before SessionStart, leaving the host waiting for the boot watchdog.
    Return whether a change was written; unreadable configs remain untouched.
    """
    abs_paths = []
    seen = set()
    for raw in paths or []:
        if not raw:
            continue
        # Claude's native trust lookup uses the same key as targeted untrust.
        ap = _requested_trust_path(raw)
        if ap not in seen:
            seen.add(ap)
            abs_paths.append(ap)
    if not abs_paths:
        return False

    def trust(data: dict) -> dict[str, dict] | None:
        projects = data.setdefault("projects", {})
        if not isinstance(projects, dict):
            return None
        changes: dict[str, dict] = {}
        for ap in abs_paths:
            # Exact path only. An ancestor's trust does not cover a child that
            # carries its own `false`, and a missing entry is not trust either.
            entry = projects.get(ap)
            if isinstance(entry, dict) and entry.get(_TRUST_KEY) is True:
                continue
            if not isinstance(entry, dict):
                changes[ap] = {"change": TRUST_CREATED_ENTRY}
                projects[ap] = entry = {}
            elif _TRUST_KEY not in entry:
                changes[ap] = {"change": TRUST_ADDED_KEY}
            else:
                changes[ap] = {"change": TRUST_REPLACED_VALUE, "previous": entry[_TRUST_KEY]}
            entry[_TRUST_KEY] = True
        return changes or None

    with _trust_lock():
        changes = _update_claude_config(trust)
        if not changes:
            return False
        # Record only what this write changed, so `ccx untrust` can undo
        # exactly ccx's entries. The first record for a path wins: a later
        # re-trust after someone else flipped it back is still ccx's doing.
        ledger = _read_trust_ledger()
        recorded = ledger["configs"].setdefault(str(CLAUDE_DOTFILE), {})
        now_ms = int(time.time() * 1000)
        for ap, change in changes.items():
            recorded.setdefault(ap, {**change, "at_ms": now_ms})
        _write_trust_ledger(ledger)
    return True


@contextlib.contextmanager
def _trust_lock():
    """Serialize ccx's own trust writers (Claude's writers are handled by CAS)."""
    path = trust_ledger_path().with_name("trust-ledger.lock")
    private_mkdir(path.parent)
    with os.fdopen(os.open(path, os.O_RDWR | os.O_CREAT, 0o600), "r+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _update_claude_config(mutate):
    """Read-modify-write the native .claude.json with compare-and-swap.

    Claude rewrites this file itself. Re-reading it just before the replace and
    starting over when its bytes moved keeps our write from erasing a
    concurrent one; the remaining window is the rename itself. ``mutate``
    edits the parsed document in place and returns a truthy result when it
    changed something; that result is returned. Fails open (None) on an
    unreadable or unparseable config, or if the file keeps changing.
    """
    for _ in range(_CAS_ATTEMPTS):
        before = _read_config_bytes()
        if before is False:
            return None
        if before is None:
            data: dict = {}
        else:
            try:
                data = json.loads(before)
            except ValueError:
                return None
            if not isinstance(data, dict):
                return None
        result = mutate(data)
        if not result:
            return None
        if _read_config_bytes() != before:
            continue
        _write_claude_config(data)
        return result
    return None


def _read_config_bytes() -> bytes | None | bool:
    """The config's bytes, None when it does not exist, False when unreadable."""
    try:
        return CLAUDE_DOTFILE.read_bytes()
    except FileNotFoundError:
        return None
    except OSError:
        return False


def _read_trust_ledger() -> dict:
    try:
        doc = json.loads(trust_ledger_path().read_text())
    except (OSError, ValueError):
        doc = {}
    configs = doc.get("configs") if isinstance(doc, dict) else None
    if not isinstance(configs, dict):
        configs = {}
    return {"schemaVersion": 1, "configs": configs}


def _write_trust_ledger(ledger: dict) -> None:
    path = trust_ledger_path()
    private_mkdir(path.parent)
    fd, tmp = tempfile.mkstemp(prefix=".trust-ledger.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(json.dumps(ledger, indent=2, sort_keys=True))
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def trusted_by_ccx() -> dict[str, dict]:
    """Ledger entries for the selected native profile: path -> what ccx changed."""
    return dict(_read_trust_ledger()["configs"].get(str(CLAUDE_DOTFILE), {}))


def _requested_trust_path(path: str) -> str:
    """Match Claude's native trust key, even after the final dir vanished.

    First resolve the physical cwd and any existing symlinks; then apply the
    same macOS NFC rule as Claude's project path, leaving Linux unchanged.
    Only the resulting exact ledger key may be undone.
    """
    from ccx.system.cwd_paths import canonical_cwd
    key = canonical_cwd(path)
    return unicodedata.normalize("NFC", key) if oslayer.claude_project_cwd_is_nfc() else key


def untrust_paths(paths: list[str] | None = None, *, dry_run: bool = False) -> list[dict]:
    """Undo ccx's own trust writes in the selected native profile.

    Only paths in the ledger are touched, and only while their entry still
    holds the ``true`` ccx wrote: a created entry loses the key (and goes away
    if nothing else was added to it), an added key is removed, and an
    overridden value is restored. An entry someone changed since is left as
    it is and dropped from the ledger. ``paths`` limits the undo (default:
    every recorded path). Returns one ``{path, action}`` row per path, with
    ``action`` in ``removed-entry``, ``removed-key``, ``restored``,
    ``changed-since``, ``not-recorded`` or ``retry`` (the config kept
    changing; the ledger row is kept).
    """
    with _trust_lock():
        ledger = _read_trust_ledger()
        recorded = ledger["configs"].get(str(CLAUDE_DOTFILE), {})
        if paths:
            wanted = list(dict.fromkeys(_requested_trust_path(p) for p in paths if p))
        else:
            wanted = sorted(recorded)
        rows: list[dict] = []

        def undo(data: dict) -> list[dict] | None:
            rows.clear()
            projects = data.get("projects")
            projects = projects if isinstance(projects, dict) else {}
            changed = False
            for ap in wanted:
                change = recorded.get(ap)
                if change is None:
                    rows.append({"path": ap, "action": "not-recorded"})
                    continue
                entry = projects.get(ap)
                if not isinstance(entry, dict) or entry.get(_TRUST_KEY) is not True:
                    rows.append({"path": ap, "action": "changed-since"})
                    continue
                kind = change.get("change")
                if kind == TRUST_REPLACED_VALUE:
                    entry[_TRUST_KEY] = change.get("previous")
                    action = "restored"
                else:
                    del entry[_TRUST_KEY]
                    action = "removed-key"
                    if kind == TRUST_CREATED_ENTRY and not entry:
                        del projects[ap]
                        action = "removed-entry"
                rows.append({"path": ap, "action": action})
                changed = True
            # A no-op result still has rows to report and ledger rows to drop.
            return rows if changed and not dry_run else None

        written = bool(wanted) and _update_claude_config(undo) is not None
        if not dry_run and not written:
            # The config kept changing under us (or became unreadable): keep
            # those ledger rows so a retry can still undo them.
            rows[:] = [row if row["action"] in ("changed-since", "not-recorded")
                       else {"path": row["path"], "action": "retry"} for row in rows]
        if not rows and wanted:
            # No callback ran: retain undo records until the config can be inspected.
            rows = [{"path": ap, "action": "retry" if ap in recorded else "not-recorded"}
                    for ap in wanted]
        if not dry_run and rows:
            for row in rows:
                if row["action"] != "retry":
                    recorded.pop(row["path"], None)
            if not recorded:
                ledger["configs"].pop(str(CLAUDE_DOTFILE), None)
            _write_trust_ledger(ledger)
        return [dict(row) for row in rows]

# --- settings (hooks) generator --------------------------------------------

HOOKS_WE_CARE_ABOUT = (
    "SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse",
    "PostToolUseFailure",
    "Stop", "SubagentStart", "SubagentStop", "Notification", "PermissionRequest",
    "Elicitation",
)
# Hooks whose relay blocks until a CCX caller decides (approve/answer/respond).
# They get the long broker budget instead of Claude's default hook timeout.
BROKERED_HOOKS = ("PermissionRequest", "Elicitation")

def write_settings(short: str) -> pathlib.Path:
    """Generate the per-session settings JSON with hook commands pointing at our relay.

    sys.argv[0] may be whatever the caller typed (e.g. `./ccx.py`), which can
    be cwd-relative. Claude runs hooks from the session cwd, so resolve the
    executable wrapper up front and pair it with the same Python that's running
    us. shlex.quote covers paths with spaces.
    """
    sock = sock_path(short)
    py = shlex.quote(sys.executable)
    script = shlex.quote(current_executable_path())
    relay_cmd = f"{py} {script} hooks-relay {short}"
    hooks = {}
    for evt in HOOKS_WE_CARE_ABOUT:
        hook = {"type": "command", "command": f"{relay_cmd} {evt}"}
        if evt in BROKERED_HOOKS:
            hook["timeout"] = int(PERMISSION_HOOK_TIMEOUT_S)
        hooks[evt] = [{"hooks": [hook]}]
    # Managed Claude workers use the classic terminal renderer: the user's
    # native profile may select fullscreen, whose input surface does not
    # acknowledge CCX's semantic PTY paste. This --settings sidecar overrides
    # only the worker, never the user's own Claude settings.json.
    cfg = {"hooks": hooks, "tui": "default"}
    # Native Claude 2.1.293 otherwise stops at a one-time interactive
    # "Bypass Permissions mode" consent dialog before SessionStart, even
    # though CCX deliberately launched its own managed worker with
    # --dangerously-skip-permissions. The caller's persisted createArgs
    # is authoritative for this worker, including on host respawn.
    #
    # A per-session --settings acknowledgement suppresses only that
    # *warning*. It does not set a default permission mode or override
    # Claude's managed disableBypassPermissionsMode policy, which remains
    # enforced by native Claude. Missing/malformed/legacy evidence is not
    # an invitation to opt a restricted worker into bypass mode.
    from ccx.store.records import read_record
    rec = read_record(short) or {}
    create_args = rec.get("createArgs") if isinstance(rec, dict) else None
    if isinstance(create_args, dict) and create_args.get("skip_permissions") is True:
        cfg["skipDangerousModePermissionPrompt"] = True
    if os.environ.get("CCX_STATUSLINE_BRIDGE", "1").lower() not in ("0", "false", "no", "off"):
        cfg["statusLine"] = {
            "type": "command",
            "command": f"{py} {script} statusline-relay {short}",
        }
    # Owner-private full-UUID settings are CCX sidecars, never provider-owned
    # ~/.claude data. Publish fd-relative, nofollow and fsynced.
    from ccx import constants
    from ccx.store.root import require_ccx_active, _write_json
    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(short)
    if owner.engine != "claude":
        raise ValueError("Claude hook settings require Claude full UUID")
    with store.lease():
        with store._gen("workers", "claude", owner.worker_id) as fd:
            _write_json(fd, "settings.json", cfg)
    return settings_path(owner.worker)
