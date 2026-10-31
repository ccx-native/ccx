from __future__ import annotations
import ccx.cli.commands.control as commands_control

import json
import pytest

from ccx.claude import settings as S
from ccx.store.root import require_ccx_active
from test_generation_store import A, C, add_worker
from test_python_worker_api import active_api_root


def test_write_settings_emits_statusline_bridge_by_default(
    active_api_root, monkeypatch, tmp_path,
):
    store = require_ccx_active(active_api_root)
    monkeypatch.setattr(S, "sock_path", lambda worker: tmp_path / "unused.sock")
    monkeypatch.setattr(S, "current_executable_path", lambda: "/opt/ccx")

    out = S.write_settings(A)

    assert out == S.settings_path(A)
    doc = store.read_worker_sidecar(A, "settings.json")
    assert json.loads(out.read_text()) == doc
    assert "hooks" in doc
    assert doc["tui"] == "default"
    assert doc["statusLine"]["type"] == "command"
    assert f"statusline-relay {A}" in doc["statusLine"]["command"]


def test_write_settings_statusline_bridge_opt_out(
    active_api_root, monkeypatch, tmp_path,
):
    store = require_ccx_active(active_api_root)
    monkeypatch.setattr(S, "sock_path", lambda worker: tmp_path / "unused.sock")
    monkeypatch.setattr(S, "current_executable_path", lambda: "/opt/ccx")
    monkeypatch.setenv("CCX_STATUSLINE_BRIDGE", "0")

    out = S.write_settings(A)

    assert out == S.settings_path(A)
    doc = store.read_worker_sidecar(A, "settings.json")
    assert json.loads(out.read_text()) == doc
    assert "hooks" in doc
    assert "statusLine" not in doc


def test_ensure_trusted_paths_trusts_cwd_and_add_dirs(monkeypatch, tmp_path):
    cfg = tmp_path / ".claude.json"
    cfg.write_text(json.dumps({
        "numStartups": 7,
        "projects": {"/some/old": {"hasTrustDialogAccepted": True, "history": [1]}},
    }))
    monkeypatch.setattr(S, "CLAUDE_DOTFILE", cfg)

    cwd = str(tmp_path / "primary")
    add = str(tmp_path / "extra")
    changed = S.ensure_trusted_paths([cwd, add])
    assert changed is True

    doc = json.loads(cfg.read_text())
    projects = doc["projects"]
    assert projects[cwd]["hasTrustDialogAccepted"] is True
    assert projects[add]["hasTrustDialogAccepted"] is True
    # Unrelated existing config keys preserved
    assert doc["numStartups"] == 7
    assert projects["/some/old"] == {"hasTrustDialogAccepted": True, "history": [1]}


def test_ensure_trusted_paths_normalizes_to_absolute(monkeypatch, tmp_path):
    cfg = tmp_path / ".claude.json"
    monkeypatch.setattr(S, "CLAUDE_DOTFILE", cfg)
    monkeypatch.chdir(tmp_path)

    assert S.ensure_trusted_paths(["./rel/sub"]) is True
    doc = json.loads(cfg.read_text())
    expected = str((tmp_path / "rel" / "sub").resolve())
    assert doc["projects"][expected]["hasTrustDialogAccepted"] is True


def test_ensure_trusted_paths_empty_and_none_noop(monkeypatch, tmp_path):
    cfg = tmp_path / ".claude.json"
    monkeypatch.setattr(S, "CLAUDE_DOTFILE", cfg)

    assert S.ensure_trusted_paths([]) is False
    assert S.ensure_trusted_paths(None) is False
    assert S.ensure_trusted_paths(["", None]) is False
    # No config written for a pure no-op
    assert not cfg.exists()


def test_ensure_trusted_paths_does_not_infer_trust_from_ancestor(monkeypatch, tmp_path):
    # A trusted PARENT must not stop us writing the child's own entry. Claude
    # reads the exact-path entry; inheriting here is how a cwd ends up spawning
    # into the trust dialog with no hook to wait for.
    cfg = tmp_path / ".claude.json"
    parent = str(tmp_path / "ws")
    child = str(tmp_path / "ws" / "child")
    cfg.write_text(json.dumps({"projects": {parent: {"hasTrustDialogAccepted": True}}}))
    monkeypatch.setattr(S, "CLAUDE_DOTFILE", cfg)

    assert S.ensure_trusted_paths([child]) is True
    doc = json.loads(cfg.read_text())
    assert doc["projects"][child]["hasTrustDialogAccepted"] is True


def test_ensure_trusted_paths_overrides_explicit_false(monkeypatch, tmp_path):
    # The 2.1.232 boot-stall shape: trusted parent, child pinned to false.
    # ccx used to see the parent, call it covered, and write nothing — then
    # every spawn in that cwd died on "no SessionStart within 90s".
    cfg = tmp_path / ".claude.json"
    child = str(tmp_path / "ws" / "child")
    cfg.write_text(json.dumps({"projects": {
        str(tmp_path / "ws"): {"hasTrustDialogAccepted": True},
        child: {"hasTrustDialogAccepted": False, "allowedTools": []},
    }}))
    monkeypatch.setattr(S, "CLAUDE_DOTFILE", cfg)

    assert S.ensure_trusted_paths([child]) is True
    entry = json.loads(cfg.read_text())["projects"][child]
    assert entry["hasTrustDialogAccepted"] is True
    # Sibling keys in the entry survive the flip.
    assert entry["allowedTools"] == []


def test_ensure_trusted_paths_entry_without_the_key(monkeypatch, tmp_path):
    cfg = tmp_path / ".claude.json"
    p = str(tmp_path / "ws")
    cfg.write_text(json.dumps({"projects": {p: {"allowedTools": []}}}))
    monkeypatch.setattr(S, "CLAUDE_DOTFILE", cfg)

    assert S.ensure_trusted_paths([p]) is True
    assert json.loads(cfg.read_text())["projects"][p]["hasTrustDialogAccepted"] is True


def test_ensure_trusted_single_path_still_works(monkeypatch, tmp_path):
    cfg = tmp_path / ".claude.json"
    monkeypatch.setattr(S, "CLAUDE_DOTFILE", cfg)

    cwd = str(tmp_path / "solo")
    assert S.ensure_trusted(cwd) is True
    doc = json.loads(cfg.read_text())
    assert doc["projects"][cwd]["hasTrustDialogAccepted"] is True
    # Idempotent second call
    assert S.ensure_trusted(cwd) is False


# Native 2.1.293 reads folder trust from CLAUDE_CONFIG_DIR/.claude.json when
# overridden, rather than HOME/.claude.json. Test isolated imports because
# CCX constants intentionally capture profile paths once at import.


def _trust_subprocess(tmp_path, *, override: bool, expect_updated: bool = True):
    import os
    import subprocess
    import sys
    from pathlib import Path

    repo = Path(__file__).resolve().parents[1]
    home = tmp_path / "scratch-home"
    profile = tmp_path / "native-profile"
    work = tmp_path / "workspace" / "child"
    added = tmp_path / "added-directory"
    home.mkdir(parents=True, exist_ok=True)
    work.mkdir(parents=True, exist_ok=True)
    added.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    for key in list(env):
        if key.startswith("CCX_") or key in {
            "ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "OPENAI_API_KEY",
            "CODEX_API_KEY",
        }:
            env.pop(key, None)
    env.update({
        "HOME": str(home),
        "CCX_ROOT": str(tmp_path / "scratch-ccx"),
        "PYTHONPATH": str(repo),
        "PYTHONNOUSERSITE": "1",
        "XDG_CACHE_HOME": str(tmp_path / "cache"),
        "XDG_CONFIG_HOME": str(tmp_path / "config"),
        "XDG_DATA_HOME": str(tmp_path / "data"),
        "XDG_STATE_HOME": str(tmp_path / "state"),
        "TMPDIR": str(tmp_path),
    })
    if override:
        env["CLAUDE_CONFIG_DIR"] = str(profile)
    else:
        env.pop("CLAUDE_CONFIG_DIR", None)
    target = profile / ".claude.json" if override else home / ".claude.json"
    other = home / ".claude.json" if override else profile / ".claude.json"
    child = r"""
import os
import sys
from pathlib import Path
import ccx
from ccx import constants; import ccx.claude.settings as settings

repo, expected, expected_home, expected_native, cwd, add, updated = sys.argv[1:]
assert Path(ccx.__file__).resolve() == Path(repo, "ccx/__init__.py").resolve()
assert Path(settings.__file__).resolve() == Path(repo, "ccx/claude/settings.py").resolve()
assert constants.CLAUDE_DOTFILE.resolve() == Path(expected).resolve()
assert constants.CLAUDE_HOME.resolve() == Path(expected_native).resolve()
assert Path(os.environ["HOME"]).resolve() == Path(expected_home).resolve()
assert Path(os.environ["CCX_ROOT"]).is_relative_to(Path(expected_home).parent)
first = settings.ensure_trusted_paths([cwd, add])
assert first is (updated == "true")
assert settings.ensure_trusted_paths([cwd, add]) is False
print("trust-probe-ok")
"""
    proc = subprocess.run(
        [sys.executable, "-c", child, str(repo), str(target), str(home),
         str(profile if override else home / ".claude"), str(work),
         str(added), "true" if expect_updated else "false"],
        cwd=repo, env=env, capture_output=True, text=True, timeout=15,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "trust-probe-ok"
    return home, profile, work, added, target, other


@pytest.mark.parametrize("override", [False, True])
def test_native_global_trust_path_preserves_default_and_explicit_override(
    tmp_path, override
):
    home = tmp_path / "scratch-home"
    profile = tmp_path / "native-profile"
    home.mkdir()
    profile.mkdir()
    work = str(tmp_path / "workspace" / "child")
    old = {"/unrelated/project": {"history": [1], "hasTrustDialogAccepted": True}}
    selected = profile / ".claude.json" if override else home / ".claude.json"
    other = home / ".claude.json" if override else profile / ".claude.json"
    selected_before = {
        "numStartups": 7, "hasCompletedOnboarding": True,
        "projects": {**old, work: {"hasTrustDialogAccepted": False,
                                  "allowedTools": ["Read"]}},
    }
    # Previously HOME accepted this cwd while the native override still
    # explicitly rejected it; the wrong-root write must not repeat.
    other_before = {"metadata": {"other": True},
                    "projects": {work: {"hasTrustDialogAccepted": True}}}
    selected.write_text(json.dumps(selected_before))
    selected.chmod(0o644)
    other.write_text(json.dumps(other_before))
    other.chmod(0o600)
    _, _, cwd, add, target, ignored = _trust_subprocess(tmp_path, override=override)
    assert target == selected and ignored == other
    actual = json.loads(selected.read_text())
    assert actual["projects"][str(cwd)] == {
        "hasTrustDialogAccepted": True, "allowedTools": ["Read"],
    }
    assert actual["projects"][str(add)] == {"hasTrustDialogAccepted": True}
    assert actual["projects"]["/unrelated/project"] == old["/unrelated/project"]
    assert actual["numStartups"] == 7
    assert actual["hasCompletedOnboarding"] is True
    assert selected.stat().st_mode & 0o777 == 0o600
    assert json.loads(other.read_text()) == other_before
    assert other.stat().st_mode & 0o777 == 0o600


def test_override_creates_native_profile_metadata_without_touching_home(tmp_path):
    home = tmp_path / "scratch-home"
    home.mkdir()
    home_file = home / ".claude.json"
    home_file.write_text(json.dumps({"projects": {"old": {"custom": True}}}))
    before = home_file.read_bytes()
    _, profile, cwd, add, selected, _ = _trust_subprocess(tmp_path, override=True)
    assert selected.exists()
    assert profile.stat().st_mode & 0o777 == 0o700
    assert selected.stat().st_mode & 0o777 == 0o600
    projects = json.loads(selected.read_text())["projects"]
    assert projects[str(cwd)]["hasTrustDialogAccepted"]
    assert projects[str(add)]["hasTrustDialogAccepted"]
    assert home_file.read_bytes() == before


def test_override_malformed_native_config_fails_open_and_preserves_home(tmp_path):
    home = tmp_path / "scratch-home"
    profile = tmp_path / "native-profile"
    home.mkdir()
    profile.mkdir()
    selected = profile / ".claude.json"
    selected.write_text("{malformed existing profile")
    selected.chmod(0o600)
    other = home / ".claude.json"
    other.write_text('{"unchanged":true}')
    _trust_subprocess(tmp_path, override=True, expect_updated=False)
    assert selected.read_text() == "{malformed existing profile"
    assert other.read_text() == '{"unchanged":true}'



@pytest.mark.parametrize(
    ("skip_permissions", "expected_ack"),
    [(True, True), (False, False), (None, False), ("true", False)],
)
def test_session_settings_bypass_warning_ack_only_for_explicit_full(
    active_api_root, tmp_path, monkeypatch, skip_permissions, expected_ack
):
    """Only a documented full-UUID owner opt-in acknowledges the warning."""
    store = require_ccx_active(active_api_root)
    if skip_permissions is not None:
        store.mutate_record(A, patches={"createArgs.skip_permissions": skip_permissions})
    cfg_path = S.settings_path(A)
    native_profile = tmp_path / "provider"
    native_profile.mkdir()
    user_settings = native_profile / "settings.json"
    managed_policy = {
        "tui": "fullscreen",
        "permissions": {
            "disableBypassPermissionsMode": "disable",
            "defaultMode": "plan",
        },
        "unrelated": {"keep": True},
    }
    user_settings.write_text(json.dumps(managed_policy))
    before = user_settings.read_bytes()
    legacy_home = tmp_path / "home" / ".claude.json"
    legacy_home.parent.mkdir()
    legacy_home.write_text('{"noGlobalPermissionMutation":true}')
    monkeypatch.setattr(S, "CLAUDE_DOTFILE", legacy_home)
    monkeypatch.setattr(S, "sock_path", lambda worker: tmp_path / "unused.sock")
    monkeypatch.setattr(S, "current_executable_path", lambda: "/worktree/ccx.py")
    output = S.write_settings(A)
    assert output == cfg_path
    doc = json.loads(cfg_path.read_text())
    assert store.read_worker_sidecar(A, "settings.json") == doc
    assert doc["tui"] == "default"
    assert doc.get("skipDangerousModePermissionPrompt") is (
        True if expected_ack else None
    )
    assert "SessionStart" in doc["hooks"]
    assert "PermissionRequest" in doc["hooks"]
    assert doc["statusLine"]["type"] == "command"
    # This field skips the one-time warning; it NEVER overrides enforced
    # native admin policy, permission mode or the provider's global config.
    assert "permissions" not in doc
    assert "disableBypassPermissionsMode" not in doc
    assert "defaultMode" not in doc
    assert user_settings.read_bytes() == before
    assert legacy_home.read_text() == '{"noGlobalPermissionMutation":true}'
    assert json.loads(cfg_path.read_text()) == doc
    S.write_settings(A)  # regenerate as on a revived original host
    assert json.loads(cfg_path.read_text()) == doc
    assert doc["tui"] == "default"


def test_session_settings_ack_is_worker_record_scoped_even_with_shared_native_profile(
    active_api_root, tmp_path, monkeypatch
):
    add_worker(active_api_root, C, "claude")
    store = require_ccx_active(active_api_root)
    store.mutate_record(A, patches={"createArgs.skip_permissions": True})
    store.mutate_record(C, patches={"createArgs.skip_permissions": False})
    monkeypatch.setattr(S, "sock_path", lambda worker: tmp_path / "unused.sock")
    monkeypatch.setattr(S, "current_executable_path", lambda: "/worktree/ccx.py")
    # Two original workers share a provider profile but not a settings sidecar.
    full_path = S.write_settings(A)
    restricted_path = S.write_settings(C)
    full = json.loads(full_path.read_text())
    restricted = json.loads(restricted_path.read_text())
    assert full["tui"] == restricted["tui"] == "default"
    assert full["skipDangerousModePermissionPrompt"] is True
    assert "skipDangerousModePermissionPrompt" not in restricted
    assert store.read_worker_sidecar(A, "settings.json") == full
    assert store.read_worker_sidecar(C, "settings.json") == restricted
    assert f"statusline-relay {A}" in full["statusLine"]["command"]
    assert f"statusline-relay {C}" in restricted["statusLine"]["command"]


# -- trust ledger and untrust -------------------------------------------------

@pytest.fixture
def trust_env(monkeypatch, tmp_path):
    cfg = tmp_path / ".claude.json"
    ledger = tmp_path / "ccx" / "trust-ledger.json"
    monkeypatch.setattr(S, "CLAUDE_DOTFILE", cfg)
    monkeypatch.setattr(S, "trust_ledger_path", lambda: ledger)
    return cfg, ledger


def _projects(cfg):
    return json.loads(cfg.read_text())["projects"]


def test_trust_records_only_what_ccx_changed(trust_env, tmp_path):
    cfg, ledger = trust_env
    mine, keyless, pinned, already = (str(tmp_path / n) for n in ("new", "keyless", "pinned", "already"))
    cfg.write_text(json.dumps({"projects": {
        keyless: {"allowedTools": []},
        pinned: {"hasTrustDialogAccepted": False},
        already: {"hasTrustDialogAccepted": True},
    }}))
    assert S.ensure_trusted_paths([mine, keyless, pinned, already]) is True
    recorded = S.trusted_by_ccx()
    assert {p: r["change"] for p, r in recorded.items()} == {
        mine: S.TRUST_CREATED_ENTRY,
        keyless: S.TRUST_ADDED_KEY,
        pinned: S.TRUST_REPLACED_VALUE,
    }
    assert recorded[pinned]["previous"] is False
    assert oct(ledger.stat().st_mode & 0o777) == "0o600"
    assert oct(ledger.with_name("trust-ledger.lock").stat().st_mode & 0o777) == "0o600"


def test_untrust_removes_only_ccx_entries_and_keeps_later_keys(trust_env, tmp_path):
    cfg, _ = trust_env
    mine, keyless, pinned, other = (str(tmp_path / n) for n in ("new", "keyless", "pinned", "other"))
    cfg.write_text(json.dumps({"numStartups": 3, "projects": {
        keyless: {"allowedTools": []},
        pinned: {"hasTrustDialogAccepted": False},
        other: {"hasTrustDialogAccepted": True},
    }}))
    S.ensure_trusted_paths([mine, keyless, pinned])
    # Claude later adds its own data to an entry ccx created.
    doc = json.loads(cfg.read_text())
    doc["projects"][mine]["lastCost"] = 1
    cfg.write_text(json.dumps(doc))

    rows = S.untrust_paths()
    assert {r["path"]: r["action"] for r in rows} == {
        mine: "removed-key", keyless: "removed-key", pinned: "restored",
    }
    projects = _projects(cfg)
    assert projects[mine] == {"lastCost": 1}
    assert projects[keyless] == {"allowedTools": []}
    assert projects[pinned] == {"hasTrustDialogAccepted": False}
    assert projects[other] == {"hasTrustDialogAccepted": True}  # not ccx's
    assert json.loads(cfg.read_text())["numStartups"] == 3
    assert S.trusted_by_ccx() == {}


def test_untrust_removes_an_entry_ccx_created_untouched(trust_env, tmp_path):
    cfg, _ = trust_env
    mine = str(tmp_path / "new")
    S.ensure_trusted_paths([mine])
    assert S.untrust_paths([mine]) == [{"path": mine, "action": "removed-entry"}]
    assert mine not in _projects(cfg)


def test_untrust_canonical_alias_only_removes_exact_trusted_path(trust_env, tmp_path):
    cfg, _ = trust_env
    real = tmp_path / "real"
    real.mkdir()
    original = real / "project"
    original.mkdir()
    unrelated = real / "unrelated"
    unrelated.mkdir()
    symlink = tmp_path / "alias"
    symlink.symlink_to(real, target_is_directory=True)
    assert S.ensure_trusted_paths([str(original.resolve()), str(unrelated.resolve())])

    # As with create(cwd=...), canonicalize the existing prefix even if the
    # final component no longer exists. Do not enumerate or undo siblings.
    original.rmdir()
    assert S.untrust_paths([str(symlink / "project")], dry_run=True) == [
        {"path": str(original), "action": "removed-entry"},
    ]
    assert S.untrust_paths([str(symlink / "project")]) == [
        {"path": str(original), "action": "removed-entry"},
    ]
    assert _projects(cfg)[str(unrelated)]["hasTrustDialogAccepted"] is True
    assert str(original) not in _projects(cfg)
    assert set(S.trusted_by_ccx()) == {str(unrelated)}


def test_untrust_dry_run_changes_nothing(trust_env, tmp_path):
    cfg, _ = trust_env
    mine = str(tmp_path / "new")
    S.ensure_trusted_paths([mine])
    before = cfg.read_text()
    assert S.untrust_paths(dry_run=True) == [{"path": mine, "action": "removed-entry"}]
    assert cfg.read_text() == before
    assert mine in S.trusted_by_ccx()


def test_untrust_leaves_an_entry_changed_since_and_reports_unknown_paths(trust_env, tmp_path):
    cfg, _ = trust_env
    mine = str(tmp_path / "new")
    S.ensure_trusted_paths([mine])
    doc = json.loads(cfg.read_text())
    doc["projects"][mine]["hasTrustDialogAccepted"] = False  # the user said no
    cfg.write_text(json.dumps(doc))
    rows = S.untrust_paths([mine, str(tmp_path / "never")])
    assert rows == [
        {"path": mine, "action": "changed-since"},
        {"path": str(tmp_path / "never"), "action": "not-recorded"},
    ]
    assert _projects(cfg)[mine] == {"hasTrustDialogAccepted": False}
    assert S.trusted_by_ccx() == {}


def test_trust_write_retries_when_the_config_changes_underneath(trust_env, tmp_path, monkeypatch):
    cfg, _ = trust_env
    cfg.write_text(json.dumps({"projects": {}}))
    real_read = S._read_config_bytes
    calls = {"n": 0}

    def racing_read():
        calls["n"] += 1
        if calls["n"] == 2:  # Claude writes between our read and our replace
            cfg.write_text(json.dumps({"projects": {}, "oauthAccount": {"id": 1}}))
        return real_read()

    monkeypatch.setattr(S, "_read_config_bytes", racing_read)
    assert S.ensure_trusted_paths([str(tmp_path / "w")]) is True
    doc = json.loads(cfg.read_text())
    assert doc["oauthAccount"] == {"id": 1}  # the concurrent write survived
    assert doc["projects"][str(tmp_path / "w")]["hasTrustDialogAccepted"] is True


def test_trust_write_gives_up_on_a_config_that_never_settles(trust_env, tmp_path, monkeypatch):
    cfg, _ = trust_env
    cfg.write_text(json.dumps({"projects": {}}))
    counter = iter(range(1000))
    monkeypatch.setattr(S, "_read_config_bytes", lambda: json.dumps({"n": next(counter)}).encode())
    assert S.ensure_trusted_paths([str(tmp_path / "w")]) is False
    assert S.trusted_by_ccx() == {}


def test_untrust_command_reports_rows(trust_env, tmp_path, capsys):
    from types import SimpleNamespace

    from ccx.ops import maintenance as maintenance

    cfg, _ = trust_env
    mine = str(tmp_path / "new")
    S.ensure_trusted_paths([mine])
    commands_control.cmd_untrust(SimpleNamespace(paths=[], dry_run=True, json=False))
    assert capsys.readouterr().out == f"{mine}: removed-entry (dry run)\n"
    commands_control.cmd_untrust(SimpleNamespace(paths=[mine], dry_run=False, json=True))
    doc = json.loads(capsys.readouterr().out)
    assert doc["paths"] == [{"path": mine, "action": "removed-entry"}]
    assert doc["dry_run"] is False


def test_untrust_with_nothing_recorded_writes_nothing(trust_env):
    _, ledger = trust_env
    assert S.untrust_paths() == []
    assert not ledger.exists()


@pytest.mark.parametrize("native_nfc", [True, False], ids=["macOS-NFC", "Linux-NFD"])
def test_native_claude_trust_key_matches_targeted_untrust(
    trust_env, tmp_path, monkeypatch, native_nfc,
):
    """Use the native project trust spelling, not the record's disk spelling."""
    import unicodedata

    cfg, _ledger = trust_env
    monkeypatch.setattr(S.oslayer, "claude_project_cwd_is_nfc", lambda: native_nfc)
    nfd = str(tmp_path / "cafe\u0301")
    from pathlib import Path
    Path(nfd).mkdir()
    nfc = unicodedata.normalize("NFC", nfd)
    native_key = nfc if native_nfc else nfd
    other_key = nfd if native_nfc else nfc
    assert nfc != nfd

    # The same input is passed to creation and targeted untrust. On Mac the
    # other Unicode spelling of the same path is deduplicated to one trust key.
    paths = [nfd, nfc] if native_nfc else [nfd]
    assert S.ensure_trusted_paths(paths) is True
    assert set(_projects(cfg)) == {native_key}
    assert _projects(cfg)[native_key]["hasTrustDialogAccepted"] is True
    assert other_key not in _projects(cfg)
    assert set(S.trusted_by_ccx()) == {native_key}
    assert S.ensure_trusted(nfd) is False  # no redundant trust rewrite

    # Untrust must still find the *same* ledger key after the physical leaf
    # disappears, without deriving another name or touching an unrelated key.
    Path(nfd).rmdir()
    assert S._requested_trust_path(nfd) == native_key
    assert S.untrust_paths([nfd], dry_run=True) == [
        {"path": native_key, "action": "removed-entry"}]
    assert S.untrust_paths([nfd]) == [
        {"path": native_key, "action": "removed-entry"}]
    assert _projects(cfg) == {}
    assert S.trusted_by_ccx() == {}


def test_mac_untrust_does_not_migrate_historical_nfd_ledger_entry(
    trust_env, tmp_path, monkeypatch,
):
    """Old NFD trust ledger rows are not silently rewritten/adopted."""
    import unicodedata

    cfg, ledger = trust_env
    monkeypatch.setattr(S.oslayer, "claude_project_cwd_is_nfc", lambda: True)
    nfd = str(tmp_path / "cafe\u0301")
    nfc = unicodedata.normalize("NFC", nfd)
    cfg.write_text(json.dumps({"projects": {
        nfd: {"hasTrustDialogAccepted": True}}}))
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text(json.dumps({"schemaVersion": 1, "configs": {
        str(cfg): {nfd: {"change": S.TRUST_CREATED_ENTRY}}}}))
    assert S.untrust_paths([nfd]) == [
        {"path": nfc, "action": "not-recorded"}]
    assert _projects(cfg) == {nfd: {"hasTrustDialogAccepted": True}}
    assert set(S.trusted_by_ccx()) == {nfd}
