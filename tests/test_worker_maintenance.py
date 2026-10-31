"""Machine maintenance: exact UUID ownership, fake-only effects, private trust."""
from __future__ import annotations
import ccx.cli.commands.control as commands_control

import json
import os
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from ccx.cli import main as cli
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.store import liveness
from ccx.ops import maintenance as maintenance
from ccx.notify import registry as parent_notify
from ccx.claude import settings
from ccx.ops import terminate
from ccx.errors import SessionUnavailable
from ccx.store.liveness import proc_start_token
from ccx.notify.paths import daemon_generation_path, ensure_dirs
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, C, add_worker, short_root  # noqa: F401  (pytest-imported fixture)


def _root(monkeypatch, path):
    monkeypatch.setattr(constants, "CCX_ROOT", path)
    monkeypatch.setenv("CCX_ROOT", str(path))
    return require_ccx_active(path)


def _registry(monkeypatch, workers=None, *, presence="served", raw=None):
    def observed(*, timeout):
        assert timeout == 2.0
        return {"presence": presence, "workers": workers or {},
                "reply": {"ok": True, "workers": workers or {}} if raw is None else raw}
    monkeypatch.setattr(codex_runtime, "observe_registry", observed)


@pytest.mark.parametrize("case", ("current", "dead", "reused", "unprovable", "malformed"))
def test_notifyd_shutdown_uses_one_stamp_and_pid_start(short_root, monkeypatch, case):
    store = _root(monkeypatch, short_root)
    ensure_dirs()
    pid, start = os.getpid(), proc_start_token(os.getpid())
    assert start
    if case == "dead":
        pid = 900_000_001
    if case == "reused":
        start = "1"
    stamp = daemon_generation_path()
    stamp.write_text("bad stamp\n" if case == "malformed" else
                     f"{pid} {constants.NOTIFYD_GENERATION} {store.generation} {start}\n")
    if case == "unprovable":
        from ccx.system import oslayer
        monkeypatch.setattr(oslayer, "process_info", lambda pid:
                            oslayer.ProcessInfo(pid, "unprovable", None, None, None))
    monkeypatch.setattr(maintenance.os, "kill", lambda *_: pytest.fail(
        "an unproved original may not be signalled"))
    if case == "unprovable":
        with pytest.raises(maintenance.CcxError, match="unprovable"):
            maintenance._bound_notifyd_pid(store)
    elif case == "current":
        bound = maintenance._bound_notifyd_pid(store)
        assert bound == (pid, start)
        stamp.write_text("different original start\n")
        assert maintenance._stop_notifyd(bound) == "not-running"
    else:
        assert maintenance._bound_notifyd_pid(store) is None


def test_scoped_claude_shutdown_only_owns_claude_and_never_touches_notifyd(
    short_root, monkeypatch, capsys,
):
    store = _root(monkeypatch, short_root)
    add_worker(short_root, A, "claude")
    add_worker(short_root, B, "codex")
    start = proc_start_token(os.getpid())
    assert start
    store.patch_lifecycle(A, changes={"state": "idle", "hostPid": os.getpid(),
                                      "hostStart": start})

    def forbidden(*_args, **_kwargs):
        pytest.fail("scoped Claude shutdown touched Codex or notifyd")

    monkeypatch.setattr(maintenance, "_loaded_codex_workers", forbidden)
    monkeypatch.setattr(maintenance, "_stop_codexd", forbidden)
    monkeypatch.setattr(maintenance, "_bound_notifyd_pid", forbidden)
    monkeypatch.setattr(maintenance, "_stop_notifyd", forbidden)
    before = {wid: store.record(wid) for wid in (A, B)}
    calls = []

    def fake_kill(bound_store, worker, *, timeout):
        assert bound_store.generation == store.generation
        calls.append((worker, timeout))
        return {"changed": True, "state": "dead"}

    monkeypatch.setattr(terminate, "confirmed_kill_uuid", fake_kill)
    for dry_run in (True, False):
        cli.main(["shutdown", "--engine", "claude", "--timeout", "4", "--json"]
                 + (["--dry-run"] if dry_run else []))
        result = json.loads(capsys.readouterr().out)
        assert result["ok"] and result["dry_run"] is dry_run
        assert result["codex"] == [] and result["codexd"] == "untouched"
        assert result["notifyd"] == "untouched" and result["errors"] == []
        assert [(row["worker"], row["action"]) for row in result["claude"]] == [
            (f"claude:{A}", "would-kill" if dry_run else "killed")]
        assert calls == ([] if dry_run else [(f"claude:{A}", 4.0)])
    assert {wid: store.record(wid) for wid in (A, B)} == before


def test_scoped_codex_shutdown_only_unloads_owned_codex_and_nonforce_daemon(
    short_root, monkeypatch, capsys,
):
    store = _root(monkeypatch, short_root)
    add_worker(short_root, A, "claude")
    add_worker(short_root, B, "codex")
    _registry(monkeypatch, {B: "busy"})

    def forbidden(*_args, **_kwargs):
        pytest.fail("scoped Codex shutdown touched Claude, notifyd or daemon directly")

    monkeypatch.setattr(maintenance, "_live_claude_workers", forbidden)
    monkeypatch.setattr(maintenance, "_bound_notifyd_pid", forbidden)
    monkeypatch.setattr(maintenance, "_stop_notifyd", forbidden)
    monkeypatch.setattr(codex_runtime, "daemon_stop", forbidden)
    calls = []

    def fake_kill(bound_store, worker, *, timeout):
        assert bound_store.generation == store.generation
        calls.append(("kill", worker, timeout))
        return {"changed": True, "state": "dead"}

    def fake_daemon_stop(deadline):
        calls.append(("codexd", deadline is not None))
        return "stopped"

    monkeypatch.setattr(terminate, "confirmed_kill_uuid", fake_kill)
    monkeypatch.setattr(maintenance, "_stop_codexd", fake_daemon_stop)
    before = {wid: store.record(wid) for wid in (A, B)}
    for dry_run in (True, False):
        cli.main(["shutdown", "--engine", "codex", "--timeout", "8", "--json"]
                 + (["--dry-run"] if dry_run else []))
        result = json.loads(capsys.readouterr().out)
        assert result["ok"] and result["dry_run"] is dry_run
        assert result["claude"] == [] and result["notifyd"] == "untouched"
        assert result["codexd"] == ("would-stop" if dry_run else "stopped")
        assert [(row["worker"], row["action"]) for row in result["codex"]] == [
            (f"codex:{B}", "would-kill" if dry_run else "killed")]
        assert calls == ([] if dry_run else [("kill", f"codex:{B}", 8.0),
                                                ("codexd", True)])

    # A confirmed already-down worker is not called "killed". An unloaded
    # registry still permits the non-force daemon stop without worker kills.
    monkeypatch.setattr(terminate, "confirmed_kill_uuid",
                        lambda *_args, **_kwargs: {"changed": False, "state": "dead"})
    calls.clear()
    already_down = maintenance.shutdown(engine="codex")
    assert already_down["ok"] and already_down["codex"][0]["action"] == "already-down"
    assert already_down["codex"][0]["state"] == "dead"
    assert calls == [("codexd", True)]

    _registry(monkeypatch, {})
    calls.clear()
    unloaded = maintenance.shutdown(engine="codex")
    assert unloaded["ok"] and unloaded["codex"] == [] and unloaded["codexd"] == "stopped"
    assert calls == [("codexd", True)]
    assert {wid: store.record(wid) for wid in (A, B)} == before


def test_scoped_codex_refuses_unowned_or_inconclusive_and_handles_absent(
    short_root, monkeypatch,
):
    _root(monkeypatch, short_root)
    add_worker(short_root, B, "codex")

    def forbidden(*_args, **_kwargs):
        pytest.fail("unavailable/unsafe Codex inventory attempted a kill or stop")

    monkeypatch.setattr(maintenance, "_live_claude_workers", forbidden)
    monkeypatch.setattr(maintenance, "_bound_notifyd_pid", forbidden)
    monkeypatch.setattr(maintenance, "_stop_notifyd", forbidden)
    monkeypatch.setattr(maintenance, "_stop_codexd", forbidden)
    monkeypatch.setattr(terminate, "confirmed_kill_uuid", forbidden)
    for presence, workers, raw, expected in (
        ("absent", {}, None, "not-running"),
        ("inconclusive", {}, {"error": "registry unreadable"}, "failed"),
        ("served", {C: "busy"}, None, "failed"),
        ("served", {}, {"ok": True, "workers": []}, "failed"),
    ):
        _registry(monkeypatch, workers, presence=presence, raw=raw)
        result = maintenance.shutdown(engine="codex")
        assert result["codexd"] == expected
        assert result["ok"] is (expected == "not-running")
        assert result["codex"] == [] and result["claude"] == []
        assert result["notifyd"] == "untouched"
        assert bool(result["errors"]) is (expected == "failed")


def test_scoped_codex_kill_timeout_and_daemon_refusal_are_failures(
    short_root, monkeypatch, capsys,
):
    _root(monkeypatch, short_root)
    add_worker(short_root, B, "codex")
    _registry(monkeypatch, {B: "busy"})
    calls = []

    def fake_kill(_store, worker, *, timeout):
        calls.append((worker, timeout))
        raise SessionUnavailable("unload confirmation timed out", short=B[:8])

    def refused(_deadline):
        raise maintenance.CcxError("codexd still holds workers; no force")

    monkeypatch.setattr(maintenance, "_live_claude_workers",
                        lambda _: pytest.fail("inspected Claude"))
    monkeypatch.setattr(maintenance, "_bound_notifyd_pid",
                        lambda _: pytest.fail("inspected notifyd"))
    monkeypatch.setattr(maintenance, "_stop_codexd", refused)
    monkeypatch.setattr(terminate, "confirmed_kill_uuid", fake_kill)
    with pytest.raises(SystemExit) as exc:
        cli.main(["shutdown", "--engine", "codex", "--timeout", "0", "--json"])
    result = json.loads(capsys.readouterr().out)
    assert exc.value.code == 3 and not result["ok"]
    assert calls == [(f"codex:{B}", 0.0)]
    assert result["codex"][0]["action"] == "failed"
    assert result["codexd"] == "failed" and result["notifyd"] == "untouched"
    assert len(result["errors"]) == 2
    assert "timed out" in result["errors"][0]["error"]


def test_scoped_claude_unbound_pid_cannot_be_signalled(short_root, monkeypatch):
    store = _root(monkeypatch, short_root)
    add_worker(short_root, A, "claude")
    fake_pid = os.getpid()  # Live PID with no recorded start: identity unknown.
    store.patch_lifecycle(A, changes={"state": "idle", "hostPid": fake_pid,
                                      "hostStart": None})
    # R3 distinguishes this unprovable live PID from a kernel-missing PID,
    # which is definitively dead even without a recorded token.
    monkeypatch.setattr(terminate, "signal_pid",
                        lambda *_args: pytest.fail("signalled unowned PID"))
    monkeypatch.setattr(maintenance, "_loaded_codex_workers",
                        lambda _: pytest.fail("inspected Codex"))
    monkeypatch.setattr(maintenance, "_bound_notifyd_pid",
                        lambda _: pytest.fail("inspected notifyd"))
    result = maintenance.shutdown(engine="claude", timeout=0)
    assert not result["ok"] and result["claude"][0]["action"] == "failed"
    assert "process could not be verified" in result["errors"][0]["error"]
    assert result["codex"] == [] and result["codexd"] == "untouched"
    assert result["notifyd"] == "untouched"


def test_uuid_maintenance_dry_run_exact_collision_read_only_and_inconclusive(
    short_root, monkeypatch, capsys,
):
    store = _root(monkeypatch, short_root)
    for wid, engine in ((A, "claude"), (B, "codex"), (C, "claude")):
        add_worker(short_root, wid, engine)
    # Same display short for A/B; C is a historical, wholly down worker.
    start = proc_start_token(os.getpid())
    assert start
    store.patch_lifecycle(A, changes={"state": "idle", "hostPid": os.getpid(),
                                      "hostStart": start})
    store.patch_lifecycle(C, changes={"state": "dead", "hostPid": 99999999,
                                      "hostStart": "stale"})
    _registry(monkeypatch, {B: "idle"})
    monkeypatch.setattr(terminate, "confirmed_kill_uuid",
                        lambda *a, **kw: pytest.fail("dry-run killed a worker"))
    monkeypatch.setattr(codex_runtime, "daemon_stop",
                        lambda **kw: pytest.fail("dry-run contacted daemon-stop"))
    before = {wid: store.record(wid) for wid in (A, B, C)}
    assert not (short_root / "sessions").exists()
    cli.main(["shutdown", "--dry-run", "--json"])
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] is True and result["dry_run"] is True
    assert set(result) == {"dry_run", "claude", "codex", "codexd",
                           "notifyd", "errors", "ok"}
    assert [(r["worker"], r["action"]) for r in result["claude"]] == [
        (f"claude:{A}", "would-kill")]
    assert [(r["worker"], r["action"]) for r in result["codex"]] == [
        (f"codex:{B}", "would-kill")]
    assert result["claude"][0]["short"] == result["codex"][0]["short"] == "aaaaaaaa"
    assert result["codexd"] == "would-stop" and result["notifyd"] == "not-running"
    assert all(r["worker_id"] in (A, B) for rows in ("claude", "codex")
               for r in result[rows])
    cli.main(["shutdown", "--dry-run"])
    plain = capsys.readouterr().out
    assert f"claude:{A}" in plain and f"codex:{B}" in plain
    assert f"claude:{C}" not in plain and "codexd: would-stop" in plain
    assert {wid: store.record(wid) for wid in (A, B, C)} == before
    assert not (short_root / "sessions").exists()

    _registry(monkeypatch, presence="inconclusive", raw={"error": "unreadable"})
    with pytest.raises(SystemExit) as exc:
        cli.main(["shutdown", "--dry-run", "--json"])
    failed = json.loads(capsys.readouterr().out)
    assert exc.value.code == 3 and failed["ok"] is False
    assert failed["codexd"] == "failed" and failed["codex"] == []
    assert failed["claude"][0]["action"] == "would-kill"
    assert "unreadable" in failed["errors"][0]["error"]
    _registry(monkeypatch, raw={"ok": True, "workers": []})
    assert maintenance.shutdown(dry_run=True)["codexd"] == "failed"
    assert {wid: store.record(wid) for wid in (A, B, C)} == before

    # A live PID with missing original start proof must remain listed.
    # Dry-run cannot signal it; real confirmed_kill_uuid owns the refusal.
    store.patch_lifecycle(C, changes={"hostPid": os.getpid(), "hostStart": None})
    exposed = maintenance.shutdown(dry_run=True)
    assert f"claude:{C}" in {row["worker"] for row in exposed["claude"]}
    before = {wid: store.record(wid) for wid in (A, B, C)}

    # Whole owner-index failure is global: no fake worker/daemon effects.
    monkeypatch.setattr(constants, "CCX_ROOT", short_root / "missing-generation")
    invalid = maintenance.shutdown(dry_run=False)
    assert invalid["ok"] is False and invalid["codexd"] == "failed"
    assert invalid["claude"] == invalid["codex"] == []
    assert {wid: store.record(wid) for wid in (A, B, C)} == before


def test_uuid_maintenance_real_dispatch_order_per_kill_error_and_exit(
    short_root, monkeypatch, capsys,
):
    store = _root(monkeypatch, short_root)
    for wid, engine in ((A, "claude"), (B, "codex"), (C, "claude")):
        add_worker(short_root, wid, engine)
    start = proc_start_token(os.getpid())
    assert start
    for wid in (A, C):
        store.patch_lifecycle(wid, changes={"hostPid": os.getpid(), "hostStart": start})
    _registry(monkeypatch, {B: "busy"})
    calls = []

    def fake_kill(bound_store, handle, *, timeout):
        assert bound_store.generation == store.generation
        calls.append(("kill", handle, timeout))
        if handle == f"claude:{A}":
            raise SessionUnavailable("original host exit not confirmed", short=A)
        return {"changed": handle != f"codex:{B}", "state": "stopped",
                "worker": handle, "history_retained": True}

    monkeypatch.setattr(terminate, "confirmed_kill_uuid", fake_kill)
    monkeypatch.setattr(maintenance, "_bound_notifyd_pid",
                        lambda _store: (12345, "test-only"))
    monkeypatch.setattr(maintenance, "_stop_codexd",
                        lambda deadline: calls.append(("codexd",)) or "stopped")
    monkeypatch.setattr(maintenance, "_stop_notifyd",
                        lambda bound: calls.append(("notifyd", bound)) or "stopped")
    with pytest.raises(SystemExit) as exc:
        commands_control.cmd_shutdown(SimpleNamespace(dry_run=False, timeout=7.0, json=True))
    result = json.loads(capsys.readouterr().out)
    assert exc.value.code == 3 and result["ok"] is False
    assert calls == [("kill", f"claude:{A}", 7.0),
                     ("kill", f"claude:{C}", 7.0),
                     ("kill", f"codex:{B}", 7.0),
                     ("codexd",), ("notifyd", (12345, "test-only"))]
    assert [(r["worker"], r["action"]) for r in result["claude"]] == [
        (f"claude:{A}", "failed"), (f"claude:{C}", "killed")]
    assert result["codex"][0]["action"] == "already-down"
    assert result["codexd"] == result["notifyd"] == "stopped"
    assert result["errors"] == [{"engine": "claude", "worker": f"claude:{A}",
                                 "short": A[:8], "error": "original host exit not confirmed"}]


def test_uuid_maintenance_registry_veto_notifyd_marker_and_private_untrust(
    short_root, tmp_path, monkeypatch, capsys,
):
    store = _root(monkeypatch, short_root)
    _registry(monkeypatch, {"cccccccc-1234-4000-8000-000000000003": "idle"})
    monkeypatch.setattr(maintenance, "_stop_codexd",
                        lambda _: pytest.fail("unknown worker unloaded by daemon-stop"))
    # notifyd stale/malformed/no-signal proof is covered by the parametrized
    # test above; keep this test focused on CLI private-profile untrust.
    # CLI untrust targets the selected private Claude profile, not the
    # unselected HOME file, and only recorded CCX changes.
    profile = tmp_path / "private-native-profile"
    profile.mkdir()
    selected = profile / ".claude.json"
    unrelated = tmp_path / "unselected-profile.json"
    ours = str(tmp_path / "our-workspace")
    untouched = str(tmp_path / "user-workspace")
    selected.write_text(json.dumps({"oauthAccount": {"id": "provider-owned"},
                                    "projects": {
                                        ours: {"hasTrustDialogAccepted": False, "lastCost": 2},
                                        untouched: {"hasTrustDialogAccepted": True},
                                    }}))
    unrelated.write_text('{"unselected":true}')
    monkeypatch.setattr(constants, "CLAUDE_DOTFILE", selected)
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(profile))
    monkeypatch.setattr(settings, "CLAUDE_DOTFILE", selected)
    # paths.CCX_ROOT was imported before this test's per-case monkeypatch.
    monkeypatch.setattr(settings, "trust_ledger_path",
                        lambda: short_root / "trust-ledger.json")
    assert settings.ensure_trusted_paths([ours])
    ledger = short_root / "trust-ledger.json"
    before_cfg, before_ledger = selected.read_bytes(), ledger.read_bytes()
    cli.main(["untrust", "--dry-run", "--json"])
    report = json.loads(capsys.readouterr().out)
    assert report == {"config": str(selected), "dry_run": True,
                      "paths": [{"path": ours, "action": "restored"}]}
    cli.main(["untrust", ours, "--dry-run"])
    assert capsys.readouterr().out == f"{ours}: restored (dry run)\n"
    assert (selected.read_bytes(), ledger.read_bytes()) == (before_cfg, before_ledger)
    assert unrelated.read_text() == '{"unselected":true}'

    cli.main(["untrust", ours, "--json"])
    assert json.loads(capsys.readouterr().out)["paths"] == [
        {"path": ours, "action": "restored"}]
    final = json.loads(selected.read_text())
    assert final["projects"][ours] == {"hasTrustDialogAccepted": False, "lastCost": 2}
    assert final["projects"][untouched] == {"hasTrustDialogAccepted": True}
    assert final["oauthAccount"] == {"id": "provider-owned"}
    assert unrelated.read_text() == '{"unselected":true}'


@pytest.mark.parametrize("engine", ("codex", None))
@pytest.mark.parametrize("json_mode", (True, False))
@pytest.mark.parametrize("generation_offset", (-1, 1))
def test_shutdown_older_codexd_uses_only_generation_agnostic_teardown(
    short_root, monkeypatch, capsys, engine, json_mode, generation_offset,
):
    """An old, authenticated daemon cannot supply current worker inventory.

    Its native worker registry is opaque: a forceful daemon-stop is the ONLY
    compatible mutation. Old workers unload on daemon exit, retaining history.
    """
    store = _root(monkeypatch, short_root)
    add_worker(short_root, B, "codex")
    original = store.record(B)
    old_gen = constants.CODEXD_RPC_GENERATION + generation_offset
    version_label = "older" if generation_offset < 0 else "newer"
    old_registry = {
        "ok": True, "transport": "served", "state": "ready",
        "rpcGeneration": old_gen,
        # The foreign entry must never be resolved against the new owner index.
        "workers": {B: "busy", "not-in-our-owner-index": "waiting"},
    }
    state = {"running": True}
    calls = []

    def old_status(timeout=2):
        calls.append(("status", timeout))
        return old_registry if state["running"] else {
            "ok": False, "transport": "absent", "error": "no daemon",
        }

    def fake_daemon_stop(*, force=False, timeout=10):
        calls.append(("daemon-stop", force))
        assert force is True, "only the incompatible daemon may be forced"
        assert state["running"]
        state["running"] = False
        return {
            "ok": True, "stopped": True, "transport": "served",
            "rpcGeneration": old_gen,
        }

    monkeypatch.setattr(codex_runtime, "daemon_status", old_status)
    monkeypatch.setattr(codex_runtime, "daemon_stop", fake_daemon_stop)
    monkeypatch.setattr(
        terminate, "confirmed_kill_uuid",
        lambda *_args, **_kw: pytest.fail("listed/killed an older daemon's workers"),
    )
    monkeypatch.setattr(
        maintenance, "_stop_notifyd",
        lambda *_args: pytest.fail("stopped an unrelated notifier"),
    )
    cli_args = ["shutdown"]
    if engine:
        cli_args += ["--engine", engine]
    if json_mode:
        cli_args += ["--json"]

    cli.main(cli_args + ["--dry-run"])
    preview = capsys.readouterr()
    assert not preview.err
    if json_mode:
        plan = json.loads(preview.out)
        assert plan["dry_run"] and plan["ok"] and plan["errors"] == []
        assert plan["codex"] == []
        assert plan["codexd"] == f"would-stop ({version_label} generation {old_gen})"
        if engine == "codex":
            assert plan["claude"] == [] and plan["notifyd"] == "untouched"
    else:
        assert f"codexd: would-stop ({version_label} generation {old_gen})" in preview.out
    assert not any(event[0] == "daemon-stop" for event in calls)
    assert state["running"]

    calls.clear()
    cli.main(cli_args)
    output = capsys.readouterr()
    assert not output.err
    if json_mode:
        result = json.loads(output.out)
        assert result["ok"] and not result["dry_run"] and not result["errors"]
        assert result["codex"] == []
        assert result["codexd"] == f"stopped ({version_label} generation {old_gen})"
        if engine == "codex":
            assert result["claude"] == [] and result["notifyd"] == "untouched"
    else:
        assert f"codexd: stopped ({version_label} generation {old_gen})" in output.out
    assert [event for event in calls if event[0] == "daemon-stop"] == [
        ("daemon-stop", True),
    ]
    assert not state["running"]
    # Cross-generation shutdown did not rewrite any worker histories.
    assert store.record(B) == original


@pytest.mark.parametrize("reply", [
    {"ok": False, "transport": "served", "rpcGeneration": 20,
     "error": "registry refused"},
    {"ok": True, "transport": "inconclusive", "rpcGeneration": 20,
     "workers": {}},
    {"ok": True, "transport": "served", "workers": {}},
])
def test_shutdown_does_not_force_unverified_codexd_generation(
    short_root, monkeypatch, reply,
):
    _root(monkeypatch, short_root)
    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout=2: reply)
    monkeypatch.setattr(
        codex_runtime, "daemon_stop",
        lambda **kw: pytest.fail("unverified generation reached force teardown"),
    )
    result = maintenance.shutdown(dry_run=False, engine="codex")
    assert not result["ok"] and result["codexd"] == "failed"
    assert result["codex"] == []
    assert len(result["errors"]) == 1
    assert "registry unavailable or malformed" in result["errors"][0]["error"]
