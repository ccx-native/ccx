"""Unique textual full-UUID prefix discovery: explicit API and CLI only.

Disposable sealed owner inventory; no provider, daemon or legacy short store.
"""
from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path

import pytest

from test_generation_store import A, B, G1, add_worker, fixture_root
from ccx import AmbiguousWorker, CcxClient, CodexClient, WorkerClient, constants, resolve_worker
from ccx.cli import main as cli
from ccx.system.cwd_paths import canonical_cwd
from ccx.errors import InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.rpc.local_rpc import dispatch_full_uuid
from ccx.store.store import GenerationStore
from ccx.store.root import StorageIntegrityError, require_ccx_active
from ccx.store.identity import UnknownWorker

# Same initial eight as A/B, and SAME engine as A. All three have distinct
# full UUIDs and intentionally no prefix uniqueness reservation.
D = "aaaaaaaa-1235-4000-8000-000000000004"
OTHER = "bbbbbbbb-1234-4000-8000-000000000003"
UNKNOWN = "cccccccc-1234-4000-8000-000000000005"


@pytest.fixture
def owners(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude", terminal=True)
    add_worker(root, B, "codex", terminal=True)
    add_worker(root, D, "claude", terminal=True)
    add_worker(root, OTHER, "codex", terminal=True)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    return root, require_ccx_active(root)


def test_python_explicit_global_and_same_engine_ambiguity_includes_stopped(owners):
    root, store = owners
    assert store.list_workers(limit=None)[0]["terminal_only"]
    expected = tuple(sorted((f"claude:{A}", f"codex:{B}", f"claude:{D}")))
    with pytest.raises(AmbiguousWorker) as e:
        resolve_worker("aaaaaaaa")
    assert e.value.candidates == expected and e.value.exit_code == 1
    assert all(handle in str(e.value) for handle in expected)
    with pytest.raises(AmbiguousWorker) as e:
        resolve_worker("claude:aaaaaaaa")
    assert e.value.candidates == tuple(sorted((f"claude:{A}", f"claude:{D}")))
    assert resolve_worker("codex:aaaaaaaa") == f"codex:{B}"
    assert resolve_worker("aaaaaaaa-1234") == f"claude:{A}"
    assert resolve_worker("aaaaaaaa-1235") == f"claude:{D}"
    assert resolve_worker("aaaaaaaa-5678") == f"codex:{B}"
    assert resolve_worker("bbbbbbbb") == f"codex:{OTHER}"
    assert not (root / "sessions").exists()
    assert not (root / "codex").exists()


@pytest.mark.parametrize("query", [
    "aaaaaaa", "AAAAAAAA", "aaaaaaaa-AB", "display is not a key",
    "claude:aaaaaaa", "claude:aaaaaaaa:xyz", "other:aaaaaaaa",
    "claude-session:aaaaaaaa", "aaaaaaaa-1234-4000-8000-00000000000z",
    "aaaaaaaa-1234-1000-8000-000000000001",
    "aaaaaaaa-1234-4000-8000-0000000000011",
])
def test_invalid_query_grammar_or_noncanonical_complete_uuid(owners, query):
    with pytest.raises(InvalidRequest):
        resolve_worker(query)


def test_complete_fast_path_qualifier_mismatch_and_no_compact_alias(owners, monkeypatch):
    _root, store = owners
    actual = GenerationStore.list_workers
    reads = []
    def spy(self, **kwargs):
        reads.append(kwargs)
        return actual(self, **kwargs)
    monkeypatch.setattr(GenerationStore, "list_workers", spy)
    assert resolve_worker(A) == f"claude:{A}"
    assert resolve_worker(f"codex:{B}") == f"codex:{B}"
    assert reads == []  # exact path never enumerates other workers
    with pytest.raises(InvalidRequest):
        resolve_worker(f"codex:{A}")
    with pytest.raises(SessionNotFound):
        resolve_worker(UNKNOWN)
    assert reads == []
    compact = A.replace("-", "")
    with pytest.raises(SessionNotFound):
        resolve_worker(compact)  # no hyphen stripping, even at 32 chars
    assert reads == []  # prefixes read only matching owner names, never the fleet
    with pytest.raises(SessionNotFound):
        resolve_worker("aaaaaaaa1234")  # not a displayed textual UUID prefix
    with pytest.raises(SessionNotFound):
        resolve_worker("dddddddd")


def test_all_or_nothing_cli_field_assignment_without_fleet_inventory_and_duplicates(
    owners, monkeypatch,
):
    _root, store = owners
    calls = []
    actual = GenerationStore.list_workers
    def spy(self, **kwargs):
        calls.append(kwargs)
        return actual(self, **kwargs)
    monkeypatch.setattr(GenerationStore, "list_workers", spy)
    args = Namespace(cmd="peek", status=True, target="aaaaaaaa-1234",
                     extra_shorts=["bbbbbbbb", "aaaaaaaa-1234", A])
    cli._preflight_worker_references(args, store)
    # A multi-target recorded peek resolves each target ONLY inside its reader.
    assert args.target == "aaaaaaaa-1234"
    assert args.extra_shorts == ["bbbbbbbb", "aaaaaaaa-1234", A]
    assert calls == []

    calls.clear()
    batch = Namespace(cmd="report",
                      targets=[A, "aaaaaaaa-1234", "aaaaaaaa"])
    original = list(batch.targets)
    with pytest.raises(AmbiguousWorker):
        cli._preflight_worker_references(batch, store)
    assert batch.targets == original  # no partial namespace mutation
    assert calls == []

    calls.clear()
    exact = Namespace(cmd="report", targets=[A, UNKNOWN, A])
    cli._preflight_worker_references(exact, store)
    assert exact.targets == [A, UNKNOWN, A] and calls == []

    mixed = Namespace(cmd="report", targets=[UNKNOWN, "aaaaaaaa-1234", A])
    cli._preflight_worker_references(mixed, store)
    assert mixed.targets == [UNKNOWN, f"claude:{A}", A]
    assert calls == []  # prefixes never enumerate the fleet


def test_multi_peek_status_does_not_attempt_atomic_preflight(owners, monkeypatch):
    from ccx.store import discovery as worker_discovery

    _root, store = owners
    attempts = []

    def no_atomic_resolution(*args, **kwargs):
        attempts.append((args, kwargs))
        raise AssertionError("multi-target peek must defer resolution to each row")

    monkeypatch.setattr(worker_discovery, "resolve_cli_fields", no_atomic_resolution)
    args = Namespace(cmd="peek", status=True, target="aaaaaaaa-1234",
                     extra_shorts=["aaaaaaaa", "bbbbbbbb"])
    cli._preflight_worker_references(args, store)
    assert not attempts
    assert args.target == "aaaaaaaa-1234"
    assert args.extra_shorts == ["aaaaaaaa", "bbbbbbbb"]


def _no_handler_parser(monkeypatch, *commands):
    """Patch *only test-local* argparse callbacks; real CLI preflight still runs."""
    parser = cli.build_parser()
    top = next(action for action in parser._actions if action.dest == "cmd")
    invoked = []
    for name in commands:
        subparser = top.choices[name]
        subparser.set_defaults(func=lambda args: invoked.append((args.cmd, vars(args))))
    monkeypatch.setattr(cli, "build_parser", lambda: parser)
    return invoked


_MISSING_CLI_VERBS = (
    ("peek", ("--status",)),
    ("send", ("hello", "--no-wait")),
    ("steer", ("hello", "--no-wait")),
    ("wait", ("--timeout", "0")),
    ("report", ()),
    ("logs", ()),
    ("context", ()),
    ("kill", ()),
    ("stop", ()),
    ("set", ("--name", "example")),
    ("resume", ()),
    ("fork", ("--no-wait",)),
    ("vars", ()),
    ("answer", ("hello", "--no-wait")),
    ("approve", ("--no-wait",)),
    ("reject", ("--no-wait",)),
    ("respond", ("--decline", "--no-wait")),
    ("events", ()),
    ("compact", ("--no-wait",)),
    ("attach", ()),
)


@pytest.mark.parametrize("verb,options", _MISSING_CLI_VERBS, ids=[row[0] for row in _MISSING_CLI_VERBS])
@pytest.mark.parametrize("target", [UNKNOWN, "dddddddd", f"claude:{UNKNOWN}",
                                    "codex:dddddddd"])
def test_cli_missing_worker_has_one_message_type_and_exit_code(
    owners, monkeypatch, capsys, verb, options, target,
):
    root, _store = owners

    def forbidden(*_args, **_kwargs):
        pytest.fail("missing worker must be refused before any command handler")

    monkeypatch.setattr(cli, "_native_handler", forbidden)
    with pytest.raises(SystemExit) as raised:
        cli._main([verb, target, *options])
    captured = capsys.readouterr()
    assert raised.value.code == SessionNotFound.exit_code == 2
    assert captured.err == f"no such worker: {target}\n"
    assert captured.out == ""
    assert not (root / "run").exists() and not (root / "sessions").exists()


_MISSING_PYTHON_METHODS = (
    ("worker", "status", (), {}),
    ("worker", "send", ("hello",), {"wait": False}),
    ("worker", "steer", ("hello",), {"wait": False}),
    ("worker", "wait", (), {"timeout": 0}),
    ("worker", "report", (), {}),
    ("worker", "context", (), {}),
    ("worker", "kill", (), {}),
    ("worker", "stop", (), {}),
    ("worker", "set_name", ("example",), {}),
    ("worker", "set_model", ("model",), {}),
    ("worker", "set_effort", ("low",), {}),
    ("worker", "resume", (), {}),
    ("worker", "fork", (), {}),
    ("worker", "variables", (), {}),
    ("worker", "approve", (), {}),
    ("worker", "reject", (), {}),
    ("worker", "events", (), {}),
    ("worker", "compact", (), {"wait": False}),
    ("worker", "conversation", (), {}),
    ("worker", "follow", (), {}),
    ("claude", "peek", (), {}),
    ("claude", "status", (), {}),
    ("claude", "send", ("hello",), {"wait": False}),
    ("claude", "steer", ("hello",), {"wait": False}),
    ("claude", "wait", (), {"timeout": 0}),
    ("claude", "report", (), {}),
    ("claude", "logs", (), {}),
    ("claude", "context", (), {}),
    ("claude", "kill", (), {}),
    ("claude", "set_name", ("example",), {}),
    ("claude", "set_model", ("model",), {}),
    ("claude", "set_effort", ("low",), {}),
    ("claude", "fork", (), {}),
    ("claude", "approve", (), {}),
    ("claude", "reject", (), {}),
    ("claude", "answer", ("hello",), {}),
    ("claude", "respond_elicitation", ("decline",), {}),
    ("claude", "events", (), {}),
    ("claude", "compact", (), {"wait": False}),
    ("codex", "send", ("hello",), {"wait": False}),
    ("codex", "steer", ("hello",), {}),
    ("codex", "wait", (), {"timeout": 0}),
    ("codex", "report", (), {}),
    ("codex", "context", (), {}),
    ("codex", "set_name", ("example",), {}),
    ("codex", "set_model", ("model",), {}),
    ("codex", "set_effort", ("low",), {}),
    ("codex", "resume", (), {}),
    ("codex", "fork", (), {}),
    ("codex", "approve", (), {"request_id": "unknown"}),
    ("codex", "reject", (), {"request_id": "unknown"}),
    ("codex", "compact", (), {}),
)


@pytest.mark.parametrize(
    "client_kind,method,args,kwargs", _MISSING_PYTHON_METHODS,
    ids=[f"{kind}.{method}" for kind, method, _args, _kwargs in _MISSING_PYTHON_METHODS],
)
def test_python_clients_missing_worker_share_session_not_found(owners, client_kind, method, args, kwargs):
    klass = {"worker": WorkerClient, "claude": CcxClient, "codex": CodexClient}[client_kind]
    client = klass()
    target = [UNKNOWN] if client_kind == "claude" and method == "status" else UNKNOWN
    with pytest.raises(SessionNotFound) as raised:
        getattr(client, method)(target, *args, **kwargs)
    assert type(raised.value) is SessionNotFound
    assert raised.value.exit_code == 2
    assert str(raised.value) == f"no such worker: {UNKNOWN}"


def test_not_found_normalization_preserves_other_refusals(owners, capsys):
    _root, store = owners
    reserved = "cccccccc-9999-4000-8000-000000000099"
    store.reserve_creation(reserved, engine="claude", fingerprint=spec("claude"))
    with pytest.raises(SystemExit) as absent:
        cli._main(["send", reserved, "hello", "--no-wait"])
    assert absent.value.code == 2
    message = capsys.readouterr().err
    assert "reserved/quarantined" in message
    assert "no such worker" not in message

    with pytest.raises(SystemExit) as malformed:
        cli._main(["peek", "not-a-worker-uuid"])
    assert malformed.value.code == InvalidRequest.exit_code == 1
    assert "worker prefix" in capsys.readouterr().err

    with pytest.raises(SystemExit) as ambiguous:
        cli._main(["peek", "aaaaaaaa"])
    assert ambiguous.value.code == AmbiguousWorker.exit_code == 1
    candidates = capsys.readouterr().err
    assert "ambiguous" in candidates.lower()
    assert f"claude:{A}" in candidates and f"codex:{B}" in candidates


def test_real_cli_report_and_send_var_batch_refuse_before_handler(
    owners, monkeypatch, capsys,
):
    root, store = owners
    invoked = _no_handler_parser(monkeypatch, "report", "peek", "send")
    for argv in (
        ["report", "aaaaaaaa-1234", "aaaaaaaa", "--json"],
        ["send", "aaaaaaaa-1234", "no provider", "--vars-from", "aaaaaaaa",
         "--no-wait"],
    ):
        with pytest.raises(SystemExit) as ex:
            cli._main(argv)
        assert ex.value.code == 1
        stderr = capsys.readouterr().err
        assert "ambiguous" in stderr
        assert f"claude:{A}" in stderr and f"codex:{B}" in stderr
        assert f"claude:{D}" in stderr
    assert invoked == []
    # Status batches now report ambiguous targets per row in the real handler,
    # rather than refusing the entire read during CLI preflight.
    cli._main(["peek", "aaaaaaaa-1234", "aaaaaaaa", "--status", "--json"])
    assert invoked[-1][0] == "peek"
    assert invoked[-1][1]["target"] == "aaaaaaaa-1234"
    assert invoked[-1][1]["extra_shorts"] == ["aaaaaaaa"]
    assert not (root / "run").exists()
    assert not (root / "sessions").exists()


def test_real_cli_prefix_missing_before_handler_and_valid_many_rewrite(
    owners, monkeypatch, capsys,
):
    _root, _store = owners
    invoked = _no_handler_parser(monkeypatch, "report", "peek", "steer", "reconcile")
    with pytest.raises(SystemExit) as ex:
        cli._main(["report", "aaaaaaaa-1234", "dddddddd"])
    assert ex.value.code == 2
    assert capsys.readouterr().err.strip() == "no such worker: dddddddd"
    assert invoked == []
    cli._main(["report", "aaaaaaaa-1234", "bbbbbbbb", "aaaaaaaa-1234"])
    assert invoked[-1][1]["targets"] == [f"claude:{A}", f"codex:{OTHER}", f"claude:{A}"]
    cli._main(["peek", "aaaaaaaa-1234", "bbbbbbbb", "--status"])
    assert invoked[-1][1]["target"] == "aaaaaaaa-1234"
    assert invoked[-1][1]["extra_shorts"] == ["bbbbbbbb"]
    cli._main(["steer", "aaaaaaaa-1234", "x", "--no-wait"])
    assert invoked[-1][1]["target"] == f"claude:{A}"
    cli._main(["reconcile", "aaaaaaaa-1234", "--delivery-id", "the-delivery-id"])
    assert invoked[-1][1]["target"] == f"claude:{A}"
    assert invoked[-1][1]["delivery_id"] == "the-delivery-id"


def test_real_cli_stopped_owner_rename_only_original_full_uuid(owners, capsys):
    root, store = owners
    target = root / "identity" / "generations" / G1 / "workers" / "claude" / A / "record.json"
    sibling = root / "identity" / "generations" / G1 / "workers" / "claude" / D / "record.json"
    codex = root / "identity" / "generations" / G1 / "workers" / "codex" / B / "record.json"
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in (target, sibling, codex)}
    assert store.record(A)["terminalOnly"] is True
    cli._main(["set", "aaaaaaaa-1234", "--name", "stopped renamed", "--json"])
    output = json.loads(capsys.readouterr().out)
    assert output["worker"] == f"claude:{A}"
    assert output["worker_id"] == A
    assert store.record(A)["name"] == "stopped renamed"
    assert target.read_bytes() != before[target][0]
    assert (sibling.read_bytes(), sibling.stat().st_mtime_ns) == before[sibling]
    assert (codex.read_bytes(), codex.stat().st_mtime_ns) == before[codex]
    assert not (root / "sessions").exists()


def test_true_corrupt_owner_inventory_blocks_unique_prefix_and_handler(
    owners, monkeypatch, capsys,
):
    root, store = owners
    # Corruption in a MATCHING owner refuses; non-matching owners are never read.
    corrupt = root / "identity" / "generations" / G1 / "owners" / (A + ".json")
    value = json.loads(corrupt.read_text())
    value["generation"] = "wrong-generation"
    corrupt.write_text(json.dumps(value))
    target = root / "identity" / "generations" / G1 / "workers" / "claude" / A / "record.json"
    before = (target.read_bytes(), target.stat().st_mtime_ns)
    invoked = _no_handler_parser(monkeypatch, "report")
    with pytest.raises(SystemExit) as e:
        cli._main(["report", "aaaaaaaa-1234"])
    assert e.value.code == 3  # real owner corruption, not not-found or unique
    assert "inventory unavailable" in capsys.readouterr().err
    assert invoked == []
    with pytest.raises(SessionUnavailable, match="inventory unavailable"):
        resolve_worker("aaaaaaaa-1234")
    assert (target.read_bytes(), target.stat().st_mtime_ns) == before


def test_explicit_helper_is_only_python_prefix_entrypoint_and_v1_rpc_remains_exact(
    owners,
):
    root, store = owners
    assert resolve_worker("aaaaaaaa-1234") == f"claude:{A}"
    with pytest.raises((InvalidRequest, SessionNotFound)):
        CcxClient().peek("aaaaaaaa-1234")
    with pytest.raises((InvalidRequest, SessionNotFound)):
        WorkerClient().status("aaaaaaaa-1234")
    with pytest.raises(Exception):
        store.resolve("aaaaaaaa-1234")
    reply = dispatch_full_uuid({
        "protocol_version": 2, "request_id": "strict-v1-prefix",
        "op": "worker.inspect", "params": {"target": "aaaaaaaa-1234"},
    }, store)
    assert reply["ok"] is False and reply["error"]["code"] == "invalid_request"
    assert store.record(A)["name"] == "display is not a key"


def test_disabled_and_internal_commands_are_not_resolved(owners):
    from ccx.cli.parser import build_parser
    _root, store = owners
    parser = build_parser()
    for argv, unchanged in (
        (["controller", "status", "aaaaaaaa"], "aaaaaaaa"),
        (["rpc"], None),
    ):
        args = parser.parse_args(argv)
        cli._preflight_worker_references(args, store)
        if unchanged is not None:
            assert args.target == unchanged


def test_reserved_worker_not_selected_and_root_verb_resolves_by_owner(
    owners, monkeypatch,
):
    from test_generation_store import spec
    root, store = owners
    reservation_id = "aaaaaaaa-9999-4000-8000-000000000099"
    store.reserve_creation(reservation_id, engine="claude", fingerprint=spec("claude"))
    matches = store.list_workers(limit=None)
    assert reservation_id not in {row["worker_id"] for row in matches}
    with pytest.raises(AmbiguousWorker) as e:
        resolve_worker("aaaaaaaa")
    assert reservation_id not in str(e.value)
    args = Namespace(cmd="set", target="aaaaaaaa-1234")
    cli._preflight_worker_references(args, store)
    assert args.target == f"claude:{A}"  # qualified by evidence, not CLI verb
    with pytest.raises(SessionNotFound):
        resolve_worker("aaaaaaaa-9999")  # reservation never discovered


def test_actual_report_all_exact_keeps_aggregate_errors_without_inventory(
    owners, monkeypatch, capsys,
):
    _root, store = owners
    def forbidden_scan(self, **kwargs):
        raise AssertionError("all-exact report must not enumerate owners")
    monkeypatch.setattr(GenerationStore, "list_workers", forbidden_scan)
    with pytest.raises(SystemExit) as result:
        cli._main(["report", A, UNKNOWN, "--json"])
    # Existing report is an aggregate, not an all-or-nothing prefix query.
    assert result.value.code == 3
    data = json.loads(capsys.readouterr().out)
    assert data["ok"] is False and len(data["errors"]) == 2
    assert {row["target"] for row in data["errors"]} == {A, UNKNOWN}


# Owned list/name/peek and exact full UUID worker inventory.
import os
import stat
import subprocess
import sys
import time
from test_generation_store import C, G2, put, privdir, record, spec, switch_pointer
def test_full_uuid_owner_inventory_lists_same_prefix_and_names_independently(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    for wid, engine in ((A, "claude"), (B, "codex")):
        r = ctx.reserve_creation(wid, engine=engine, fingerprint=spec(engine))
        rec = record(wid, engine)
        rec.update({
            "name": "same name",
            "cwd": "/tmp/owner-shared",
            "createdAt": 1700000000000,
            "lastTurnAt": 1700000001111 if engine == "codex" else 1700000002222,
            "lifecycle": {"state": "idle" if engine == "claude" else "stopped"},
            "stats": {"turns": 2 if engine == "claude" else 0},
        })
        ctx.publish_first_record(r, rec)
    rows = ctx.list_workers()
    assert len(rows) == 2
    assert {row["worker_id"] for row in rows} == {A, B}
    assert {row["worker"] for row in rows} == {f"claude:{A}", f"codex:{B}"}
    assert all(row["short"] == "aaaaaaaa" for row in rows)
    assert all(row["ambiguous_short"] for row in rows)
    assert all(row["name"] == "same name" for row in rows)
    assert all(row["state_source"] == "recorded" and row["loaded"] is None
               for row in rows)
    assert rows[0]["worker_id"] == A
    assert len(ctx.list_workers(engine="claude")) == 1
    assert ctx.list_workers(engine="codex")[0]["worker_id"] == B
    assert ctx.list_workers(cwd="/tmp/not-worker") == []
    assert [row["worker_id"] for row in ctx.list_workers(state="idle")] == [A]
    assert len(ctx.list_workers(limit=1)) == 1
    assert ctx.list_workers(limit=0) == []


def test_full_uuid_owner_inventory_skips_only_proven_reserved_and_refuses_corrupt(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    reserved = root / "identity" / "generations" / G1 / "owners" / f"{A}.json"
    put(reserved, {
        "schemaVersion": 2, "workerId": A, "engine": "claude",
        "generation": G1, "status": "indeterminate-reserved",
    })
    assert ctx.list_workers() == []
    put(reserved, {
        "schemaVersion": 2, "workerId": A, "engine": "claude",
        "generation": G1, "status": "worker",  # without a real record
    })
    with pytest.raises(StorageIntegrityError, match="no matching"):
        ctx.list_workers()
    with pytest.raises(StorageIntegrityError):
        ctx.list_workers(max_owners=0)


def test_full_uuid_owner_inventory_never_reimports_external_or_native_only(tmp_path):
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    external = root / "external" / "claude"
    privdir(external)
    put(external / f"{A}.json", {"sessionId": A, "origin": "external", "managed": False})
    assert ctx.list_workers() == []
    with pytest.raises(UnknownWorker):
        ctx.resolve(A)
    assert not (root / "identity" / "generations" / G1 / "owners").exists()


def test_cli_full_uuid_list_real_subprocess_records_only_no_legacy_writes(tmp_path):
    import subprocess, sys
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    for wid, engine in ((A, "claude"), (B, "codex")):
        r = ctx.reserve_creation(wid, engine=engine, fingerprint=spec(engine))
        rec = record(wid, engine)
        rec.update({
            "name": "COLLIDING NAME",
            # Records hold the canonical cwd (macOS /tmp -> /private/tmp).
            "cwd": canonical_cwd("/tmp/workspace/team"),
            "lifecycle": {"state": "idle"},
            "createdAt": 1700000000000,
            "lastTurnAt": 1700000000000,
        })
        ctx.publish_first_record(r, rec)
    before = sorted(
        (str(p.relative_to(root)), p.stat().st_mtime_ns)
        for p in root.rglob("*") if p.is_file()
    )
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    def run(*args):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", "list", *args],
            env=env, text=True, capture_output=True, timeout=25,
        )
    listed = run("--json")
    assert listed.returncode == 0, listed.stderr
    rows = json.loads(listed.stdout)
    assert len(rows) == 2
    assert {r["worker"] for r in rows} == {f"claude:{A}", f"codex:{B}"}
    assert all(r["ambiguous_short"] for r in rows)
    assert all(r["loaded"] is None for r in rows)
    assert json.loads(run("--json", "--engine", "codex").stdout)[0]["worker_id"] == B
    assert len(json.loads(run("--json", "--cwd-prefix", "/tmp/workspace").stdout)) == 2
    assert json.loads(run("--json", "--cwd", "/tmp/foreign").stdout) == []
    assert len(json.loads(run("--json", "--state", "idle", "-n", "1").stdout)) == 1
    text_out = run()
    assert text_out.returncode == 0
    assert f"claude:{A}" in text_out.stdout and f"codex:{B}" in text_out.stdout
    assert sorted(
        (str(p.relative_to(root)), p.stat().st_mtime_ns)
        for p in root.rglob("*") if p.is_file()
    ) == before
    assert not (root / "sessions").exists()


@pytest.mark.parametrize("flag", [
    "--external", "--native-bg", "--importable", "--all", "--live",
])
def test_cli_full_uuid_list_refuses_removed_external_filters(tmp_path, flag):
    import subprocess, sys
    root = fixture_root(tmp_path)
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))
    result = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "list", flag],
        env=env, text=True, capture_output=True, timeout=25,
    )
    assert result.returncode == 1
    assert "unrecognized arguments:" in result.stderr
    assert sorted(str(p.relative_to(root)) for p in root.rglob("*")) == before


def test_native_claude_sid_is_not_an_owned_peek_target(tmp_path):
    import subprocess, sys
    root = fixture_root(tmp_path)
    native_sid = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    before = sorted(str(path.relative_to(root)) for path in root.rglob("*"))
    result = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "peek", native_sid,
         "--status", "--json"],
        env=env, text=True, capture_output=True, timeout=25,
    )
    assert result.returncode != 0
    assert result.stderr
    assert sorted(str(path.relative_to(root)) for path in root.rglob("*")) == before


def test_cli_list_absent_generation_never_uses_old_sessions(tmp_path):
    import subprocess, sys
    root = tmp_path / "legacy"
    root.mkdir(mode=0o700)
    old_sessions = root / "sessions"
    old_sessions.mkdir(mode=0o700)
    (old_sessions / "aaaaaaaa.json").write_text("A secret stale record")
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    before = (old_sessions / "aaaaaaaa.json").read_bytes()
    result = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "list", "--json"],
        env=env, text=True, capture_output=True, timeout=25,
    )
    assert result.returncode != 0
    assert result.stderr.startswith("ccx unavailable: ")
    assert (old_sessions / "aaaaaaaa.json").read_bytes() == before
    assert not (root / "identity").exists()


def test_cli_claude_full_uuid_set_name_keeps_unrelated_record_fields(tmp_path):
    import subprocess, sys
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    for wid in (A, B):
        r = ctx.reserve_creation(wid, engine="claude", fingerprint=spec("claude"))
        base = record(wid, "claude")
        base.update(name="same display name", cwd="/tmp/workspace",
                    createArgs={"model": "keep-original"},
                    stats={"turns": 4, "inputTokens": 900},
                    lifecycle={"state": "idle", "hostPid": None},
                    analytics={"sampleCount": 2},
                    recordRevision=0)
        ctx.publish_first_record(r, base)
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    def run(*args):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", "set", *args],
            env=env, text=True, capture_output=True, timeout=25,
        )
    changed = run(f"claude:{A}", "--name", "alpha\nbeta", "--json")
    assert changed.returncode == 0, changed.stderr
    out = json.loads(changed.stdout)
    assert out["worker"] == f"claude:{A}" and out["worker_id"] == A
    assert out["short"] == "aaaaaaaa" and out["name"] == "alpha beta"
    fresh = ctx.record(A)
    assert fresh["recordRevision"] == 1  # CAS fact, not a required CLI field
    assert fresh["name"] == "alpha beta"
    assert fresh["native"]["sessionId"] == A
    assert fresh["lifecycle"] == {"state": "idle", "hostPid": None}
    assert fresh["createArgs"] == {"model": "keep-original"}
    assert fresh["analytics"] == {"sampleCount": 2}
    assert fresh["stats"] == {"turns": 4, "inputTokens": 900}
    assert ctx.record(B)["name"] == "same display name"
    assert ctx.record(B)["recordRevision"] == 0
    reset = run(A, "--name", "", "--json")
    assert reset.returncode == 0, reset.stderr
    assert json.loads(reset.stdout)["name"] == "ccx-aaaaaaaa"
    assert ctx.record(A)["recordRevision"] == 2
    assert ctx.record(B)["name"] == "same display name"
    assert not (root / "sessions").exists()
    assert not (root / "run").exists()


@pytest.mark.parametrize("arguments", [
    ("aaaaaaaa", "--name", "wrong"),
    ("same display name", "--name", "wrong"),
    ("claude-session:" + A, "--name", "wrong"),
    ("claude:" + A, "--model", "sonnet"),
    ("claude:" + A, "--name", "wrong", "--restart"),
    ("claude:" + A, "--permissions", "restricted"),
    ("claude:" + A, "--effort", "high"),
    ("codex:" + B, "--permissions", "restricted"),
])
def test_cli_set_name_rejects_aliases_unsafe_config_and_wrong_engine_before_write(tmp_path, arguments):
    import subprocess, sys
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    add_worker(root, A, "claude")
    add_worker(root, B, "codex")
    # A live host PID without a start proof: absence is unproven, so a plain
    # config edit must refuse (a provably stopped worker may be edited).
    ctx.mutate_record(A, patches={"lifecycle.hostPid": os.getpid()})
    old_a = ctx.record(A)
    old_b = ctx.record(B)
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    result = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "set", *arguments],
        env=env, capture_output=True, text=True, timeout=25,
    )
    assert result.returncode != 0
    assert ctx.record(A) == old_a
    assert ctx.record(B) == old_b
    assert not (root / "sessions").exists()


def test_cli_set_name_uses_record_CAS_concurrent_with_stats_patches(tmp_path):
    import subprocess, sys
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    r = ctx.reserve_creation(A, engine="claude", fingerprint=spec("claude"))
    rec = record(A, "claude")
    rec.update(
        name="original",
        stats={"turns": 0},
        lifecycle={"state": "idle"},
        recordRevision=0,
    )
    ctx.publish_first_record(r, rec)
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    proc = subprocess.Popen(
        [sys.executable, "-B", "-m", "ccx", "set",
         f"claude:{A}", "--name", "new name", "--json"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    # No ordering assumption: fresh semantic patch under the UUID record lock
    # preserves the other process's independent stats update in either order.
    ctx.mutate_record(A, increments={"stats.turns": 7})
    stdout, stderr = proc.communicate(timeout=25)
    assert proc.returncode == 0, stderr
    assert json.loads(stdout)["name"] == "new name"
    latest = ctx.record(A)
    assert latest["name"] == "new name"
    assert latest["stats"]["turns"] == 7
    assert latest["lifecycle"]["state"] == "idle"
    assert latest["recordRevision"] == 2



def test_cli_codex_full_uuid_name_update_requires_owned_UUID(tmp_path):
    import subprocess, sys
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    add_worker(root, A, "claude")
    add_worker(root, B, "codex")
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    def run(*args):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", "set", *args],
            env=env, text=True, capture_output=True, timeout=25,
        )
    before_claude = ctx.record(A)
    initial_codex = ctx.record(B)
    changed = run(f"codex:{B}", "--name", "codex task", "--json")
    assert changed.returncode == 0, changed.stderr
    result = json.loads(changed.stdout)
    assert result["worker"] == f"codex:{B}"
    assert result["worker_id"] == B
    assert result["name"] == "codex task"
    assert ctx.record(B)["recordRevision"] == 1
    assert ctx.record(B)["native"] == initial_codex["native"]
    assert ctx.record(A) == before_claude
    reset = run(B, "--name", "", "--json")
    assert reset.returncode == 0, reset.stderr
    assert json.loads(reset.stdout)["name"] == "codex-aaaaaaaa"
    assert ctx.record(B)["recordRevision"] == 2
    assert not (root / "codex").exists()
    assert not (root / "sessions").exists()
    assert not (root / "run").exists()


@pytest.mark.parametrize("args", [
    ("aaaaaaaa", "--name", "no"),
    ("same display name", "--name", "no"),
    ("codex:" + B, "--restart", "--model", "gpt-5"),
])
def test_cli_codex_name_rejects_legacy_targets_or_native_config(tmp_path, args):
    import subprocess, sys
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    add_worker(root, B, "codex")
    ctx = require_ccx_active(root)
    first = ctx.record(A)
    second = ctx.record(B)
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    result = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "set", *args],
        env=env, capture_output=True, text=True, timeout=25,
    )
    assert result.returncode != 0
    assert ctx.record(A) == first
    assert ctx.record(B) == second


def test_cli_peek_status_exact_full_UUIDs_forensic_only(tmp_path):
    import subprocess, sys
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    for wid, engine in ((A, "claude"), (B, "codex")):
        r = ctx.reserve_creation(wid, engine=engine, fingerprint=spec(engine))
        source = record(wid, engine,
                        native_state="unbound-unknown" if engine == "codex"
                        else "thread-bound",
                        terminal=(engine == "codex"))
        source["lifecycle"] = {"state": "idle" if engine == "claude" else "crashed"}
        source["stats"] = {"turns": 3 if engine == "claude" else 0}
        source["lastTurnAt"] = 1700000000000
        ctx.publish_first_record(r, source)
    before = sorted(
        (str(p.relative_to(root)), p.stat().st_mtime_ns)
        for p in root.rglob("*") if p.is_file()
    )
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    cmd = [sys.executable, "-B", "-m", "ccx", "peek", f"claude:{A}", B,
           "--status", "--json"]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=25)
    assert proc.returncode == 0, proc.stderr
    statuses = json.loads(proc.stdout)["statuses"]
    assert len(statuses) == 2
    assert {x["worker"] for x in statuses} == {f"claude:{A}", f"codex:{B}"}
    assert statuses[0]["turn"] == 3
    assert statuses[1]["terminal_only"] is True
    assert statuses[1]["native_state"] == "unbound-unknown"
    assert all(s["host_status"] == "unobserved" and s["loaded"] is None
               and not s["pending_input_observed"] for s in statuses)
    assert "provider" not in proc.stdout
    assert sorted(
        (str(p.relative_to(root)), p.stat().st_mtime_ns)
        for p in root.rglob("*") if p.is_file()
    ) == before
    assert not (root / "run").exists()
    assert not (root / "sessions").exists()
    plain = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "peek", A, "--status"],
        env=env, capture_output=True, text=True, timeout=25,
    )
    assert plain.returncode == 0
    assert f"claude:{A}" in plain.stdout
    assert plain.stdout.startswith(f"claude:{A}: ")


@pytest.mark.parametrize("tail", [
    ("aaaaaaaa", "--status", "--json"),
    ("same display name", "--status"),
    ("claude:" + B, "--status"),
    (C, "--status"),
    ("claude:" + A, "--live"),
    ("claude:" + A, "--status", "--debug"),
    ("claude:" + A, "--status", "--full"),
    ("claude:" + A, "--status", "--tools-only"),
    ("claude:" + A, "--status", "--max-chars", "20"),
])
def test_cli_peek_short_native_wrong_engine_and_unimplemented_live_refuse(tmp_path, tail):
    import subprocess, sys
    root = fixture_root(tmp_path)
    add_worker(root, A)
    add_worker(root, B, "codex")
    ctx = require_ccx_active(root)
    original_a = ctx.record(A)
    original_b = ctx.record(B)
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    process = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "peek", *tail],
        env=env, capture_output=True, text=True, timeout=25,
    )
    assert process.returncode != 0
    assert ctx.record(A) == original_a
    assert ctx.record(B) == original_b
    assert not (root / "run").exists()
    assert not (root / "sessions").exists()
