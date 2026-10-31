"""Full-UUID Claude compact facades over the accepted native C1B core.

The original disposable host runs the generated-hook fake Claude provider.
Locally appended provider-shaped terminal rows do not claim real-provider parity.
Codex CLI compact and persistent V1 --serve remain gated.
"""
from __future__ import annotations

import json
from pathlib import Path
import time

import pytest

from ccx import WorkerClient
from ccx.store import paths
from ccx.errors import CompactIndeterminate, SessionUnavailable
from ccx.store.root import NativeActionDenied
from ccx.claude.transcript import render_transcript_json
from ccx.store.worker_state import normalize_native_state
from test_generation_store import A, B, add_worker, short_root
from test_compact_claude import (
    _manual_boundary, _manual_command, _native_append, _original_native_source,
)
from test_claude_native_cli import generated_hook_host


def test_worker_client_compact_preserves_recorded_before_and_proven_boundary(
    generated_hook_host, short_root, monkeypatch,
):
    store, _call, env, _hooks = generated_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    source = _original_native_source(store)
    render_transcript_json(A)

    worker = WorkerClient()
    before = worker.context(f"claude:{A}")
    assert before["tokens_used"] == 1234, before
    assert "compact" not in worker.capabilities("claude")
    assert "compact" not in worker.capabilities("codex")

    add_worker(short_root, B, "codex")
    codex_record = store.record(B)
    # Codex accepts caller IDs, but this pre-v2 fixture has no verified
    # native source. Its original binding guard rejects before any native effect.
    with pytest.raises(SessionUnavailable, match="native binding requires v2"):
        worker.compact(B, wait=False, operation_id="caller-id")
    with pytest.raises(NativeActionDenied, match="native binding requires v2"):
        worker.compact_status(B, "not-a-codex-compact")
    assert store.record(B) == codex_record

    # Prove the wrapper uses just one existing recorded context reading.
    seen = []
    original_context = worker.context

    def observed_context(target):
        seen.append(target)
        return original_context(target)

    monkeypatch.setattr(worker, "context", observed_context)
    token = "worker-client-compact-original"
    accepted = worker.compact(A, wait=False, operation_id=token, timeout=12)
    assert seen == [f"claude:{A}"]
    assert accepted["worker"] == f"claude:{A}"
    assert accepted["worker_id"] == A and accepted["short"] == A[:8]
    assert accepted["engine"] == "claude"
    assert accepted["state"] == normalize_native_state(accepted["native_state"])
    assert accepted["operation_id"] == token
    assert accepted["operation_status"] in {"accepted", "pasted", "submitted"}
    assert accepted["tokens_before"] == 1234
    assert accepted["tokens_after"] is None and accepted["changed"] is None
    assert accepted["compacted"] is False
    assert store.read_receipt(A, "compact", token)["state"] == "accepted"
    assert not store.worker_path(B, "compact.json").exists()

    _native_append(source, _manual_command(),
                   _manual_boundary("worker-proven-manual-boundary"))
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline:
        settled = worker.compact_status(A, token)
        if settled["operation_status"] == "succeeded":
            break
        time.sleep(.08)
    assert settled["operation_status"] == "succeeded", settled
    assert settled["compacted"] is True and settled["changed"] is True
    assert (settled["tokens_before"], settled["tokens_after"]) == (1234, 100)
    assert settled["evidence_id"] == "worker-proven-manual-boundary"
    assert settled["worker"] == f"claude:{A}"
    assert settled["state"] == normalize_native_state(settled["native_state"])

    # Synthetic transcript-only evidence does not also finish the fake host's
    # input lifecycle. A synchronous compact cannot return as settled yet.
    with pytest.raises(CompactIndeterminate):
        worker.compact(A, operation_id=token, wait=True, timeout=.5)
    assert worker.compact_status(A, token)["operation_status"] == "succeeded"
    assert len(seen) == 2  # one recorded pre-read per facade compact invocation


def test_cli_compact_prefix_discovery_stable_id_and_no_parent_refusal(
    generated_hook_host, short_root, monkeypatch,
):
    store, call, env, _hooks = generated_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    source = _original_native_source(store)
    add_worker(short_root, B, "codex")  # stopped, same display prefix
    native_input = Path(env["FAKE_LOG"])

    # Codex CLI compact is now routed, but this Claude fake-host fixture has
    # NO original native Codex creation claim. Refuse unavailable before effect.
    codex = call("compact", B, "--no-wait")
    assert codex.returncode == 3, (codex.stdout, codex.stderr)
    assert "not integrated" not in codex.stderr
    assert "native" in codex.stderr.lower() or "original" in codex.stderr.lower()
    assert "operation_id=" in codex.stderr
    assert not store.worker_path(B, "compact.json").exists()

    ambiguous = call(
        "compact", "aaaaaaaa", "--no-wait",
        "--operation-id", "should-not-exist", "--json",
    )
    assert ambiguous.returncode == 1, (ambiguous.stdout, ambiguous.stderr)
    assert ambiguous.stdout == ""
    assert f"claude:{A}" in ambiguous.stderr
    assert f"codex:{B}" in ambiguous.stderr
    assert "ambiguous" in ambiguous.stderr.lower()
    assert store.read_receipt(A, "compact", "should-not-exist") is None
    assert not store.worker_path(A, "compact.json").exists()
    assert not native_input.exists() or b"/compact" not in native_input.read_bytes()

    # N4 now works with a proven Codex parent, but an ordinary local caller
    # cannot manufacture one, so refusal must precede any original /compact.
    no_parent_id = "compact-no-parent-must-not-submit"
    notify = call(
        "compact", "aaaaaaaa-1234", "--notify-parent",
        "--operation-id", no_parent_id, "--json",
    )
    assert notify.returncode != 0, (notify.stdout, notify.stderr)
    assert "parent notification" in notify.stderr.lower() and no_parent_id in notify.stderr
    assert store.read_receipt(A, "compact", no_parent_id) is None
    assert not store.worker_path(A, "compact.json").exists()

    # --settle is parsed, then rejected for Claude before any native effect.
    for forbidden, detail in (("--settle", "Codex worker"),
                              ("--cap-5h", "unrecognized arguments"),
                              ("--cap-opus", "unrecognized arguments")):
        parsed = call("compact", A, forbidden, "--no-wait")
        assert parsed.returncode != 0, (forbidden, parsed.stdout, parsed.stderr)
        assert detail in parsed.stderr
        assert parsed.stdout == ""
    assert not store.worker_path(A, "compact.json").exists()

    token = "cli-original-same-id"
    submitted = call(
        "compact", "aaaaaaaa-1234", "--no-wait",
        "--operation-id", token, "--json",
    )
    assert submitted.returncode == 0, (submitted.stdout, submitted.stderr)
    first = json.loads(submitted.stdout)
    assert first["worker"] == f"claude:{A}"
    assert first["worker_id"] == A and first["short"] == A[:8]
    assert first["operation_id"] == token
    assert first["operation_status"] in {"accepted", "pasted", "submitted"}
    assert first["compacted"] is False
    assert first["pre_tokens"] is None and first["post_tokens"] is None

    until = time.monotonic() + 8
    while time.monotonic() < until:
        if native_input.exists() and b"/compact" in native_input.read_bytes():
            break
        time.sleep(.05)
    assert native_input.exists() and native_input.read_bytes().count(b"/compact") == 1

    retried = call(
        "compact", "aaaaaaaa-1234", "--no-wait",
        "--operation-id", token, "--json",
    )
    assert retried.returncode == 0, (retried.stdout, retried.stderr)
    assert json.loads(retried.stdout)["operation_id"] == token

    plain = call(
        "compact", "aaaaaaaa-1234", "--no-wait", "--operation-id", token,
    )
    assert plain.returncode == 0, (plain.stdout, plain.stderr)
    assert token in plain.stdout and "not yet proven" in plain.stdout
    assert native_input.read_bytes().count(b"/compact") == 1

    _native_append(source, _manual_command(),
                   _manual_boundary("cli-proven-original-boundary"))
    completed = call(
        "compact", "aaaaaaaa-1234", "--operation-id", token, "--json",
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    terminal = json.loads(completed.stdout)
    assert terminal["worker"] == f"claude:{A}"
    assert terminal["operation_id"] == token
    assert terminal["compacted"] is True
    assert terminal["operation_status"] == "succeeded"
    assert terminal["evidence_uuid"] == "cli-proven-original-boundary"
    assert native_input.read_bytes().count(b"/compact") == 1
    assert not store.worker_path(B, "compact.json").exists()

    # The original result may later become unverifiable, not newly failed;
    # the CLI must expose the same caller-owned ID so status can reconcile.
    source.rename(source.with_suffix(".gone"))
    indeterminate = call(
        "compact", "aaaaaaaa-1234", "--operation-id", token,
        "--timeout", "1", "--json",
    )
    assert indeterminate.returncode == 6, (
        indeterminate.stdout, indeterminate.stderr,
    )
    assert token in indeterminate.stderr
    assert "compact" in indeterminate.stderr.lower()
    assert native_input.read_bytes().count(b"/compact") == 1


def test_claude_v1_compact_pending_ok_and_pure_late_status(
    generated_hook_host, short_root, monkeypatch,
):
    """Real admission, main V1 B1 DATA; strict V1 status never writes."""
    from ccx.rpc import local_rpc
    from ccx.client import CcxClient
    from ccx.ops.compact_ops import get_operation
    from ccx.errors import CompactFailed, DeliveryRejected, build_worker_api_error
    from test_claude_native_cli import _stop_disposable_original
    from test_v1_worker_protocol import request as v1_request

    store, _call, env, _hooks = generated_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", Path(env["CLAUDE_CONFIG_DIR"]))
    native = _original_native_source(store)
    native_log = Path(env["FAKE_LOG"])
    add_worker(short_root, B, "codex")

    capabilities = local_rpc.dispatch_full_uuid(
        v1_request("system.info"), store,
    )
    assert capabilities["ok"] is True
    advertised = capabilities["result"]
    assert set(advertised) == {"protocol_version", "ccx_version"}
    assert advertised["protocol_version"] == 2

    # Exact native operation identity is required during pure V1 envelope
    # validation, before any original root/owner/host effect.
    for op in ("worker.compact", "worker.compact.status"):
        missing_id = local_rpc.dispatch_full_uuid(v1_request(op, A), store)
        assert missing_id["ok"] is False
        assert missing_id["error"]["code"] == "invalid_request"

    # V1 never resolves a CLI textual prefix, even when one worker matches.
    for op in ("worker.compact", "worker.compact.status"):
        prefix = local_rpc.dispatch_full_uuid(
            v1_request(op, "aaaaaaaa-1234", operation_id="not-a-native-id"),
            store,
        )
        assert prefix["ok"] is False
        assert prefix["error"]["code"] == "invalid_request"
    assert not store.worker_path(B, "compact.json").exists()

    absent = local_rpc.dispatch_full_uuid(
        v1_request("worker.compact.status", A, operation_id="missing-id"), store,
    )
    assert absent["ok"] is True
    assert absent["result"]["status"] == "not_found"
    assert absent["result"]["recommended_action"] == "inspect"
    assert absent["result"]["compacted"] is None

    token = "v1-original-pending-native-effect"
    req = v1_request("worker.compact", A, operation_id=token)
    req["timeout_ms"] = 1750  # after genuine admission, not a 1ms IPC race
    pending = local_rpc.dispatch_full_uuid(req, store)
    assert pending["ok"] is True, pending
    row = pending["result"]
    assert row["worker"] == f"claude:{A}"
    assert row["operation_id"] == token
    assert row["status"] == "submitted" and row["recommended_action"] == "wait"
    assert row["compacted"] is False  # main V1 compact: plain boolean
    assert row["context_after"] is None
    assert row["receipt"]["operation_id"] == token
    # The V1 observation clock never shortens the native /compact pump.
    operation = get_operation(A, token)
    assert operation is not None and operation["timeout_s"] >= 300
    assert store.read_receipt(A, "compact", token)["state"] in (
        "accepted", "submitted-unknown",
    )
    end = time.monotonic() + 6
    while time.monotonic() < end:
        if native_log.exists() and b"/compact" in native_log.read_bytes():
            break
        time.sleep(.05)
    assert native_log.exists() and native_log.read_bytes().count(b"/compact") == 1

    # The already-running original host contributes main's read-only
    # compact-observe input surface: this exact operation holds the input.
    live = local_rpc.dispatch_full_uuid(
        v1_request("worker.compact.status", A, operation_id=token), store,
    )
    assert live["ok"] is True, live
    surface = live["result"]["input_surface"]
    assert surface["compaction_ownership"] == "held", surface
    assert surface["operation_id"] == token and surface["source"] == "live"
    assert native_log.read_bytes().count(b"/compact") == 1

    # Stop the disposable fake host before appending terminal-shaped evidence:
    # any stronger journal update is then attributable to the V1 observer.
    _stop_disposable_original(short_root)
    _native_append(native, _manual_command(),
                   _manual_boundary("v1-source-matched-terminal"))

    watched = (
        store.worker_path(A, "record.json"),
        store.worker_path(A, "compact.json"),
        store.receipt_path(A, "compact", token),
    )
    assert all(p.exists() for p in watched)
    snapshots = {
        str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in watched
    }

    def no_native_rpc(*_args, **_kwargs):
        pytest.fail("strict V1 compact.status cannot revive or call native RPC")

    with monkeypatch.context() as patch:
        patch.setattr('ccx.rpc.rpc._rpc', no_native_rpc)
        patch.setattr('ccx.ops.compact.ensure_ready', no_native_rpc)
        status = local_rpc.dispatch_full_uuid(
            v1_request("worker.compact.status", A, operation_id=token),
            store,
        )
    assert status["ok"] is True, status
    stronger = status["result"]
    assert stronger["status"] == "succeeded" and stronger["compacted"] is True
    assert stronger["operation_id"] == token
    assert stronger["evidence"]["event_id"] == "v1-source-matched-terminal"
    assert stronger["context_before"]["tokens_used"] == 1234
    assert stronger["context_after"]["tokens_used"] == 100
    assert stronger["input_surface"]["compaction_ownership"] == "unknown"
    assert stronger["receipt_status"] != "succeeded"
    assert all((p.read_bytes(), p.stat().st_mtime_ns) == snapshots[str(p)]
               for p in watched)
    # Python status deliberately uses the same verified evaluator with the
    # existing journal-only monotonic publication, unlike pure V1 status.
    assert CcxClient().compact_status(A, token).operation_status == "succeeded"
    assert store.worker_path(A, "compact.json").read_bytes() != snapshots[
        str(store.worker_path(A, "compact.json"))
    ][0]
    assert native_log.read_bytes().count(b"/compact") == 1

    # Main B1 no-op and confirmed failure are *adapter* mappings; these
    # checks need no second fake provider or repeated native failure matrix.
    owner = store.resolve(A)
    noop = local_rpc._rpc_compact_result(
        store, owner, token, {"operation_status": "noop", "compacted": False},
        observing=False,
    )
    assert noop["status"] == "succeeded" and noop["compacted"] is False
    assert noop["recommended_action"] == "none"

    api = build_worker_api_error(
        "Claude API unavailable", short=A[:8], kind="server_error",
        status=503, transient=True,
    )
    failed = CompactFailed(
        "proven native compact failed", short=A[:8],
        operation_id=token, api_error=api,
    )
    with monkeypatch.context() as patch:
        patch.setattr(local_rpc, "compact_worker", lambda *a, **k: (_ for _ in ()).throw(failed))
        failure = local_rpc.dispatch_full_uuid(
            v1_request("worker.compact", A, operation_id=token), store,
        )
        # Main keeps both reason= and the chained typed cause.
        with pytest.raises(DeliveryRejected) as rejected:
            local_rpc._op_worker_compact(
                {"target": A, "operation_id": token}, 1, store=store,
            )
    assert rejected.value.reason == str(failed)
    assert rejected.value.__cause__ is failed
    assert failure["ok"] is False
    assert failure["error"]["details"]["refusal"] == "compact-failed"
    assert failure["error"]["details"]["api_error"]["status"] == 503
    assert native_log.read_bytes().count(b"/compact") == 1
