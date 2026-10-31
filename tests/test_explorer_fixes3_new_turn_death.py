"""Confirmed old-child death resolves paced new-turn attempts without replay.

Only temporary owned fake Claude transcripts and receipts; no PTY/worker.
"""
from __future__ import annotations
import ccx.ops.wait as ops_wait

import json

import pytest

from ccx.claude import delivery as cd
from ccx.ops import common as protocol_ops
from ccx.ops import steer_ops
from ccx.store import liveness
from ccx.claude.host.session import _semantic_slot_from_receipt
from test_generation_store import A, short_root  # noqa: F401
from test_notify_claude import owners  # noqa: F401
from test_explorer_fixes2_kill_steer import _fixture


def _old_new_turn(owners, tmp_path, monkeypatch, *, marker=None):
    store, handle, path, _steer_token = _fixture(owners, tmp_path, monkeypatch)
    token = "old-pace-new-turn"
    prompt = "large inline paste before Enter " * 25
    cd.prepare(handle, token, prompt, mode="new_turn_only")
    cd.mark_owned(handle, token, route="new_turn")
    hot, created = steer_ops.begin_delivery(
        A, delivery_id=token, text=prompt, transcript=str(path),
        host_epoch="old-child-epoch", state="idle",
        requested_mode="new_turn_only", route="new_turn",
    )
    assert created and hot["status"] == "accepted"
    # The old child persisted hot/semantic MAY-BE-WRITTEN before the paced
    # writer sent prompt bytes, and its original thread was killed mid-frame.
    steer_ops.mark_pasted(A, token, offset=path.stat().st_size)
    cd.mark_attempt(handle, token, maybe_written=True)
    if marker:
        cd.mark_evidence(handle, token, evidence_status=marker, maybe_written=True)
    receipt = cd.read_receipt(handle, token)
    assert receipt["state"] == "owned" and receipt["maybeWritten"] is True
    assert _semantic_slot_from_receipt(receipt, steer_ops.get_delivery(A, token))[
        "phase"] == "await_evidence"
    return store, handle, path, token, prompt


@pytest.mark.parametrize("marker", [None, "pty-write-failed"])
def test_killed_mid_frame_or_stalled_write_before_clear_abandons_and_unblocks(
    owners, tmp_path, monkeypatch, marker,
):
    store, handle, _path, token, _prompt = _old_new_turn(
        owners, tmp_path, monkeypatch, marker=marker,
    )
    if marker:
        assert cd.read_receipt(handle, token)["evidenceStatus"] == marker
    resolved = steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start")
    assert token in resolved
    hot, receipt = steer_ops.get_delivery(A, token), cd.read_receipt(handle, token)
    assert hot["status"] == receipt["state"] == "abandoned"
    assert hot["text"] is None and receipt["promptText"] is None
    assert receipt["maybeWritten"] is False
    assert ops_wait.reconcile_turn(handle, delivery_id=token)["pending"] is False
    assert all(row["deliveryId"] != token or row["state"] != "owned"
               for row in cd.iter_receipts(handle))
    assert token not in {row["delivery_id"] for row in steer_ops.unproven_deliveries(A)}
    # On revive host recovery enumerates *owned* receipts only; the old
    # attempted paste cannot restore an await_evidence slot or block the
    # next strict idle send. A new token can durably become owned.
    new_token = "revived-next-message"
    cd.prepare(handle, new_token, "next legitimate request", mode="new_turn_only")
    cd.mark_owned(handle, new_token, route="new_turn")
    next_hot, created = steer_ops.begin_delivery(
        A, delivery_id=new_token, text="next legitimate request",
        transcript=str(_path), host_epoch="replacement-child", state="idle",
        requested_mode="new_turn_only", route="new_turn",
    )
    assert created and next_hot["status"] == "accepted"
    assert [row["deliveryId"] for row in cd.iter_receipts(handle)
            if row["state"] == "owned"] == [new_token]
    assert cd.read_receipt(handle, token)["state"] == "abandoned"
    assert cd.read_receipt(handle, new_token)["state"] == "owned"


def test_exact_ordinary_user_source_proves_started_new_turn_not_stale_reply(
    owners, tmp_path, monkeypatch,
):
    store, handle, path, token, prompt = _old_new_turn(
        owners, tmp_path, monkeypatch,
    )
    with path.open("a") as out:
        out.write(json.dumps({
            "type": "user", "uuid": "only-current-user-event",
            "message": {"content": prompt},
        }) + "\n")
    resolved = steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start")
    assert token in resolved
    hot, receipt = steer_ops.get_delivery(A, token), cd.read_receipt(handle, token)
    assert hot["status"] == receipt["state"] == "delivered"
    assert hot["evidence"] == receipt["evidence"] == "started"
    assert receipt["placement"] == "new_turn"
    assert receipt["evidenceUuid"] == "only-current-user-event"
    assert type(receipt["evidenceOffset"]) is int
    assert steer_ops.public_delivery(hot)["status"] == "started-next-turn"
    assert ops_wait.reconcile_turn(handle, delivery_id=token)["pending"] is False
    # Repeat after hot-first/semantic-second publication is idempotent.
    assert token in steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start")
    assert cd.read_receipt(handle, token)["evidenceUuid"] == "only-current-user-event"


def test_queue_dequeue_or_substring_is_not_exact_new_turn_user_proof(
    owners, tmp_path, monkeypatch,
):
    store, handle, path, token, prompt = _old_new_turn(
        owners, tmp_path, monkeypatch,
    )
    with path.open("a") as out:
        out.write(json.dumps({
            "type": "queue-operation", "operation": "dequeue",
            "content": prompt, "uuid": "unrelated-queue",
        }) + "\n")
        out.write(json.dumps({
            "type": "user", "uuid": "other-user",
            "message": {"content": "unrelated" + prompt},
        }) + "\n")
    assert token in steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start")
    assert cd.read_receipt(handle, token)["state"] == "abandoned"


def test_unproven_or_replaced_lifetime_never_resolves_new_turn(
    owners, tmp_path, monkeypatch,
):
    from ccx.rpc import rpc
    store, handle, _path, token, _prompt = _old_new_turn(
        owners, tmp_path, monkeypatch,
    )
    # The previous child died but a new child has since been installed.
    store.mutate_record(handle, patches={
        "lifecycle.workerPid": 22222, "lifecycle.workerStart": "new-live",
        "lifecycle.nativeStartProof": {
            "workerPid": 22222, "workerStart": "new-live",
        },
    })
    rpc._finalize_killed_steers(
        store, handle, child_pid=12345, child_start="old-start",
    )
    assert cd.read_receipt(handle, token)["state"] == "owned"
    assert steer_ops.get_delivery(A, token)["status"] == "pasted"


def _recover_terminal_and_accept_fresh_send(store, handle, path, token, old_slot):
    """Run actual host semantic-reconcile body against a fake recovered slot."""
    import ast
    from pathlib import Path
    from ccx.claude.host import session as host_module
    tree = ast.parse(Path(host_module.__file__).read_text())
    runner = next(n for n in tree.body
                  if isinstance(n, ast.FunctionDef) and n.name == "_run_host")
    reconcile = next(n for n in runner.body
                     if isinstance(n, ast.FunctionDef)
                     and n.name == "reconcile_semantic_delivery")
    state = {"semantic_delivery": old_slot}
    env = dict(vars(host_module))
    env.update({
        "state": state, "short": A, "broadcast": lambda *_a: None,
        "current_semantic_slot": lambda: state["semantic_delivery"],
        "read_semantic_receipt": cd.read_receipt,
    })
    exec(compile(ast.Module(body=[reconcile], type_ignores=[]),
                 "<real host recovered semantic reconciler>", "exec"), env)
    assert old_slot["phase"] == "await_evidence"
    env["reconcile_semantic_delivery"]()
    assert state["semantic_delivery"] is None
    next_token = "fresh-after-" + token
    cd.prepare(handle, next_token, "a legitimate later send", mode="new_turn_only")
    cd.mark_owned(handle, next_token, route="new_turn")
    hot, created = steer_ops.begin_delivery(
        A, delivery_id=next_token, text="a legitimate later send",
        transcript=str(path), host_epoch="new-child-epoch",
        state="idle", requested_mode="new_turn_only",
        route="new_turn",
    )
    assert created and hot["status"] == "accepted"
    assert cd.read_receipt(handle, next_token)["state"] == "owned"
    assert cd.read_receipt(handle, token)["state"] == "delivered"


def test_queued_command_generic_hot_consumed_then_kill_releases_input(
    owners, tmp_path, monkeypatch,
):
    from ccx.claude.host import session as host_module
    store, handle, path, token, prompt = _old_new_turn(
        owners, tmp_path, monkeypatch,
    )
    with path.open("a") as stream:
        stream.write(json.dumps({
            "type": "attachment", "uuid": "native-queued",
            "attachment": {"type": "queued_command", "prompt": prompt},
        }) + "\n")
    # The ACTUAL generic consumer sees substring/queue evidence and terminalizes
    # the hot delivery before the semantic new-turn ack has been written.
    hot = steer_ops.reconcile_delivery(A, token, transcript=str(path))
    assert hot["status"] == "consumed" and hot["evidence"] == "consumed"
    assert cd.read_receipt(handle, token)["state"] == "owned"
    old_slot = host_module._semantic_slot_from_receipt(
        cd.read_receipt(handle, token), hot)
    assert token in steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start")
    semantic = cd.read_receipt(handle, token)
    assert semantic["state"] == "delivered" and semantic["placement"] == "new_turn"
    assert semantic["evidence"] == "consumed"
    assert semantic["evidenceUuid"] == "native-queued"
    _recover_terminal_and_accept_fresh_send(store, handle, path, token, old_slot)


def test_hook_hot_started_before_semantic_accept_without_offset_is_delivered(
    owners, tmp_path, monkeypatch,
):
    from ccx.claude.host import session as host_module
    store, handle, path, token, _prompt = _old_new_turn(
        owners, tmp_path, monkeypatch,
    )
    # Production hook's hot-first step; no semantic acceptance yet and no
    # native transcript offset supplied with the user-prompt-submit marker.
    steer_ops.transition_delivery(
        A, token, "started", evidence="user-prompt-submit")
    hot = steer_ops.get_delivery(A, token)
    assert hot["status"] == "started" and hot["evidence_offset"] is None
    assert cd.read_receipt(handle, token)["state"] == "owned"
    old_slot = host_module._semantic_slot_from_receipt(
        cd.read_receipt(handle, token), hot)
    assert token in steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start")
    semantic = cd.read_receipt(handle, token)
    assert semantic["state"] == "delivered"
    assert semantic["placement"] == "new_turn"
    assert semantic["evidence"] == "user-prompt-submit"
    assert semantic["evidenceOffset"] is None
    _recover_terminal_and_accept_fresh_send(store, handle, path, token, old_slot)


@pytest.mark.parametrize("kind", [
    "pasted", "maybe_written", "started", "consumed", "consumed_no_offset",
])
def test_proven_dead_new_turn_never_remains_owned_for_any_hot_phase(
    owners, tmp_path, monkeypatch, kind,
):
    store, handle, path, token, prompt = _old_new_turn(
        owners, tmp_path, monkeypatch,
    )
    if kind == "maybe_written":
        cd.mark_evidence(handle, token, evidence_status="pty-write-failed",
                         maybe_written=True)
    elif kind == "started":
        steer_ops.transition_delivery(
            A, token, "started", evidence="user-prompt-submit")
    elif kind == "consumed_no_offset":
        steer_ops.transition_delivery(A, token, "consumed",
                                      evidence="generic-attachment")
    elif kind == "consumed":
        with path.open("a") as stream:
            stream.write(json.dumps({
                "type": "attachment", "uuid": "queued",
                "attachment": {"type": "queued_command", "prompt": prompt},
            }) + "\n")
        assert steer_ops.reconcile_delivery(
            A, token, transcript=str(path))["status"] == "consumed"
    assert token in steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start")
    semantic = cd.read_receipt(handle, token)
    assert semantic["state"] == (
        "abandoned" if kind in {"pasted", "maybe_written"} else "delivered")
    assert semantic["promptText"] is None and semantic["maybeWritten"] is False
    assert not any(row["state"] == "owned"
                   for row in cd.iter_receipts(handle))


def test_hot_started_exact_evidence_survives_transient_ordinary_user_scan(
    owners, tmp_path, monkeypatch,
):
    import ast
    from pathlib import Path
    from ccx.claude.host import session as H

    _store, _handle, path, token, prompt = _old_new_turn(owners, tmp_path, monkeypatch)
    with path.open('a') as stream:
        stream.write(json.dumps({
            'type': 'user', 'uuid': 'exact-native-start',
            'message': {'content': prompt},
        }) + '\n')
    end = path.stat().st_size
    assert H.exact_user_prompt_event(path, prompt)['uuid'] == 'exact-native-start'
    slot = H._semantic_slot_from_receipt(
        cd.read_receipt(A, token), steer_ops.get_delivery(A, token))
    state = {'semantic_delivery': slot, 'current': H.S.BUSY}
    scans, starts, accepted = [], [], []

    def transient_scan(*args, **kwargs):
        scans.append((args, kwargs))
        raise OSError('temporary transcript read failure')

    def ensure_turn(source, **kwargs):
        starts.append((source, kwargs))
        return 'ccx-turn:claude:v1:exact-native-start'

    # Exercise the actual host closures without launching a PTY or provider.
    tree = ast.parse(Path(H.__file__).read_text())
    runner = next(n for n in tree.body
                  if isinstance(n, ast.FunctionDef) and n.name == '_run_host')
    names = {'semantic_exact_event', 'semantic_new_turn_ref', 'reconcile_semantic_delivery'}
    bodies = [n for n in runner.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(bodies) == len(names)
    env = dict(vars(H))
    env.update({
        'state': state, 'short': A, 'event_epoch': 'old-child-epoch',
        'rec': {'transcript': str(path)},
        'current_semantic_slot': lambda: state['semantic_delivery'],
        'semantic_hot': lambda delivery_id: steer_ops.get_delivery(A, delivery_id),
        'exact_user_prompt_event': transient_scan,
        'ensure_turn_instance': ensure_turn,
        'semantic_accept': lambda slot, **evidence: accepted.append((slot, evidence)),
        'broadcast': lambda event: pytest.fail(f'unexpected host error: {event}'),
    })
    exec(compile(ast.Module(body=bodies, type_ignores=[]),
                 '<real host hot-started reconciler>', 'exec'), env)
    env['reconcile_semantic_delivery']()

    hot = steer_ops.get_delivery(A, token)
    assert hot['status'] == hot['evidence'] == 'started'
    assert len(scans) == 1
    assert starts == [('semantic-hot-started', {
        'delivery_id': token, 'report_start_offset': None,
    })]
    assert accepted == [(slot, {
        'placement': 'new_turn',
        'turn_ref': 'ccx-turn:claude:v1:exact-native-start',
        'evidence': 'started', 'evidence_uuid': 'exact-native-start',
        'evidence_offset': end,
        'native_user_event': {
            'uuid': 'exact-native-start', 'offset': end, 'kind': 'native-started-user',
        },
    })]
