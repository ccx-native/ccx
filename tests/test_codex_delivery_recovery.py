"""Lost/late Codex send ACKs recover only from exact journaled native evidence.

Real codexd + the disposable fake app-server from test_steer_codex, extended
with a control file: the native ``userMessage`` item (carrying CCX's own
``clientId``) can arrive before or after the JSON-RPC response, the response
can be delayed past the caller's 1 s dispatch budget or codexd's 2 s native
timeout, or dropped; the evidence itself can be altered. No live provider.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from ccx.codex import delivery as codex_delivery
from ccx import constants
from ccx.errors import SendIndeterminate
from ccx.ops.wait import reconcile_turn
from ccx.ops.send import send_turn
from test_generation_store import A, B, short_root  # noqa: F401 (fixture)
import test_steer_codex
from test_steer_codex import _hold_busy, original_codex  # noqa: F401 (fixture)

_FAKE_WITH_STEER = test_steer_codex._fake_with_steer


_CONTROL_HELPERS = r"""
CTL=TRACE+'.ctl'
def _ctl():
    try:
        with open(CTL,encoding='utf8') as handle: return json.load(handle)
    except Exception:
        return {}
def _note(method,params):
    print(json.dumps({'jsonrpc':'2.0','method':method,'params':params}),flush=True)
def _user_message(tid,turn_id,params,ctl,text):
    if not ctl.get('emit_message',True): return
    cid=ctl.get('client_id',params.get('clientUserMessageId'))
    if ctl.get('client_id_from_file'):
        cid=open(ctl['client_id_from_file'],encoding='utf8').read().strip()
    copies=2 if ctl.get('duplicate_item') else 1
    items=[{'type':ctl.get('item_type','userMessage'),'id':'um-%s-%d-%d'%(turn_id,len(text),n),
            'content':[{'type':'text','text':text}],'clientId':cid} for n in range(copies)]
    for method in ('item/started','item/completed'):
        for item in items:
            _note(method,{'threadId':ctl.get('thread',tid),
                          'turnId':ctl.get('turn',turn_id),'item':item})
def _reply(msg_id,result,ctl):
    if ctl.get('drop_response'): return
    if ctl.get('response_delay'): time.sleep(ctl['response_delay'])
    print(json.dumps({'jsonrpc':'2.0','id':msg_id,'result':result}),flush=True)
"""


def _fake_with_recovery_controls(path: Path, trace: Path) -> Path:
    executable = _FAKE_WITH_STEER(path, trace)
    script = executable.read_text()
    old = "HELD_APPROVAL_TURNS={}"
    assert script.count(old) == 1
    script = script.replace(old, old + "\n" + _CONTROL_HELPERS, 1)
    # turn/steer: the item and the response in a controlled order.
    old = """            if prompt.startswith('finish-original-'):"""
    assert script.count(old) == 1
    script = script.replace(old, """            ctl=_ctl()
            if ctl.get('controlled'):
                if ctl.get('order')=='message-first':
                    _user_message(tid,params.get('expectedTurnId'),params,ctl,prompt)
                    _reply(msg['id'],{},ctl)
                else:
                    _reply(msg['id'],{},ctl)
                    _user_message(tid,params.get('expectedTurnId'),params,ctl,prompt)
                continue
            if prompt.startswith('finish-original-'):""", 1)
    # turn/start: turn/started + item and the response in a controlled order.
    old = "            result={'turn':{'id':turn_id,'status':'inProgress'}}"
    assert script.count(old) == 1
    script = script.replace(old, old + """
            ctl=_ctl()
            if ctl.get('controlled'):
                def _notes():
                    _note('turn/started',{'threadId':tid,'turn':{'id':turn_id}})
                    _user_message(tid,turn_id,params,ctl,text)
                if ctl.get('order')=='message-first':
                    _notes(); _reply(msg['id'],result,ctl)
                else:
                    _reply(msg['id'],result,ctl); _notes()
                if not text.startswith('steer-hold-') and not ctl.get('hold'):
                    _note('turn/completed',{'threadId':tid,'turn':turn})
                continue""", 1)
    executable.write_text(script)
    executable.chmod(0o700)
    return executable


@pytest.fixture
def codex(monkeypatch, request, tmp_path):
    """original_codex (real codexd, two bound workers) on the controlled fake."""
    monkeypatch.setattr(test_steer_codex, "_fake_with_steer", _fake_with_recovery_controls)
    store, trace = request.getfixturevalue("original_codex")
    return store, trace


def control(trace: Path, **values) -> None:
    Path(str(trace) + ".ctl").write_text(json.dumps({"controlled": True, **values}))


def uncontrolled(trace: Path) -> None:
    Path(str(trace) + ".ctl").unlink(missing_ok=True)


def until(predicate, seconds=6.0, step=0.05):
    deadline = time.monotonic() + seconds
    value = predicate()
    while not value and time.monotonic() < deadline:
        time.sleep(step)
        value = predicate()
    return value


def journaled_user_messages(worker) -> list[tuple[str, str]]:
    """(turnId, clientId) of every journaled native userMessage item."""
    from ccx.codex import evidence as codex_evidence
    found = []
    for event in codex_evidence.read_journal(worker).events:
        item = (event.get("params") or {}).get("item")
        if isinstance(item, dict) and item.get("type") == "userMessage":
            found.append((event.get("turnId"), item.get("clientId")))
    return found


def lost_send(worker, text, token, *, mode):
    """The REAL V2 send path; its answer is lost (caller budget or native timeout)."""
    with pytest.raises(SendIndeterminate):
        send_turn(worker, text, delivery_id=token, mode=mode)


# -- reproduction: positive evidence exists, receipt stays submitted ----------

@pytest.mark.parametrize("order", ["message-first", "response-first"])
def test_lost_steer_ack_is_recovered_from_exact_journaled_user_message(codex, order):
    store, trace = codex
    r = _hold_busy(store, trace)
    token = f"lost-steer-{order}"
    # Past the caller's 1 s dispatch budget, inside codexd's 2 s native timeout.
    control(trace, order=order, response_delay=1.5)
    lost_send(B, "late steer lands", token, mode="steer_only")
    assert trace.read_text().count("RPC turn/steer") == 1
    client = codex_delivery.read_receipt(B, token)["clientUserMessageId"]
    assert until(lambda: (r, client) in journaled_user_messages(B))
    accepted = until(lambda: codex_delivery.read_receipt(B, token)["state"] == "accepted")
    receipt = codex_delivery.read_receipt(B, token)
    assert accepted, receipt
    assert receipt["turnId"] == r and receipt["placement"] == "current_turn"
    assert receipt.get("settlement") is None  # Placement, never completion.
    result = reconcile_turn(B, delivery_id=token)
    assert result["status"] == "accepted" and result["pending"] is False
    assert trace.read_text().count("RPC turn/steer") == 1


def test_dropped_new_turn_ack_recovers_and_settles_by_exact_turn(codex):
    store, trace = codex
    token = "lost-start-dropped"
    control(trace, order="response-first", drop_response=True)
    lost_send(B, "dropped start completes", token, mode="new_turn_only")
    assert trace.read_text().count("RPC turn/start") == 1
    client = codex_delivery.read_receipt(B, token)["clientUserMessageId"]
    assert until(lambda: any(c == client for _t, c in journaled_user_messages(B)))
    settled = until(lambda: isinstance(
        codex_delivery.read_receipt(B, token).get("settlement"), dict))
    receipt = codex_delivery.read_receipt(B, token)
    assert settled, receipt
    turn_id = receipt["turnId"]
    assert receipt["placement"] == "new_turn"
    by_turn = reconcile_turn(B, turn_id=turn_id)
    assert by_turn["status"] == "settled"
    assert by_turn["delivery"]["delivery_id"] == token
    assert store.read_receipt(B, "codex-send", token)["state"] == "accepted"
    assert trace.read_text().count("RPC turn/start") == 1


def wakes(worker) -> list[dict]:
    """Journaled delivery wake lines, with their sequence."""
    from ccx.codex import evidence as codex_evidence
    return [{"sequence": e.get("sequence"), **(e.get("params") or {})}
            for e in codex_evidence.read_journal(worker).events
            if e.get("method") == codex_evidence.DELIVERY_CHANGED_METHOD]


def user_message_sequence(worker, client) -> int | None:
    from ccx.codex import evidence as codex_evidence
    for event in codex_evidence.read_journal(worker).events:
        item = (event.get("params") or {}).get("item")
        if isinstance(item, dict) and item.get("clientId") == client:
            return event.get("sequence")
    return None


@pytest.mark.parametrize("order", ["message-first", "response-first"])
def test_native_timeout_new_turn_recovers_in_codexd_and_wakes_after_acceptance(codex, order):
    """Answer past codexd's own 2 s native timeout: the late reply is discarded."""
    store, trace = codex
    token = f"late-start-{order}"
    control(trace, order=order, response_delay=2.6)
    lost_send(B, "native timeout start", token, mode="new_turn_only")
    assert until(lambda: isinstance(
        codex_delivery.read_receipt(B, token).get("settlement"), dict), seconds=8)
    receipt = codex_delivery.read_receipt(B, token)
    lines = until(lambda: [w for w in wakes(B) if w["deliveryId"] == token])
    assert lines and lines[0]["state"] == "accepted"
    assert lines[0]["turnRef"] == receipt["turnRef"]
    assert "native timeout start" not in json.dumps(lines)  # Content-free.
    assert lines[0]["sequence"] > user_message_sequence(B, receipt["clientUserMessageId"])
    assert trace.read_text().count("RPC turn/start") == 1


def test_late_ack_within_budget_after_codexd_recovered_still_answers_accepted(codex):
    """message-first and an in-budget answer: codexd's resolver can accept
    before the caller's own acknowledgment; the caller must not call that a
    different native identity."""
    store, trace = codex
    r = _hold_busy(store, trace)
    token = "race-steer-in-budget"
    control(trace, order="message-first", response_delay=0.6)
    result = send_turn(B, "racing steer", delivery_id=token, mode="steer_only")
    assert result["status"] == "accepted", result
    assert result["delivery"]["turn_id"] == r
    assert store.read_receipt(B, "codex-send", token)["state"] == "accepted"
    assert trace.read_text().count("RPC turn/steer") == 1


def test_killed_caller_process_mid_dispatch_is_recovered_by_codexd(codex, tmp_path):
    store, trace = codex
    r = _hold_busy(store, trace)
    token = "killed-caller-steer"
    control(trace, order="response-first", response_delay=1.8)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(constants.CCX_ROOT), "PYTHONPATH": str(repo)}
    for name in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(name, None)
    caller = subprocess.Popen(
        [sys.executable, "-B", "-c",
         "import sys; from ccx.ops.send import send_turn; send_turn(sys.argv[1], 'killed caller steer', delivery_id=sys.argv[2], mode='steer_only')", B, token],
        env=env, cwd=str(repo), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        assert until(lambda: "RPC turn/steer" in trace.read_text())
        caller.kill()  # Exactly the caller this test started.
        caller.communicate(timeout=5)
    finally:
        if caller.poll() is None:
            caller.kill()
            caller.communicate(timeout=5)
    assert until(lambda: codex_delivery.read_receipt(B, token)["state"] == "accepted", seconds=8)
    receipt = codex_delivery.read_receipt(B, token)
    assert receipt["turnId"] == r and receipt.get("settlement") is None
    assert trace.read_text().count("RPC turn/steer") == 1


def test_event_driven_reader_never_sleeps_past_the_acceptance_write(codex):
    """A subscriber that only reacts to events (reading the receipt after
    each page, with no resolver of its own) always reaches the final answer:
    the last event it needs is written after acceptance."""
    from ccx.ops import worker_events
    store, trace = codex
    r = _hold_busy(store, trace)
    cursor = worker_events.read_full_uuid_event_page(store, f"codex:{B}", tail=0)["next_cursor"]
    token = "event-driven-steer"
    control(trace, order="message-first", response_delay=1.5)
    lost_send(B, "observed only by events", token, mode="steer_only")
    seen = []
    for _ in range(20):
        page = worker_events.wait_full_uuid_event_page(
            store, f"codex:{B}", after=cursor, wait=5.0)
        assert not page["timed_out"], ("slept without a wake", seen,
                                       codex_delivery.read_receipt(B, token))
        cursor = page["next_cursor"]
        seen += [e.get("kind") for e in page["events"]]
        if codex_delivery.read_receipt(B, token)["state"] == "accepted":
            break
    assert codex_delivery.read_receipt(B, token)["state"] == "accepted"
    assert "delivery.changed" in seen
    events = worker_events.read_full_uuid_event_page(
        store, f"codex:{B}", tail=20, kinds=["delivery.changed"])["events"]
    projected = [e for e in events if e.get("delivery_id") == token]
    assert projected and projected[0]["status"] == "accepted"
    assert projected[0]["route"] == "steer" and projected[0]["placement"] == "current_turn"
    assert projected[0]["turn_ref"] == f"ccx-turn:codex:v1:{r}"
    assert "observed only by events" not in json.dumps(projected)


@pytest.mark.parametrize("evidence", [
    "absent", "foreign_worker_client", "unknown_client", "duplicate_items",
    "wrong_turn", "not_user_message",
])
def test_codexd_never_recovers_from_missing_or_mismatched_evidence(codex, evidence):
    store, trace = codex
    r = _hold_busy(store, trace)
    token = f"negative-{evidence}"
    overrides = {
        "absent": {"emit_message": False},
        "foreign_worker_client": {"client_id":
            f"ccx:v2:{A}:steer:00000000-0000-4000-8000-000000000001"},
        "unknown_client": {"client_id":
            f"ccx:v2:{B}:steer:00000000-0000-4000-8000-000000000002"},
        "duplicate_items": {"duplicate_item": True},
        "wrong_turn": {"turn": "native-fake-turn-elsewhere"},
        "not_user_message": {"item_type": "agentMessage"},
    }[evidence]
    control(trace, order="message-first", response_delay=1.5, **overrides)
    lost_send(B, f"negative {evidence}", token, mode="steer_only")
    time.sleep(2.5)  # Past the late reply and any recovery attempt.
    receipt = codex_delivery.read_receipt(B, token)
    assert receipt["state"] == "submitted", receipt
    assert store.read_receipt(B, "codex-send", token)["state"] == "submitted-unknown"
    assert not [w for w in wakes(B) if w["deliveryId"] == token]
    # Restart reconciliation reaches the same conservative answer.
    pending = reconcile_turn(B, delivery_id=token)
    assert pending["status"] == "submitted" and pending["pending"] is True
    assert pending["recommended_action"] == "wait"
    assert trace.read_text().count("RPC turn/steer") == 1
    uncontrolled(trace)
