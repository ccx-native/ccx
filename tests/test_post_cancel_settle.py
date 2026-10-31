"""Provider-free regression: accepted delivery must settle after native Stop.

The on-disk M1 S28 journal showed turn_start without delivery_id, followed by
semantic_accepted on that *same* turn_ref, then turn_complete without
semantic_settled. Reproduce that exact ordering in the real disposable host
using one fake Claude process and generated (authenticated) native hooks.
"""
from __future__ import annotations
import ccx.claude.host.successor as H_successor

import json
from pathlib import Path
import shlex
import sys
import time

import pytest

from ccx import constants
from ccx.claude.host import session as H
from ccx.claude import transcript as T
from ccx.claude.delivery import read_receipt
from ccx.store.root import require_ccx_active
from test_claude_native_cli import _stop_disposable_original
from test_create_cross_surface import _first_wait_original_claude_ready
from test_generation_store import A, short_root


def _accepted_original_turn():
    """One positively matched original transcript user event in this host epoch."""
    state = {
        "current": H.S.BUSY,
        "active_turn_ref": "ccx-turn:claude:v1:original-turn",
        "active_turn_delivery_id": None,
    }
    receipt = {
        "deliveryId": "one-native-delivery",
        "state": "accepted",
        "route": "new_turn",
        "placement": "new_turn",
        "turnRef": state["active_turn_ref"],
        "evidence": "user-event",
        "evidenceUuid": "original-user-event-uuid",
        "evidenceOffset": 1234,
    }
    evidence = {
        "turn_ref": receipt["turnRef"],
        "native_user_event": {"uuid": receipt["evidenceUuid"], "offset": 1234},
        "delivery_host_epoch": "exact-current-host-epoch",
        "current_host_epoch": "exact-current-host-epoch",
    }
    return state, receipt, evidence


def test_late_positive_native_event_attaches_only_unowned_active_new_turn():
    state, receipt, evidence = _accepted_original_turn()
    assert H_successor.bind_accepted_original_new_turn(state, receipt, **evidence) is True
    assert state["active_turn_delivery_id"] == receipt["deliveryId"]
    # After attachment, the same event cannot cause another state mutation.
    assert H_successor.bind_accepted_original_new_turn(state, receipt, **evidence) is False


@pytest.mark.parametrize("route,placement", [
    ("steer", "current_turn"),
    ("steer", "new_turn"),  # steer can fall back to the successor turn
])
def test_steer_acceptance_never_claims_active_turn_delivery(route, placement):
    state, receipt, evidence = _accepted_original_turn()
    receipt.update(route=route, placement=placement)
    assert H_successor.bind_accepted_original_new_turn(state, receipt, **evidence) is False
    assert state["active_turn_delivery_id"] is None


def test_existing_active_delivery_is_not_overwritten_by_late_acceptance():
    state, receipt, evidence = _accepted_original_turn()
    state["active_turn_delivery_id"] = "another-original-delivery"
    assert H_successor.bind_accepted_original_new_turn(state, receipt, **evidence) is False
    assert state["active_turn_delivery_id"] == "another-original-delivery"


def test_other_active_turn_cannot_claim_accepted_original_ref():
    state, receipt, evidence = _accepted_original_turn()
    state["active_turn_ref"] = "ccx-turn:claude:v1:new-successor-turn"
    assert H_successor.bind_accepted_original_new_turn(state, receipt, **evidence) is False
    assert state["active_turn_delivery_id"] is None


def test_normal_known_delivery_at_turn_start_remains_unchanged():
    state, receipt, evidence = _accepted_original_turn()
    state["active_turn_delivery_id"] = receipt["deliveryId"]
    assert H_successor.bind_accepted_original_new_turn(state, receipt, **evidence) is False
    assert state["active_turn_ref"] == receipt["turnRef"]
    assert state["active_turn_delivery_id"] == receipt["deliveryId"]


@pytest.mark.parametrize("mutation", [
    "old_host_epoch", "missing_native_event", "foreign_native_uuid",
    "foreign_native_offset", "unaccepted_receipt", "non_native_evidence",
    "missing_delivery_id", "idle_host",
])
def test_no_native_current_generation_proof_means_no_active_turn_binding(mutation):
    state, receipt, evidence = _accepted_original_turn()
    if mutation == "old_host_epoch":
        evidence["delivery_host_epoch"] = "stale-host-epoch"
    elif mutation == "missing_native_event":
        evidence["native_user_event"] = None
    elif mutation == "foreign_native_uuid":
        evidence["native_user_event"] = {"uuid": "different", "offset": 1234}
    elif mutation == "foreign_native_offset":
        evidence["native_user_event"] = {"uuid": "original-user-event-uuid", "offset": 4321}
    elif mutation == "unaccepted_receipt":
        receipt["state"] = "owned"
    elif mutation == "non_native_evidence":
        receipt["evidence"] = "user-prompt-submit"
    elif mutation == "missing_delivery_id":
        receipt["deliveryId"] = None
    elif mutation == "idle_host":
        state["current"] = H.S.IDLE
    assert H_successor.bind_accepted_original_new_turn(state, receipt, **evidence) is False
    assert state["active_turn_delivery_id"] is None


@pytest.mark.parametrize("kind,allowed", [
    ("ordinary-user", True),
    ("native-paste", True),
    ("prose-wrapped-user", False),
    ("meta-user", False),
    ("queued-user", False),
    ("dequeued-operation", False),
    ("queued-attachment", False),
    ("assistant", False),
    ("unrelated-user", False),
])
def test_hot_started_claim_requires_reverified_original_ordinary_user_row(
    tmp_path, kind, allowed,
):
    """Only an exact ordinary-user row can bind hot evidence to Stop proof."""
    original = "unique original native instruction"
    native_event = {
        "type": "user", "uuid": "native-user-uuid",
        "message": {"content": original},
    }
    if kind == "native-paste":
        native_event["message"]["content"] = (
            '<pasted_content id="native">\n' + original
            + '\n</pasted_content id="native">'
        )
    elif kind == "prose-wrapped-user":
        native_event["message"]["content"] = "Provider wrapper: " + original + " (end)"
    elif kind == "meta-user":
        native_event["isMeta"] = True
    elif kind == "queued-user":
        native_event["attachments"] = [{"type": "queued_command"}]
    elif kind == "dequeued-operation":
        native_event.update(type="queue-operation", operation="dequeue",
                            content=original)
    elif kind == "queued-attachment":
        native_event.update(type="attachment", attachment={
            "type": "queued_command", "content": original,
        })
    elif kind == "assistant":
        native_event["type"] = "assistant"
    elif kind == "unrelated-user":
        native_event["message"]["content"] = "different unrelated user message"
    source = tmp_path / "fake-original-native-source.jsonl"
    source.write_text(json.dumps(native_event) + "\n")
    offset = source.stat().st_size
    proof = T.original_started_user_event(
        source, original, after_offset=0,
        evidence_uuid="native-user-uuid", evidence_offset=offset,
    )
    assert (proof is not None) is allowed
    state, receipt, evidence = _accepted_original_turn()
    receipt.update(evidence="started", evidenceUuid="native-user-uuid",
                   evidenceOffset=offset)
    evidence["native_user_event"] = proof
    assert H_successor.bind_accepted_original_new_turn(state, receipt, **evidence) is allowed
    assert (state["active_turn_delivery_id"] == receipt["deliveryId"]) is allowed


@pytest.mark.parametrize("wrong", ["uuid", "offset", "fence", "text"])
def test_hot_started_native_claim_must_match_same_uuid_byte_fence_and_prompt(
    tmp_path, wrong,
):
    source = tmp_path / "native.jsonl"
    source.write_text(json.dumps({
        "type": "user", "uuid": "native-bound-uuid",
        "message": {"content": "unique delivery"},
    }) + "\n")
    end = source.stat().st_size
    kwargs = dict(after_offset=0, evidence_uuid="native-bound-uuid",
                  evidence_offset=end)
    text = "unique delivery"
    if wrong == "uuid":
        kwargs["evidence_uuid"] = "other-event"
    elif wrong == "offset":
        kwargs["evidence_offset"] = end + 1
    elif wrong == "fence":
        kwargs["after_offset"] = end
    else:
        text = "different delivery"
    assert T.original_started_user_event(source, text, **kwargs) is None


@pytest.mark.parametrize("mutation", [
    "missing_kind", "old_epoch", "steer_current", "steer_fallback",
    "occupied_original", "other_ref", "wrong_receipt_uuid", "wrong_offset",
])
def test_started_native_proof_never_overwrites_foreign_or_steer_turn(mutation):
    state, receipt, evidence = _accepted_original_turn()
    receipt["evidence"] = "started"
    evidence["native_user_event"]["kind"] = "native-started-user"
    if mutation == "missing_kind":
        del evidence["native_user_event"]["kind"]
    elif mutation == "old_epoch":
        evidence["delivery_host_epoch"] = "another-host-epoch"
    elif mutation == "steer_current":
        receipt.update(route="steer", placement="current_turn")
    elif mutation == "steer_fallback":
        receipt["route"] = "steer"
    elif mutation == "occupied_original":
        state["active_turn_delivery_id"] = "other-original-id"
    elif mutation == "other_ref":
        state["active_turn_ref"] = "ccx-turn:claude:v1:other-active"
    elif mutation == "wrong_receipt_uuid":
        receipt["evidenceUuid"] = "different-native-uuid"
    elif mutation == "wrong_offset":
        receipt["evidenceOffset"] = 5678
    before = state["active_turn_delivery_id"]
    assert H_successor.bind_accepted_original_new_turn(state, receipt, **evidence) is False
    assert state["active_turn_delivery_id"] == before


@pytest.mark.parametrize("native_evidence", ["exact", "native-paste"])
def test_generic_turn_start_then_late_exact_semantic_accept_must_settle_on_stop(
    short_root, tmp_path, monkeypatch, native_evidence,
):
    """An original Stop must not lose the accepted delivery on a pre-minted turn."""
    from ccx import CcxClient

    repo = Path(__file__).resolve().parent.parent
    original = (repo / "tests" / "fake_claude.py").read_text()
    ups_anchor = '\n        hook("UserPromptSubmit", {"prompt": text})\n        native_row('
    stop_anchor = '            time.sleep(0.3)\n            hook("Stop", {'
    assert original.count(ups_anchor) == original.count(stop_anchor) == 1
    # One controlled original-provider race: a UPS arrives (and legitimately
    # mints a generic turn) before the exact native JSONL user row is visible.
    # The fake later writes the *actual* semantic text to the native transcript
    # and the host positively accepts the same delivery on that generic ref.
    modified = original.replace(
        ups_anchor,
        '\n        hook("UserPromptSubmit", {"prompt": '
        '(text + " [fake pre-transcript hook drift]" if "SETTLE_RACE" in text else text)})\n        native_row(',
    ).replace(
        stop_anchor,
        '            if "SETTLE_RACE" in text:\n'
        '                until = time.monotonic() + 15\n'
        '                while not Path(os.environ["FAKE_STOP_GATE"]).exists() and time.monotonic() < until:\n'
        '                    time.sleep(0.025)\n'
        '            else:\n'
        '                time.sleep(0.3)\n'
        '            hook("Stop", {',
    )
    fake = tmp_path / "native-provider-settle-race.py"
    fake.write_text(modified)
    wrapper = tmp_path / "fake-native-claude"
    wrapper.write_text(
        "#!/bin/sh\nexec " + shlex.quote(sys.executable) + " "
        + shlex.quote(str(fake)) + ' "$@"\n'
    )
    wrapper.chmod(0o700)
    gate = tmp_path / "permit-exact-original-stop"
    for key, value in {
        "CCX_ROOT": str(short_root), "CCX_CLAUDE_BIN": str(wrapper),
        "CCX_SPAWN_SCOPE": "0",
        "CCX_BOOT_TIMEOUT_S": "15", "PYTHONPATH": str(repo),
        "CLAUDE_CONFIG_DIR": str(tmp_path / "private-fake-provider-home"),
        "FAKE_HOOKS_VIA_SETTINGS": "1", "FAKE_STOP_GATE": str(gate),
        "FAKE_LOG": str(tmp_path / "test-owned-pty-input.log"),
    }.items():
        monkeypatch.setenv(key, value)
    for key in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(wrapper))
    store = require_ccx_active(short_root)
    events_path = store.pointer.generation_root / "workers" / "claude" / A / "events.ndjson"

    def events():
        if not events_path.exists():
            return []
        return [json.loads(line) for line in events_path.read_text().splitlines()]

    token = "post-cancel-SETTLE_RACE-delivery"
    worker = f"claude:{A}"
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        delivered = CcxClient().send(worker, "SETTLE_RACE exact original reply", wait=False,
                                     delivery_id=token)
        assert delivered.delivery_id == token
        deadline = time.monotonic() + 12
        start = None
        while time.monotonic() < deadline:
            rows = events()
            starts = [r for r in rows if r.get("type") == "turn_start"]
            if starts:
                start = starts[-1]
                break
            time.sleep(0.04)
        assert start, [r.get("type") for r in events()]
        assert start.get("delivery_id") is None, start
        # The genuine provider JSONL is written only *after* the upstream UPS
        # hook. Supply that same ordered evidence as a disposable fake row on
        # the exact original native source that CCX pinned in its record.
        native = Path(store.record(A)["transcript"])
        native.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        native_text = "SETTLE_RACE exact original reply"
        if native_evidence == "native-paste":
            # Recognized native paste envelopes retain exact prompt identity.
            native_text = (
                '<pasted_content id="native">\n' + native_text
                + '\n</pasted_content id="native">'
            )
        with native.open("a", encoding="utf-8") as target:
            target.write(json.dumps({
                "type": "user", "uuid": "fake-native-user-event-after-generic-ups",
                "sessionId": store.record(A)["native"]["sessionId"],
                "message": {"content": native_text},
            }) + "\n")
        deadline = time.monotonic() + 10
        accepted = None
        while time.monotonic() < deadline:
            accepts = [r for r in events() if r.get("type") == "semantic_accepted"
                       and r.get("delivery_id") == token]
            if accepts:
                accepted = accepts[-1]
                break
            time.sleep(0.04)
        assert accepted, [(r.get("type"), r.get("source")) for r in events()]
        assert start["turn_ref"] == accepted["turn_ref"]
        assert accepted["evidence"] == "user-event", accepted
        receipt = read_receipt(worker, token)
        assert receipt["state"] == "accepted" and receipt["turnRef"] == start["turn_ref"]

        # Release exactly one provider-issued, session-bound Stop only AFTER
        # the host has positively accepted the delivery on the generic turn.
        gate.write_text("release\n")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if any(r.get("type") == "turn_complete"
                   and r.get("turn_ref") == start["turn_ref"] for r in events()):
                break
            time.sleep(0.04)
        assert any(r.get("type") == "turn_complete"
                   and r.get("turn_ref") == start["turn_ref"] for r in events())
        assert store.record(A)["lastSettledTurn"]["turnRef"] == start["turn_ref"]
        # Red on 24bc3d6: generic UPS left active_turn_delivery_id=None;
        # Stop correctly completed the turn but skipped semantic settlement.
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and read_receipt(worker, token).get("settlement") is None:
            time.sleep(0.04)
        assert read_receipt(worker, token).get("settlement") is not None, (
            "positive original Stop did not settle its already accepted delivery",
            [(r.get("type"), r.get("delivery_id")) for r in events()],
        )
    finally:
        gate.write_text("release\n")
        _stop_disposable_original(short_root)
