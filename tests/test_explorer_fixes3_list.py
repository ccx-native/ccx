"""Cheap list overlay screening must keep real pending Claude sends visible."""
from __future__ import annotations

import json
import os
import time

from ccx.claude import delivery as claude_delivery
from ccx import constants
from ccx.store import store as generation
from ccx.store.root import require_ccx_active
from test_generation_store import A, add_worker, short_root, privdir  # noqa: F401


def test_old_settled_receipts_parsed_and_new_owned_busy_is_still_visible(
    short_root, monkeypatch,
):
    add_worker(short_root, A, "claude")
    store = require_ccx_active(short_root)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    now = int(time.time() * 1000)
    settlement_floor = now - 120_000
    store.mutate_record(A, patches={
        "lifecycle.state": "idle", "lastSettledAt": settlement_floor,
    })
    receipt_dir = (store.pointer.generation_root
                   / "semantic-deliveries" / "claude" / A)
    privdir(receipt_dir.parent.parent)
    privdir(receipt_dir.parent)
    privdir(receipt_dir)
    for i in range(30):
        entry = receipt_dir / f"old-{i}.json"
        entry.write_text(json.dumps({
            "workerId": A, "engine": "claude", "generation": store.generation,
            "state": "accepted", "requestedMode": "new_turn_only",
            "ownedAtMs": now - 250_000, "settlement": {
                "status": "completed", "text": "a previous finished turn",
            },
        }))
        entry.chmod(0o600)
        os.utime(entry, ns=(int((now - 240_000) * 1e6),) * 2)
    handle = "claude:" + A
    reads = []
    original = generation._read_json
    def tracked(fd, name, *args, **kwargs):
        if name.startswith("old-"):
            reads.append(name)
        return original(fd, name, *args, **kwargs)
    monkeypatch.setattr(generation, "_read_json", tracked)
    # Age cannot hide a receipt. Even very old archive files are opened:
    # correctness takes priority over the old mtime prefilter.
    assert store.list_workers()[0]["state"] == "idle"
    assert len(reads) == 30
    claude_delivery.prepare(handle, "active-turn", "real ongoing prompt",
                            mode="new_turn_only")
    claude_delivery.mark_owned(handle, "active-turn", route="new_turn")
    rows = store.list_workers()
    assert len(rows) == 1 and rows[0]["state"] == "busy"
    assert store.list_workers(state="idle") == []
    # Once the exact new-turn timestamp is no longer newer than last
    # settlement, the same still-owned receipt cannot make IDLE busy.
    current = claude_delivery.read_receipt(handle, "active-turn")
    store.mutate_record(handle, patches={
        "lastSettledAt": current["ownedAtMs"] + 1,
    })
    assert store.list_workers()[0]["state"] == "idle"


def test_untrusted_future_settlement_mtime_does_not_hide_owned_receipt(
    short_root, monkeypatch,
):
    add_worker(short_root, A, "claude")
    store = require_ccx_active(short_root)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    handle = "claude:" + A
    future = int(time.time() * 1000) + 120_000
    store.mutate_record(A, patches={
        "lifecycle.state": "idle", "lastSettledAt": future,
    })
    semantic_dir = store.pointer.generation_root / "semantic-deliveries" / "claude" / A
    privdir(semantic_dir.parent.parent)
    privdir(semantic_dir.parent)
    privdir(semantic_dir)
    path = semantic_dir / "future.json"
    path.write_text(json.dumps({
        "workerId": A, "engine": "claude", "generation": store.generation,
        "state": "owned", "requestedMode": "new_turn_only",
        "ownedAtMs": future + 1, "settlement": None,
    }))
    path.chmod(0o600)
    os.utime(path, ns=(int((future - 300_000) * 1e6),) * 2)
    assert store.list_workers()[0]["state"] == "busy"

def test_backdated_mtime_cannot_hide_a_logically_pending_new_turn(short_root, monkeypatch):
    add_worker(short_root, A, "claude")
    store = require_ccx_active(short_root)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    handle = "claude:" + A
    now = int(time.time() * 1000)
    floor = now - 10_000
    store.mutate_record(A, patches={
        "lifecycle.state": "idle", "lastSettledAt": floor,
    })
    claude_delivery.prepare(handle, "after-clock-step", "still pending",
                            mode="new_turn_only")
    claude_delivery.mark_owned(handle, "after-clock-step", route="new_turn")
    receipt = claude_delivery.read_receipt(handle, "after-clock-step")
    assert receipt["ownedAtMs"] > floor and receipt["state"] == "owned"
    assert store.list_workers()[0]["state"] == "busy"
    path = claude_delivery.receipt_path(handle, "after-clock-step")
    os.utime(path, ns=((floor - 120_000) * 1_000_000,) * 2)
    # Deliberately wrong filesystem age cannot override an ownedAtMs > floor.
    assert store.list_workers()[0]["state"] == "busy"
    assert store.list_workers(state="idle") == []
