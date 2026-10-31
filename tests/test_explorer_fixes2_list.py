"""No-wait Claude accepted receipts project the same recorded list/peek state."""
from __future__ import annotations

import json

from ccx.claude import delivery as claude_delivery
from ccx import constants
from ccx.cli.main import _main
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, add_worker, short_root


def test_list_and_peek_shared_owned_receipt_overlay(short_root, monkeypatch, capsys):
    add_worker(short_root, A, "claude")
    add_worker(short_root, B, "codex")
    store = require_ccx_active(short_root)
    store.mutate_record(A, patches={"lifecycle.state": "idle"})
    store.mutate_record(B, patches={"lifecycle.state": "idle"})
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    claude_delivery.prepare(A, "not-yet-owned", "start", mode="new_turn_only")
    _main(["list", "--json"])
    rows = json.loads(capsys.readouterr().out)
    assert rows[0]["state"] == "idle"  # prepared is not BUSY evidence

    claude_delivery.prepare(A, "turn-1", "start now", mode="new_turn_only")
    claude_delivery.mark_owned(A, "turn-1", route="new_turn")
    _main(["list", "--json"])
    rows = json.loads(capsys.readouterr().out)
    assert {r["engine"]: r["state"] for r in rows} == {
        "claude": "busy", "codex": "idle"
    }
    _main(["list"])
    printed = capsys.readouterr().out
    assert "busy" in printed and "idle" in printed
    _main(["peek", A, "--status", "--json"])
    peek = json.loads(capsys.readouterr().out)["statuses"][0]
    assert peek["state"] == "busy"

    _main(["list", "--state", "idle", "--json"])
    assert [r["worker_id"] for r in json.loads(capsys.readouterr().out)] == [B]
    _main(["list", "--sendable", "--json"])
    assert [r["worker_id"] for r in json.loads(capsys.readouterr().out)] == [B]

    stamp = claude_delivery.read_receipt(A, "turn-1")["ownedAtMs"]
    store.mutate_record(A, patches={"lastSettledAt": stamp + 1})
    _main(["list", "--json"])
    assert {r["engine"]: r["state"] for r in json.loads(capsys.readouterr().out)} == {
        "claude": "idle", "codex": "idle"
    }


def test_stop_and_crash_do_not_become_busy_from_old_receipt(short_root, monkeypatch):
    add_worker(short_root, A, "claude")
    store = require_ccx_active(short_root)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    claude_delivery.prepare(A, "turn-2", "hello", mode="new_turn_only")
    claude_delivery.mark_owned(A, "turn-2", route="new_turn")
    for status, public in (("dead", "stopped"), ("crashed", "failed")):
        store.mutate_record(A, patches={"lifecycle.state": status})
        assert store.list_workers()[0]["state"] == public
