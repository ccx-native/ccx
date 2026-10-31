"""MCP elicitation brokering: an MCP server's `elicitation/create` request.

Claude Code hands it to the `Elicitation` hook before showing its own dialog.
CCX brokers that hook like an AskUserQuestion — the worker goes WAITING with a
`waitingFor` of type "elicitation" — and resolves it with accept/decline/cancel
via `respond`, never via `answer` or `approve`/`reject`.
"""
from __future__ import annotations
import ccx.claude.host.events as H_events
import ccx.claude.host.hooks as H_hooks
import ccx.claude.host.subagents as H_subagents

import io
import json
from types import SimpleNamespace

import pytest

import ccx.errors as E
import ccx.ops.semantics as SEM
from ccx.constants import PERMISSION_HOOK_TIMEOUT_S, S
from ccx.types import PendingInput

from tests.test_client_api import active_input_client  # noqa: F401  (fixture)


# Payload shape of Claude Code 2.1.296's Elicitation hook input, with the
# requestedSchema the Blender MCP consent prompt sends.
CONSENT_SCHEMA = {
    "type": "object",
    "properties": {
        "consent": {
            "type": "boolean",
            "title": "Contribute session data",
            "default": False,
        },
    },
    "required": ["consent"],
}


def _hook(**extra):
    data = {
        "session_id": "s",
        "hook_event_name": "Elicitation",
        "mcp_server_name": "blender",
        "message": "Opt in?",
        "mode": "form",
        "requested_schema": CONSENT_SCHEMA,
    }
    data.update(extra)
    return data


def _waiting(**extra):
    req = H_events._elicitation_request(_hook(), "req-1")
    out = H_events._permission_preview(req)
    out.update(extra)
    return out


# --- host preview / request ---------------------------------------------------

def test_elicitation_preview_is_typed_and_keeps_schema():
    wf = _waiting()
    assert wf["type"] == "elicitation"
    assert wf["id"] == "req-1"
    assert wf["tool"] is None
    assert wf["server"] == "blender"
    assert wf["mode"] == "form"
    assert wf["message"] == "Opt in?"
    assert wf["schema"] == CONSENT_SCHEMA
    assert isinstance(wf["requested_at"], int)


def test_elicitation_preview_bounds_message():
    req = H_events._elicitation_request(_hook(message="x" * 10_000), "req-1")
    assert len(H_events._permission_preview(req)["message"]) == 4001


def test_url_mode_request_carries_url_and_elicitation_id():
    req = H_events._elicitation_request(
        _hook(mode="url", url="https://example.test/auth", elicitation_id="e-9",
              requested_schema=None),
        "req-2",
    )
    wf = H_events._permission_preview(req)
    assert (wf["mode"], wf["url"], wf["elicitation_id"], wf["schema"]) == (
        "url", "https://example.test/auth", "e-9", None)


# --- relay ------------------------------------------------------------------

# The relay only brokers for the exact owned full-UUID Claude worker whose
# native session and inherited worker stamp match the hook payload.
RELAY_WORKER = "aaaaaaaa-1234-4000-8000-000000000001"
RELAY_SESSION = "bbbbbbbb-1234-4000-8000-000000000002"


def _run_relay(monkeypatch, capsys, decision):
    seen = {}

    def fake_broker(short, data, **kw):
        seen.update(kw, data=data)
        return decision

    monkeypatch.setattr(H_hooks, "_relay_broker_permission", fake_broker)
    monkeypatch.setattr(H_hooks, "read_record", lambda short: {
        "engine": "claude", "workerId": RELAY_WORKER,
        "native": {"sessionId": RELAY_SESSION},
        "createArgs": {"skip_permissions": True},
    })
    monkeypatch.setenv("CCX_SELF_WORKER_ID", RELAY_WORKER)
    monkeypatch.setenv("CCX_SELF_ENGINE", "claude")
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(
        {**_hook(), "session_id": RELAY_SESSION})))
    H_hooks.cmd_hooks_relay(SimpleNamespace(short=RELAY_WORKER, event="Elicitation"))
    assert seen, "the patched hooks._relay_broker_permission must be called"
    return json.loads(capsys.readouterr().out), seen


def test_relay_brokers_even_on_skip_permissions_and_returns_accept(monkeypatch, capsys):
    out, seen = _run_relay(monkeypatch, capsys, {"action": "accept", "content": {"consent": False}})
    assert out == {"hookSpecificOutput": {
        "hookEventName": "Elicitation", "action": "accept", "content": {"consent": False}}}
    assert seen["register_op"] == "elicitation-request"
    assert seen["data"]["mcp_server_name"] == "blender"
    # Every failure path is cancel-shaped, never a permission deny.
    assert seen["gone"]["action"] == "cancel"
    assert seen["timed_out"]["action"] == "cancel"


@pytest.mark.parametrize("decision", [None, {"action": "bogus"}, {"decision": "deny"}])
def test_relay_falls_back_to_cancel(monkeypatch, capsys, decision):
    out, _ = _run_relay(monkeypatch, capsys, decision)
    assert out == {"hookSpecificOutput": {"hookEventName": "Elicitation", "action": "cancel"}}


def test_relay_decline_carries_no_content(monkeypatch, capsys):
    out, _ = _run_relay(monkeypatch, capsys, {"action": "decline", "content": {"x": 1}})
    assert out["hookSpecificOutput"] == {"hookEventName": "Elicitation", "action": "decline"}


def test_broker_returns_cancel_shape_when_host_gone(monkeypatch):
    replies = iter([{"ok": True, "request_id": "r1"}])
    rpc_calls = []

    def fake_rpc(short, msg, timeout=2.0, **kw):
        rpc_calls.append(msg["op"])
        if msg["op"] == "elicitation-request":
            return next(replies)
        raise OSError("gone")

    monkeypatch.setattr(H_hooks, "_host_rpc", fake_rpc)
    monkeypatch.setattr(H_hooks, "read_record", lambda short: {"lifecycle": {"hostPid": 999999}})
    monkeypatch.setattr(H_hooks, "PERMISSION_POLL_INTERVAL_S", 0)
    monkeypatch.setattr(H_hooks, "sock_path", lambda short: SimpleNamespace(exists=lambda: False))
    monkeypatch.setattr(H_hooks, "lifecycle_host_alive", lambda lifecycle: False)
    gone = {"action": "cancel", "reason": "gone"}
    assert H_hooks._relay_broker_permission(
        "abc12345", _hook(), register_op="elicitation-request", gone=gone) == gone
    assert rpc_calls, "the patched hooks._host_rpc must be called"


# --- notifications (unbrokered dialog fallback) -------------------------------

def test_notification_classifiers():
    assert H_subagents.notification_elicitation_dialog({"notification_type": "elicitation_dialog"})
    assert H_subagents.notification_elicitation_dialog({"notification_type": "elicitation_url_dialog"})
    assert not H_subagents.notification_elicitation_dialog({"notification_type": "permission_prompt"})
    assert H_subagents.notification_elicitation_settled({"notification_type": "elicitation_response"})
    assert H_subagents.notification_elicitation_settled({"notification_type": "elicitation_complete"})
    assert not H_subagents.notification_elicitation_settled(None)
    # A permission prompt still sets waiting; elicitation types do not via it.
    assert not H_subagents.notification_sets_waiting({"notification_type": "elicitation_dialog"})


# --- settings -----------------------------------------------------------------

def test_settings_register_elicitation_hook_with_broker_timeout(
    active_input_client, tmp_path, monkeypatch,
):
    import ccx.claude.settings as ST

    _client, store, worker_id = active_input_client
    # This test proves generated settings, not AF_UNIX pathname budgeting.
    monkeypatch.setattr(ST, "sock_path", lambda _wid: tmp_path / "unused.sock")
    ST.write_settings(worker_id)
    # The accepted settings document is an owner-private generation sidecar.
    doc = store.read_worker_sidecar(worker_id, "settings.json")
    assert isinstance(doc, dict)
    hook = doc["hooks"]["Elicitation"][0]["hooks"][0]
    assert hook["command"].endswith(f"hooks-relay {worker_id} Elicitation")
    assert hook["timeout"] == int(PERMISSION_HOOK_TIMEOUT_S)

# --- semantics ----------------------------------------------------------------

def test_kind_and_wrong_verbs():
    wf = _waiting()
    assert SEM.pending_input_kind(wf) == SEM.KIND_ELICITATION
    assert "use respond" in SEM.answer_illegality(wf, "abc")
    assert SEM.elicitation_illegality(wf, "abc") is None
    assert "use answer" in SEM.elicitation_illegality(
        {"tool": "AskUserQuestion", "id": "q"}, "abc")
    assert "approve/reject" in SEM.elicitation_illegality({"tool": "Bash", "id": "p"}, "abc")
    assert "no pending elicitation" in SEM.elicitation_illegality(None, "abc")
    # Observed-only dialog: visible, but only the terminal can answer it.
    assert "ccx attach abc" in SEM.elicitation_illegality(_waiting(id=None), "abc")


@pytest.mark.parametrize("action,content,problem", [
    ("accept", {"consent": False}, None),
    ("accept", {"consent": True}, None),
    ("decline", None, None),
    ("cancel", None, None),
    ("accept", {}, "missing required field(s): consent"),
    ("accept", {"consent": "no"}, "must be a boolean"),
    ("accept", {"consent": False, "extra": 1}, "unknown field(s): extra"),
    ("accept", [1], "content must be an object"),
    ("decline", {"consent": False}, "decline does not take content"),
    ("approve", None, "action must be one of"),
])
def test_response_validation_against_schema(action, content, problem):
    got = SEM.elicitation_response_problem(_waiting(), action, content)
    if problem is None:
        assert got is None
    else:
        assert problem in got


def test_response_validation_covers_enums_numbers_strings_and_lists():
    wf = _waiting(schema={
        "type": "object",
        "properties": {
            "color": {"type": "string", "enum": ["red", "blue"]},
            "size": {"type": "string", "oneOf": [{"const": "s"}, {"const": "m"}]},
            "n": {"type": "integer", "minimum": 1, "maximum": 3},
            "ratio": {"type": "number"},
            "name": {"type": "string", "minLength": 2, "maxLength": 4},
            "tags": {"type": "array", "items": {"type": "string", "enum": ["a", "b"]},
                     "maxItems": 2},
        },
    })
    ok = {"color": "red", "size": "m", "n": 2, "ratio": 0.5, "name": "abc", "tags": ["a"]}
    assert SEM.elicitation_response_problem(wf, "accept", ok) is None
    assert "one of" in SEM.elicitation_response_problem(wf, "accept", {"color": "green"})
    assert "one of" in SEM.elicitation_response_problem(wf, "accept", {"size": "xl"})
    assert ">= 1" in SEM.elicitation_response_problem(wf, "accept", {"n": 0})
    assert "integer" in SEM.elicitation_response_problem(wf, "accept", {"n": 1.5})
    assert "number" in SEM.elicitation_response_problem(wf, "accept", {"ratio": True})
    assert "at least 2" in SEM.elicitation_response_problem(wf, "accept", {"name": "a"})
    assert "outside" in SEM.elicitation_response_problem(wf, "accept", {"tags": ["c"]})
    assert "at most 2" in SEM.elicitation_response_problem(wf, "accept", {"tags": ["a", "b", "a"]})


def test_url_mode_accept_takes_no_content():
    wf = _waiting(mode="url", schema=None)
    assert SEM.elicitation_response_problem(wf, "accept", None) is None
    assert "does not take content" in SEM.elicitation_response_problem(wf, "accept", {"x": 1})


def test_field_coercion_uses_schema_types():
    wf = _waiting(schema={"type": "object", "properties": {
        "consent": {"type": "boolean"}, "n": {"type": "integer"},
        "r": {"type": "number"}, "s": {"type": "string"},
        "tags": {"type": "array", "items": {"enum": ["a", "b"]}}}})
    content, err = SEM.elicitation_field_content(
        wf, [("consent", "no"), ("n", "3"), ("r", "0.5"), ("s", "007"),
             ("tags", "a"), ("tags", "b")])
    assert err is None
    assert content == {"consent": False, "n": 3, "r": 0.5, "s": "007", "tags": ["a", "b"]}
    assert "expects true/false" in SEM.elicitation_field_content(wf, [("consent", "maybe")])[1]
    assert "expects a integer" in SEM.elicitation_field_content(wf, [("n", "x")])[1]
    assert "more than once" in SEM.elicitation_field_content(wf, [("s", "a"), ("s", "b")])[1]


# --- typed projections ----------------------------------------------------------

def test_pending_input_exposes_elicitation_fields():
    pi = PendingInput.from_waiting_for(_waiting())
    assert pi.kind == "elicitation"
    assert (pi.server, pi.mode, pi.schema, pi.request_id) == (
        "blender", "form", CONSENT_SCHEMA, "req-1")
    assert pi.questions == []
    assert pi.as_dict()["type"] == "elicitation"


def test_worker_client_pending_projection():
    from ccx.workers import _claude_pending
    pending = _claude_pending(_waiting())
    assert pending["kind"] == "elicitation"
    assert pending["request_id"] == "req-1"
    assert pending["elicitation"] == {
        "server": "blender", "mode": "form", "url": None, "schema": CONSENT_SCHEMA}


# --- CcxClient ---------------------------------------------------------------------

def _waiting_session(store, worker_id, preview=None):
    store.mutate_record(worker_id, patches={
        "waitingFor": _waiting() if preview is None else preview,
        "lifecycle.state": S.WAITING,
    })


def _patch_live(monkeypatch):
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", lambda *_a, **_k: True)


def test_respond_elicitation_sends_host_op(active_input_client, monkeypatch):
    client, store, worker_id = active_input_client
    _waiting_session(store, worker_id)
    before = store.record(worker_id)
    _patch_live(monkeypatch)
    seen = []

    def fake_rpc(target, message, **_kwargs):
        seen.append((target, message))
        return {"ok": True, "request_id": "req-1", "response_id": "resp-1"}

    monkeypatch.setattr("ccx.client._rpc", fake_rpc)
    result = client.respond_elicitation(
        worker_id, "accept", {"consent": False},
        request_id="req-1", response_id="resp-1",
    )
    assert (result.worker_id, result.engine, result.queued, result.state) == (
        worker_id, "claude", True, S.BUSY,
    )
    assert result.as_dict()["worker"] == f"claude:{worker_id}"
    assert seen == [(worker_id, {
        "op": "elicitation-response", "action": "accept",
        "request_id": "req-1", "content": {"consent": False},
        "response_id": "resp-1",
    })]
    assert store.record(worker_id) == before


def test_respond_elicitation_validates_before_rpc(active_input_client, monkeypatch):
    client, store, worker_id = active_input_client
    _waiting_session(store, worker_id)
    before = store.record(worker_id)
    _patch_live(monkeypatch)
    monkeypatch.setattr("ccx.client._rpc",
                        lambda *_a, **_k: pytest.fail("invalid input reached RPC"))
    with pytest.raises(E.InvalidRequest) as invalid:
        client.respond_elicitation(
            worker_id, "accept", {"consent": "yes"}, request_id="req-1",
        )
    assert invalid.value.short == worker_id
    with pytest.raises(E.DeliveryRejected) as stale:
        client.respond_elicitation(worker_id, "decline", request_id="stale")
    assert stale.value.reason == "request id mismatch"
    assert stale.value.short == worker_id
    assert store.record(worker_id) == before


def test_other_verbs_refuse_an_elicitation(active_input_client, monkeypatch):
    client, store, worker_id = active_input_client
    _waiting_session(store, worker_id)
    before = store.record(worker_id)
    _patch_live(monkeypatch)
    monkeypatch.setattr("ccx.client._rpc",
                        lambda *_a, **_k: pytest.fail("wrong verb reached RPC"))
    with pytest.raises(E.SessionUnavailable, match="respond_elicitation"):
        client.approve(worker_id, request_id="req-1")
    with pytest.raises(E.SessionUnavailable, match="respond"):
        client.answer(worker_id, "yes", request_id="req-1")
    assert store.record(worker_id) == before


def test_respond_elicitation_refuses_non_elicitation(active_input_client, monkeypatch):
    client, store, worker_id = active_input_client
    _waiting_session(store, worker_id, {
        "type": "permission", "tool": "Bash", "id": "p1", "input": {},
    })
    before = store.record(worker_id)
    monkeypatch.setattr("ccx.client._rpc",
                        lambda *_a, **_k: pytest.fail("wrong verb reached RPC"))
    with pytest.raises(E.SessionUnavailable, match="approve/reject"):
        client.respond_elicitation(worker_id, "cancel", request_id="p1")
    assert store.record(worker_id) == before

# --- CLI rendering ---------------------------------------------------------------------

def test_cli_renders_fields_and_exact_verbs():
    from ccx.cli.commands._common import _render_elicitation_wait_lines
    text = "\n".join(_render_elicitation_wait_lines("abc12345", _waiting()))
    assert "waiting for MCP input: blender (form)" in text
    assert "consent: boolean (default false, required) — Contribute session data" in text
    assert "ccx respond abc12345 --accept --field consent=<value> --wait" in text
    assert "ccx respond abc12345 --decline --wait" in text
    assert "ccx respond abc12345 --cancel --wait" in text


def test_cli_points_unbrokered_dialog_to_attach():
    from ccx.cli.commands._common import _render_elicitation_wait_lines
    text = "\n".join(_render_elicitation_wait_lines("abc12345", _waiting(id=None)))
    assert "ccx attach abc12345" in text
    assert "ccx respond" not in text


def test_cli_respond_parses():
    from ccx.cli.parser import build_parser
    args = build_parser().parse_args(
        ["respond", "w1", "--accept", "--field", "consent=false", "--wait"])
    assert (args.accept, args.field, args.wait) == (True, ["consent=false"], True)
    with pytest.raises(SystemExit):
        build_parser().parse_args(["respond", "w1"])  # an action is required
