"""Shared CLI vocabulary and completion-mode regressions.
Original full-UUID worker tests own identity, dispatch, stop and kill semantics.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from ccx.cli import codex as codex_commands
from ccx.ops import terminate as T
from ccx.cli.parser import build_parser
from test_notify_claude import owners

def _top_subparsers():
    return build_parser()._subparsers._group_actions[0]


# --- vocabulary -------------------------------------------------------------

def test_both_engines_share_one_root_vocabulary_no_namespace():
    root = set(_top_subparsers().choices)
    for verb in ("new", "send", "steer", "stop", "kill", "wait", "notify"):
        assert verb in root
    assert "codex" not in root


def test_kill_takes_a_target_and_a_confirmation_budget_on_both_engines():
    claude = build_parser().parse_args(["kill", "calm-shell-design-20260812"])
    codex = build_parser().parse_args(["kill", "architecture-refresh"])
    assert claude.target == "calm-shell-design-20260812"
    assert codex.target == "architecture-refresh"
    assert claude.timeout == codex.timeout == T.KILL_TIMEOUT_DEFAULT_S == 90


@pytest.mark.parametrize("argv", [
    ["send", "calm-shell-design-20260812", "go"],
    ["steer", "calm-shell-design-20260812", "go"],
    ["stop", "calm-shell-design-20260812"],
    ["kill", "calm-shell-design-20260812"],
    ["wait", "calm-shell-design-20260812"],
    ["notify", "calm-shell-design-20260812"],
    ["peek", "calm-shell-design-20260812"],
    ["compact", "calm-shell-design-20260812"],
    ["context", "calm-shell-design-20260812"],
    ["answer", "calm-shell-design-20260812", "yes"],
    ["approve", "calm-shell-design-20260812"],
    ["reject", "calm-shell-design-20260812"],
    ["attach", "calm-shell-design-20260812"],
    ["logs", "calm-shell-design-20260812"],
    ["set", "calm-shell-design-20260812", "--name", "x"],
    ["send", "architecture-refresh", "go"],
    ["steer", "architecture-refresh", "go"],
    ["stop", "architecture-refresh"],
    ["kill", "architecture-refresh"],
    ["wait", "architecture-refresh"],
    ["notify", "architecture-refresh"],
])
def test_target_taking_commands_name_their_positional_target(argv):
    """Real display names, addressed as `target` — not a short-id-shaped `short`."""
    args = build_parser().parse_args(argv)
    expected = "architecture-refresh" if "architecture-refresh" in argv else "calm-shell-design-20260812"
    assert args.target == expected
    assert getattr(args, "short", None) is None


def test_internal_relays_still_take_a_literal_short():
    """The hook/statusline/host relays are not addressed by name — ever."""
    for argv in (["hooks-relay", "ab12", "Stop"], ["statusline-relay", "ab12"],
                 ["host", "ab12"]):
        assert build_parser().parse_args(argv).short == "ab12"


# --- send / steer state contracts -------------------------------------------

def test_codex_send_rejection_guidance_matches_the_state():
    busy = codex_commands._send_rejection_guidance(
        "cx1", {"ok": False, "error": "session is busy", "state": "busy"})
    waiting = codex_commands._send_rejection_guidance(
        "cx1", {"ok": False, "error": "session is waiting", "state": "waiting"})
    assert "ccx steer cx1" in busy
    assert "ccx approve cx1" in waiting


def test_codex_busy_refusal_before_submission_says_retry_the_same_id(capsys):
    from ccx.errors import DeliveryRejected, SendIndeterminate

    refused = DeliveryRejected("worker is busy", short="cx1", state="busy",
                               delivery_id="d-1", delivery_status="prepared")
    with pytest.raises(SystemExit) as caught:
        codex_commands._fail_send_rejected(refused, "cx1")
    # A definite refusal, like Claude's busy send: exit 5, not indeterminate 6.
    assert caught.value.code == 5
    err = capsys.readouterr().err
    assert "nothing was submitted" in err
    assert "--delivery-id d-1" in err
    assert "ccx steer cx1" in err
    assert "do not resend" not in err

    unknown = SendIndeterminate("reply was interrupted", short="cx1",
                                delivery_id="d-2", delivery_status="submitted")
    with pytest.raises(SystemExit):
        codex_commands._fail_send_indeterminate(unknown, SimpleNamespace(json=False))
    assert "do not resend it" in capsys.readouterr().err


@pytest.mark.parametrize("native_state", ["busy", "waiting"])
def test_worker_facade_codex_state_refusal_keeps_native_code(
    tmp_path, monkeypatch, native_state
):
    from ccx.codex import delivery as codex_delivery
    from ccx import constants
    from ccx.codex import runtime as codex_runtime
    from ccx.constants import CODEXD_RPC_GENERATION
    from ccx.ops.creation_steps import activate_prepared_worker, prepare_worker
    from ccx.errors import DeliveryRejected, SessionWaiting
    from ccx.store.root import require_ccx_active
    from ccx.workers import WorkerClient
    from test_generation_store import B, fixture_root

    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    store = require_ccx_active(root)
    original = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)

    def fake_activation(_budget):
        activation = store.native_activation(B)
        return {
            "accepted": True, "workerId": B, "engine": "codex",
            "source": original.native_source, "submissionId": activation["submissionId"],
            "nativeEvidenceSha256": "c" * 64,
            "nativeBinding": {"threadId": "test-original-native-thread"},
        }

    activate_prepared_worker(store, original, bounded_native_submit=fake_activation)
    monkeypatch.setattr(codex_runtime, "ensure_daemon", lambda **_k: None)
    calls = []

    def fake_daemon_rpc(request, *, timeout):
        calls.append(request["op"])
        return {"transport": "served", "rpcGeneration": CODEXD_RPC_GENERATION,
                "ok": True, "loaded": True, "state": native_state}

    monkeypatch.setattr(codex_runtime, "daemon_rpc", fake_daemon_rpc)
    expected = SessionWaiting if native_state == "waiting" else DeliveryRejected
    with pytest.raises(expected) as captured:
        WorkerClient().send(B, "fresh while not idle",
                            delivery_id="facade-state-refusal", wait=False)
    error = captured.value
    refusal = error.__cause__ if native_state == "waiting" else error
    assert isinstance(refusal, DeliveryRejected)
    assert refusal.state == native_state
    assert refusal.refusal == ("worker-state" if native_state == "waiting" else "busy")
    assert refusal.native_refusal == "worker-state"
    assert refusal.delivery_id == "facade-state-refusal"
    if native_state == "waiting":
        assert error.waiting_for is None and error.pending_input is None
    # Refused at the idle load: no turn/start and no delivery receipt.
    assert calls == ["load"]
    assert codex_delivery.read_receipt("codex:" + B, "facade-state-refusal") is None
    assert store.read_receipt("codex:" + B, "codex-send", "facade-state-refusal") is None


def test_worker_facade_claude_positive_waiting_only_and_preserves_native_cause(
    owners, monkeypatch,
):
    from ccx.errors import DeliveryRejected, SessionUnavailable, SessionWaiting
    from ccx.workers import WorkerClient
    from test_generation_store import A

    _root, store = owners
    gate = {"id": "permission-wait", "tool": "PermissionPrompt",
            "message": "Allow native action?"}
    store.mutate_record("claude:" + A, patches={
        "waitingFor": gate, "lifecycle.state": "waiting",
    })
    client = WorkerClient()
    result = [DeliveryRejected(
        "native worker waiting", state="waiting", refusal="state_unavailable",
    )]

    def refusing_send(*_args, **_kwargs):
        raise result[0]

    monkeypatch.setattr(client._claude, "send", refusing_send)
    with pytest.raises(SessionWaiting) as captured:
        client.send(A, "new work", wait=False)
    waiting = captured.value
    assert waiting.waiting_for == gate
    assert waiting.pending_input is not None
    assert waiting.__cause__ is result[0]
    assert waiting.__cause__.refusal == waiting.__cause__.native_refusal == "state_unavailable"

    # The recorded gate is never sufficient to upgrade an unrelated refusal.
    result[0] = DeliveryRejected("native busy", state="busy", refusal="busy")
    with pytest.raises(DeliveryRejected) as busy:
        client.send(A, "new work", wait=False)
    assert busy.value is result[0]
    result[0] = SessionUnavailable("native unavailable")
    with pytest.raises(SessionUnavailable) as unavailable:
        client.send(A, "new work", wait=False)
    assert unavailable.value is result[0]

# --- wait defaults ----------------------------------------------------------

def test_claude_new_wait_is_unbounded_unless_timeout_given():
    unbounded = build_parser().parse_args(["new", "--engine", "claude", "hi", "--wait"])
    assert unbounded.wait is True and unbounded.timeout is None
    bounded = build_parser().parse_args(["new", "--engine", "claude", "hi", "--wait", "--timeout", "30"])
    assert bounded.timeout == 30.0


# Pre-semantic-bootstrap test test_claude_new_wait_polls_past_the_old_default replaced by worker-scoped receipt wait tests.



# Pre-semantic-bootstrap test test_claude_new_wait_respects_an_explicit_timeout replaced by worker-scoped receipt wait tests.



def test_plain_wait_stays_unbounded():
    args = build_parser().parse_args(["wait", "calm-shell-design-20260812"])
    assert args.next_turn is False
    # Unbounded by default; an explicit --timeout bounds plain wait too.
    assert args.timeout is None
    assert "default: no limit" in _timeout_help(build_parser(), "wait")


def _timeout_help(parser, command):
    sub = parser._subparsers._group_actions[0].choices[command]
    for action in sub._actions:
        if action.dest == "timeout":
            return action.help or ""
    raise AssertionError(f"{command} has no --timeout")


# --- orchestrator completion-mode parity ------------------------------------

@pytest.mark.parametrize(("wait_argv", "notify_argv"), [
    (["new", "--engine", "claude", "task", "--wait"],
     ["new", "--engine", "claude", "task", "--notify-parent"]),
    (["send", "worker", "go", "--wait"], ["send", "worker", "go", "--notify-parent"]),
    (["wait", "worker"], ["notify", "worker"]),
    (["wait", "worker", "--next-turn"], ["notify", "worker", "--next-turn"]),
    (["answer", "worker", "yes", "--wait"], ["answer", "worker", "yes", "--notify-parent"]),
    (["approve", "worker", "--wait"], ["approve", "worker", "--notify-parent"]),
    (["reject", "worker", "--wait"], ["reject", "worker", "--notify-parent"]),
    (["new", "--engine", "codex", "task", "--wait"], ["new", "--engine", "codex", "task", "--notify-parent"]),
    (["send", "worker", "go", "--wait"], ["send", "worker", "go", "--notify-parent"]),
    (["wait", "worker"], ["notify", "worker"]),
])
def test_every_worker_turn_wait_has_a_codex_notification_sibling(wait_argv, notify_argv):
    parser = build_parser()
    wait_args = parser.parse_args(wait_argv)
    notify_args = parser.parse_args(notify_argv)
    assert callable(wait_args.func)
    assert callable(notify_args.func)


def test_codex_approval_resumption_has_notification_mode():
    parser = build_parser()
    for verb in ("approve", "reject"):
        args = parser.parse_args([verb, "worker", "--notify-parent", "--settle"])
        assert args.notify_parent is True
        assert args.settle is True


def test_local_json_adapter_uses_engine_neutral_protocol_operations():
    """Public V1 must not regress into a Codex-internal adapter."""
    import inspect
    import ccx.rpc.local_rpc as local_rpc

    source = inspect.getsource(local_rpc)
    assert local_rpc.send_turn.__module__ == "ccx.ops.send"
    assert local_rpc.report_worker.__module__ == "ccx.ops.report"
    assert "from ccx.codex.ops import (" not in source
