"""A slow codexd makes presence checks wait, never refuse early or misjudge."""

import socket

import pytest

from ccx import constants
from ccx.codex import runtime as codex_runtime
from ccx.codex.daemon.daemon import CodexDaemon
from ccx.errors import SessionUnavailable
from test_generation_store import short_root  # noqa: F401 - real ACTIVE fixture

READY = {"ok": True, "transport": "served", "state": "ready",
         "rpcGeneration": codex_runtime.CODEXD_RPC_GENERATION}
TIMEOUT = {"ok": False, "error": "codexd unavailable: TimeoutError",
           "transport": "inconclusive", "retry": True}


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


@pytest.fixture
def probes(monkeypatch):
    """Fake codexd: replies are popped in order; a timeout uses its whole budget."""
    clock = _Clock()
    monkeypatch.setattr(codex_runtime, "time", clock)
    state = {"replies": [], "calls": [], "clock": clock}

    def fake_rpc(message, timeout=10, **_):
        state["calls"].append((message, timeout))
        if timeout <= 0:
            return {"ok": False, "error": "codexd unavailable: TimeoutError",
                    "transport": "inconclusive"}
        reply = state["replies"].pop(0) if state["replies"] else TIMEOUT
        if reply is TIMEOUT:
            clock.now += timeout
        return dict(reply)

    monkeypatch.setattr(codex_runtime, "daemon_rpc", fake_rpc)
    return state


def test_preflight_asks_again_after_a_slow_reply_instead_of_refusing(probes):
    probes["replies"] = [TIMEOUT, TIMEOUT, READY]
    deadline = probes["clock"].now + 60

    assert codex_runtime.preflight_daemon(deadline) is None
    assert len(probes["calls"]) == 3
    # Presence checks do not ask for every worker's state.
    assert all(message == {"op": "daemon-status", "workers": False}
               for message, _ in probes["calls"])


def test_presence_that_never_settles_refuses_at_the_deadline_with_its_evidence(
    probes, monkeypatch
):
    sent = []
    monkeypatch.setattr(codex_runtime, "daemon_call",
                        lambda *a, **k: sent.append(a) or {"ok": True})
    clock = probes["clock"]
    deadline = clock.now + 12

    with pytest.raises(SessionUnavailable) as refusal:
        codex_runtime.rpc("worker", {"op": "approve"}, timeout=12)

    assert sent == []
    assert clock.now == deadline  # waited the whole budget, never past it
    assert all(timeout <= codex_runtime.PRESENCE_PROBE_S
               for _, timeout in probes["calls"])
    assert str(refusal.value).startswith(
        "codexd presence is inconclusive (TimeoutError after 3 probes over 12.0s)"
        "; nothing was sent")


def test_an_expired_deadline_sends_no_probe(probes):
    deadline = probes["clock"].now - 1

    with pytest.raises(SessionUnavailable):
        codex_runtime.preflight_daemon(deadline)

    assert [timeout for _, timeout in probes["calls"]] == [0]


def test_a_failed_identity_proof_is_not_retried(probes):
    probes["replies"] = [{"ok": False, "transport": "inconclusive",
                          "error": "codexd unavailable: TransportError"}, READY]

    with pytest.raises(SessionUnavailable, match=r"\(TransportError\)"):
        codex_runtime.preflight_daemon(probes["clock"].now + 60)

    assert len(probes["calls"]) == 1


def test_slow_presence_never_starts_a_second_daemon(probes, monkeypatch):
    def no_spawn(*_a, **_k):
        raise AssertionError("an inconclusive probe must not reach the spawn lock")

    monkeypatch.setattr(codex_runtime, "daemon_spawn_lock", no_spawn)
    monkeypatch.setattr(codex_runtime, "ensure_dirs", lambda: None)

    with pytest.raises(SessionUnavailable,
                       match="; refusing to start a second daemon"):
        codex_runtime.ensure_daemon(deadline=probes["clock"].now + 20)


def test_without_a_deadline_presence_waits_the_default_budget(probes):
    probes["replies"] = [TIMEOUT, TIMEOUT, READY]

    assert codex_runtime.settled_daemon_status(None)["state"] == "ready"
    probes["replies"] = []
    start = probes["clock"].now
    probe = codex_runtime.settled_daemon_status(None)
    assert probe["transport"] == "inconclusive"
    assert probes["clock"].now - start == pytest.approx(codex_runtime.CODEXD_STOP_WAIT_S)


@pytest.mark.parametrize("error", [socket.timeout(), TimeoutError(), EOFError()])
def test_timeouts_and_a_closed_reply_are_retryable_but_never_absence(error):
    failure = codex_runtime._transport_failure(error)
    assert failure["transport"] == "inconclusive"
    assert failure["retry"] is True
    assert "transitional" not in failure  # that one lets ensure_daemon proceed


def test_presence_status_skips_worker_locks(monkeypatch, short_root):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setenv("CCX_ROOT", str(short_root))
    daemon = CodexDaemon()

    class _LockedWorker:
        def state(self):
            raise AssertionError("presence must not wait on a worker lock")

    daemon.workers["w"] = _LockedWorker()
    status = daemon.handle({"op": "daemon-status", "workers": False})
    assert status["ok"] is True and status["state"] == "booting"
    assert status["workers"] is None

    daemon.workers["w"] = type("Idle", (), {"state": lambda self: "idle"})()
    assert daemon.handle({"op": "daemon-status"})["workers"] == {"w": "idle"}


def test_an_expired_deadline_never_opens_a_socket(monkeypatch):
    def no_socket(*_a, **_k):
        raise AssertionError("an expired presence probe must not connect")

    monkeypatch.setattr(codex_runtime.socket, "socket", no_socket)
    probe = codex_runtime.settled_daemon_status(codex_runtime.time.monotonic() - 1)
    assert probe["transport"] == "inconclusive" and "retry" not in probe
