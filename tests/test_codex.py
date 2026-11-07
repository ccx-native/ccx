from __future__ import annotations
from ccx.version import ccx_version
import ccx.codex.daemon.worker as daemon_worker
import os
import json
from argparse import Namespace
from types import SimpleNamespace
import pytest
from ccx.cli import codex as codex_commands
from ccx.codex import runtime as codex_runtime
from ccx.codex.daemon import daemon as codexd_module
from ccx.codex.daemon.daemon import CodexDaemon
from ccx.codex.protocol import PendingRequest, ProtocolError, ProtocolTransportError
from ccx.codex.reports import (
    merge_report_cache,
    report_from_thread,
    report_from_turn,
    should_replace_live_report,
)
from ccx.codex.provenance import (
    new_client_message_id,
    origin_of,
    parse_client_message_id,
)
from ccx.constants import CODEXD_RPC_GENERATION
from ccx.errors import InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.cli.parser import build_parser


@pytest.fixture(autouse=True)
def _unit_tests_do_not_require_codex_binary(executable_engine_stubs):
    """Fake app-server tests resolve only a harmless executable stub."""


def test_codex_new_wait_is_unbounded_unless_timeout_given():
    args = build_parser().parse_args(["new", "--engine", "codex", "hi", "--wait"])
    assert args.wait is True and args.timeout is None
    bounded = build_parser().parse_args(
        ["new", "--engine", "codex", "hi", "--wait", "--timeout", "45"])
    assert bounded.timeout == 45.0


def test_codex_kill_takes_a_target_and_a_confirmation_budget():
    args = build_parser().parse_args(["kill", "architecture-refresh"])
    assert args.target == "architecture-refresh"
    assert args.timeout == 90
    assert build_parser().parse_args(
        ["kill", "w1", "--timeout", "5"]).timeout == 5.0


def test_codex_create_full_access_defaults():
    args = build_parser().parse_args(["new", "--engine", "codex"])
    assert args.sandbox is None and args.approval_policy is None
    assert args.permissions is None  # Native core owns full posture defaults.
    assert args.idle_timeout == 1800
    assert args.model is None
    assert args.effort is None


@pytest.mark.parametrize("verb", ["new", "send", "steer"])
def test_codex_prompt_commands_accept_stdin_and_file(verb):
    target = [] if verb == "new" else ["worker"]
    engine = ["--engine", "codex"] if verb == "new" else []
    stdin_args = build_parser().parse_args([verb, *engine, *target, "--stdin"])
    file_args = build_parser().parse_args(
        [verb, *engine, *target, "--file", "prompt.md"]
    )
    assert stdin_args.stdin is True
    assert file_args.file == "prompt.md"


def test_codex_cli_wait_is_unbounded_without_explicit_timeout():
    args = build_parser().parse_args(["wait", "w", "--settle"])
    assert args.timeout is None
    assert args.wake_grace is None
    assert not hasattr(args, "heartbeat")


def _begin_now(self, method, params=None):
    """`begin_request` for a fake whose `request` answers synchronously."""
    pending = PendingRequest(method)
    try:
        result = self.request(method, params)
    except ProtocolError as error:
        pending.put(error)
    else:
        pending.put({"result": result})
    return pending


class _FakeClient:
    begin_request = _begin_now

    def __init__(self):
        self.requests = []
        self.responses = []
        self.rejections = []

    def request(self, method, params, timeout=30):
        self.requests.append((method, params))
        if method == "turn/steer":
            return {"turn": {"id": params["expectedTurnId"]}}
        return {}

    def respond(self, request_id, result):
        self.responses.append((request_id, result))

    def reject_request(self, request_id, message):
        self.rejections.append((request_id, message))


def test_codex_history_read_refuses_an_incompatible_running_daemon(
    monkeypatch, tmp_path
):
    """Observation must not quietly start a second app-server to dodge this."""
    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout=2, **_: {
        "ok": True, "state": "ready", "rpcGeneration": 0, "transport": "served"
    })
    monkeypatch.setattr(
        codex_runtime, "AppServerClient",
        lambda *a, **kw: pytest.fail("started an app-server beside codexd"),
    )

    with pytest.raises(SessionUnavailable, match="RPC generation"):
        codex_runtime.read_thread("thread-history", cwd=str(tmp_path), timeout=2,
                              include_turns=False)


def test_codex_history_read_keeps_a_broad_invalid_request_unavailable(
    monkeypatch,
):
    """Codex answers a malformed id with the same -32600 as a missing one."""
    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout=2, **_: {
        "ok": True, "state": "ready", "rpcGeneration": CODEXD_RPC_GENERATION, "transport": "served"
    })
    monkeypatch.setattr(
        codex_runtime, "daemon_rpc",
        lambda message, timeout=10: {
            "ok": False,
            "error": "thread/read failed: invalid thread id: invalid character",
            "protocolError": {
                "method": "thread/read", "code": -32600,
                "message": "invalid thread id: invalid character",
                "data": None, "requestId": 4, "transport": False,
            },
        },
    )

    with pytest.raises(SessionUnavailable):
        codex_runtime.read_thread("not-a-uuid", cwd="/tmp", timeout=2)


class _LaunchProbeClient:
    """A temporary app-server stand-in that records only its launch cwd."""

    def __init__(self, launches, cwd):
        launches.append(cwd)

    def request(self, method, params=None, timeout=30):
        if method == "thread/read":
            return {"thread": {
                "id": "thread-history",
                "cwd": "/gone/original-workspace",
                "turns": [],
            }}
        return {}

    def notify(self, method, params=None):
        return None

    def close(self):
        return None


def _record_launch(launches, kwargs):
    return _LaunchProbeClient(launches, kwargs["cwd"])


def _ready_daemon(client=None):
    daemon = CodexDaemon()
    daemon.client = client
    daemon.ready.set()
    return daemon


def test_codex_cap_admission_uses_only_fresh_duration_windows():
    daemon = _ready_daemon()
    daemon.rate_limits_cache = {"rateLimits": {
        "primary": {"windowDurationMins": 10080, "usedPercent": 67,
                    "resetsAt": 4102444800},
    }}
    daemon.rate_limits_read_at = codexd_module.time.monotonic()
    assert daemon._cap_at_admission({"7d": 67}) == ("7d", 67.0, 67.0)
    assert daemon._cap_at_admission({"7d": 68}) is None
    assert daemon._cap_at_admission({"5h": 30}) is None
    daemon.rate_limits_read_at -= codexd_module.RATE_LIMIT_CACHE_TTL_S + 1
    assert daemon._cap_at_admission({"7d": 10}) is None


def test_codex_cap_push_interrupts_only_known_active_turn_once():
    import threading
    from ccx.claude.usage import _merge_caps

    daemon = _ready_daemon()
    worker = object.__new__(daemon_worker.Worker)
    worker.lock = threading.RLock()
    worker.cap_turn_id = "turn-a"
    worker.active_caps = _merge_caps([{"5h": 50, "7d": 70},
                                      {"5h": 35, "7d": 60}])
    assert worker.active_caps == {"5h": 35, "7d": 60}
    worker.record = {"lifecycle": {"state": "busy", "activeTurnId": "turn-a"}}
    writes = []
    worker._save = lambda: writes.append(worker.record.get("quotaStop"))
    interrupted = threading.Event()
    calls = []
    worker._interrupt_turn = lambda turn_id: calls.append(turn_id) or interrupted.set()
    daemon.workers["worker"] = worker
    update = lambda pct, *, duration=10080: {"rateLimits": {
        "primary": {"windowDurationMins": duration, "usedPercent": pct,
                    "resetsAt": 4102444800},
    }}
    daemon._on_notification("account/rateLimits/updated", update(59))
    assert worker.record.get("quotaStop") is None
    daemon._on_notification("account/rateLimits/updated", {
        "rateLimits": {"limitId": "codex-spark",
                       "primary": {"windowDurationMins": 10080,
                                   "usedPercent": 99, "resetsAt": 4102444800}},
    })
    assert worker.record.get("quotaStop") is None
    daemon._on_notification("account/rateLimits/updated", update(65, duration=60))
    assert worker.record.get("quotaStop") is None
    daemon._on_notification("account/rateLimits/updated", update(60))
    assert interrupted.wait(1)
    assert calls == ["turn-a"]
    assert worker.record["quotaStop"]["turnId"] == "turn-a"
    assert "7d" in worker.record["quotaStop"]["reason"]
    daemon._on_notification("account/rateLimits/updated", update(90))
    assert calls == ["turn-a"] and len(writes) == 1

    # The stopped turn's marker cannot trip an unrelated following turn.
    worker.cap_turn_id = "turn-b"
    worker.record["lifecycle"]["activeTurnId"] = "turn-b"
    worker.active_caps = {"5h": 40}
    daemon._on_notification("account/rateLimits/updated", update(99))
    assert calls == ["turn-a"]


class _RateLimitClient:
    def __init__(self, rate_limit_reads, *, usage_response=None):
        self.rate_limit_reads = list(rate_limit_reads)
        self.usage_response = usage_response or {"summary": {}}
        self.requests = []

    def request(self, method, params=None, timeout=30):
        self.requests.append((method, params, timeout))
        if method == "account/rateLimits/read":
            if not self.rate_limit_reads:
                raise AssertionError("unexpected account/rateLimits/read")
            return self.rate_limit_reads.pop(0)
        if method == "account/usage/read":
            return self.usage_response
        raise AssertionError(method)


def _full_rate_limits_response(*, used_percent=10):
    return {
        "rateLimits": {
            "primary": {
                "usedPercent": used_percent,
                "resetsAt": 4102444800,
                "windowDurationMins": 300,
            },
            "secondary": {
                "usedPercent": 3,
                "resetsAt": 4102444800,
                "windowDurationMins": 10080,
            },
        },
        "rateLimitsByLimitId": {
            "codex": {"primary": {"usedPercent": used_percent}}
        },
        "rateLimitResetCredits": {"codex": 7},
        "unrelated": {"provider": "codex"},
    }


def test_codex_cap_cold_admission_reads_weekly_once_and_fails_open_without_five_hour():
    weekly = {"rateLimits": {
        "primary": {"usedPercent": 29, "windowDurationMins": 10080,
                    "resetsAt": 4102444800},
    }}
    client = _RateLimitClient([weekly])
    daemon = _ready_daemon(client)
    assert daemon._cap_at_admission({"7d": 20}) == ("7d", 29.0, 20.0)
    assert daemon._cap_at_admission({"5h": 1}) is None
    assert client.requests == [
        ("account/rateLimits/read", None, codexd_module.CAP_ADMISSION_READ_TIMEOUT_S),
    ]
    assert 0 < client.requests[0][2] <= 2


def test_codex_cap_cold_read_failure_fails_open_and_push_seeds_cache():
    class Unavailable:
        def __init__(self):
            self.calls = []

        def request(self, method, params=None, *, timeout):
            self.calls.append((method, timeout))
            raise TimeoutError("mocked bounded quota read timeout")

    client = Unavailable()
    daemon = _ready_daemon(client)
    assert daemon._cap_at_admission({"7d": 20}) is None
    assert client.calls == [("account/rateLimits/read",
                             codexd_module.CAP_ADMISSION_READ_TIMEOUT_S)]
    daemon._on_notification("account/rateLimits/updated", {
        "rateLimits": {"primary": {
            "usedPercent": 29, "windowDurationMins": 10080,
            "resetsAt": 4102444800,
        }},
    })
    assert daemon._cap_at_admission({"7d": 20}) == ("7d", 29.0, 20.0)
    assert client.calls == [("account/rateLimits/read",
                             codexd_module.CAP_ADMISSION_READ_TIMEOUT_S)]


def test_codexd_rate_limit_push_merges_sparse_values_without_touching_full_fields(
    monkeypatch,
):
    clock = [0.0]
    monkeypatch.setattr(codexd_module.time, "monotonic", lambda: clock[0])
    response = _full_rate_limits_response()
    response["rateLimits"]["primary"].update({
        "flag": True,
        "label": "old",
        "items": ["old"],
    })
    response["rateLimits"]["secondary"]["untouched"] = "keep"
    client = _RateLimitClient([response])
    daemon = _ready_daemon(client)
    daemon.handle({"op": "account-read", "tokens": False})

    daemon._on_notification("account/rateLimits/updated", {
        "rateLimits": {
            "primary": {
                "usedPercent": 0,
                "resetsAt": None,
                "flag": False,
                "label": "",
                "items": [],
            },
            "secondary": {"windowDurationMins": 0},
        }
    })
    result = daemon.handle({"op": "account-read", "tokens": False})["data"]

    primary = result["rateLimits"]["primary"]
    secondary = result["rateLimits"]["secondary"]
    assert primary["usedPercent"] == 0
    assert primary["resetsAt"] == 4102444800
    assert primary["windowDurationMins"] == 300
    assert primary["flag"] is False
    assert primary["label"] == ""
    assert primary["items"] == []
    assert secondary["windowDurationMins"] == 0
    assert secondary["untouched"] == "keep"
    assert result["rateLimitsByLimitId"] == response["rateLimitsByLimitId"]
    assert result["rateLimitResetCredits"] == response["rateLimitResetCredits"]
    assert result["unrelated"] == {"provider": "codex"}
    assert len(client.requests) == 1


def test_codexd_rate_limit_push_does_not_extend_full_read_freshness(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(codexd_module.time, "monotonic", lambda: clock[0])
    client = _RateLimitClient([
        _full_rate_limits_response(used_percent=10),
        _full_rate_limits_response(used_percent=20),
    ])
    daemon = _ready_daemon(client)
    daemon.handle({"op": "account-read", "tokens": False})
    clock[0] = 29.0
    daemon._on_notification("account/rateLimits/updated", {
        "rateLimits": {"primary": {"usedPercent": 11}}
    })
    clock[0] = 31.0

    result = daemon.handle({"op": "account-read", "tokens": False})

    assert result["data"]["rateLimits"]["primary"]["usedPercent"] == 20
    assert [call[0] for call in client.requests] == [
        "account/rateLimits/read",
        "account/rateLimits/read",
    ]


@pytest.mark.parametrize("window", ["primary", "secondary"])
def test_codexd_rate_limit_reset_boundary_forces_refresh(monkeypatch, window):
    clock = [0.0]
    wall_clock = [1000.0]
    monkeypatch.setattr(codexd_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(codexd_module.time, "time", lambda: wall_clock[0])
    first = _full_rate_limits_response(used_percent=10)
    first["rateLimits"][window]["resetsAt"] = 999.0
    client = _RateLimitClient([
        first,
        _full_rate_limits_response(used_percent=20),
    ])
    daemon = _ready_daemon(client)
    daemon.handle({"op": "account-read", "tokens": False})
    clock[0] = 1.0

    result = daemon.handle({"op": "account-read", "tokens": False})

    assert result["data"]["rateLimits"]["primary"]["usedPercent"] == 20
    assert len(client.requests) == 2


def test_codexd_rate_limit_cache_is_account_scoped_and_tokens_stay_direct(
    monkeypatch,
):
    clock = [0.0]
    monkeypatch.setattr(codexd_module.time, "monotonic", lambda: clock[0])
    client = _RateLimitClient(
        [_full_rate_limits_response()],
        usage_response={"tokens": 123},
    )
    daemon = _ready_daemon(client)
    daemon.handle({"op": "account-read", "tokens": False})
    cached = daemon.rate_limits_cache.copy()
    read_at = daemon.rate_limits_read_at

    result = daemon.handle({"op": "account-read", "tokens": True})

    assert result == {"ok": True, "data": {"tokens": 123}}
    assert [call[0] for call in client.requests] == [
        "account/rateLimits/read",
        "account/usage/read",
    ]
    assert daemon.rate_limits_cache == cached
    assert daemon.rate_limits_read_at == read_at
    restarted = CodexDaemon()
    assert restarted.rate_limits_cache is None
    assert restarted.rate_limits_read_at is None


def test_codexd_rate_limit_refresh_failure_preserves_structured_error(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(codexd_module.time, "monotonic", lambda: clock[0])

    class RefreshFailureClient(_RateLimitClient):
        def request(self, method, params=None, timeout=30):
            if method == "account/rateLimits/read" and self.rate_limit_reads:
                return super().request(method, params, timeout)
            self.requests.append((method, params, timeout))
            raise ProtocolError(
                "rate-limit service unavailable",
                method=method,
                code=-32000,
                data={"retry": True},
                request_id=11,
            )

    client = RefreshFailureClient([_full_rate_limits_response()])
    daemon = _ready_daemon(client)
    daemon.handle({"op": "account-read", "tokens": False})
    clock[0] = 31.0

    reply = daemon.handle({"op": "account-read", "tokens": False})

    assert reply["ok"] is False
    assert reply["error"] == (
        "account/rateLimits/read failed: rate-limit service unavailable"
    )
    assert reply["protocolError"] == {
        "method": "account/rateLimits/read",
        "code": -32000,
        "message": "rate-limit service unavailable",
        "data": {"retry": True},
        "requestId": 11,
        "transport": False,
    }


def test_codex_runtime_cold_thread_read_uses_temporary_client(
    monkeypatch, tmp_path
):
    thread = {"id": "thread-history", "turns": []}
    calls = []

    class TemporaryClient:
        def __init__(self, command, **kwargs):
            calls.append(("init", command, kwargs))

        def request(self, method, params=None, timeout=30):
            calls.append(("request", method, params))
            return {} if method == "initialize" else {"thread": thread}

        def notify(self, method, params=None):
            calls.append(("notify", method, params))

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout=2, **_: {
        "ok": False, "error": "codexd unavailable", "transport": "absent"
    })
    monkeypatch.setattr(codex_runtime, "AppServerClient", TemporaryClient)

    assert codex_runtime.read_thread(
        "thread-history", cwd=str(tmp_path), timeout=2, include_turns=False
    ) == thread
    assert [value[0] for value in calls] == [
        "init", "request", "notify", "request", "close"
    ]
    assert calls[1][1] == "initialize"
    assert calls[2][1] == "initialized"
    assert calls[3][1] == "thread/read"


def test_codex_runtime_cold_read_launches_in_an_existing_worker_cwd(
    monkeypatch, tmp_path
):
    launches = []
    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout=2, **_: {
        "ok": False, "transport": "absent"
    })
    monkeypatch.setattr(
        codex_runtime, "AppServerClient",
        lambda command, **kwargs: _record_launch(launches, kwargs),
    )

    codex_runtime.read_thread("thread-history", cwd=str(tmp_path), timeout=2,
                              include_turns=False)

    assert launches == [str(tmp_path)]


def test_codex_runtime_running_thread_read_maps_thread_not_loaded_to_not_found(
    monkeypatch,
):
    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout=2, **_: {
        "ok": True, "state": "ready", "rpcGeneration": CODEXD_RPC_GENERATION, "transport": "served"
    })
    monkeypatch.setattr(
        codex_runtime,
        "daemon_rpc",
        lambda message, timeout=10: {
            "ok": False,
            "rpcGeneration": CODEXD_RPC_GENERATION,
            "error": "thread/read failed: thread not loaded: missing-thread",
            "protocolError": {
                "method": "thread/read",
                "code": -32600,
                "message": "thread not loaded: missing-thread",
                "data": None,
                "requestId": 4,
                "transport": False,
            },
        },
    )

    with pytest.raises(SessionNotFound):
        codex_runtime.read_thread(
            "missing-thread", cwd="/tmp", timeout=2
        )


def test_codex_runtime_cold_thread_read_maps_thread_not_loaded_to_not_found(
    monkeypatch, tmp_path
):
    class MissingClient:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, method, params=None, timeout=30):
            if method == "thread/read":
                raise ProtocolError(
                    "thread/read failed: thread not loaded: missing-thread"
                )
            return {}

        def notify(self, method, params=None):
            pass

        def close(self):
            pass

    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout=2, **_: {
        "ok": False, "transport": "absent"
    })
    monkeypatch.setattr(codex_runtime, "AppServerClient", MissingClient)

    with pytest.raises(SessionNotFound):
        codex_runtime.read_thread(
            "missing-thread", cwd=str(tmp_path), timeout=2
        )


def test_codex_runtime_cold_thread_read_keeps_transport_failure_unavailable(
    monkeypatch, tmp_path
):
    class BrokenClient:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, method, params=None, timeout=30):
            if method == "thread/read":
                raise ProtocolError("connection reset by peer")
            return {}

        def notify(self, method, params=None):
            pass

        def close(self):
            pass

    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout=2, **_: {
        "ok": False, "transport": "absent"
    })
    monkeypatch.setattr(codex_runtime, "AppServerClient", BrokenClient)

    with pytest.raises(SessionUnavailable):
        codex_runtime.read_thread(
            "thread-1", cwd=str(tmp_path), timeout=2
        )


def test_codexd_rejects_server_requests_for_unowned_threads(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("CCX_ROOT", str(tmp_path))
    client = _FakeClient()
    daemon = _ready_daemon(client)

    daemon._on_server_request(
        "req-9",
        "item/commandExecution/requestApproval",
        {"threadId": "foreign-thread", "command": "rm -rf /"},
    )

    assert client.rejections[0][0] == "req-9"
    assert client.rejections[0][1] == (
        "CCX has no handler for app-server request "
        "item/commandExecution/requestApproval"
    )


def test_codex_report_selection_prefers_final_answer_over_commentary():
    thread = {
        "turns": [{
            "id": "turn-1",
            "status": "completed",
            "items": [
                {"type": "agentMessage", "id": "item-commentary",
                 "text": "progress", "phase": "commentary"},
                {"type": "agentMessage", "id": "item-final",
                 "text": "answer", "phase": "final_answer"},
            ],
        }],
    }

    assert report_from_thread(thread) == {
        "text": "answer",
        "turnId": "turn-1",
        "itemId": "item-final",
        "phase": "final_answer",
        "completedAt": None,
    }


def test_codex_report_selection_keeps_phase_less_compatibility_fallback():
    turn = {
        "id": "turn-1",
        "status": "completed",
        "items": [
            {"type": "agentMessage", "id": "item-commentary",
             "text": "progress", "phase": "commentary"},
            {"type": "agentMessage", "id": "item-legacy", "text": "answer"},
        ],
    }

    assert report_from_turn(turn) == {
        "text": "answer",
        "turnId": "turn-1",
        "itemId": "item-legacy",
        "phase": None,
        "completedAt": None,
    }


def test_codex_report_selection_ignores_commentary_only_turn():
    turn = {
        "id": "turn-1",
        "status": "completed",
        "items": [{
            "type": "agentMessage",
            "id": "item-commentary",
            "text": "progress",
            "phase": "commentary",
        }],
    }

    assert report_from_turn(turn) is None
    assert report_from_thread({"turns": [turn]}) is None


def test_codex_historical_report_uses_native_turn_completion_timestamp():
    turn = {
        "id": "turn-1",
        "status": "completed",
        "completedAt": 1780000001.25,
        "items": [{"type": "agentMessage", "id": "item-1", "text": "answer"}],
    }

    assert report_from_turn(turn)["completedAt"] == 1780000001250


def test_codex_historical_report_cache_merge_matches_final_text_across_item_ids():
    existing = {
        "text": "LIVE_REPORT", "turnId": "turn-1", "itemId": "msg_live",
        "phase": "final_answer", "completedAt": 1234567890123,
    }
    candidate = {
        "text": "LIVE_REPORT", "turnId": "turn-1", "itemId": "item-2",
        "phase": "final_answer", "completedAt": 1234567890000,
    }

    merged = merge_report_cache(existing, candidate)

    assert merged["completedAt"] == existing["completedAt"]
    assert merged["itemId"] == "item-2"
    assert merged["itemId"] != existing["itemId"]


@pytest.mark.parametrize(
    "candidate",
    [
        {
            "text": "OTHER_REPORT", "turnId": "turn-1", "itemId": "item-2",
            "phase": "final_answer", "completedAt": 456,
        },
        {
            "text": "LIVE_REPORT", "turnId": "turn-2", "itemId": "item-2",
            "phase": "final_answer", "completedAt": 456,
        },
        {
            "text": "LIVE_REPORT", "turnId": "turn-1", "itemId": "item-2",
            "phase": None, "completedAt": 456,
        },
    ],
)
def test_codex_historical_report_cache_merge_rejects_ambiguous_equivalence(
    candidate,
):
    existing = {
        "text": "LIVE_REPORT", "turnId": "turn-1", "itemId": "msg_live",
        "phase": "final_answer", "completedAt": 123,
    }

    assert merge_report_cache(existing, candidate) == candidate


def test_codex_historical_report_cache_merge_does_not_claim_legacy_identity():
    existing = {
        "text": "old", "turnId": "turn-1", "itemId": None,
        "phase": None, "completedAt": 123,
    }
    candidate = {
        "text": "new", "turnId": "turn-1", "itemId": None,
        "phase": None, "completedAt": 456,
    }

    assert merge_report_cache(existing, candidate) == candidate


def test_codex_live_report_selection_does_not_downgrade_final_to_legacy():
    current = {
        "text": "answer",
        "turnId": "turn-1",
        "itemId": "item-final",
        "phase": "final_answer",
    }
    legacy = {
        "text": "late provider text",
        "turnId": "turn-1",
        "itemId": "item-legacy",
        "phase": None,
    }

    assert should_replace_live_report(current, legacy) is False


def test_codex_report_selection_keeps_older_report_when_new_turn_is_commentary_only():
    thread = {
        "turns": [
            {
                "id": "turn-old",
                "status": "completed",
                "items": [{
                    "type": "agentMessage", "id": "item-old",
                    "text": "answer", "phase": "final_answer",
                }],
            },
            {
                "id": "turn-new",
                "status": "completed",
                "items": [{
                    "type": "agentMessage", "id": "item-progress",
                    "text": "progress", "phase": "commentary",
                }],
            },
        ],
    }

    assert report_from_thread(thread)["turnId"] == "turn-old"
    assert report_from_thread(thread)["itemId"] == "item-old"


def test_codex_model_list_reads_every_page_from_short_lived_appserver(
    monkeypatch, tmp_path
):
    # Use the autouse fake executable instead of a nonexistent /opt path.
    instances = []

    class ModelClient:
        def __init__(self, command, **kwargs):
            self.command = command
            self.requests = []
            self.notifications = []
            self.closed = False
            instances.append(self)

        def request(self, method, params=None, timeout=30):
            self.requests.append((method, params, timeout))
            if method == "initialize":
                return {"userAgent": "codex"}
            if params == {}:
                return {
                    "data": [
                        {"id": "gpt-5.6-sol", "model": "gpt-5.6-sol"},
                        {"id": "gpt-5.6-terra", "model": "gpt-5.6-terra"},
                    ],
                    "nextCursor": "next-page",
                }
            return {
                "data": [
                    {"id": "gpt-5.6-luna", "model": "gpt-5.6-luna"},
                ],
                "nextCursor": None,
            }

        def notify(self, method, params=None):
            self.notifications.append((method, params))

        def reject_request(self, request_id, message):
            raise AssertionError((request_id, message))

        def close(self):
            self.closed = True

    monkeypatch.setattr(codex_runtime, "AppServerClient", ModelClient)
    result = codex_runtime.validate_model(
        "gpt-5.6-luna", cwd=str(tmp_path), timeout=7
    )

    assert result == ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"]
    client = instances[0]
    assert client.command == [os.environ["CCX_CODEX_BIN"], "app-server", "--stdio"]
    assert client.requests[-2:] == [
        ("model/list", {}, 7),
        ("model/list", {"cursor": "next-page"}, 7),
    ]
    assert client.closed is True


@pytest.mark.parametrize(
    ("path", "expected_method", "expected_title"),
    [
        ("models", "model/list", "CCX Codex model validation"),
        ("usage", "account/rateLimits/read", "CCX Codex usage"),
        ("thread", "thread/read", "CCX Codex history read"),
    ],
)
def test_cold_codex_paths_share_stable_appserver_lifecycle(
    monkeypatch, tmp_path, path, expected_method, expected_title
):
    monkeypatch.setenv("CCX_ROOT", str(tmp_path))
    # Use the autouse fake executable instead of a nonexistent /opt path.
    monkeypatch.chdir(tmp_path)
    instances = []

    class LifecycleClient:
        def __init__(self, command, **kwargs):
            self.command = command
            self.kwargs = kwargs
            self.requests = []
            self.notifications = []
            self.rejections = []
            self.closed = False
            instances.append(self)

        def request(self, method, params=None, timeout=30):
            self.requests.append((method, params, timeout))
            if method == "initialize":
                return {"userAgent": "codex"}
            self.kwargs["on_server_request"](
                "request-1", "future/request", {"future": True}
            )
            if method == "model/list":
                return {"data": [{"model": "gpt-5.6-luna"}], "nextCursor": None}
            if method == "account/rateLimits/read":
                return {"rateLimits": {"primary": {"usedPercent": 12}}}
            return {"thread": {"id": "thread-history", "turns": []}}

        def notify(self, method, params=None):
            self.notifications.append((method, params))

        def reject_request(self, request_id, message):
            self.rejections.append((request_id, message))

        def close(self):
            self.closed = True

    monkeypatch.setattr(codex_runtime, "AppServerClient", LifecycleClient)
    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout=2, **_: {
        "ok": False, "transport": "absent"
    })

    if path == "models":
        result = codex_runtime.list_models(cwd=str(tmp_path), timeout=7)
    elif path == "usage":
        result = codex_runtime.read_account_usage(timeout=7)
    else:
        result = codex_runtime.read_thread(
            "thread-history", cwd=str(tmp_path), timeout=7, include_turns=False
        )

    client = instances[0]
    assert result
    assert client.command == [os.environ["CCX_CODEX_BIN"], "app-server", "--stdio"]
    assert [method for method, _params, _timeout in client.requests] == [
        "initialize", expected_method
    ]
    initialize = client.requests[0][1]
    assert initialize["clientInfo"] == {
        "name": "ccx",
        "title": expected_title,
        "version": ccx_version(),
    }
    assert initialize["capabilities"] == {"experimentalApi": False}
    assert client.notifications == [("initialized", None)]
    rejection_prefix = {
        "models": "unsupported request during model list read",
        "usage": "unsupported account request during usage read",
        "thread": "unsupported request during history read",
    }[path]
    assert client.rejections == [
        ("request-1", f"{rejection_prefix}: future/request")
    ]
    assert client.closed is True


@pytest.mark.parametrize("path", ["models", "usage", "thread"])
def test_cold_codex_paths_close_short_lived_client_on_failure(
    monkeypatch, tmp_path, path
):
    monkeypatch.setenv("CCX_ROOT", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    instances = []

    class BrokenClient:
        def __init__(self, command, **kwargs):
            self.closed = False
            instances.append(self)

        def request(self, method, params=None, timeout=30):
            if method == "initialize":
                return {}
            raise RuntimeError("operation failed")

        def notify(self, method, params=None):
            pass

        def close(self):
            self.closed = True

    monkeypatch.setattr(codex_runtime, "AppServerClient", BrokenClient)
    monkeypatch.setattr(codex_runtime, "daemon_status", lambda timeout=2, **_: {
        "ok": False, "transport": "absent"
    })

    expected = RuntimeError if path == "thread" else SessionUnavailable
    with pytest.raises(expected):
        if path == "models":
            codex_runtime.list_models(cwd=str(tmp_path))
        elif path == "usage":
            codex_runtime.read_account_usage()
        else:
            codex_runtime.read_thread(
                "thread-history", cwd=str(tmp_path), timeout=2
            )
    assert instances[0].closed is True


def test_codex_model_validation_reports_suggestion_and_complete_list(
    monkeypatch
):
    monkeypatch.setattr(
        codex_runtime,
        "list_models",
        lambda **kwargs: [
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
        ],
    )
    with pytest.raises(InvalidRequest) as caught:
        codex_runtime.validate_model("luna", cwd="/tmp")
    message = str(caught.value)
    assert "Did you mean 'gpt-5.6-luna'?" in message
    assert message.endswith(
        "Valid models: gpt-5.6-sol, gpt-5.6-terra, gpt-5.6-luna"
    )


def test_codex_cli_usage_json_preserves_protocol_payload(
    monkeypatch, capsys
):
    payload = {"summary": {"lifetimeTokens": 1234}}
    monkeypatch.setattr(
        codex_runtime,
        "read_account_usage",
        lambda **kwargs: payload,
    )
    codex_commands.cmd_usage(Namespace(tokens=True, json=True))
    assert json.loads(capsys.readouterr().out) == payload


def test_a_failing_completion_callback_never_escapes(capsys):
    pending = PendingRequest("thread/unsubscribe")
    pending.add_done_callback(lambda _answered: 1 / 0)

    pending.put({"result": {}})

    assert pending.result() == {}
    assert "completion callback failed" in capsys.readouterr().err


def test_codexd_treats_a_latched_reader_failure_as_fatal():
    """A dead reader with a live app-server must not leave codexd healthy."""
    class _Client:
        def __init__(self):
            self.process = SimpleNamespace(poll=lambda: None, pid=7)
            self.failure = None

    daemon = CodexDaemon()
    assert daemon.runtime_failure() is None
    daemon.client = _Client()
    assert daemon.runtime_failure() is None

    daemon.client.failure = ProtocolTransportError(
        "invalid JSON from app-server stdout: 'x'"
    )
    assert "invalid JSON" in daemon.runtime_failure()


def test_codexd_still_treats_app_server_exit_as_fatal():
    class _Client:
        def __init__(self):
            self.process = SimpleNamespace(poll=lambda: 1, pid=7)
            self.failure = None

    daemon = CodexDaemon()
    daemon.client = _Client()
    assert daemon.runtime_failure() == "Codex app-server exited"


def test_codex_client_message_ids_parse_conservatively():
    """Only attributable CCX-native v2 IDs, plus read-only historical v1."""
    worker_id = "aaaaaaaa-1234-4000-8000-000000000001"
    mine = new_client_message_id(worker_id, "start")
    parsed = parse_client_message_id(mine)
    assert parsed == {
        "workerId": worker_id,
        "kind": "start",
        "uuid": mine.rsplit(":", 1)[1],
    }
    assert origin_of(mine) == "ccx-start"
    assert origin_of(new_client_message_id(worker_id, "steer")) == "ccx-steer"
    assert "private user prompt" not in mine

    tail = mine.rsplit(":", 1)[1]
    for foreign in (
        None,
        "",
        b"bytes",
        123,
        "user_message_1",                              # another client's scheme
        f"vscode:ccx:v2:{worker_id}:start:{tail}",    # ours embedded in theirs
        mine.upper(),                                 # noncanonical case
        mine + ":extra",
        f"ccx:v3:{worker_id}:start:{tail}",           # an unrecognized version
        f"ccx:v2:abc12345:start:{tail}",              # v2 must have full owner
        f"ccx:v2:{worker_id}:compact:{tail}",         # never minted by CCX
        f"ccx:v2:{worker_id.upper()}:start:{tail}",   # uppercase owner
        f"ccx:v2:{worker_id}:start:not-a-uuid",
        f"ccx:v2:{worker_id}:start:{tail.replace('-', '')}",
        f"ccx:v1:abc12345:compact:{tail}",            # malformed historical v1
        f"ccx:v1:ABC12345:start:{tail}",
        "ccx:v1:abc12345:start:not-a-uuid",
        f"ccx:v1:abc12345:start:{tail.replace('-', '')}",
    ):
        assert parse_client_message_id(foreign) is None, foreign
        assert origin_of(foreign) is None, foreign

    with pytest.raises(ValueError):
        new_client_message_id(worker_id, "compact")  # validates kind, not owner
    with pytest.raises(ValueError):
        new_client_message_id("abc12345", "start")   # no new short-keyed minting
    with pytest.raises(ValueError):
        new_client_message_id("not-a-short", "start")
