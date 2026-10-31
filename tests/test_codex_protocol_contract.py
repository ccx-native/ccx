import importlib
import json
import os
import socket
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx.codex.protocol import ProtocolError
from ccx.codex.daemon import daemon as codexd_module
from ccx.codex.schema import (
    CRITICAL_CLIENT_METHODS,
    CRITICAL_CLIENT_NOTIFICATIONS,
    CRITICAL_FIELDS,
    CRITICAL_SERVER_NOTIFICATIONS,
    CRITICAL_THREAD_ITEM_TYPES,
    REFERENCE_CODEX_SCHEMA_VERSION,
    REFERENCE_SCHEMA_DIR,
    REFERENCE_SCHEMA_FILES,
    critical_schema_projection,
    missing_critical_schema_facts,
    protocol_summary,
    semantic_schema_diff,
    validate_reference_schema,
)
from ccx.codex.daemon.daemon import CodexDaemon
from ccx.ops.conversation import build_codex
from ccx import constants
from test_generation_store import short_root  # noqa: F401 - real ACTIVE fixture


SNAPSHOT = REFERENCE_SCHEMA_DIR


@pytest.fixture
def active_daemon_root(monkeypatch, short_root):
    # Starting an app-server now resolves the sealed daemon transport root.
    # Keep provider readiness under test without bypassing ACTIVE ownership.
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setenv("CCX_ROOT", str(short_root))


def _copy_reference_aggregates(candidate: Path) -> None:
    candidate.mkdir()
    for filename in REFERENCE_SCHEMA_FILES:
        shutil.copy2(SNAPSHOT / filename, candidate / filename)


def test_reference_schema_path_is_explicit():
    assert REFERENCE_CODEX_SCHEMA_VERSION == "0.147.0"
    assert SNAPSHOT == Path(__file__).parent / "protocol" / "codex" / "0.147.0"


def test_codex_protocol_contract_module_is_removed():
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("ccx.codex_protocol_contract")


class _FakeAppServer:
    def __init__(self, command, **kwargs):
        self.command = command
        self.kwargs = kwargs
        self.process = SimpleNamespace(pid=4321, poll=lambda: None)
        self.failure = None
        self.notifications = []
        if path := kwargs.get("socket_path"):
            Path(path).parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as bound:
                bound.bind(path)
            os.chmod(path, 0o666)

    def request(self, method, params=None, timeout=30):
        assert method == "initialize"
        return {"userAgent": "fake-codex"}

    def notify(self, method, params=None):
        self.notifications.append((method, params))


def test_future_version_does_not_gate_appserver_start_before_ready_publication(
    monkeypatch, capsys, active_daemon_root
):
    monkeypatch.setattr(
        codexd_module, "_probe_codex_version", lambda _: "codex-cli 99.0.0"
    )
    monkeypatch.setattr(codexd_module, "AppServerClient", _FakeAppServer)
    daemon = CodexDaemon()

    daemon._start_client()

    # Generation 12 publishes ready only after boot-time orphan reconciliation.
    # A successful app-server initialize is necessary but no longer sufficient.
    assert not daemon.ready.is_set()
    assert isinstance(daemon.client, _FakeAppServer)
    # The native Mac listener need not respect our umask on bind.
    assert os.stat(daemon.app_server_path).st_mode & 0o777 == 0o600
    status = daemon.handle({"op": "daemon-status"})
    assert status["state"] == "booting"
    assert status["codexVersion"] == "codex-cli 99.0.0"
    assert "codexCompatibility" not in status
    assert not status["diagnostics"]
    assert "untested" not in capsys.readouterr().err.lower()


def test_failed_version_probe_does_not_gate_working_appserver(monkeypatch, active_daemon_root):
    monkeypatch.setattr(codexd_module, "_probe_codex_version", lambda _: None)
    monkeypatch.setattr(codexd_module, "AppServerClient", _FakeAppServer)
    daemon = CodexDaemon()

    daemon._start_client()

    assert not daemon.ready.is_set()
    assert isinstance(daemon.client, _FakeAppServer)
    assert daemon.codex_version is None


def test_mac_native_symlink_endpoint_is_accepted_without_following_target(
    monkeypatch, active_daemon_root,
):
    class SymlinkAppServer(_FakeAppServer):
        def __init__(self, command, **kwargs):
            path = Path(kwargs["socket_path"])
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            native = path.with_name("native.sock")
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as bound:
                bound.bind(str(native))
            path.symlink_to(native.name)
            self.command = command
            self.kwargs = kwargs
            self.process = SimpleNamespace(pid=4321, poll=lambda: None)
            self.failure = None
            self.notifications = []

    monkeypatch.setattr(codexd_module, "_probe_codex_version", lambda _: "codex-cli 0.165.0")
    monkeypatch.setattr(codexd_module, "AppServerClient", SymlinkAppServer)
    daemon = CodexDaemon()
    daemon._start_client()
    endpoint = Path(daemon.app_server_path)
    assert endpoint.is_symlink()
    assert daemon.app_server_inode == (os.lstat(endpoint).st_dev, os.lstat(endpoint).st_ino)


def test_restart_unlinks_only_stale_owned_mac_socket_symlink(
    monkeypatch, active_daemon_root, tmp_path,
):
    from ccx.codex.paths import codexd_sock_path

    app = codexd_sock_path().with_name("app.sock")
    app.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    external = tmp_path / "untouched-target"
    external.write_text("not a socket; must remain untouched")
    app.symlink_to(external)
    assert app.is_symlink()

    monkeypatch.setattr(codexd_module, "_probe_codex_version", lambda _: None)
    monkeypatch.setattr(codexd_module, "AppServerClient", _FakeAppServer)
    daemon = CodexDaemon()
    daemon._start_client()

    assert external.read_text() == "not a socket; must remain untouched"
    assert not app.is_symlink()
    assert app.is_socket()
    assert isinstance(daemon.client, _FakeAppServer)


def test_actual_initialize_failure_still_surfaces(monkeypatch, active_daemon_root):
    class FailingAppServer(_FakeAppServer):
        def request(self, method, params=None, timeout=30):
            raise ProtocolError("initialize rejected", method=method, code=-32602)

    monkeypatch.setattr(
        codexd_module, "_probe_codex_version", lambda _: "mystery"
    )
    monkeypatch.setattr(codexd_module, "AppServerClient", FailingAppServer)

    with pytest.raises(ProtocolError, match="initialize rejected"):
        CodexDaemon()._start_client()


def test_probe_failure_is_silent_best_effort_metadata(monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("gone")

    monkeypatch.setattr(codexd_module.subprocess, "run", fail)
    assert codexd_module._probe_codex_version("codex") is None


def test_reference_schema_checks_only_critical_facts():
    summary = validate_reference_schema(SNAPSHOT)
    assert len(summary["threadItemTypes"]) == 18
    assert len(summary["clientNotificationMethods"]) == 1
    assert len(summary["notificationMethods"]) == 70
    assert len(summary["serverRequestMethods"]) == 10
    assert summary["clientNotificationMethods"] == ["initialized"]


def test_critical_projection_is_small_and_present():
    projection = critical_schema_projection(SNAPSHOT)
    assert CRITICAL_CLIENT_METHODS
    assert CRITICAL_CLIENT_NOTIFICATIONS == ("initialized",)
    assert CRITICAL_SERVER_NOTIFICATIONS
    assert CRITICAL_THREAD_ITEM_TYPES == ("contextCompaction",)
    assert "account/rateLimits/updated" in CRITICAL_SERVER_NOTIFICATIONS
    assert "thread/compacted" not in CRITICAL_SERVER_NOTIFICATIONS
    assert len(CRITICAL_FIELDS) == 8
    assert all(projection["threadItemTypes"].values())
    assert all(projection["clientMethods"].values())
    assert all(projection["clientNotificationMethods"].values())
    assert all(projection["notificationMethods"].values())
    assert all(projection["fields"].values())
    assert missing_critical_schema_facts(SNAPSHOT) == []


def test_reference_aggregates_are_valid_and_complete():
    summary = validate_reference_schema(SNAPSHOT)
    assert len(summary["threadItemTypes"]) == 18
    assert len(summary["clientNotificationMethods"]) == 1
    assert len(summary["notificationMethods"]) == 70
    assert len(summary["serverRequestMethods"]) == 10
    assert {path.name for path in SNAPSHOT.glob("*.json")} == set(
        REFERENCE_SCHEMA_FILES
    )
    assert not (SNAPSHOT / "MANIFEST.json").exists()
    assert not (SNAPSHOT.parent / "policy" / "0.147.0.json").exists()
    assert set(protocol_summary(SNAPSHOT)) == {
        "threadItemTypes",
        "clientNotificationMethods",
        "notificationMethods",
        "serverRequestMethods",
    }


def test_schema_checker_offline_default_does_not_need_codex_binary():
    result = subprocess.run(
        [sys.executable, "scripts/check_codex_protocol.py"],
        cwd=Path(__file__).parents[1],
        env={**dict(__import__("os").environ), "CCX_CODEX_BIN": "/missing/codex"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "reference" in result.stdout.lower()


def test_schema_checker_candidate_mode_does_not_probe_codex_version(tmp_path):
    candidate = tmp_path / "candidate"
    _copy_reference_aggregates(candidate)
    result = subprocess.run(
        [
            sys.executable,
            "scripts/check_codex_protocol.py",
            "--generated",
            str(candidate),
        ],
        cwd=Path(__file__).parents[1],
        env={**dict(__import__("os").environ), "CCX_CODEX_BIN": "/missing/codex"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "compared with reference" in result.stdout


def test_candidate_additions_are_reported_without_runtime_incompatibility(tmp_path):
    candidate = tmp_path / "candidate"
    _copy_reference_aggregates(candidate)
    path = candidate / "ServerNotification.json"
    value = json.loads(path.read_text())
    value["oneOf"].append({
        "properties": {"method": {"enum": ["future/notice"]}},
    })
    path.write_text(json.dumps(value))

    diff = semantic_schema_diff(SNAPSHOT, candidate)

    assert diff["notificationMethods"]["added"] == ["future/notice"]
    assert diff["notificationMethods"]["removed"] == []
    assert diff["criticalMissing"] == []


def test_candidate_client_notification_addition_is_reported(tmp_path):
    candidate = tmp_path / "candidate"
    _copy_reference_aggregates(candidate)
    path = candidate / "ClientNotification.json"
    value = json.loads(path.read_text())
    value["oneOf"].append({
        "properties": {"method": {"enum": ["future/clientNotice"]}},
    })
    path.write_text(json.dumps(value))

    diff = semantic_schema_diff(SNAPSHOT, candidate)

    assert diff["clientNotificationMethods"]["added"] == ["future/clientNotice"]
    assert diff["clientNotificationMethods"]["removed"] == []
    assert diff["criticalMissing"] == []


def test_candidate_removal_of_initialized_client_notification_is_flagged(tmp_path):
    candidate = tmp_path / "candidate"
    _copy_reference_aggregates(candidate)
    path = candidate / "ClientNotification.json"
    value = json.loads(path.read_text())
    value["oneOf"] = [
        branch for branch in value["oneOf"]
        if "initialized" not in (
            ((branch.get("properties") or {}).get("method") or {}).get("enum")
            or []
        )
    ]
    path.write_text(json.dumps(value))

    missing = missing_critical_schema_facts(candidate)

    assert "client notification initialized" in missing


def test_candidate_removal_of_critical_fact_is_flagged(tmp_path):
    candidate = tmp_path / "candidate"
    _copy_reference_aggregates(candidate)
    path = candidate / "ClientRequest.json"
    value = json.loads(path.read_text())
    value["oneOf"] = [
        branch for branch in value["oneOf"]
        if "thread/read" not in (
            ((branch.get("properties") or {}).get("method") or {}).get("enum") or []
        )
    ]
    path.write_text(json.dumps(value))

    missing = missing_critical_schema_facts(candidate)

    assert "client method thread/read" in missing


def test_unknown_notification_is_a_harmless_noop(capsys):
    daemon = CodexDaemon()

    daemon._on_notification("future/additiveNotification", {"threadId": "x"})

    assert daemon.diagnostics == []
    assert capsys.readouterr().err == ""


def test_diagnostic_logging_once_per_distinct_message_and_daemon_run(capsys):
    from concurrent.futures import ThreadPoolExecutor

    daemon = CodexDaemon()
    duplicate = {"summary": "Full-history hydration is deprecated", "details": "page instead"}
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: daemon._on_notification("deprecationNotice", duplicate), range(100)))
    assert len(daemon.diagnostics) == 1
    assert capsys.readouterr().err.count("Full-history hydration") == 1

    daemon._on_notification("deprecationNotice", {**duplicate, "details": "a different detail"})
    daemon._on_notification("warning", duplicate)
    assert len(daemon.diagnostics) == 3
    assert len(capsys.readouterr().err.splitlines()) == 2
    for index in range(25):
        daemon._on_notification("warning", {"message": f"distinct-{index}"})
    assert len(daemon.diagnostics) == 20
    capsys.readouterr()
    daemon._on_notification("deprecationNotice", duplicate)
    assert capsys.readouterr().err == ""  # Eviction is not a reset.
    restarted = CodexDaemon()
    restarted._on_notification("deprecationNotice", duplicate)
    assert len(restarted.diagnostics) == 1
    assert capsys.readouterr().err.count("Full-history hydration") == 1


def test_unknown_server_request_is_explicitly_rejected():
    class Client:
        def __init__(self):
            self.rejections = []

        def reject_request(self, request_id, message):
            self.rejections.append((request_id, message))

    client = Client()
    daemon = CodexDaemon()
    daemon.client = client

    daemon._on_server_request("req-1", "future/request", {"threadId": "x"})

    assert client.rejections == [
        ("req-1", "CCX has no handler for app-server request future/request")
    ]


def test_unknown_thread_item_is_preserved_as_other_and_sanitized():
    document = build_codex(
        {"short": "abc12345"},
        {"turns": [{
            "id": "turn-1",
            "status": "completed",
            "items": [{
                "id": "item-1",
                "type": "futureItem",
                "content": "preserve this native field",
                "nested": {"type": "futureReasoning", "content": "secret"},
            }],
        }]},
        state="idle",
        worker="abc12345",
    )

    item = document["turns"][0]["output"][0]
    assert item["kind"] == "other"
    assert item["nativeType"] == "futureItem"
    assert item["native"]["content"] == "preserve this native field"
    assert "secret" not in json.dumps(item)


@pytest.mark.parametrize("native_status", ["futureStatus", None])
def test_unknown_turn_status_does_not_become_completed(native_status):
    document = build_codex(
        {"short": "abc12345"},
        {"turns": [{"id": "turn-1", "status": native_status, "items": []}]},
        state="idle",
        worker="abc12345",
    )
    assert document["turns"][0]["status"] is None


@pytest.mark.parametrize("item_type", [
    "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall",
    "collabAgentToolCall", "imageGeneration",
])
def test_unknown_item_status_does_not_become_completed(item_type):
    item = {"id": "item-1", "type": item_type, "status": "futureStatus"}
    if item_type == "commandExecution":
        item.update({"command": "true", "aggregatedOutput": "", "exitCode": 0})
    elif item_type == "fileChange":
        item["changes"] = []
    elif item_type == "mcpToolCall":
        item.update({"server": "s", "tool": "t", "arguments": {}})
    elif item_type == "dynamicToolCall":
        item.update({"tool": "t", "arguments": {}})
    elif item_type == "collabAgentToolCall":
        item.update({"tool": "wait", "agentsStates": {},
                     "receiverThreadIds": [], "senderThreadId": "s"})
    else:
        item["result"] = "image-ref"
    document = build_codex(
        {"short": "abc12345"},
        {"turns": [{"id": "turn-1", "status": "completed", "items": [item]}]},
        state="idle",
        worker="abc12345",
    )
    output = document["turns"][0]["output"][0]
    assert output["status"] is None


def test_known_statuses_remain_unchanged():
    document = build_codex(
        {"short": "abc12345"},
        {"turns": [{
            "id": "turn-1",
            "status": "inProgress",
            "items": [{
                "id": "item-1", "type": "commandExecution",
                "status": "inProgress", "command": "true",
                "aggregatedOutput": "", "exitCode": 0,
            }],
        }]},
        state="busy",
        worker="abc12345",
    )
    assert document["turns"][0]["status"] == "in-progress"
    assert document["turns"][0]["output"][0]["status"] == "in-progress"


def test_original_native_creation_model_effort_request_fields_are_schema_supported():
    definitions = json.loads((SNAPSHOT / "ClientRequest.json").read_text())["definitions"]
    thread_start = definitions["ThreadStartParams"]["properties"]
    turn_start = definitions["TurnStartParams"]["properties"]
    assert "model" in thread_start
    assert "effort" not in thread_start
    assert {"model", "effort"}.issubset(turn_start)
    assert turn_start["effort"]["anyOf"][0]["$ref"] == "#/definitions/ReasoningEffort"
