"""Public Slack workspace status receipt and lifecycle contracts."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
import plugins.platforms.slack.adapter as slack_module
from plugins.platforms.slack.adapter import SlackAdapter, _apply_yaml_config
from plugins.platforms.slack.workspace_status import SlackWorkspaceStatusWriter


FIXED_NOW = "2026-07-17T03:04:05Z"


class _FakeConnectBoltApp:
    def __init__(self, authorize):
        self.authorize = authorize
        self.client = MagicMock(proxy=None)

    def event(self, _event_type):
        return lambda fn: fn

    def command(self, _command):
        return lambda fn: fn

    def action(self, _action_id):
        return lambda fn: fn


def _patch_successful_connect_dependencies(
    monkeypatch, tmp_path: Path, adapter: SlackAdapter, *, team_id: str
) -> None:
    class FakeWebClient:
        def __init__(self, token):
            self.token = token
            self.proxy = None

        async def auth_test(self):
            return {
                "team_id": team_id,
                "user_id": "U0BOT",
                "bot_id": "B0BOT",
                "user": "testbot",
                "team": "Test Team",
            }

    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-test")
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(slack_module, "AsyncWebClient", FakeWebClient)
    monkeypatch.setattr(slack_module, "AsyncApp", _FakeConnectBoltApp)
    monkeypatch.setattr(adapter, "_acquire_platform_lock", lambda *_args: True)
    monkeypatch.setattr(adapter, "_release_platform_lock", lambda: None)
    monkeypatch.setattr(adapter, "_stop_socket_mode_handler", AsyncMock())
    monkeypatch.setattr(adapter, "_start_socket_mode_handler", lambda: None)
    monkeypatch.setattr(adapter, "_ensure_socket_watchdog", lambda: None)


def _writer(tmp_path: Path) -> SlackWorkspaceStatusWriter:
    return SlackWorkspaceStatusWriter(
        workspace_keys={"T0TEAM": "team", "T0APOM": "apom"},
        agent_key="chami",
        path=tmp_path / "state" / "slack_workspace_status.json",
        clock=lambda: FIXED_NOW,
    )


def _read(writer: SlackWorkspaceStatusWriter) -> dict:
    return json.loads(writer.path.read_text(encoding="utf-8"))


def test_receipt_has_exact_schema_private_mode_and_timestamp_order(tmp_path):
    writer = _writer(tmp_path)
    writer.mark_verified("T0TEAM", write=False)
    writer.mark_verified("T0APOM", write=False)
    writer.mark_socket_connected()

    payload = _read(writer)
    assert set(payload) == {
        "schema_version",
        "agent_key",
        "generated_at",
        "workspaces",
    }
    assert payload["schema_version"] == 1
    assert payload["agent_key"] == "chami"
    assert [row["workspace_key"] for row in payload["workspaces"]] == [
        "apom",
        "team",
    ]
    for row in payload["workspaces"]:
        assert set(row) == {
            "workspace_key",
            "team_id",
            "connected",
            "verified_at",
            "heartbeat_at",
            "last_inbound_at",
            "error_code",
        }
        assert row["connected"] is True
        assert row["error_code"] is None
        for field in ("verified_at", "heartbeat_at", "last_inbound_at"):
            if row[field] is not None:
                assert row[field] <= payload["generated_at"]
    assert os.stat(writer.path).st_mode & 0o777 == 0o600


def test_default_receipt_path_is_profile_local(tmp_path, monkeypatch):
    profile_home = tmp_path / "profiles" / "chami"
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    writer = SlackWorkspaceStatusWriter(
        workspace_keys={"T0TEAM": "team"},
        agent_key="chami",
        clock=lambda: FIXED_NOW,
    )
    writer.mark_disconnected("not_authed")

    assert writer.path == profile_home / "state" / "slack_workspace_status.json"
    assert writer.path.exists()


def test_two_workspace_inbound_isolation_and_unexpected_workspace_rejection(tmp_path):
    writer = _writer(tmp_path)
    writer.mark_verified("T0TEAM", write=False)
    writer.mark_verified("T0APOM", write=False)
    writer.mark_socket_connected()
    writer.mark_inbound("T0TEAM")

    rows = {row["team_id"]: row for row in _read(writer)["workspaces"]}
    assert rows["T0TEAM"]["last_inbound_at"] == FIXED_NOW
    assert rows["T0APOM"]["last_inbound_at"] is None
    with pytest.raises(KeyError, match="Unexpected Slack workspace"):
        writer.mark_inbound("T0OTHER")
    assert {row["team_id"] for row in _read(writer)["workspaces"]} == {
        "T0TEAM",
        "T0APOM",
    }


def test_receipt_never_contains_token_url_user_or_exception_text(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-super-secret-token")
    monkeypatch.setenv("HTTPS_PROXY", "https://user:pass@example.invalid/proxy")
    monkeypatch.setenv("SLACK_ALLOWED_USERS", "U_CHAD_SECRET")
    writer = _writer(tmp_path)
    writer.mark_disconnected("invalid_auth")

    serialized = writer.path.read_text(encoding="utf-8")
    assert "xoxb-super-secret-token" not in serialized
    assert "example.invalid" not in serialized
    assert "U_CHAD_SECRET" not in serialized
    assert "Traceback" not in serialized
    assert "invalid_auth" in serialized


def test_disconnected_rows_always_use_consumer_safe_error_code(tmp_path):
    writer = _writer(tmp_path)
    writer.mark_disconnected("socket_disconnected")
    for row in _read(writer)["workspaces"]:
        assert row["connected"] is False
        assert row["error_code"] == "socket_disconnected"

    with pytest.raises(ValueError, match="Unsafe"):
        writer.mark_disconnected("raw exception: token=xoxb-secret")


def test_socket_cannot_mark_unverified_workspace_healthy(tmp_path):
    writer = _writer(tmp_path)
    writer.mark_verified("T0TEAM", write=False)
    writer.mark_socket_connected()

    rows = {row["team_id"]: row for row in _read(writer)["workspaces"]}
    assert rows["T0TEAM"]["connected"] is True
    assert rows["T0TEAM"]["error_code"] is None
    assert rows["T0APOM"]["connected"] is False
    assert rows["T0APOM"]["error_code"] == "not_authed"


def test_yaml_config_validates_workspace_keys_and_agent_key():
    extras = _apply_yaml_config(
        {},
        {
            "workspace_keys": {"T0TEAM": "team", "T0APOM": "apom"},
            "workspace_status_agent_key": "Chami",
        },
    )
    assert extras == {
        "workspace_keys": {"T0TEAM": "team", "T0APOM": "apom"},
        "workspace_status_agent_key": "chami",
    }

    with pytest.raises(ValueError, match="must be a mapping"):
        _apply_yaml_config({}, {"workspace_keys": ["team"]})
    with pytest.raises(ValueError, match="duplicate workspace key"):
        _apply_yaml_config(
            {}, {"workspace_keys": {"T0TEAM": "same", "T0APOM": "same"}}
        )
    with pytest.raises(ValueError, match="workspace_status_agent_key"):
        _apply_yaml_config({}, {"workspace_status_agent_key": "not valid"})


@pytest.mark.asyncio
async def test_exact_dispatched_inbound_refreshes_only_outer_workspace(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-test"))
    adapter._workspace_status_writer = _writer(tmp_path)
    adapter._team_clients = {"T0TEAM": MagicMock(), "T0APOM": MagicMock()}
    adapter._workspace_status_writer.mark_verified("T0TEAM", write=False)
    adapter._workspace_status_writer.mark_verified("T0APOM", write=False)
    adapter._workspace_status_writer.mark_socket_connected()
    handler = AsyncMock()

    assert await adapter._dispatch_workspace_event(
        event={"type": "message", "channel": "C1", "ts": "1.0"},
        body={"team_id": "T0APOM", "event_id": "Ev1"},
        context={"team_id": "T0APOM"},
        handler=handler,
    )

    rows = {
        row["team_id"]: row
        for row in _read(adapter._workspace_status_writer)["workspaces"]
    }
    assert rows["T0APOM"]["last_inbound_at"] == FIXED_NOW
    assert rows["T0TEAM"]["last_inbound_at"] is None


@pytest.mark.asyncio
async def test_watchdog_connected_refreshes_public_heartbeat(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-test"))
    adapter._workspace_status_writer = _writer(tmp_path)
    adapter._workspace_status_writer.mark_verified("T0TEAM", write=False)
    adapter._workspace_status_writer.mark_verified("T0APOM", write=False)
    adapter._workspace_status_writer.write()
    adapter._running = True
    adapter._socket_watchdog_interval_s = 0
    adapter._socket_mode_task = asyncio.get_running_loop().create_future()

    async def connected_once():
        adapter._running = False
        return True

    adapter._socket_transport_connected = connected_once
    await adapter._socket_watchdog_loop()

    for row in _read(adapter._workspace_status_writer)["workspaces"]:
        assert row["connected"] is True
        assert row["heartbeat_at"] == FIXED_NOW
        assert row["error_code"] is None
    adapter._socket_mode_task.cancel()


@pytest.mark.asyncio
async def test_reconnect_success_and_failure_publish_safe_states(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-test"))
    adapter._workspace_status_writer = _writer(tmp_path)
    adapter._workspace_status_writer.mark_verified("T0TEAM", write=False)
    adapter._workspace_status_writer.mark_verified("T0APOM", write=False)
    adapter._workspace_status_writer.mark_socket_connected()
    adapter._running = True
    adapter._app = MagicMock()
    adapter._app_token = "xapp-secret-never-published"
    adapter._stop_socket_mode_handler = AsyncMock()
    adapter._start_socket_mode_handler = MagicMock()

    await adapter._restart_socket_mode("transport disconnected")
    assert all(
        row["connected"]
        for row in _read(adapter._workspace_status_writer)["workspaces"]
    )

    adapter._start_socket_mode_handler.side_effect = RuntimeError(
        "secret failure xoxb-must-not-leak"
    )
    await adapter._restart_socket_mode("socket task exited")
    payload = _read(adapter._workspace_status_writer)
    assert all(not row["connected"] for row in payload["workspaces"])
    assert {row["error_code"] for row in payload["workspaces"]} == {
        "socket_error"
    }
    assert "xoxb-must-not-leak" not in writer_text(adapter._workspace_status_writer)


def writer_text(writer: SlackWorkspaceStatusWriter) -> str:
    return writer.path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_disconnect_publishes_workspace_disabled(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-test"))
    adapter._workspace_status_writer = _writer(tmp_path)
    adapter._workspace_status_writer.mark_verified("T0TEAM", write=False)
    adapter._workspace_status_writer.mark_verified("T0APOM", write=False)
    adapter._workspace_status_writer.mark_socket_connected()
    adapter._release_platform_lock = MagicMock()

    await adapter.disconnect()

    rows = _read(adapter._workspace_status_writer)["workspaces"]
    assert all(not row["connected"] for row in rows)
    assert {row["error_code"] for row in rows} == {"workspace_disabled"}


@pytest.mark.asyncio
async def test_auth_failure_maps_slack_code_without_leaking_exception(
    tmp_path, monkeypatch
):
    class AuthFailure(RuntimeError):
        response = {"error": "token_revoked"}

    class FailingWebClient:
        def __init__(self, token):
            self.token = token
            self.proxy = None

        async def auth_test(self):
            raise AuthFailure("token xoxb-top-secret was revoked")

    adapter = SlackAdapter(
        PlatformConfig(
            enabled=True,
            token="xoxb-top-secret",
            extra={
                "workspace_keys": {"T0TEAM": "team"},
                "workspace_status_agent_key": "chami",
            },
        )
    )
    adapter._workspace_status_writer = SlackWorkspaceStatusWriter(
        workspace_keys={"T0TEAM": "team"},
        agent_key="chami",
        path=tmp_path / "state" / "slack_workspace_status.json",
        clock=lambda: FIXED_NOW,
    )
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-test")
    monkeypatch.setattr(slack_module, "AsyncWebClient", FailingWebClient)
    monkeypatch.setattr(adapter, "_acquire_platform_lock", lambda *_args: True)
    monkeypatch.setattr(adapter, "_release_platform_lock", lambda: None)

    assert await adapter.connect() is False
    payload = _read(adapter._workspace_status_writer)
    assert payload["workspaces"][0]["error_code"] == "token_revoked"
    assert "xoxb-top-secret" not in writer_text(adapter._workspace_status_writer)


@pytest.mark.asyncio
async def test_workspace_receipt_failure_does_not_block_slack_connect(
    tmp_path, monkeypatch
):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-test"))
    status_writer = MagicMock()
    status_writer.mark_verified.side_effect = OSError("status path unavailable")
    adapter._workspace_status_writer = status_writer
    _patch_successful_connect_dependencies(
        monkeypatch, tmp_path, adapter, team_id="T0LIVE"
    )

    assert await adapter.connect() is True
    status_writer.validate_authorized_workspaces.assert_called_once_with({"T0LIVE"})


@pytest.mark.asyncio
async def test_workspace_set_mismatch_still_blocks_slack_connect(
    tmp_path, monkeypatch
):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-test"))
    adapter._workspace_status_writer = SlackWorkspaceStatusWriter(
        workspace_keys={"T0EXPECTED": "team"},
        agent_key="chami",
        path=tmp_path / "state" / "slack_workspace_status.json",
        clock=lambda: FIXED_NOW,
    )
    _patch_successful_connect_dependencies(
        monkeypatch, tmp_path, adapter, team_id="T0ACTUAL"
    )

    assert await adapter.connect() is False
    assert adapter._running is False
