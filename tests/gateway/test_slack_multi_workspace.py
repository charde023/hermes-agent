"""Slack multi-workspace authorization, isolation, and routing contracts."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from slack_bolt.async_app import AsyncApp
from slack_bolt.request.async_request import AsyncBoltRequest

from gateway.config import PlatformConfig
from gateway.platforms.base import ProcessingOutcome
import plugins.platforms.slack.adapter as slack_adapter_module
from plugins.platforms.slack.adapter import (
    SlackAdapter,
    _apply_yaml_config,
    _is_connected,
    _redact_slack_secrets,
    _slash_context_key,
    _standalone_send,
)


def _adapter(*, multi_workspace: bool = True) -> SlackAdapter:
    config = PlatformConfig(
        enabled=True,
        token="xoxb-primary,xoxb-secondary",
        extra={"multi_workspace": multi_workspace},
    )
    adapter = SlackAdapter(config)
    adapter._app = MagicMock()
    adapter._app.client = AsyncMock()
    adapter._team_clients = {
        "T0PRIMARY": AsyncMock(token="xoxb-primary"),
        "T0SECONDARY": AsyncMock(token="xoxb-secondary"),
    }
    adapter._team_tokens = {
        "T0PRIMARY": "xoxb-primary",
        "T0SECONDARY": "xoxb-secondary",
    }
    adapter._team_bot_user_ids = {
        "T0PRIMARY": "U_PRIMARY_BOT",
        "T0SECONDARY": "U_SECONDARY_BOT",
    }
    adapter._team_bot_ids = {
        "T0PRIMARY": "B_PRIMARY",
        "T0SECONDARY": "B_SECONDARY",
    }
    return adapter


class _FakeBoltApp:
    def __init__(self, authorize):
        self.authorize = authorize
        self.client = MagicMock(proxy=None)

    def event(self, _event_type):
        return lambda fn: fn

    def command(self, _command):
        return lambda fn: fn

    def action(self, _action_id):
        return lambda fn: fn


def _fake_web_client_factory(auth_by_token):
    class FakeWebClient:
        def __init__(self, token):
            self.token = token
            self.proxy = None
            self.auth_test = AsyncMock(return_value=auth_by_token[token])

    return FakeWebClient


@pytest.mark.asyncio
async def test_connect_builds_multi_team_authorizer_and_seeds_home_channels(
    monkeypatch, tmp_path
):
    auth_by_token = {
        "xoxb-primary": {
            "team_id": "T0PRIMARY",
            "user_id": "U_PRIMARY_BOT",
            "bot_id": "B_PRIMARY",
            "user": "primary",
            "team": "Primary",
        },
        "xoxb-secondary": {
            "team_id": "T0SECONDARY",
            "user_id": "U_SECONDARY_BOT",
            "bot_id": "B_SECONDARY",
            "user": "secondary",
            "team": "Secondary",
        },
    }
    adapter = SlackAdapter(
        PlatformConfig(
            enabled=True,
            token="xoxb-primary",
            extra={
                "workspace_bot_token_refs": {
                    "T0SECONDARY": "env://CHAMI_SLACK_BOT_TOKEN_APOM",
                },
                "workspace_home_channels": {
                    "T0PRIMARY": "C_PRIMARY_HOME",
                    "T0SECONDARY": "C_SECONDARY_HOME",
                }
            },
        )
    )
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-test-token")
    monkeypatch.setenv("CHAMI_SLACK_BOT_TOKEN_APOM", "xoxb-secondary")
    monkeypatch.setattr(
        "hermes_constants.get_hermes_home", lambda: Path(tmp_path)
    )
    monkeypatch.setattr(
        slack_adapter_module,
        "AsyncWebClient",
        _fake_web_client_factory(auth_by_token),
    )
    monkeypatch.setattr(slack_adapter_module, "AsyncApp", _FakeBoltApp)
    monkeypatch.setattr(adapter, "_acquire_platform_lock", lambda *_args: True)
    monkeypatch.setattr(adapter, "_release_platform_lock", lambda: None)
    monkeypatch.setattr(adapter, "_start_socket_mode_handler", lambda: None)
    monkeypatch.setattr(adapter, "_ensure_socket_watchdog", lambda: None)

    assert await adapter.connect() is True
    secondary_auth = await adapter._app.authorize(
        enterprise_id=None,
        team_id="T0SECONDARY",
        user_id="U_HUMAN",
    )
    assert secondary_auth.bot_token == "xoxb-secondary"
    assert secondary_auth.bot_user_id == "U_SECONDARY_BOT"
    assert adapter._channel_team == {
        "C_PRIMARY_HOME": "T0PRIMARY",
        "C_SECONDARY_HOME": "T0SECONDARY",
    }


@pytest.mark.asyncio
async def test_connect_rejects_duplicate_raw_token_without_logging_it(
    monkeypatch, caplog
):
    secret = "xoxb-duplicate-secret-token"
    adapter = SlackAdapter(
        PlatformConfig(enabled=True, token=f"{secret},{secret}")
    )
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-test-token")

    assert await adapter.connect() is False
    assert secret not in caplog.text
    assert "Duplicate bot token" in caplog.text


@pytest.mark.asyncio
async def test_connect_rejects_legacy_and_ref_duplicate_without_logging_it(
    monkeypatch, caplog
):
    secret = "xoxb-duplicate-secret-token"
    adapter = SlackAdapter(
        PlatformConfig(
            enabled=True,
            token=secret,
            extra={
                "workspace_bot_token_refs": {
                    "T0SECONDARY": "env://CHAMI_SLACK_BOT_TOKEN_APOM",
                }
            },
        )
    )
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-test-token")
    monkeypatch.setenv("CHAMI_SLACK_BOT_TOKEN_APOM", secret)

    assert await adapter.connect() is False
    assert secret not in caplog.text
    assert "Duplicate bot token" in caplog.text


@pytest.mark.asyncio
async def test_connect_rejects_two_tokens_for_same_workspace(
    monkeypatch, tmp_path, caplog
):
    auth_by_token = {
        "xoxb-one": {
            "team_id": "T0DUPLICATE",
            "user_id": "U_BOT_ONE",
            "bot_id": "B_ONE",
            "user": "one",
            "team": "Duplicate",
        },
        "xoxb-two": {
            "team_id": "T0DUPLICATE",
            "user_id": "U_BOT_TWO",
            "bot_id": "B_TWO",
            "user": "two",
            "team": "Duplicate",
        },
    }
    adapter = SlackAdapter(
        PlatformConfig(
            enabled=True,
            token="xoxb-one",
            extra={
                "workspace_bot_token_refs": {
                    "T0DUPLICATE": "env://CHAMI_SLACK_BOT_TOKEN_APOM",
                }
            },
        )
    )
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-test-token")
    monkeypatch.setenv("CHAMI_SLACK_BOT_TOKEN_APOM", "xoxb-two")
    monkeypatch.setattr(
        "hermes_constants.get_hermes_home", lambda: Path(tmp_path)
    )
    monkeypatch.setattr(
        slack_adapter_module,
        "AsyncWebClient",
        _fake_web_client_factory(auth_by_token),
    )
    monkeypatch.setattr(adapter, "_acquire_platform_lock", lambda *_args: True)
    monkeypatch.setattr(adapter, "_release_platform_lock", lambda: None)

    assert await adapter.connect() is False
    assert "Duplicate Slack workspace authorization" in caplog.text


def test_slack_error_redaction_covers_configured_and_slack_shaped_tokens():
    configured = "xoxb-configured-super-secret"
    shaped = "xapp-unexpected-super-secret"

    safe = _redact_slack_secrets(
        f"request failed token={configured} app={shaped}", [configured]
    )

    assert configured not in safe
    assert shaped not in safe
    assert safe.count("[REDACTED_SLACK_TOKEN]") == 2


def test_yaml_config_preserves_structured_workspace_routing():
    seeded = _apply_yaml_config(
        {},
        {
            "multi_workspace": True,
            "workspace_home_channels": {
                "T0PRIMARY": "C_PRIMARY_HOME",
                "T0SECONDARY": "C_SECONDARY_HOME",
            },
            "workspace_bot_token_refs": {
                "T0PRIMARY": "env://CHAMI_SLACK_BOT_TOKEN_TEAM",
                "T0SECONDARY": "env://CHAMI_SLACK_BOT_TOKEN_APOM",
            },
            "workspace_allowed_users": {
                "T0PRIMARY": ["U_CHAD"],
                "T0SECONDARY": ["U_APOM_ADMIN"],
            },
        },
    )

    assert seeded == {
        "multi_workspace": True,
        "workspace_home_channels": {
            "T0PRIMARY": "C_PRIMARY_HOME",
            "T0SECONDARY": "C_SECONDARY_HOME",
        },
        "workspace_bot_token_refs": {
            "T0PRIMARY": "env://CHAMI_SLACK_BOT_TOKEN_TEAM",
            "T0SECONDARY": "env://CHAMI_SLACK_BOT_TOKEN_APOM",
        },
        "workspace_allowed_users": {
            "T0PRIMARY": ["U_CHAD"],
            "T0SECONDARY": ["U_APOM_ADMIN"],
        },
    }


@pytest.mark.parametrize(
    "invalid_ref",
    ["xoxb-plain-token-in-yaml", "CHAMI_SLACK_BOT_TOKEN_APOM", "env://lowercase"],
)
def test_yaml_config_rejects_plain_or_invalid_workspace_token_refs(invalid_ref):
    with pytest.raises(ValueError, match="env://"):
        _apply_yaml_config(
            {},
            {"workspace_bot_token_refs": {"T0APOM": invalid_ref}},
        )


def test_download_token_selection_requires_unambiguous_workspace():
    adapter = _adapter()

    assert adapter._bot_token_for_workspace("T0SECONDARY") == "xoxb-secondary"
    with pytest.raises(RuntimeError, match="workspace scope is required"):
        adapter._bot_token_for_workspace("")
    with pytest.raises(RuntimeError, match="Unknown Slack workspace"):
        adapter._bot_token_for_workspace("T0UNKNOWN")


@pytest.mark.asyncio
async def test_download_uses_matching_workspace_token(monkeypatch):
    import httpx

    adapter = _adapter()
    requests = []
    response = MagicMock(
        content=b"workspace-file",
        headers={"content-type": "application/octet-stream"},
    )
    response.raise_for_status = MagicMock()

    class FakeAsyncClient:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, headers):
            requests.append((url, headers))
            return response

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

    content = await adapter._download_slack_file_bytes(
        "https://files.slack.test/secondary",
        team_id="T0SECONDARY",
    )

    assert content == b"workspace-file"
    assert requests[0][1]["Authorization"] == "Bearer xoxb-secondary"


@pytest.mark.asyncio
async def test_download_unknown_workspace_fails_before_http(monkeypatch):
    import httpx

    adapter = _adapter()
    client = MagicMock()
    monkeypatch.setattr(httpx, "AsyncClient", client)

    with pytest.raises(RuntimeError, match="Unknown Slack workspace"):
        await adapter._download_slack_file_bytes(
            "https://files.slack.test/unknown",
            team_id="T0UNKNOWN",
        )

    client.assert_not_called()


def test_is_connected_accepts_resolved_workspace_token_refs(monkeypatch):
    import hermes_cli.gateway as gateway_mod

    monkeypatch.setattr(gateway_mod, "get_env_value", lambda _name: None)
    monkeypatch.setenv("CHAMI_SLACK_BOT_TOKEN_APOM", "xoxb-secondary")
    config = SimpleNamespace(
        extra={
            "workspace_bot_token_refs": {
                "T0SECONDARY": "env://CHAMI_SLACK_BOT_TOKEN_APOM",
            }
        }
    )

    assert _is_connected(config) is True


@pytest.mark.asyncio
async def test_standalone_send_routes_home_channel_to_matching_ref(monkeypatch):
    monkeypatch.setenv("CHAMI_SLACK_BOT_TOKEN_TEAM", "xoxb-primary")
    monkeypatch.setenv("CHAMI_SLACK_BOT_TOKEN_APOM", "xoxb-secondary")
    pconfig = SimpleNamespace(
        token=None,
        extra={
            "workspace_bot_token_refs": {
                "T0PRIMARY": "env://CHAMI_SLACK_BOT_TOKEN_TEAM",
                "T0SECONDARY": "env://CHAMI_SLACK_BOT_TOKEN_APOM",
            },
            "workspace_home_channels": {
                "T0PRIMARY": "C_PRIMARY_HOME",
                "T0SECONDARY": "C_SECONDARY_HOME",
            },
        },
    )
    response = AsyncMock()
    response.json = AsyncMock(return_value={"ok": True, "ts": "2.1"})
    response.__aenter__ = AsyncMock(return_value=response)
    response.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.post = MagicMock(return_value=response)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(
        slack_adapter_module.aiohttp,
        "ClientSession",
        MagicMock(return_value=session),
    )

    result = await _standalone_send(pconfig, "C_SECONDARY_HOME", "hello")

    assert result["success"] is True
    assert session.post.call_args.kwargs["headers"]["Authorization"] == (
        "Bearer xoxb-secondary"
    )


@pytest.mark.asyncio
async def test_standalone_send_multi_workspace_unknown_channel_fails_closed(monkeypatch):
    primary = "xoxb-primary-secret"
    secondary = "xoxb-secondary-secret"
    monkeypatch.setenv("CHAMI_SLACK_BOT_TOKEN_TEAM", primary)
    monkeypatch.setenv("CHAMI_SLACK_BOT_TOKEN_APOM", secondary)
    pconfig = SimpleNamespace(
        token=None,
        extra={
            "workspace_bot_token_refs": {
                "T0PRIMARY": "env://CHAMI_SLACK_BOT_TOKEN_TEAM",
                "T0SECONDARY": "env://CHAMI_SLACK_BOT_TOKEN_APOM",
            }
        },
    )
    client_session = MagicMock()
    monkeypatch.setattr(
        slack_adapter_module.aiohttp,
        "ClientSession",
        client_session,
    )

    result = await _standalone_send(pconfig, "C_UNKNOWN", "hello")

    assert "error" in result
    assert "workspace scope is required" in result["error"]
    assert primary not in result["error"]
    assert secondary not in result["error"]
    client_session.assert_not_called()


@pytest.mark.asyncio
async def test_standalone_send_never_uses_comma_token_as_bearer(monkeypatch):
    pconfig = SimpleNamespace(
        token="xoxb-primary-secret,xoxb-secondary-secret",
        extra={},
    )
    client_session = MagicMock()
    monkeypatch.setattr(
        slack_adapter_module.aiohttp,
        "ClientSession",
        client_session,
    )

    result = await _standalone_send(pconfig, "C_UNKNOWN", "hello")

    assert "error" in result
    assert "comma-separated" in result["error"].lower()
    client_session.assert_not_called()


@pytest.mark.asyncio
async def test_standalone_send_redacts_token_from_transport_error(monkeypatch):
    secret = "custom-token-that-must-not-leak"
    pconfig = SimpleNamespace(token=secret, extra={})
    session = MagicMock()
    session.post = MagicMock(side_effect=RuntimeError(f"transport used {secret}"))
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(
        slack_adapter_module.aiohttp,
        "ClientSession",
        MagicMock(return_value=session),
    )

    result = await _standalone_send(pconfig, "C_PRIMARY", "hello")

    assert "error" in result
    assert secret not in result["error"]
    assert "[REDACTED_SLACK_TOKEN]" in result["error"]


@pytest.mark.asyncio
async def test_secondary_workspace_async_dispatch_uses_outer_team_identity():
    """Real Bolt dispatch must authorize and preserve the secondary team."""
    adapter = _adapter()
    adapter._handle_slack_message = AsyncMock()

    app = AsyncApp(
        authorize=adapter._workspace_authorizer(),
        process_before_response=True,
        request_verification_enabled=False,
    )

    @app.event("message")
    async def handle_message(event, body, context):
        await adapter._dispatch_workspace_event(
            event=event,
            body=body,
            context=context,
            handler=adapter._handle_slack_message,
        )

    body = {
        "type": "event_callback",
        "team_id": "T0SECONDARY",
        "api_app_id": "A_TEST",
        "event_id": "Ev-secondary-1",
        "event_time": 1,
        "authorizations": [
            {
                "enterprise_id": None,
                "team_id": "T0SECONDARY",
                "user_id": "U_SECONDARY_BOT",
                "is_bot": True,
            }
        ],
        "event": {
            "type": "message",
            "channel": "C_SHARED",
            "channel_type": "channel",
            "user": "U_HUMAN",
            "text": "hello",
            "ts": "1.000001",
        },
    }

    response = await app.async_dispatch(
        AsyncBoltRequest(body=body, mode="socket_mode")
    )

    assert response.status == 200
    adapter._handle_slack_message.assert_awaited_once()
    scoped_event = adapter._handle_slack_message.await_args.args[0]
    assert scoped_event["team_id"] == "T0SECONDARY"
    assert scoped_event["_outer_event_id"] == "Ev-secondary-1"


@pytest.mark.asyncio
async def test_authorize_unknown_workspace_fails_closed():
    adapter = _adapter()

    with pytest.raises(PermissionError, match="Unknown Slack workspace"):
        await adapter._authorize_workspace(
            enterprise_id=None,
            team_id="T0UNKNOWN",
            user_id="U_HUMAN",
        )


def test_outbound_prefers_explicit_scope_then_channel_mapping():
    adapter = _adapter()
    adapter._channel_team["C_MAPPED"] = "T0PRIMARY"

    assert (
        adapter._get_client("C_MAPPED", metadata={"scope_id": "T0SECONDARY"})
        is adapter._team_clients["T0SECONDARY"]
    )
    assert adapter._get_client("C_MAPPED") is adapter._team_clients["T0PRIMARY"]


def test_outbound_unknown_channel_fails_closed_with_multiple_workspaces():
    adapter = _adapter()

    with pytest.raises(RuntimeError, match="workspace scope is required"):
        adapter._get_client("C_UNKNOWN")


def test_same_channel_id_seen_in_two_workspaces_becomes_ambiguous():
    adapter = _adapter()
    adapter._learn_channel_team("C_SHARED", "T0PRIMARY")
    adapter._learn_channel_team("C_SHARED", "T0SECONDARY")

    assert "C_SHARED" not in adapter._channel_team
    with pytest.raises(RuntimeError, match="workspace scope is required"):
        adapter._get_client("C_SHARED")
    assert (
        adapter._get_client("C_SHARED", metadata={"scope_id": "T0SECONDARY"})
        is adapter._team_clients["T0SECONDARY"]
    )


def test_single_workspace_keeps_legacy_outbound_fallback():
    adapter = _adapter(multi_workspace=False)
    primary = adapter._team_clients["T0PRIMARY"]
    adapter._team_clients = {"T0PRIMARY": primary}

    assert adapter._get_client("C_UNKNOWN") is primary


@pytest.mark.asyncio
async def test_send_routes_by_metadata_scope():
    adapter = _adapter()
    adapter._running = True
    secondary = adapter._team_clients["T0SECONDARY"]
    secondary.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "2.1"})

    result = await adapter.send(
        "C_SHARED",
        "secondary reply",
        metadata={"scope_id": "T0SECONDARY"},
    )

    assert result.success is True
    secondary.chat_postMessage.assert_awaited_once()
    adapter._team_clients["T0PRIMARY"].chat_postMessage.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_timestamp_in_different_workspaces_is_not_duplicate(monkeypatch):
    adapter = _adapter()
    adapter.handle_message = AsyncMock()
    adapter._resolve_user_name = AsyncMock(return_value="Human")
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")
    monkeypatch.setenv("SLACK_REQUIRE_MENTION", "false")

    base = {
        "type": "message",
        "channel": "C_SHARED",
        "channel_type": "im",
        "user": "U_HUMAN",
        "text": "hello",
        "ts": "3.000001",
    }
    await adapter._handle_slack_message({**base, "team_id": "T0PRIMARY"})
    await adapter._handle_slack_message({**base, "team_id": "T0SECONDARY"})

    assert adapter.handle_message.await_count == 2
    scopes = [
        call.args[0].source.scope_id
        for call in adapter.handle_message.await_args_list
    ]
    assert scopes == ["T0PRIMARY", "T0SECONDARY"]


@pytest.mark.asyncio
async def test_channel_overrides_are_scoped_when_channel_ids_collide(monkeypatch):
    adapter = _adapter()
    adapter.config.extra.update(
        {
            "channel_prompts": {
                "T0PRIMARY": {"D_SHARED": "primary prompt"},
                "T0SECONDARY": {"D_SHARED": "secondary prompt"},
            },
            "channel_skill_bindings": [
                {
                    "team_id": "T0PRIMARY",
                    "id": "D_SHARED",
                    "skills": ["primary-skill"],
                },
                {
                    "team_id": "T0SECONDARY",
                    "id": "D_SHARED",
                    "skills": ["secondary-skill"],
                },
            ],
        }
    )
    adapter.handle_message = AsyncMock()
    adapter._resolve_user_name = AsyncMock(return_value="Human")
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")

    base = {
        "type": "message",
        "channel": "D_SHARED",
        "channel_type": "im",
        "user": "U_HUMAN",
        "text": "hello",
    }
    await adapter._handle_slack_message(
        {**base, "team_id": "T0PRIMARY", "ts": "3.100001"}
    )
    await adapter._handle_slack_message(
        {**base, "team_id": "T0SECONDARY", "ts": "3.100002"}
    )

    events = [call.args[0] for call in adapter.handle_message.await_args_list]
    assert [(event.channel_prompt, event.auto_skill) for event in events] == [
        ("primary prompt", ["primary-skill"]),
        ("secondary prompt", ["secondary-skill"]),
    ]


@pytest.mark.asyncio
async def test_multi_workspace_ignores_legacy_unscoped_channel_overrides(
    monkeypatch,
):
    adapter = _adapter()
    adapter.config.extra.update(
        {
            "channel_prompts": {"D_SHARED": "legacy prompt"},
            "channel_skill_bindings": [
                {"id": "D_SHARED", "skills": ["legacy-skill"]},
            ],
        }
    )
    adapter.handle_message = AsyncMock()
    adapter._resolve_user_name = AsyncMock(return_value="Human")
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")

    await adapter._handle_slack_message(
        {
            "type": "message",
            "team_id": "T0SECONDARY",
            "channel": "D_SHARED",
            "channel_type": "im",
            "user": "U_HUMAN",
            "text": "hello",
            "ts": "3.200001",
        }
    )

    event = adapter.handle_message.await_args.args[0]
    assert event.channel_prompt is None
    assert event.auto_skill is None


@pytest.mark.asyncio
async def test_single_workspace_keeps_legacy_unscoped_channel_overrides(
    monkeypatch,
):
    adapter = _adapter(multi_workspace=False)
    adapter._team_clients = {
        "T0PRIMARY": adapter._team_clients["T0PRIMARY"],
    }
    adapter._team_tokens = {"T0PRIMARY": "xoxb-primary"}
    adapter._team_bot_user_ids = {"T0PRIMARY": "U_PRIMARY_BOT"}
    adapter._team_bot_ids = {"T0PRIMARY": "B_PRIMARY"}
    adapter.config.extra.update(
        {
            "channel_prompts": {"D_SHARED": "legacy prompt"},
            "channel_skill_bindings": [
                {"id": "D_SHARED", "skills": ["legacy-skill"]},
            ],
        }
    )
    adapter.handle_message = AsyncMock()
    adapter._resolve_user_name = AsyncMock(return_value="Human")
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")

    await adapter._handle_slack_message(
        {
            "type": "message",
            "team_id": "T0PRIMARY",
            "channel": "D_SHARED",
            "channel_type": "im",
            "user": "U_HUMAN",
            "text": "hello",
            "ts": "3.300001",
        }
    )

    event = adapter.handle_message.await_args.args[0]
    assert event.channel_prompt == "legacy prompt"
    assert event.auto_skill == ["legacy-skill"]


@pytest.mark.asyncio
async def test_same_workspace_same_timestamp_in_different_channels_is_not_duplicate(
    monkeypatch,
):
    """Slack message timestamps are unique only within one conversation."""
    adapter = _adapter()
    adapter.handle_message = AsyncMock()
    adapter._resolve_user_name = AsyncMock(return_value="Human")
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")

    base = {
        "type": "message",
        "channel_type": "im",
        "user": "U_HUMAN",
        "text": "hello",
        "ts": "3.000002",
        "team_id": "T0PRIMARY",
    }
    await adapter._handle_slack_message({**base, "channel": "D_PRIMARY_A"})
    await adapter._handle_slack_message({**base, "channel": "D_PRIMARY_B"})

    assert adapter.handle_message.await_count == 2
    assert [
        call.args[0].source.chat_id
        for call in adapter.handle_message.await_args_list
    ] == ["D_PRIMARY_A", "D_PRIMARY_B"]


@pytest.mark.asyncio
async def test_reaction_lifecycle_isolated_by_workspace_channel_and_timestamp(
    monkeypatch,
):
    adapter = _adapter()
    adapter.handle_message = AsyncMock()
    adapter._resolve_user_name = AsyncMock(return_value="Human")
    client = adapter._team_clients["T0PRIMARY"]
    client.reactions_add = AsyncMock()
    client.reactions_remove = AsyncMock()
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")

    base = {
        "type": "message",
        "channel_type": "im",
        "user": "U_HUMAN",
        "text": "hello",
        "ts": "3.000003",
        "team_id": "T0PRIMARY",
    }
    await adapter._handle_slack_message({**base, "channel": "D_PRIMARY_A"})
    await adapter._handle_slack_message({**base, "channel": "D_PRIMARY_B"})
    events = [call.args[0] for call in adapter.handle_message.await_args_list]

    for event in events:
        await adapter.on_processing_start(event)
    for event in events:
        await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)

    completed_channels = [
        call.kwargs["channel"]
        for call in client.reactions_add.await_args_list
        if call.kwargs["name"] == "white_check_mark"
    ]
    assert completed_channels == ["D_PRIMARY_A", "D_PRIMARY_B"]


@pytest.mark.asyncio
async def test_approval_cache_isolated_by_workspace_channel_and_timestamp():
    adapter = _adapter()
    adapter.config.extra["workspace_allowed_users"] = {
        "T0PRIMARY": ["U_HUMAN"],
        "T0SECONDARY": ["U_HUMAN"],
    }
    client = adapter._team_clients["T0PRIMARY"]
    client.chat_postMessage = AsyncMock(return_value={"ts": "3.000004"})
    client.chat_update = AsyncMock()

    await adapter.send_exec_approval(
        "C_PRIMARY_A",
        "echo one",
        "session-one",
        metadata={"scope_id": "T0PRIMARY"},
    )
    await adapter.send_exec_approval(
        "C_PRIMARY_B",
        "echo two",
        "session-two",
        metadata={"scope_id": "T0PRIMARY"},
    )

    def body(channel_id):
        return {
            "team": {"id": "T0PRIMARY"},
            "message": {"ts": "3.000004", "blocks": []},
            "channel": {"id": channel_id},
            "user": {"name": "human", "id": "U_HUMAN"},
        }

    with patch(
        "tools.approval.resolve_gateway_approval", return_value=1
    ) as resolve:
        await adapter._handle_approval_action(
            AsyncMock(),
            body("C_PRIMARY_A"),
            {"action_id": "hermes_approve_once", "value": "session-one"},
        )
        await adapter._handle_approval_action(
            AsyncMock(),
            body("C_PRIMARY_B"),
            {"action_id": "hermes_deny", "value": "session-two"},
        )

    assert [entry.args for entry in resolve.call_args_list] == [
        ("session-one", "once"),
        ("session-two", "deny"),
    ]


@pytest.mark.asyncio
async def test_secondary_bot_self_event_is_ignored(monkeypatch):
    adapter = _adapter()
    adapter.handle_message = AsyncMock()
    monkeypatch.setenv("SLACK_ALLOW_BOTS", "all")

    await adapter._handle_slack_message(
        {
            "type": "message",
            "subtype": "bot_message",
            "bot_id": "B_SECONDARY",
            "team_id": "T0SECONDARY",
            "channel": "C_SECONDARY",
            "channel_type": "channel",
            "user": "U_SECONDARY_BOT",
            "text": "my own response",
            "ts": "4.000001",
        }
    )

    adapter.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_thread_engagement_cache_is_workspace_scoped(monkeypatch):
    adapter = _adapter()
    adapter.handle_message = AsyncMock()
    adapter._resolve_user_name = AsyncMock(return_value="Human")
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")
    monkeypatch.setenv("SLACK_REQUIRE_MENTION", "true")
    thread_ts = "5.000001"
    adapter._mentioned_threads.add(("T0PRIMARY", "C_SHARED", thread_ts))

    event = {
        "type": "message",
        "team_id": "T0SECONDARY",
        "channel": "C_SHARED",
        "channel_type": "channel",
        "user": "U_HUMAN",
        "text": "follow up",
        "thread_ts": thread_ts,
        "ts": "5.000002",
    }
    await adapter._handle_slack_message(event)
    adapter.handle_message.assert_not_awaited()

    adapter._mentioned_threads.add(("T0SECONDARY", "C_SHARED", thread_ts))
    await adapter._handle_slack_message({**event, "ts": "5.000003"})
    adapter.handle_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_mentioned_thread_engagement_does_not_cross_channels(
    monkeypatch,
):
    adapter = _adapter()
    adapter.handle_message = AsyncMock()
    adapter._resolve_user_name = AsyncMock(return_value="Human")
    adapter._fetch_thread_context = AsyncMock(return_value="")
    adapter._fetch_thread_parent_text = AsyncMock(return_value="")
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")
    monkeypatch.setenv("SLACK_REQUIRE_MENTION", "true")

    base = {
        "type": "message",
        "team_id": "T0PRIMARY",
        "channel_type": "channel",
        "user": "U_HUMAN",
        "thread_ts": "5.000020",
    }
    await adapter._handle_slack_message(
        {
            **base,
            "channel": "C_PRIMARY_A",
            "ts": "5.000021",
            "text": "<@U_PRIMARY_BOT> start",
        }
    )
    adapter.handle_message.reset_mock()

    await adapter._handle_slack_message(
        {
            **base,
            "channel": "C_PRIMARY_B",
            "ts": "5.000022",
            "text": "other channel follow-up",
        }
    )
    adapter.handle_message.assert_not_awaited()

    await adapter._handle_slack_message(
        {
            **base,
            "channel": "C_PRIMARY_A",
            "ts": "5.000023",
            "text": "same channel follow-up",
        }
    )
    adapter.handle_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_bot_thread_engagement_does_not_cross_channels_with_same_timestamp(
    monkeypatch,
):
    adapter = _adapter()
    adapter.handle_message = AsyncMock()
    adapter._resolve_user_name = AsyncMock(return_value="Human")
    adapter._fetch_thread_context = AsyncMock(return_value="")
    adapter._fetch_thread_parent_text = AsyncMock(return_value="")
    client = adapter._team_clients["T0PRIMARY"]
    client.chat_postMessage = AsyncMock(return_value={"ts": "5.100001"})
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")
    monkeypatch.setenv("SLACK_REQUIRE_MENTION", "true")

    await adapter.send(
        "C_PRIMARY_A",
        "bot reply",
        metadata={"scope_id": "T0PRIMARY", "thread_id": "5.000010"},
    )

    base = {
        "type": "message",
        "team_id": "T0PRIMARY",
        "channel_type": "channel",
        "user": "U_HUMAN",
        "text": "unmentioned follow-up",
        "thread_ts": "5.000010",
    }
    await adapter._handle_slack_message(
        {**base, "channel": "C_PRIMARY_B", "ts": "5.000011"}
    )
    adapter.handle_message.assert_not_awaited()

    await adapter._handle_slack_message(
        {**base, "channel": "C_PRIMARY_A", "ts": "5.000012"}
    )
    adapter.handle_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_workspace_allowlist_does_not_leak_across_same_channel_id(monkeypatch):
    adapter = _adapter()
    adapter.config.extra["workspace_allowed_channels"] = {
        "T0PRIMARY": ["C_SHARED"],
        "T0SECONDARY": ["C_SECONDARY_ONLY"],
    }
    adapter.handle_message = AsyncMock()
    adapter._resolve_user_name = AsyncMock(return_value="Human")
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")

    base = {
        "type": "message",
        "channel": "C_SHARED",
        "channel_type": "channel",
        "user": "U_HUMAN",
        "text": "hello",
        "ts": "7.1",
    }
    await adapter._handle_slack_message(
        {**base, "team_id": "T0SECONDARY", "text": "<@U_SECONDARY_BOT> no"}
    )
    adapter.handle_message.assert_not_awaited()

    await adapter._handle_slack_message(
        {
            **base,
            "team_id": "T0PRIMARY",
            "text": "<@U_PRIMARY_BOT> yes",
            "ts": "7.2",
        }
    )
    adapter.handle_message.assert_awaited_once()


def test_assistant_thread_cache_is_workspace_scoped():
    adapter = _adapter()
    adapter._cache_assistant_thread_metadata(
        {
            "team_id": "T0PRIMARY",
            "channel_id": "D_SHARED",
            "thread_ts": "6.1",
            "user_id": "U_PRIMARY_HUMAN",
        }
    )
    adapter._cache_assistant_thread_metadata(
        {
            "team_id": "T0SECONDARY",
            "channel_id": "D_SHARED",
            "thread_ts": "6.1",
            "user_id": "U_SECONDARY_HUMAN",
        }
    )

    primary = adapter._lookup_assistant_thread_metadata(
        {"team_id": "T0PRIMARY"}, channel_id="D_SHARED", thread_ts="6.1"
    )
    secondary = adapter._lookup_assistant_thread_metadata(
        {"team_id": "T0SECONDARY"}, channel_id="D_SHARED", thread_ts="6.1"
    )
    assert primary["user_id"] == "U_PRIMARY_HUMAN"
    assert secondary["user_id"] == "U_SECONDARY_HUMAN"


@pytest.mark.asyncio
async def test_assistant_status_isolated_by_workspace_channel_and_thread():
    adapter = _adapter()
    client = adapter._team_clients["T0PRIMARY"]
    client.assistant_threads_setStatus = AsyncMock()
    first = {"scope_id": "T0PRIMARY", "thread_id": "6.100001"}
    second = {"scope_id": "T0PRIMARY", "thread_id": "6.100002"}

    await adapter.send_typing("C_SHARED", metadata=first)
    await adapter.send_typing("C_SHARED", metadata=second)
    await adapter.stop_typing("C_SHARED", metadata=first)
    await adapter.stop_typing("C_SHARED", metadata=second)

    cleared_threads = [
        call.kwargs["thread_ts"]
        for call in client.assistant_threads_setStatus.await_args_list
        if call.kwargs["status"] == ""
    ]
    assert cleared_threads == ["6.100001", "6.100002"]


def test_slash_context_is_workspace_scoped():
    adapter = _adapter()
    primary_key = ("T0PRIMARY", "C_SHARED", "U_HUMAN", "trigger-primary")
    secondary_key = ("T0SECONDARY", "C_SHARED", "U_HUMAN", "trigger-secondary")
    adapter._slash_command_contexts[primary_key] = {
        "response_url": "https://example.test/primary",
        "ts": 1e12,
    }
    adapter._slash_command_contexts[secondary_key] = {
        "response_url": "https://example.test/secondary",
        "ts": 1e12,
    }
    token = _slash_context_key.set(secondary_key)
    try:
        ctx = adapter._pop_slash_context(
            "C_SHARED", metadata={"scope_id": "T0SECONDARY"}
        )
    finally:
        _slash_context_key.reset(token)

    assert ctx["response_url"].endswith("/secondary")
    assert primary_key in adapter._slash_command_contexts


def test_slash_context_without_contextvar_never_crosses_workspaces():
    adapter = _adapter()
    primary_key = ("T0PRIMARY", "C_SHARED", "U_HUMAN", "trigger-primary")
    secondary_key = ("T0SECONDARY", "C_SHARED", "U_HUMAN", "trigger-secondary")
    adapter._slash_command_contexts[primary_key] = {
        "response_url": "https://example.test/primary",
        "ts": 1e12,
    }
    adapter._slash_command_contexts[secondary_key] = {
        "response_url": "https://example.test/secondary",
        "ts": 1e12,
    }

    assert _slash_context_key.get() is None
    assert (
        adapter._pop_slash_context(
            "C_SHARED", metadata={"scope_id": "T0SECONDARY"}
        )
        is None
    )
    assert set(adapter._slash_command_contexts) == {primary_key, secondary_key}


def test_legacy_slash_context_is_rejected_in_multi_workspace_mode():
    adapter = _adapter()
    legacy_key = ("_legacy", "C_SHARED", "U_HUMAN", "legacy-trigger")
    adapter._slash_command_contexts[legacy_key] = {
        "response_url": "https://example.test/legacy",
        "ts": 1e12,
    }

    token = _slash_context_key.set(legacy_key)
    try:
        ctx = adapter._pop_slash_context(
            "C_SHARED", metadata={"scope_id": "T0PRIMARY"}
        )
    finally:
        _slash_context_key.reset(token)

    assert ctx is None
    assert legacy_key in adapter._slash_command_contexts


@pytest.mark.asyncio
async def test_thread_context_trust_uses_exact_workspace_allowlist():
    adapter = _adapter()
    adapter.config.extra["workspace_allowed_users"] = {
        "T0PRIMARY": ["U_SHARED"],
        "T0SECONDARY": ["U_OTHER"],
    }
    replies = {
        "messages": [
            {"ts": "8.1", "user": "U_SHARED", "text": "workspace history"},
            {"ts": "8.2", "user": "U_TRIGGER", "text": "current"},
        ]
    }
    adapter._team_clients["T0PRIMARY"].conversations_replies = AsyncMock(
        return_value=replies
    )
    adapter._team_clients["T0SECONDARY"].conversations_replies = AsyncMock(
        return_value=replies
    )
    adapter._user_name_cache = {
        ("T0PRIMARY", "U_SHARED"): "Shared User",
        ("T0SECONDARY", "U_SHARED"): "Shared User",
    }

    primary = await adapter._fetch_thread_context(
        "C_SHARED", "8.1", "8.2", team_id="T0PRIMARY"
    )
    secondary = await adapter._fetch_thread_context(
        "C_SHARED", "8.1", "8.2", team_id="T0SECONDARY"
    )

    assert "[thread parent] Shared User: workspace history" in primary
    assert "[unverified]" not in primary
    assert "[thread parent] [unverified] Shared User: workspace history" in secondary
