"""Workspace-scoped Slack authorization contracts."""

from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.session import SessionSource


def _runner(workspace_allowed_users):
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(
        platforms={
            Platform.SLACK: PlatformConfig(
                enabled=True,
                extra={"workspace_allowed_users": workspace_allowed_users},
            )
        }
    )
    runner.pairing_store = SimpleNamespace(is_approved=lambda *_args: False)
    runner.adapters = {}
    return runner


def _source(team_id: str | None, user_id: str = "U_CHAD") -> SessionSource:
    return SessionSource(
        platform=Platform.SLACK,
        chat_id="C_SHARED",
        chat_type="group",
        user_id=user_id,
        scope_id=team_id,
    )


@pytest.fixture(autouse=True)
def _clear_legacy_slack_authorization(monkeypatch):
    for name in (
        "SLACK_ALLOWED_USERS",
        "SLACK_ALLOW_ALL_USERS",
        "GATEWAY_ALLOWED_USERS",
        "GATEWAY_ALLOW_ALL_USERS",
    ):
        monkeypatch.delenv(name, raising=False)


def test_workspace_allowlist_authorizes_only_the_matching_installation():
    runner = _runner(
        {
            "T_TEAM": ["U_CHAD"],
            "T_APOM": ["U_APOM_ADMIN"],
        }
    )

    assert runner._is_user_authorized(_source("T_TEAM")) is True
    assert runner._is_user_authorized(_source("T_APOM")) is False


def test_workspace_allowlist_missing_scope_fails_closed():
    runner = _runner({"T_TEAM": ["U_CHAD"]})

    assert runner._is_user_authorized(_source(None)) is False
    assert runner._is_user_authorized(_source("T_UNKNOWN")) is False


def test_workspace_allowlist_overrides_legacy_allow_all(monkeypatch):
    runner = _runner({"T_TEAM": ["U_CHAD"], "T_APOM": ["U_APOM_ADMIN"]})
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")
    monkeypatch.setenv("GATEWAY_ALLOW_ALL_USERS", "true")

    assert runner._is_user_authorized(_source("T_APOM")) is False


@pytest.mark.parametrize("invalid", [[], 123, None])
def test_workspace_allowlist_invalid_or_missing_team_entry_denies(invalid):
    runner = _runner({"T_TEAM": invalid})

    assert runner._is_user_authorized(_source("T_TEAM")) is False
