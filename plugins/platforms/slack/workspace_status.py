"""Public, profile-local Slack workspace health receipt.

The receipt is deliberately a tiny allow-listed schema.  It is consumed by
the Smith monitoring dashboard, so credentials, URLs, Slack user IDs, and raw
exception text must never cross this boundary.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

from utils import atomic_json_write


# v2: 채널별 수신기록 last_inbound_by_channel 추가(2026-07-23 자가치유 기획서 §2-2).
# Monitoring-dashboard 리더가 {1,2}를 수용하도록 먼저 배포된 뒤에만 2를 발행한다.
RECEIPT_SCHEMA_VERSION = 2
RECEIPT_FILENAME = "slack_workspace_status.json"

_STABLE_KEY_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
# Slack workspace IDs use an uppercase letter prefix followed by uppercase
# alphanumerics. Keep this byte-identical to Monitoring-dashboard's receipt
# consumer so a writer-accepted row can never become a schema error downstream.
_TEAM_ID_RE = re.compile(r"^[A-Z][A-Z0-9]{2,31}$")
# 채널 ID도 같은 표면(대문자 프리픽스+영숫자). Monitoring-dashboard 소비자의
# _CHANNEL_ID_RE와 byte-identical — writer가 수락한 키가 하류에서 스키마 에러가
# 되지 않게 한다. 형식 밖(channel 없음 포함)은 조용히 skip(워크스페이스 필드만 갱신).
_CHANNEL_ID_RE = re.compile(r"^[A-Z][A-Z0-9]{2,31}$")
# 채널 dict 무한 성장 방지 — 초과 시 가장 오래된 ts 항목을 제거한다.
_MAX_CHANNEL_ROWS = 32

# Finite codes keep the public receipt diagnostic without ever copying a raw
# exception (which can contain a token, URL, user ID, or response body).
SAFE_ERROR_CODES = frozenset(
    {
        "not_authed",
        "invalid_auth",
        "account_inactive",
        "token_revoked",
        "token_expired",
        "org_login_required",
        "team_not_found",
        "auth_test_failed",
        "socket_disconnected",
        "socket_error",
        "workspace_disabled",
        "heartbeat_error",
    }
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _stable_key(value: Any, *, label: str) -> str:
    key = str(value or "").strip().casefold()
    if not _STABLE_KEY_RE.fullmatch(key):
        raise ValueError(
            f"{label} must match [a-z][a-z0-9_-] and be at most 64 characters"
        )
    return key


def validate_workspace_keys(value: Any) -> Dict[str, str]:
    """Validate ``team_id -> stable workspace key`` structured config."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("slack.workspace_keys must be a mapping")

    result: Dict[str, str] = {}
    seen_keys: set[str] = set()
    for raw_team_id, raw_workspace_key in value.items():
        team_id = str(raw_team_id or "").strip()
        if not _TEAM_ID_RE.fullmatch(team_id):
            raise ValueError(
                "slack.workspace_keys contains an invalid or empty team_id"
            )
        workspace_key = _stable_key(
            raw_workspace_key, label="slack.workspace_keys value"
        )
        if workspace_key in seen_keys:
            raise ValueError(
                f"slack.workspace_keys contains duplicate workspace key: "
                f"{workspace_key}"
            )
        result[team_id] = workspace_key
        seen_keys.add(workspace_key)
    return result


def resolve_agent_key(configured: Any = None) -> str:
    """Resolve the receipt owner without exposing a process credential.

    An explicit structured config value wins, followed by the queue/gateway
    process identity and finally the active Hermes profile.
    """
    candidates = [
        configured,
        os.getenv("QUEUE_AGENT"),
        os.getenv("HERMES_GATEWAY_NAME"),
        os.getenv("HERMES_PROFILE"),
    ]
    for candidate in candidates:
        if str(candidate or "").strip():
            return _stable_key(candidate, label="Slack workspace status agent key")

    try:
        from hermes_cli.profiles import get_active_profile_name

        active = get_active_profile_name()
    except Exception:
        active = "default"
    return _stable_key(active or "default", label="active Hermes profile")


def validate_agent_key(value: Any) -> str:
    """Validate one explicit receipt agent key without environment fallback."""
    return _stable_key(value, label="slack.workspace_status_agent_key")


def classify_auth_failure(error: BaseException) -> str:
    """Map a Slack SDK auth failure to the public finite error vocabulary."""
    response = getattr(error, "response", None)
    code = None
    if response is not None and hasattr(response, "get"):
        try:
            code = response.get("error")
        except Exception:
            code = None
    code = str(code or "").strip()
    if code in {
        "invalid_auth",
        "account_inactive",
        "token_revoked",
        "token_expired",
        "org_login_required",
        "team_not_found",
    }:
        return code
    return "auth_test_failed"


@dataclass
class _WorkspaceState:
    workspace_key: str
    team_id: str
    connected: bool = False
    verified_at: Optional[str] = None
    heartbeat_at: Optional[str] = None
    last_inbound_at: Optional[str] = None
    error_code: Optional[str] = "not_authed"
    last_inbound_by_channel: Dict[str, str] = field(default_factory=dict)

    def public_dict(self) -> Dict[str, Any]:
        return {
            "workspace_key": self.workspace_key,
            "team_id": self.team_id,
            "connected": self.connected,
            "verified_at": self.verified_at,
            "heartbeat_at": self.heartbeat_at,
            "last_inbound_at": self.last_inbound_at,
            "error_code": self.error_code,
            "last_inbound_by_channel": dict(self.last_inbound_by_channel),
        }


class SlackWorkspaceStatusWriter:
    """Own and atomically publish one agent's workspace health receipt."""

    def __init__(
        self,
        *,
        workspace_keys: Any = None,
        agent_key: Any = None,
        path: Optional[Path] = None,
        clock: Callable[[], str] = _utc_now,
    ) -> None:
        self.workspace_keys = validate_workspace_keys(workspace_keys)
        self.agent_key = resolve_agent_key(agent_key)
        if path is None:
            # Import at construction time so profile/test overrides are honored.
            from hermes_constants import get_hermes_home

            path = get_hermes_home() / "state" / RECEIPT_FILENAME
        self.path = Path(path)
        self._clock = clock
        self._states: Dict[str, _WorkspaceState] = {
            team_id: _WorkspaceState(
                workspace_key=workspace_key,
                team_id=team_id,
            )
            for team_id, workspace_key in self.workspace_keys.items()
        }

    def _state_for(self, team_id: str) -> _WorkspaceState:
        team_id = str(team_id or "").strip()
        if not _TEAM_ID_RE.fullmatch(team_id):
            raise ValueError("Slack workspace status requires a valid team_id")
        state = self._states.get(team_id)
        if state is not None:
            return state
        if self.workspace_keys:
            # Never publish an unexpected workspace when an explicit expected
            # set exists; the consumer treats that as whole-receipt corruption.
            raise KeyError(f"Unexpected Slack workspace: {team_id}")
        state = _WorkspaceState(workspace_key=team_id.casefold(), team_id=team_id)
        self._states[team_id] = state
        return state

    def validate_authorized_workspaces(self, team_ids: set[str]) -> None:
        """Require the live authorization set to match configured keys exactly."""
        if self.workspace_keys and set(team_ids) != set(self.workspace_keys):
            raise ValueError("Authorized Slack workspaces do not match workspace_keys")

    def mark_connecting(self) -> None:
        self._mark_all_disconnected("not_authed")

    def mark_verified(self, team_id: str, *, write: bool = True) -> None:
        state = self._state_for(team_id)
        state.connected = False
        state.verified_at = self._clock()
        state.error_code = "socket_disconnected"
        if write:
            self.write()

    def mark_socket_connected(self) -> None:
        now = self._clock()
        for state in self._states.values():
            if state.verified_at is None:
                state.connected = False
                state.error_code = "not_authed"
                continue
            state.connected = True
            state.heartbeat_at = now
            state.error_code = None
        self.write()

    def mark_inbound(self, team_id: str, channel_id: Optional[str] = None) -> None:
        state = self._state_for(team_id)
        if state.verified_at is None:
            raise RuntimeError("Unverified Slack workspace cannot report inbound")
        now = self._clock()
        state.connected = True
        state.heartbeat_at = now
        state.last_inbound_at = now
        state.error_code = None
        channel_id = str(channel_id or "").strip()
        if _CHANNEL_ID_RE.fullmatch(channel_id):
            state.last_inbound_by_channel[channel_id] = now
            if len(state.last_inbound_by_channel) > _MAX_CHANNEL_ROWS:
                oldest = min(
                    state.last_inbound_by_channel,
                    key=lambda cid: state.last_inbound_by_channel[cid],
                )
                state.last_inbound_by_channel.pop(oldest, None)
        self.write()

    def mark_disconnected(self, error_code: str) -> None:
        self._mark_all_disconnected(error_code)
        self.write()

    def _mark_all_disconnected(self, error_code: str) -> None:
        if error_code not in SAFE_ERROR_CODES:
            raise ValueError("Unsafe Slack workspace status error code")
        for state in self._states.values():
            state.connected = False
            state.error_code = error_code

    def snapshot(self) -> Dict[str, Any]:
        rows = sorted(
            (state.public_dict() for state in self._states.values()),
            key=lambda item: (item["workspace_key"], item["team_id"]),
        )
        row_timestamps = [
            value
            for row in rows
            for value in (
                row["verified_at"],
                row["heartbeat_at"],
                row["last_inbound_at"],
                *row["last_inbound_by_channel"].values(),
            )
            if value is not None
        ]
        generated_at = self._clock()
        if row_timestamps:
            # ISO-8601 UTC timestamps use one fixed representation here, so
            # lexical ordering is chronological. Guard against a wall-clock
            # step backwards between state mutation and atomic publication.
            generated_at = max(generated_at, max(row_timestamps))
        payload = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "agent_key": self.agent_key,
            "generated_at": generated_at,
            "workspaces": rows,
        }
        # Defensive invariant: a healthy row never carries an error, while a
        # disconnected row always explains itself with a finite safe code.
        for row in rows:
            if (row["connected"] and row["error_code"] is not None) or (
                not row["connected"] and row["error_code"] is None
            ):
                raise AssertionError("Slack workspace receipt state conflict")
        return payload

    def write(self) -> None:
        atomic_json_write(
            self.path,
            self.snapshot(),
            mode=0o600,
            sort_keys=False,
        )
