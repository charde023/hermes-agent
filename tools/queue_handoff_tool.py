"""queue_handoff — 헤르메스 에이전트 간 DB 큐 handoff 도구(agent-callable).

현재 에이전트가 **Agent Directory에서 허용된 다른 Hermes 에이전트**에게 작업을
넘길 때 쓴다. ``conversation.v1``이 켜져 있으면 canonical event와 delivery
receipt를 쓰고, 꺼져 있으면 기존 QueueRepo v1 ``slack_inbox``에 적재한다.

send_message와의 차이: **외부 플랫폼(슬랙/텔레그램 등) 발신이 아니다.** 순수하게
큐 내부 에이전트 라우팅만 한다. 외부 채널로 보내려면 send_message/자동 응답 경로를
써야 하며, raw 슬랙 채널ID를 여기에 넘기면 거부된다.

발신자 정체성 = ``QUEUE_AGENT`` env(프로세스 고정). 현재 대화 스레드 =
``HERMES_SESSION_THREAD_ID``(있으면 이어감).

필수 env: QUEUE_AGENT / QUEUE_REPO_ROOT, 그리고 QUEUE_DB_PATH 또는 QUEUE_ENDPOINT.
선택 env: QUEUE_TOKEN. 로스터 SSOT는
``QUEUE_REPO_ROOT/contracts/examples/agent-directory.v1.json``이다. 해당 파일이
**없을 때만** 명시적 ``QUEUE_KNOWN_AGENTS``를 legacy fallback으로 쓴다.

conversation.v1 선택: ``QUEUE_PROTOCOL_VERSION=conversation.v1``과
``QUEUE_ENDPOINT``·``QUEUE_CONVERSATION_CREDENTIAL``이 필요하다. credential 값은
schema·오류·receipt·로컬 상태에 저장하지 않는다.

⚠️ legacy v1 수신측 인가(숨은 전제): handoff row는
slack_user_id=<발신 에이전트 키>로 박히고,
수신 에이전트 코어 authz는 default-deny다. 수신측이 자기 QUEUE_ALLOWED_SENDERS에
발신 키를 넣거나 QUEUE_ALLOW_ALL_USERS를 켜지 않으면 handoff가 "sender not allowed"로
error 마킹돼 소실된다 — 이 도구가 success를 반환해도 그렇다(인가 우회는 별도 이슈).

⚠️ F4(순서 제약, 문서화만 — 동작 변경 없음): terminal event_type
(completed/failed/no_action)은 인바운드 conversation.v1 턴이 아직 진행 중일 때
(= 최종 답을 보내기 전에) 호출하면 안 된다. 게이트웨이는 턴이 끝난 뒤 모델의
최종 답을 자동으로 ``reply`` canonical event로 append하는데, ledger는 이미
terminal(DONE)로 닫힌 conversation에 append를 거부한다 — 즉 턴 도중
completed/failed/no_action을 먼저 호출하면 정상적인 게이트웨이 auto-reply가
delivery error를 만든다. 종결은 요청자 쪽에서 걸거나, 더 이상 답을 만들지 않는
턴에서 호출해야 한다(종결 메커니즘 재설계는 P2).
"""

import asyncio
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from tools.registry import registry, tool_error, tool_result
from tools.queue_handoff_receipts import (
    QueueHandoffReceiptStore,
    ReceiptCollisionError,
)

# raw 슬랙 채널ID(C/G/D + 영숫자) 패턴 — 에이전트 키(소문자)가 아니라 채널ID가
# 잘못 넘어온 경우를 식별해 거부한다(어댑터 _route_and_insert와 동일 규칙).
_SLACK_CHANNEL_ID_RE = re.compile(r"^[CGD][A-Z0-9]{7,}$")

# 아웃바운드 handoff의 합성 event_ts/thread_ts 접두(슬랙 ts와 충돌 없는 값).
_HANDOFF_EVENT_PREFIX = "qh-"

_CONVERSATION_PROTOCOL = "conversation.v1"
_LEGACY_PROTOCOL = "queue.v1"
_IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_SLACK_TOKEN_RE = re.compile(r"xox(?:a|b|p|r|s)-[A-Za-z0-9-]+", re.IGNORECASE)
_DELIVERY_STATES = {"pending", "claimed", "accepted", "completed", "error"}
_TRUST_TIERS = {
    "internal",
    "observed",
    "invited",
    "trusted",
    "operator",
    "quarantined",
}
_EXECUTION_TRUST_TIERS = {"internal", "trusted", "operator"}

# 도구가 명시적으로 받는 event_type. 전체 canonical 스키마(opened/ack/reply/
# permission_*)엔 게이트웨이·시스템 전용 값이 더 있지만 그건 여기 노출하지 않는다
# — reply는 게이트웨이가 본문 답변에서 자동 생성하고, ack/opened/permission_*은
# 세션 lifecycle 신호라 에이전트가 직접 호출할 이유가 없다(계약 §4-2).
_EVENT_TYPES = {
    "request",
    "progress",
    "question",
    "completed",
    "failed",
    "no_action",
}
_DEFAULT_EVENT_TYPE = "request"

# insert_inbox는 어댑터 send()를 우회하므로 플랫폼 max_message_length(register의
# 40_000) 가드가 적용되지 않는다 — 여기서 동일 상한을 직접 강제한다.
_MAX_MESSAGE_LENGTH = 40_000


def _env(name: str) -> str:
    return (os.getenv(name, "") or "").strip()


class RosterError(ValueError):
    """Agent Directory 또는 명시적 legacy roster가 유효하지 않다."""


@dataclass(frozen=True)
class RosterTarget:
    agent_id: str
    display_name: str
    trust_tier: str


@dataclass(frozen=True)
class RosterSnapshot:
    sender_agent_id: str
    sender_trust_tier: str
    targets: Mapping[str, RosterTarget]
    source: str


def _directory_path(repo_root: str) -> Path:
    return (
        Path(repo_root)
        / "contracts"
        / "examples"
        / "agent-directory.v1.json"
    )


def _validate_directory_contract(directory: object, repo_root: str) -> None:
    """slack_agent의 **공유** Directory 계약을 그대로 실행한다.

    Hermes 쪽에 축약 schema를 복제하면 중앙 계약의 ``additionalProperties``나
    workspace/installation cross-record 의미론이 바뀔 때 roster 인가가 더 넓어질
    수 있다. 그래서 ``QUEUE_REPO_ROOT`` 아래 production validator를 정확한 파일
    경로로 로드하고, 그 validator가 고정한 JSON Schema checksum + public JSON +
    semantic gate를 모두 통과시킨다. 파일/함수/의존성이 없거나 어느 gate든
    실패하면 Directory가 존재하는 상태이므로 legacy roster로 후퇴하지 않는다.
    """
    root = Path(repo_root)
    validator_path = root / "bridge" / "conversation_contracts.py"
    if not validator_path.is_file():
        raise RosterError("Agent Directory shared validator is unavailable")

    # 일반 ``import bridge...``는 다른 QUEUE_REPO_ROOT에서 먼저 import된 module을
    # sys.modules cache로 재사용할 수 있다. 보안 경계이므로 설정된 repo의 정확한
    # production 파일을 고유 module name으로 로드한다.
    module_name = (
        "_hermes_queue_directory_contracts_"
        + hashlib.sha256(str(validator_path.resolve()).encode("utf-8")).hexdigest()
        + f"_{id(directory):x}"
    )
    try:
        spec = importlib.util.spec_from_file_location(module_name, validator_path)
        if spec is None or spec.loader is None:
            raise ImportError("shared validator module spec is unavailable")
        module = importlib.util.module_from_spec(spec)
        previous = sys.modules.get(module_name)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        finally:
            if previous is None:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = previous

        validate_schema = getattr(module, "validate_directory_schema")
        validate_semantics = getattr(module, "validate_directory")
        validate_schema(directory, root / "contracts")
        validate_semantics(directory)
    except Exception as exc:
        raise RosterError("Agent Directory contract is invalid") from exc


def _load_directory_roster(repo_root: str, sender_agent_id: str) -> RosterSnapshot | None:
    """검증된 Session 0/1 fixture에서 sender별 queue_handoff peer를 파생한다.

    파일 **부재**만 ``None``이다. 파일이 있는데 JSON/semantic이 깨졌으면 legacy
    env로 우회하지 않고 fail closed한다.
    """
    path = _directory_path(repo_root)
    if not path.is_file():
        return None
    try:
        directory = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RosterError("Agent Directory is invalid") from exc
    _validate_directory_contract(directory, repo_root)
    if (
        not isinstance(directory, dict)
        or directory.get("contract_version") != "1.0"
        or not isinstance(directory.get("agents"), list)
    ):
        raise RosterError("Agent Directory contract is invalid")

    agents: dict[str, dict] = {}
    for raw in directory["agents"]:
        if not isinstance(raw, dict):
            raise RosterError("Agent Directory agent record is invalid")
        agent_id = str(raw.get("agent_id") or "").strip().casefold()
        if not agent_id or agent_id in agents:
            raise RosterError("Agent Directory agent identity is invalid")
        if raw.get("protocol_version") != _CONVERSATION_PROTOCOL:
            raise RosterError("Agent Directory protocol version is unsupported")
        capabilities = raw.get("capabilities")
        allowed_peers = raw.get("allowed_peers")
        if not isinstance(capabilities, list) or not isinstance(allowed_peers, list):
            raise RosterError("Agent Directory capability/peer fields are invalid")
        if not all(isinstance(value, str) and value for value in capabilities):
            raise RosterError("Agent Directory capability is invalid")
        if not all(isinstance(value, str) and value for value in allowed_peers):
            raise RosterError("Agent Directory peer is invalid")
        if raw.get("trust_tier") not in _TRUST_TIERS:
            raise RosterError("Agent Directory trust tier is invalid")
        agents[agent_id] = raw

    sender_key = sender_agent_id.casefold()
    sender = agents.get(sender_key)
    if sender is None:
        raise RosterError(f"Agent Directory has no sender '{sender_key}'")
    if "queue_handoff" not in sender["capabilities"]:
        raise RosterError("Agent Directory sender lacks queue_handoff capability")
    if sender.get("trust_tier") not in _EXECUTION_TRUST_TIERS:
        raise RosterError("Agent Directory sender trust tier cannot execute handoff")

    allowed = {str(value).strip().casefold() for value in sender["allowed_peers"]}
    targets: dict[str, RosterTarget] = {}
    for target_id in sorted(allowed):
        target = agents.get(target_id)
        if target is None:
            raise RosterError("Agent Directory allowed peer is missing")
        if (
            "queue_handoff" not in target["capabilities"]
            or target.get("transport") not in {"queue_native", "hybrid"}
            or target.get("trust_tier") not in _EXECUTION_TRUST_TIERS
        ):
            continue
        targets[target_id] = RosterTarget(
            agent_id=target_id,
            display_name=str(target.get("display_name") or target_id),
            trust_tier=str(target.get("trust_tier") or "observed"),
        )
    return RosterSnapshot(
        sender_agent_id=sender_key,
        sender_trust_tier=str(sender.get("trust_tier") or "observed"),
        targets=targets,
        source="agent_directory",
    )


def _resolve_roster(
    *, agent: str | None = None, repo_root: str | None = None
) -> RosterSnapshot:
    agent = (agent or _env("QUEUE_AGENT")).casefold()
    repo_root = repo_root or _env("QUEUE_REPO_ROOT")
    directory = _load_directory_roster(repo_root, agent)
    if directory is not None:
        return directory

    # 기존 QueueRepo v1 설치를 위한 명시적 fallback. 기본 5명 목록은 제거한다.
    raw = _env("QUEUE_KNOWN_AGENTS")
    keys = sorted({value.strip().casefold() for value in raw.split(",") if value.strip()})
    if not keys:
        raise RosterError(
            "Agent Directory is missing and QUEUE_KNOWN_AGENTS legacy fallback is not set"
        )
    return RosterSnapshot(
        sender_agent_id=agent,
        sender_trust_tier="internal",
        targets={
            key: RosterTarget(key, key, "internal")
            for key in keys
            if key != agent
        },
        source="legacy_env",
    )


def _known_agents() -> set[str]:
    """호환용 helper. runtime 검증과 dynamic schema가 같은 roster를 읽는다."""
    return set(_resolve_roster().targets)


def _protocol_version() -> str:
    raw = _env("QUEUE_PROTOCOL_VERSION")
    if not raw:
        return _LEGACY_PROTOCOL
    if raw not in {_LEGACY_PROTOCOL, _CONVERSATION_PROTOCOL}:
        raise ValueError(f"unsupported queue protocol version: {raw}")
    return raw


def _redact_error(exc: BaseException, *secrets: str) -> str:
    text = str(exc)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    text = _SLACK_TOKEN_RE.sub("[REDACTED]", text)
    return text[:500]


def check_queue_handoff() -> bool:
    """큐가 활성(필수 env + local db 또는 HTTP endpoint)일 때만 도구를 노출한다."""
    try:
        protocol = _protocol_version()
    except ValueError:
        return False
    common = bool(_env("QUEUE_AGENT") and _env("QUEUE_REPO_ROOT"))
    if protocol == _CONVERSATION_PROTOCOL:
        return bool(
            common
            and _env("QUEUE_ENDPOINT")
            and _env("QUEUE_CONVERSATION_CREDENTIAL")
        )
    return bool(common and (_env("QUEUE_DB_PATH") or _env("QUEUE_ENDPOINT")))


def _do_insert(
    repo_root: str,
    db_path: str,
    *,
    endpoint: str,
    token: str,
    event_ts: str,
    channel_id: str,
    thread_ts: str,
    agent: str,
    message: str,
    target: str,
) -> bool:
    """blocking 큐 삽입 — asyncio.to_thread로 감싸 호출한다.

    QUEUE_REPO_ROOT를 sys.path에 **append**(insert(0) 금지 — slack_agent 루트의
    config.py 시크릿이 전역 최우선 import 되는 걸 막는다)한 뒤 bridge 패키지를
    import 한다.
    """
    if repo_root and repo_root not in sys.path:
        sys.path.append(repo_root)
    from bridge.agent_repo import make_agent_queue_repo

    repo = make_agent_queue_repo(db_path=db_path, endpoint=endpoint, token=token)
    return repo.insert_inbox(
        slack_event_ts=event_ts,
        channel_id=channel_id,
        thread_ts=thread_ts,
        slack_user_id=agent,
        text=message,
        target=target,
    )


def _do_conversation_append(
    repo_root: str,
    *,
    endpoint: str,
    credential: str,
    event: Mapping,
) -> dict:
    """blocking conversation.v1 append/query. credential은 반환·상태에 저장하지 않는다."""
    if repo_root and repo_root not in sys.path:
        sys.path.append(repo_root)
    from bridge.conversation_http import HttpConversationClient

    result = HttpConversationClient(endpoint, credential).call(
        "append_event", {"event": dict(event)}
    )
    if not isinstance(result, dict):
        raise RuntimeError("conversation API returned an invalid receipt")
    return result


def _canonical_hash(value: Mapping) -> str:
    raw = json.dumps(
        dict(value), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _validated_idempotency_key(value: object) -> str:
    key = str(value or "").strip()
    if not key or _IDEMPOTENCY_KEY_RE.fullmatch(key) is None:
        raise ValueError(
            "idempotency_key must match [A-Za-z0-9][A-Za-z0-9._:-]{0,199}"
        )
    return key


def _canonical_caller_key(agent: str, value: object) -> str:
    """caller key를 sender namespace에 고정해 중앙 ledger의 전역 UNIQUE 충돌을 막는다."""
    key = _validated_idempotency_key(value)
    prefix = f"queue_handoff:{agent}:"
    suffix = key[len(prefix):] if key.startswith(prefix) else ""
    if len(suffix) == 64 and all(ch in "0123456789abcdef" for ch in suffix):
        return key
    digest = hashlib.sha256(f"caller:{key}".encode("utf-8")).hexdigest()
    return f"{prefix}{digest}"


def _stable_thread_id(
    *, agent: str, target: str, message: str, args: Mapping, source_anchor: str
) -> str:
    from gateway.session_context import get_session_env

    explicit = str(args.get("thread") or "").strip()
    current = get_session_env("HERMES_SESSION_THREAD_ID", "")
    if explicit or current:
        return explicit or current
    seed = {
        "agent": agent,
        "target": target,
        "source_anchor": source_anchor,
        "explicit_idempotency_key": str(args.get("idempotency_key") or ""),
        "message": message,
    }
    return f"{_HANDOFF_EVENT_PREFIX}thread-{_canonical_hash(seed)[:32]}"


def _request_identity(
    *,
    agent: str,
    target: str,
    message: str,
    thread_ts: str,
    source_anchor: str,
    event_type: str = _DEFAULT_EVENT_TYPE,
) -> dict:
    identity = {
        "sender_agent_id": agent,
        "target_agent_id": target,
        "message": message,
        "thread_ts": thread_ts,
        "source_anchor": source_anchor,
    }
    # event_type=="request"(기본값)일 땐 키 자체를 넣지 않는다 — 구버전이 발급한
    # idempotency_key/재시도와 계속 같은 해시를 내야 하기 때문. progress/completed
    # 등 명시적 값일 때만 해시에 반영해, 같은 to/message/thread라도 event_type이
    # 다르면 서로 다른 idempotency_key(다른 canonical event)로 갈라지게 한다.
    if event_type != _DEFAULT_EVENT_TYPE:
        identity["event_type"] = event_type
    return identity


def _stable_idempotency_key(request: Mapping, explicit: object = None) -> str:
    agent = str(request["sender_agent_id"])
    if explicit is not None and str(explicit).strip():
        return _canonical_caller_key(agent, explicit)
    return f"queue_handoff:{agent}:{_canonical_hash(request)}"


def _build_conversation_event(
    *,
    request: Mapping,
    idempotency_key: str,
    sender_trust_tier: str,
    event_type: str = _DEFAULT_EVENT_TYPE,
    parent_conversation_id: str | None = None,
    parent_event_id: str | None = None,
) -> dict:
    """canonical conversation.v1 이벤트 순수 빌더. 컨텍스트/env는 호출부에서 읽는다.

    D2(계약확정서): 대화 정체성은 스레드 1:1이고 **방향 무관** — sender/target을
    conversation_id 산식에 넣지 않는다(같은 thread_ts면 A→B든 B→A든 같은
    conversation). 답신(부모 conversation_id가 있는 인바운드 v2 턴 안의 handoff)은
    새 conversation_id를 발급하지 않고 부모를 그대로 승계하며, causation_id는
    부모 event_id를 가리킨다.
    """
    agent = str(request["sender_agent_id"])
    target = str(request["target_agent_id"])
    thread_ts = str(request["thread_ts"])
    source_anchor = str(request.get("source_anchor") or "")
    message = str(request["message"])
    key_digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    event_id = f"qh_evt_{key_digest[:32]}"

    if parent_conversation_id:
        conversation_id = parent_conversation_id
        causation_id = parent_event_id or (source_anchor or None)
    else:
        conversation_digest = _canonical_hash({"thread_ts": thread_ts})
        conversation_id = f"qh_conv_{conversation_digest[:32]}"
        causation_id = source_anchor or None

    summary = " ".join(message.split())[:200] or "queue handoff request"
    return {
        "contract_version": "1.0",
        "conversation_id": conversation_id,
        "source_conversation_id": None,
        "workspace_key": "internal",
        "slack_team_id": None,
        "slack_enterprise_id": None,
        "channel_scope": f"queue:{conversation_id}",
        "event_id": event_id,
        "event_type": event_type,
        "sender_agent_id": agent,
        "target_agent_ids": [target],
        "correlation_id": conversation_id,
        "causation_id": causation_id,
        "idempotency_key": idempotency_key,
        "body": message,
        "summary": summary,
        "delivery_status": "pending",
        "slack_channel_id": None,
        "slack_thread_ts": None,
        "slack_message_ts": None,
        "slack_app_id": None,
        "slack_bot_user_id": None,
        "slack_bot_id": None,
        "slack_installation_id": None,
        "publisher_agent_id": None,
        "source_event_id": f"queue:{source_anchor or event_id}",
        "projected_by": "not_projected",
        "protocol_version": _CONVERSATION_PROTOCOL,
        "capability": "queue_handoff",
        "trust_tier": sender_trust_tier,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _conversation_result(
    *, target: str, idempotency_key: str, receipt: Mapping
) -> str:
    status = str(receipt.get("delivery_status") or "")
    if status not in _DELIVERY_STATES:
        return tool_error(
            "conversation API returned an invalid delivery status",
            idempotency_key=idempotency_key,
        )
    return tool_result(
        success=status != "error",
        target=target,
        message_id=str(receipt.get("event_id") or ""),
        idempotency_key=idempotency_key,
        protocol_version=_CONVERSATION_PROTOCOL,
        receipt=str(receipt.get("receipt") or "enqueued"),
        delivery="enqueued",
        delivery_status=status,
        accepted=status in {"accepted", "completed"},
        completed=status == "completed",
        duplicate=not bool(receipt.get("inserted", False)),
    )


async def queue_handoff_tool(args: dict, **kw) -> str:
    """다른 헤르메스 에이전트에게 큐 handoff. 반환 = JSON 문자열."""
    args = args or {}
    agent = _env("QUEUE_AGENT").casefold()
    if not agent:
        return tool_error("QUEUE_AGENT is not set — cannot identify sender agent")

    db_path = _env("QUEUE_DB_PATH")
    endpoint = _env("QUEUE_ENDPOINT")
    token = _env("QUEUE_TOKEN")
    conversation_credential = _env("QUEUE_CONVERSATION_CREDENTIAL")
    repo_root = _env("QUEUE_REPO_ROOT")
    if not repo_root or (not db_path and not endpoint):
        return tool_error(
            "queue is not configured (QUEUE_REPO_ROOT and QUEUE_DB_PATH or QUEUE_ENDPOINT missing)"
        )
    try:
        protocol = _protocol_version()
    except ValueError as exc:
        return tool_error(str(exc))
    if protocol == _CONVERSATION_PROTOCOL:
        if not endpoint:
            return tool_error("conversation.v1 requires QUEUE_ENDPOINT")
        if not conversation_credential:
            return tool_error(
                "conversation.v1 requires QUEUE_CONVERSATION_CREDENTIAL"
            )

    action = str(args.get("action") or "handoff").strip().casefold()
    if action not in {"handoff", "receipt"}:
        return tool_error("action must be 'handoff' or 'receipt'")

    # receipt 조회는 최초 호출 때 profile-local DB에 보존한 byte-identical event를
    # append_event로 다시 보낸다. S1 ledger는 duplicate receipt에 현재 aggregate
    # delivery_status를 돌려주므로 별도 unauthenticated query endpoint가 필요 없다.
    if action == "receipt":
        if protocol != _CONVERSATION_PROTOCOL:
            return tool_error(
                "accepted/completed receipt tracking requires conversation.v1"
            )
        try:
            idempotency_key = _canonical_caller_key(
                agent, args.get("idempotency_key")
            )
            store = await asyncio.to_thread(QueueHandoffReceiptStore)
            record = await asyncio.to_thread(store.get, idempotency_key)
        except (OSError, sqlite3.Error) as exc:
            return tool_error(f"handoff receipt store failed: {_redact_error(exc)}")
        except ValueError as exc:
            return tool_error(str(exc))
        if record is None:
            return tool_error(
                "unknown idempotency_key — no local handoff receipt event",
                idempotency_key=idempotency_key,
            )
        if record.protocol_version != _CONVERSATION_PROTOCOL:
            return tool_error(
                "accepted/completed receipt tracking requires conversation.v1",
                idempotency_key=idempotency_key,
            )
        event = record.event
        if event.get("sender_agent_id") != agent:
            return tool_error("stored handoff sender does not match QUEUE_AGENT")
        targets = event.get("target_agent_ids") or []
        target = str(targets[0]).casefold() if len(targets) == 1 else ""
        try:
            roster = _resolve_roster(agent=agent, repo_root=repo_root)
        except RosterError as exc:
            return tool_error(str(exc))
        if target not in roster.targets:
            return tool_error(
                f"unknown agent key '{target}' — eligible targets: {sorted(roster.targets)}"
            )
        try:
            receipt = await asyncio.to_thread(
                _do_conversation_append,
                repo_root,
                endpoint=endpoint,
                credential=conversation_credential,
                event=event,
            )
            if receipt.get("event_id") != event.get("event_id"):
                raise RuntimeError("conversation receipt event identity mismatch")
            status = str(receipt.get("delivery_status") or "")
            if status in _DELIVERY_STATES:
                await asyncio.to_thread(store.update_status, idempotency_key, status)
        except Exception as exc:  # noqa: BLE001 — JSON 도구 계약으로 격리
            return tool_error(
                f"handoff receipt request failed: "
                f"{_redact_error(exc, conversation_credential, token)}",
                idempotency_key=idempotency_key,
            )
        return _conversation_result(
            target=target, idempotency_key=idempotency_key, receipt=receipt
        )

    to = (args.get("to") or "").strip()
    message = args.get("message")

    # event_type: conversation.v1 전용 canonical 어휘(계약 §4-2). legacy queue.v1엔
    # event vocabulary가 없으므로 명시됐는데 v1이면 조용히 무시하지 않고 즉시 거부.
    event_type_raw = str(args.get("event_type") or "").strip().casefold()
    if event_type_raw:
        if protocol != _CONVERSATION_PROTOCOL:
            return tool_error(
                "event_type requires conversation.v1; legacy queue.v1 has no "
                "canonical event vocabulary"
            )
        if event_type_raw not in _EVENT_TYPES:
            return tool_error(
                f"event_type must be one of {sorted(_EVENT_TYPES)} — got "
                f"'{event_type_raw}'"
            )
    event_type = event_type_raw or _DEFAULT_EVENT_TYPE

    # --- 방어(삽입 없이 거부) ---
    if not to:
        return tool_error("empty target")
    if not (message and str(message).strip()):
        return tool_error("empty message")
    if _SLACK_CHANNEL_ID_RE.match(to):
        return tool_error(
            "looks like a raw slack channel id, not a queue handoff target "
            "(pass an Agent Directory key, not a Slack channel id)"
        )
    # 대소문자 무시 self-check + canonical화: 'Chami'가 self 가드를 우회해
    # 아무도 claim 못 하는 죽은 row가 되는 걸 막는다(claim은 소문자 target 정확일치).
    to = to.casefold()
    if to == agent.casefold():
        return tool_error("cannot handoff to self")
    # 알려진 에이전트 키만 허용 — 오타·환각이 pending으로 영구 잔류하며
    # success로 위장되는 걸 막는다.
    try:
        roster = _resolve_roster(agent=agent, repo_root=repo_root)
    except RosterError as exc:
        return tool_error(str(exc))
    if to not in roster.targets:
        return tool_error(
            f"unknown agent key '{to}' — eligible targets: {sorted(roster.targets)}"
        )

    message = str(message)
    if len(message) > _MAX_MESSAGE_LENGTH:
        return tool_error(
            f"message too long ({len(message)} chars > {_MAX_MESSAGE_LENGTH})"
        )

    # 스레드: 명시 인자 > 현재 세션 스레드 > 요청 identity 기반 합성 ID.
    # HERMES_SESSION_CHAT_ID(채널 식별자)는 thread_ts 폴백에서 제외한다 —
    # 채널ID를 thread_ts로 쓰면 같은 채널에서 발생한 서로 무관한 handoff들이
    # 수신측 한 세션으로 뭉쳐 격리가 깨진다(session_context.py: CHAT_ID=채널).
    from gateway.session_context import get_session_env

    source_anchor = (
        get_session_env("HERMES_SESSION_MESSAGE_ID", "")
        or get_session_env("HERMES_SESSION_KEY", "")
    )
    thread_ts = _stable_thread_id(
        agent=agent,
        target=to,
        message=message,
        args=args,
        source_anchor=source_anchor,
    )
    request = _request_identity(
        agent=agent,
        target=to,
        message=message,
        thread_ts=thread_ts,
        source_anchor=source_anchor,
        event_type=event_type,
    )
    try:
        idempotency_key = _stable_idempotency_key(
            request, args.get("idempotency_key")
        )
    except ValueError as exc:
        return tool_error(str(exc))
    request_hash = _canonical_hash(request)

    try:
        store = await asyncio.to_thread(QueueHandoffReceiptStore)
        if protocol == _CONVERSATION_PROTOCOL:
            # 답신(인바운드 v2 턴 안에서의 handoff)은 새 conversation_id를 발급하지
            # 않고 부모 conversation을 그대로 승계한다(D2). 큐 어댑터가
            # set_conversation_context로 싣고 get_session_env가 os.environ 폴백까지
            # 포함해 읽는다.
            parent_conversation_id = (
                get_session_env("HERMES_SESSION_CONVERSATION_ID", "") or None
            )
            parent_event_id = (
                get_session_env("HERMES_SESSION_CONVERSATION_EVENT_ID", "") or None
            )
            candidate_event = _build_conversation_event(
                request=request,
                idempotency_key=idempotency_key,
                sender_trust_tier=roster.sender_trust_tier,
                event_type=event_type,
                parent_conversation_id=parent_conversation_id,
                parent_event_id=parent_event_id,
            )
            record, _ = await asyncio.to_thread(
                store.reserve,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                protocol_version=protocol,
                event=candidate_event,
                initial_status="pending",
            )
            event = record.event
            receipt = await asyncio.to_thread(
                _do_conversation_append,
                repo_root,
                endpoint=endpoint,
                credential=conversation_credential,
                event=event,
            )
            if receipt.get("event_id") != event.get("event_id"):
                raise RuntimeError("conversation receipt event identity mismatch")
            status = str(receipt.get("delivery_status") or "")
            if status in _DELIVERY_STATES:
                await asyncio.to_thread(store.update_status, idempotency_key, status)
            return _conversation_result(
                target=to, idempotency_key=idempotency_key, receipt=receipt
            )

        event_digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        legacy_event = {
            **request,
            "event_ts": f"{_HANDOFF_EVENT_PREFIX}{event_digest}",
            "channel_id": f"queue:handoff:{agent}->{to}",
        }
        record, _ = await asyncio.to_thread(
            store.reserve,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            protocol_version=protocol,
            event=legacy_event,
            initial_status="enqueued",
        )
        legacy_event = record.event
        inserted = await asyncio.to_thread(
            _do_insert,
            repo_root,
            db_path,
            endpoint=endpoint,
            token=token,
            event_ts=legacy_event["event_ts"],
            channel_id=legacy_event["channel_id"],
            thread_ts=legacy_event["thread_ts"],
            agent=agent,
            message=legacy_event["message"],
            target=to,
        )
        await asyncio.to_thread(store.update_status, idempotency_key, "enqueued")
    except ReceiptCollisionError as exc:
        return tool_error(str(exc), idempotency_key=idempotency_key)
    except Exception as exc:  # noqa: BLE001 — 어떤 실패든 도구 계약(JSON error)로 변환
        return tool_error(
            f"handoff request failed: "
            f"{_redact_error(exc, conversation_credential, token)}",
            idempotency_key=idempotency_key,
        )

    # legacy v1은 enqueued 이후 aggregate receipt를 조회할 인증 표면이 없다.
    # stable event_ts로 재시도 중복만 흡수하고, accepted/completed는 v2 전용이다.
    return tool_result(
        success=True,
        target=to,
        message_id=legacy_event["event_ts"],
        idempotency_key=idempotency_key,
        protocol_version=_LEGACY_PROTOCOL,
        thread_ts=thread_ts,
        delivery="enqueued",
        delivery_status="enqueued",
        accepted=False,
        completed=False,
        duplicate=not bool(inserted),
        note=(
            "legacy QueueRepo v1 confirms enqueue only; enable conversation.v1 "
            "to track accepted/completed receipts"
        ),
    )


QUEUE_HANDOFF_SCHEMA = {
    "name": "queue_handoff",
    "description": (
        "Hand off work to another Agent Directory peer through the internal queue, "
        "or check a previously returned delivery receipt. This never sends directly "
        "to Slack, Telegram, or another external platform. The runtime schema lists "
        "the targets currently permitted for this sender."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["handoff", "receipt"],
                "description": (
                    "Optional; default handoff. Use receipt with a previously returned "
                    "idempotency_key to refresh accepted/completed state."
                ),
            },
            "to": {
                "type": "string",
                "description": (
                    "Target Agent Directory key for action=handoff. Must be an eligible "
                    "queue_handoff peer, not yourself or a raw Slack channel id."
                ),
            },
            "message": {
                "type": "string",
                "description": (
                    "The handoff content — the task/instruction for the target "
                    "agent. Include enough context to act without this conversation."
                ),
            },
            "thread": {
                "type": "string",
                "description": (
                    "Optional. Existing thread_ts to thread this handoff under. "
                    "Omit to derive a stable thread from the current session/request."
                ),
            },
            "idempotency_key": {
                "type": "string",
                "pattern": "^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$",
                "description": (
                    "Optional stable caller key for handoff retries; the tool derives one "
                    "when omitted. Required for action=receipt. Reusing a key with different "
                    "target/message/thread is rejected as a collision."
                ),
            },
            "event_type": {
                "type": "string",
                "enum": [
                    "request",
                    "progress",
                    "question",
                    "completed",
                    "failed",
                    "no_action",
                ],
                "description": (
                    "Optional; default request. Use completed/failed/no_action to "
                    "explicitly close out a handoff (contract Sec 4-2) — closure is "
                    "never inferred automatically. The reply event type is generated "
                    "automatically by the gateway for inline answers and is not "
                    "exposed here. conversation.v1 only; legacy queue.v1 has no "
                    "canonical event vocabulary and rejects this field instead of "
                    "silently ignoring it. IMPORTANT ordering constraint: terminal "
                    "events (completed/failed/no_action) must not be sent during an "
                    "active inbound conversation turn before your final answer — the "
                    "gateway auto-appends your final answer as a `reply` event AFTER "
                    "the turn, and the ledger rejects appends to an already-terminal "
                    "conversation. Close from the requester side instead, or only use "
                    "a terminal event_type in a turn where you produce no further "
                    "answer."
                ),
            },
        },
        "anyOf": [
            {"required": ["to", "message"]},
            {
                "required": ["action", "idempotency_key"],
                "properties": {"action": {"const": "receipt"}},
            },
        ],
    },
}


def _dynamic_queue_handoff_schema() -> dict:
    """매 tool-definition 생성 시 Directory와 같은 roster로 설명을 갱신한다."""
    try:
        roster = _resolve_roster()
        labels = [
            f"{target.agent_id} ({target.display_name})"
            for target in roster.targets.values()
        ]
        rendered = ", ".join(labels) if labels else "none"
        source = "Agent Directory" if roster.source == "agent_directory" else "legacy env"
        suffix = (
            f" Current eligible targets for {roster.sender_agent_id} from {source}: "
            f"{rendered}."
        )
    except Exception as exc:  # schema 생성 실패가 registry 전체를 깨면 안 된다.
        rendered = "unavailable"
        suffix = f" Current eligible targets are unavailable ({type(exc).__name__})."

    parameters = json.loads(json.dumps(QUEUE_HANDOFF_SCHEMA["parameters"]))
    parameters["properties"]["to"]["description"] = (
        parameters["properties"]["to"]["description"]
        + f" Eligible now: {rendered}."
    )
    return {
        "description": QUEUE_HANDOFF_SCHEMA["description"] + suffix,
        "parameters": parameters,
    }


# --- Registry (모듈 최상위 등록 → discover_builtin_tools가 AST로 자동 발견·import) ---
registry.register(
    name="queue_handoff",
    toolset="queue",  # hermes-queue 플러그인-플랫폼 번들에 자동 편입(toolset=="queue")
    schema=QUEUE_HANDOFF_SCHEMA,
    handler=lambda args, **kw: queue_handoff_tool(args, **kw),  # 코루틴 반환 → is_async
    check_fn=check_queue_handoff,
    is_async=True,
    description="현재 에이전트가 다른 헤르메스 에이전트에게 DB 큐로 handoff. 외부 플랫폼 발신 아님.",
    emoji="📬",
    dynamic_schema_overrides=_dynamic_queue_handoff_schema,
)
