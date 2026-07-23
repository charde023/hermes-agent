"""Queue platform adapter — slack_agent queue <-> Hermes 게이트웨이 브리지.

slack_agent 레포(bridge.local_repo.SQLiteQueueRepo)의 slack_inbox를 폴링해
세션 턴을 claim 하고, 코어(handle_message)로 넘긴 뒤 결과를 slack_outbox에
INSERT 한다(실제 슬랙 발신은 slack_bridge.py 센더가 담당).

내구성 의미론 (at-least-once):
- 코어 handle_message는 백그라운드 태스크를 스폰하고 즉시 리턴하므로,
  done/error 마킹과 세션 락 해제는 코어의 처리완료 훅
  on_processing_complete(SUCCESS/FAILURE/CANCELLED)에서 수행한다.
  즉 done = "에이전트 런 + 응답 발신까지 끝남"이다.
- 처리 중 프로세스가 죽으면 row는 'claimed'로 남고, 폴링 루프의 주기적
  reclaim(reclaim_stale_claimed)이 CLAIM_TTL 경과 후 pending으로 복구한다.
  따라서 재시작 시 유실 대신 재처리(드물게 중복 응답 가능)가 일어난다.

기본 ``queue.v1``은 QUEUE_AGENT / QUEUE_REPO_ROOT와 QUEUE_DB_PATH 또는
QUEUE_ENDPOINT를 사용한다. ``QUEUE_PROTOCOL_VERSION=conversation.v1``은
QUEUE_ENDPOINT와 별도 per-agent QUEUE_CONVERSATION_CREDENTIAL을 필수로 사용한다.
두 프로토콜은 같은 어댑터 안에서 선택되지만 저장·인증 계약은 섞지 않는다.
"""

import asyncio
import contextvars
import hashlib
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    ProcessingOutcome,
    SendResult,
)
from gateway.config import Platform, PlatformConfig
from gateway.conversation_closeout import mark_turn_active, mark_turn_closed
from gateway.session_context import reset_conversation_context, set_conversation_context

logger = logging.getLogger(__name__)

# done/error 마킹이 에이전트 런 전체(응답 발신 포함)를 커버하므로, TTL이 짧으면
# 장시간 턴 도중 락 만료·reclaim으로 중복 실행이 난다 — 600s에서 상향.
CLAIM_TTL_SECONDS = 1800
# reclaim은 매 폴링이 아니라 이 간격으로 throttle(러너와 동일 패턴).
RECLAIM_INTERVAL_SECONDS = 60
# 0/음수 폴링 간격은 공유 라이브 SQLite(slack_bridge와 공유) 상대 busy-loop이
# 되므로 하한을 강제한다.
MIN_POLL_INTERVAL_SECONDS = 0.2
_LEGACY_PROTOCOL = "queue.v1"
_CONVERSATION_PROTOCOL = "conversation.v1"
_SUPPORTED_PROTOCOLS = {_LEGACY_PROTOCOL, _CONVERSATION_PROTOCOL}
# canonical event의 delivery_status 유효집합(schema enum과 동일) — append_event
# 영수증이 이 밖의 값을 반환하면 서버/스키마 드리프트로 보고 실패 처리한다.
_DELIVERY_STATES = {"pending", "claimed", "accepted", "completed", "error"}
# F1: claim된 canonical event 중 실제로 코어 dispatch(+ack)를 받을 event_type만
# 화이트리스트한다. 그 외(ack/progress/reply/completed/failed/no_action/
# opened/permission_requested/permission_resolved 등)는 서버가 인바운드
# delivery row도 만드는 관측(observability) 전용 신호다 — 여기에 무조건 ack를
# 걸고 코어로도 넘기면, 서로 ack를 주고받는 두 v2 어댑터 사이에 ack→ack→ack…
# 무한 핑퐁이 생긴다(request 1건이 6홉까지 증식 재현). 에이전트에게 이런 신호를
# 어떻게 보여줄지(가시화)는 P2 설계 — 지금은 ledger·projector가 관측을 담당하고
# 여기선 claimed→accepted→completed로 조용히 삼킨다(ack 없음, dispatch 없음).
_ACTIONABLE_EVENT_TYPES = frozenset({"request", "question"})
# ack/reply producer event의 capability 필드 — tools/queue_handoff_tool.py의
# 아웃바운드 handoff와 동일 값을 써서 두 producer가 같은 계약을 공유함을 표시한다.
_PRODUCER_CAPABILITY = "queue_handoff"

# F5: producer(어댑터) 노이즈 게이트 — bridge.conversation_render.is_system_noise
# 를 그대로 재사용하지 않는다. 그쪽은 substring-anywhere 판정(예: 'Codex gpt-',
# 'caps context at' 이 본문 어디에 있든 매치)이라 모델 얘기를 하는 *진짜 답변*
# ('지금 Codex gpt-5 계열로 라우팅 중이야')까지 event 자체를 소멸시킨다 — 표시측
# (renderer) 오탐 강등과 달리 여기서 오탐은 데이터 손실이다. 그래서 producer
# 게이트는 첫 줄 prefix만 보는 고정밀 판정으로 좁힌다. 광범위한 노이즈 강등은
# 표시측(renderer) 소관(§6-2).
_PRODUCER_NOISE_PREFIXES = ("⏳", ":hourglass", "Working —", "◐ ")


def _is_producer_noise(text: str) -> bool:
    """producer가 event 자체를 만들지 말지 판정 — 첫 줄 prefix 판정만 한다.

    substring-anywhere 판정과 달리 오탐이 실답변 소멸로 이어지지 않는다 —
    status ping은 실제로 그 문구로 "시작"하지, 본문 중간에 우연히 섞이지
    않는다는 전제다.
    """
    stripped = (text or "").strip()
    if not stripped:
        return False
    first_line = stripped.splitlines()[0]
    return first_line.startswith(_PRODUCER_NOISE_PREFIXES)

# send() 라우팅 접두 규약.
#   "queue:<slack채널>"  → 자동 응답(코어가 인바운드 턴에 답) → slack_outbox.
#   그 외(에이전트 키)    → 아웃바운드 handoff → 상대 target의 slack_inbox.
# 인바운드 턴은 build_source에서 이 접두로 정규화되므로(_dispatch_turn) 자기
# 채널로의 응답이 handoff로 오분류되지 않는다. slack_bridge 센더는 raw 슬랙
# 채널만 발송 허용(allowed_channel_ids 게이트)하므로 outbox 삽입 직전 접두를 벗긴다.
_REPLY_CHANNEL_PREFIX = "queue:"
# handoff 응답은 다시 상대에게 큐로 보내되, 상대가 그 답에 또 답하지 않도록
# 별도 합성채널로 표시한다. 수신측은 이 채널을 no-reply sink로 라우팅한다.
_HANDOFF_REPLY_TARGET_PREFIX = "handoff-reply:"
_HANDOFF_CHANNEL_PREFIX = f"{_REPLY_CHANNEL_PREFIX}handoff:"
_HANDOFF_REPLY_CHANNEL_PREFIX = f"{_REPLY_CHANNEL_PREFIX}handoff-reply:"
_NO_REPLY_CHANNEL = f"{_REPLY_CHANNEL_PREFIX}no-reply"
# 아웃바운드 handoff의 합성 event_ts/thread_ts 접두(슬랙 ts와 충돌 없는 고유값).
_HANDOFF_EVENT_PREFIX = "qho-"
# raw 슬랙 채널ID(C/G/D + 영숫자) 패턴 — 에이전트 키가 아니라 채널ID가 큐
# handoff target으로 잘못 넘어온 경우를 식별한다. 에이전트 키는 소문자라
# (chami/chadol/mei/anna/jeff) 이 대문자 접두 패턴에 매치되지 않는다.
_SLACK_CHANNEL_ID_RE = re.compile(r"^[CGD][A-Z0-9]{7,}$")

# BasePlatformAdapter.handle_message()는 실제 처리 태스크를 spawn한 뒤 즉시
# 돌아온다. ContextVar는 그 spawn 시점에 claim 식별자를 자식 태스크로 복사해
# 주므로, 최종 send()가 원래 DB 턴 소유권을 다시 확인할 수 있다.
_ACTIVE_QUEUE_EVENT_ID: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "queue_active_event_id",
    default=None,
)
# conversation.v1은 event ID만으로 소유권을 증명하지 않는다. claim 때 발급된
# active_delivery_token을 처리 task context까지 같이 운반해 stale worker가
# no-reply send 경계를 통과하지 못하게 한다.
_ACTIVE_QUEUE_DELIVERY_TOKEN: contextvars.ContextVar[Optional[str]] = (
    contextvars.ContextVar("queue_active_delivery_token", default=None)
)


@dataclass(frozen=True)
class _ConversationTurn:
    """검증을 통과해 ``accepted``로 전이된 canonical delivery claim."""

    event: Mapping[str, Any]
    event_id: str
    conversation_id: str
    target_agent_id: str
    worker: str
    active_delivery_token: str
    lease_expires_at: datetime


def _normalize_reply_channel(channel: str) -> str:
    """인바운드 채널을 자동응답용 'queue:<채널>' 형태로 정규화(멱등)."""
    channel = channel or ""
    if channel.startswith(_REPLY_CHANNEL_PREFIX):
        return channel
    return _REPLY_CHANNEL_PREFIX + channel


def _parse_handoff_channel(channel: str, *, reply: bool = False) -> tuple[str, str] | None:
    """합성 handoff 채널이면 (sender, target)을 반환한다.

    일반 handoff: ``queue:handoff:<sender>-><target>``
    handoff 응답: ``queue:handoff-reply:<sender>-><target>``
    """
    prefix = _HANDOFF_REPLY_CHANNEL_PREFIX if reply else _HANDOFF_CHANNEL_PREFIX
    channel = (channel or "").strip()
    if not channel.startswith(prefix):
        return None
    rest = channel[len(prefix):]
    if "->" not in rest:
        return None
    sender, target = (part.strip() for part in rest.split("->", 1))
    if not sender or not target:
        return None
    return sender, target


def _route_and_insert(repo, agent: str, target: str, content: str, thread_hint) -> Dict[str, Any]:
    """큐 send 라우팅(blocking) — send()와 _standalone_send() 공용.

    - target이 'queue:' 접두면 자동 응답 → insert_outbox(raw 채널로 접두 제거).
    - 그 외(에이전트 키) → 아웃바운드 handoff → insert_inbox(target=상대).

    반환은 standalone_sender_fn 계약(dict)과 동일: {"success": True, "message_id": ...}
    또는 {"error": str}. 방어: 빈 target·자기 자신 target·raw 슬랙 채널ID는
    삽입 없이 에러.

    ⚠️ 발신자 인가 결합(숨은 전제): handoff row는 slack_user_id=<발신 에이전트
    키>(예 'chami')로 박힌다. 수신 에이전트의 코어 authz는 default-deny라,
    수신측이 자기 QUEUE_ALLOWED_SENDERS에 이 발신 에이전트 키를 넣거나
    QUEUE_ALLOW_ALL_USERS를 켜지 않으면 handoff가 "sender not allowed"로 error
    마킹되고 소실된다. 즉 여기서 success=True가 나도 수신측 게이팅에 따라 조용히
    버려질 수 있다(코드 우회는 M2 — 지금은 이 전제만 문서화). """
    target = (target or "").strip()
    if not target:
        return {"error": "empty target"}

    if target == _NO_REPLY_CHANNEL:
        # handoff-reply 수신 후 모델이 답을 만들어도 여기서 성공 no-op 처리해
        # "답의 답" 무한왕복을 끊는다. 성공으로 반환해야 원 inbound가 done 된다.
        return {"success": True, "message_id": "queue:no-reply"}

    if target.startswith(_HANDOFF_CHANNEL_PREFIX) or target.startswith(_HANDOFF_REPLY_CHANNEL_PREFIX):
        # 합성 handoff 채널은 슬랙 발신 채널이 아니다. 코어/스트리밍/후처리 경로가
        # 원 source.chat_id(예: queue:handoff-reply:chami->chadol)로 한 번 더
        # deliver를 시도해도 outbox에 넣지 말고 성공 no-op으로 삼킨다. 실제 답은
        # _dispatch_turn의 handoff-reply:<sender> 경로가 이미 상대 inbox로 보냈다.
        return {"success": True, "message_id": "queue:handoff-synthetic-noop"}

    if target.startswith(_REPLY_CHANNEL_PREFIX):
        channel = target[len(_REPLY_CHANNEL_PREFIX):]
        row_id = repo.insert_outbox(
            channel_id=channel,
            thread_ts=str(thread_hint or ""),
            text=content,
            created_by=f"queue:{agent}",
        )
        return {"success": True, "message_id": str(row_id)}

    is_handoff_reply = False
    if target.startswith(_HANDOFF_REPLY_TARGET_PREFIX):
        is_handoff_reply = True
        target = target[len(_HANDOFF_REPLY_TARGET_PREFIX):].strip()
        if not target:
            return {"error": "empty handoff reply target"}

    if target == agent:
        # 자기 자신에게 handoff = 무한 루프 위험 → 삽입 없이 거부.
        return {"error": "cannot handoff to self"}

    if _SLACK_CHANNEL_ID_RE.match(target):
        # raw 슬랙 채널ID('C0B69KP8G2J' 등)는 handoff target(에이전트 키)이 아니다.
        # 그대로 insert하면 아무 워커도 처리 못 하는 죽은 row가 생기고, 발신자에겐
        # success로 보여 실패가 은폐된다. 자동 응답이라면 'queue:<채널>' 접두를
        # 써야 outbox로 간다 → 여기선 삽입 없이 명시적으로 거부한다.
        return {"error": "looks like a raw slack channel id, not a queue handoff target"}

    event_ts = f"{_HANDOFF_EVENT_PREFIX}{uuid.uuid4()}"
    thread_ts = str(thread_hint) if thread_hint else f"{_HANDOFF_EVENT_PREFIX}{uuid.uuid4()}"
    inserted = repo.insert_inbox(
        slack_event_ts=event_ts,
        channel_id=(
            f"{_HANDOFF_REPLY_CHANNEL_PREFIX}{agent}->{target}"
            if is_handoff_reply
            else f"{_HANDOFF_CHANNEL_PREFIX}{agent}->{target}"
        ),
        thread_ts=thread_ts,
        slack_user_id=agent,
        text=content,
        target=target,
    )
    if not inserted:
        # event_ts는 row 식별자일 뿐이며 매 send마다 새 uuid라, 재시도 시엔 새
        # event_ts가 생겨 이 UNIQUE 충돌 분기는 사실상 안 탄다(= at-least-once,
        # 재시도가 중복 handoff를 만들 수 있음). 따라서 이 분기는 "재시도 흡수"가
        # 아니라 동일 event_ts를 두 번 넣는 드문 경우(테스트·호출자 uuid 고정)만
        # 방어한다. 진짜 idempotency는 M2에서 검토.
        return {"error": "duplicate handoff"}
    return {"success": True, "message_id": event_ts}


def check_queue_requirements() -> bool:
    """Queue 어댑터는 Python stdlib(sqlite3)만 사용한다 — 추가 의존성 없음."""
    return True


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


class QueueAdapter(BasePlatformAdapter):
    """slack_agent 로컬 SQLite 큐를 폴링하는 게이트웨이 어댑터."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("queue"))

        extra = config.extra or {}
        self._db_path = (os.getenv("QUEUE_DB_PATH", "") or str(extra.get("db_path", ""))).strip()
        self._endpoint = (os.getenv("QUEUE_ENDPOINT", "") or str(extra.get("endpoint", ""))).strip()
        self._token = (os.getenv("QUEUE_TOKEN", "") or str(extra.get("token", ""))).strip()
        self._agent = (os.getenv("QUEUE_AGENT", "") or str(extra.get("agent", ""))).strip()
        self._repo_root = (os.getenv("QUEUE_REPO_ROOT", "") or str(extra.get("repo_root", ""))).strip()
        self._protocol_version = (
            os.getenv("QUEUE_PROTOCOL_VERSION", "")
            or str(extra.get("protocol_version", ""))
            or _LEGACY_PROTOCOL
        ).strip()
        # credential 값은 config.yaml extra로 받지 않는다. 프로필 private env에만
        # 두어 config dump/plugin receipt/log로 새는 표면을 줄인다.
        self._conversation_credential = os.getenv(
            "QUEUE_CONVERSATION_CREDENTIAL", ""
        ).strip()
        self._poll_interval = max(
            MIN_POLL_INTERVAL_SECONDS, _float_env("QUEUE_POLL_INTERVAL", 2.0)
        )
        # pid 접미: 같은 agent로 이중 기동돼도 release_session_lock의
        # locked_by=worker 가드가 상대 프로세스의 락을 풀지 않게.
        self._worker = f"queue-adapter-{self._agent}-{os.getpid()}"
        self._repo = None
        self._conversation_client = None
        self._conversation_directory: Optional[Mapping[str, Any]] = None
        self._poll_task: Optional[asyncio.Task] = None
        # 코어로 인계된 in-flight 턴: v1 SessionTurn 또는 v2 _ConversationTurn.
        # on_processing_complete에서 protocol별 상태 전이를 마감한다.
        self._inflight: Dict[str, Any] = {}
        self._last_reclaim = 0.0

        logger.info(
            "[Queue] Adapter initialized (protocol=%s, db=%s, endpoint=%s, agent=%s)",
            self._protocol_version,
            self._db_path,
            self._endpoint,
            self._agent,
        )

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """필수 설정 검증 후 bridge repo를 열고 폴링 태스크를 시작한다."""
        if self._protocol_version not in _SUPPORTED_PROTOCOLS:
            message = f"Unsupported QUEUE_PROTOCOL_VERSION: {self._protocol_version}"
            logger.error("[Queue] %s", message)
            self._set_fatal_error(
                "queue_unsupported_protocol", message, retryable=False
            )
            return False

        required = [
            ("QUEUE_AGENT", self._agent),
            ("QUEUE_REPO_ROOT", self._repo_root),
        ]
        if self._protocol_version == _CONVERSATION_PROTOCOL:
            required.extend(
                [
                    ("QUEUE_ENDPOINT", self._endpoint),
                    (
                        "QUEUE_CONVERSATION_CREDENTIAL",
                        self._conversation_credential,
                    ),
                ]
            )
        elif not self._endpoint:
            required.insert(0, ("QUEUE_DB_PATH", self._db_path))
        missing = [name for name, value in required if not value]
        if missing:
            message = (
                "Not configured — missing "
                + ", ".join(missing)
                + ". Set the QUEUE_* env vars (or platforms.queue in config.yaml)."
            )
            logger.error("[Queue] %s", message)
            # 설정 누락은 non-retryable — 빈 설정 상대로 무한 재접속을 막는다.
            self._set_fatal_error(
                "queue_missing_configuration", message, retryable=False
            )
            return False

        # bridge 패키지(slack_agent 레포)를 import 가능하게 — idempotent.
        # append(insert(0) 금지): slack_agent 루트엔 최상위 config.py(시크릿) 등
        # 흔한 이름의 모듈이 있어, 최우선 경로로 두면 향후 어떤 코드의 bare
        # `import config` 한 줄로 시크릿 모듈이 조용히 로드된다.
        if self._repo_root not in sys.path:
            sys.path.append(self._repo_root)
        try:
            if self._protocol_version == _CONVERSATION_PROTOCOL:
                def _load_conversation():
                    from bridge.conversation_contracts import (
                        validate_directory,
                        validate_directory_schema,
                    )
                    from bridge.conversation_http import HttpConversationClient

                    contracts_dir = Path(self._repo_root) / "contracts"
                    directory_path = (
                        contracts_dir / "examples" / "agent-directory.v1.json"
                    )
                    directory = json.loads(directory_path.read_text(encoding="utf-8"))
                    if not isinstance(directory, dict):
                        raise ValueError("Agent Directory root must be an object")
                    validate_directory_schema(directory, contracts_dir)
                    validate_directory(directory)
                    client = HttpConversationClient(
                        endpoint=self._endpoint,
                        credential=self._conversation_credential,
                    )
                    return client, directory

                (
                    self._conversation_client,
                    self._conversation_directory,
                ) = await asyncio.to_thread(_load_conversation)
            else:
                def _load_repo():
                    from bridge.agent_repo import make_agent_queue_repo

                    return make_agent_queue_repo(
                        db_path=self._db_path,
                        endpoint=self._endpoint,
                        token=self._token,
                    )

                # 생성자가 sqlite connect + DDL(blocking, busy 시 최대 30초)을
                # 수행하므로 이벤트루프 밖(스레드)에서 만든다.
                self._repo = await asyncio.to_thread(_load_repo)
        except Exception as e:
            message = (
                f"Failed to load slack_agent queue protocol "
                f"(protocol={self._protocol_version}, "
                f"QUEUE_REPO_ROOT={self._repo_root}, QUEUE_DB_PATH={self._db_path}, "
                f"QUEUE_ENDPOINT={self._endpoint}): {e}"
            )
            logger.error("[Queue] %s", message, exc_info=True)
            self._set_fatal_error("queue_repo_import_failed", message, retryable=False)
            return False

        self._mark_connected()
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info(
            "[Queue] Connected — polling %s as target '%s' via %s (interval %.2fs)",
            self._endpoint or self._db_path,
            self._agent,
            self._protocol_version,
            self._poll_interval,
        )
        return True

    async def _poll_loop(self) -> None:
        """상시 폴링 루프 — 어떤 예외에도 죽지 않는다(최외곽 가드)."""
        while self._running:
            processed = False
            try:
                await self._maybe_reclaim()
                processed = await self._poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[Queue] Poll error: %s", e, exc_info=True)
            if not processed:
                await asyncio.sleep(self._poll_interval)

    async def _maybe_reclaim(self) -> None:
        """크래시/취소로 'claimed'에 고착된 row를 pending으로 복구한다.

        claim과 done/error 마킹 사이에 프로세스가 죽으면 아무도 되돌리지
        않으므로(러너의 reaper는 자기 target만 회수) 여기서 주기적으로
        재수거한다. 기준은 세션 락 TTL과 동일(CLAIM_TTL_SECONDS) — 락이
        만료된 row만 되돌아오므로 정상 처리 중인 턴은 건드리지 않는다.
        """
        if self._protocol_version == _CONVERSATION_PROTOCOL:
            # v2 claim_delivery가 pending뿐 아니라 lease-expired
            # claimed/accepted까지 원자적으로 재claim한다.
            return
        now = time.monotonic()
        if self._last_reclaim and now - self._last_reclaim < RECLAIM_INTERVAL_SECONDS:
            return
        self._last_reclaim = now
        recovered = await asyncio.to_thread(
            self._repo.reclaim_stale_claimed,
            target=self._agent,
            older_than_seconds=CLAIM_TTL_SECONDS,
        )
        if recovered:
            logger.warning(
                "[Queue] Reclaimed %d stale claimed row(s) (older than %ds)",
                recovered, CLAIM_TTL_SECONDS,
            )

    async def _poll_once(self) -> bool:
        """큐에서 세션 턴 하나를 claim해 처리한다. 처리했으면 True.

        SQLiteQueueRepo는 동기 blocking이므로 모든 repo 호출은 asyncio.to_thread
        로 스레드에 내린다(이벤트루프 블로킹 금지).
        """
        if self._protocol_version == _CONVERSATION_PROTOCOL:
            return await self._poll_conversation_once()

        turn = await asyncio.to_thread(
            self._repo.claim_session_turn,
            target=self._agent,
            worker=self._worker,
            ttl_seconds=CLAIM_TTL_SECONDS,
        )
        if turn is None:
            return False
        handed_off = False
        try:
            handed_off = await self._process_turn(turn)
        finally:
            # 코어로 인계되지 못한 턴(발신자 불허·디스패치 실패·취소)만 여기서
            # 즉시 락 해제. 인계된 턴은 on_processing_complete가 done/error
            # 마킹과 함께 해제한다 — 그동안 같은 세션의 후속 턴은 SQLite
            # pending에 내구성 있게 대기한다(인메모리 busy 큐로 안 흘러감).
            if not handed_off:
                try:
                    await asyncio.to_thread(
                        self._repo.release_session_lock,
                        session_id=turn.session_id,
                        worker=self._worker,
                    )
                except Exception:
                    logger.exception(
                        "[Queue] Failed to release session lock (session_id=%s)",
                        turn.session_id,
                    )
        return True

    async def _conversation_call(
        self, method: str, params: Mapping[str, Any]
    ) -> Optional[Mapping[str, Any]]:
        """동기 HttpConversationClient를 이벤트루프 밖에서 호출한다."""
        if self._conversation_client is None:
            raise RuntimeError("conversation client not connected")
        result = await asyncio.to_thread(
            self._conversation_client.call, method, dict(params)
        )
        if result is not None and not isinstance(result, Mapping):
            raise RuntimeError("conversation API returned a non-object result")
        return result

    def _own_trust_tier(self, fallback: str) -> str:
        """Agent Directory에서 자기(self._agent) 레코드의 trust_tier를 조회한다.

        디렉터리는 connect()에서 스키마·의미 검증을 이미 거쳤으므로 정상 경로에선
        반드시 self._agent 레코드가 있다. 그래도 조회 실패(테스트 fixture가 빈
        디렉터리를 주입하는 경우 등)엔 인바운드 event의 trust_tier로 폴백해
        producer event 생성 자체가 막히지 않게 한다.
        """
        directory = self._conversation_directory
        agents = directory.get("agents") if isinstance(directory, Mapping) else None
        if isinstance(agents, list):
            for agent in agents:
                if isinstance(agent, Mapping) and agent.get("agent_id") == self._agent:
                    tier = agent.get("trust_tier")
                    if tier:
                        return str(tier)
        return fallback

    def _build_producer_event(
        self,
        turn: _ConversationTurn,
        *,
        event_type: str,
        body: str,
        idempotency_key: str,
    ) -> dict:
        """인바운드 turn.event에서 승계해 ack/reply canonical event를 만든다.

        tools/queue_handoff_tool.py:_build_conversation_event와 동일한 event_id
        파생(qh_evt_{sha256(idempotency_key)[:32]})과 31필드 schema를 쓴다 — 서버
        ledger가 handoff 도구의 아웃바운드 event와 여기 producer event를 구분하지
        않고 같은 schema로 검증하기 때문이다.

        F3(byte-stability): ``created_at``은 인바운드 ``turn.event["created_at"]``
        을 그대로 승계한다(없을 때만 now() 폴백) — 매번 새로 찍으면 안 된다.
        서버 dedupe는 같은 idempotency_key에 대해 payload_hash(event 전체 직렬화)
        가 동일해야만 duplicate로 흡수하고, 다르면 ValueError('idempotency
        collision')로 영구 error가 된다. reclaim 뒤 같은 turn으로 이 함수를 다시
        불러 재전송할 때 event가 byte-identical해야 그 dedupe가 성립한다 — 이
        전제가 깨지면(예: LLM이 본문을 재생성해 body 자체가 달라지는 크래시
        -재생성 케이스) 여전히 collision이 날 수 있다(그 잔여 케이스는 P2
        receipt-store 검토 대상).
        """
        inbound = turn.event
        workspace_key = str(inbound.get("workspace_key") or "internal")
        channel_scope = str(
            inbound.get("channel_scope") or f"queue:{turn.conversation_id}"
        )
        correlation_id = str(inbound.get("correlation_id") or turn.conversation_id)
        sender = str(inbound.get("sender_agent_id") or "")
        trust_tier = self._own_trust_tier(str(inbound.get("trust_tier") or ""))
        key_digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        event_id = f"qh_evt_{key_digest[:32]}"
        summary = " ".join(body.split())[:200] or event_type
        return {
            "contract_version": "1.0",
            "conversation_id": turn.conversation_id,
            "source_conversation_id": None,
            "workspace_key": workspace_key,
            "slack_team_id": None,
            "slack_enterprise_id": None,
            "channel_scope": channel_scope,
            "event_id": event_id,
            "event_type": event_type,
            "sender_agent_id": self._agent,
            "target_agent_ids": [sender],
            "correlation_id": correlation_id,
            "causation_id": turn.event_id,
            "idempotency_key": idempotency_key,
            "body": body,
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
            "source_event_id": f"queue:{event_id}",
            "projected_by": "not_projected",
            "protocol_version": _CONVERSATION_PROTOCOL,
            "capability": _PRODUCER_CAPABILITY,
            "trust_tier": trust_tier,
            "created_at": str(
                inbound.get("created_at") or datetime.now(timezone.utc).isoformat()
            ),
        }

    async def _send_ack(self, turn: _ConversationTurn) -> None:
        """인바운드 수신 시 ack event를 자동 1회 append한다(§4-1 ACK 계약).

        idempotency_key를 turn.event_id에서 안정적으로 파생시켜(재claim/재수신
        때도 동일) 서버 ledger의 idempotency dedupe가 "자동 1회"를 보장한다 —
        같은 키로 두 번 append해도 동일 payload면 ledger가 중복으로 흡수한다.
        fail-soft: ack 실패가 턴 처리 자체를 막으면 안 되므로 예외는 warning
        로그만 남기고 삼킨다(dispatch는 이어서 계속된다).
        """
        idempotency_key = f"queue_ack:{self._agent}:{turn.event_id}"
        event = self._build_producer_event(
            turn, event_type="ack", body="ack", idempotency_key=idempotency_key
        )
        try:
            await self._conversation_call("append_event", {"event": event})
        except Exception as exc:  # noqa: BLE001 — ack는 fail-soft, 턴 처리는 계속
            logger.warning(
                "[Queue] Ack append failed (event=%s): %s", turn.event_id, exc
            )

    async def _poll_conversation_once(self) -> bool:
        """canonical delivery 하나를 claim하고 accepted 뒤 코어로 인계한다."""
        claim = await self._conversation_call(
            "claim_delivery",
            {
                "target_agent_id": self._agent,
                "worker": self._worker,
                "ttl_seconds": CLAIM_TTL_SECONDS,
            },
        )
        if claim is None:
            return False

        try:
            turn = await self._validate_conversation_claim(claim)
        except Exception as exc:
            logger.error("[Queue] Invalid conversation claim payload: %s", exc)
            await self._error_owned_conversation_claim(claim)
            return True

        try:
            await self._advance_conversation_delivery(
                turn,
                expected_state="claimed",
                next_state="accepted",
            )
        except Exception as exc:
            # server가 token/lease를 최종 판정한다. accepted 실패 claim을 코어에
            # 넘기지 않고, 새 owner/reclaim 경로가 이어받게 둔다.
            logger.warning(
                "[Queue] Conversation claim acceptance fenced (event=%s): %s",
                turn.event_id,
                exc,
            )
            return True

        event_type = str(turn.event.get("event_type") or "")
        if event_type not in _ACTIONABLE_EVENT_TYPES:
            # F1: ack/progress/reply/completed/failed/no_action/opened/
            # permission_* 등 observability 전용 event — ack도 걸지 않고 코어로도
            # 넘기지 않는다(무한 ack 핑퐁 차단). 조용히 claimed→accepted→completed
            # 로만 소비한다.
            logger.info(
                "[Queue] Consuming observability event without ack/dispatch "
                "(event=%s, event_type=%s)",
                turn.event_id,
                event_type,
            )
            try:
                await self._advance_conversation_delivery(
                    turn,
                    expected_state="accepted",
                    next_state="completed",
                )
            except Exception as exc:
                logger.warning(
                    "[Queue] Conversation observability-event completion fenced "
                    "(event=%s): %s",
                    turn.event_id,
                    exc,
                )
            return True

        # accepted 전이 성공 직후·코어 인계 전 — ack는 fail-soft라 실패해도
        # 아래 dispatch는 계속된다(_send_ack 내부에서 예외를 삼킴).
        await self._send_ack(turn)

        try:
            await self._dispatch_conversation_turn(turn)
        except BaseException as exc:
            logger.error(
                "[Queue] Conversation turn dispatch failed (event=%s): %s",
                turn.event_id,
                exc,
                exc_info=True,
            )
            try:
                await self._advance_conversation_delivery(
                    turn,
                    expected_state="accepted",
                    next_state="error",
                )
            except Exception:
                logger.warning(
                    "[Queue] Conversation error transition fenced (event=%s)",
                    turn.event_id,
                    exc_info=True,
                )
        return True

    async def _validate_conversation_claim(
        self, claim: Mapping[str, Any]
    ) -> _ConversationTurn:
        """claim envelope와 canonical event를 독립 재검증한다."""
        if not isinstance(claim, Mapping):
            raise ValueError("claim must be an object")
        event = claim.get("event")
        if not isinstance(event, Mapping):
            raise ValueError("claim event payload is required")

        event_id = str(claim.get("event_id") or "").strip()
        conversation_id = str(claim.get("conversation_id") or "").strip()
        target_agent_id = str(claim.get("target_agent_id") or "").strip()
        worker = str(claim.get("worker") or "").strip()
        active_token = str(claim.get("active_delivery_token") or "").strip()
        lease_raw = claim.get("lease_expires_at")
        if not all(
            (event_id, conversation_id, target_agent_id, worker, active_token, lease_raw)
        ):
            raise ValueError("claim identity fields are required")
        if target_agent_id != self._agent:
            raise ValueError("claim target does not match adapter agent")
        if worker != self._worker:
            raise ValueError("claim worker does not match adapter worker")
        try:
            lease_expires_at = datetime.fromisoformat(
                str(lease_raw).replace("Z", "+00:00")
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("claim lease_expires_at is invalid") from exc
        if lease_expires_at.tzinfo is None or lease_expires_at.utcoffset() is None:
            raise ValueError("claim lease_expires_at must be timezone-aware")
        lease_expires_at = lease_expires_at.astimezone(timezone.utc)

        canonical_event = json.loads(
            json.dumps(dict(event), ensure_ascii=False, sort_keys=True)
        )
        if canonical_event.get("event_id") != event_id:
            raise ValueError("claim/event event_id mismatch")
        if canonical_event.get("conversation_id") != conversation_id:
            raise ValueError("claim/event conversation_id mismatch")
        if canonical_event.get("protocol_version") != _CONVERSATION_PROTOCOL:
            raise ValueError("claim event protocol mismatch")
        targets = canonical_event.get("target_agent_ids")
        if not isinstance(targets, list) or self._agent not in targets:
            raise ValueError("adapter agent is not an event target")
        if self._conversation_directory is None:
            raise ValueError("Agent Directory is not loaded")

        def _validate():
            from bridge.conversation_contracts import (
                validate_event,
                validate_event_schema,
            )

            contracts_dir = Path(self._repo_root) / "contracts"
            validate_event_schema(canonical_event, contracts_dir)
            validate_event(canonical_event, self._conversation_directory)

        await asyncio.to_thread(_validate)
        return _ConversationTurn(
            event=canonical_event,
            event_id=event_id,
            conversation_id=conversation_id,
            target_agent_id=target_agent_id,
            worker=worker,
            active_delivery_token=active_token,
            lease_expires_at=lease_expires_at,
        )

    async def _error_owned_conversation_claim(
        self, claim: Mapping[str, Any]
    ) -> None:
        """invalid payload도 server가 token 소유권을 확인할 때만 error로 전이."""
        if not isinstance(claim, Mapping):
            return
        event_id = str(claim.get("event_id") or "").strip()
        target = str(claim.get("target_agent_id") or "").strip()
        worker = str(claim.get("worker") or "").strip()
        token = str(claim.get("active_delivery_token") or "").strip()
        if (
            not event_id
            or target != self._agent
            or worker != self._worker
            or not token
        ):
            return
        try:
            await self._conversation_call(
                "advance_delivery",
                {
                    "event_id": event_id,
                    "target_agent_id": self._agent,
                    "expected_state": "claimed",
                    "next_state": "error",
                    "active_token": token,
                },
            )
        except Exception:
            logger.warning(
                "[Queue] Invalid claim error transition fenced (event=%s)",
                event_id,
                exc_info=True,
            )

    async def _advance_conversation_delivery(
        self,
        turn: _ConversationTurn,
        *,
        expected_state: str,
        next_state: str,
    ) -> Mapping[str, Any]:
        result = await self._conversation_call(
            "advance_delivery",
            {
                "event_id": turn.event_id,
                "target_agent_id": turn.target_agent_id,
                "expected_state": expected_state,
                "next_state": next_state,
                "active_token": turn.active_delivery_token,
            },
        )
        if not isinstance(result, Mapping) or result.get("updated") is not True:
            raise RuntimeError("conversation delivery transition was not acknowledged")
        return result

    async def _dispatch_conversation_turn(self, turn: _ConversationTurn) -> None:
        """canonical event를 internal/no-reply MessageEvent로 변환한다.

        v2 handoff는 one-way다. 일반 모델 응답은 외부 Slack/outbox로 보내지 않고,
        결과 회신이 필요하면 모델이 ``queue_handoff``를 역방향으로 명시 호출한다.
        """
        event_payload = turn.event
        sender = str(event_payload["sender_agent_id"])
        source = self.build_source(
            chat_id=_NO_REPLY_CHANNEL,
            chat_name=f"queue:{turn.conversation_id}",
            chat_type="channel",
            user_id=sender,
            user_name=sender,
            thread_id=turn.conversation_id,
        )
        event = MessageEvent(
            text=str(event_payload["body"]),
            source=source,
            raw_message=dict(event_payload),
            message_id=turn.event_id,
            internal=True,
            metadata={
                "protocol_version": _CONVERSATION_PROTOCOL,
                "conversation_id": turn.conversation_id,
                "correlation_id": event_payload.get("correlation_id"),
                "causation_id": event_payload.get("causation_id"),
                "event_type": event_payload.get("event_type"),
                "capability": event_payload.get("capability"),
                "trust_tier": event_payload.get("trust_tier"),
                "active_delivery_token": turn.active_delivery_token,
            },
        )
        logger.info(
            "[Queue] New conversation turn from %s (event=%s, conversation=%s)",
            sender,
            turn.event_id,
            turn.conversation_id,
        )
        self._inflight[turn.event_id] = turn
        # F4: 활성 턴 registry에 생존 선언 — queue_handoff 도구의 terminal
        # defer 등록은 여기 마킹된 턴에만 허용된다(턴 종료 후 상속 ContextVar
        # 스냅샷의 terminal 호출이 고아 등록이 되는 것을 구조적으로 차단).
        # requester는 인바운드 sender — 활성 턴 안의 종결 대상 대조용.
        mark_turn_active(
            parent_event_id=turn.event_id,
            conversation_id=turn.conversation_id,
            requester=sender,
            delivery_token=turn.active_delivery_token,
        )
        event_token = _ACTIVE_QUEUE_EVENT_ID.set(turn.event_id)
        delivery_token = _ACTIVE_QUEUE_DELIVERY_TOKEN.set(
            turn.active_delivery_token
        )
        # canonical conversation_id/event_id를 task-local로 실어 handle_message가
        # 스폰하는 백그라운드 태스크가 상속하게 한다. 답신 handoff 도구가
        # get_session_env(HERMES_SESSION_CONVERSATION_ID/_EVENT_ID)로 읽어
        # 부모 conversation을 승계한다(계약 배경: docs 큐 conversation.v1 C4).
        conversation_tokens = set_conversation_context(
            turn.conversation_id, turn.event_id
        )
        try:
            await self.handle_message(event)
        except BaseException:
            # 동기 실패(스폰 전) — 완료훅이 오지 않으므로 활성 마킹·등록도
            # 여기서 걷는다(reclaim 재처리 시 재활성화된다).
            self._inflight.pop(turn.event_id, None)
            mark_turn_closed(turn.event_id, turn.active_delivery_token)
            raise
        finally:
            reset_conversation_context(conversation_tokens)
            _ACTIVE_QUEUE_DELIVERY_TOKEN.reset(delivery_token)
            _ACTIVE_QUEUE_EVENT_ID.reset(event_token)

    async def _process_turn(self, turn) -> bool:
        """claim된 턴 하나를 코어로 인계한다. 인계 성공 시 True.

        예외는 여기서 삼키고 row를 error로 마킹한다 — 폴링 루프는 절대 죽지 않는다.
        """
        try:
            return await self._dispatch_turn(turn)
        except Exception as e:
            logger.error(
                "[Queue] Turn processing failed (inbox_id=%s): %s",
                turn.inbox_id, e, exc_info=True,
            )
            try:
                await asyncio.to_thread(
                    self._repo.mark_inbox_error, turn.inbox_id, str(e)[:500]
                )
            except Exception:
                logger.exception(
                    "[Queue] Failed to mark inbox error (inbox_id=%s)", turn.inbox_id
                )
            return False

    @staticmethod
    def _sender_allowed(slack_user_id: str) -> bool:
        """QUEUE_ALLOWED_SENDERS 조기 차단 가드(email 어댑터 관례).

        미설정이면 어댑터 레벨에서는 전부 통과 — 진짜 인가는 코어 authz가
        QUEUE_ALLOWED_SENDERS / QUEUE_ALLOW_ALL_USERS / 전역 GATEWAY_*로
        default-deny 판정한다(이중 게이트).
        """
        allowed_raw = os.getenv("QUEUE_ALLOWED_SENDERS", "").strip()
        if not allowed_raw:
            return True
        allowed = {uid.strip() for uid in allowed_raw.split(",") if uid.strip()}
        return slack_user_id in allowed

    async def _dispatch_turn(self, turn) -> bool:
        """턴을 MessageEvent로 변환해 코어로 넘긴다. 인계했으면 True.

        핵심: 코어 handle_message는 백그라운드 태스크를 스폰하고 즉시 리턴하는
        fire-and-forget이다. 따라서 여기서 done을 찍지 않고, in-flight 테이블에
        등록한 뒤 처리완료 훅(on_processing_complete)에서 마킹한다. 훅이 끝내
        오지 않는 비정상 경로는 CLAIM_TTL 경과 후 reclaim이 pending으로 복구.
        """
        if not self._sender_allowed(turn.slack_user_id):
            logger.warning(
                "[Queue] Dropping non-allowlisted sender: %s (inbox_id=%s)",
                turn.slack_user_id, turn.inbox_id,
            )
            await asyncio.to_thread(
                self._repo.mark_inbox_error, turn.inbox_id, "sender not allowed"
            )
            return False
        handoff = _parse_handoff_channel(turn.channel_id)
        handoff_reply = _parse_handoff_channel(turn.channel_id, reply=True)
        if handoff_reply:
            # handoff 응답을 받은 쪽이 또 답하면 무한 왕복이 된다. 메시지는
            # 정상 처리하되, 모델 응답은 no-op sink로 라우팅해 원 row를 done으로
            # 마감하고 추가 큐 row/outbox를 만들지 않는다.
            reply_channel = _NO_REPLY_CHANNEL
        elif handoff:
            # 큐 handoff로 받은 턴의 답은 슬랙 outbox가 아니라 발신자 큐로 보낸다.
            # send()는 이 특수 target을 handoff-reply row로 삽입하고, 수신측은
            # 위 no-reply 분기로 "답의 답"을 끊는다.
            reply_channel = f"{_HANDOFF_REPLY_TARGET_PREFIX}{turn.slack_user_id}"
        else:
            # 접두 규약 방어: 자동 응답이 handoff로 오분류되지 않게 채널을 'queue:'로
            # 정규화한다(멱등). send()가 이 접두를 보고 outbox로 라우팅하며, 삽입
            # 직전 접두를 벗겨 raw 슬랙 채널로 발송한다.
            reply_channel = _normalize_reply_channel(turn.channel_id)
        source = self.build_source(
            chat_id=reply_channel,
            chat_name=reply_channel,
            chat_type="channel",
            user_id=turn.slack_user_id,
            user_name=turn.slack_user_id,
            thread_id=turn.thread_ts,
        )
        event = MessageEvent(
            text=turn.text,
            source=source,
            message_id=turn.slack_event_ts,
        )
        logger.info(
            "[Queue] New turn from %s in %s (inbox_id=%s)",
            turn.slack_user_id, turn.channel_id, turn.inbox_id,
        )
        self._inflight[event.message_id] = turn
        context_token = _ACTIVE_QUEUE_EVENT_ID.set(event.message_id)
        try:
            await self.handle_message(event)
        except BaseException:
            # 동기 실패(스폰 전) — in-flight 등록을 되돌리고 상위에서 error 마킹.
            self._inflight.pop(event.message_id, None)
            raise
        finally:
            # handle_message가 만든 백그라운드 태스크는 위 ContextVar 값을 이미
            # 상속했다. 폴링 태스크 자체에는 다음 턴으로 새지 않게 즉시 복원한다.
            _ACTIVE_QUEUE_EVENT_ID.reset(context_token)
        return True

    async def _is_claim_active(self, turn: Any, message_id: str) -> bool:
        """주어진 claim이 DB에서도 여전히 유효한지 확인한다.

        조회 실패·repo 미연결은 fail-closed다. 원격 HTTP 큐가 잠깐
        불확실할 때 중복 게시하는 것보다 row를 reclaim 경로에 남기는 편이
        안전하다.
        """
        if turn is None or self._repo is None:
            logger.warning(
                "[Queue] Delivery fence rejected missing claim context (event=%s)",
                message_id or "<missing>",
            )
            return False
        try:
            active = await asyncio.to_thread(
                self._repo.is_active_turn,
                session_id=turn.session_id,
                active_turn_id=turn.active_turn_id,
            )
        except Exception as exc:
            logger.error(
                "[Queue] Delivery fence check failed (event=%s, session=%s): %s",
                message_id,
                turn.session_id,
                exc,
            )
            return False
        if not active:
            logger.warning(
                "[Queue] Delivery fence rejected stale claim (event=%s, session=%s)",
                message_id,
                turn.session_id,
            )
        return bool(active)

    def _is_conversation_claim_active(
        self,
        turn: Any,
        message_id: str,
        delivery_token: Optional[str],
    ) -> bool:
        """v2 no-reply send 직전의 local token/lease fence.

        실제 상태 전이는 server의 active token 비교가 최종 권위다. send는
        외부 I/O 없는 no-reply sink지만, stale task가 정상 응답처럼 통과하지
        않도록 task context token과 claim token, lease 시각을 모두 확인한다.
        """
        if not isinstance(turn, _ConversationTurn):
            return False
        if self._inflight.get(message_id) is not turn:
            return False
        if not delivery_token or delivery_token != turn.active_delivery_token:
            logger.warning(
                "[Queue] Conversation delivery token mismatch (event=%s)",
                message_id or "<missing>",
            )
            return False
        if datetime.now(timezone.utc) >= turn.lease_expires_at:
            logger.warning(
                "[Queue] Conversation delivery lease expired (event=%s)",
                message_id,
            )
            return False
        return True

    async def _is_active_message_id(self, message_id: Optional[str]) -> bool:
        """in-flight message ID를 DB active_turn_id 소유권으로 검증한다."""
        message_id = str(message_id or "").strip()
        turn = self._inflight.get(message_id) if message_id else None
        if isinstance(turn, _ConversationTurn):
            return self._is_conversation_claim_active(
                turn,
                message_id,
                _ACTIVE_QUEUE_DELIVERY_TOKEN.get(),
            )
        return await self._is_claim_active(turn, message_id)

    async def is_active_turn(self, event: MessageEvent) -> bool:
        """GatewayRunner가 최종 응답 반환 직전에 호출하는 claim fence hook."""
        return await self._is_active_message_id(getattr(event, "message_id", None))

    async def on_processing_start(self, event: MessageEvent) -> None:
        """턴 태스크 초입에서 태스크 컨텍스트를 이 턴의 claim 식별자로
        재바인딩한다(F2 근본수리 — 턴별 컨텍스트 재바인딩).

        코어의 pending drain 태스크는 직전 턴 태스크 안에서 create_task로
        스폰돼 직전 턴의 ContextVar 스냅샷을 그대로 상속한다. 재바인딩이
        없으면 후속 턴의 send()가 직전 턴의 stale 식별자로 fence에 걸려
        (ownership lost) 최종 답이 유실되고 delivery가 error(비재claim)로
        끝난다. 이 훅은 코어 _process_message_background의 첫 동작으로 —
        첫 턴/drain 턴 구분 없이 — **해당 턴 태스크의 컨텍스트 안에서**
        실행되므로, 여기서 set한 값이 턴 전체(에이전트 런 → send → 완료훅)에
        적용된다. 첫 턴에서는 폴링 태스크에서 상속된 값과 같은 값을 다시
        set하는 멱등 동작이다.

        _inflight에 없는 event_id는 건드리지 않는다(no-touch) — 소유권을
        잃은 턴은 상속된 stale 컨텍스트가 fence에 걸려 가시적 실패로 남는
        기존 fail-visible 경로를 보존한다. 여기서 clear(set None)하면 v2
        send가 '부모 턴 없음' no-op success로 강등돼 침묵 소실이 재발한다.

        set_conversation_context의 반환 토큰은 버린다 — 이 바인딩은 중첩
        스코프가 아니라 턴 태스크 수명이며, 태스크 종료와 함께 컨텍스트가
        통째로 소멸하므로 reset이 필요 없다.
        """
        event_id = str(getattr(event, "message_id", "") or "")
        if not event_id:
            return
        turn = self._inflight.get(event_id)
        if turn is None:
            return
        _ACTIVE_QUEUE_EVENT_ID.set(event_id)
        if isinstance(turn, _ConversationTurn):
            _ACTIVE_QUEUE_DELIVERY_TOKEN.set(turn.active_delivery_token)
            set_conversation_context(turn.conversation_id, turn.event_id)

    async def on_processing_complete(
        self, event: MessageEvent, outcome: ProcessingOutcome
    ) -> None:
        """코어 백그라운드 처리 완료 훅 — 여기서 비로소 done/error + 락 해제.

        SUCCESS = 에이전트 런과 응답 발신까지 끝남 -> done.
        FAILURE/CANCELLED -> error(유실 아님 — 상태로 가시화).
        """
        event_id = str(event.message_id or "")
        turn = self._inflight.get(event_id) if event_id else None
        if turn is None:
            return
        if isinstance(turn, _ConversationTurn):
            # advance_delivery가 active token/expected_state/lease를 한
            # transaction에서 검증한다. 실패한 stale worker는 상태를 쓰지 못하고,
            # accepted row는 만료 뒤 새 worker가 재claim한다.
            self._inflight.pop(event_id, None)
            # F4: 활성 해제 + 등록 소비를 한 락 안에서 원자화 — 처리 결과와
            # 무관하게 여기서 턴을 닫는다. 이 시점 이후의 terminal 호출(상속
            # 스냅샷)은 register가 거부해 즉시-append로 폴스루하므로 고아
            # 등록이 없고, stale 워커(재claim 후 옛 token)의 늦은 close는
            # token 대조로 새 턴의 상태를 건드리지 못한다.
            deferred = mark_turn_closed(turn.event_id, turn.active_delivery_token)
            next_state = (
                "completed"
                if outcome is ProcessingOutcome.SUCCESS
                else "error"
            )
            try:
                await self._advance_conversation_delivery(
                    turn,
                    expected_state="accepted",
                    next_state=next_state,
                )
                # F2(여전히 ContextVar를 여기서 clear하지 않는다): 근본 수리는
                # on_processing_start의 턴별 재바인딩이다 — 후속 drain 턴은
                # 자기 태스크 초입에서 자기 claim 식별자로 재바인딩되므로 이
                # 훅이 남긴 값에 의존하지 않는다. 그래도 clear(set None)는
                # 금지다: 재바인딩이 no-touch로 빠지는 소유권 상실 턴에서
                # None이 상속되면 v2 send가 "부모 턴 없음" no-op success로
                # 강등돼 침묵 소실이 재발한다. stale 값이 남아야
                # fence(ownership lost)로 가시화된다 — fail-visible 백스톱.
            except Exception as exc:
                logger.warning(
                    "[Queue] Conversation completion fenced "
                    "(event=%s, state=%s): %s",
                    event_id,
                    next_state,
                    exc,
                )
                # 소유권/lease fence에 걸린 stale worker는 종결(terminal)도
                # 쓰지 않는다 — 새 owner의 재처리 턴이 다시 판단한다.
                return
            if deferred is None:
                return
            if outcome is not ProcessingOutcome.SUCCESS:
                # 답이 나가지 못한 턴의 종결 의도는 폐기한다 — 등록만으로
                # 대화를 닫으면 "실제로 안 끝났는데 ✅"(거짓 종결)가 된다.
                # delivery가 error로 가시화됐으므로 재처리 턴이 다시 낸다.
                logger.warning(
                    "[Queue] Discarding deferred terminal for %s turn "
                    "(event=%s, type=%s)",
                    outcome.value,
                    event_id,
                    deferred.event_type,
                )
                return
            if deferred.conversation_id != turn.conversation_id:
                # 다른 대화의 종결 의도가 이 턴으로 새는 오염 방어.
                logger.warning(
                    "[Queue] Discarding deferred terminal with mismatched "
                    "conversation (event=%s, registered=%s, turn=%s)",
                    event_id,
                    deferred.conversation_id,
                    turn.conversation_id,
                )
                return
            # §5 순서의 기계적 보장: 코어는 최종 답(reply) 발신을 마친 뒤에야
            # 이 훅을 부르므로, 여기서의 terminal append는 구조적으로 항상
            # reply 뒤다. idempotency_key는 인바운드 event_id에서 파생돼
            # 재시도에도 안정적이고, _build_producer_event가 created_at을
            # 인바운드에서 승계해 byte-stable(서버 dedupe 전제)이다.
            terminal_event = self._build_producer_event(
                turn,
                event_type=deferred.event_type,
                body=deferred.body or deferred.event_type,
                idempotency_key=f"queue_close:{self._agent}:{turn.event_id}",
            )
            try:
                receipt = await self._conversation_call(
                    "append_event", {"event": terminal_event}
                )
                if (
                    not isinstance(receipt, Mapping)
                    or receipt.get("event_id") != terminal_event["event_id"]
                ):
                    raise RuntimeError(
                        "deferred terminal receipt event identity mismatch"
                    )
            except Exception as exc:  # noqa: BLE001 — 훅 밖으로 예외 전파 금지
                # reply까지는 이미 진실이므로 delivery(completed)는 되돌리지
                # 않는다. terminal만 유실된 대화는 닫히지 않은 채 남는데,
                # 이는 계약 철학("성급히 종결하지 마라")상 가시적 안전측이다
                # — 요청자 종결/다음 턴 종결이 폴백으로 남는다.
                logger.warning(
                    "[Queue] Deferred terminal append failed "
                    "(event=%s, type=%s): %s",
                    event_id,
                    deferred.event_type,
                    exc,
                )
            return
        # run.py가 stale 결과를 None으로 억제하면 BasePlatformAdapter는 이를
        # 정상 무응답으로 분류할 수 있다. DB 소유권을 여기서도 확인하지 않으면
        # old worker가 새 owner의 row를 done으로 덮어쓴다. 소유권 상실 시에는
        # 어떤 status도 쓰지 않고 새 owner/reclaim 경로에 그대로 맡긴다.
        if not await self._is_claim_active(turn, event_id):
            self._inflight.pop(event_id, None)
            logger.warning(
                "[Queue] Skipping completion write for stale claim (event=%s)",
                event_id,
            )
            return
        self._inflight.pop(event_id, None)
        try:
            if outcome is ProcessingOutcome.SUCCESS:
                await asyncio.to_thread(self._repo.mark_inbox_done, turn.inbox_id)
                # Main response is durably queued and the inbox row is done.
                # BasePlatformAdapter's post-delivery callback runs after this
                # hook; clear only this task's fence so that ordered goal/status
                # notices may use their own outbound delivery path.
                if _ACTIVE_QUEUE_EVENT_ID.get() == event_id:
                    _ACTIVE_QUEUE_EVENT_ID.set(None)
            else:
                await asyncio.to_thread(
                    self._repo.mark_inbox_error,
                    turn.inbox_id,
                    f"processing {outcome.value}",
                )
        except Exception:
            logger.exception(
                "[Queue] Failed to mark inbox %s (inbox_id=%s)",
                outcome.value, turn.inbox_id,
            )
        finally:
            try:
                await asyncio.to_thread(
                    self._repo.release_session_lock,
                    session_id=turn.session_id,
                    worker=self._worker,
                )
            except Exception:
                logger.exception(
                    "[Queue] Failed to release session lock (session_id=%s)",
                    turn.session_id,
                )

    async def disconnect(self) -> None:
        """폴링을 멈추고 태스크를 정리한다."""
        self._mark_disconnected()
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        logger.info("[Queue] Disconnected.")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """큐로 메시지를 보낸다.

        queue.v1: chat_id 접두로 두 경로 분기(_route_and_insert).
        - 'queue:<채널>'  → 자동 응답: slack_outbox INSERT(발신은 브리지 센더).
        - 그 외(에이전트) → 아웃바운드 handoff: 상대 target의 slack_inbox INSERT.
        thread_ts는 코어가 넣어주는 metadata["thread_id"](= source.thread_id)
        우선, 없으면 reply_to(트리거 메시지 ts)로 폴백한다.

        conversation.v1(C3): 활성 인바운드 턴 중의 코어 최종 응답을 자동 `reply`
        canonical event로 append한다(§4-1) — 노이즈(status ping 등)는 게이팅해
        event를 만들지 않는다. one-way라 chat_id는 항상 _NO_REPLY_CHANNEL이어야
        하고, 그 외 target은 여전히 거부(명시적 역방향은 queue_handoff 도구).
        """
        active_event_id = _ACTIVE_QUEUE_EVENT_ID.get()
        if self._protocol_version == _CONVERSATION_PROTOCOL:
            if chat_id != _NO_REPLY_CHANNEL:
                return SendResult(
                    success=False,
                    error=(
                        "conversation.v1 adapter is one-way; use queue_handoff "
                        "for an explicit reverse handoff"
                    ),
                )
            if not active_event_id:
                # 처리 완료 후 in-flight 정리가 끝난 뒤의 stray send 등 — reply를
                # 걸어줄 부모 턴이 없으므로 기존대로 no-op 성공.
                return SendResult(
                    success=True,
                    message_id="conversation:no-reply",
                )
            if not await self._is_active_message_id(active_event_id):
                return SendResult(
                    success=False,
                    error="queue turn ownership lost before send",
                )
            # fence(_is_active_message_id)가 이미 _inflight[active_event_id]가 이
            # ContextVar 턴과 동일 객체임을 검증했다 — 코어의 최종 응답을 부모
            # 인바운드 event에 causation으로 잇는 자동 reply event로 투영한다
            # (§4-1). 노이즈(status ping 등)는 event로 만들지 않고 조용히
            # no-op 처리한다.
            turn = self._inflight.get(active_event_id)
            if not isinstance(turn, _ConversationTurn):
                # 정상 경로에서는 도달하지 않는다(위 fence가 이미 보장) — 방어적
                # no-op.
                return SendResult(
                    success=True,
                    message_id="conversation:no-reply",
                )
            if _is_producer_noise(content):
                return SendResult(
                    success=True,
                    message_id="conversation:noise-gated",
                )
            content_digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
            idempotency_key = (
                f"queue_reply:{self._agent}:{turn.event_id}:{content_digest[:16]}"
            )
            reply_event = self._build_producer_event(
                turn,
                event_type="reply",
                body=content,
                idempotency_key=idempotency_key,
            )
            try:
                receipt = await self._conversation_call(
                    "append_event", {"event": reply_event}
                )
            except Exception as exc:
                # 침묵 실패 금지 — 실패를 가시화해 코어 FAILURE로 넘기고
                # reclaim/재시도 경로가 이어받게 한다.
                return SendResult(
                    success=False,
                    error=f"conversation reply append failed: {str(exc)[:500]}",
                )
            if (
                not isinstance(receipt, Mapping)
                or receipt.get("event_id") != reply_event["event_id"]
            ):
                return SendResult(
                    success=False,
                    error="conversation reply receipt event identity mismatch",
                )
            status = str(receipt.get("delivery_status") or "")
            if status not in _DELIVERY_STATES:
                return SendResult(
                    success=False,
                    error=(
                        "conversation reply receipt has an invalid "
                        f"delivery_status: {status!r}"
                    ),
                )
            return SendResult(success=True, message_id=reply_event["event_id"])

        if self._repo is None:
            return SendResult(success=False, error="queue repo not connected")
        if active_event_id and not await self._is_active_message_id(active_event_id):
            # 실제 INSERT 바로 앞의 최종 fence. run.py의 반환 경계 검사와 함께
            # 검증~BasePlatformAdapter.send 사이의 TOCTOU 창까지 닫는다.
            return SendResult(
                success=False,
                error="queue turn ownership lost before send",
            )
        thread_hint = (metadata.get("thread_id") if metadata else None) or reply_to
        try:
            result = await asyncio.to_thread(
                _route_and_insert, self._repo, self._agent, chat_id, content, thread_hint
            )
        except Exception as e:
            logger.error("[Queue] Send failed to %s: %s", chat_id, e)
            return SendResult(success=False, error=str(e))
        if result.get("success"):
            return SendResult(success=True, message_id=result.get("message_id"))
        return SendResult(success=False, error=result.get("error", "send failed"))

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """슬랙 채널 큐 대화의 기본 정보."""
        return {"name": chat_id, "type": "channel", "chat_id": chat_id}


def _is_connected(config) -> bool:
    """선택한 protocol의 필수 설정이 모두 있을 때만 활성으로 판정."""
    extra = getattr(config, "extra", {}) or {}

    def _value(extra_key: str, env_name: str) -> str:
        raw = extra.get(extra_key)
        if raw:
            return str(raw).strip()
        import hermes_cli.gateway as gateway_mod

        return (gateway_mod.get_env_value(env_name) or "").strip()

    protocol = _value("protocol_version", "QUEUE_PROTOCOL_VERSION") or _LEGACY_PROTOCOL
    if protocol not in _SUPPORTED_PROTOCOLS:
        return False
    if not all(
        _value(key, env)
        for key, env in (
            ("agent", "QUEUE_AGENT"),
            ("repo_root", "QUEUE_REPO_ROOT"),
        )
    ):
        return False
    if protocol == _CONVERSATION_PROTOCOL:
        # credential은 config.extra가 아니라 profile private env에서만 읽는다.
        import hermes_cli.gateway as gateway_mod

        credential = (
            gateway_mod.get_env_value("QUEUE_CONVERSATION_CREDENTIAL") or ""
        ).strip()
        return bool(_value("endpoint", "QUEUE_ENDPOINT") and credential)
    return bool(
        _value("endpoint", "QUEUE_ENDPOINT")
        or _value("db_path", "QUEUE_DB_PATH")
    )


def _build_adapter(config):
    """PlatformConfig로부터 QueueAdapter를 만드는 팩토리."""
    return QueueAdapter(config)


async def _standalone_send(
    pconfig,
    chat_id,
    message,
    *,
    thread_id=None,
    media_files=None,
    force_document=False,
):
    """게이트웨이 없이(out-of-process) 큐로 보내는 one-shot 전송.

    standalone_sender_fn 계약(email 어댑터와 동일 시그니처·반환)을 구현한다.
    cron/러너가 게이트웨이와 다른 프로세스로 돌 때 send_message가 이 경로로
    떨어진다. QUEUE_AGENT/QUEUE_REPO_ROOT와 QUEUE_DB_PATH 또는 QUEUE_ENDPOINT는
    pconfig.extra 우선, 없으면 os.getenv 폴백. send()와 동일한 라우팅(_route_and_insert)을 쓴다.

    ⚠️ 아웃바운드 handoff의 발신자 인가 결합: 여기서 만드는 handoff row는
    slack_user_id=<발신 에이전트 키>로 박히므로, 수신 에이전트가 자기
    QUEUE_ALLOWED_SENDERS에 이 발신 에이전트 키를 넣거나 QUEUE_ALLOW_ALL_USERS를
    켜야 handoff가 처리된다. 아니면 수신측 코어 authz(default-deny)가
    "sender not allowed"로 error 마킹해 소실시킨다(상세: _route_and_insert).
    """
    extra = getattr(pconfig, "extra", {}) or {}

    def _cfg(extra_key: str, env_name: str) -> str:
        raw = extra.get(extra_key)
        if raw:
            return str(raw).strip()
        return os.getenv(env_name, "").strip()

    db_path = _cfg("db_path", "QUEUE_DB_PATH")
    endpoint = _cfg("endpoint", "QUEUE_ENDPOINT")
    token = _cfg("token", "QUEUE_TOKEN")
    agent = _cfg("agent", "QUEUE_AGENT")
    repo_root = _cfg("repo_root", "QUEUE_REPO_ROOT")
    protocol = _cfg("protocol_version", "QUEUE_PROTOCOL_VERSION") or _LEGACY_PROTOCOL
    if protocol == _CONVERSATION_PROTOCOL:
        return {
            "error": (
                "conversation.v1 platform send is one-way; use queue_handoff "
                "for a canonical outbound handoff"
            )
        }
    if protocol != _LEGACY_PROTOCOL:
        return {"error": f"Unsupported QUEUE_PROTOCOL_VERSION: {protocol}"}
    if not all([agent, repo_root]) or (not db_path and not endpoint):
        return {
            "error": "Queue not configured "
            "(QUEUE_AGENT, QUEUE_REPO_ROOT, and QUEUE_DB_PATH or QUEUE_ENDPOINT required)"
        }

    # append(insert(0) 금지): slack_agent 루트의 최상위 config.py(시크릿) 등이
    # 전역 최우선 import가 되지 않게 — 어댑터 본체와 동일 규칙.
    if repo_root not in sys.path:
        sys.path.append(repo_root)

    def _do():
        from bridge.agent_repo import make_agent_queue_repo

        repo = make_agent_queue_repo(db_path=db_path, endpoint=endpoint, token=token)
        return _route_and_insert(repo, agent, chat_id, message, thread_id)

    try:
        return await asyncio.to_thread(_do)
    except Exception as e:
        logger.error("[Queue] Standalone send failed to %s: %s", chat_id, e)
        return {"error": f"Queue send failed: {e}"}


def register(ctx) -> None:
    """플러그인 진입점 — Hermes 플러그인 시스템이 호출한다."""
    ctx.register_platform(
        name="queue",
        label="Queue",
        adapter_factory=_build_adapter,
        check_fn=check_queue_requirements,
        is_connected=_is_connected,
        required_env=["QUEUE_AGENT", "QUEUE_REPO_ROOT"],
        install_hint="Queue uses slack_agent bridge; set QUEUE_DB_PATH for local SQLite or QUEUE_ENDPOINT/QUEUE_TOKEN for HTTP",
        allowed_users_env="QUEUE_ALLOWED_SENDERS",
        allow_all_env="QUEUE_ALLOW_ALL_USERS",
        standalone_sender_fn=_standalone_send,
        max_message_length=40_000,
        emoji="📬",
    )
