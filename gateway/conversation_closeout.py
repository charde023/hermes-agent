"""conversation.v1 deferred terminal 등록소 (F4).

계약(§5, canonical-event 계약확정서)은 terminal(DONE/FAILED/NO_ACTION) 뒤
append를 전면 거부하고, 지원 순서는 ``reply → completed``다. 그런데 게이트웨이는
인바운드 턴의 최종 답을 **턴이 끝날 때** 자동 ``reply``로 append하므로, 에이전트가
같은 턴 도중 도구로 terminal을 즉시 append하면 자기 최종 답이 terminal 뒤
append로 거부돼 delivery error가 된다.

그래서 종결은 두 단계로 나눈다:
- **등록**: ``queue_handoff`` 도구가 활성 v2 인바운드 턴 안에서 terminal
  event_type을 받으면 즉시 append하지 않고 여기 등록만 한다.
- **소비**: 큐 어댑터의 ``on_processing_complete``가 최종 답(reply) 발신 뒤
  ``mark_turn_closed``로 pop해 terminal event를 append한다 — reply → terminal
  순서의 기계적 보장.

★활성 턴 registry가 등록의 유일한 관문이다(SPEC-1). "활성 인바운드 턴"을
ContextVar 존재로 판정하면 안 된다 — 스냅샷은 턴이 끝난 뒤에도 상속처
(``delegate_task`` background 워커의 ``copy_context()`` 등)에 그대로 남아,
턴 종료 후의 terminal 호출이 아무 완료훅도 pop하지 않을 등록(고아)을 만들고
도구는 success를 반환하는 침묵 소실이 된다. 그래서 어댑터가 dispatch 시점에
``mark_turn_active``로 턴의 생존을 선언하고, ``register_deferred_terminal``은
같은 락 안에서 활성 여부를 확인해 비활성이면 ``None``을 반환한다(호출자는
기존 즉시-append 경로로 폴스루). ``mark_turn_closed``는 활성 해제와 등록
소비를 한 락 안에서 원자화하므로 "등록됐는데 아무도 pop하지 않는" 창이
구조적으로 존재하지 않는다.

도구는 어댑터 인스턴스에 접근할 수 없고, ContextVar는 자식 태스크에서 set한
값이 부모(어댑터 훅)로 역전파되지 않으므로, 프로세스-로컬 모듈 전역이 유일한
공유 통로다.

크래시 내구성의 정확한 범위: 등록·활성 마킹은 인메모리라 프로세스가 죽으면
같이 사라진다. 완료훅(advance) **전에** 죽으면 delivery가 reclaim돼 턴이
재처리되고 재활성화가 이전 등록을 클리어하므로 유실이 아니다. 단
advance(completed) **성공 후** terminal append 전에 죽는 좁은 창에서는
delivery가 completed라 reclaim이 없어 이 턴의 종결 의도만 소실된다 — 대화는
열린 채 남고(가시적), 종결은 요청자측/후속 턴 폴백으로 넘어간다. 이는 §5
철학("성급히 종결하지 마라")상 안전측 트레이드오프다.
"""

import threading
from dataclasses import dataclass

# 계약 §3 전이표의 terminal 3종. 이 밖의 event_type은 등록을 거부한다 —
# 비-terminal이 등록되면 어댑터가 훅에서 그것을 종결처럼 append하게 된다.
TERMINAL_EVENT_TYPES = frozenset({"completed", "failed", "no_action"})


@dataclass(frozen=True)
class ActiveTurn:
    """어댑터가 dispatch 중이라고 선언한 인바운드 v2 턴."""

    parent_event_id: str
    conversation_id: str
    # 인바운드 event의 sender — 활성 턴 안의 terminal은 이 요청자에게만
    # 허용된다(어댑터가 append하는 terminal의 대상이 sender로 고정되므로,
    # 다른 to의 종결을 받으면 조용한 오발이 된다 — 도구가 대조해 거부).
    requester: str
    # 재claim(새 token) 뒤 늦게 도착한 옛 워커의 close가 새 턴의 상태를
    # 지우지 못하게 하는 소유 증명.
    delivery_token: str


@dataclass(frozen=True)
class DeferredTerminal:
    """한 인바운드 턴에서 등록된 종결 의도."""

    conversation_id: str
    parent_event_id: str
    event_type: str
    body: str


_LOCK = threading.Lock()
# key = 인바운드 턴의 event_id(parent_event_id). 도구와 어댑터가 같은 값을
# 각각 HERMES_SESSION_CONVERSATION_EVENT_ID / turn.event_id에서 얻는다.
_ACTIVE_TURNS: dict[str, ActiveTurn] = {}
_PENDING: dict[str, DeferredTerminal] = {}


def mark_turn_active(
    *,
    parent_event_id: str,
    conversation_id: str,
    requester: str,
    delivery_token: str,
) -> None:
    """어댑터가 dispatch 직전에 이 턴의 생존을 선언한다.

    재claim(같은 event_id, 새 token)이면 항목을 덮어쓰고, 이전 런이 소비하지
    못하고 남긴 등록을 클리어한다 — 새 턴은 깨끗하게 시작하고 종결 판단은
    새 런이 다시 낸다.
    """
    parent_event_id = str(parent_event_id or "").strip()
    if not parent_event_id:
        return
    record = ActiveTurn(
        parent_event_id=parent_event_id,
        conversation_id=str(conversation_id or "").strip(),
        requester=str(requester or "").strip(),
        delivery_token=str(delivery_token or "").strip(),
    )
    with _LOCK:
        _ACTIVE_TURNS[parent_event_id] = record
        _PENDING.pop(parent_event_id, None)


def mark_turn_closed(
    parent_event_id: str, delivery_token: str
) -> DeferredTerminal | None:
    """턴 종료 — 활성 해제와 등록 소비를 한 락 안에서 원자화한다.

    token이 현재 활성 항목과 다르면(재claim된 새 턴이 소유) 아무것도 건드리지
    않고 None — stale 워커의 늦은 close가 새 턴의 등록을 훔치지 못한다.
    """
    parent_event_id = str(parent_event_id or "").strip()
    if not parent_event_id:
        return None
    with _LOCK:
        active = _ACTIVE_TURNS.get(parent_event_id)
        if active is None or active.delivery_token != str(delivery_token or "").strip():
            return None
        del _ACTIVE_TURNS[parent_event_id]
        return _PENDING.pop(parent_event_id, None)


def active_turn(parent_event_id: str) -> ActiveTurn | None:
    """활성 턴 조회(도구의 defer 프리체크·requester 대조용)."""
    parent_event_id = str(parent_event_id or "").strip()
    if not parent_event_id:
        return None
    with _LOCK:
        return _ACTIVE_TURNS.get(parent_event_id)


def register_deferred_terminal(
    *,
    conversation_id: str,
    parent_event_id: str,
    event_type: str,
    body: str,
) -> DeferredTerminal | None:
    """이 턴의 종결 의도를 등록한다.

    활성 턴(같은 대화)이 아니면 ``None`` — 호출자는 즉시-append 경로로
    폴스루해야 한다. 같은 턴 재등록은 최신이 이긴다(에이전트가 completed →
    failed로 판단을 정정하는 경우 허용 — 실제 terminal append는 어댑터
    훅에서 어차피 1회다). 잘못된 event_type/식별자는 ValueError.
    """
    conversation_id = str(conversation_id or "").strip()
    parent_event_id = str(parent_event_id or "").strip()
    event_type = str(event_type or "").strip()
    if not conversation_id or not parent_event_id:
        raise ValueError(
            "deferred terminal requires conversation_id and parent_event_id"
        )
    if event_type not in TERMINAL_EVENT_TYPES:
        raise ValueError(
            f"deferred terminal event_type must be one of "
            f"{sorted(TERMINAL_EVENT_TYPES)} — got '{event_type}'"
        )
    record = DeferredTerminal(
        conversation_id=conversation_id,
        parent_event_id=parent_event_id,
        event_type=event_type,
        body=str(body or ""),
    )
    with _LOCK:
        active = _ACTIVE_TURNS.get(parent_event_id)
        if active is None or active.conversation_id != conversation_id:
            return None
        _PENDING[parent_event_id] = record
    return record


def pop_deferred_terminal(parent_event_id: str) -> DeferredTerminal | None:
    """등록을 1회성으로 소비한다. 없으면 None.

    프로덕션 소비 경로는 ``mark_turn_closed``다(활성 해제와 원자화) — 이
    함수는 테스트 검증과 활성 해제 없이 등록만 걷어내야 하는 방어 경로용.
    """
    parent_event_id = str(parent_event_id or "").strip()
    if not parent_event_id:
        return None
    with _LOCK:
        return _PENDING.pop(parent_event_id, None)
