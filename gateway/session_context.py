"""
Session-scoped context variables for the Hermes gateway.

Replaces the previous ``os.environ``-based session state
(``HERMES_SESSION_PLATFORM``, ``HERMES_SESSION_CHAT_ID``, etc.) with
Python's ``contextvars.ContextVar``.

**Why this matters**

The gateway processes messages concurrently via ``asyncio``.  When two
messages arrive at the same time the old code did:

    os.environ["HERMES_SESSION_THREAD_ID"] = str(context.source.thread_id)

Because ``os.environ`` is *process-global*, Message A's value was
silently overwritten by Message B before Message A's agent finished
running.  Background-task notifications and tool calls therefore routed
to the wrong thread.

``contextvars.ContextVar`` values are *task-local*: each ``asyncio``
task (and any ``run_in_executor`` thread it spawns) gets its own copy,
so concurrent messages never interfere.

**Backward compatibility**

The public helper ``get_session_env(name, default="")`` mirrors the old
``os.getenv("HERMES_SESSION_*", ...)`` calls.  Existing tool code only
needs to replace the import + call site:

    # before
    import os
    platform = os.getenv("HERMES_SESSION_PLATFORM", "")

    # after
    from gateway.session_context import get_session_env
    platform = get_session_env("HERMES_SESSION_PLATFORM", "")

**canonical conversation 승계 통로**

``HERMES_SESSION_CONVERSATION_ID``/``HERMES_SESSION_CONVERSATION_EVENT_ID``는
conversation.v1 인바운드 턴의 부모 conversation_id/event_id를 나르는 전용
통로다. 큐 어댑터(``plugins/platforms/queue/adapter.py``)가
``set_conversation_context``로 dispatch 스코프에 싣고, 답신 handoff 도구가
``get_session_env``로 읽어 답신이 같은 conversation을 승계하게 한다.
"""

from contextvars import ContextVar
from typing import Any

# Sentinel to distinguish "never set in this context" from "explicitly set to empty".
# When a contextvar holds _UNSET, we fall back to os.environ (CLI/cron compat).
# When it holds "" (after clear_session_vars resets it), we return "" — no fallback.
_UNSET: Any = object()

# ---------------------------------------------------------------------------
# Per-task session variables
# ---------------------------------------------------------------------------

_SESSION_PLATFORM: ContextVar = ContextVar("HERMES_SESSION_PLATFORM", default=_UNSET)
_SESSION_SOURCE: ContextVar = ContextVar("HERMES_SESSION_SOURCE", default=_UNSET)
_SESSION_CHAT_ID: ContextVar = ContextVar("HERMES_SESSION_CHAT_ID", default=_UNSET)
_SESSION_CHAT_NAME: ContextVar = ContextVar("HERMES_SESSION_CHAT_NAME", default=_UNSET)
_SESSION_THREAD_ID: ContextVar = ContextVar("HERMES_SESSION_THREAD_ID", default=_UNSET)
_SESSION_USER_ID: ContextVar = ContextVar("HERMES_SESSION_USER_ID", default=_UNSET)
_SESSION_USER_NAME: ContextVar = ContextVar("HERMES_SESSION_USER_NAME", default=_UNSET)
_SESSION_KEY: ContextVar = ContextVar("HERMES_SESSION_KEY", default=_UNSET)
_SESSION_ID: ContextVar = ContextVar("HERMES_SESSION_ID", default=_UNSET)
# ID of the message that triggered the current turn. Used as a reply anchor
# so background-process notifications stay inside the originating Telegram
# private-chat topic (those lanes route only with thread id + reply anchor).
_SESSION_MESSAGE_ID: ContextVar = ContextVar("HERMES_SESSION_MESSAGE_ID", default=_UNSET)

# canonical conversation.v1 인바운드 턴의 승계 통로. 큐 어댑터의
# ``_dispatch_conversation_turn``이 dispatch 스코프에서 set하고, 답신
# handoff 도구(``queue_handoff_tool``)가 ``get_session_env``로 읽어 부모
# conversation_id/event_id를 이어받는다. task-local이라 동시에 여러 턴이
# dispatch돼도 서로의 conversation 식별자를 덮어쓰지 않는다.
_SESSION_CONVERSATION_ID: ContextVar = ContextVar(
    "HERMES_SESSION_CONVERSATION_ID", default=_UNSET
)
_SESSION_CONVERSATION_EVENT_ID: ContextVar = ContextVar(
    "HERMES_SESSION_CONVERSATION_EVENT_ID", default=_UNSET
)

# Whether the current session's delivery channel can route an ASYNC completion
# back to the agent AFTER the current turn ends (i.e. wake a fresh turn).
#
# True  — CLI (in-process completion_queue drain) and the real gateway
#         platforms (Telegram/Discord/Slack/...), which hold a persistent
#         outbound channel and run the watcher/drain loops.
# False — stateless request/response adapters (the API server: every route,
#         spec and proprietary, tears down its channel when the turn ends, so
#         a background completion that finishes later has nowhere to go).
#
# Tools that promise async delivery (terminal notify_on_complete /
# watch_patterns, delegate_task background=True) read this via
# ``async_delivery_supported()`` and refuse to hand out a promise the channel
# can't keep — turning a silent no-op into an explicit contract.
#
# Default _UNSET => treated as supported, so CLI (which never sets a platform)
# and any contextvar-unaware path keep working. Stateless adapters opt OUT by
# setting ``supports_async_delivery = False`` on the adapter class; the gateway
# propagates that into this contextvar at session-bind time.
_SESSION_ASYNC_DELIVERY: ContextVar = ContextVar("HERMES_SESSION_ASYNC_DELIVERY", default=_UNSET)

# Cron auto-delivery vars — set per-job in run_job() so concurrent jobs
# don't clobber each other's delivery targets.
_CRON_AUTO_DELIVER_PLATFORM: ContextVar = ContextVar("HERMES_CRON_AUTO_DELIVER_PLATFORM", default=_UNSET)
_CRON_AUTO_DELIVER_CHAT_ID: ContextVar = ContextVar("HERMES_CRON_AUTO_DELIVER_CHAT_ID", default=_UNSET)
_CRON_AUTO_DELIVER_THREAD_ID: ContextVar = ContextVar("HERMES_CRON_AUTO_DELIVER_THREAD_ID", default=_UNSET)

_VAR_MAP = {
    "HERMES_SESSION_PLATFORM": _SESSION_PLATFORM,
    "HERMES_SESSION_SOURCE": _SESSION_SOURCE,
    "HERMES_SESSION_CHAT_ID": _SESSION_CHAT_ID,
    "HERMES_SESSION_CHAT_NAME": _SESSION_CHAT_NAME,
    "HERMES_SESSION_THREAD_ID": _SESSION_THREAD_ID,
    "HERMES_SESSION_USER_ID": _SESSION_USER_ID,
    "HERMES_SESSION_USER_NAME": _SESSION_USER_NAME,
    "HERMES_SESSION_KEY": _SESSION_KEY,
    "HERMES_SESSION_ID": _SESSION_ID,
    "HERMES_SESSION_MESSAGE_ID": _SESSION_MESSAGE_ID,
    "HERMES_SESSION_CONVERSATION_ID": _SESSION_CONVERSATION_ID,
    "HERMES_SESSION_CONVERSATION_EVENT_ID": _SESSION_CONVERSATION_EVENT_ID,
    "HERMES_CRON_AUTO_DELIVER_PLATFORM": _CRON_AUTO_DELIVER_PLATFORM,
    "HERMES_CRON_AUTO_DELIVER_CHAT_ID": _CRON_AUTO_DELIVER_CHAT_ID,
    "HERMES_CRON_AUTO_DELIVER_THREAD_ID": _CRON_AUTO_DELIVER_THREAD_ID,
}


def set_current_session_id(session_id: str) -> None:
    """Synchronize ``HERMES_SESSION_ID`` across ContextVar and ``os.environ``.

    Long-lived single-process entrypoints like the CLI can rotate sessions via
    ``/new``, ``/resume``, ``/branch``, or compression splits without
    reconstructing the entire agent. Tools still consult
    ``get_session_env("HERMES_SESSION_ID")`` with an ``os.environ`` fallback,
    so both storage paths must move together when the active session changes.
    """
    import os

    os.environ["HERMES_SESSION_ID"] = session_id
    _SESSION_ID.set(session_id)


def set_session_vars(
    platform: str = "",
    source: str = "",
    chat_id: str = "",
    chat_name: str = "",
    thread_id: str = "",
    user_id: str = "",
    user_name: str = "",
    session_key: str = "",
    session_id: str = "",
    message_id: str = "",
    cwd: str = "",
    async_delivery: bool = True,
) -> list:
    """Set all session context variables and return reset tokens.

    Call ``clear_session_vars(tokens)`` in a ``finally`` block when the handler
    exits. Note ``clear_session_vars`` resets every var to ``""`` (to suppress
    the ``os.environ`` fallback) rather than restoring prior values — these
    helpers are not nestable/stack-safe, and the returned tokens are accepted
    only for API compatibility.

    ``cwd`` pins the logical working directory for this context.

    ``async_delivery`` declares whether this session's channel can route a
    background completion back to the agent after the turn ends (see
    ``_SESSION_ASYNC_DELIVERY`` / ``async_delivery_supported``). Stateless
    request/response adapters (the API server) pass ``False``.
    """
    tokens = [
        _SESSION_PLATFORM.set(platform),
        _SESSION_SOURCE.set(source),
        _SESSION_CHAT_ID.set(chat_id),
        _SESSION_CHAT_NAME.set(chat_name),
        _SESSION_THREAD_ID.set(thread_id),
        _SESSION_USER_ID.set(user_id),
        _SESSION_USER_NAME.set(user_name),
        _SESSION_KEY.set(session_key),
        _SESSION_ID.set(session_id),
        _SESSION_MESSAGE_ID.set(message_id),
        _SESSION_ASYNC_DELIVERY.set(bool(async_delivery)),
    ]
    try:
        from agent.runtime_cwd import set_session_cwd

        set_session_cwd(cwd)
    except Exception:
        pass
    return tokens


def clear_session_vars(tokens: list) -> None:
    """Mark session context variables as explicitly cleared.

    Sets all variables to ``""`` so that ``get_session_env`` returns an empty
    string instead of falling back to (potentially stale) ``os.environ``
    values.  The *tokens* argument is accepted for API compatibility with
    callers that saved the return value of ``set_session_vars``, but the
    actual clearing uses ``var.set("")`` rather than ``var.reset(token)``
    to ensure the "explicitly cleared" state is distinguishable from
    "never set" (which holds the ``_UNSET`` sentinel).
    """
    for var in (
        _SESSION_PLATFORM,
        _SESSION_SOURCE,
        _SESSION_CHAT_ID,
        _SESSION_CHAT_NAME,
        _SESSION_THREAD_ID,
        _SESSION_USER_ID,
        _SESSION_USER_NAME,
        _SESSION_KEY,
        _SESSION_ID,
        _SESSION_MESSAGE_ID,
        _SESSION_CONVERSATION_ID,
        _SESSION_CONVERSATION_EVENT_ID,
    ):
        var.set("")
    # Reset async-delivery capability to the "never set" sentinel rather than a
    # falsy value: a cleared context should fall back to the default-supported
    # behavior (CLI / unaware paths), not be mistaken for an opted-out
    # stateless adapter.
    _SESSION_ASYNC_DELIVERY.set(_UNSET)
    try:
        from agent.runtime_cwd import clear_session_cwd

        clear_session_cwd()
    except Exception:
        pass


def set_conversation_context(conversation_id: str, event_id: str) -> list:
    """canonical conversation.v1 턴의 conversation_id/event_id를 현재 컨텍스트에 싣는다.

    ``set_session_vars``/``clear_session_vars`` 쌍과 달리 이 helper는
    **중첩 안전(nesting-safe)** 하도록 ``var.reset(token)`` 방식으로 설계했다.
    ``set_session_vars``는 세션 하나가 프로세스 수명 전체에 걸쳐 있다고 가정하고
    ``clear_session_vars``에서 무조건 ``""``로 되돌리지만, 이 컨텍스트는 큐
    어댑터의 ``_dispatch_conversation_turn`` 처럼 **더 좁은 스코프**(턴 하나의
    dispatch) 안에서만 유효해야 한다. 토큰 기반 reset이라야 그 스코프가
    끝났을 때 정확히 "호출 전 값"(대개 ``_UNSET``, 드물게 바깥 스코프가 이미
    set해둔 값)으로 복원되고, 동시에 여러 턴이 겹쳐도 서로의 컨텍스트를
    덮어쓰지 않는다.

    사용 패턴은 둘이다: ① 중첩 스코프 바인딩 — 반환 토큰을
    ``reset_conversation_context``에 넘겨 스코프 종료 시 복원한다(폴링
    태스크처럼 오래 사는 컨텍스트). ② 턴 태스크 수명 바인딩 — 큐 어댑터의
    ``on_processing_start``처럼 턴 태스크 초입에서 재바인딩할 때는 토큰을
    버려도 된다: 그 컨텍스트는 태스크 종료와 함께 통째로 소멸하므로 reset이
    필요 없다.
    """
    return [
        _SESSION_CONVERSATION_ID.set(conversation_id),
        _SESSION_CONVERSATION_EVENT_ID.set(event_id),
    ]


def reset_conversation_context(tokens: list) -> None:
    """``set_conversation_context``가 반환한 토큰으로 이전 상태를 복원한다.

    set 순서의 역순(LIFO)으로 reset한다 — 이 두 변수는 서로 독립적이라
    기능적으로는 순서가 상관없지만, 큐 어댑터의 기존
    ``_ACTIVE_QUEUE_EVENT_ID``/``_ACTIVE_QUEUE_DELIVERY_TOKEN`` set/reset
    쌍과 스타일을 맞춘다.
    """
    conversation_token, event_token = tokens
    _SESSION_CONVERSATION_EVENT_ID.reset(event_token)
    _SESSION_CONVERSATION_ID.reset(conversation_token)


def get_session_env(name: str, default: str = "") -> str:
    """Read a session context variable by its legacy ``HERMES_SESSION_*`` name.

    Drop-in replacement for ``os.getenv("HERMES_SESSION_*", default)``.

    Resolution order:
    1. Context variable (set by the gateway for concurrency-safe access).
       If the variable was explicitly set (even to ``""``) via
       ``set_session_vars`` or ``clear_session_vars``, that value is
       returned — **no fallback to os.environ**.
    2. ``os.environ`` (only when the context variable was never set in
       this context — i.e. CLI, cron scheduler, and test processes that
       don't use ``set_session_vars`` at all).
    3. *default*
    """
    import os

    var = _VAR_MAP.get(name)
    if var is not None:
        value = var.get()
        if value is not _UNSET:
            return value
    # Fall back to os.environ for CLI, cron, and test compatibility
    return os.getenv(name, default)


def async_delivery_supported() -> bool:
    """Whether the current session can deliver a background completion later.

    Returns ``False`` only when the active session was explicitly bound by a
    stateless adapter (the API server) that cannot route a notification back to
    the agent after the turn ends. CLI, cron, and the real gateway platforms —
    and any path that never bound the contextvar — return ``True``.

    Tools that promise async delivery (``terminal`` notify_on_complete /
    watch_patterns, ``delegate_task`` background=True) consult this before
    registering a watcher / dispatching a detached child, so they can refuse a
    promise the channel can't keep instead of silently no-op'ing.
    """
    value = _SESSION_ASYNC_DELIVERY.get()
    if value is _UNSET:
        return True
    return bool(value)
