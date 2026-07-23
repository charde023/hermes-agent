# Conversation Transport MAP

Slack 멀티 워크스페이스와 Agent Directory 기반 `conversation.v1` 큐 대화를 함께 다루는 항법도다.

## 진입점

| 목적 | 진입점 | 계약 테스트 |
|---|---|---|
| Slack 설치·워크스페이스 판별 | `plugins/platforms/slack/adapter.py` | `tests/gateway/test_slack_multi_workspace.py` |
| Slack 워크스페이스 공개 상태 영수증 | `plugins/platforms/slack/workspace_status.py` | `tests/gateway/test_slack_workspace_status.py` |
| 워크스페이스별 사용자 권한 | `gateway/authz_mixin.py` | `tests/gateway/test_slack_workspace_authz.py` |
| 세션·응답 범위 보존 | `gateway/session.py`, `gateway/platforms/base.py` | `tests/gateway/test_session.py`, `tests/gateway/test_platform_base.py` |
| 큐 수신·lease fencing | `plugins/platforms/queue/adapter.py` | `tests/gateway/test_queue_adapter.py` |
| 봇 간 handoff·receipt | `tools/queue_handoff_tool.py`, `tools/queue_handoff_receipts.py` | `tests/gateway/test_queue_handoff_tool.py` |
| deferred terminal 등록소·활성 턴 registry | `gateway/conversation_closeout.py` | `tests/gateway/test_conversation_closeout.py` |
| 시작·종료·인계 보고 | `gateway/run.py`, `gateway/config.py` | `tests/gateway/test_restart_notification.py`, `tests/gateway/test_gateway_shutdown.py` |

## 흐름

1. Slack 이벤트는 외부 envelope·body·authorization·context의 `team_id`가 모두 일치해야 한다.
2. `SessionSource.scope_id`가 세션 키와 모든 발신 metadata에 보존된다.
3. 발신은 `metadata.scope_id` → 학습된 채널 매핑 → 단일 설치 fallback 순서다. 둘 이상의 설치에서 범위가 없거나 충돌하면 실패한다.
4. 큐 워커는 Agent Directory에서 활성 대상과 credential ref를 읽고 `conversation.v1` delivery를 claim한다.
5. claim의 canonical event를 처리하며 `accepted` → `completed|error` receipt를 남긴다. 현재 lease 판단은 event 안의 과거 `delivery_status`가 아니라 바깥 `active_delivery_token`을 사용한다.
6. 실제 전송 직전에도 generation과 active delivery token을 다시 검사한다. lease를 잃은 이전 워커는 응답·완료 처리를 할 수 없다.

## 설정 SSOT

- 비밀값: 환경변수만 사용한다. YAML에는 `env://NAME` ref만 둔다.
- Slack: `workspace_bot_token_refs`, `workspace_allowed_users`, `workspace_allowed_channels`, `workspace_home_channels`, `workspace_keys(team_id → team/apom 같은 안정 키)`.
- 관제탑 공개 영수증: 프로파일별 `state/slack_workspace_status.json`, mode `0600`. `connected=true`면 `error_code=null`, 끊겼으면 제한된 안전 코드만 쓴다. 토큰·URL·사용자 ID·예외 문자열은 금지한다.
- Queue: `protocol_version=conversation.v1`, endpoint,
  `${QUEUE_REPO_ROOT}/contracts/examples/agent-directory.v1.json` 로컬 SSOT,
  private env의 `QUEUE_CONVERSATION_CREDENTIAL`.
- 공통 home channel은 `home_channel.chat_id` + `home_channel.scope_id` 쌍이다.

## conversation.v1 producer (Phase C, 2026-07-23)

hermes가 canonical event를 생산한다 — 계약 SSOT는 slack_agent `design/2026-07-20_canonical-event_계약확정서.md`.

- **인바운드 게이팅**: `request`/`question`만 ack+코어 dispatch(`_ACTIONABLE_EVENT_TYPES`). 그 외 event_type은 ack·dispatch 없이 `accepted→completed`로 조용히 소비 — 안 하면 두 v2 어댑터 간 **무한 ack 핑퐁**(적대검증 재현 6홉). 에이전트 가시화는 P2.
- **conversation_id는 방향 무관**(thread 해시)이고, 답신 handoff는 `HERMES_SESSION_CONVERSATION_ID`/`_EVENT_ID` ContextVar(세션 컨텍스트 통로)로 **부모를 승계**한다.
- **대화 세대(epoch, #21 E1)**: terminal로 닫힌 스레드의 재사용 요청이 같은 conversation_id로 가면 ledger가 append를 영구 거부한다(스레드 벽돌·generic 400이라 클라이언트 식별 불가). 신규 발급 경로는 서버 `get_conversation_state`로 **비terminal 첫 세대를 탐색**해 열린 세대에 합류하거나 미존재 세대로 새 대화를 연다. 산식: epoch 0 = 기존 해시 그대로(하위호환·기존 대화 정체성 불변), N≥1만 seed·request identity에 포함(세대별 conversation_id/idempotency_key/event_id 분리 — 재시도 dedupe는 같은 세대 안에서만). 조회 실패(구서버 404 등)는 epoch 0 폴백(기존 동작). 턴 밖(요청자) terminal은 열린 세대가 있어야 하며 없으면 거부(빈 대화 생성-즉시-종결 방지). 답신(부모 승계)·활성 턴 내 deferred terminal은 탐색을 타지 않는다.
- **producer event는 byte-stable이 계약**: `created_at`을 인바운드 event에서 승계 — 서버 dedupe가 payload_hash 일치를 요구해, 재빌드가 1바이트라도 다르면 idempotency collision→영구 error가 된다.
- **producer 노이즈 게이트는 첫줄 prefix만**(`_is_producer_noise`). bridge `is_system_noise`(substring-anywhere)는 표시측 전용 — producer에서 쓰면 진짜 답변이 event째 소멸한다.
- **같은 턴 terminal = deferred(F4, #45)**: 활성 인바운드 턴 안의 도구 terminal(completed/failed/no_action)은 즉시 append하지 않고 `gateway/conversation_closeout.py`에 등록만 한다(도구 반환 `deferred=true`) — 어댑터 `on_processing_complete`가 SUCCESS 시 reply 발신 뒤 append해 §5 순서(reply→terminal)를 기계 보장(안정 키 `queue_close:{agent}:{parent_event_id}`·created_at 승계 byte-stable). FAILURE/CANCELLED·advance fence·conversation 불일치는 폐기. ★"활성 턴" 판정 권위는 ContextVar 존재가 아니라 **활성 턴 registry**(dispatch가 `mark_turn_active`, 훅이 `mark_turn_closed`로 원자 소비)다 — 스냅샷은 턴 종료 후에도 상속처(delegate background 워커)에 남아, 존재만으로 defer하면 고아 등록(성공 반환 동반 침묵 소실)이 된다. 활성 턴 안 terminal은 그 턴의 요청자(to==requester)에게만 — 다른 handoff 종결은 턴 밖에서(즉시 append). 잔여 창: advance(completed) 성공 직후(또는 응답만 유실) terminal append 전 크래시/파티션이면 delivery가 completed라 재claim이 없어 그 턴의 종결 의도만 소실 — 대화는 열린 채 가시적으로 남고 요청자측/후속 턴 종결이 폴백(§5 "성급 종결 금지" 철학상 안전측).
- **턴별 컨텍스트 재바인딩(F2, #45) — drain 턴도 자기 식별자로 답한다**: 코어 pending drain 태스크는 직전 턴 태스크의 ContextVar 스냅샷을 상속하므로, `QueueAdapter.on_processing_start`(코어 lifecycle hook — 모든 턴 태스크 초입·태스크 컨텍스트 안에서 실행)가 `event.message_id→_inflight` 조회로 재바인딩한다(v2: event_id·delivery token·conversation ctx 3종 / v1: event_id만). 코어 base.py 수정 0. ★완료훅의 ContextVar 클리어는 여전히 금지: 재바인딩이 no-touch로 빠지는 소유권 상실 턴(_inflight 미존재)에서 None이 상속되면 v2 send가 "부모 턴 없음" no-op success로 강등돼 침묵 소실이 재발한다 — stale 값이 남아야 ownership-lost로 가시화(fail-visible 백스톱).
- **★병합-흡수 claim 잔여(미해소, #21 전 선행 검토)**: 같은 대화 후속 턴 2건 이상이 한 턴 처리 중 도착하면 코어 `merge_pending_message_event`가 TEXT 병합(message_id는 첫 건 유지)해 흡수된 건의 claim이 고아가 된다 — 병합 답으로 내용은 답변되지만 그 delivery는 accepted 정체 후 lease 만료(CLAIM_TTL 1800s) 재claim·재처리돼 같은 대화에 중복 답변 가능. F2 이전엔 같은 조건에서 병합 턴 답이 통째로 유실(error·비재claim)됐으므로 유실→중복으로 완화된 상태다. 근본 수리는 대화별 직렬화(어댑터 로컬 FIFO 또는 서버 claim 계약) 설계 과제.
- **v2 전환 스위치** = 프로필 `.env` 3키(`QUEUE_PROTOCOL_VERSION`·`QUEUE_ENDPOINT`·`QUEUE_CONVERSATION_CREDENTIAL`) + `launchctl kickstart -k`(plist 무수정·bootout 불필요). ⚠️전환 즉시 옛 pending delivery를 claim하므로 **전환 전 stale delivery drain 필수**.

## ★ 음성지식·함정

- 한 Socket Mode app-level token은 같은 앱의 여러 설치를 받을 수 있다. bot token과 bot/user ID는 워크스페이스별이다.
- 쉼표로 합친 bot token, 평문 YAML token, 중복 token/team, `auth.test`의 team 불일치는 시작 단계에서 거부한다.
- 멀티 설치에서 workspace를 모르는 채널·파일·승인·reaction·thread 작업은 임의의 첫 client로 보내지 않는다.
- Slack channel/user/message ID는 워크스페이스 전역 고유가 아니다. cache key는 반드시 scope를 포함한다.
- Agent Directory가 존재하지만 잘못됐을 때 하드코딩 roster로 조용히 후퇴하지 않는다. legacy v1 fallback은 Directory 자체가 없고 명시적 legacy roster가 있을 때만 허용한다.
- `claim.event.delivery_status`는 append 당시의 원본 값이다. 현재 ownership은 outer lease/token으로 판단한다.
- 발신 전 fencing과 DB INSERT 사이에는 미세한 TOCTOU가 남는다. 완전 제거는 원자적 finish RPC가 필요하다.
- 라이브 APOM 설치 성공은 코드·설정 지원과 별개다. OAuth receipt, APOM inbound, APOM outbound, 재시작 후 지속성까지 각각 증거가 필요하다.
- `workspace_keys`가 있으면 `auth.test`로 확인된 workspace 집합과 정확히 같아야 한다. 예상 밖 workspace를 영수증에 추가하면 관제탑이 전체 receipt를 거부한다.
- 현재 Agent Directory fixture에는 `chami`, `chadol`, `smith`, `claude-dev`(queue_native·러너 #21 E2·slack_app 없음)가 있다. `may`, `anna`, `jeff`는 Directory 등록 전 성공으로 보고하지 않는다.

## 변경 시 게이트

`scripts/run_tests.sh`만 사용한다. Slack 범위 변경은 multi-workspace·approval·authz를, 큐 변경은 adapter·handoff를 함께 돌린 뒤 전체 suite, `git diff --check`, 비밀값 검사를 통과한다.
