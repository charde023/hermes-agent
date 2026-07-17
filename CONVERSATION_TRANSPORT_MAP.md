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
- 현재 Agent Directory fixture에는 `chami`, `chadol`, `smith`만 있다. `may`, `anna`, `jeff`는 Directory 등록 전 성공으로 보고하지 않는다.

## 변경 시 게이트

`scripts/run_tests.sh`만 사용한다. Slack 범위 변경은 multi-workspace·approval·authz를, 큐 변경은 adapter·handoff를 함께 돌린 뒤 전체 suite, `git diff --check`, 비밀값 검사를 통과한다.
