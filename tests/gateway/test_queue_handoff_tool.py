"""queue_handoff agent-callable 도구 테스트 (TDD).

큐 내부 에이전트 간 handoff 전용 도구. send_message처럼 외부 플랫폼으로
발신하는 게 아니라, slack_agent 로컬 SQLite 큐(bridge.local_repo.SQLiteQueueRepo)의
slack_inbox에 target=<상대 에이전트>, slack_user_id=<자기 에이전트>로 INSERT 해서
상대 워커가 claim 하게 만든다.

tmp SQLite + env 픽스처. sys.path 조작(QUEUE_REPO_ROOT)은 tool 본체가 수행한다.

커버:
T1 정상 handoff -> slack_inbox에 target/slack_user_id/text row 생성 + success 반환
T2 check_fn: QUEUE_AGENT/QUEUE_REPO_ROOT + QUEUE_DB_PATH 또는 QUEUE_ENDPOINT 필요
T3 (negative) to=자기자신 / to="" / to=raw 슬랙채널ID -> error + insert 없음
T4 (registry) import 시 registry에 name=queue_handoff 자동 등록(toolset/check_fn/is_async)
T5 반환 형식이 도구 반환 규약(JSON str: success/target/message_id 또는 error)과 일치
"""

import asyncio
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

# slack_agent 레포 루트(bridge 패키지 제공). clean-env test wrapper가 사용자 env를
# 지우므로, 현재 Session 2 worktree가 있으면 S1 계약을 포함한 쪽을 우선한다.
_LIVE_SLACK_AGENT_ROOT = Path("/Users/charde023/workspace/slack_agent")
_SESSION2_SLACK_AGENT_ROOT = (
    _LIVE_SLACK_AGENT_ROOT / ".worktrees" / "slack-conversation-s2"
)
_DEFAULT_SLACK_AGENT_ROOT = (
    _SESSION2_SLACK_AGENT_ROOT
    if (_SESSION2_SLACK_AGENT_ROOT / "contracts" / "conversation-event.schema.json").is_file()
    else _LIVE_SLACK_AGENT_ROOT
)
SLACK_AGENT_ROOT = os.environ.get(
    "QUEUE_TEST_REPO_ROOT", str(_DEFAULT_SLACK_AGENT_ROOT)
)
_HAS_SLACK_AGENT = (
    Path(SLACK_AGENT_ROOT, "bridge", "local_repo.py").is_file()
    and Path(SLACK_AGENT_ROOT, "bridge", "agent_repo.py").is_file()
)
_HAS_CONVERSATION_V1 = (
    _HAS_SLACK_AGENT
    and Path(SLACK_AGENT_ROOT, "bridge", "conversation_http.py").is_file()
    and Path(SLACK_AGENT_ROOT, "contracts", "conversation-event.schema.json").is_file()
)

if _HAS_SLACK_AGENT and SLACK_AGENT_ROOT not in sys.path:
    # append — insert(0)은 slack_agent 루트의 config.py(시크릿)가 전역 최우선
    # import 되는 구조라 금지(어댑터/도구 본체와 동일 규칙).
    sys.path.append(SLACK_AGENT_ROOT)


@unittest.skipUnless(_HAS_SLACK_AGENT, f"slack_agent repo not found: {SLACK_AGENT_ROOT}")
class QueueHandoffToolTestBase(unittest.TestCase):
    """tmp SQLite + env 픽스처 공통 베이스."""

    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db_path = str(Path(tmpdir.name) / "queue-test.sqlite3")
        self.hermes_home = str(Path(tmpdir.name) / "hermes-home")
        self.env = {
            "QUEUE_DB_PATH": self.db_path,
            "QUEUE_AGENT": "chami",
            "QUEUE_REPO_ROOT": SLACK_AGENT_ROOT,
            # Session 2부터 기본 로스터는 Agent Directory다. 이 legacy fixture는
            # 외부 slack_agent checkout이 아직 S0/S1 계약을 포함하지 않는 CI에서도
            # v1 회귀 테스트가 명시적 fallback으로 계속 돌게 한다.
            "QUEUE_KNOWN_AGENTS": "chami,chadol,mei,anna,jeff",
            "HERMES_HOME": self.hermes_home,
        }
        env_guard = patch.dict(os.environ, self.env, clear=False)
        env_guard.start()
        self.addCleanup(env_guard.stop)
        # 주변 셸/게이트웨이 세션 env가 새어 위양성을 내지 않게 정리.
        for k in (
            "HERMES_SESSION_THREAD_ID",
            "HERMES_SESSION_CHAT_ID",
            "HERMES_SESSION_KEY",
            "HERMES_SESSION_MESSAGE_ID",
            "QUEUE_ENDPOINT",
            "QUEUE_TOKEN",
            "QUEUE_PROTOCOL_VERSION",
            "QUEUE_CONVERSATION_CREDENTIAL",
        ):
            os.environ.pop(k, None)

    def make_repo(self):
        from bridge.local_repo import SQLiteQueueRepo

        # 생성 시 _init_schema()로 slack_inbox/outbox 테이블 자동 생성.
        return SQLiteQueueRepo(self.db_path)

    def call_tool(self, **args):
        from tools.queue_handoff_tool import queue_handoff_tool

        result = asyncio.run(queue_handoff_tool(args))
        return json.loads(result)

    def inbox_rows(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        try:
            return [
                dict(r)
                for r in con.execute(
                    "SELECT slack_user_id, target, text, status FROM slack_inbox"
                ).fetchall()
            ]
        finally:
            con.close()


class TestHandoffInsert(QueueHandoffToolTestBase):
    """T1: 정상 handoff → slack_inbox row 생성 + success 반환."""

    def test_handoff_inserts_inbox_row(self):
        self.make_repo()  # 스키마 선생성
        result = self.call_tool(to="chadol", message="배포 좀 봐줘")

        self.assertTrue(result.get("success"), result)
        self.assertEqual(result.get("target"), "chadol")
        self.assertTrue(result.get("message_id"))

        rows = self.inbox_rows()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["target"], "chadol")
        self.assertEqual(row["slack_user_id"], "chami")  # 발신 = 자기 에이전트
        self.assertEqual(row["text"], "배포 좀 봐줘")
        self.assertEqual(row["status"], "pending")

    def test_handoff_uses_session_thread_when_present(self):
        self.make_repo()
        with patch.dict(os.environ, {"HERMES_SESSION_THREAD_ID": "1720000000.111"}):
            result = self.call_tool(to="chadol", message="스레드 이어가기")
        self.assertTrue(result.get("success"), result)
        con = sqlite3.connect(self.db_path)
        try:
            thread_ts = con.execute(
                "SELECT thread_ts FROM slack_inbox WHERE target='chadol'"
            ).fetchone()[0]
        finally:
            con.close()
        self.assertEqual(thread_ts, "1720000000.111")

    def test_chat_id_not_used_as_thread_ts(self):
        # THREAD_ID 없이 CHAT_ID(채널 식별자)만 있을 때, 채널ID를 thread_ts로
        # 쓰면 안 된다(수신측 세션 오분류) — 합성 qh- 스레드로 새로 시작해야 한다.
        self.make_repo()
        with patch.dict(os.environ, {"HERMES_SESSION_CHAT_ID": "C0B69KP8G2J"}):
            result = self.call_tool(to="chadol", message="채널ID 폴백 금지")
        self.assertTrue(result.get("success"), result)
        self.assertNotEqual(result.get("thread_ts"), "C0B69KP8G2J")
        self.assertTrue(str(result.get("thread_ts")).startswith("qh-"), result)

    def test_same_request_reuses_stable_idempotency_key_without_second_row(self):
        self.make_repo()
        args = {"to": "chadol", "message": "한 번만 처리", "thread": "topic-1"}

        first = self.call_tool(**args)
        second = self.call_tool(**args)

        self.assertTrue(first.get("success"), first)
        self.assertTrue(second.get("success"), second)
        self.assertEqual(first["idempotency_key"], second["idempotency_key"])
        self.assertTrue(first["idempotency_key"].startswith("queue_handoff:chami:"))
        self.assertEqual(first["message_id"], second["message_id"])
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(len(self.inbox_rows()), 1)

    def test_explicit_idempotency_key_collision_is_rejected_client_side(self):
        self.make_repo()
        first = self.call_tool(
            to="chadol", message="원본", thread="topic-1", idempotency_key="job-42"
        )
        second = self.call_tool(
            to="chadol", message="변조", thread="topic-1", idempotency_key="job-42"
        )

        self.assertTrue(first.get("success"), first)
        self.assertIn("collision", second.get("error", ""))
        self.assertEqual(len(self.inbox_rows()), 1)


class TestCheckFn(QueueHandoffToolTestBase):
    """T2: check_fn — agent/root + db_path 또는 endpoint가 필요."""

    def test_all_env_present_true(self):
        from tools.queue_handoff_tool import check_queue_handoff

        self.assertTrue(check_queue_handoff())

    def test_missing_agent_or_repo_root_false(self):
        from tools.queue_handoff_tool import check_queue_handoff

        for key in ("QUEUE_AGENT", "QUEUE_REPO_ROOT"):
            with self.subTest(missing=key):
                with patch.dict(os.environ, {}, clear=False):
                    os.environ.pop(key, None)
                    self.assertFalse(check_queue_handoff())

    def test_missing_db_path_false_without_endpoint(self):
        from tools.queue_handoff_tool import check_queue_handoff

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("QUEUE_DB_PATH", None)
            os.environ.pop("QUEUE_ENDPOINT", None)
            self.assertFalse(check_queue_handoff())

    def test_endpoint_allows_missing_db_path(self):
        from tools.queue_handoff_tool import check_queue_handoff

        with patch.dict(os.environ, {"QUEUE_ENDPOINT": "http://127.0.0.1:8770"}, clear=False):
            os.environ.pop("QUEUE_DB_PATH", None)
            self.assertTrue(check_queue_handoff())


class TestDefenses(QueueHandoffToolTestBase):
    """T3: 방어 — 자기자신/빈값/raw 슬랙채널ID는 error + insert 없음."""

    def _assert_error_no_insert(self, **args):
        self.make_repo()
        result = self.call_tool(**args)
        self.assertIn("error", result)
        self.assertNotIn("success", result)
        self.assertEqual(self.inbox_rows(), [])

    def test_handoff_to_self_rejected(self):
        self._assert_error_no_insert(to="chami", message="자기 자신")

    def test_empty_target_rejected(self):
        self._assert_error_no_insert(to="   ", message="빈 타겟")

    def test_raw_slack_channel_id_rejected(self):
        self._assert_error_no_insert(to="C0B69KP8G2J", message="채널ID 오입력")

    def test_case_variant_of_self_rejected(self):
        # 'Chami'는 자기 자신(QUEUE_AGENT=chami)의 대소문자 변형 — self 가드를
        # 우회하면 아무도 claim 못 하는 죽은 row가 된다.
        self._assert_error_no_insert(to="Chami", message="대문자 self")

    def test_unknown_agent_key_rejected(self):
        # 오타·환각 키는 어떤 워커도 claim 못 하므로 삽입 없이 거부.
        self._assert_error_no_insert(to="chadl", message="오타 타겟")
        self._assert_error_no_insert(to="claude", message="로스터 밖 키")

    def test_oversized_message_rejected(self):
        self._assert_error_no_insert(to="chadol", message="x" * 40_001)

    def test_uppercase_valid_agent_normalized_and_delivered(self):
        # 'Chadol'은 유효 에이전트의 대소문자 변형 — 소문자 canonical로 정규화해
        # target='chadol'로 배달되어야 한다(수신 워커의 QUEUE_AGENT와 정확일치).
        self.make_repo()
        result = self.call_tool(to="Chadol", message="대소문자 정규화")
        self.assertTrue(result.get("success"), result)
        self.assertEqual(result.get("target"), "chadol")
        rows = self.inbox_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["target"], "chadol")

    def test_known_agents_env_override(self):
        # Directory가 **없는 legacy checkout**에서만 QUEUE_KNOWN_AGENTS fallback.
        empty_repo = tempfile.TemporaryDirectory()
        self.addCleanup(empty_repo.cleanup)
        self.make_repo()
        with patch.dict(
            os.environ,
            {
                "QUEUE_REPO_ROOT": empty_repo.name,
                "QUEUE_KNOWN_AGENTS": "kc,zed",
            },
            clear=False,
        ), patch("tools.queue_handoff_tool._do_insert", return_value=True):
            result = self.call_tool(to="kc", message="확장 로스터")
        self.assertTrue(result.get("success"), result)
        self.assertEqual(result.get("target"), "kc")

    def test_missing_directory_and_missing_legacy_roster_fails_closed(self):
        empty_repo = tempfile.TemporaryDirectory()
        self.addCleanup(empty_repo.cleanup)
        self.make_repo()
        with patch.dict(
            os.environ,
            {"QUEUE_REPO_ROOT": empty_repo.name},
            clear=False,
        ):
            os.environ.pop("QUEUE_KNOWN_AGENTS", None)
            result = self.call_tool(to="chadol", message="로스터 없음")
        self.assertIn("Agent Directory", result.get("error", ""))
        self.assertEqual(self.inbox_rows(), [])

    def test_missing_agent_identity_rejected(self):
        # QUEUE_AGENT 없으면 발신자 정체성이 없어 handoff 불가.
        self.make_repo()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("QUEUE_AGENT", None)
            result = self.call_tool(to="chadol", message="정체성 없음")
        self.assertIn("error", result)
        self.assertEqual(self.inbox_rows(), [])


class TestRegistryRegistration(QueueHandoffToolTestBase):
    """T4: import 시 registry 자동 등록 확인."""

    def test_registered_in_registry(self):
        import tools.queue_handoff_tool  # noqa: F401  (등록 부작용)
        from tools.registry import registry

        entry = registry.get_entry("queue_handoff")
        self.assertIsNotNone(entry, "queue_handoff 도구가 registry에 없음")
        self.assertEqual(entry.name, "queue_handoff")
        self.assertEqual(entry.toolset, "queue")  # hermes-queue 번들 자동 편입 조건
        self.assertTrue(entry.is_async)
        self.assertIsNotNone(entry.check_fn)

    def test_exposed_via_get_definitions_when_env_present(self):
        import tools.queue_handoff_tool  # noqa: F401
        from tools.registry import registry

        # check_fn 30s TTL 캐시 회피 위해 이름으로 직접 조회.
        defs = registry.get_definitions({"queue_handoff"})
        names = [d["function"]["name"] for d in defs]
        self.assertIn("queue_handoff", names)

    def test_exposed_in_hermes_queue_toolset(self):
        # 실제 세션-노출 경로: resolve_toolset("hermes-queue")의 플러그인-플랫폼
        # 자동생성 분기가 toolset=="queue" 도구를 끌어와야 큐 세션이 이 도구를
        # 본다. registry 등록/check_fn만으로는 이 경로를 증명하지 못한다 —
        # toolset명 변경·자동생성 분기 회귀를 이 테스트가 잡는다.
        import tools.queue_handoff_tool  # noqa: F401
        from toolsets import resolve_toolset
        from gateway.platform_registry import platform_registry

        with patch.object(
            platform_registry, "is_registered", side_effect=lambda p: p == "queue"
        ):
            resolved = resolve_toolset("hermes-queue")
        self.assertIn("queue_handoff", resolved)


class TestDynamicAgentDirectoryRoster(QueueHandoffToolTestBase):
    """Agent Directory의 capability/peer ACL이 schema와 runtime의 같은 SSOT다."""

    def _directory_root(self):
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: shutil.rmtree(root, ignore_errors=True))
        path = root / "contracts" / "examples"
        path.mkdir(parents=True)
        shutil.copyfile(
            Path(SLACK_AGENT_ROOT, "contracts", "agent-directory.schema.json"),
            root / "contracts" / "agent-directory.schema.json",
        )
        validator_dir = root / "bridge"
        validator_dir.mkdir()
        shutil.copyfile(
            Path(SLACK_AGENT_ROOT, "bridge", "conversation_contracts.py"),
            validator_dir / "conversation_contracts.py",
        )
        directory = {
            "contract_version": "1.0",
            "workspaces": [
                {
                    "workspace_key": "team",
                    "display_name": "테스트 팀",
                    "slack_team_id": "T0TEAM",
                    "slack_enterprise_id": None,
                    "canonical_channel_ids": ["C0TEAM"],
                    "enabled": True,
                },
                {
                    "workspace_key": "apom",
                    "display_name": "테스트 APOM",
                    "slack_team_id": "T0APOM",
                    "slack_enterprise_id": None,
                    "canonical_channel_ids": ["C0APOM"],
                    "enabled": False,
                },
            ],
            "agents": [
                {
                    "agent_id": "chami",
                    "display_name": "차미",
                    "owner": "차드",
                    "transport": "hybrid",
                    "protocol_version": "conversation.v1",
                    "capabilities": ["conversation", "queue_handoff"],
                    "trust_tier": "internal",
                    "allowed_peers": ["zed", "observer", "invited"],
                    "slack_app": {
                        "app_id": "A0CHAMI",
                        "app_token_ref": "env://CHAMI_SLACK_APP_TOKEN",
                        "leader_policy": "single_active_gateway",
                        "installations": [
                            {
                                "installation_id": "chami:team",
                                "workspace_key": "team",
                                "bot_user_id": "U0CHAMI",
                                "bot_id": "B0CHAMI",
                                "bot_token_ref": "env://CHAMI_SLACK_BOT_TOKEN_TEAM",
                                "granted_scopes": [],
                                "enabled": True,
                            },
                            {
                                "installation_id": "chami:apom",
                                "workspace_key": "apom",
                                "bot_user_id": None,
                                "bot_id": None,
                                "bot_token_ref": "env://CHAMI_SLACK_BOT_TOKEN_APOM",
                                "granted_scopes": [],
                                "enabled": False,
                            },
                        ],
                    },
                },
                {
                    "agent_id": "zed",
                    "display_name": "제드",
                    "owner": "테스트",
                    "transport": "queue_native",
                    "protocol_version": "conversation.v1",
                    "capabilities": ["conversation", "queue_handoff"],
                    "trust_tier": "trusted",
                    "allowed_peers": ["chami"],
                    "slack_app": None,
                },
                {
                    "agent_id": "observer",
                    "display_name": "관찰자",
                    "owner": "테스트",
                    "transport": "queue_native",
                    "protocol_version": "conversation.v1",
                    "capabilities": ["conversation"],
                    "trust_tier": "observed",
                    "allowed_peers": [],
                    "slack_app": None,
                },
                {
                    "agent_id": "invited",
                    "display_name": "초대봇",
                    "owner": "외부",
                    "transport": "queue_native",
                    "protocol_version": "conversation.v1",
                    "capabilities": ["conversation", "queue_handoff"],
                    "trust_tier": "invited",
                    "allowed_peers": ["chami"],
                    "slack_app": None,
                },
            ],
        }
        (path / "agent-directory.v1.json").write_text(
            json.dumps(directory, ensure_ascii=False), encoding="utf-8"
        )
        return root

    def test_directory_capability_and_allowed_peer_drive_runtime_roster(self):
        root = self._directory_root()
        with patch.dict(os.environ, {"QUEUE_REPO_ROOT": str(root)}, clear=False), patch(
            "tools.queue_handoff_tool._do_insert", return_value=True
        ):
            accepted = self.call_tool(to="zed", message="새 봇에게 인계")
            rejected = self.call_tool(to="observer", message="실행 권한 없는 봇")
            invited = self.call_tool(to="invited", message="승격 전 실행 금지")
            legacy_only = self.call_tool(to="chadol", message="Directory 밖")

        self.assertTrue(accepted.get("success"), accepted)
        self.assertIn("unknown agent", rejected.get("error", ""))
        self.assertIn("unknown agent", invited.get("error", ""))
        self.assertIn("unknown agent", legacy_only.get("error", ""))

    def test_dynamic_tool_schema_names_only_current_directory_targets(self):
        from tools.registry import registry

        root = self._directory_root()
        with patch.dict(os.environ, {"QUEUE_REPO_ROOT": str(root)}, clear=False):
            definition = next(
                row["function"]
                for row in registry.get_definitions({"queue_handoff"})
                if row["function"]["name"] == "queue_handoff"
            )

        self.assertIn("zed", definition["description"])
        self.assertIn("제드", definition["parameters"]["properties"]["to"]["description"])
        self.assertNotIn("chadol", definition["description"])
        self.assertNotIn("QUEUE_CONVERSATION_CREDENTIAL", json.dumps(definition))

    def test_invalid_existing_directory_does_not_fall_back_to_legacy_env(self):
        root = self._directory_root()
        directory_path = root / "contracts" / "examples" / "agent-directory.v1.json"
        directory = json.loads(directory_path.read_text(encoding="utf-8"))
        directory["agents"][0]["capabilities"] = "queue_handoff"
        directory_path.write_text(json.dumps(directory), encoding="utf-8")

        with patch.dict(
            os.environ,
            {
                "QUEUE_REPO_ROOT": str(root),
                "QUEUE_KNOWN_AGENTS": "chadol,zed",
            },
            clear=False,
        ), patch("tools.queue_handoff_tool._do_insert", return_value=True) as insert:
            result = self.call_tool(to="zed", message="깨진 Directory 우회 금지")

        self.assertIn("Agent Directory", result.get("error", ""))
        insert.assert_not_called()

    def test_schema_forbidden_extra_field_does_not_open_roster(self):
        root = self._directory_root()
        directory_path = root / "contracts" / "examples" / "agent-directory.v1.json"
        directory = json.loads(directory_path.read_text(encoding="utf-8"))
        directory["unexpected_authorization"] = {"zed": True}
        directory_path.write_text(json.dumps(directory), encoding="utf-8")

        with patch.dict(
            os.environ,
            {
                "QUEUE_REPO_ROOT": str(root),
                "QUEUE_KNOWN_AGENTS": "zed",
            },
            clear=False,
        ), patch("tools.queue_handoff_tool._do_insert", return_value=True) as insert:
            result = self.call_tool(to="zed", message="extra field 우회 금지")

        self.assertIn("Agent Directory", result.get("error", ""))
        insert.assert_not_called()

    def test_invalid_workspace_and_installation_do_not_open_roster(self):
        mutations = {
            "duplicate_workspace": lambda directory: directory["workspaces"][1].update(
                {"workspace_key": "team"}
            ),
            "installation_id_mismatch": lambda directory: directory["agents"][0][
                "slack_app"
            ]["installations"][0].update({"installation_id": "chami:other"}),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                root = self._directory_root()
                directory_path = (
                    root / "contracts" / "examples" / "agent-directory.v1.json"
                )
                directory = json.loads(directory_path.read_text(encoding="utf-8"))
                mutate(directory)
                directory_path.write_text(json.dumps(directory), encoding="utf-8")

                with patch.dict(
                    os.environ,
                    {
                        "QUEUE_REPO_ROOT": str(root),
                        "QUEUE_KNOWN_AGENTS": "zed",
                    },
                    clear=False,
                ), patch(
                    "tools.queue_handoff_tool._do_insert", return_value=True
                ) as insert:
                    result = self.call_tool(to="zed", message=f"{label} 우회 금지")

                self.assertIn("Agent Directory", result.get("error", ""))
                insert.assert_not_called()

    def test_directory_schema_checksum_drift_does_not_open_roster(self):
        root = self._directory_root()
        schema_path = root / "contracts" / "agent-directory.schema.json"
        schema_path.write_text(
            schema_path.read_text(encoding="utf-8") + "\n", encoding="utf-8"
        )

        with patch.dict(
            os.environ,
            {
                "QUEUE_REPO_ROOT": str(root),
                "QUEUE_KNOWN_AGENTS": "zed",
            },
            clear=False,
        ), patch("tools.queue_handoff_tool._do_insert", return_value=True) as insert:
            result = self.call_tool(to="zed", message="checksum drift 우회 금지")

        self.assertIn("Agent Directory", result.get("error", ""))
        insert.assert_not_called()


class TestConversationV1Receipts(QueueHandoffToolTestBase):
    """conversation.v1은 stable event 재조회로 accepted/completed receipt를 추적한다."""

    def _v2_env(self):
        return {
            "QUEUE_PROTOCOL_VERSION": "conversation.v1",
            "QUEUE_ENDPOINT": "http://127.0.0.1:8770",
            "QUEUE_CONVERSATION_CREDENTIAL": "private-agent-credential",
        }

    def test_retry_reuses_byte_identical_event_and_observes_accepted(self):
        seen = []

        def append(*args, **kwargs):
            seen.append(json.loads(json.dumps(kwargs["event"])))
            return {
                "receipt": "enqueued",
                "event_id": kwargs["event"]["event_id"],
                "delivery_status": "pending" if len(seen) == 1 else "accepted",
                "inserted": len(seen) == 1,
            }

        with patch.dict(os.environ, self._v2_env(), clear=False), patch(
            "tools.queue_handoff_tool._do_conversation_append", side_effect=append
        ):
            first = self.call_tool(
                to="chadol", message="배포 확인", thread="incident-1"
            )
            second = self.call_tool(
                to="chadol", message="배포 확인", thread="incident-1"
            )

        self.assertEqual(seen[0], seen[1])
        self.assertEqual(first["idempotency_key"], second["idempotency_key"])
        self.assertEqual(first["delivery_status"], "pending")
        self.assertEqual(second["delivery_status"], "accepted")
        self.assertTrue(second["accepted"])
        self.assertFalse(second["completed"])
        self.assertEqual(seen[0]["capability"], "queue_handoff")
        self.assertEqual(seen[0]["sender_agent_id"], "chami")
        self.assertEqual(seen[0]["target_agent_ids"], ["chadol"])
        state_db = Path(self.hermes_home) / "state" / "queue_handoff_receipts.sqlite3"
        self.assertNotIn(b"private-agent-credential", state_db.read_bytes())
        self.assertNotIn(
            "private-agent-credential",
            json.dumps([first, second], ensure_ascii=False),
        )

    def test_receipt_action_loads_durable_event_and_observes_completed(self):
        calls = []

        def append(*args, **kwargs):
            calls.append(kwargs["event"])
            status = "pending" if len(calls) == 1 else "completed"
            return {
                "receipt": "enqueued",
                "event_id": kwargs["event"]["event_id"],
                "delivery_status": status,
                "inserted": len(calls) == 1,
            }

        with patch.dict(os.environ, self._v2_env(), clear=False), patch(
            "tools.queue_handoff_tool._do_conversation_append", side_effect=append
        ):
            sent = self.call_tool(
                to="chadol", message="결과 추적", idempotency_key="deploy-2026-07-17"
            )
            receipt = self.call_tool(
                action="receipt", idempotency_key="deploy-2026-07-17"
            )

        self.assertTrue(sent.get("success"), sent)
        self.assertEqual(receipt["delivery_status"], "completed")
        self.assertTrue(receipt["accepted"])
        self.assertTrue(receipt["completed"])
        self.assertEqual(calls[0], calls[1])

    def test_v2_requires_endpoint_and_private_credential(self):
        with patch.dict(
            os.environ,
            {"QUEUE_PROTOCOL_VERSION": "conversation.v1", "QUEUE_ENDPOINT": "http://x"},
            clear=False,
        ):
            os.environ.pop("QUEUE_CONVERSATION_CREDENTIAL", None)
            result = self.call_tool(to="chadol", message="설정 미완성")
        self.assertIn("credential", result.get("error", "").lower())

    def test_unknown_protocol_fails_closed_instead_of_falling_back_to_v1(self):
        self.make_repo()
        with patch.dict(os.environ, {"QUEUE_PROTOCOL_VERSION": "conversation.v9"}):
            result = self.call_tool(to="chadol", message="미지원 프로토콜")
        self.assertIn("unsupported", result.get("error", ""))
        self.assertEqual(self.inbox_rows(), [])

    def test_explicit_queue_v1_keeps_legacy_handoff(self):
        self.make_repo()
        with patch.dict(os.environ, {"QUEUE_PROTOCOL_VERSION": "queue.v1"}):
            result = self.call_tool(to="chadol", message="명시적 v1")
        self.assertTrue(result.get("success"), result)
        self.assertEqual(result["protocol_version"], "queue.v1")
        self.assertEqual(self.inbox_rows()[0]["text"], "명시적 v1")

    def test_transport_error_redacts_conversation_credential(self):
        with patch.dict(os.environ, self._v2_env(), clear=False), patch(
            "tools.queue_handoff_tool._do_conversation_append",
            side_effect=RuntimeError("private-agent-credential leaked upstream"),
        ):
            result = self.call_tool(to="chadol", message="오류 위생")
        rendered = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("private-agent-credential", rendered)
        self.assertIn("handoff", result.get("error", ""))

    @unittest.skipUnless(
        _HAS_CONVERSATION_V1,
        f"slack_agent conversation.v1 contract not found: {SLACK_AGENT_ROOT}",
    )
    def test_real_s1_http_ledger_roundtrip_tracks_pending_accepted_completed(self):
        """실 HTTP/client/ledger를 통과해 mock이 숨길 수 있는 contract drift를 잡는다."""
        from bridge.conversation_auth import AuthenticationError, ConversationAuthorizer
        from bridge.conversation_contracts import load_contract_bundle
        from bridge.conversation_http import dispatch_conversation
        from bridge.conversation_ledger import SQLiteConversationLedger
        from bridge.queue_server import make_server
        from bridge.repo import InMemoryQueueRepo

        contracts = Path(SLACK_AGENT_ROOT) / "contracts"
        bundle = load_contract_bundle(contracts)
        ledger = SQLiteConversationLedger(
            str(Path(self.hermes_home) / "s1-ledger.sqlite3"),
            directory=bundle.directory,
        )
        authorizer = ConversationAuthorizer(bundle.directory)

        def authenticate(provided):
            if provided != "private-agent-credential":
                raise AuthenticationError("invalid credential")
            return "chami"

        def dispatch_api(principal, method, params):
            return dispatch_conversation(ledger, authorizer, principal, method, params)

        server = make_server(
            InMemoryQueueRepo(),
            host="127.0.0.1",
            port=0,
            conversation_authenticate=authenticate,
            conversation_dispatch=dispatch_api,
        )
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        endpoint = f"http://127.0.0.1:{server.server_address[1]}"
        env = self._v2_env() | {
            "QUEUE_ENDPOINT": endpoint,
            "QUEUE_REPO_ROOT": SLACK_AGENT_ROOT,
        }
        try:
            with patch.dict(os.environ, env, clear=False):
                sent = self.call_tool(
                    to="chadol",
                    message="실계약 왕복",
                    thread="incident-real",
                    idempotency_key="real-s1-roundtrip",
                )
                claim = ledger.claim_delivery("chadol", "worker", 60)
                self.assertIsNotNone(claim)
                ledger.advance_delivery(claim, "accepted")
                accepted = self.call_tool(
                    action="receipt", idempotency_key="real-s1-roundtrip"
                )
                ledger.advance_delivery(claim, "completed")
                completed = self.call_tool(
                    action="receipt", idempotency_key="real-s1-roundtrip"
                )
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=5)

        self.assertEqual(sent["delivery_status"], "pending")
        self.assertEqual(accepted["delivery_status"], "accepted")
        self.assertTrue(accepted["accepted"])
        self.assertEqual(completed["delivery_status"], "completed")
        self.assertTrue(completed["completed"])


class TestReceiptStoreConcurrency(QueueHandoffToolTestBase):
    def test_same_key_concurrent_reserve_selects_one_durable_event(self):
        from tools.queue_handoff_receipts import QueueHandoffReceiptStore

        path = Path(self.hermes_home) / "receipt-race.sqlite3"
        barrier = threading.Barrier(2)

        def reserve(marker):
            store = QueueHandoffReceiptStore(path)
            barrier.wait(timeout=5)
            return store.reserve(
                idempotency_key="queue_handoff:chami:" + "a" * 64,
                request_hash="b" * 64,
                protocol_version="conversation.v1",
                event={"event_id": marker, "created_at": marker},
                initial_status="pending",
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(reserve, ["first", "second"]))

        records = [record for record, _created in results]
        created = [_created for _record, _created in results]
        self.assertEqual(sum(created), 1)
        self.assertEqual(records[0].event, records[1].event)
        with sqlite3.connect(path) as con:
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM queue_handoff_receipts").fetchone()[0],
                1,
            )


class TestReturnContract(QueueHandoffToolTestBase):
    """T5: 반환 규약 — 성공/실패 모두 JSON str."""

    def test_returns_json_string(self):
        from tools.queue_handoff_tool import queue_handoff_tool

        self.make_repo()
        raw = asyncio.run(queue_handoff_tool({"to": "chadol", "message": "문자열 반환"}))
        self.assertIsInstance(raw, str)
        parsed = json.loads(raw)
        self.assertEqual(set(parsed) >= {"success", "target", "message_id"}, True)

    def test_error_returns_json_string(self):
        from tools.queue_handoff_tool import queue_handoff_tool

        self.make_repo()
        raw = asyncio.run(queue_handoff_tool({"to": "chami", "message": "self"}))
        self.assertIsInstance(raw, str)
        self.assertIn("error", json.loads(raw))


if __name__ == "__main__":
    unittest.main()
