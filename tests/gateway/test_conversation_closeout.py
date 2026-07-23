"""gateway.conversation_closeout — deferred terminal 등록소 단위 테스트(F4).

도구(queue_handoff_tool)가 활성 v2 인바운드 턴 안에서 terminal
(completed/failed/no_action)을 즉시 append하는 대신 여기 등록하고, 큐 어댑터의
on_processing_complete가 최종 답(reply) 발신 뒤 pop해 append한다 — §5 순서
(reply → terminal)의 기계적 보장. 프로세스-로컬이며 턴 스코프에서 소비된다.

★활성 턴 registry(SPEC-1 수리): ContextVar 스냅샷은 턴이 끝난 뒤에도 상속처
(백그라운드 delegate 워커 등)에 남는다 — 등록은 어댑터가 mark_turn_active로
살아있다고 선언한 턴에만 허용되고, mark_turn_closed가 같은 락 안에서 활성
해제+등록 소비를 원자화한다. 닫힌 턴의 등록 시도는 None(호출자는 즉시 append로
폴스루)이라 고아 등록이 구조적으로 불가능하다.

각 테스트는 고유 parent_event_id를 써서 모듈 전역 등록소를 공유해도 서로
오염되지 않는다.
"""

import unittest


def _activate(parent_event_id, *, conversation_id="conv_closeout", token="tok-1"):
    from gateway.conversation_closeout import mark_turn_active

    mark_turn_active(
        parent_event_id=parent_event_id,
        conversation_id=conversation_id,
        requester="chadol",
        delivery_token=token,
    )


class TestDeferredTerminalRegistry(unittest.TestCase):
    def test_register_then_close_consumes_once(self):
        from gateway.conversation_closeout import (
            mark_turn_closed,
            pop_deferred_terminal,
            register_deferred_terminal,
        )

        _activate("evt_closeout_consume_once", conversation_id="conv_closeout_1")
        record = register_deferred_terminal(
            conversation_id="conv_closeout_1",
            parent_event_id="evt_closeout_consume_once",
            event_type="completed",
            body="작업 완료",
        )
        self.assertIsNotNone(record)
        self.assertEqual(record.conversation_id, "conv_closeout_1")
        self.assertEqual(record.parent_event_id, "evt_closeout_consume_once")
        self.assertEqual(record.event_type, "completed")
        self.assertEqual(record.body, "작업 완료")

        popped = mark_turn_closed("evt_closeout_consume_once", "tok-1")
        self.assertEqual(popped, record)
        # 1회성 소비 — 재close/재pop은 None(다음 턴으로 새지 않는다).
        self.assertIsNone(mark_turn_closed("evt_closeout_consume_once", "tok-1"))
        self.assertIsNone(pop_deferred_terminal("evt_closeout_consume_once"))

    def test_register_without_active_turn_returns_none(self):
        """SPEC-1 핵심: 활성 턴이 아니면 등록 자체가 거부된다 — 턴 종료 후
        상속 스냅샷(백그라운드 워커)의 terminal은 여기서 None을 받아 즉시
        append 경로로 폴스루해야 하며, 고아 등록(성공 보고 동반 침묵 소실)이
        구조적으로 불가능해야 한다."""
        from gateway.conversation_closeout import (
            pop_deferred_terminal,
            register_deferred_terminal,
        )

        record = register_deferred_terminal(
            conversation_id="conv_closeout_orphan",
            parent_event_id="evt_closeout_never_active",
            event_type="completed",
            body="고아가 될 뻔한 종결",
        )
        self.assertIsNone(record)
        self.assertIsNone(pop_deferred_terminal("evt_closeout_never_active"))

    def test_register_after_close_returns_none(self):
        """턴이 닫힌 뒤의 등록 시도(레이스 꼬리)도 거부된다 — close가 락 안에서
        활성 해제+소비를 원자화하므로 '등록됐는데 아무도 안 pop'하는 창이 없다."""
        from gateway.conversation_closeout import (
            mark_turn_closed,
            register_deferred_terminal,
        )

        _activate("evt_closeout_after_close")
        self.assertIsNone(mark_turn_closed("evt_closeout_after_close", "tok-1"))
        record = register_deferred_terminal(
            conversation_id="conv_closeout",
            parent_event_id="evt_closeout_after_close",
            event_type="completed",
            body="늦은 종결",
        )
        self.assertIsNone(record)

    def test_register_conversation_mismatch_returns_none(self):
        """활성 턴이라도 등록 conversation이 그 턴의 대화와 다르면 거부 — 다른
        대화의 종결 의도가 이 턴의 완료훅으로 새는 오염을 등록 단계에서 차단."""
        from gateway.conversation_closeout import (
            mark_turn_closed,
            register_deferred_terminal,
        )

        _activate("evt_closeout_conv_mismatch", conversation_id="conv_closeout_real")
        record = register_deferred_terminal(
            conversation_id="conv_closeout_other",
            parent_event_id="evt_closeout_conv_mismatch",
            event_type="completed",
            body="다른 대화 종결",
        )
        self.assertIsNone(record)
        self.assertIsNone(mark_turn_closed("evt_closeout_conv_mismatch", "tok-1"))

    def test_close_with_stale_token_leaves_new_turn_intact(self):
        """재claim(새 delivery token) 뒤 늦게 도착한 옛 워커의 close는 새 턴의
        활성 상태·등록을 건드리지 못한다(토큰 대조)."""
        from gateway.conversation_closeout import (
            active_turn,
            mark_turn_closed,
            register_deferred_terminal,
        )

        _activate("evt_closeout_stale_close", token="tok-old")
        _activate("evt_closeout_stale_close", token="tok-new")  # 재claim
        record = register_deferred_terminal(
            conversation_id="conv_closeout",
            parent_event_id="evt_closeout_stale_close",
            event_type="completed",
            body="새 턴의 종결",
        )
        self.assertIsNotNone(record)

        self.assertIsNone(mark_turn_closed("evt_closeout_stale_close", "tok-old"))
        self.assertIsNotNone(active_turn("evt_closeout_stale_close"))
        popped = mark_turn_closed("evt_closeout_stale_close", "tok-new")
        self.assertEqual(popped, record)

    def test_reactivation_clears_previous_pending(self):
        """크래시로 소비되지 못한 이전 런의 등록은 재claim 활성화 때 클리어된다
        — 새 턴은 깨끗하게 시작하고, 종결 판단은 새 런이 다시 낸다."""
        from gateway.conversation_closeout import (
            mark_turn_closed,
            register_deferred_terminal,
        )

        _activate("evt_closeout_reactivate", token="tok-old")
        register_deferred_terminal(
            conversation_id="conv_closeout",
            parent_event_id="evt_closeout_reactivate",
            event_type="failed",
            body="이전 런의 판단",
        )
        _activate("evt_closeout_reactivate", token="tok-new")  # 재claim
        self.assertIsNone(mark_turn_closed("evt_closeout_reactivate", "tok-new"))

    def test_reregister_same_parent_latest_wins(self):
        """같은 턴에서 재등록하면 최신 등록이 이긴다(에이전트의 판단 정정 허용).
        terminal append는 어차피 훅에서 1회다."""
        from gateway.conversation_closeout import (
            mark_turn_closed,
            register_deferred_terminal,
        )

        _activate("evt_closeout_latest_wins", conversation_id="conv_closeout_2")
        register_deferred_terminal(
            conversation_id="conv_closeout_2",
            parent_event_id="evt_closeout_latest_wins",
            event_type="completed",
            body="처음 판단",
        )
        register_deferred_terminal(
            conversation_id="conv_closeout_2",
            parent_event_id="evt_closeout_latest_wins",
            event_type="failed",
            body="정정: 실패로 종결",
        )

        popped = mark_turn_closed("evt_closeout_latest_wins", "tok-1")
        self.assertEqual(popped.event_type, "failed")
        self.assertEqual(popped.body, "정정: 실패로 종결")

    def test_active_turn_exposes_requester_for_target_check(self):
        """도구가 to(대상)를 인바운드 요청자와 대조할 수 있게 활성 턴 조회가
        requester를 노출한다(SPEC-3: 오대상 종결 방어)."""
        from gateway.conversation_closeout import active_turn, mark_turn_closed

        _activate("evt_closeout_requester", conversation_id="conv_closeout_5")
        active = active_turn("evt_closeout_requester")
        self.assertIsNotNone(active)
        self.assertEqual(active.requester, "chadol")
        self.assertEqual(active.conversation_id, "conv_closeout_5")
        mark_turn_closed("evt_closeout_requester", "tok-1")
        self.assertIsNone(active_turn("evt_closeout_requester"))

    def test_pop_unknown_returns_none(self):
        from gateway.conversation_closeout import pop_deferred_terminal

        self.assertIsNone(pop_deferred_terminal("evt_closeout_never_registered"))
        self.assertIsNone(pop_deferred_terminal(""))

    def test_non_terminal_event_type_rejected(self):
        """등록소 어휘는 terminal 3종뿐 — reply/ack/request 등이 등록되면
        어댑터가 훅에서 비-terminal event를 종결처럼 append하게 된다."""
        from gateway.conversation_closeout import (
            mark_turn_closed,
            register_deferred_terminal,
        )

        _activate("evt_closeout_bad_type", conversation_id="conv_closeout_3")
        try:
            for bad in ("reply", "ack", "request", "progress", "question", ""):
                with self.subTest(event_type=bad):
                    with self.assertRaises(ValueError):
                        register_deferred_terminal(
                            conversation_id="conv_closeout_3",
                            parent_event_id="evt_closeout_bad_type",
                            event_type=bad,
                            body="x",
                        )
        finally:
            self.assertIsNone(mark_turn_closed("evt_closeout_bad_type", "tok-1"))

    def test_missing_identity_rejected(self):
        from gateway.conversation_closeout import register_deferred_terminal

        with self.assertRaises(ValueError):
            register_deferred_terminal(
                conversation_id="",
                parent_event_id="evt_closeout_no_conv",
                event_type="completed",
                body="x",
            )
        with self.assertRaises(ValueError):
            register_deferred_terminal(
                conversation_id="conv_closeout_4",
                parent_event_id="",
                event_type="completed",
                body="x",
            )


if __name__ == "__main__":
    unittest.main()
