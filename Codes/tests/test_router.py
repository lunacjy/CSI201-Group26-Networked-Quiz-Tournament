from __future__ import annotations

import unittest

from message_router import MessageRouter, PendingEvent, RequestContext, ServiceResult


CTX = RequestContext("s1", "u1", "Alice", "conn", "r1")


class FakeQuiz:
    def list_quizzes(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        return ServiceResult.success(
            "LIST_QUIZZES_RESPONSE",
            {"quizzes": [{"quiz_id": "q1", "title": "Quiz", "description": "", "question_count": 5}]},
        )


class LeakyQuiz:
    def list_quizzes(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        return ServiceResult.success("LIST_QUIZZES_RESPONSE", {"answer": "A"})


class FakeRoom:
    def list_rooms(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        event = PendingEvent("PLAYER_STATUS_EVENT", (ctx.session_id,), {"room_code": "R1"})
        return ServiceResult.success("LIST_ROOMS_RESPONSE", {"rooms": []}, (event,))


class LeakyRoom:
    def list_rooms(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        event = PendingEvent(
            "QUESTION_EVENT", (ctx.session_id,), {"question_id": "q1", "answer": "A"}
        )
        return ServiceResult.success("LIST_ROOMS_RESPONSE", {"rooms": []}, (event,))


class FailingDisconnectRoom:
    def on_disconnect(self, session_id: str) -> tuple[PendingEvent, ...]:
        raise RuntimeError("test failure")


class SharedPrivateResultRoom:
    def list_rooms(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        event = PendingEvent(
            "QUESTION_RESULT_EVENT", ("s1", "s2"), {"question_id": "q1", "your_answer": "A"}
        )
        return ServiceResult.success("LIST_ROOMS_RESPONSE", {"rooms": []}, (event,))


class RouterTests(unittest.TestCase):
    def test_missing_service_is_explicit(self) -> None:
        result = MessageRouter().dispatch("LIST_ROOMS_REQUEST", CTX, {})
        self.assertEqual("INTEGRATION_UNAVAILABLE", result.error_code)

    def test_unknown_message(self) -> None:
        result = MessageRouter().dispatch("DO_THING_REQUEST", CTX, {})
        self.assertEqual("UNSUPPORTED_MESSAGE", result.error_code)

    def test_success_and_events(self) -> None:
        result = MessageRouter(room_service=FakeRoom()).dispatch("LIST_ROOMS_REQUEST", CTX, {})
        self.assertEqual("LIST_ROOMS_RESPONSE", result.response_type)
        self.assertEqual(("s1",), result.events[0].recipients)

    def test_catalogue_answer_leak_is_blocked(self) -> None:
        result = MessageRouter(quiz_service=LeakyQuiz()).dispatch("LIST_QUIZZES_REQUEST", CTX, {})
        self.assertEqual("INTERNAL_SERVICE_ERROR", result.error_code)

    def test_valid_catalogue(self) -> None:
        result = MessageRouter(quiz_service=FakeQuiz()).dispatch("LIST_QUIZZES_REQUEST", CTX, {})
        self.assertEqual("LIST_QUIZZES_RESPONSE", result.response_type)

    def test_open_question_event_answer_leak_is_blocked(self) -> None:
        result = MessageRouter(room_service=LeakyRoom()).dispatch("LIST_ROOMS_REQUEST", CTX, {})
        self.assertEqual("INTERNAL_SERVICE_ERROR", result.error_code)

    def test_shared_private_result_event_is_blocked(self) -> None:
        result = MessageRouter(room_service=SharedPrivateResultRoom()).dispatch(
            "LIST_ROOMS_REQUEST", CTX, {}
        )
        self.assertEqual("INTERNAL_SERVICE_ERROR", result.error_code)

    def test_disconnect_callback_failure_is_diagnostic_and_safe(self) -> None:
        router = MessageRouter(room_service=FailingDisconnectRoom())
        with self.assertLogs("csi201.router", "WARNING") as captured:
            self.assertEqual((), router.disconnect_events("s1"))
        self.assertEqual(
            ["WARNING:csi201.router:disconnect callback failed safely"], captured.output
        )


if __name__ == "__main__":
    unittest.main()
