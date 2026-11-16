from __future__ import annotations

import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from message_router import PendingEvent, PublishResult, RequestContext, ServiceResult
from quiz_engine import QuizEngine
from room_manager import RoomManager
from server import QuizServer, main as server_main
from tests.helpers import valid_bank_dict
from question_bank import validate_bank


class RecordingSink:
    def __init__(self):
        self.lock = threading.Lock()
        self.events: list[PendingEvent] = []

    def publish(self, event: PendingEvent) -> PublishResult:
        with self.lock:
            self.events.append(event)
        return PublishResult(True, len(event.recipients))


def context(number: int) -> RequestContext:
    return RequestContext(f"session-{number}", f"user-{number}", f"Player {number}", object(), f"req-{number}")


def create_ready_room(rooms: RoomManager, host: RequestContext, guest: RequestContext) -> str:
    created = rooms.create_room(host, {"quiz_id": "quiz-1"})
    assert created.response_payload is not None
    code = created.response_payload["room_code"]
    rooms.join_room(guest, {"room_code": code})
    rooms.set_ready(host, {"ready": True})
    rooms.set_ready(guest, {"ready": True})
    return code


class QuizLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sink = RecordingSink()
        self.engine = QuizEngine(validate_bank(valid_bank_dict()), self.sink)
        self.rooms = RoomManager(self.engine)
        self.engine.bind_room_manager(self.rooms)

    def tearDown(self) -> None:
        self.rooms.shutdown()

    def test_twenty_finished_rooms_keep_scores_until_last_departure_then_release_state(self) -> None:
        for room_number in range(20):
            host, guest = context(room_number * 2), context(room_number * 2 + 1)
            code = create_ready_room(self.rooms, host, guest)
            started = self.rooms.start_room(host, {})
            self.assertIsInstance(started, ServiceResult)
            for question_no in range(5):
                question_id = f"Q{question_no}"
                for player in (host, guest):
                    answer = self.engine.submit_answer(
                        player, {"question_id": question_id, "answer": "A"}
                    )
                    self.assertEqual("ANSWER_RESPONSE", answer.response_type)
            leaderboard = self.engine.get_leaderboard(host, {})
            self.assertEqual([500, 500], [row["score"] for row in leaderboard.response_payload["leaderboard"]])
            self.assertIn(code, self.engine._runs, "finished run must remain queryable before departure")
            self.rooms.leave_room(host, {})
            self.assertIn(code, self.engine._runs, "one remaining participant still owns the final board")
            self.rooms.leave_room(guest, {})
            self.assertNotIn(code, self.engine._runs)
            self.assertNotIn(host.session_id, self.engine._session_room)
            self.assertNotIn(guest.session_id, self.engine._session_room)
            self.assertEqual(0, self.engine.active_run_count)
            self.assertEqual(0, self.engine.active_session_mapping_count)
            self.assertEqual(0, self.engine.active_timer_count)
            self.assertEqual(0, len(self.engine._timer_runs))
        self.assertEqual({}, self.rooms._rooms)
        self.assertEqual({}, self.rooms._membership)

    def test_room_errors_host_transfer_capacity_and_playing_join_are_deterministic(self) -> None:
        host, guest, newcomer = context(300), context(301), context(302)
        self.assertEqual("QUIZ_NOT_FOUND", self.rooms.create_room(host, {"quiz_id": "missing"}).error_code)
        self.assertEqual("ROOM_NOT_FOUND", self.rooms.join_room(guest, {"room_code": "missing"}).error_code)
        code = create_ready_room(self.rooms, host, guest)
        self.assertEqual("ALREADY_IN_ROOM", self.rooms.create_room(host, {"quiz_id": "quiz-1"}).error_code)
        self.rooms.leave_room(host, {})
        self.assertEqual("NOT_IN_ROOM", self.rooms.start_room(host, {}).error_code)
        guest_players = self.rooms.list_players(guest, {}).response_payload["players"]
        self.assertTrue(guest_players[0]["is_host"])
        self.assertEqual("PLAYERS_NOT_READY", self.rooms.start_room(guest, {}).error_code)
        self.rooms.join_room(newcomer, {"room_code": code})
        self.rooms.set_ready(newcomer, {"ready": True})
        self.assertIsNone(self.rooms.start_room(guest, {}).error_code)
        self.assertEqual("INVALID_STATE", self.rooms.join_room(context(303), {"room_code": code}).error_code)
        invalid = self.engine.submit_answer(guest, {"question_id": "Q0", "answer": "AB"})
        self.assertEqual("INVALID_ANSWER", invalid.error_code)
        self.assertEqual({}, self.engine._runs[code].answers)
        self.rooms.shutdown()

        host = context(310)
        code = self.rooms.create_room(host, {"quiz_id": "quiz-1"}).response_payload["room_code"]
        for number in range(311, 320):
            result = self.rooms.join_room(context(number), {"room_code": code})
            self.assertIsNone(result.error_code)
        self.assertEqual(10, self.rooms.list_rooms(context(320), {})
                         .response_payload["rooms"][0]["player_count"])
        self.assertEqual("ROOM_FULL", self.rooms.join_room(context(320), {"room_code": code}).error_code)

    def test_playing_room_all_disconnects_or_shutdown_cancel_run_and_stale_timer(self) -> None:
        host, guest = context(100), context(101)
        code = create_ready_room(self.rooms, host, guest)
        self.rooms.start_room(host, {})
        run = self.engine._runs[code]
        self.rooms.on_disconnect(host.session_id)
        self.assertIn(code, self.engine._runs)
        self.rooms.on_disconnect(guest.session_id)
        self.assertNotIn(code, self.engine._runs)
        self.assertEqual(0, self.engine.active_session_mapping_count)
        events_before_stale_timeout = len(self.sink.events)
        self.engine._timeout(code, 0, run)
        self.assertEqual(events_before_stale_timeout, len(self.sink.events))
        self.assertEqual(0, self.engine.active_timer_count)
        self.assertEqual(0, len(self.engine._timer_runs))

        host2, guest2 = context(102), context(103)
        code2 = create_ready_room(self.rooms, host2, guest2)
        self.rooms.start_room(host2, {})
        self.rooms.shutdown()
        self.assertNotIn(code2, self.engine._runs)
        self.assertEqual(0, self.engine.active_run_count)
        self.assertEqual(0, self.engine.active_session_mapping_count)
        self.assertEqual(0, self.engine.active_timer_count)
        events_before_stale_timeout = len(self.sink.events)
        self.engine._timeout(code2, 0)
        self.assertEqual(events_before_stale_timeout, len(self.sink.events))

    def test_normal_server_main_return_shuts_down_active_rooms_and_timers(self) -> None:
        project = Path(__file__).resolve().parents[1]
        instances: list[tuple[RoomManager, QuizEngine]] = []

        def stop_after_start(server: QuizServer) -> None:
            rooms = server.router.room_service
            engine = server.router.quiz_service
            host, guest = context(200), context(201)
            created = rooms.create_room(host, {"quiz_id": "networking-basics"})
            code = created.response_payload["room_code"]
            rooms.join_room(guest, {"room_code": code})
            rooms.set_ready(host, {"ready": True})
            rooms.set_ready(guest, {"ready": True})
            rooms.start_room(host, {})
            instances.append((rooms, engine))

        with patch.object(QuizServer, "serve_forever", stop_after_start):
            exit_code = server_main(["--host", "127.0.0.1", "--port", "43129",
                                     "--questions", str(project / "questions.json")])
        self.assertEqual(0, exit_code)
        self.assertEqual(1, len(instances))
        rooms, engine = instances[0]
        self.assertEqual({}, rooms._rooms)
        self.assertEqual({}, rooms._membership)
        self.assertEqual(0, engine.active_run_count)
        self.assertEqual(0, engine.active_session_mapping_count)
        self.assertEqual(0, engine.active_timer_count)
        self.assertEqual(0, len(engine._timer_runs))

    def test_timer_wrapper_logs_unexpected_callback_error_without_thread_crash(self) -> None:
        class BrokenEngine(QuizEngine):
            def _timeout(self, room_code: str, index: int, expected_run=None) -> None:
                raise RuntimeError("injected callback failure")

        engine = BrokenEngine(validate_bank(valid_bank_dict()), self.sink)
        players = (SimpleNamespace(session_id="bh", user_id="bh", name="Broken Host"),
                   SimpleNamespace(session_id="bg", user_id="bg", name="Broken Guest"))
        engine.start_room("BROKEN", "quiz-1", players)
        run = engine._runs["BROKEN"]
        uncaught: list[BaseException] = []
        thread = threading.Thread(target=engine._timer_callback,
                                  args=("BROKEN", 0, run), daemon=True)
        with patch.object(threading, "excepthook", lambda args: uncaught.append(args.exc_value)):
            with self.assertLogs("csi201.quiz", level="ERROR"):
                thread.start()
                thread.join(timeout=1)
        self.assertFalse(thread.is_alive())
        self.assertEqual([], uncaught)
        engine.close_room("BROKEN")
        self.assertEqual(0, engine.active_timer_count)


if __name__ == "__main__":
    unittest.main()
