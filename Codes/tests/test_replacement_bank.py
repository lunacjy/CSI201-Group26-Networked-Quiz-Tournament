from __future__ import annotations

import json
from pathlib import Path
import tempfile
import threading
import unittest

from message_router import MessageRouter
from question_bank import validate_bank
from quiz_engine import QuizEngine
from room_manager import RoomManager
from server import QuizServer
from tests.bank_fixtures import bank_with_sizes
from tests.replacement_bank_scenario import RecordingClient, run_bank_slice


class ReplacementBankTests(unittest.TestCase):
    def test_generated_different_room_scores_and_real_timeout_are_isolated(self) -> None:
        data = bank_with_sizes([5, 5], times=[5, 15], points=[10, 100])
        data["quizzes"][0]["questions"][0].update(time_limit=15, points=500)
        bank = validate_bank(data)
        server = QuizServer("127.0.0.1", 0, bank)
        engine = QuizEngine(bank, server.event_sink)
        rooms = RoomManager(engine)
        engine.bind_room_manager(rooms)
        server.router = MessageRouter(rooms, engine)
        ready = threading.Event()
        thread = threading.Thread(target=server.serve_forever, args=(ready,), daemon=True)
        thread.start()
        self.assertTrue(ready.wait(3))
        clients = [RecordingClient("127.0.0.1", server.bound_port, f"isolate-{i}", f"Player {i}")
                   for i in range(4)]
        try:
            for client in clients:
                client.connect()
            codes = []
            for base, quiz_id in ((0, "quiz-00"), (2, "quiz-01")):
                code = clients[base].request("CREATE_ROOM_REQUEST", {"quiz_id": quiz_id})["payload"]["room_code"]
                codes.append(code)
                clients[base + 1].request("JOIN_ROOM_REQUEST", {"room_code": code})
                for client in clients[base:base + 2]:
                    client.request("READY_REQUEST", {"ready": True})
                clients[base].request("START_QUIZ_REQUEST", {})
            self.assertNotEqual(*codes)
            clients[0].request("ANSWER_REQUEST", {"question_id": "Q00_00", "answer": "A"})
            clients[1].request("ANSWER_REQUEST", {"question_id": "Q00_00", "answer": "B"})

            def result_for(client: RecordingClient, question_id: str, timeout: float) -> dict:
                import time
                end = time.monotonic() + timeout
                while time.monotonic() < end:
                    event = client.events.get(timeout=max(0.01, end - time.monotonic()))
                    if event["type"] == "QUESTION_RESULT_EVENT" and event["payload"]["question_id"] == question_id:
                        return event
                self.fail(f"no result for {question_id}")

            a = result_for(clients[0], "Q00_00", 3)
            b = result_for(clients[2], "Q01_00", 8)
            self.assertEqual([500, 0], [row["score"] for row in a["payload"]["leaderboard"]])
            self.assertEqual([0, 0], [row["score"] for row in b["payload"]["leaderboard"]])
            self.assertIsNone(b["payload"]["your_answer"])
            self.assertEqual([500, 0], [row["score"] for row in
                         clients[0].request("LEADERBOARD_REQUEST", {})["payload"]["leaderboard"]])
            self.assertEqual([0, 0], [row["score"] for row in
                         clients[2].request("LEADERBOARD_REQUEST", {})["payload"]["leaderboard"]])
            self.assertEqual(15, bank.quizzes[0].questions[0].time_limit)
            self.assertEqual(5, bank.quizzes[1].questions[0].time_limit)
        finally:
            for client in clients:
                client.close()
            server.stop()
            rooms.shutdown()
            thread.join(timeout=4)
            self.assertFalse(thread.is_alive())

    def test_submitted_and_generated_mixed_bank_via_production_processes(self) -> None:
        project = Path(__file__).resolve().parents[1]
        submitted = run_bank_slice(project / "questions.json")
        self.assertEqual("NB01", submitted["first_question"])
        self.assertEqual(4, submitted["quizzes"])

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "mixed.json"
            path.write_text(json.dumps(bank_with_sizes([5, 10], times=[5, 15, 60],
                                                       points=[1, 100, 1000]), ensure_ascii=False),
                            encoding="utf-8")
            generated = run_bank_slice(path)
        self.assertEqual(2, generated["quizzes"])
        self.assertEqual([1, 0], generated["scores"])

    def test_new_submitted_quizzes_select_and_settle_first_question(self) -> None:
        project = Path(__file__).resolve().parents[1]
        expected = (("application-layer", 10, "AP01"),
                    ("socket-programming", 12, "SK01"))
        for quiz_id, count, first_id in expected:
            with self.subTest(quiz_id=quiz_id):
                result = run_bank_slice(project / "questions.json", quiz_id)
                self.assertEqual(4, result["quizzes"])
                self.assertEqual(quiz_id, result["selected_quiz"])
                self.assertEqual(count, result["questions_in_quiz"])
                self.assertEqual(first_id, result["first_question"])


if __name__ == "__main__":
    unittest.main()
