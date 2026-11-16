"""Tests for the submitted question bank and complete quiz sessions."""
from __future__ import annotations

import io
import select
import signal
import socket
import subprocess
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from question_bank import load_and_validate_bank
from tests.replacement_bank_scenario import RecordingClient, _until


PROJECT = Path(__file__).resolve().parents[1]
BANK_PATH = PROJECT / "questions.json"


class SubmittedBankRound2Tests(unittest.TestCase):
    def test_submitted_bank_has_four_distinct_quizzes_and_35_valid_questions(self) -> None:
        bank = load_and_validate_bank(BANK_PATH)
        self.assertEqual(["networking-basics", "transport-layer", "application-layer",
                          "socket-programming"], [quiz.quiz_id for quiz in bank.quizzes])
        self.assertEqual([5, 8, 10, 12], [len(quiz.questions) for quiz in bank.quizzes])
        self.assertEqual([850, 1450, 2050, 2050],
                         [sum(question.points for question in quiz.questions)
                          for quiz in bank.quizzes])
        questions = [question for quiz in bank.quizzes for question in quiz.questions]
        self.assertEqual(35, len({question.question_id for question in questions}))
        self.assertEqual({10, 15, 20, 30}, {question.time_limit for question in questions})
        self.assertEqual({100, 150, 200, 250, 300},
                         {question.points for question in questions})
        self.assertEqual(["NB01", "NB02", "NB03", "NB04", "NB05"],
                         [question.question_id for question in bank.quizzes[0].questions])

    def test_ten_and_twelve_question_quizzes_complete_over_real_tcp(self) -> None:
        bank = load_and_validate_bank(BANK_PATH)
        for quiz in bank.quizzes[2:]:
            with self.subTest(quiz_id=quiz.quiz_id):
                self._run_complete_quiz(quiz, bank)

    def _run_complete_quiz(self, quiz, bank) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = subprocess.Popen(
            [sys.executable, str(PROJECT / "server.py"), "--host", "127.0.0.1",
             "--port", str(port), "--questions", str(BANK_PATH)],
            cwd=PROJECT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )
        clients: list[RecordingClient] = []
        try:
            readable, _, _ = select.select([server.stdout], [], [], 5)
            self.assertTrue(readable, "production server did not become ready")
            self.assertEqual("SERVER_READY", server.stdout.readline().strip())
            clients = [RecordingClient("127.0.0.1", port,
                                       f"round2-{quiz.quiz_id}-{index}",
                                       f"Round2 {index}") for index in range(2)]
            with redirect_stdout(io.StringIO()):
                for client in clients:
                    client.connect()
                catalogue = clients[0].request("LIST_QUIZZES_REQUEST", {})["payload"]["quizzes"]
                self.assertEqual([q.quiz_id for q in bank.quizzes],
                                 [row["quiz_id"] for row in catalogue])
                self.assertEqual([len(q.questions) for q in bank.quizzes],
                                 [row["question_count"] for row in catalogue])
                self.assertTrue(all(set(row) == {"quiz_id", "title", "description",
                                                     "question_count"} for row in catalogue))
                room = clients[0].request("CREATE_ROOM_REQUEST", {"quiz_id": quiz.quiz_id})
                room_code = room["payload"]["room_code"]
                clients[1].request("JOIN_ROOM_REQUEST", {"room_code": room_code})
                for client in clients:
                    client.request("READY_REQUEST", {"ready": True})
                clients[0].request("START_QUIZ_REQUEST", {})
                running_total = 0
                for question in quiz.questions:
                    for client in clients:
                        event = _until(client, "QUESTION_EVENT", question.question_id)
                        payload = event["payload"]
                        self.assertEqual(question.text, payload["text"])
                        self.assertEqual(question.time_limit, payload["time_limit"])
                        self.assertEqual(question.points, payload["points"])
                        self.assertNotIn("answer", payload)
                        self.assertNotIn("correct_answer", payload)
                    wrong = next(letter for letter in "ABCD" if letter != question.answer)
                    for client, answer in zip(clients, (question.answer, wrong)):
                        response = client.request("ANSWER_REQUEST", {
                            "question_id": question.question_id, "answer": answer})
                        self.assertEqual("ANSWER_RESPONSE", response["type"])
                    for index, client in enumerate(clients):
                        settled = _until(client, "QUESTION_RESULT_EVENT", question.question_id)
                        self.assertEqual(question.answer, settled["payload"]["correct_answer"])
                        self.assertEqual(question.points if index == 0 else 0,
                                         settled["payload"]["points_awarded"])
                    running_total += question.points
                    board = clients[0].request("LEADERBOARD_REQUEST", {})["payload"]["leaderboard"]
                    self.assertEqual([running_total, 0], [row["score"] for row in board])
                for client in clients:
                    finished = _until(client, "QUIZ_FINISHED_EVENT")
                    self.assertEqual([running_total, 0],
                                     [row["score"] for row in finished["payload"]["final_leaderboard"]])
                    self.assertEqual([1, 2],
                                     [row["rank"] for row in finished["payload"]["final_leaderboard"]])
                    self.assertEqual([finished["payload"]["winner"]], finished["payload"]["winners"])
        finally:
            for client in clients:
                client.close()
            if server.poll() is None:
                server.send_signal(signal.SIGINT)
            _, error = server.communicate(timeout=5)
            self.assertEqual(0, server.returncode, error)
            with socket.socket() as released:
                released.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                released.bind(("127.0.0.1", port))
