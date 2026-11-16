"""Production server-process and TCP-client slice for a supplied question bank."""
from __future__ import annotations

import io
import queue
import select
import signal
import socket
import subprocess
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path

from client import Client
from question_bank import load_and_validate_bank


class RecordingClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.events: queue.Queue[dict] = queue.Queue()

    def _display(self, message: dict) -> None:
        if message["type"].endswith("_EVENT"):
            self.events.put(message)
        super()._display(message)


def _until(client: RecordingClient, kind: str, question_id: str | None = None) -> dict:
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        event = client.events.get(timeout=max(0.01, deadline - time.monotonic()))
        if event["type"] == kind and (question_id is None or
                                      event["payload"].get("question_id") == question_id):
            return event
    raise AssertionError(f"missing {kind} for {client.user_id}")


def run_bank_slice(bank_path: str | Path, quiz_id: str | None = None) -> dict:
    """Open a real room, settle its first question, then cleanly stop the server."""
    bank_path = Path(bank_path).resolve()
    bank = load_and_validate_bank(bank_path)
    project = Path(__file__).resolve().parents[1]
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = subprocess.Popen(
        [sys.executable, str(project / "server.py"), "--host", "127.0.0.1",
         "--port", str(port), "--questions", str(bank_path)],
        cwd=project, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
    )
    clients: list[RecordingClient] = []
    try:
        readable, _, _ = select.select([server.stdout], [], [], 5)
        if not readable or server.stdout.readline().strip() != "SERVER_READY":
            raise AssertionError("production server did not print SERVER_READY")
        clients = [RecordingClient("127.0.0.1", port, f"slice-{i}", f"Slice {i}")
                   for i in range(2)]
        with redirect_stdout(io.StringIO()):
            for client in clients:
                client.connect()
            catalogue = clients[0].request("LIST_QUIZZES_REQUEST", {})["payload"]["quizzes"]
            assert [row["quiz_id"] for row in catalogue] == [q.quiz_id for q in bank.quizzes]
            assert [row["question_count"] for row in catalogue] == [len(q.questions) for q in bank.quizzes]
            assert all(set(row) == {"quiz_id", "title", "description", "question_count"}
                       for row in catalogue)
            quiz = next((entry for entry in bank.quizzes if entry.quiz_id == quiz_id), None) if quiz_id else bank.quizzes[0]
            assert quiz is not None
            room = clients[0].request("CREATE_ROOM_REQUEST", {"quiz_id": quiz.quiz_id})["payload"]["room_code"]
            clients[1].request("JOIN_ROOM_REQUEST", {"room_code": room})
            for client in clients:
                client.request("READY_REQUEST", {"ready": True})
            clients[0].request("START_QUIZ_REQUEST", {})
            first = quiz.questions[0]
            for client in clients:
                question_event = _until(client, "QUESTION_EVENT", first.question_id)
                assert question_event["payload"]["text"] == first.text
                assert question_event["payload"]["time_limit"] == first.time_limit
                assert question_event["payload"]["points"] == first.points
                assert "answer" not in question_event["payload"]
                assert "correct_answer" not in question_event["payload"]
            wrong = next(letter for letter in "ABCD" if letter != first.answer)
            clients[0].request("ANSWER_REQUEST", {"question_id": first.question_id,
                                                  "answer": first.answer})
            clients[1].request("ANSWER_REQUEST", {"question_id": first.question_id,
                                                  "answer": wrong})
            results = [_until(client, "QUESTION_RESULT_EVENT", first.question_id)
                       for client in clients]
            assert [e["payload"]["points_awarded"] for e in results] == [first.points, 0]
            assert all(e["payload"]["correct_answer"] == first.answer for e in results)
            board = clients[0].request("LEADERBOARD_REQUEST", {})["payload"]["leaderboard"]
            assert [row["score"] for row in board] == [first.points, 0]
            assert [row["rank"] for row in board] == [1, 2]
        return {"quizzes": len(bank.quizzes), "selected_quiz": quiz.quiz_id,
                "questions_in_quiz": len(quiz.questions), "first_question": first.question_id,
                "points": first.points, "scores": [row["score"] for row in board]}
    finally:
        for client in clients:
            client.close()
        if server.poll() is None:
            server.send_signal(signal.SIGINT)
        _, error = server.communicate(timeout=5)
        if server.returncode != 0:
            raise AssertionError(f"production server exited {server.returncode}: {error}")
        with socket.socket() as released:
            released.bind(("127.0.0.1", port))
