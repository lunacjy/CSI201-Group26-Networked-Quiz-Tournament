from __future__ import annotations

import time
import unittest
import io
import json
import queue
import socket
import threading
from pathlib import Path
from contextlib import redirect_stdout
from unittest.mock import patch

from client import Client
import client as client_module
from message_router import MessageRouter
from quiz_engine import QuizEngine
from room_manager import RoomManager
from tests.helpers import RunningServer
from tests.helpers import valid_bank_dict
from question_bank import validate_bank
import server as server_module
from server import QuizServer


class RecordingClient(Client):
    def __init__(self, *args: object, **kwargs: object):
        super().__init__(*args, **kwargs)
        self.events: queue.Queue[dict] = queue.Queue()

    def _display(self, msg: dict) -> None:
        if msg["type"].endswith("_EVENT"):
            self.events.put(msg)
        super()._display(msg)


class FullSystemTests(unittest.TestCase):
    def test_client_cli_launch_args_login_and_quit(self) -> None:
        bank = validate_bank(valid_bank_dict())
        with redirect_stdout(io.StringIO()), RunningServer() as running:
            engine = QuizEngine(bank, running.server.event_sink)
            rooms = RoomManager(engine)
            engine.bind_room_manager(rooms)
            running.server.router = MessageRouter(rooms, engine)
            try:
                with patch("builtins.input", side_effect=["/quizzes", "/quit"]):
                    code = client_module.main(["--server", "127.0.0.1", "--port", str(running.server.bound_port),
                                               "--id", "cli-smoke", "--name", "CLI Smoke"])
                self.assertEqual(0, code)
            finally:
                rooms.shutdown()

    def test_ten_production_clients_two_rooms_and_complete_quizzes(self) -> None:
        with redirect_stdout(io.StringIO()):
            bank = validate_bank(valid_bank_dict())
            with RunningServer() as running:
                engine = QuizEngine(bank, running.server.event_sink)
                rooms = RoomManager(engine)
                engine.bind_room_manager(rooms)
                running.server.router = MessageRouter(rooms, engine)
                clients = [RecordingClient("127.0.0.1", running.server.bound_port, f"u{i}", f"Player {i}") for i in range(10)]
                try:
                    for client in clients:
                        client.connect()
                    catalogue = clients[0].request("LIST_QUIZZES_REQUEST", {})
                    self.assertEqual(1, len(catalogue["payload"]["quizzes"]))
                    for base, quiz_id in ((0, "quiz-1"), (5, "quiz-1")):
                        host = clients[base]
                        created = host.request("CREATE_ROOM_REQUEST", {"quiz_id": quiz_id})
                        code = created["payload"]["room_code"]
                        self.assertEqual(code, host.request("LIST_ROOMS_REQUEST", {})["payload"]["rooms"][0]["room_code"])
                        for client in clients[base + 1:base + 5]:
                            client.request("JOIN_ROOM_REQUEST", {"room_code": code})
                        self.assertEqual(5, len(host.request("PLAYER_LIST_REQUEST", {})["payload"]["players"]))
                        self.assertEqual("ERROR_RESPONSE", host.request("START_QUIZ_REQUEST", {})["type"])
                        self.assertEqual("ERROR_RESPONSE", clients[base + 1].request("START_QUIZ_REQUEST", {})["type"])
                        for client in clients[base:base + 5]:
                            client.request("READY_REQUEST", {"ready": True})
                        host.request("START_QUIZ_REQUEST", {})
                    for question_no in range(5):
                        expected_qid = f"Q{question_no}"
                        deadline = time.monotonic() + 3
                        while time.monotonic() < deadline and any(
                            getattr(c, "current_question_id", None) != expected_qid for c in clients
                        ):
                            time.sleep(0.01)
                        qids = [getattr(c, "current_question_id", None) for c in clients]
                        self.assertEqual([expected_qid] * 10, qids)
                        if question_no == 0:
                            bad_question = clients[0].request("ANSWER_REQUEST", {"question_id": "wrong", "answer": "A"})
                            self.assertEqual("ERROR_RESPONSE", bad_question["type"])
                            first = clients[0].request("ANSWER_REQUEST", {"question_id": qids[0], "answer": "A"})
                            self.assertEqual("ANSWER_RESPONSE", first["type"])
                            duplicate = clients[0].request("ANSWER_REQUEST", {"question_id": qids[0], "answer": "A"})
                            self.assertEqual("ERROR_RESPONSE", duplicate["type"])
                            clients[1].request("ANSWER_REQUEST", {"question_id": qids[1], "answer": "A"})
                            for client, qid in zip(clients[2:], qids[2:]):
                                client.request("ANSWER_REQUEST", {"question_id": qid, "answer": "A"})
                        else:
                            for client, qid in zip(clients, qids):
                                client.request("ANSWER_REQUEST", {"question_id": qid, "answer": "A"})
                        if question_no == 0:
                            for client in clients:
                                deadline = time.monotonic() + 3
                                while time.monotonic() < deadline:
                                    event = client.events.get(timeout=max(0.01, deadline - time.monotonic()))
                                    if event["type"] == "QUESTION_RESULT_EVENT" and event["payload"]["question_id"] == "Q0":
                                        scores = [row["score"] for row in event["payload"]["leaderboard"]]
                                        self.assertEqual([100] * 5, scores)
                                        break
                                else:
                                    self.fail("timed out waiting for the personalised Q0 result")
                    self.assertEqual(2, len(engine._runs))
                    self.assertTrue(all(run.closed for run in engine._runs.values()))
                    self.assertTrue(all(set(run.scores.values()) == {500} for run in engine._runs.values()))
                    self.assertEqual("LEAVE_ROOM_RESPONSE", clients[0].request("LEAVE_ROOM_REQUEST", {})["type"])
                finally:
                    for client in clients:
                        client.close()
                    rooms.shutdown()

    def test_official_server_cli_composition_runs_complete_quiz(self) -> None:
        project = Path(__file__).resolve().parents[1]
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        servers: list[QuizServer] = []
        original_serve = QuizServer.serve_forever

        def observe_server(instance: QuizServer, ready_event: threading.Event | None = None) -> None:
            servers.append(instance)
            original_serve(instance, ready_event)

        output = io.StringIO()
        runner = threading.Thread(
            target=server_module.main,
            args=(["--host", "127.0.0.1", "--port", str(port), "--questions", str(project / "questions.json")],),
            daemon=True,
        )
        clients = [Client("127.0.0.1", port, f"cli{i}", f"CLI Player {i}") for i in range(2)]
        try:
            with patch.object(QuizServer, "serve_forever", observe_server), redirect_stdout(output):
                runner.start()
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and (not servers or servers[0].bound_port is None):
                    time.sleep(0.01)
                self.assertTrue(servers and servers[0].bound_port == port, msg=output.getvalue())
                for client in clients:
                    client.connect()
                created = clients[0].request("CREATE_ROOM_REQUEST", {"quiz_id": "networking-basics"})
                code = created["payload"]["room_code"]
                clients[1].request("JOIN_ROOM_REQUEST", {"room_code": code})
                for client in clients:
                    client.request("READY_REQUEST", {"ready": True})
                clients[0].request("START_QUIZ_REQUEST", {})
                for question_id in ("NB01", "NB02", "NB03", "NB04", "NB05"):
                    deadline = time.monotonic() + 3
                    while time.monotonic() < deadline and any(
                        getattr(client, "current_question_id", None) != question_id for client in clients
                    ):
                        time.sleep(0.01)
                    self.assertTrue(all(getattr(client, "current_question_id", None) == question_id for client in clients))
                    for client in clients:
                        answer = {question.question_id: question.answer
                                  for question in servers[0].question_bank.quizzes[0].questions}[question_id]
                        client.request("ANSWER_REQUEST", {"question_id": question_id, "answer": answer})
                result = clients[0].request("LEADERBOARD_REQUEST", {})
                expected = sum(question.points for question in servers[0].question_bank.quizzes[0].questions)
                self.assertEqual([expected, expected],
                                 [row["score"] for row in result["payload"]["leaderboard"]])
        finally:
            for client in clients:
                client.close()
            if servers:
                servers[0].stop()
            if runner.is_alive():
                runner.join(timeout=5)
            self.assertFalse(runner.is_alive(), "server.main composition root did not shut down")

    def test_real_timer_closes_question_without_any_answers(self) -> None:
        bank = validate_bank(valid_bank_dict())
        with redirect_stdout(io.StringIO()), RunningServer() as running:
            engine = QuizEngine(bank, running.server.event_sink)
            rooms = RoomManager(engine)
            engine.bind_room_manager(rooms)
            running.server.router = MessageRouter(rooms, engine)
            clients = [RecordingClient("127.0.0.1", running.server.bound_port, f"t{i}", f"Timer {i}") for i in range(2)]
            try:
                for client in clients:
                    client.connect()
                created = clients[0].request("CREATE_ROOM_REQUEST", {"quiz_id": "quiz-1"})
                clients[1].request("JOIN_ROOM_REQUEST", {"room_code": created["payload"]["room_code"]})
                for client in clients:
                    client.request("READY_REQUEST", {"ready": True})
                clients[0].request("START_QUIZ_REQUEST", {})
                result_events: list[dict] = []
                for client in clients:
                    deadline = time.monotonic() + 8
                    while time.monotonic() < deadline:
                        event = client.events.get(timeout=max(0.01, deadline - time.monotonic()))
                        if event["type"] == "QUESTION_RESULT_EVENT":
                            result_events.append(event)
                            break
                self.assertEqual(2, len(result_events))
                self.assertTrue(all(event["payload"]["question_id"] == "Q0" for event in result_events))
                self.assertTrue(all(event["payload"]["your_answer"] is None for event in result_events))
                self.assertTrue(all(event["payload"]["points_awarded"] == 0 for event in result_events))
                run = next(iter(engine._runs.values()))
                self.assertEqual({0}, set(run.scores.values()))
                self.assertEqual(1, run.question_index)
            finally:
                for client in clients:
                    client.close()
                rooms.shutdown()

    def test_formal_main_ten_clients_different_quizzes_host_disconnect_and_isolated_results(self) -> None:
        project = Path(__file__).resolve().parents[1]
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        servers: list[QuizServer] = []
        original_serve = QuizServer.serve_forever

        def observe_server(instance: QuizServer, ready_event: threading.Event | None = None) -> None:
            servers.append(instance)
            original_serve(instance, ready_event)

        output = io.StringIO()
        runner = threading.Thread(
            target=server_module.main,
            args=(["--host", "127.0.0.1", "--port", str(port), "--questions", str(project / "questions.json")],),
            daemon=True,
        )
        clients = [RecordingClient("127.0.0.1", port, f"u{i}", f"Player {i}") for i in range(10)]
        seen: dict[Client, list[dict]] = {client: [] for client in clients}

        def await_event(client: RecordingClient, predicate, timeout: float = 4.0) -> dict:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                event = client.events.get(timeout=max(0.01, deadline - time.monotonic()))
                seen[client].append(event)
                if predicate(event):
                    return event
            raise AssertionError(f"timed out waiting for event for {client.user_id}")

        def await_questions(active: list[Client], expected: list[str]) -> None:
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                if all(getattr(client, "current_question_id", None) == qid
                       for client, qid in zip(active, expected)):
                    return
                time.sleep(0.005)
            self.fail(f"question events not received: expected={expected}, "
                      f"actual={[getattr(c, 'current_question_id', None) for c in active]}")

        submitted = validate_bank(json.loads((project / "questions.json").read_text()))
        selected_quizzes = {quiz.quiz_id: quiz for quiz in submitted.quizzes
                            if quiz.quiz_id in ("networking-basics", "transport-layer")}
        correct = {quiz_id: [question.answer for question in quiz.questions]
                   for quiz_id, quiz in selected_quizzes.items()}
        question_ids = {quiz_id: [question.question_id for question in quiz.questions]
                        for quiz_id, quiz in selected_quizzes.items()}
        points: dict[str, list[int]] = {}

        def selected_answer(quiz: str, player: int, index: int) -> str:
            key = correct[quiz][index]
            if quiz == "networking-basics":
                if player in (0, 1) or (player == 3 and index < 3):
                    return key
                return next(answer for answer in "ABCD" if answer != key)
            if player == 7 or (player == 5 and index in (0, 2)) or (player == 8 and index in (1, 3)):
                return key
            return next(answer for answer in "ABCD" if answer != key)

        try:
            with patch.object(QuizServer, "serve_forever", observe_server), redirect_stdout(output):
                runner.start()
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and (not servers or servers[0].bound_port is None):
                    time.sleep(0.01)
                self.assertTrue(servers and servers[0].bound_port == port, msg=output.getvalue())
                for client in clients:
                    client.connect()

                created_a = clients[0].request("CREATE_ROOM_REQUEST", {"quiz_id": "networking-basics"})
                room_a = created_a["payload"]["room_code"]
                created_b = clients[5].request("CREATE_ROOM_REQUEST", {"quiz_id": "transport-layer"})
                room_b = created_b["payload"]["room_code"]
                for client in clients[1:5]:
                    client.request("JOIN_ROOM_REQUEST", {"room_code": room_a})
                for client in clients[6:10]:
                    client.request("JOIN_ROOM_REQUEST", {"room_code": room_b})
                for client in clients:
                    client.request("READY_REQUEST", {"ready": True})
                clients[0].request("START_QUIZ_REQUEST", {})
                clients[5].request("START_QUIZ_REQUEST", {})
                quizzes_from_server = {quiz.quiz_id: quiz for quiz in servers[0].question_bank.quizzes}
                points = {quiz_id: [question.points for question in quiz.questions]
                          for quiz_id, quiz in quizzes_from_server.items()}

                for index in range(len(question_ids["transport-layer"])):
                    if index == 0:
                        active_a = clients[:5]
                    elif index >= len(question_ids["networking-basics"]):
                        active_a = []
                    else:
                        active_a = clients[1:5]
                    active_b = clients[5:10]
                    await_questions(active_a + active_b,
                                    ([question_ids["networking-basics"][index]] * len(active_a)
                                     if active_a else [])
                                    + [question_ids["transport-layer"][index]] * len(active_b))
                    for player_index in range(5 if active_a else 0):
                        client = clients[player_index]
                        if index > 0 and player_index == 0:
                            continue
                        client.request("ANSWER_REQUEST", {
                            "question_id": question_ids["networking-basics"][index],
                            "answer": selected_answer("networking-basics", player_index, index),
                        })
                    for player_index in range(5, 10):
                        clients[player_index].request("ANSWER_REQUEST", {
                            "question_id": question_ids["transport-layer"][index],
                            "answer": selected_answer("transport-layer", player_index, index),
                        })
                    if index == 0:
                        await_event(
                            clients[0],
                            lambda event: (
                                event["type"] == "QUESTION_RESULT_EVENT"
                                and event["payload"].get("question_id")
                                == question_ids["networking-basics"][0]
                            ),
                        )
                        clients[0].close()
                        moved = await_event(clients[1], lambda e: e["type"] == "PLAYER_STATUS_EVENT"
                                            and e["payload"].get("room_code") == room_a
                                            and e["payload"].get("host_id") == "u1")
                        self.assertEqual("u1", moved["payload"]["host_id"])

                expected = {}
                for user_id, quiz, player in [(f"u{i}", "networking-basics", i) for i in range(1, 5)] \
                        + [(f"u{i}", "transport-layer", i) for i in range(5, 10)]:
                    expected[user_id] = sum(
                        points[quiz][i] if selected_answer(quiz, player, i) == correct[quiz][i] else 0
                        for i in range(len(question_ids[quiz]))
                    )
                board_a = clients[1].request("LEADERBOARD_REQUEST", {})["payload"]["leaderboard"]
                board_b = clients[7].request("LEADERBOARD_REQUEST", {})["payload"]["leaderboard"]
                actual_a = {row["user_id"]: row["score"] for row in board_a}
                actual_b = {row["user_id"]: row["score"] for row in board_b}
                self.assertEqual({u: expected[u] for u in ("u1", "u2", "u3", "u4")}, actual_a)
                self.assertEqual({u: expected[u] for u in ("u5", "u6", "u7", "u8", "u9")}, actual_b)
                self.assertNotEqual(actual_a, actual_b)

                for client in clients[1:]:
                    finished = await_event(client, lambda e: e["type"] == "QUIZ_FINISHED_EVENT")
                    final = finished["payload"]
                    self.assertEqual([row for row in final["final_leaderboard"] if row["rank"] == 1],
                                     final["winners"])
                    self.assertEqual(final["winners"][0], final["winner"])
                for client in clients:
                    while True:
                        try:
                            seen[client].append(client.events.get_nowait())
                        except queue.Empty:
                            break
                for index, client in enumerate(clients):
                    own_quiz = "networking-basics" if index < 5 else "transport-layer"
                    own_ids = set(question_ids[own_quiz])
                    own_room = room_a if index < 5 else room_b
                    for event in seen[client]:
                        self.assertEqual(client.session_id, event["session_id"])
                        payload = event["payload"]
                        if event["type"] == "QUESTION_EVENT":
                            self.assertIn(payload["question_id"], own_ids)
                            self.assertNotIn("answer", payload)
                            self.assertNotIn("correct_answer", payload)
                        if event["type"] == "QUESTION_RESULT_EVENT":
                            self.assertIn(payload["question_id"], own_ids)
                            qindex = question_ids[own_quiz].index(payload["question_id"])
                            player_index = index
                            self.assertEqual(selected_answer(own_quiz, player_index, qindex), payload["your_answer"])
                        if event["type"] in ("PLAYER_STATUS_EVENT", "QUIZ_FINISHED_EVENT"):
                            self.assertEqual(own_room, payload["room_code"])
                    expected_result_count = (1 if index == 0 else
                                             len(question_ids[own_quiz]))
                    self.assertEqual(expected_result_count,
                                     sum(e["type"] == "QUESTION_RESULT_EVENT" for e in seen[client]))
        finally:
            with redirect_stdout(output):
                for client in clients:
                    client.close()
                if servers:
                    servers[0].stop()
                if runner.is_alive():
                    runner.join(timeout=5)
            self.assertFalse(runner.is_alive(), "server.main did not shut down")


if __name__ == "__main__":
    unittest.main()
