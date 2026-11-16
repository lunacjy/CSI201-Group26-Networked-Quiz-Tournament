from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys
from types import SimpleNamespace
import tempfile
import unittest

from message_router import RequestContext, ServiceResult
from question_bank import MAX_FILE_BYTES, QuestionBankError, load_and_validate_bank, validate_bank
from quiz_engine import QuizEngine
from tests.bank_fixtures import bank_with_sizes


class Sink:
    def __init__(self) -> None:
        self.events = []

    def publish(self, event) -> None:
        self.events.append(event)


def context(sid: str) -> RequestContext:
    return RequestContext(sid, sid, sid, object(), "dynamic-test")


class DynamicBankTests(unittest.TestCase):
    def _load_file(self, data: dict):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bank.json"
            path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            return load_and_validate_bank(path)

    def test_legal_counts_unicode_extras_catalogue_and_order(self) -> None:
        for sizes in ([5], [5, 8, 10, 25], [10] * 5, [5] * 10,
                      [5] * 20, [50], [50] * 20, [5, 10, 50]):
            with self.subTest(sizes=sizes):
                data = bank_with_sizes(list(sizes))
                self.assertLessEqual(len(json.dumps(data, ensure_ascii=False).encode("utf-8")),
                                     MAX_FILE_BYTES)
                bank = self._load_file(data)
                self.assertEqual(list(sizes), [len(q.questions) for q in bank.quizzes])
                sink = Sink()
                engine = QuizEngine(bank, sink)
                listed = engine.list_quizzes(context("s0"), {})
                actual = listed.response_payload["quizzes"]
                self.assertEqual([q.quiz_id for q in bank.quizzes], [q["quiz_id"] for q in actual])
                self.assertEqual(list(sizes), [q["question_count"] for q in actual])
                self.assertEqual([q.title for q in bank.quizzes], [q["title"] for q in actual])
                self.assertIn("测验", actual[0]["title"])
                self.assertNotIn("questions", str(actual))
                self.assertNotIn("answer", str(actual))
                players = (SimpleNamespace(session_id="s0", user_id="u0", name="甲"),
                           SimpleNamespace(session_id="s1", user_id="u1", name="乙"))
                started = engine.start_room("DYNAMIC", bank.quizzes[-1].quiz_id, players)
                self.assertNotIsInstance(started, ServiceResult)
                question = started[1]
                self.assertEqual(bank.quizzes[-1].questions[0].question_id, question.payload["question_id"])
                self.assertEqual(bank.quizzes[-1].questions[0].text, question.payload["text"])
                self.assertNotIn("answer", question.payload)
                engine.close_room("DYNAMIC")

    def test_invalid_schema_values_reject_whole_file(self) -> None:
        cases = {}
        def case(name, mutate):
            bank = bank_with_sizes([5, 5]); mutate(bank); cases[name] = bank
        case("zero quizzes", lambda b: b.update(quizzes=[]))
        case("21 quizzes", lambda b: b.update(quizzes=bank_with_sizes([5] * 21)["quizzes"]))
        case("four questions", lambda b: b["quizzes"][1].update(questions=b["quizzes"][1]["questions"][:4]))
        case("51 questions", lambda b: b["quizzes"][1].update(questions=bank_with_sizes([51])["quizzes"][0]["questions"]))
        case("missing schema", lambda b: b.pop("schema_version"))
        case("schema bool", lambda b: b.update(schema_version=True))
        case("schema string", lambda b: b.update(schema_version="1"))
        case("schema 2", lambda b: b.update(schema_version=2))
        case("quizzes wrong type", lambda b: b.update(quizzes="not array"))
        case("missing title", lambda b: b["quizzes"][1].pop("title"))
        case("blank quiz id", lambda b: b["quizzes"][1].update(quiz_id=" "))
        case("bad question id", lambda b: b["quizzes"][1]["questions"][0].update(question_id="bad id"))
        case("duplicate quiz id", lambda b: b["quizzes"][1].update(quiz_id=b["quizzes"][0]["quiz_id"]))
        case("cross quiz question id", lambda b: b["quizzes"][1]["questions"][0].update(question_id=b["quizzes"][0]["questions"][0]["question_id"]))
        case("missing option", lambda b: b["quizzes"][1]["questions"][0]["options"].pop("D"))
        case("extra option", lambda b: b["quizzes"][1]["questions"][0]["options"].update(E="Extra"))
        case("invalid answer", lambda b: b["quizzes"][1]["questions"][0].update(answer="E"))
        case("time 4", lambda b: b["quizzes"][1]["questions"][0].update(time_limit=4))
        case("time 61", lambda b: b["quizzes"][1]["questions"][0].update(time_limit=61))
        case("time bool", lambda b: b["quizzes"][1]["questions"][0].update(time_limit=True))
        case("points 0", lambda b: b["quizzes"][1]["questions"][0].update(points=0))
        case("points 1001", lambda b: b["quizzes"][1]["questions"][0].update(points=1001))
        case("points bool", lambda b: b["quizzes"][1]["questions"][0].update(points=False))
        case("missing points", lambda b: b["quizzes"][1]["questions"][0].pop("points"))
        for name, data in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(QuestionBankError) as caught:
                    self._load_file(data)
                self.assertTrue(str(caught.exception), name)

    def test_invalid_bytes_json_and_decimal_size_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bank.json"
            for raw in (b"", b"\xff", b"{", b'{"schema_version":1,"quizzes":NaN}',
                        b'{"schema_version":1,"quizzes":Infinity}'):
                with self.subTest(raw=raw):
                    path.write_bytes(raw)
                    with self.assertRaises(QuestionBankError):
                        load_and_validate_bank(path)
            duplicate_options = json.dumps(bank_with_sizes([5]), ensure_ascii=False)
            duplicate_options = duplicate_options.replace('"A": "正确 answer"',
                                                          '"A": "正确 answer", "A": "duplicate"', 1)
            self.assertIn('"A": "duplicate"', duplicate_options)
            path.write_text(duplicate_options, encoding="utf-8")
            with self.assertRaisesRegex(QuestionBankError, "duplicate JSON object key"):
                load_and_validate_bank(path)
            valid = json.dumps(bank_with_sizes([5]), ensure_ascii=False, separators=(",", ":"))
            prefix = valid[:-1] + ',"padding":"'
            exact = (prefix + "x" * (MAX_FILE_BYTES - len(prefix.encode()) - 2) + '"}').encode()
            self.assertEqual(MAX_FILE_BYTES, len(exact))
            path.write_bytes(exact)
            self.assertEqual(1, len(load_and_validate_bank(path).quizzes))
            path.write_bytes(exact + b" ")
            with self.assertRaisesRegex(QuestionBankError, "maximum"):
                load_and_validate_bank(path)

    def test_invalid_file_prevents_production_server_listening(self) -> None:
        root = Path(__file__).resolve().parents[1]
        invalid = [b"", b"\xff", b"{",
                   json.dumps(bank_with_sizes([5, 5]) | {"schema_version": False}).encode()]
        duplicate = bank_with_sizes([5, 5])
        duplicate["quizzes"][1]["questions"][4]["question_id"] = duplicate["quizzes"][0]["questions"][0]["question_id"]
        invalid.append(json.dumps(duplicate).encode())
        with tempfile.TemporaryDirectory() as directory:
            bank_path = Path(directory) / "invalid.json"
            for raw in invalid:
                with self.subTest(raw=raw[:30]):
                    bank_path.write_bytes(raw)
                    with socket.socket() as probe:
                        probe.bind(("127.0.0.1", 0))
                        port = probe.getsockname()[1]
                    result = subprocess.run(
                        [sys.executable, str(root / "server.py"), "--host", "127.0.0.1",
                         "--port", str(port), "--questions", str(bank_path)],
                        cwd=root, capture_output=True, text=True, timeout=3)
                    self.assertEqual(2, result.returncode)
                    self.assertNotIn("SERVER_READY", result.stdout)
                    self.assertIn("QUESTION_BANK_ERROR", result.stderr)
                    with socket.socket() as released:
                        released.bind(("127.0.0.1", port))

    def test_mixed_times_points_deadline_scores_and_50_question_order(self) -> None:
        clock = [0.0]
        data = bank_with_sizes([5], times=[5, 10, 15, 30, 60],
                               points=[1, 100, 250, 500, 1000])
        bank = validate_bank(data)
        sink = Sink()
        engine = QuizEngine(bank, sink, monotonic=lambda: clock[0])
        players = (SimpleNamespace(session_id="a", user_id="a", name="甲"),
                   SimpleNamespace(session_id="b", user_id="b", name="乙"))
        started = engine.start_room("MIX", "quiz-00", players)
        cumulative = 0
        for index, q in enumerate(bank.quizzes[0].questions):
            run = engine._runs["MIX"]
            self.assertEqual(q.time_limit, run.deadline - clock[0])
            self.assertEqual("ANSWER_RESPONSE", engine.submit_answer(
                context("a"), {"question_id": q.question_id, "answer": "A"}).response_type)
            if index == 2:
                clock[0] = run.deadline
                self.assertEqual("QUESTION_CLOSED", engine.submit_answer(
                    context("b"), {"question_id": q.question_id, "answer": "A"}).error_code)
            else:
                self.assertEqual("ANSWER_RESPONSE", engine.submit_answer(
                    context("b"), {"question_id": q.question_id, "answer": "B"}).response_type)
            cumulative += q.points
            board = engine.get_leaderboard(context("a"), {}).response_payload["leaderboard"]
            self.assertEqual([cumulative, 0], [entry["score"] for entry in board])
        self.assertEqual([1, 100, 250, 500, 1000], [q.points for q in bank.quizzes[0].questions])
        self.assertEqual([5, 10, 15, 30, 60], [q.time_limit for q in bank.quizzes[0].questions])
        self.assertEqual([f"Q00_{i:02d}" for i in range(5)],
                         [started[1].payload["question_id"]] +
                         [e.payload["question_id"] for e in sink.events if e.event_type == "QUESTION_EVENT"])
        self.assertEqual(1, len([e for e in sink.events if e.event_type == "QUIZ_FINISHED_EVENT"]))
        engine.close_room("MIX")
        self.assertEqual(0, engine.active_timer_count)

        long_bank = validate_bank(bank_with_sizes([50]))
        clock[0] = 0.0
        long_engine = QuizEngine(long_bank, Sink(), monotonic=lambda: clock[0])
        long_engine.start_room("LONG", "quiz-00", players)
        for question in long_bank.quizzes[0].questions:
            run = long_engine._runs["LONG"]
            self.assertEqual(question.question_id, run.questions[run.question_index].question_id)
            clock[0] = run.deadline
            long_engine._timeout("LONG", run.question_index, run)
        self.assertTrue(long_engine._runs["LONG"].closed)
        long_engine.close_room("LONG")
        self.assertEqual(0, long_engine.active_run_count)
