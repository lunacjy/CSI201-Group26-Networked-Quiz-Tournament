from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from client import Client
from message_router import RequestContext
from protocol import decode_payload, encode_message
from question_bank import validate_bank
from quiz_engine import QuizEngine, QuizRun
from tests.bank_fixtures import bank_with_sizes


def ctx(sid: str) -> RequestContext:
    return RequestContext(sid, sid, sid, object(), "tie-test")


class Sink:
    def publish(self, event) -> None:
        pass


class LeaderboardTieTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bank = validate_bank(bank_with_sizes([5]))
        self.engine = QuizEngine(self.bank, Sink())
        self.players = {f"s{i}": (f"u{i}", f"Player {i}") for i in range(3)}

    def run_with_scores(self, scores: list[int]) -> QuizRun:
        return QuizRun("TIE", "quiz-00", dict(self.players),
                       self.bank.quizzes[0].questions,
                       {f"s{i}": value for i, value in enumerate(scores)})

    def test_competition_ranks_and_join_order(self) -> None:
        for scores, expected_scores, expected_ranks, expected_ids in (
            ([500, 500, 400], [500, 500, 400], [1, 1, 3], ["u0", "u1", "u2"]),
            ([400, 500, 400], [500, 400, 400], [1, 2, 2], ["u1", "u0", "u2"]),
            ([0, 0, 0], [0, 0, 0], [1, 1, 1], ["u0", "u1", "u2"]),
            ([0, 500, 400], [500, 400, 0], [1, 2, 3], ["u1", "u2", "u0"]),
        ):
            with self.subTest(scores=scores):
                board = self.engine._leaderboard(self.run_with_scores(scores))
                self.assertEqual(expected_scores, [row["score"] for row in board])
                self.assertEqual(expected_ranks, [row["rank"] for row in board])
                self.assertEqual(expected_ids, [row["user_id"] for row in board])

    def test_final_event_winners_legacy_field_wire_and_cli(self) -> None:
        for scores, winner_count in (([500, 500, 400], 2), ([0, 0, 0], 3),
                                     ([500, 400, 400], 1)):
            with self.subTest(scores=scores):
                run = self.run_with_scores(scores)
                run.question_index = 4
                events = self.engine._close_question_locked(run)
                final = next(e for e in events if e.event_type == "QUIZ_FINISHED_EVENT")
                payload = final.payload
                self.assertEqual(winner_count, len(payload["winners"]))
                self.assertEqual(payload["winners"][0], payload["winner"])
                self.assertEqual([row for row in payload["final_leaderboard"] if row["rank"] == 1],
                                 payload["winners"])
                frame = encode_message({"version": 1, "type": "QUIZ_FINISHED_EVENT",
                                        "request_id": None, "event_id": "evt-1", "session_id": "s0",
                                        "payload": payload}, direction="event")
                decoded = decode_payload(frame[4:])
                self.assertEqual(payload, decoded["payload"])
                output = io.StringIO()
                with redirect_stdout(output):
                    Client("127.0.0.1", 12000, "u0", "Player 0")._display(decoded)
                self.assertIn("Joint Winners" if winner_count > 1 else "Winner:", output.getvalue())
                self.assertIn("Final leaderboard", output.getvalue())

    def test_departure_removes_player_and_final_room_cleanup(self) -> None:
        members = tuple(SimpleNamespace(session_id=sid, user_id=user, name=name)
                        for sid, (user, name) in self.players.items())
        self.engine.start_room("TIE", "quiz-00", members)
        run = self.engine._runs["TIE"]
        run.scores.update({"s0": 500, "s1": 500, "s2": 400})
        self.engine.player_departed("TIE", "s0")
        board = self.engine.get_leaderboard(ctx("s1"), {}).response_payload["leaderboard"]
        self.assertEqual(["u1", "u2"], [row["user_id"] for row in board])
        self.assertEqual([1, 2], [row["rank"] for row in board])
        self.engine.player_departed("TIE", "s1")
        self.engine.player_departed("TIE", "s2")
        self.engine.close_room("TIE")
        self.assertEqual(0, self.engine.active_run_count)
        self.assertEqual(0, self.engine.active_session_mapping_count)
        self.assertEqual(0, self.engine.active_timer_count)


if __name__ == "__main__":
    unittest.main()
