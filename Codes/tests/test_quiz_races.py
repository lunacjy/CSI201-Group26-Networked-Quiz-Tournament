from __future__ import annotations

from dataclasses import dataclass
import threading
import time
import unittest

from message_router import PendingEvent, PublishResult, RequestContext, ServiceResult
from question_bank import validate_bank
from quiz_engine import QuizEngine
from tests.helpers import valid_bank_dict


class Sink:
    def __init__(self):
        self.lock = threading.Lock()
        self.events: list[PendingEvent] = []

    def publish(self, event: PendingEvent) -> PublishResult:
        with self.lock:
            self.events.append(event)
        return PublishResult(True, len(event.recipients))


@dataclass
class Player:
    session_id: str
    user_id: str
    name: str


def ctx(session: str, user: str) -> RequestContext:
    return RequestContext(session, user, user, object(), "race")


class QuizRaceTests(unittest.TestCase):
    def test_150_barrier_synchronized_answer_deadline_races_settle_once(self) -> None:
        clock = [0.0]
        sink = Sink()
        engine = QuizEngine(validate_bank(valid_bank_dict()), sink, monotonic=lambda: clock[0])
        started_at = time.monotonic()
        rounds = 150
        for turn in range(rounds):
            room = "RACE-REUSE"
            host = Player(f"h-{turn}", f"host-{turn}", f"Host {turn}")
            guest = Player(f"g-{turn}", f"guest-{turn}", f"Guest {turn}")
            result = engine.start_room(room, "quiz-1", (host, guest))
            self.assertNotIsInstance(result, ServiceResult)
            run = engine._runs[room]
            if turn % 2 == 0:
                self.assertEqual("ANSWER_RESPONSE", engine.submit_answer(
                    ctx(host.session_id, host.user_id), {"question_id": "Q0", "answer": "A"}
                ).response_type)
                clock[0] = 4.999
                expected_score = 100
            else:
                clock[0] = 5.0
                expected_score = 0

            barrier = threading.Barrier(3)
            answer_results: list[ServiceResult] = []
            errors: list[BaseException] = []

            def last_answer() -> None:
                try:
                    barrier.wait(timeout=1)
                    answer_results.append(engine.submit_answer(
                        ctx(guest.session_id, guest.user_id),
                        {"question_id": "Q0", "answer": "A"},
                    ))
                except BaseException as exc:
                    errors.append(exc)

            def timer_callback() -> None:
                try:
                    barrier.wait(timeout=1)
                    engine._timeout(room, 0, run)
                except BaseException as exc:
                    errors.append(exc)

            answer_thread = threading.Thread(target=last_answer)
            timeout_thread = threading.Thread(target=timer_callback)
            answer_thread.start()
            timeout_thread.start()
            barrier.wait(timeout=1)
            answer_thread.join(timeout=1)
            timeout_thread.join(timeout=1)
            self.assertFalse(answer_thread.is_alive())
            self.assertFalse(timeout_thread.is_alive())
            self.assertEqual([], errors)
            self.assertEqual(1, len(answer_results))
            self.assertEqual(1, run.question_index, f"round={turn}")
            self.assertEqual({expected_score}, set(run.scores.values()), f"round={turn}")
            q0_results = [e for e in sink.events if e.event_type == "QUESTION_RESULT_EVENT"
                          and e.payload["question_id"] == "Q0"
                          and e.recipients[0] in (host.session_id, guest.session_id)]
            self.assertEqual(2, len(q0_results), f"round={turn}: one private result per participant")
            self.assertEqual({host.session_id, guest.session_id}, {e.recipients[0] for e in q0_results})

            engine.close_room(room)
            # A stale callback from the removed run cannot affect a new run with the same room code.
            next_host = Player(f"next-h-{turn}", f"next-host-{turn}", "Next Host")
            next_guest = Player(f"next-g-{turn}", f"next-guest-{turn}", "Next Guest")
            engine.start_room(room, "quiz-1", (next_host, next_guest))
            engine._timeout(room, 0, run)
            self.assertEqual(0, engine._runs[room].question_index)
            engine.close_room(room)
            self.assertEqual(0, engine.active_run_count)
            self.assertEqual(0, engine.active_session_mapping_count)
            self.assertEqual(0, engine.active_timer_count)
            self.assertEqual(0, len(engine._timer_runs))
            clock[0] = 0.0

        self.assertEqual(rounds, 150)
        self.assertLess(time.monotonic() - started_at, 20.0)


if __name__ == "__main__":
    unittest.main()
