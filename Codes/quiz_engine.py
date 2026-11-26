"""Manage quiz timing, answer submission, scoring and results for each room."""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
import threading
import time
from typing import Any, Callable

from message_router import PendingEvent, RequestContext, RoomEventStream, ServiceResult
from question_bank import QuestionBank

LOG = logging.getLogger("csi201.quiz")


class _QuizTimer(threading.Timer):
    def __init__(self, interval: float, callback: Callable, args: tuple,
                 on_exit: Callable[[threading.Timer], None]):
        super().__init__(interval, callback, args=args)
        self._on_exit = on_exit

    def run(self) -> None:
        try:
            super().run()
        finally:
            # Timer.run can skip its callback entirely after cancellation.
            self._on_exit(self)


@dataclass
class QuizRun:
    room_code: str
    quiz_id: str
    players: dict[str, tuple[str, str]]
    questions: tuple[Any, ...]
    scores: dict[str, int]
    events: RoomEventStream | None = field(default=None, kw_only=True, repr=False)
    question_index: int = 0
    deadline: float = 0.0
    answers: dict[str, str] = field(default_factory=dict)
    timer: threading.Timer | None = None
    timers: set[threading.Timer] = field(default_factory=set, repr=False)
    closed: bool = False

    def stage(self, events: list[PendingEvent]) -> Callable[[], None]:
        # Detached runs used for leaderboard calculations have no event stream.
        assert self.events is not None
        return self.events.stage(tuple(events))


class QuizEngine:
    def __init__(
        self,
        bank: QuestionBank,
        event_sink: object,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.bank = bank
        self.event_sink = event_sink
        self._lock = threading.RLock()
        self._runs: dict[str, QuizRun] = {}
        self._session_room: dict[str, str] = {}
        self._timers: set[threading.Timer] = set()
        self._timer_runs: dict[threading.Timer, QuizRun] = {}
        self._pending_timer_joins: dict[str, set[threading.Timer]] = {}
        self._room_manager: object | None = None
        self._monotonic = monotonic

    @property
    def active_run_count(self) -> int:
        with self._lock:
            return len(self._runs)

    @property
    def active_session_mapping_count(self) -> int:
        with self._lock:
            return len(self._session_room)

    @property
    def active_timer_count(self) -> int:
        with self._lock:
            return sum(timer.is_alive() for timer in self._timers)

    def bind_room_manager(self, manager: object) -> None:
        self._room_manager = manager

    @property
    def transition_lock(self):
        """Let RoomManager commit membership and its quiz effects together."""
        return self._lock

    def new_event_stream(self) -> RoomEventStream:
        stream = RoomEventStream(lambda events: self._publish(events, stream))
        return stream

    def has_quiz(self, quiz_id: str) -> bool:
        return any(q.quiz_id == quiz_id for q in self.bank.quizzes)

    def list_quizzes(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        return ServiceResult.success("LIST_QUIZZES_RESPONSE", {"quizzes": [
            {"quiz_id": q.quiz_id, "title": q.title, "description": q.description,
             "question_count": len(q.questions)} for q in self.bank.quizzes]})

    def start_room(
        self, room_code: str, quiz_id: str, players: tuple[Any, ...],
        *, stream: RoomEventStream | None = None,
    ) -> tuple[PendingEvent, ...] | ServiceResult:
        quiz = next((q for q in self.bank.quizzes if q.quiz_id == quiz_id), None)
        if quiz is None:
            return ServiceResult.error("QUIZ_NOT_FOUND", "Quiz ID does not exist.")
        with self._lock:
            if room_code in self._runs:
                return ServiceResult.error("INVALID_STATE", "Quiz already started.")
            members = {p.session_id: (p.user_id, p.name) for p in players}
            run = QuizRun(room_code, quiz_id, members, quiz.questions,
                          {sid: 0 for sid in members}, events=stream or self.new_event_stream())
            self._runs[room_code] = run
            for sid in members:
                self._session_room[sid] = room_code
            started = PendingEvent("QUIZ_STARTED_EVENT", tuple(members), {
                "quiz_id": quiz_id, "title": quiz.title, "total_questions": len(quiz.questions)})
            try:
                question = self._open_question_locked(run)
            except RuntimeError:
                self._runs.pop(room_code, None)
                for sid in members:
                    self._session_room.pop(sid, None)
                return ServiceResult.error("INTERNAL_SERVICE_ERROR", "Unable to start the quiz timer.")
        return (started, question)

    def _open_question_locked(self, run: QuizRun) -> PendingEvent:
        q = run.questions[run.question_index]
        run.answers = {}
        run.deadline = self._monotonic() + q.time_limit
        event = PendingEvent("QUESTION_EVENT", tuple(run.players), {
            "question_id": q.question_id, "number": run.question_index + 1,
            "text": q.text, "options": dict(q.options), "time_limit": q.time_limit,
            "points": q.points})
        timer = _QuizTimer(q.time_limit, self._timer_callback,
                           (run.room_code, run.question_index, run),
                           lambda timer: self._timer_exited(run.room_code, timer))
        timer.daemon = True
        self._timers.add(timer)
        self._timer_runs[timer] = run
        run.timers.add(timer)
        try:
            timer.start()
        except RuntimeError:
            self._timers.discard(timer)
            self._timer_runs.pop(timer, None)
            run.timers.discard(timer)
            raise
        run.timer = timer
        return event

    def submit_answer(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        release = None
        with self._lock:
            code = self._session_room.get(ctx.session_id)
            run = self._runs.get(code) if code else None
            if run is None or run.closed:
                return ServiceResult.error("NOT_IN_QUIZ", "You are not in an active quiz.")
            index = run.question_index
            q = run.questions[index]
            if self._monotonic() >= run.deadline:
                pass_timeout = True
            else:
                pass_timeout = False
            if pass_timeout:
                pass
            elif payload["question_id"] != q.question_id:
                return ServiceResult.error("INVALID_ANSWER", "Question ID is not current.")
            elif payload["answer"] not in ("A", "B", "C", "D"):
                return ServiceResult.error("INVALID_ANSWER", "Answer must be A, B, C or D.")
            elif ctx.session_id in run.answers:
                return ServiceResult.error("DUPLICATE_ANSWER", "An answer was already accepted.")
            else:
                run.answers[ctx.session_id] = payload["answer"]
                if len(run.answers) == len(run.players):
                    release = run.stage(self._close_question_locked(run))
        if pass_timeout:
            self._timeout(run.room_code, index, run)
            return ServiceResult.error("QUESTION_CLOSED", "The deadline has passed.")
        if release is not None:
            release()
            self._reap_timers(run.room_code)
        return ServiceResult.success("ANSWER_RESPONSE", {"accepted": True, "question_id": payload["question_id"]})

    def _timer_callback(self, room_code: str, index: int, expected_run: QuizRun) -> None:
        try:
            self._timeout(room_code, index, expected_run)
        except Exception:
            LOG.exception("quiz timer callback failed for room %s", room_code)

    def _timeout(self, room_code: str, index: int, expected_run: QuizRun | None = None) -> None:
        with self._lock:
            run = self._runs.get(room_code)
            if (run is None or (expected_run is not None and run is not expected_run)
                    or run.closed or run.question_index != index
                    or self._monotonic() < run.deadline):
                return
            release = run.stage(self._close_question_locked(run))
        release()
        self._reap_timers(room_code)

    def _close_question_locked(self, run: QuizRun) -> list[PendingEvent]:
        self._cancel_timer_locked(run)
        q = run.questions[run.question_index]
        awarded: dict[str, tuple[str | None, int]] = {}
        for sid in run.players:
            answer = run.answers.get(sid)
            points = q.points if answer == q.answer else 0
            run.scores[sid] += points
            awarded[sid] = (answer, points)
        events = []
        for sid in run.players:
            answer, points = awarded[sid]
            # Every recipient sees the same complete post-settlement scoreboard.
            events.append(PendingEvent("QUESTION_RESULT_EVENT", (sid,), {
                "question_id": q.question_id, "correct_answer": q.answer,
                "your_answer": answer, "points_awarded": points,
                "leaderboard": self._leaderboard(run)}))
        run.question_index += 1
        if run.question_index >= len(run.questions):
            run.closed = True
            final_board = self._leaderboard(run)
            winners = [entry for entry in final_board if entry["rank"] == 1]
            events.append(PendingEvent("QUIZ_FINISHED_EVENT", tuple(run.players), {
                "quiz_id": run.quiz_id, "room_code": run.room_code,
                "final_leaderboard": final_board, "winners": winners,
                "winner": winners[0] if winners else None}))
        else:
            events.append(self._open_question_locked(run))
        return events

    def _publish(self, events: tuple[PendingEvent, ...], stream: RoomEventStream) -> None:
        for event in events:
            if event.event_type == "QUIZ_FINISHED_EVENT" and self._room_manager:
                try:
                    self._room_manager.mark_finished(event.payload["room_code"], stream)
                except Exception:
                    LOG.exception("failed to mark finished room %s", event.payload["room_code"])
            try:
                self.event_sink.publish(event)
            except Exception:
                LOG.exception("event publication failed for %s", event.event_type)

    def _leaderboard(self, run: QuizRun) -> list[dict[str, Any]]:
        order = {sid: i for i, sid in enumerate(run.players)}
        sorted_ids = sorted(run.players, key=lambda s: (-run.scores[s], order[s], run.players[s][0]))
        board: list[dict[str, Any]] = []
        last_score: int | None = None
        rank = 0
        for position, sid in enumerate(sorted_ids, 1):
            score = run.scores[sid]
            if score != last_score:
                rank = position
                last_score = score
            board.append({"user_id": run.players[sid][0], "name": run.players[sid][1],
                          "score": score, "rank": rank})
        return board

    def get_leaderboard(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        with self._lock:
            code = self._session_room.get(ctx.session_id)
            run = self._runs.get(code) if code else None
            if run is None:
                return ServiceResult.error("NOT_IN_QUIZ", "You are not in a quiz.")
            return ServiceResult.success("LEADERBOARD_RESPONSE", {"leaderboard": self._leaderboard(run)})

    def player_departed(self, room_code: str, session_id: str) -> list[PendingEvent]:
        events: list[PendingEvent] = []
        with self._lock:
            run = self._runs.get(room_code)
            self._session_room.pop(session_id, None)
            if run:
                run.players.pop(session_id, None)
                run.answers.pop(session_id, None)
                run.scores.pop(session_id, None)
                if not run.players:
                    self._close_locked(run)
                    self._cancel_run_timers_locked(run)
                    self._runs.pop(room_code, None)
                    for sid, code in tuple(self._session_room.items()):
                        if code == room_code:
                            self._session_room.pop(sid, None)
                elif not run.closed and len(run.answers) == len(run.players):
                    events = self._close_question_locked(run)
        return events

    def on_disconnect(self, session_id: str) -> tuple[PendingEvent, ...]:
        return ()

    def close_room(self, room_code: str, *, stream: RoomEventStream | None = None) -> None:
        with self._lock:
            run = self._runs.get(room_code)
            if run and (stream is None or run.events is stream):
                self._runs.pop(room_code)
                self._close_locked(run)
                self._cancel_run_timers_locked(run)
                for sid, code in tuple(self._session_room.items()):
                    if code == room_code:
                        self._session_room.pop(sid, None)
        self._reap_timers(room_code, wait=True)

    def _close_locked(self, run: QuizRun) -> None:
        run.closed = True
        self._cancel_timer_locked(run)

    def _cancel_timer_locked(self, run: QuizRun) -> None:
        timer, run.timer = run.timer, None
        if timer is not None:
            timer.cancel()
            if timer in self._timer_runs:
                self._pending_timer_joins.setdefault(run.room_code, set()).add(timer)

    def _cancel_run_timers_locked(self, run: QuizRun) -> None:
        for timer in tuple(run.timers):
            timer.cancel()
            self._pending_timer_joins.setdefault(run.room_code, set()).add(timer)

    def _timer_exited(self, room_code: str, timer: threading.Timer) -> None:
        with self._lock:
            joins = self._pending_timer_joins.get(room_code)
            if joins is not None:
                joins.discard(timer)
                if not joins:
                    del self._pending_timer_joins[room_code]
            self._timers.discard(timer)
            run = self._timer_runs.pop(timer, None)
            if run is not None:
                run.timers.discard(timer)
                if run.timer is timer:
                    run.timer = None

    def _reap_timers(self, room_code: str, *, wait: bool = False) -> None:
        with self._lock:
            pending = tuple(self._pending_timer_joins.get(room_code, ()))
        current = threading.current_thread()
        for timer in pending:
            if wait and timer is not current:
                timer.join(timeout=2)
            if not timer.is_alive():
                self._timer_exited(room_code, timer)
