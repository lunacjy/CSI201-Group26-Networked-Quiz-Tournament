"""Routes client requests to the appropriate application service."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field, replace
import logging
import threading
from typing import Any, Callable, Protocol


LOG = logging.getLogger("csi201.router")


@dataclass(frozen=True, slots=True)
class RequestContext:
    session_id: str
    user_id: str
    name: str
    connection_handle: object
    request_id: str
    defer_room_events: bool = False


@dataclass(frozen=True, slots=True)
class PendingEvent:
    event_type: str
    recipients: tuple[str, ...]
    payload: dict[str, Any]
    departed: dict[str, threading.Event] = field(default_factory=dict, repr=False, compare=False)


@dataclass(slots=True)
class _EventBatch:
    events: tuple[PendingEvent, ...]
    ready: bool = False


class RoomEventStream:
    """One FIFO for a room's committed batches, with at most one drainer."""

    def __init__(self, publish: Callable[[tuple[PendingEvent, ...]], None]):
        self._publish = publish
        self._batches: deque[_EventBatch] = deque()
        self._members: dict[str, threading.Event] = {}
        self._lock = threading.Lock()
        self._draining = False

    def add_member(self, session_id: str) -> None:
        with self._lock:
            self._members[session_id] = threading.Event()

    def remove_member(self, session_id: str) -> None:
        with self._lock:
            self._members.pop(session_id).set()

    def stage(self, events: tuple[PendingEvent, ...]) -> Callable[[], None]:
        # Reserve the position at commit time. Release outside business locks,
        # after the response attempt when the batch belongs to a request.
        with self._lock:
            batch = _EventBatch(tuple(
                replace(event, departed={sid: self._members[sid] for sid in event.recipients
                                         if sid in self._members})
                for event in events
            ))
            self._batches.append(batch)
        return lambda: self._release(batch)

    def _release(self, batch: _EventBatch) -> None:
        with self._lock:
            batch.ready = True
            if self._draining:
                return
            self._draining = True
        while True:
            with self._lock:
                if not self._batches or not self._batches[0].ready:
                    self._draining = False
                    return
                events = self._batches.popleft().events
            try:
                self._publish(events)
            except Exception:
                LOG.exception("room event batch publication failed")


@dataclass(frozen=True, slots=True)
class PublishResult:
    """Result of publishing an event to connected clients."""

    accepted: bool
    delivered: int
    reason: str | None = None


class EventSink(Protocol):
    """Interface for publishing events without direct socket access."""

    def publish(self, event: PendingEvent) -> PublishResult:
        """Publish after the caller releases its business lock."""


@dataclass(frozen=True, slots=True)
class ServiceResult:
    response_type: str | None = None
    response_payload: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    events: tuple[PendingEvent, ...] = field(default_factory=tuple)
    release_events: Callable[[], None] | None = None

    @classmethod
    def success(
        cls,
        response_type: str,
        payload: dict[str, Any],
        events: tuple[PendingEvent, ...] = (),
        release_events: Callable[[], None] | None = None,
    ) -> "ServiceResult":
        return cls(response_type=response_type, response_payload=payload, events=events,
                   release_events=release_events)

    @classmethod
    def error(cls, code: str, message: str) -> "ServiceResult":
        return cls(error_code=code, error_message=message)


class BusinessService(Protocol):
   """Interface for services handling routed requests."""


ROUTES = {
    "LIST_QUIZZES_REQUEST": ("quiz", "list_quizzes", "LIST_QUIZZES_RESPONSE"),
    "LIST_ROOMS_REQUEST": ("room", "list_rooms", "LIST_ROOMS_RESPONSE"),
    "CREATE_ROOM_REQUEST": ("room", "create_room", "CREATE_ROOM_RESPONSE"),
    "JOIN_ROOM_REQUEST": ("room", "join_room", "JOIN_ROOM_RESPONSE"),
    "PLAYER_LIST_REQUEST": ("room", "list_players", "PLAYER_LIST_RESPONSE"),
    "READY_REQUEST": ("room", "set_ready", "READY_RESPONSE"),
    "START_QUIZ_REQUEST": ("room", "start_room", "START_QUIZ_RESPONSE"),
    "ANSWER_REQUEST": ("quiz", "submit_answer", "ANSWER_RESPONSE"),
    "LEADERBOARD_REQUEST": ("quiz", "get_leaderboard", "LEADERBOARD_RESPONSE"),
    "LEAVE_ROOM_REQUEST": ("room", "leave_room", "LEAVE_ROOM_RESPONSE"),
}

EVENT_TYPES = frozenset(
    {
        "PLAYER_STATUS_EVENT",
        "ROOM_CLOSED_EVENT",
        "QUIZ_STARTED_EVENT",
        "QUESTION_EVENT",
        "QUESTION_RESULT_EVENT",
        "QUIZ_FINISHED_EVENT",
    }
)

EMPTY_PAYLOAD_REQUESTS = frozenset(
    {
        "LIST_QUIZZES_REQUEST",
        "LIST_ROOMS_REQUEST",
        "PLAYER_LIST_REQUEST",
        "START_QUIZ_REQUEST",
        "LEADERBOARD_REQUEST",
        "LEAVE_ROOM_REQUEST",
        "LOGOUT_REQUEST",
    }
)


def _short_text(value: object, maximum: int = 128) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= maximum
        and all(char.isprintable() for char in value)
    )


def validate_request_payload(message_type: str, payload: dict[str, Any]) -> str | None:
    """Return a stable human-readable error, or None when valid."""
    if message_type in EMPTY_PAYLOAD_REQUESTS:
        return None if not payload else "payload must be empty"
    if message_type == "LOGIN_REQUEST":
        if set(payload) != {"user_id", "name"}:
            return "payload must contain exactly user_id and name"
        if not _short_text(payload.get("user_id"), 64):
            return "user_id must be a printable 1-64 character string"
        if not _short_text(payload.get("name"), 100):
            return "name must be a printable 1-100 character string"
        return None
    if message_type == "CREATE_ROOM_REQUEST":
        return _one_text_field(payload, "quiz_id")
    if message_type == "JOIN_ROOM_REQUEST":
        return _one_text_field(payload, "room_code")
    if message_type == "READY_REQUEST":
        if set(payload) != {"ready"} or type(payload.get("ready")) is not bool:
            return "payload must contain exactly boolean ready"
        return None
    if message_type == "ANSWER_REQUEST":
        if set(payload) != {"question_id", "answer"}:
            return "payload must contain exactly question_id and answer"
        if not _short_text(payload.get("question_id")):
            return "question_id must be a printable 1-128 character string"
        if payload.get("answer") not in ("A", "B", "C", "D"):
            return "answer must be A, B, C or D"
        return None
    return None


def _one_text_field(payload: dict[str, Any], field_name: str) -> str | None:
    if set(payload) != {field_name} or not _short_text(payload.get(field_name)):
        return f"payload must contain exactly printable string {field_name}"
    return None


def _contains_forbidden_answer(value: object) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {"answer", "correct_answer"}:
                return True
            if _contains_forbidden_answer(child):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_forbidden_answer(item) for item in value)
    return False


def validate_pending_event(event: object) -> bool:
    if (
        not isinstance(event, PendingEvent)
        or event.event_type not in EVENT_TYPES
        or not event.recipients
        or not all(_short_text(recipient) for recipient in event.recipients)
        or not isinstance(event.payload, dict)
    ):
        return False
    if event.event_type == "QUESTION_RESULT_EVENT" and len(event.recipients) != 1:
        return False
    return not (
        event.event_type == "QUESTION_EVENT" and _contains_forbidden_answer(event.payload)
    )


class MessageRouter:
    def __init__(self, room_service: object | None = None, quiz_service: object | None = None):
        self.room_service = room_service
        self.quiz_service = quiz_service

    def dispatch(
        self, message_type: str, ctx: RequestContext, payload: dict[str, Any]
    ) -> ServiceResult:
        route = ROUTES.get(message_type)
        if route is None:
            return ServiceResult.error("UNSUPPORTED_MESSAGE", "Unsupported request type.")
        owner, method_name, expected_response = route
        service = self.room_service if owner == "room" else self.quiz_service
        if service is None:
            return ServiceResult.error(
                "INTEGRATION_UNAVAILABLE", f"{owner.capitalize()} service is not connected."
            )
        method = getattr(service, method_name, None)
        if method is None:
            return ServiceResult.error(
                "INTEGRATION_UNAVAILABLE", f"{owner.capitalize()} service method is unavailable."
            )
        try:
            result = method(ctx, dict(payload))
        except Exception:
            # Keep service exceptions from reaching the connection handler.
            # Return a generic error without exposing internal details.
            return ServiceResult.error("INTERNAL_SERVICE_ERROR", "Business service failed safely.")
        return self._validate_result(result, expected_response)

    @staticmethod
    def _validate_result(result: object, expected_response: str) -> ServiceResult:
        if not isinstance(result, ServiceResult):
            return ServiceResult.error("INTERNAL_SERVICE_ERROR", "Invalid service result.")
        is_success = result.response_type is not None
        is_error = result.error_code is not None
        if is_success == is_error:
            return ServiceResult.error("INTERNAL_SERVICE_ERROR", "Ambiguous service result.")
        if is_error:
            if not _short_text(result.error_code, 64) or not _short_text(result.error_message, 300):
                return ServiceResult.error("INTERNAL_SERVICE_ERROR", "Invalid service error.")
            return result
        if result.response_type != expected_response or not isinstance(result.response_payload, dict):
            return ServiceResult.error("INTERNAL_SERVICE_ERROR", "Invalid service response.")
        if expected_response == "LIST_QUIZZES_RESPONSE" and _contains_forbidden_answer(
            result.response_payload
        ):
            return ServiceResult.error(
                "INTERNAL_SERVICE_ERROR", "Quiz catalogue attempted to disclose an answer."
            )
        for event in result.events:
            if not validate_pending_event(event):
                return ServiceResult.error("INTERNAL_SERVICE_ERROR", "Invalid pending event.")
        return result

    def disconnect_events(self, session_id: str) -> tuple[PendingEvent, ...]:
        events: list[PendingEvent] = []
        for service in (self.room_service, self.quiz_service):
            callback = getattr(service, "on_disconnect", None) if service is not None else None
            if callback is None:
                continue
            try:
                returned = tuple(callback(session_id) or ())
            except Exception:
                LOG.warning("disconnect callback failed safely")
                continue
            for event in returned:
                if validate_pending_event(event):
                    events.append(event)
        return tuple(events)
