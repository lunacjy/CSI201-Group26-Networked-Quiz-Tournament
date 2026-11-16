"""TCP message framing, JSON decoding and protocol validation."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import struct
from typing import Any, Final

from strict_json import loads as strict_json_loads


PROTOCOL_VERSION: Final = 1
HEADER_SIZE: Final = 4
MAX_PAYLOAD_BYTES: Final = 65_536
MAX_TYPE_LENGTH: Final = 64
MAX_ID_LENGTH: Final = 128
ENVELOPE_FIELDS: Final = frozenset(
    {"version", "type", "request_id", "event_id", "session_id", "payload"}
)


class ProtocolError(ValueError):
    """Base class for protocol errors."""


class FramingError(ProtocolError):
    """The byte stream cannot be recovered safely; close this connection.

    ``completed_frames`` contains frames extracted before the fatal header in
    the same ``feed`` call.  They are already removed from the receive buffer
    and must be dispatched at most once before the connection is closed.
    """

    def __init__(self, message: str, completed_frames: tuple[bytes, ...] = ()) -> None:
        super().__init__(message)
        self.completed_frames = completed_frames


class PayloadDecodeError(ProtocolError):
    """A complete frame had invalid UTF-8, JSON, or a non-object JSON value."""


class EnvelopeError(ProtocolError):
    """A decoded JSON object did not satisfy protocol v1."""

    def __init__(self, message: str, request_id: str | None = None) -> None:
        super().__init__(message)
        self.request_id = request_id


class MessageTooLarge(ProtocolError):
    """An outgoing JSON message exceeds the protocol limit."""


@dataclass(slots=True)
class FrameBuffer:
    """Per-connection incremental length-prefixed frame extractor."""

    max_payload: int = MAX_PAYLOAD_BYTES
    _buffer: bytearray = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._buffer = bytearray()

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)

    def feed(self, data: bytes) -> list[bytes]:
        if not isinstance(data, bytes):
            raise TypeError("FrameBuffer.feed expects bytes")
        self._buffer.extend(data)
        frames: list[bytes] = []
        while len(self._buffer) >= HEADER_SIZE:
            length = struct.unpack("!I", self._buffer[:HEADER_SIZE])[0]
            if length == 0:
                raise FramingError(
                    "frame payload length must be at least 1 byte", tuple(frames)
                )
            if length > self.max_payload:
                raise FramingError(
                    f"frame payload length {length} exceeds limit {self.max_payload}",
                    tuple(frames),
                )
            total = HEADER_SIZE + length
            if len(self._buffer) < total:
                break
            frames.append(bytes(self._buffer[HEADER_SIZE:total]))
            del self._buffer[:total]
        return frames

    def end_of_stream(self) -> None:
        if self._buffer:
            raise FramingError("connection ended with an incomplete frame")


def decode_payload(payload: bytes) -> dict[str, Any]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PayloadDecodeError("payload is not valid UTF-8") from exc
    try:
        value = strict_json_loads(text)
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise PayloadDecodeError("payload is not valid JSON") from exc
    if not isinstance(value, dict):
        raise PayloadDecodeError("JSON payload must be an object")
    return value


def _safe_request_id(message: dict[str, Any]) -> str | None:
    value = message.get("request_id")
    if isinstance(value, str) and 1 <= len(value) <= MAX_ID_LENGTH:
        return value
    return None


def _valid_optional_id(value: object, *, allow_null: bool = True) -> bool:
    if value is None:
        return allow_null
    return isinstance(value, str) and 1 <= len(value) <= MAX_ID_LENGTH


def validate_envelope(message: dict[str, Any], *, direction: str = "request") -> None:
    """Validate the exact common envelope and role-specific ID semantics.

    ``direction`` is one of ``request``, ``response`` or ``event``.
    """

    request_id = _safe_request_id(message)
    if set(message) != ENVELOPE_FIELDS:
        missing = sorted(ENVELOPE_FIELDS - set(message))
        extra = sorted(set(message) - ENVELOPE_FIELDS)
        detail = []
        if missing:
            detail.append(f"missing fields: {', '.join(missing)}")
        if extra:
            detail.append(f"unknown fields: {', '.join(extra)}")
        raise EnvelopeError("; ".join(detail), request_id)

    version = message["version"]
    if type(version) is not int or version != PROTOCOL_VERSION:
        raise EnvelopeError("version must be integer 1", request_id)
    message_type = message["type"]
    if not isinstance(message_type, str) or not 1 <= len(message_type) <= MAX_TYPE_LENGTH:
        raise EnvelopeError("type must be a non-empty string of at most 64 characters", request_id)
    if not _valid_optional_id(message["request_id"]):
        raise EnvelopeError("request_id must be null or a 1-128 character string", None)
    if not _valid_optional_id(message["event_id"]):
        raise EnvelopeError("event_id must be null or a 1-128 character string", request_id)
    if not _valid_optional_id(message["session_id"]):
        raise EnvelopeError("session_id must be null or a 1-128 character string", request_id)
    if not isinstance(message["payload"], dict):
        raise EnvelopeError("payload must be an object", request_id)

    if direction == "request":
        if not message_type.endswith("_REQUEST"):
            raise EnvelopeError("client message type must end with _REQUEST", request_id)
        if message["request_id"] is None or message["event_id"] is not None:
            raise EnvelopeError("request requires request_id and null event_id", request_id)
    elif direction == "response":
        if not (message_type.endswith("_RESPONSE") or message_type == "ERROR_RESPONSE"):
            raise EnvelopeError("response type is invalid", request_id)
        if message["request_id"] is None or message["event_id"] is not None:
            raise EnvelopeError("response requires request_id and null event_id", request_id)
    elif direction == "event":
        if not message_type.endswith("_EVENT"):
            raise EnvelopeError("event type must end with _EVENT", request_id)
        if message["request_id"] is not None or message["event_id"] is None:
            raise EnvelopeError("event requires event_id and null request_id", request_id)
    else:
        raise ValueError("direction must be request, response, or event")


def encode_message(message: dict[str, Any], *, direction: str | None = None) -> bytes:
    if direction is not None:
        validate_envelope(message, direction=direction)
    try:
        payload = json.dumps(
            message, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise ProtocolError("message is not JSON serialisable") from exc
    if not payload or len(payload) > MAX_PAYLOAD_BYTES:
        raise MessageTooLarge(
            f"encoded message is {len(payload)} bytes; limit is {MAX_PAYLOAD_BYTES}"
        )
    return struct.pack("!I", len(payload)) + payload


def make_response(
    message_type: str,
    request_id: str,
    session_id: str | None,
    payload: dict[str, Any],
) -> dict[str, Any]:
    message = {
        "version": PROTOCOL_VERSION,
        "type": message_type,
        "request_id": request_id,
        "event_id": None,
        "session_id": session_id,
        "payload": payload,
    }
    validate_envelope(message, direction="response")
    return message


def make_error(
    request_id: str | None,
    session_id: str | None,
    code: str,
    human_message: str,
) -> dict[str, Any]:
    # An unparseable message has no request ID. This is the sole response
    # exception to the normal non-null response request_id rule.
    message = {
        "version": PROTOCOL_VERSION,
        "type": "ERROR_RESPONSE",
        "request_id": request_id,
        "event_id": None,
        "session_id": session_id,
        "payload": {"code": code, "message": human_message},
    }
    if request_id is not None:
        validate_envelope(message, direction="response")
    return message


def make_event(
    message_type: str,
    event_id: str,
    recipient_session_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    message = {
        "version": PROTOCOL_VERSION,
        "type": message_type,
        "request_id": None,
        "event_id": event_id,
        "session_id": recipient_session_id,
        "payload": payload,
    }
    validate_envelope(message, direction="event")
    return message
