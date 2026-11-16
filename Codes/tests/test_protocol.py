from __future__ import annotations

import struct
import unittest

from protocol import (
    EnvelopeError,
    FrameBuffer,
    FramingError,
    MAX_PAYLOAD_BYTES,
    MessageTooLarge,
    PayloadDecodeError,
    decode_payload,
    encode_message,
    make_event,
    make_response,
    validate_envelope,
)
from tests.helpers import request


class FrameBufferTests(unittest.TestCase):
    def test_every_byte_fragmented(self) -> None:
        frame = encode_message(request("LIST_ROOMS_REQUEST", "r1"), direction="request")
        buffer = FrameBuffer()
        messages = []
        for byte in frame:
            messages.extend(buffer.feed(bytes([byte])))
        self.assertEqual(1, len(messages))
        self.assertEqual("r1", decode_payload(messages[0])["request_id"])

    def test_coalesced_frames_preserve_order(self) -> None:
        first = encode_message(request("LIST_ROOMS_REQUEST", "r1"))
        second = encode_message(request("LIST_QUIZZES_REQUEST", "r2"))
        payloads = FrameBuffer().feed(first + second)
        self.assertEqual(["r1", "r2"], [decode_payload(item)["request_id"] for item in payloads])

    def test_partial_header_and_body_wait(self) -> None:
        frame = encode_message(request("LIST_ROOMS_REQUEST", "r1"))
        buffer = FrameBuffer()
        self.assertEqual([], buffer.feed(frame[:3]))
        self.assertEqual([], buffer.feed(frame[3:9]))
        self.assertEqual(1, len(buffer.feed(frame[9:])))

    def test_zero_length_is_fatal(self) -> None:
        with self.assertRaises(FramingError):
            FrameBuffer().feed(b"\x00\x00\x00\x00")

    def test_oversized_length_is_fatal_before_body(self) -> None:
        with self.assertRaises(FramingError):
            FrameBuffer().feed(struct.pack("!I", MAX_PAYLOAD_BYTES + 1))

    def test_completed_frames_are_preserved_before_fatal_header(self) -> None:
        first = encode_message(request("LIST_ROOMS_REQUEST", "first"))
        second = encode_message(request("LIST_QUIZZES_REQUEST", "second"))
        with self.assertRaises(FramingError) as raised:
            FrameBuffer().feed(first + second + b"\x00\x00\x00\x00")
        self.assertEqual(
            ["first", "second"],
            [decode_payload(frame)["request_id"] for frame in raised.exception.completed_frames],
        )

    def test_fatal_header_before_valid_frame_has_no_completed_frames(self) -> None:
        valid = encode_message(request("LIST_ROOMS_REQUEST", "unreachable"))
        with self.assertRaises(FramingError) as raised:
            FrameBuffer().feed(struct.pack("!I", MAX_PAYLOAD_BYTES + 1) + valid)
        self.assertEqual((), raised.exception.completed_frames)

    def test_fragment_then_fatal_header_preserves_one_completed_frame(self) -> None:
        frame = encode_message(request("LIST_ROOMS_REQUEST", "fragmented"))
        buffer = FrameBuffer()
        self.assertEqual([], buffer.feed(frame[:7]))
        with self.assertRaises(FramingError) as raised:
            buffer.feed(frame[7:] + struct.pack("!I", 0))
        self.assertEqual(
            ["fragmented"],
            [decode_payload(item)["request_id"] for item in raised.exception.completed_frames],
        )

    def test_incomplete_eof_is_fatal(self) -> None:
        buffer = FrameBuffer()
        buffer.feed(b"\x00\x00")
        with self.assertRaises(FramingError):
            buffer.end_of_stream()


class PayloadTests(unittest.TestCase):
    def test_invalid_utf8(self) -> None:
        with self.assertRaises(PayloadDecodeError):
            decode_payload(b"\xff")

    def test_invalid_json(self) -> None:
        with self.assertRaises(PayloadDecodeError):
            decode_payload(b"{")

    def test_nonstandard_json_numbers_are_rejected(self) -> None:
        for token in (b"NaN", b"Infinity", b"-Infinity"):
            with self.subTest(token=token):
                with self.assertRaises(PayloadDecodeError):
                    decode_payload(b'{"extra":' + token + b"}")

    def test_non_object_json(self) -> None:
        with self.assertRaises(PayloadDecodeError):
            decode_payload(b"[]")

    def test_unicode_length_is_encoded_byte_length(self) -> None:
        frame = encode_message(request("LOGIN_REQUEST", "r", {"user_id": "1", "name": "\u7231"}))
        self.assertEqual(len(frame) - 4, int.from_bytes(frame[:4], "big"))

    def test_outgoing_size_limit(self) -> None:
        message = request("LOGIN_REQUEST", "r", {"blob": "x" * MAX_PAYLOAD_BYTES})
        with self.assertRaises(MessageTooLarge):
            encode_message(message)


class EnvelopeTests(unittest.TestCase):
    def test_valid_request_response_and_event(self) -> None:
        validate_envelope(request("LIST_ROOMS_REQUEST", "r1"), direction="request")
        validate_envelope(make_response("LIST_ROOMS_RESPONSE", "r1", "s1", {}), direction="response")
        validate_envelope(make_event("PLAYER_STATUS_EVENT", "e1", "s1", {}), direction="event")

    def test_bool_version_rejected(self) -> None:
        message = request("LIST_ROOMS_REQUEST", "r1")
        message["version"] = True
        with self.assertRaises(EnvelopeError):
            validate_envelope(message)

    def test_missing_and_extra_fields_rejected(self) -> None:
        missing = request("LIST_ROOMS_REQUEST", "r1")
        del missing["payload"]
        with self.assertRaises(EnvelopeError):
            validate_envelope(missing)
        extra = request("LIST_ROOMS_REQUEST", "r1")
        extra["extra"] = 1
        with self.assertRaises(EnvelopeError):
            validate_envelope(extra)

    def test_wrong_field_types_rejected(self) -> None:
        message = request("LIST_ROOMS_REQUEST", "r1")
        message["payload"] = []
        with self.assertRaises(EnvelopeError):
            validate_envelope(message)

    def test_request_role_semantics(self) -> None:
        message = request("LIST_ROOMS_REQUEST", "r1")
        message["event_id"] = "e1"
        with self.assertRaises(EnvelopeError):
            validate_envelope(message)


if __name__ == "__main__":
    unittest.main()
