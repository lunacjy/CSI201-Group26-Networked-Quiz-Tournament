from __future__ import annotations

import struct
import time
import unittest

from protocol import MAX_PAYLOAD_BYTES, encode_message
from tests.helpers import RunningServer, login, recv_message, request, send_message


class NetworkErrorTests(unittest.TestCase):
    def test_invalid_utf8_json_and_non_object_are_recoverable(self) -> None:
        with RunningServer() as running:
            sock = running.connect()
            for payload in (b"\xff", b"{", b"[]"):
                sock.sendall(struct.pack("!I", len(payload)) + payload)
                response = recv_message(sock)
                self.assertEqual("INVALID_MESSAGE", response["payload"]["code"])
                self.assertIsNone(response["request_id"])
            self.assertTrue(login(sock, "u1", "Alice"))
            sock.close()

    def test_nonstandard_json_payload_is_recoverable(self) -> None:
        with RunningServer() as running:
            sock = running.connect()
            for token in (b"NaN", b"Infinity", b"-Infinity"):
                payload = b'{"version":1,"type":"LOGIN_REQUEST","request_id":"r","event_id":null,"session_id":null,"payload":{"x":' + token + b"}}"
                sock.sendall(struct.pack("!I", len(payload)) + payload)
                response = recv_message(sock)
                self.assertEqual("INVALID_MESSAGE", response["payload"]["code"])
            self.assertTrue(login(sock, "u1", "Alice", "real-login"))
            sock.close()

    def test_valid_frame_before_fatal_header_is_processed_once_then_closed(self) -> None:
        with RunningServer() as running:
            sock = running.connect()
            unaffected = running.connect()
            login_frame = encode_message(
                request("LOGIN_REQUEST", "login-1", {"user_id": "u1", "name": "Alice"}),
                direction="request",
            )
            sock.sendall(login_frame + struct.pack("!I", 0))
            self.assertEqual("LOGIN_RESPONSE", recv_message(sock)["type"])
            self.assertEqual(b"", sock.recv(1))
            deadline = time.monotonic() + 2
            while len(running.server.sessions) and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(0, len(running.server.sessions))
            self.assertTrue(login(unaffected, "u2", "Bob"))
            sock.close()
            unaffected.close()

    def test_bad_envelope_returns_error_and_connection_survives(self) -> None:
        with RunningServer() as running:
            sock = running.connect()
            bad = request("LOGIN_REQUEST", "bad-1", {"user_id": "u1", "name": "Alice"})
            bad["version"] = 2
            sock.sendall(encode_message(bad))
            response = recv_message(sock)
            self.assertEqual("INVALID_MESSAGE", response["payload"]["code"])
            self.assertEqual("bad-1", response["request_id"])
            self.assertTrue(login(sock, "u1", "Alice", "good-1"))
            sock.close()

    def test_zero_and_oversized_lengths_close_only_bad_clients(self) -> None:
        for header in (struct.pack("!I", 0), struct.pack("!I", MAX_PAYLOAD_BYTES + 1)):
            with RunningServer() as running:
                bad = running.connect()
                bad.sendall(header)
                self.assertEqual(b"", bad.recv(1))
                bad.close()
                good = running.connect()
                self.assertTrue(login(good, "u1", "Alice"))
                good.close()

    def test_unsupported_request_and_invalid_payload(self) -> None:
        with RunningServer() as running:
            sock = running.connect()
            session = login(sock, "u1", "Alice")
            send_message(sock, request("WHATEVER_REQUEST", "r1", session_id=session))
            self.assertEqual("UNSUPPORTED_MESSAGE", recv_message(sock)["payload"]["code"])
            send_message(sock, request("READY_REQUEST", "r2", {"ready": 1}, session))
            self.assertEqual("INVALID_MESSAGE", recv_message(sock)["payload"]["code"])
            sock.close()


if __name__ == "__main__":
    unittest.main()
