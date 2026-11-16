from __future__ import annotations

import io
import socket
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout

from client import Client
from protocol import encode_message
from tests.helpers import recv_message


class ClientDisplayTests(unittest.TestCase):
    def test_empty_rooms_and_joint_winner_output(self) -> None:
        client = Client("127.0.0.1", 12000, "u1", "Alice")
        rows = [{"user_id": "u1", "name": "Alice", "score": 0, "rank": 1},
                {"user_id": "u2", "name": "Bob", "score": 0, "rank": 1}]
        output = io.StringIO()
        with redirect_stdout(output):
            client._display({"type": "LIST_ROOMS_RESPONSE", "payload": {"rooms": []}})
            client._display({"type": "QUIZ_FINISHED_EVENT", "payload": {
                "winner": rows[0], "winners": rows, "final_leaderboard": rows}})
            legacy_rows = [dict(rows[0]), dict(rows[1], rank=2)]
            client._display({"type": "QUIZ_FINISHED_EVENT", "payload": {
                "winner": legacy_rows[0], "final_leaderboard": legacy_rows}})
            client._display({"type": "QUIZ_FINISHED_EVENT", "payload": {
                "winner": None, "winners": [], "final_leaderboard": []}})
            client._display({"type": "ANSWER_RESPONSE", "payload": {"accepted": True}})
        rendered = output.getvalue()
        self.assertIn("No joinable rooms.", rendered)
        self.assertIn("Joint Winners: Alice, Bob (0 points)", rendered)
        self.assertEqual(2, rendered.count("Joint Winners: Alice, Bob"))
        self.assertIn("Quiz finished with no remaining players.", rendered)
        self.assertIn("#1 Alice", rendered)
        self.assertIn("#1 Bob", rendered)
        self.assertIn("points are confirmed when the question closes", rendered)

    def test_async_event_output_is_atomic_with_prompt(self) -> None:
        client = Client("127.0.0.1", 12000, "u1", "Alice")
        client.sock, peer = socket.socketpair()
        client.sock.settimeout(0.05)
        client._reader = threading.Thread(target=client._receive_loop, daemon=True)
        output = io.StringIO()
        event = {"version": 1, "type": "PLAYER_STATUS_EVENT", "request_id": None,
                 "event_id": "evt-1", "session_id": "secret-token",
                 "payload": {"room_code": "AB12CD", "host_id": "u1", "players": [
                     {"user_id": "u1", "name": "Alice", "ready": True, "is_host": True}]}}
        try:
            with redirect_stdout(output):
                client._reader.start()
                client.prompt()
                with client._output_lock:
                    peer.sendall(encode_message(event, direction="event"))
                    time.sleep(0.05)
                    self.assertEqual("> ", output.getvalue())
                deadline = time.monotonic() + 1
                while "AB12CD" not in output.getvalue() and time.monotonic() < deadline:
                    time.sleep(0.001)
            rendered = output.getvalue()
            self.assertIn("\n[EVENT] Room AB12CD status", rendered)
            self.assertIn("Alice (u1) — Host, Ready\n> ", rendered)
            self.assertNotIn("secret-token", rendered)
        finally:
            peer.close()
            client.close()

    def _one_response_peer(self, response_type: str, payload: dict) -> tuple[socket.socket, threading.Thread]:
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)

        def serve_once() -> None:
            try:
                peer, _ = listener.accept()
                with peer:
                    request = recv_message(peer)
                    response = {
                        "version": 1,
                        "type": response_type,
                        "request_id": request["request_id"],
                        "event_id": None,
                        "session_id": None,
                        "payload": payload,
                    }
                    peer.sendall(encode_message(response, direction="response"))
            finally:
                listener.close()

        thread = threading.Thread(target=serve_once)
        thread.start()
        return listener, thread

    def test_login_session_token_is_not_printed_to_terminal(self) -> None:
        token = "SENSITIVE-SESSION-TOKEN-123"
        listener, peer_thread = self._one_response_peer(
            "LOGIN_RESPONSE", {"success": True, "session_id": token}
        )
        client = Client("127.0.0.1", listener.getsockname()[1], "u-token", "Token User")
        output, errors = io.StringIO(), io.StringIO()
        try:
            with redirect_stdout(output), redirect_stderr(errors):
                client.connect()
            self.assertEqual(token, client.session_id)
        finally:
            client.close()
            peer_thread.join(timeout=2)
        self.assertFalse(peer_thread.is_alive())
        self.assertIn("Logged in as Token User", output.getvalue())
        self.assertNotIn(token, output.getvalue())
        self.assertNotIn(token, errors.getvalue())
        self.assertNotIn("session_id", output.getvalue())

    def test_error_output_redacts_known_session_token(self) -> None:
        token = "SENSITIVE-SESSION-TOKEN-456"
        listener, peer_thread = self._one_response_peer(
            "ERROR_RESPONSE", {"code": "INVALID_SESSION", "message": f"Rejected {token}"}
        )
        client = Client("127.0.0.1", listener.getsockname()[1], "u-token", "Token User")
        client.sock = socket.create_connection(listener.getsockname())
        client.sock.settimeout(0.1)
        client.session_id = token
        client._reader = threading.Thread(target=client._receive_loop, daemon=True)
        client._reader.start()
        output, errors = io.StringIO(), io.StringIO()
        try:
            with redirect_stdout(output), redirect_stderr(errors):
                response = client.request("LEADERBOARD_REQUEST", {})
            self.assertEqual("ERROR_RESPONSE", response["type"])
        finally:
            client.close()
            peer_thread.join(timeout=2)
        self.assertFalse(peer_thread.is_alive())
        self.assertIn("INVALID_SESSION", output.getvalue())
        self.assertIn("[redacted]", output.getvalue())
        self.assertNotIn(token, output.getvalue())
        self.assertNotIn(token, errors.getvalue())

    def test_room_status_result_and_winner_are_readable_in_plain_terminal(self) -> None:
        client = Client("127.0.0.1", 12000, "u1", "Alice")
        output = io.StringIO()
        with redirect_stdout(output):
            client._display({"type": "PLAYER_STATUS_EVENT", "payload": {
                "room_code": "AB12CD", "host_id": "u1", "players": [
                    {"user_id": "u1", "name": "Alice", "ready": True, "is_host": True},
                    {"user_id": "u2", "name": "Bob", "ready": False, "is_host": False},
                ]}})
            client._display({"type": "QUESTION_RESULT_EVENT", "payload": {
                "question_id": "N01", "correct_answer": "B", "your_answer": "A",
                "points_awarded": 0, "leaderboard": [
                    {"rank": 1, "name": "Bob", "score": 100},
                    {"rank": 2, "name": "Alice", "score": 0},
                ]}})
            client._display({"type": "QUIZ_FINISHED_EVENT", "payload": {
                "winner": {"name": "Bob", "score": 100}, "final_leaderboard": [
                    {"rank": 1, "name": "Bob", "score": 100},
                    {"rank": 2, "name": "Alice", "score": 0},
                ]}})
        rendered = output.getvalue()
        for expected in ("AB12CD", "Host", "Ready", "Not ready", "Correct: B",
                         "Current leaderboard", "Final leaderboard", "Winner: Bob"):
            self.assertIn(expected, rendered)
        self.assertNotIn("\x1b[", rendered)
        self.assertNotIn("session_id", rendered)

    def test_argumentless_commands_reject_extra_arguments(self) -> None:
        client = Client("127.0.0.1", 12000, "u1", "Alice")
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertTrue(client.command("/ready extra"))
            self.assertTrue(client.command("/start extra"))
            self.assertTrue(client.command("/help extra"))
        self.assertIn("Usage: /ready", output.getvalue())
        self.assertIn("Usage: /start", output.getvalue())
        self.assertIn("Usage: /help", output.getvalue())

    def test_closed_server_wakes_pending_request_without_ten_second_wait(self) -> None:
        client = Client("127.0.0.1", 12000, "u1", "Alice")
        client.sock, peer = socket.socketpair()
        client.sock.settimeout(0.05)
        client._send = lambda *args, **kwargs: None
        client._reader = threading.Thread(target=client._receive_loop, daemon=True)
        client._reader.start()
        outcome: list[BaseException] = []

        def wait_for_response() -> None:
            try:
                client.request("LEADERBOARD_REQUEST", {})
            except BaseException as exc:
                outcome.append(exc)

        requester = threading.Thread(target=wait_for_response)
        requester.start()
        deadline = time.monotonic() + 1
        while not client._pending and time.monotonic() < deadline:
            time.sleep(0.001)
        peer.close()
        requester.join(timeout=1)
        client.close()
        self.assertFalse(requester.is_alive(), "pending request did not wake after connection close")
        self.assertEqual(1, len(outcome))
        self.assertIsInstance(outcome[0], ConnectionError)


if __name__ == "__main__":
    unittest.main()
