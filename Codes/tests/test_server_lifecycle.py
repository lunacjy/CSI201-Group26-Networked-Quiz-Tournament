from __future__ import annotations

import socket
import subprocess
import sys
import tempfile
from pathlib import Path
import json
import signal
import threading
import time
import unittest

from tests.helpers import RunningServer, login, recv_message, request, send_message, valid_bank_dict


class ServerLifecycleTests(unittest.TestCase):
    def test_login_duplicate_identity_and_disconnect_cleanup(self) -> None:
        with RunningServer() as running:
            first = running.connect()
            second = running.connect()
            session = login(first, "u1", "Alice")
            send_message(second, request("LOGIN_REQUEST", "login-2", {"user_id": "u1", "name": "Bob"}))
            self.assertEqual("DUPLICATE_USER", recv_message(second)["payload"]["code"])
            first.close()
            deadline = time.monotonic() + 2
            while len(running.server.sessions) and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(0, len(running.server.sessions))
            self.assertIsNone(running.server.sessions.get(session))
            self.assertTrue(login(second, "u1", "Alice", "login-3"))
            second.close()

    def test_session_spoof_and_duplicate_request(self) -> None:
        with RunningServer() as running:
            first = running.connect()
            second = running.connect()
            first_session = login(first, "u1", "Alice")
            second_session = login(second, "u2", "Bob")
            send_message(second, request("LIST_ROOMS_REQUEST", "r2", session_id=first_session))
            self.assertEqual("INVALID_SESSION", recv_message(second)["payload"]["code"])
            send_message(second, request("LIST_ROOMS_REQUEST", "r2", session_id=second_session))
            self.assertEqual("DUPLICATE_REQUEST", recv_message(second)["payload"]["code"])
            first.close()
            second.close()

    def test_logout_ack_then_connection_closes(self) -> None:
        with RunningServer() as running:
            sock = running.connect()
            session = login(sock, "u1", "Alice")
            send_message(sock, request("LOGOUT_REQUEST", "logout-1", session_id=session))
            self.assertEqual("LOGOUT_RESPONSE", recv_message(sock)["type"])
            self.assertEqual(b"", sock.recv(1))
            sock.close()

    def test_slow_partial_client_does_not_block_other_login(self) -> None:
        with RunningServer() as running:
            slow = running.connect()
            slow.sendall(b"\x00\x00")
            normal = running.connect()
            self.assertTrue(login(normal, "u1", "Alice"))
            slow.close()
            normal.close()

    def test_ten_concurrent_clients_can_login(self) -> None:
        with RunningServer() as running:
            barrier = threading.Barrier(10)
            outcomes: list[bool] = []
            sockets: list[socket.socket] = []
            lock = threading.Lock()

            def connect(index: int) -> None:
                sock = running.connect()
                barrier.wait()
                session = login(sock, f"u{index}", f"Name {index}")
                with lock:
                    sockets.append(sock)
                    outcomes.append(bool(session))

            threads = [threading.Thread(target=connect, args=(index,)) for index in range(10)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(4)
            self.assertEqual([True] * 10, sorted(outcomes))
            self.assertEqual(10, len(running.server.sessions))
            for sock in sockets:
                sock.close()

    def test_stop_releases_bound_port(self) -> None:
        running = RunningServer()
        with running:
            port = running.server.bound_port
        replacement = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            replacement.bind(("127.0.0.1", port))
        finally:
            replacement.close()

    def test_invalid_bank_cli_exits_without_ready(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "invalid.json")
            path.write_text("{}", encoding="utf-8")
            completed = subprocess.run(
                [
                    sys.executable,
                    "server.py",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "54321",
                    "--questions",
                    str(path),
                ],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        self.assertNotEqual(0, completed.returncode)
        self.assertNotIn("SERVER_READY", completed.stdout)
        self.assertIn("QUESTION_BANK_ERROR", completed.stderr)

    def test_valid_cli_prints_ready_and_handles_sigint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "valid.json")
            path.write_text(json.dumps(valid_bank_dict()), encoding="utf-8")
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
            probe.close()
            process = subprocess.Popen(
                [
                    sys.executable,
                    "server.py",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--questions",
                    str(path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertIsNotNone(process.stdout)
            self.assertEqual("SERVER_READY", process.stdout.readline().strip())
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=5)
        self.assertEqual(0, process.returncode, (stdout, stderr))
        self.assertIn("SERVER_STOPPING", stderr)
        replacement = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            replacement.bind(("127.0.0.1", port))
        finally:
            replacement.close()


if __name__ == "__main__":
    unittest.main()
