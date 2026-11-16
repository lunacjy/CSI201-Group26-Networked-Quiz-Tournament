"""Real server and CLI OS processes complete a tied, mixed-score tournament."""
from __future__ import annotations

import json
from pathlib import Path
import re
import select
import signal
import socket
import subprocess
import sys
import threading
import time
import unittest


class ProcessClient:
    def __init__(self, project: Path, port: int, index: int, answers: list[str]):
        self.index = index
        self.answers = answers
        self.log: list[str] = []
        self.sent: set[int] = set()
        self._lock = threading.Lock()
        self.process = subprocess.Popen(
            [sys.executable, str(project / "client.py"), "--server", "127.0.0.1",
             "--port", str(port), "--id", f"tie-{index}", "--name", f"Player {index}"],
            cwd=project, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self) -> None:
        for raw in self.process.stdout:
            line = raw.rstrip()
            with self._lock:
                self.log.append(line)
            match = re.search(r"Question (\d+):", line)
            if match:
                number = int(match.group(1))
                if number not in self.sent:
                    self.sent.add(number)
                    self.command(f"/answer {self.answers[number - 1]}")
            if self.index == 0 and "Question NB01 closed." in line:
                self.command("/score")

    def command(self, line: str) -> None:
        if self.process.poll() is None:
            self.process.stdin.write(line + "\n")
            self.process.stdin.flush()

    def contains(self, text: str) -> bool:
        with self._lock:
            return any(text in line for line in self.log)

    def wait(self, text: str, timeout: float = 8) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                for line in self.log:
                    if text in line:
                        return line
            time.sleep(0.01)
        raise AssertionError(f"client {self.index} missing {text}; tail={self.log[-10:]}")

    def close(self) -> None:
        if self.process.poll() is None:
            self.command("/quit")
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                self.process.wait(timeout=3)
        self.reader.join(timeout=2)
        self.process.stdin.close()
        self.process.stdout.close()


class CliTieProcessTests(unittest.TestCase):
    def test_three_real_cli_processes_finish_with_joint_winners(self) -> None:
        project = Path(__file__).resolve().parents[1]
        bank = json.loads((project / "questions.json").read_text())
        quiz = bank["quizzes"][0]
        correct = [q["answer"] for q in quiz["questions"]]
        wrong = [next(answer for answer in "ABCD" if answer != q["answer"])
                 for q in quiz["questions"]]
        total = sum(q["points"] for q in quiz["questions"])
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = subprocess.Popen(
            [sys.executable, str(project / "server.py"), "--host", "127.0.0.1",
             "--port", str(port), "--questions", str(project / "questions.json")],
            cwd=project, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1)
        clients: list[ProcessClient] = []
        try:
            readable, _, _ = select.select([server.stdout], [], [], 5)
            self.assertTrue(readable)
            self.assertEqual("SERVER_READY", server.stdout.readline().strip())
            clients = [ProcessClient(project, port, i, correct if i < 2 else wrong)
                       for i in range(3)]
            for client in clients:
                client.wait("Logged in as")
            clients[0].command("/create " + quiz["quiz_id"])
            created = clients[0].wait("Room created:")
            room_code = created.split("Room created:", 1)[1].strip()
            for client in clients[1:]:
                client.command("/join " + room_code)
                client.wait("Joined room")
            for client in clients:
                client.command("/ready")
                client.wait("ready': True")
            clients[0].command("/start")
            for client in clients:
                client.wait("Quiz finished.", timeout=25)
            clients[0].wait("Leaderboard:", timeout=3)
            with clients[0]._lock:
                previous_views = sum("Leaderboard:" in line for line in clients[0].log)
            clients[0].command("/score")
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                with clients[0]._lock:
                    views = sum("Leaderboard:" in line for line in clients[0].log)
                if views > previous_views:
                    break
                time.sleep(0.01)
            self.assertGreater(views, previous_views)
            self.assertEqual([1, 2, 3, 4, 5], sorted(clients[0].sent))
            for client in clients:
                self.assertIn("Joint Winners: Player 0, Player 1", client.wait("Joint Winners:"))
                self.assertTrue(client.contains(f"#1 Player 0: {total}"))
                self.assertTrue(client.contains(f"#1 Player 1: {total}"))
                self.assertTrue(client.contains("#3 Player 2: 0"))
                self.assertFalse(client.contains("session_id"))
        finally:
            for client in clients:
                client.close()
            if server.poll() is None:
                server.send_signal(signal.SIGINT)
            _, error = server.communicate(timeout=5)
            self.assertEqual(0, server.returncode, error)
            with socket.socket() as released:
                released.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                released.bind(("127.0.0.1", port))


if __name__ == "__main__":
    unittest.main()
