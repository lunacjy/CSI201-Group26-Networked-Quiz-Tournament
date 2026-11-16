from __future__ import annotations

import socket
import threading
from typing import Any

from protocol import FrameBuffer, decode_payload, encode_message
from question_bank import validate_bank
from server import QuizServer


def valid_bank_dict() -> dict[str, Any]:
    questions = []
    for index in range(5):
        questions.append(
            {
                "question_id": f"Q{index}",
                "text": f"Question {index}?",
                "options": {"A": "One", "B": "Two", "C": "Three", "D": "Four"},
                "answer": "A",
                "time_limit": 5,
                "points": 100,
            }
        )
    return {
        "schema_version": 1,
        "quizzes": [
            {
                "quiz_id": "quiz-1",
                "title": "Quiz One",
                "description": "Test quiz",
                "questions": questions,
            }
        ],
    }


def request(
    message_type: str,
    request_id: str,
    payload: dict[str, Any] | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    return {
        "version": 1,
        "type": message_type,
        "request_id": request_id,
        "event_id": None,
        "session_id": session_id,
        "payload": payload or {},
    }


def send_message(sock: socket.socket, message: dict[str, Any]) -> None:
    sock.sendall(encode_message(message, direction="request"))


def recv_message(sock: socket.socket) -> dict[str, Any]:
    header = recv_exact(sock, 4)
    length = int.from_bytes(header, "big")
    return decode_payload(recv_exact(sock, length))


def recv_exact(sock: socket.socket, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = sock.recv(length - len(chunks))
        if not chunk:
            raise EOFError("socket closed")
        chunks.extend(chunk)
    return bytes(chunks)


class RunningServer:
    def __init__(self, *, room_service: object | None = None, quiz_service: object | None = None):
        self.server = QuizServer(
            "127.0.0.1",
            0,
            validate_bank(valid_bank_dict()),
            room_service=room_service,
            quiz_service=quiz_service,
        )
        self.ready = threading.Event()
        self.thread = threading.Thread(
            target=self.server.serve_forever, args=(self.ready,), daemon=True
        )

    def __enter__(self) -> "RunningServer":
        self.thread.start()
        if not self.ready.wait(3):
            raise RuntimeError("test server did not become ready")
        return self

    def connect(self) -> socket.socket:
        sock = socket.create_connection(("127.0.0.1", self.server.bound_port), timeout=2)
        sock.settimeout(2)
        return sock

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.server.stop()
        self.thread.join(4)
        if self.thread.is_alive():
            raise RuntimeError("test server did not stop")


def login(sock: socket.socket, user_id: str, name: str, request_id: str = "login-1") -> str:
    send_message(
        sock,
        request("LOGIN_REQUEST", request_id, {"user_id": user_id, "name": name}),
    )
    response = recv_message(sock)
    if response["type"] != "LOGIN_RESPONSE":
        raise AssertionError(response)
    return response["payload"]["session_id"]
