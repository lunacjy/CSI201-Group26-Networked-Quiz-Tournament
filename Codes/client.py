"""Command-line client for the quiz tournament."""
from __future__ import annotations

import argparse
import queue
import socket
import sys
import threading
import uuid

from protocol import FrameBuffer, FramingError, ProtocolError, decode_payload, encode_message


# Keep printable Unicode and grapheme joiners intact. Escape terminal controls,
# Unicode line/paragraph separators and explicit bidirectional controls.
_OPTION_DISPLAY_ESCAPES = {
    code: f"\\x{code:02x}" for code in (*range(0x20), *range(0x7f, 0xa0))
}
_OPTION_DISPLAY_ESCAPES.update({
    code: f"\\u{code:04x}"
    for code in (0x061c, 0x200e, 0x200f, *range(0x2028, 0x202f), *range(0x2066, 0x206a))
})


class Client:
    def __init__(self, host: str, port: int, user_id: str, name: str):
        self.host, self.port, self.user_id, self.name = host, port, user_id, name
        self.sock: socket.socket | None = None
        self.session_id: str | None = None
        self._send_lock = threading.Lock()
        self._output_lock = threading.RLock()
        self._pending: dict[str, queue.Queue[dict]] = {}
        self._pending_lock = threading.Lock()
        self._stop = threading.Event()
        self._reader: threading.Thread | None = None

    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=5)
        self.sock.settimeout(1)
        self._reader = threading.Thread(target=self._receive_loop, daemon=True)
        self._reader.start()
        response = self.request("LOGIN_REQUEST", {"user_id": self.user_id, "name": self.name}, login=True)
        if response.get("type") != "LOGIN_RESPONSE" or not response.get("payload", {}).get("success"):
            payload = response.get("payload", {})
            raise ValueError(f"login rejected: {payload.get('code', response.get('type', 'unknown'))}")
        self.session_id = response["payload"]["session_id"]
        self._print(f"Logged in as {self.name}. Type /help.")

    def _print(self, *args: object, **kwargs: object) -> None:
        with self._output_lock:
            print(*args, **kwargs)

    def prompt(self) -> None:
        with self._output_lock:
            print("> ", end="", flush=True)

    def _send(self, message_type: str, payload: dict, request_id: str, login: bool = False) -> None:
        msg = {"version": 1, "type": message_type, "request_id": request_id,
               "event_id": None, "session_id": None if login else self.session_id,
               "payload": payload}
        frame = encode_message(msg, direction="request")
        with self._send_lock:
            assert self.sock
            self.sock.sendall(frame)

    def request(self, message_type: str, payload: dict | None = None, login: bool = False) -> dict:
        request_id = uuid.uuid4().hex
        waiter: queue.Queue[dict] = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[request_id] = waiter
        try:
            self._send(message_type, payload or {}, request_id, login)
            response = waiter.get(timeout=10)
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)
        if response.get("type") == "_CLIENT_CONNECTION_ERROR":
            raise ConnectionError(response.get("message", "server connection closed"))
        if response["type"] == "ERROR_RESPONSE":
            safe_payload = self._safe_display_value(response.get("payload", {}))
            self._print(f"{safe_payload.get('code')}: {safe_payload.get('message')}")
        else:
            self._display(response)
        return response

    def _safe_display_value(self, value: object) -> object:
        """Remove credential fields and redact this client's token before display."""
        sensitive_names = {"session_id", "session_token", "token"}
        if isinstance(value, dict):
            return {
                key: "[redacted]" if key.lower() in sensitive_names
                else self._safe_display_value(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._safe_display_value(item) for item in value]
        if isinstance(value, str) and self.session_id:
            return value.replace(self.session_id, "[redacted]")
        return value

    def _receive_loop(self) -> None:
        buffer = FrameBuffer()
        try:
            while not self._stop.is_set():
                try:
                    data = self.sock.recv(16_384) if self.sock else b""
                except socket.timeout:
                    continue
                if not data:
                    buffer.end_of_stream()
                    break
                for raw in buffer.feed(data):
                    message = decode_payload(raw)
                    if message["type"].endswith("_EVENT"):
                        with self._output_lock:
                            print("\n[EVENT]", end=" ")
                            self._display(message)
                            print("> ", end="", flush=True)
                    else:
                        rid = message.get("request_id")
                        with self._pending_lock:
                            waiter = self._pending.get(rid)
                        if waiter:
                            waiter.put(message)
        except (OSError, FramingError, ValueError) as exc:
            if not self._stop.is_set():
                self._print(f"Connection closed: {exc}", file=sys.stderr)
        finally:
            self._wake_pending("server connection closed")
            self._stop.set()

    def _wake_pending(self, reason: str) -> None:
        message = {"type": "_CLIENT_CONNECTION_ERROR", "message": reason}
        with self._pending_lock:
            waiters = tuple(self._pending.values())
        for waiter in waiters:
            try:
                waiter.put_nowait(message)
            except queue.Full:
                pass

    def _display(self, msg: dict) -> None:
        with self._output_lock:
            self._display_unlocked(msg)

    def _display_unlocked(self, msg: dict) -> None:
        typ, p = msg["type"], msg["payload"]
        if typ == "LOGIN_RESPONSE":
            print("Login successful." if p.get("success") else "Login response received.")
        elif typ == "LIST_QUIZZES_RESPONSE":
            for q in p.get("quizzes", []): print(f"{q['quiz_id']}: {q['title']} ({q['question_count']} questions) - {q['description']}")
        elif typ == "LIST_ROOMS_RESPONSE":
            rooms = p.get("rooms", [])
            if not rooms: print("No joinable rooms.")
            for r in rooms: print(f"{r['room_code']} quiz={r['quiz_id']} players={r['player_count']}/{r['max_players']} host={r['host_id']}")
        elif typ == "PLAYER_LIST_RESPONSE":
            for pl in p.get("players", []): print(f"{pl['name']} ({pl['user_id']}) {'ready' if pl['ready'] else 'not ready'}{' [host]' if pl['is_host'] else ''}")
        elif typ == "QUESTION_EVENT":
            self.current_question_id = p["question_id"]
            print(f"Question {p['number']}: {p['text']} [{p['time_limit']}s, {p['points']} points]")
            for key, value in p["options"].items():
                print(f"  {key}. {value.translate(_OPTION_DISPLAY_ESCAPES)}")
            print("Answer with /answer A|B|C|D")
        elif typ == "QUESTION_RESULT_EVENT":
            if getattr(self, "current_question_id", None) == p["question_id"]:
                self.current_question_id = None
            print(f"Question {p['question_id']} closed. Correct: {p['correct_answer']}; your answer: {p['your_answer']}; points: {p['points_awarded']}")
            self._display_leaderboard(p.get("leaderboard", []), "Current leaderboard")
        elif typ in ("QUIZ_FINISHED_EVENT", "LEADERBOARD_RESPONSE"):
            rows = p.get("final_leaderboard", p.get("leaderboard", []))
            if typ == "QUIZ_FINISHED_EVENT":
                winners = p.get("winners")
                if winners is None:
                    winners = [row for row in rows if rows and row["score"] == rows[0]["score"]]
                    if not winners and p.get("winner"):
                        winners = [p["winner"]]
                if len(winners) > 1:
                    print(f"Quiz finished. Joint Winners: {', '.join(row['name'] for row in winners)} ({winners[0]['score']} points).")
                elif winners:
                    print(f"Quiz finished. Winner: {winners[0]['name']} ({winners[0]['score']} points).")
                else:
                    print("Quiz finished with no remaining players.")
            self._display_leaderboard(rows, "Final leaderboard" if typ == "QUIZ_FINISHED_EVENT" else "Leaderboard")
        elif typ == "PLAYER_STATUS_EVENT":
            print(f"Room {p['room_code']} status. Host: {p.get('host_id', 'unknown')}")
            for player in p.get("players", []):
                flags = ["Host"] if player.get("is_host") else []
                flags.append("Ready" if player.get("ready") else "Not ready")
                print(f"  {player['name']} ({player['user_id']}) — {', '.join(flags)}")
        elif typ == "ROOM_CLOSED_EVENT": print(f"Room {p['room_code']} closed ({p['reason']})")
        elif typ == "QUIZ_STARTED_EVENT": print(f"Quiz started: {p['title']} ({p['total_questions']} questions)")
        elif typ == "CREATE_ROOM_RESPONSE": print(f"Room created: {p['room_code']}")
        elif typ == "JOIN_ROOM_RESPONSE": print(f"Joined room {p['room']['room_code']}")
        elif typ == "LEAVE_ROOM_RESPONSE":
            self.current_question_id = None
            print("OK", self._safe_display_value(p))
        elif typ == "ANSWER_RESPONSE": print("Answer accepted; points are confirmed when the question closes.")
        elif typ.endswith("_RESPONSE"):
            print("OK", self._safe_display_value(p))

    @staticmethod
    def _display_leaderboard(rows: list[dict], title: str) -> None:
        print(title + ":")
        for row in rows:
            print(f"#{row['rank']} {row['name']}: {row['score']}")

    def command(self, line: str) -> bool:
        parts = line.strip().split()
        if not parts: return True
        cmd = parts[0].lower()
        if cmd == "/help":
            if len(parts) != 1:
                self._print("Usage: /help")
                return True
            self._print("/quizzes /rooms /create <quiz_id> /join <room_code> /players /ready /unready /start /answer <A|B|C|D> /score /leave /quit")
        elif cmd == "/quit":
            if len(parts) != 1:
                self._print("Usage: /quit")
                return True
            try: self.request("LOGOUT_REQUEST", {})
            except Exception: pass
            return False
        else:
            mapping = {"/quizzes": ("LIST_QUIZZES_REQUEST", {}), "/rooms": ("LIST_ROOMS_REQUEST", {}),
                       "/players": ("PLAYER_LIST_REQUEST", {}), "/ready": ("READY_REQUEST", {"ready": True}),
                       "/unready": ("READY_REQUEST", {"ready": False}), "/start": ("START_QUIZ_REQUEST", {}),
                       "/score": ("LEADERBOARD_REQUEST", {}), "/leave": ("LEAVE_ROOM_REQUEST", {})}
            if cmd in ("/create", "/join"):
                if len(parts) != 2: self._print(f"Usage: {cmd} <value>"); return True
                typ = "CREATE_ROOM_REQUEST" if cmd == "/create" else "JOIN_ROOM_REQUEST"
                key = "quiz_id" if cmd == "/create" else "room_code"
                payload = {key: parts[1]}
            elif cmd == "/answer":
                if len(parts) != 2 or parts[1].upper() not in "ABCD" or len(parts[1]) != 1:
                    self._print("Usage: /answer A|B|C|D"); return True
                qid = getattr(self, "current_question_id", None)
                if not qid: self._print("No active question received yet."); return True
                typ, payload = "ANSWER_REQUEST", {"question_id": qid, "answer": parts[1].upper()}
            elif cmd in mapping:
                if len(parts) != 1:
                    self._print(f"Usage: {cmd}")
                    return True
                typ, payload = mapping[cmd]
            else: self._print("Unknown command. Type /help."); return True
            try: self.request(typ, payload)
            except (OSError, queue.Empty, ProtocolError) as exc:
                self._print(f"Request failed: {exc}")
        return True

    def close(self) -> None:
        self._stop.set()
        if self.sock:
            try: self.sock.shutdown(socket.SHUT_RDWR)
            except OSError: pass
            self.sock.close()
        if self._reader and self._reader is not threading.current_thread(): self._reader.join(timeout=2)


def main(argv: list[str] | None = None) -> int:
    def port_number(value: str) -> int:
        try:
            port = int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("port must be an integer") from exc
        if not 1 <= port <= 65_535:
            raise argparse.ArgumentTypeError("port must be in 1-65535")
        return port

    parser = argparse.ArgumentParser(description="CSI201 quiz tournament CLI client")
    parser.add_argument("--server", required=True)
    parser.add_argument("--port", required=True, type=port_number)
    parser.add_argument("--id", required=True, dest="user_id")
    parser.add_argument("--name", required=True)
    args = parser.parse_args(argv)
    client = Client(args.server, args.port, args.user_id, args.name)
    try:
        client.connect()
        while not client._stop.is_set():
            client.prompt()
            try: line = input()
            except EOFError: break
            if not client.command(line): break
    except (OSError, TimeoutError, queue.Empty, ValueError) as exc:
        client._print(f"Unable to connect or log in: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        client._print("\nLeaving...")
    finally:
        client.close()
    return 0


if __name__ == "__main__": raise SystemExit(main())
