"""TCP quiz server.

Validates the question bank before opening the listening socket.
Handles client connections, requests and game events.
"""

from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass, field
import logging
import queue
import secrets
import socket
import sys
import threading
from typing import Any, Hashable

from message_router import (
    MessageRouter,
    PendingEvent,
    PublishResult,
    RequestContext,
    ServiceResult,
    validate_request_payload,
    validate_pending_event,
)
from protocol import (
    EnvelopeError,
    FrameBuffer,
    FramingError,
    PayloadDecodeError,
    decode_payload,
    encode_message,
    make_error,
    make_event,
    make_response,
    validate_envelope,
)
from question_bank import QuestionBank, QuestionBankError, load_and_validate_bank
from quiz_engine import QuizEngine
from room_manager import RoomManager
from session_manager import DuplicateName, DuplicateUser, SessionError, SessionManager


LOG = logging.getLogger("csi201.server")
ACCEPT_TIMEOUT_SECONDS = 0.5
SOCKET_TIMEOUT_SECONDS = 1.0
LISTEN_BACKLOG = 64
MAX_CONNECTIONS = 128
REQUEST_ID_WINDOW = 1_024
EVENT_QUEUE_LIMIT = 64


@dataclass(slots=True)
class ConnectionState:
    sock: socket.socket
    address: tuple[Any, ...]
    handle: Hashable = field(default_factory=object)
    frames: FrameBuffer = field(default_factory=FrameBuffer)
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    cleanup_lock: threading.Lock = field(default_factory=threading.Lock)
    session_id: str | None = None
    closed: bool = False
    _request_order: deque[str] = field(default_factory=deque)
    _request_ids: set[str] = field(default_factory=set)
    event_outbox: queue.Queue[tuple[bytes, threading.Event | None] | None] = field(
        default_factory=lambda: queue.Queue(maxsize=EVENT_QUEUE_LIMIT), repr=False
    )
    event_sender: threading.Thread | None = field(default=None, repr=False)

    def record_request(self, request_id: str) -> bool:
        """Return False for a duplicate within the documented bounded window."""
        if request_id in self._request_ids:
            return False
        self._request_ids.add(request_id)
        self._request_order.append(request_id)
        if len(self._request_order) > REQUEST_ID_WINDOW:
            expired = self._request_order.popleft()
            self._request_ids.discard(expired)
        return True


class QuizServer:
    def __init__(
        self,
        host: str,
        port: int,
        question_bank: QuestionBank,
        *,
        room_service: object | None = None,
        quiz_service: object | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.question_bank = question_bank
        self.sessions = SessionManager()
        self.router = MessageRouter(room_service, quiz_service)
        self._listener: socket.socket | None = None
        self._stop_event = threading.Event()
        self._connections_lock = threading.RLock()
        self._connections: dict[Hashable, ConnectionState] = {}
        self._workers: set[threading.Thread] = set()
        self._event_senders: set[threading.Thread] = set()
        self._event_lock = threading.Lock()
        self._event_counter = 0
        self.bound_port: int | None = None
        self.event_sink = ServerEventSink(self)

    def serve_forever(self, ready_event: threading.Event | None = None) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.host, self.port))
            listener.listen(LISTEN_BACKLOG)
            listener.settimeout(ACCEPT_TIMEOUT_SECONDS)
            self._listener = listener
            self.bound_port = listener.getsockname()[1]
            print("SERVER_READY", flush=True)
            if ready_event is not None:
                ready_event.set()
            while not self._stop_event.is_set():
                try:
                    client, address = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self._stop_event.is_set():
                        break
                    raise
                client.settimeout(SOCKET_TIMEOUT_SECONDS)
                state = ConnectionState(client, address)
                with self._connections_lock:
                    if len(self._connections) >= MAX_CONNECTIONS:
                        client.close()
                        LOG.warning("connection limit reached; rejected one client")
                        continue
                    self._connections[state.handle] = state
                self._start_event_sender(state)
                worker = threading.Thread(
                    target=self._connection_worker,
                    args=(state,),
                    name="client-worker",
                    daemon=True,
                )
                with self._connections_lock:
                    self._workers.add(worker)
                worker.start()
        finally:
            self.stop()
            # Also covers bind/listen failure before self._listener is assigned.
            try:
                listener.close()
            except OSError:
                pass
            self._join_workers()

    def stop(self) -> None:
        if self._stop_event.is_set() and self._listener is None:
            return
        self._stop_event.set()
        listener, self._listener = self._listener, None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        with self._connections_lock:
            states = tuple(self._connections.values())
        for state in states:
            self._close_socket(state)

    def _join_workers(self) -> None:
        current = threading.current_thread()
        with self._connections_lock:
            workers = tuple(self._workers)
        for worker in workers:
            if worker is not current:
                worker.join(timeout=2.0)
        with self._connections_lock:
            senders = tuple(self._event_senders)
        for sender in senders:
            if sender is not current:
                sender.join(timeout=2.0)

    def _start_event_sender(self, state: ConnectionState) -> None:
        sender = threading.Thread(
            target=self._event_sender_worker,
            args=(state,),
            name="event-sender",
            daemon=True,
        )
        state.event_sender = sender
        with self._connections_lock:
            self._event_senders.add(sender)
        sender.start()

    def _event_sender_worker(self, state: ConnectionState) -> None:
        try:
            while True:
                try:
                    item = state.event_outbox.get(timeout=0.25)
                except queue.Empty:
                    if state.closed or self._stop_event.is_set():
                        return
                    continue
                try:
                    if item is None or state.closed or self._stop_event.is_set():
                        return
                    frame, departed = item
                    with state.send_lock:
                        if departed is None or not departed.is_set():
                            state.sock.sendall(frame)
                except OSError:
                    self._close_socket(state)
                    return
                finally:
                    state.event_outbox.task_done()
        finally:
            with self._connections_lock:
                self._event_senders.discard(threading.current_thread())

    def _connection_worker(self, state: ConnectionState) -> None:
        try:
            keep_running = True
            while keep_running and not self._stop_event.is_set():
                try:
                    data = state.sock.recv(16_384)
                except socket.timeout:
                    continue
                except OSError:
                    break
                if not data:
                    try:
                        state.frames.end_of_stream()
                    except FramingError:
                        LOG.warning("client disconnected with an incomplete frame")
                    break
                try:
                    payloads = state.frames.feed(data)
                except FramingError as exc:
                    # Completed frames were removed from the buffer before the
                    # fatal header was observed.  Dispatch each once, then close
                    # this connection because framing is no longer recoverable.
                    for raw_payload in exc.completed_frames:
                        if not self._process_payload(state, raw_payload):
                            keep_running = False
                            break
                    LOG.warning("closing one client after an invalid frame length")
                    break
                for raw_payload in payloads:
                    if not self._process_payload(state, raw_payload):
                        keep_running = False
                        break
        finally:
            self._cleanup_connection(state)
            with self._connections_lock:
                self._workers.discard(threading.current_thread())

    def _process_payload(self, state: ConnectionState, raw_payload: bytes) -> bool:
        try:
            message = decode_payload(raw_payload)
        except PayloadDecodeError as exc:
            return self._send_error(state, None, "INVALID_MESSAGE", str(exc))
        try:
            validate_envelope(message, direction="request")
        except EnvelopeError as exc:
            return self._send_error(state, exc.request_id, "INVALID_MESSAGE", str(exc))

        request_id = message["request_id"]
        if not state.record_request(request_id):
            return self._send_error(
                state, request_id, "DUPLICATE_REQUEST", "Request ID was already used."
            )
        message_type = message["type"]
        payload_error = validate_request_payload(message_type, message["payload"])
        if payload_error is not None:
            return self._send_error(state, request_id, "INVALID_MESSAGE", payload_error)

        if message_type == "LOGIN_REQUEST":
            return self._handle_login(state, message)

        try:
            session = self.sessions.authenticate(message["session_id"], state.handle)
        except SessionError:
            return self._send_error(
                state, request_id, "INVALID_SESSION", "Session is not valid for this connection."
            )

        if message_type == "LOGOUT_REQUEST":
            sent = self._send_message(
                state,
                make_response("LOGOUT_RESPONSE", request_id, session.session_id, {"success": True}),
            )
            return False if sent else False

        ctx = RequestContext(
            session.session_id,
            session.user_id,
            session.name,
            state.handle,
            request_id,
            defer_room_events=True,
        )
        result = self.router.dispatch(message_type, ctx, message["payload"])
        return self._deliver_result(state, session.session_id, request_id, result)

    def _handle_login(self, state: ConnectionState, message: dict[str, Any]) -> bool:
        request_id = message["request_id"]
        if message["session_id"] is not None or state.session_id is not None:
            return self._send_error(
                state, request_id, "INVALID_SESSION", "Login requires an unauthenticated connection."
            )
        payload = message["payload"]
        try:
            session = self.sessions.login(payload["user_id"], payload["name"], state.handle)
        except (DuplicateUser, DuplicateName) as exc:
            return self._send_error(state, request_id, exc.code, str(exc))
        except SessionError as exc:
            return self._send_error(state, request_id, exc.code, str(exc))
        state.session_id = session.session_id
        response = make_response(
            "LOGIN_RESPONSE",
            request_id,
            None,
            {"success": True, "session_id": session.session_id},
        )
        return self._send_message(state, response)

    def _deliver_result(
        self,
        state: ConnectionState,
        session_id: str,
        request_id: str,
        result: ServiceResult,
    ) -> bool:
        if result.error_code is not None:
            return self._send_error(
                state, request_id, result.error_code, result.error_message or "Request failed."
            )
        response = make_response(
            result.response_type or "ERROR_RESPONSE",
            request_id,
            session_id,
            result.response_payload or {},
        )
        sent = self._send_message(state, response)
        if result.release_events is not None:
            result.release_events()
        else:
            for event in result.events:
                self.send_event(event)
        return sent

    def _send_error(
        self,
        state: ConnectionState,
        request_id: str | None,
        code: str,
        human_message: str,
    ) -> bool:
        return self._send_message(
            state, make_error(request_id, state.session_id, code, human_message)
        )

    def _send_message(self, state: ConnectionState, message: dict[str, Any]) -> bool:
        try:
            frame = encode_message(message)
            with state.send_lock:
                state.sock.sendall(frame)
            return True
        except (OSError, ValueError):
            self._close_socket(state)
            return False

    def send_event(self, event: PendingEvent) -> int:
        """Queue an event only for sessions resolved from the trusted registry.

        The return value is the number of live recipient queues that accepted
        the event, not a TCP delivery acknowledgement.
        """
        if not validate_pending_event(event):
            LOG.error("business service produced an invalid event")
            return 0
        delivered = 0
        for session_id in tuple(dict.fromkeys(event.recipients)):
            departed = event.departed.get(session_id)
            if departed is not None and departed.is_set():
                continue
            session = self.sessions.get(session_id)
            if session is None:
                continue
            with self._connections_lock:
                state = self._connections.get(session.connection_handle)
            if state is None:
                continue
            message = make_event(
                event.event_type, self._next_event_id(), session_id, dict(event.payload)
            )
            if self._enqueue_event(state, message, departed):
                delivered += 1
        return delivered

    def _enqueue_event(self, state: ConnectionState, message: dict[str, Any],
                       departed: threading.Event | None = None) -> bool:
        try:
            frame = encode_message(message)
        except ValueError:
            LOG.error("business service produced an event exceeding protocol limits")
            return False
        close_slow_client = False
        with state.cleanup_lock:
            if state.closed or self._stop_event.is_set() or (departed is not None and departed.is_set()):
                return False
            try:
                state.event_outbox.put_nowait((frame, departed))
            except queue.Full:
                close_slow_client = True
        if close_slow_client:
            LOG.warning("outgoing event queue full; closing one slow client")
            self._close_socket(state)
            return False
        return True

    def publish(self, event: PendingEvent) -> PublishResult:
        """Publish timer and state-transition events from application services."""
        if self._stop_event.is_set():
            return PublishResult(False, 0, "SERVER_STOPPING")
        if not validate_pending_event(event):
            LOG.error("business service attempted to publish an invalid event")
            return PublishResult(False, 0, "INVALID_EVENT")
        return PublishResult(True, self.send_event(event))

    def _next_event_id(self) -> str:
        with self._event_lock:
            self._event_counter += 1
            counter = self._event_counter
        return f"evt-{counter}-{secrets.token_hex(6)}"

    def _close_socket(self, state: ConnectionState) -> None:
        try:
            state.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            state.sock.close()
        except OSError:
            pass

    def _cleanup_connection(self, state: ConnectionState) -> None:
        with state.cleanup_lock:
            if state.closed:
                return
            state.closed = True
            try:
                state.event_outbox.put_nowait(None)
            except queue.Full:
                # Socket closure below unblocks an in-flight sender; queued
                # events are deliberately abandoned during cleanup.
                pass
        self._close_socket(state)
        with self._connections_lock:
            self._connections.pop(state.handle, None)
        session = self.sessions.remove_by_connection(state.handle)
        if session is not None:
            for event in self.router.disconnect_events(session.session_id):
                self.send_event(event)


class ServerEventSink:
    """Forward service events without exposing client sockets."""

    def __init__(self, server: QuizServer) -> None:
        self._server = server

    def publish(self, event: PendingEvent) -> PublishResult:
        return self._server.publish(event)


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not 1 <= port <= 65_535:
        raise argparse.ArgumentTypeError("port must be in 1-65535")
    return port


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CSI201 Networked Quiz server")
    parser.add_argument("--host", required=True, help="IPv4 address or hostname to bind")
    parser.add_argument("--port", required=True, type=_port, help="TCP port in 1-65535")
    parser.add_argument("--questions", required=True, help="path to the JSON question bank")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        question_bank = load_and_validate_bank(args.questions)
    except QuestionBankError as exc:
        print(f"QUESTION_BANK_ERROR: {exc}", file=sys.stderr)
        return 2
    server = QuizServer(args.host, args.port, question_bank)
    quiz_engine = QuizEngine(question_bank, server.event_sink)
    room_manager = RoomManager(quiz_engine)
    quiz_engine.bind_room_manager(room_manager)
    server.router = MessageRouter(room_manager, quiz_engine)
    try:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("SERVER_STOPPING", file=sys.stderr, flush=True)
            server.stop()
            return 0
        except OSError as exc:
            print(f"SERVER_ERROR: unable to listen or accept: {exc}", file=sys.stderr)
            server.stop()
            return 3
        return 0
    finally:
        # `serve_forever()` may return normally when stopped programmatically.
        # Business timers and room/session indexes must be released on every exit path.
        server.stop()
        room_manager.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
