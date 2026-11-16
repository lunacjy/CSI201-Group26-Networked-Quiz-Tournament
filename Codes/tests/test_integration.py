from __future__ import annotations

import socket
import threading
import time
import unittest

from message_router import PendingEvent, RequestContext, ServiceResult
from server import EVENT_QUEUE_LIMIT
from tests.helpers import RunningServer, login, recv_message, request, send_message


class EventRoomService:
    def __init__(self) -> None:
        self.recipients: tuple[str, ...] = ()
        self.disconnects: list[str] = []

    def list_rooms(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        event = PendingEvent(
            "PLAYER_STATUS_EVENT",
            self.recipients,
            {"room_code": "ROOM-A", "host_id": "u1", "players": []},
        )
        return ServiceResult.success("LIST_ROOMS_RESPONSE", {"rooms": []}, (event,))

    def on_disconnect(self, session_id: str) -> tuple[PendingEvent, ...]:
        self.disconnects.append(session_id)
        return ()


class IntegrationScaffoldTests(unittest.TestCase):
    def test_request_id_event_id_and_trusted_room_isolation(self) -> None:
        room = EventRoomService()
        with RunningServer(room_service=room) as running:
            first = running.connect()
            second = running.connect()
            outsider = running.connect()
            first_session = login(first, "u1", "Alice", "login-a")
            second_session = login(second, "u2", "Bob", "login-b")
            login(outsider, "u3", "Cara", "login-c")
            room.recipients = (first_session, second_session)

            send_message(first, request("LIST_ROOMS_REQUEST", "rooms-1", session_id=first_session))
            response = recv_message(first)
            self.assertEqual("rooms-1", response["request_id"])
            first_event = recv_message(first)
            second_event = recv_message(second)
            self.assertEqual("PLAYER_STATUS_EVENT", first_event["type"])
            self.assertIsNone(first_event["request_id"])
            self.assertTrue(first_event["event_id"])
            self.assertNotEqual(first_event["event_id"], second_event["event_id"])
            outsider.settimeout(0.2)
            with self.assertRaises(socket.timeout):
                outsider.recv(1)
            first.close()
            second.close()
            outsider.close()

    def test_disconnect_notification_is_once(self) -> None:
        room = EventRoomService()
        with RunningServer(room_service=room) as running:
            sock = running.connect()
            session = login(sock, "u1", "Alice")
            sock.close()
            deadline = time.monotonic() + 2
            while not room.disconnects and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual([session], room.disconnects)

    def test_async_private_results_are_per_recipient_and_shutdown_safe(self) -> None:
        with RunningServer() as running:
            alice = running.connect()
            bob = running.connect()
            outsider = running.connect()
            alice_session = login(alice, "u1", "Alice")
            bob_session = login(bob, "u2", "Bob")
            login(outsider, "u3", "Cara")
            results = []

            def timer_callback() -> None:
                results.append(
                    running.server.event_sink.publish(
                        PendingEvent(
                            "QUESTION_RESULT_EVENT",
                            (alice_session,),
                            {"question_id": "q1", "correct_answer": "A", "your_answer": "A", "points_awarded": 100},
                        )
                    )
                )
                results.append(
                    running.server.event_sink.publish(
                        PendingEvent(
                            "QUESTION_RESULT_EVENT",
                            (bob_session,),
                            {"question_id": "q1", "correct_answer": "A", "your_answer": "B", "points_awarded": 0},
                        )
                    )
                )

            publisher = threading.Thread(target=timer_callback)
            publisher.start()
            publisher.join(2)
            self.assertFalse(publisher.is_alive())
            self.assertTrue(all(result.accepted and result.delivered == 1 for result in results))
            alice_event = recv_message(alice)
            bob_event = recv_message(bob)
            self.assertEqual("A", alice_event["payload"]["your_answer"])
            self.assertEqual(100, alice_event["payload"]["points_awarded"])
            self.assertEqual("B", bob_event["payload"]["your_answer"])
            self.assertEqual(0, bob_event["payload"]["points_awarded"])
            outsider.settimeout(0.2)
            with self.assertRaises(socket.timeout):
                outsider.recv(1)
            running.server.stop()
            rejected = running.server.event_sink.publish(
                PendingEvent("PLAYER_STATUS_EVENT", (alice_session,), {"room_code": "R"})
            )
            self.assertFalse(rejected.accepted)
            self.assertEqual("SERVER_STOPPING", rejected.reason)
            alice.close()
            bob.close()
            outsider.close()

    def test_slow_event_recipient_does_not_delay_other_event_queues(self) -> None:
        with RunningServer() as running:
            slow = running.connect()
            normal = running.connect()
            slow_session = login(slow, "slow", "Slow")
            normal_session = login(normal, "normal", "Normal")
            slow.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
            slow_state = running.server._connections[
                running.server.sessions.get(slow_session).connection_handle
            ]
            slow_state.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            event = PendingEvent(
                "QUESTION_EVENT",
                (slow_session, normal_session),
                {"question_id": "q1", "time_limit": 5, "padding": "x" * 60_000},
            )
            started = time.monotonic()
            accepted = [running.server.send_event(event) for _ in range(30)]
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 1.0, f"event enqueue took {elapsed:.3f}s")
            self.assertEqual([2] * 30, accepted)
            self.assertEqual("QUESTION_EVENT", recv_message(normal)["type"])
            slow.close()
            normal.close()

    def test_full_event_queue_closes_only_slow_recipient(self) -> None:
        with RunningServer() as running:
            slow = running.connect()
            normal = running.connect()
            slow_session = login(slow, "queue-slow", "Queue Slow")
            normal_session = login(normal, "queue-normal", "Queue Normal")
            state = running.server._connections[
                running.server.sessions.get(slow_session).connection_handle
            ]
            state.event_outbox.put_nowait(None)
            self.assertIsNotNone(state.event_sender)
            state.event_sender.join(timeout=2)
            self.assertFalse(state.event_sender.is_alive())

            event = PendingEvent("ROOM_CLOSED_EVENT", (slow_session,),
                                 {"room_code": "SLOW", "reason": "TEST"})
            for _ in range(EVENT_QUEUE_LIMIT):
                self.assertEqual(1, running.server.send_event(event))
            self.assertEqual(EVENT_QUEUE_LIMIT, state.event_outbox.qsize())
            self.assertEqual(0, running.server.send_event(event))
            normal.settimeout(2)
            send_message(normal, request("LIST_ROOMS_REQUEST", "normal-still-live",
                                         session_id=normal_session))
            response = recv_message(normal)
            self.assertEqual("ERROR_RESPONSE", response["type"])
            self.assertEqual("normal-still-live", response["request_id"])
            slow.close()
            normal.close()


if __name__ == "__main__":
    unittest.main()
