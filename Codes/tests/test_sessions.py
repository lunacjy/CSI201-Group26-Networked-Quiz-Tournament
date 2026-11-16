from __future__ import annotations

import threading
import unittest

from session_manager import DuplicateName, DuplicateUser, SessionError, SessionManager


class SessionManagerTests(unittest.TestCase):
    def test_duplicate_user_and_name(self) -> None:
        manager = SessionManager()
        manager.login("u1", "Alice", "conn-1")
        with self.assertRaises(DuplicateUser):
            manager.login("u1", "Bob", "conn-2")
        with self.assertRaises(DuplicateName):
            manager.login("u2", "Alice", "conn-3")

    def test_session_is_bound_to_connection(self) -> None:
        manager = SessionManager()
        session = manager.login("u1", "Alice", "conn-1")
        self.assertEqual(session, manager.authenticate(session.session_id, "conn-1"))
        with self.assertRaises(SessionError):
            manager.authenticate(session.session_id, "conn-2")

    def test_cleanup_is_idempotent_and_identity_reusable(self) -> None:
        manager = SessionManager()
        manager.login("u1", "Alice", "conn-1")
        self.assertIsNotNone(manager.remove_by_connection("conn-1"))
        self.assertIsNone(manager.remove_by_connection("conn-1"))
        manager.login("u1", "Alice", "conn-2")
        self.assertEqual(1, len(manager))

    def test_concurrent_duplicate_login_has_one_winner(self) -> None:
        manager = SessionManager()
        barrier = threading.Barrier(12)
        outcomes: list[str] = []
        lock = threading.Lock()

        def attempt(index: int) -> None:
            barrier.wait()
            try:
                manager.login("same-user", f"Name-{index}", f"conn-{index}")
                outcome = "success"
            except DuplicateUser:
                outcome = "duplicate"
            with lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=attempt, args=(index,)) for index in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(1, outcomes.count("success"))
        self.assertEqual(11, outcomes.count("duplicate"))


if __name__ == "__main__":
    unittest.main()
