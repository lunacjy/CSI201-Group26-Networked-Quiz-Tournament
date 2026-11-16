"""Thread-safe management of active user identities and sessions."""

from __future__ import annotations

from dataclasses import dataclass
import secrets
import threading
from typing import Hashable


class SessionError(ValueError):
    code = "INVALID_SESSION"


class DuplicateUser(SessionError):
    code = "DUPLICATE_USER"


class DuplicateName(SessionError):
    code = "DUPLICATE_NAME"


@dataclass(frozen=True, slots=True)
class Session:
    session_id: str
    user_id: str
    name: str
    connection_handle: Hashable


class SessionManager:
    """Owns all active identities; check-and-insert is one locked operation."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._by_session: dict[str, Session] = {}
        self._by_user: dict[str, str] = {}
        self._by_name: dict[str, str] = {}
        self._by_connection: dict[Hashable, str] = {}

    def login(self, user_id: str, name: str, connection_handle: Hashable) -> Session:
        with self._lock:
            if connection_handle in self._by_connection:
                raise SessionError("connection is already authenticated")
            if user_id in self._by_user:
                raise DuplicateUser("user ID already has an active session")
            if name in self._by_name:
                raise DuplicateName("display name already has an active session")
            session_id = secrets.token_urlsafe(24)
            while session_id in self._by_session:
                session_id = secrets.token_urlsafe(24)
            session = Session(session_id, user_id, name, connection_handle)
            self._by_session[session_id] = session
            self._by_user[user_id] = session_id
            self._by_name[name] = session_id
            self._by_connection[connection_handle] = session_id
            return session

    def authenticate(self, session_id: object, connection_handle: Hashable) -> Session:
        if not isinstance(session_id, str):
            raise SessionError("a valid session is required")
        with self._lock:
            session = self._by_session.get(session_id)
            if session is None or session.connection_handle != connection_handle:
                raise SessionError("session is not valid for this connection")
            return session

    def get(self, session_id: str) -> Session | None:
        with self._lock:
            return self._by_session.get(session_id)

    def remove_by_connection(self, connection_handle: Hashable) -> Session | None:
        """Remove once and return the removed session; repeated cleanup returns None."""
        with self._lock:
            session_id = self._by_connection.pop(connection_handle, None)
            if session_id is None:
                return None
            session = self._by_session.pop(session_id, None)
            if session is not None:
                self._by_user.pop(session.user_id, None)
                self._by_name.pop(session.name, None)
            return session

    def remove_session(self, session_id: str) -> Session | None:
        with self._lock:
            session = self._by_session.pop(session_id, None)
            if session is None:
                return None
            self._by_user.pop(session.user_id, None)
            self._by_name.pop(session.name, None)
            self._by_connection.pop(session.connection_handle, None)
            return session

    def snapshot(self) -> tuple[Session, ...]:
        with self._lock:
            return tuple(self._by_session.values())

    def __len__(self) -> int:
        with self._lock:
            return len(self._by_session)
