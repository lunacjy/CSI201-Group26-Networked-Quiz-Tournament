"""Manage quiz rooms, players and ready status."""
from __future__ import annotations

from dataclasses import dataclass, field
import secrets
import threading
from typing import Any

from message_router import PendingEvent, RequestContext, ServiceResult


@dataclass
class Player:
    session_id: str
    user_id: str
    name: str
    ready: bool = False


@dataclass
class Room:
    code: str
    quiz_id: str
    host_id: str
    players: dict[str, Player] = field(default_factory=dict)
    status: str = "LOBBY"
    joined_order: list[str] = field(default_factory=list)
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)


class RoomManager:
    MAX_PLAYERS = 10

    def __init__(self, quiz_engine: object):
        self.quiz_engine = quiz_engine
        self._lock = threading.RLock()
        self._rooms: dict[str, Room] = {}
        self._membership: dict[str, str] = {}

    def _room(self, session_id: str) -> Room | None:
        code = self._membership.get(session_id)
        return self._rooms.get(code) if code else None

    @staticmethod
    def _players(room: Room) -> list[dict[str, Any]]:
        return [{"user_id": p.user_id, "name": p.name, "ready": p.ready,
                 "is_host": p.session_id == room.host_id}
                for p in (room.players[s] for s in room.joined_order if s in room.players)]

    @classmethod
    def _view(cls, room: Room) -> dict[str, Any]:
        return {"room_code": room.code, "quiz_id": room.quiz_id,
                "host_id": room.players.get(room.host_id, Player("", room.host_id, "")).user_id,
                "player_count": len(room.players), "max_players": cls.MAX_PLAYERS,
                "status": room.status, "players": cls._players(room)}

    def _status_event(self, room: Room) -> PendingEvent:
        host = room.players.get(room.host_id)
        return PendingEvent("PLAYER_STATUS_EVENT", tuple(room.players), {
            "room_code": room.code, "host_id": host.user_id if host else "",
            "players": self._players(room)})

    def list_rooms(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        with self._lock:
            rooms = [
                r for r in self._rooms.values()
                if r.status == "LOBBY" and len(r.players) < self.MAX_PLAYERS
            ]
            result = [{"room_code": r.code, "quiz_id": r.quiz_id,
                       "host_id": r.players[r.host_id].user_id,
                       "player_count": len(r.players), "max_players": self.MAX_PLAYERS,
                       "status": r.status} for r in rooms]
        return ServiceResult.success("LIST_ROOMS_RESPONSE", {"rooms": result})

    def create_room(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        quiz_id = payload["quiz_id"]
        if not self.quiz_engine.has_quiz(quiz_id):
            return ServiceResult.error("QUIZ_NOT_FOUND", "Quiz ID does not exist.")
        with self._lock:
            if ctx.session_id in self._membership:
                return ServiceResult.error("ALREADY_IN_ROOM", "Leave the current room first.")
            code = secrets.token_hex(3).upper()
            while code in self._rooms:
                code = secrets.token_hex(3).upper()
            room = Room(code, quiz_id, ctx.session_id)
            room.players[ctx.session_id] = Player(ctx.session_id, ctx.user_id, ctx.name)
            room.joined_order.append(ctx.session_id)
            self._rooms[code] = room
            self._membership[ctx.session_id] = code
            event = self._status_event(room)
            view = self._view(room)
        return ServiceResult.success("CREATE_ROOM_RESPONSE", {"room_code": code, "room": view}, (event,))

    def join_room(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        with self._lock:
            if ctx.session_id in self._membership:
                return ServiceResult.error("ALREADY_IN_ROOM", "Leave the current room first.")
            room = self._rooms.get(payload["room_code"].upper())
            if room is None:
                return ServiceResult.error("ROOM_NOT_FOUND", "Room does not exist.")
            with room.lock:
                if room.status != "LOBBY":
                    return ServiceResult.error("INVALID_STATE", "Room is already playing or closed.")
                if len(room.players) >= self.MAX_PLAYERS:
                    return ServiceResult.error("ROOM_FULL", "Room is full.")
                room.players[ctx.session_id] = Player(ctx.session_id, ctx.user_id, ctx.name)
                room.joined_order.append(ctx.session_id)
                self._membership[ctx.session_id] = room.code
                return ServiceResult.success("JOIN_ROOM_RESPONSE", {"room": self._view(room)},
                                             (self._status_event(room),))

    def list_players(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        with self._lock:
            room = self._room(ctx.session_id)
            if room is None:
                return ServiceResult.error("NOT_IN_ROOM", "Join a room first.")
            with room.lock:
                return ServiceResult.success("PLAYER_LIST_RESPONSE", {
                    "room_code": room.code, "players": self._players(room)})

    def set_ready(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        with self._lock:
            room = self._room(ctx.session_id)
            if room is None:
                return ServiceResult.error("NOT_IN_ROOM", "Join a room first.")
            with room.lock:
                if room.status != "LOBBY":
                    return ServiceResult.error("INVALID_STATE", "Readiness can change only in a lobby.")
                room.players[ctx.session_id].ready = payload["ready"]
                return ServiceResult.success("READY_RESPONSE", {"ready": payload["ready"]},
                                             (self._status_event(room),))

    def start_room(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        with self._lock:
            room = self._room(ctx.session_id)
            if room is None:
                return ServiceResult.error("NOT_IN_ROOM", "Join a room first.")
            with room.lock:
                if room.host_id != ctx.session_id:
                    return ServiceResult.error("NOT_ROOM_HOST", "Only the host can start.")
                if room.status != "LOBBY":
                    return ServiceResult.error("INVALID_STATE", "Room is not in a lobby.")
                if len(room.players) < 2 or not all(p.ready for p in room.players.values()):
                    return ServiceResult.error("PLAYERS_NOT_READY", "At least two players must all be ready.")
                # Engine state is prepared while room transition is protected. No events
                # are emitted until the state commit succeeds.
                started = self.quiz_engine.start_room(room.code, room.quiz_id,
                                                       tuple(room.players.values()))
                if isinstance(started, ServiceResult):
                    return started
                room.status = "PLAYING"
                return ServiceResult.success("START_QUIZ_RESPONSE", {"success": True}, started)

    def leave_room(self, ctx: RequestContext, payload: dict) -> ServiceResult:
        events = self._remove_player(ctx.session_id, disconnected=False)
        if events is None:
            return ServiceResult.error("NOT_IN_ROOM", "You are not in a room.")
        return ServiceResult.success("LEAVE_ROOM_RESPONSE", {"success": True}, events)

    def _remove_player(self, session_id: str, disconnected: bool) -> tuple[PendingEvent, ...] | None:
        events: list[PendingEvent] = []
        quiz_events: list[PendingEvent] = []
        close_code: str | None = None
        with self._lock:
            room = self._room(session_id)
            if room is None:
                return None
            with room.lock:
                was_host = room.host_id == session_id
                room.players.pop(session_id, None)
                room.joined_order = [
                    sid for sid in room.joined_order if sid != session_id
                ]
                self._membership.pop(session_id, None)
                if hasattr(self.quiz_engine, "player_departed"):
                    quiz_events.extend(self.quiz_engine.player_departed(room.code, session_id))
                if not room.players:
                    room.status = "CLOSED"
                    self._rooms.pop(room.code, None)
                    close_code = room.code
                    events.append(PendingEvent("ROOM_CLOSED_EVENT", (session_id,),
                                                {"room_code": room.code, "reason": "EMPTY_ROOM"}))
                else:
                    if was_host:
                        room.host_id = next((s for s in room.joined_order if s in room.players), next(iter(room.players)))
                    events.append(self._status_event(room))
        if close_code is not None and hasattr(self.quiz_engine, "close_room"):
            self.quiz_engine.close_room(close_code)
        if hasattr(self.quiz_engine, "publish_events"):
            self.quiz_engine.publish_events(quiz_events)
        return tuple(events)

    def on_disconnect(self, session_id: str) -> tuple[PendingEvent, ...]:
        return self._remove_player(session_id, disconnected=True) or ()

    def mark_finished(self, room_code: str) -> None:
        with self._lock:
            room = self._rooms.get(room_code)
            if room:
                with room.lock:
                    room.status = "FINISHED"

    def shutdown(self) -> None:
        with self._lock:
            codes = tuple(self._rooms)
            self._rooms.clear()
            self._membership.clear()
        for code in codes:
            self.quiz_engine.close_room(code)
