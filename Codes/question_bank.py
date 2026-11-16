"""Load and validate the JSON question bank before the server starts."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any

from strict_json import loads as strict_json_loads


MAX_FILE_BYTES = 1_000_000
ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
OPTION_KEYS = frozenset("ABCD")


class QuestionBankError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class Question:
    question_id: str
    text: str
    options: tuple[tuple[str, str], ...]
    answer: str
    time_limit: int
    points: int


@dataclass(frozen=True, slots=True)
class Quiz:
    quiz_id: str
    title: str
    description: str
    questions: tuple[Question, ...]


@dataclass(frozen=True, slots=True)
class QuestionBank:
    schema_version: int
    quizzes: tuple[Quiz, ...]


def _object(value: object, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise QuestionBankError(f"{where} must be an object")
    return value


def _array(value: object, where: str, low: int, high: int) -> list[Any]:
    if not isinstance(value, list) or not low <= len(value) <= high:
        raise QuestionBankError(f"{where} must be an array with {low}-{high} items")
    return value


def _required(obj: dict[str, Any], field: str, where: str) -> Any:
    if field not in obj:
        raise QuestionBankError(f"{where}.{field} is required")
    return obj[field]


def _text(value: object, where: str, low: int, high: int) -> str:
    if not isinstance(value, str) or not low <= len(value) <= high:
        raise QuestionBankError(f"{where} must be a string with {low}-{high} characters")
    if any(not char.isprintable() for char in value):
        raise QuestionBankError(f"{where} contains a non-visible control character")
    if low > 0 and not value.strip():
        raise QuestionBankError(f"{where} must contain a visible non-space character")
    return value


def _integer(value: object, where: str, low: int, high: int) -> int:
    if type(value) is not int or not low <= value <= high:
        raise QuestionBankError(f"{where} must be an integer in {low}-{high}")
    return value


def _identifier(value: object, where: str) -> str:
    if not isinstance(value, str) or ID_PATTERN.fullmatch(value) is None:
        raise QuestionBankError(
            f"{where} must be 1-32 ASCII letters, digits, underscores or hyphens"
        )
    return value


def validate_bank(data: object) -> QuestionBank:
    root = _object(data, "question bank")
    schema_version = _required(root, "schema_version", "question bank")
    if type(schema_version) is not int or schema_version != 1:
        raise QuestionBankError("question bank.schema_version must be integer 1")
    raw_quizzes = _array(_required(root, "quizzes", "question bank"), "quizzes", 1, 20)
    quiz_ids: set[str] = set()
    question_ids: set[str] = set()
    quizzes: list[Quiz] = []
    for quiz_index, raw_quiz in enumerate(raw_quizzes):
        where = f"quizzes[{quiz_index}]"
        quiz = _object(raw_quiz, where)
        quiz_id = _identifier(_required(quiz, "quiz_id", where), f"{where}.quiz_id")
        if quiz_id in quiz_ids:
            raise QuestionBankError(f"duplicate quiz_id: {quiz_id}")
        quiz_ids.add(quiz_id)
        title = _text(_required(quiz, "title", where), f"{where}.title", 1, 100)
        description = _text(
            _required(quiz, "description", where), f"{where}.description", 0, 300
        )
        raw_questions = _array(
            _required(quiz, "questions", where), f"{where}.questions", 5, 50
        )
        questions: list[Question] = []
        for question_index, raw_question in enumerate(raw_questions):
            qwhere = f"{where}.questions[{question_index}]"
            question = _object(raw_question, qwhere)
            question_id = _identifier(
                _required(question, "question_id", qwhere), f"{qwhere}.question_id"
            )
            if question_id in question_ids:
                raise QuestionBankError(f"duplicate question_id: {question_id}")
            question_ids.add(question_id)
            prompt = _text(_required(question, "text", qwhere), f"{qwhere}.text", 1, 500)
            options = _object(_required(question, "options", qwhere), f"{qwhere}.options")
            if set(options) != OPTION_KEYS:
                raise QuestionBankError(f"{qwhere}.options must contain exactly A, B, C and D")
            validated_options = tuple(
                (key, _text(options[key], f"{qwhere}.options.{key}", 1, 200))
                for key in "ABCD"
            )
            answer = _required(question, "answer", qwhere)
            if not isinstance(answer, str) or answer not in OPTION_KEYS:
                raise QuestionBankError(f"{qwhere}.answer must be A, B, C or D")
            time_limit = _integer(
                _required(question, "time_limit", qwhere), f"{qwhere}.time_limit", 5, 60
            )
            points = _integer(
                _required(question, "points", qwhere), f"{qwhere}.points", 1, 1000
            )
            questions.append(
                Question(question_id, prompt, validated_options, answer, time_limit, points)
            )
        quizzes.append(Quiz(quiz_id, title, description, tuple(questions)))
    return QuestionBank(1, tuple(quizzes))


def load_and_validate_bank(path: str | Path) -> QuestionBank:
    bank_path = Path(path)
    try:
        size = bank_path.stat().st_size
    except OSError as exc:
        raise QuestionBankError(f"cannot read question bank: {exc}") from exc
    if size == 0:
        raise QuestionBankError("question bank is empty")
    if size > MAX_FILE_BYTES:
        raise QuestionBankError(
            f"question bank is {size} bytes; maximum is {MAX_FILE_BYTES} bytes"
        )
    try:
        with bank_path.open("rb") as stream:
            raw = stream.read(MAX_FILE_BYTES + 1)
    except OSError as exc:
        raise QuestionBankError(f"cannot read question bank: {exc}") from exc
    if len(raw) > MAX_FILE_BYTES:
        raise QuestionBankError(
            f"question bank exceeds maximum {MAX_FILE_BYTES} bytes while reading"
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise QuestionBankError("question bank is not valid UTF-8") from exc
    try:
        data = strict_json_loads(text)
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise QuestionBankError(f"question bank is not valid JSON: {exc}") from exc
    return validate_bank(data)
