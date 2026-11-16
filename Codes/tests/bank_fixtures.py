"""Create sample question banks for automated tests."""
from __future__ import annotations


def bank_with_sizes(sizes: list[int], *, times: list[int] | None = None,
                    points: list[int] | None = None) -> dict:
    quizzes = []
    for quiz_number, size in enumerate(sizes):
        questions = []
        for number in range(size):
            questions.append({
                "question_id": f"Q{quiz_number:02d}_{number:02d}",
                "text": f"题目 {quiz_number + 1}-{number + 1}: Which option is correct?",
                "options": {"A": "正确 answer", "B": "Wrong one",
                            "C": "Wrong two", "D": "Wrong three"},
                "answer": "A", "time_limit": times[number % len(times)] if times else 5,
                "points": points[number % len(points)] if points else 100,
                "future_question_field": "allowed",
            })
        quizzes.append({"quiz_id": f"quiz-{quiz_number:02d}",
                        "title": f"测验 {quiz_number + 1}",
                        "description": "Unicode description",
                        "questions": questions, "future_quiz_field": 42})
    return {"schema_version": 1, "quizzes": quizzes, "future_root_field": True}
