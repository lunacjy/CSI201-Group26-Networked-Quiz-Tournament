from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest

from question_bank import MAX_FILE_BYTES, QuestionBankError, load_and_validate_bank, validate_bank
from tests.helpers import valid_bank_dict


class QuestionBankTests(unittest.TestCase):
    def test_valid_bank_and_extra_members(self) -> None:
        data = valid_bank_dict()
        data["future"] = {"allowed": True}
        data["quizzes"][0]["future"] = 1
        bank = validate_bank(data)
        self.assertEqual("quiz-1", bank.quizzes[0].quiz_id)
        self.assertEqual(5, len(bank.quizzes[0].questions))

    def test_boolean_integer_is_rejected(self) -> None:
        data = valid_bank_dict()
        data["quizzes"][0]["questions"][0]["points"] = True
        with self.assertRaisesRegex(QuestionBankError, "points"):
            validate_bank(data)

    def test_duplicate_question_id_across_quizzes_rejected(self) -> None:
        data = valid_bank_dict()
        second = copy.deepcopy(data["quizzes"][0])
        second["quiz_id"] = "quiz-2"
        data["quizzes"].append(second)
        with self.assertRaisesRegex(QuestionBankError, "duplicate question_id"):
            validate_bank(data)

    def test_options_must_be_exact(self) -> None:
        data = valid_bank_dict()
        data["quizzes"][0]["questions"][0]["options"]["E"] = "Five"
        with self.assertRaisesRegex(QuestionBankError, "exactly"):
            validate_bank(data)

    def test_ranges_and_required_fields(self) -> None:
        data = valid_bank_dict()
        data["quizzes"][0]["questions"] = data["quizzes"][0]["questions"][:4]
        with self.assertRaises(QuestionBankError):
            validate_bank(data)
        data = valid_bank_dict()
        del data["quizzes"][0]["title"]
        with self.assertRaises(QuestionBankError):
            validate_bank(data)

    def test_invalid_utf8_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "bank.json")
            path.write_bytes(b"\xff")
            with self.assertRaisesRegex(QuestionBankError, "UTF-8"):
                load_and_validate_bank(path)

    def test_oversized_file_rejected_before_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "bank.json")
            path.write_bytes(b" " * (MAX_FILE_BYTES + 1))
            with self.assertRaisesRegex(QuestionBankError, "maximum"):
                load_and_validate_bank(path)

    def test_file_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "bank.json")
            path.write_text(json.dumps(valid_bank_dict()), encoding="utf-8")
            self.assertEqual(1, len(load_and_validate_bank(path).quizzes))

    def test_nonstandard_json_constants_are_rejected_at_any_depth(self) -> None:
        for token in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(token=token), tempfile.TemporaryDirectory() as directory:
                path = Path(directory, "bank.json")
                raw = json.dumps(valid_bank_dict())[:-1]
                for suffix in (
                    f', "future_metadata": {token}}}',
                    f', "future_metadata": {{"nested": {token}}}}}',
                ):
                    path.write_text(raw + suffix, encoding="utf-8")
                    with self.assertRaisesRegex(QuestionBankError, "not valid JSON"):
                        load_and_validate_bank(path)


if __name__ == "__main__":
    unittest.main()
