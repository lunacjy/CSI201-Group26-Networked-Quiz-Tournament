"""JSON decoding helpers that reject non-standard constants and duplicate keys."""

from __future__ import annotations

import json
from typing import Any


def _reject_nonstandard_constant(token: str) -> None:
    raise ValueError(f"non-standard JSON numeric constant is not allowed: {token}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def loads(document: str) -> Any:
    """Decode JSON without NaN/Infinity extensions or ambiguous duplicate keys."""
    return json.loads(document, parse_constant=_reject_nonstandard_constant,
                      object_pairs_hook=_unique_object)
