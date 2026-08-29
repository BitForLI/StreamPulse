from __future__ import annotations

import ipaddress
import json
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = ROOT / "schemas"
VALID_FIXTURES = ROOT / "contracts" / "fixtures" / "valid"
INVALID_FIXTURES = ROOT / "contracts" / "fixtures" / "invalid"

SCHEMA_FILES = {
    "delivery": SCHEMA_DIR / "delivery-event-v1.schema.json",
    "routing": SCHEMA_DIR / "routing-event-v1.schema.json",
    "player": SCHEMA_DIR / "player-event-v1.schema.json",
    "recommendation": SCHEMA_DIR / "recommendation-v1.schema.json",
}

FORBIDDEN_KEYS = {
    "authorization",
    "client_ip",
    "cookie",
    "device_id",
    "email",
    "full_ip",
    "phone",
    "query_string",
    "raw_user_agent",
    "signed_url",
    "user_account",
    "user_agent",
}


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise AssertionError(f"{path} must contain one JSON object")
    return value


def fixture_kind(path: Path) -> str:
    kind = path.name.split("-", 1)[0]
    if kind not in SCHEMA_FILES:
        raise AssertionError(f"unknown fixture prefix for {path.name}")
    return kind


def parse_rfc3339(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def walk_json(value: Any, path: tuple[str, ...] = ()) -> Iterable[tuple[tuple[str, ...], Any]]:
    yield path, value
    if isinstance(value, dict):
        for key, child in value.items():
            yield from walk_json(child, path + (key,))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk_json(child, path + (str(index),))


def privacy_violations(value: dict[str, Any]) -> list[str]:
    violations: list[str] = []
    for path, child in walk_json(value):
        if path and path[-1].lower() in FORBIDDEN_KEYS:
            violations.append(f"forbidden field: {'.'.join(path)}")
        if not isinstance(child, str):
            continue
        if "?" in child and (child.startswith("/") or "://" in child):
            violations.append(f"URL/query value at {'.'.join(path)}")
        try:
            ipaddress.ip_address(child)
        except ValueError:
            pass
        else:
            violations.append(f"complete IP value at {'.'.join(path)}")
    return violations


def semantic_violations(kind: str, value: dict[str, Any]) -> list[str]:
    violations: list[str] = []
    if kind == "delivery":
        if parse_rfc3339(value["ingest_time"]) < parse_rfc3339(value["event_time"]):
            violations.append("ingest_time precedes event_time")
    elif kind == "routing":
        if value["selected_node"] not in value["candidate_nodes"]:
            violations.append("selected_node is not in candidate_nodes")
    elif kind == "recommendation":
        if parse_rfc3339(value["valid_until"]) <= parse_rfc3339(value["created_at"]):
            violations.append("valid_until must be later than created_at")
        if sum(value["proposed"].values()) <= 0:
            violations.append("proposed weights have no eligible capacity")
    return violations


class ContractSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schemas = {kind: load_json(path) for kind, path in SCHEMA_FILES.items()}
        cls.validators = {
            kind: Draft202012Validator(schema, format_checker=FormatChecker())
            for kind, schema in cls.schemas.items()
        }

    def test_schemas_are_valid_draft_2020_12(self) -> None:
        for kind, schema in self.schemas.items():
            with self.subTest(kind=kind):
                Draft202012Validator.check_schema(schema)

    def test_schema_identifiers_are_unique(self) -> None:
        identifiers = [schema["$id"] for schema in self.schemas.values()]
        self.assertEqual(len(identifiers), len(set(identifiers)))

    def test_valid_fixtures_pass_schema_semantics_and_privacy(self) -> None:
        fixtures = sorted(VALID_FIXTURES.glob("*.json"))
        self.assertGreaterEqual(len(fixtures), 8)
        for path in fixtures:
            kind = fixture_kind(path)
            value = load_json(path)
            with self.subTest(path=path.name):
                self.validators[kind].validate(value)
                self.assertEqual([], semantic_violations(kind, value))
                self.assertEqual([], privacy_violations(value))

    def test_forward_compatible_optional_field_is_accepted(self) -> None:
        value = load_json(VALID_FIXTURES / "delivery-forward-compatible.json")
        self.assertIn("future_optional_field", value)
        self.validators["delivery"].validate(value)

    def test_schema_invalid_fixtures_are_rejected(self) -> None:
        fixtures = sorted(
            path
            for path in INVALID_FIXTURES.glob("*.json")
            if "privacy" not in path.name and "expired" not in path.name
        )
        self.assertGreaterEqual(len(fixtures), 6)
        for path in fixtures:
            kind = fixture_kind(path)
            value = load_json(path)
            with self.subTest(path=path.name):
                errors = list(self.validators[kind].iter_errors(value))
                self.assertTrue(errors, f"{path.name} unexpectedly passed its schema")

    def test_privacy_fixture_is_rejected_independently_of_schema(self) -> None:
        path = INVALID_FIXTURES / "delivery-privacy-raw-ip.json"
        value = load_json(path)
        self.validators["delivery"].validate(value)
        self.assertTrue(privacy_violations(value))

    def test_expired_recommendation_is_rejected_semantically(self) -> None:
        path = INVALID_FIXTURES / "recommendation-expired.json"
        value = load_json(path)
        self.validators["recommendation"].validate(value)
        self.assertIn(
            "valid_until must be later than created_at",
            semantic_violations("recommendation", value),
        )


if __name__ == "__main__":
    unittest.main()
