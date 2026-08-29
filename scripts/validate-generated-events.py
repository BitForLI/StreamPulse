#!/usr/bin/env python3
"""Validate generator JSONL envelopes against v1 contracts and privacy rules."""

from __future__ import annotations

import argparse
import ipaddress
import json
from collections import Counter
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = {
    "cdn.delivery.v1": ROOT / "schemas" / "delivery-event-v1.schema.json",
    "cdn.routing.v1": ROOT / "schemas" / "routing-event-v1.schema.json",
    "cdn.player.v1": ROOT / "schemas" / "player-event-v1.schema.json",
}
FORBIDDEN_KEYS = {
    "authorization", "client_ip", "cookie", "device_id", "email", "full_ip",
    "phone", "query_string", "raw_user_agent", "signed_url", "user_account", "user_agent",
}


def privacy_errors(value: Any, path: str = "$") -> list[str]:
    errors: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if key.lower() in FORBIDDEN_KEYS:
                errors.append(f"{child_path}: forbidden field")
            errors.extend(privacy_errors(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            errors.extend(privacy_errors(child, f"{path}[{index}]"))
    elif isinstance(value, str):
        if value.startswith(("http://", "https://")) and "?" in value:
            errors.append(f"{path}: URL query string is forbidden")
        try:
            ipaddress.ip_address(value)
            errors.append(f"{path}: complete IP address is forbidden")
        except ValueError:
            pass
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl", type=Path)
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()

    validators = {
        topic: Draft202012Validator(json.loads(path.read_text(encoding="utf-8")), format_checker=FormatChecker())
        for topic, path in SCHEMAS.items()
    }
    topics: Counter[str] = Counter()
    delivery_keys: Counter[str] = Counter()
    schema_errors = duplicates = 0
    payload_by_identity: dict[tuple[str, str], Any] = {}

    with args.jsonl.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            envelope = json.loads(line)
            topic, key, value = envelope["topic"], envelope["key"], envelope["value"]
            if topic not in validators:
                raise ValueError(f"line {line_number}: unsupported topic {topic}")
            errors = sorted(validators[topic].iter_errors(value), key=lambda item: list(item.path))
            marked_invalid = bool(envelope.get("schema_invalid"))
            if marked_invalid:
                schema_errors += 1
                if not errors:
                    raise ValueError(f"line {line_number}: marked schema-invalid but contract accepted it")
            elif errors:
                raise ValueError(f"line {line_number}: {errors[0].message}")
            if privacy := privacy_errors(value):
                raise ValueError(f"line {line_number}: {privacy[0]}")
            if topic == "cdn.routing.v1" and value["selected_node"] not in value["candidate_nodes"]:
                raise ValueError(f"line {line_number}: selected node is not a candidate")

            identity = (topic, value["event_id"])
            if envelope.get("duplicate"):
                duplicates += 1
                if identity not in payload_by_identity or payload_by_identity[identity] != value:
                    raise ValueError(f"line {line_number}: duplicate is not an exact earlier payload")
            else:
                payload_by_identity[identity] = value
            topics[topic] += 1
            if topic == "cdn.delivery.v1":
                delivery_keys[key] += 1

    counts = list(delivery_keys.values())
    summary = {
        "records_by_topic": dict(sorted(topics.items())),
        "duplicate_records": duplicates,
        "schema_error_records": schema_errors,
        "delivery_key_count": len(counts),
        "delivery_key_max_to_mean": round(max(counts) / (sum(counts) / len(counts)), 6) if counts else None,
    }
    rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    print(rendered, end="")
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
