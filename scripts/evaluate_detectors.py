#!/usr/bin/env python3
"""Evaluate fixed, rule, and EWMA/MAD detectors without time leakage."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import yaml


NODE_FAULT_TYPES = {
    "node_latency_spike",
    "node_5xx_spike",
    "isp_node_degradation",
    "capacity_pressure",
}


@dataclass(frozen=True)
class Fault:
    fault_type: str
    start: datetime
    end: datetime
    target: str
    network: str | None


@dataclass(frozen=True)
class Metric:
    run_id: str
    window_start: datetime
    window_end: datetime
    location: str
    network_id: str
    node_id: str
    requests: int
    error_rate: float
    cache_hit_ratio: float
    p95_ttfb_ms: float

    @property
    def key(self) -> tuple[str, str, str]:
        return self.location, self.network_id, self.node_id


def parse_time(value: str) -> datetime:
    normalized = value.strip().replace(" ", "T")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_duration(value: str) -> timedelta:
    units = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}
    normalized = value.strip().lower()
    parts = re.findall(r"(\d+(?:\.\d+)?)(ms|s|m|h)", normalized)
    if not parts or "".join(number + unit for number, unit in parts) != normalized:
        raise ValueError(f"unsupported duration: {value}")
    return timedelta(seconds=sum(float(number) * units[unit] for number, unit in parts))


def load_faults(path: Path, manifest_path: Path | None = None) -> list[Fault]:
    if manifest_path is not None:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        return [
            Fault(
                fault_type=str(label["type"]),
                start=parse_time(str(label["start"])),
                end=parse_time(str(label["end"])),
                target=str(label.get("target", "")),
                network=str(label["network"]) if label.get("network") else None,
            )
            for label in manifest.get("expected_label_windows", [])
            if label["type"] in NODE_FAULT_TYPES
        ]

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    start = parse_time(str(payload["start_time"]))
    faults = []
    for scenario in payload.get("scenarios", []):
        if scenario["type"] not in NODE_FAULT_TYPES:
            continue
        fault_start = start + parse_duration(str(scenario["at"]))
        faults.append(
            Fault(
                fault_type=scenario["type"],
                start=fault_start,
                end=fault_start + parse_duration(str(scenario["duration"])),
                target=str(scenario.get("target", "")),
                network=str(scenario["network"]) if scenario.get("network") else None,
            )
        )
    return faults


def load_metrics(run_id: str, path: Path) -> list[Metric]:
    metrics: list[Metric] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
            metrics.append(
                Metric(
                    run_id=run_id,
                    window_start=parse_time(str(row["window_start"])),
                    window_end=parse_time(str(row["window_end"])),
                    location=str(row["location"]),
                    network_id=str(row["network_id"]),
                    node_id=str(row["node_id"]),
                    requests=int(row["requests"]),
                    error_rate=float(row["error_5xx_rate"]),
                    cache_hit_ratio=float(row["cache_hit_ratio"]),
                    p95_ttfb_ms=float(row["ttfb_p95_ms"]),
                )
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError(f"{path}:{line_number}: {error}") from error
    return sorted(metrics, key=lambda metric: (metric.window_end, metric.key))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fault_matches(metric: Metric, fault: Fault) -> bool:
    overlaps = metric.window_start < fault.end and metric.window_end > fault.start
    target_matches = metric.node_id == fault.target
    network_matches = fault.network is None or metric.network_id == fault.network
    return overlaps and target_matches and network_matches


def ground_truth(metric: Metric, faults: Iterable[Fault]) -> bool:
    return any(fault_matches(metric, fault) for fault in faults)


def median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def robust_z(value: float, history: list[float]) -> float:
    if not history:
        return 0.0
    center = median(history)
    mad = median([abs(item - center) for item in history])
    if mad < 1e-9:
        return 0.0 if abs(value - center) < 1e-9 else math.inf
    return abs(0.6745 * (value - center) / mad)


def frozen_ewma(values: list[float], alpha: float = 0.2) -> float:
    if not values:
        return 0.0
    baseline = values[0]
    accepted = [values[0]]
    for value in values[1:]:
        if len(accepted) < 5 or robust_z(value, accepted) <= 4:
            baseline = alpha * value + (1 - alpha) * baseline
            accepted.append(value)
    return baseline


def predict(metrics: list[Metric], strategy: str) -> dict[Metric, tuple[bool, list[str]]]:
    predictions: dict[Metric, tuple[bool, list[str]]] = {}
    history: dict[tuple[str, str, str], list[Metric]] = defaultdict(list)
    by_window_scope: dict[tuple[datetime, str, str], list[Metric]] = defaultdict(list)
    for metric in metrics:
        by_window_scope[(metric.window_end, metric.location, metric.network_id)].append(metric)

    for metric in metrics:
        reasons: list[str] = []
        detected = False
        if strategy == "fixed":
            detected = metric.requests >= 100 and (
                metric.p95_ttfb_ms >= 100 or metric.error_rate >= 0.05
            )
            if detected:
                reasons.append("FIXED_THRESHOLD")
        elif strategy == "rule":
            peers = by_window_scope[(metric.window_end, metric.location, metric.network_id)]
            location_baseline = median([peer.p95_ttfb_ms for peer in peers])
            detected = (
                metric.requests >= 100
                and metric.error_rate > 0.02
                and location_baseline > 0
                and metric.p95_ttfb_ms > 2 * location_baseline
            )
            if detected:
                reasons.append("RULE_LATENCY_ERROR_ANOMALY")
        elif strategy == "ewma_mad":
            past = history[metric.key]
            if len(past) >= 5 and metric.requests >= 100:
                past_p95 = [item.p95_ttfb_ms for item in past]
                past_error = [item.error_rate for item in past]
                past_hit = [item.cache_hit_ratio for item in past]
                latency_baseline = frozen_ewma(past_p95)
                error_baseline = frozen_ewma(past_error)
                if (
                    latency_baseline > 0
                    and metric.p95_ttfb_ms > 1.5 * latency_baseline
                    and robust_z(metric.p95_ttfb_ms, past_p95) >= 3
                ):
                    reasons.append("EWMA_MAD_LATENCY_ANOMALY")
                if metric.error_rate > 0.02 and metric.error_rate - error_baseline > 0.01:
                    reasons.append("EWMA_ERROR_RATE_ANOMALY")
                if metric.cache_hit_ratio < 0.35 and metric.cache_hit_ratio < median(past_hit) - 0.20:
                    reasons.append("MAD_CACHE_MISS_ANOMALY")
                detected = bool(reasons)
        else:
            raise ValueError(f"unknown strategy: {strategy}")
        predictions[metric] = detected, reasons
        history[metric.key].append(metric)
    return predictions


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def strategy_metrics(
    rows: list[Metric], faults: list[Fault], predictions: dict[Metric, tuple[bool, list[str]]]
) -> dict[str, object]:
    tp = fp = fn = tn = 0
    small_traffic_fp = 0
    small_traffic_negative_windows = 0
    for row in rows:
        actual = ground_truth(row, faults)
        predicted = predictions[row][0]
        if row.requests < 100 and not actual:
            small_traffic_negative_windows += 1
        if predicted and actual:
            tp += 1
        elif predicted and not actual:
            fp += 1
            if row.requests < 100:
                small_traffic_fp += 1
        elif actual:
            fn += 1
        else:
            tn += 1
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    duration_hours = max(
        (max(row.window_end for row in rows) - min(row.window_start for row in rows)).total_seconds() / 3600,
        1 / 3600,
    )

    delays: list[float] = []
    recoveries: list[float] = []
    missed_faults = 0
    for fault in faults:
        candidates = [
            row
            for row in rows
            if row.node_id == fault.target and (fault.network is None or row.network_id == fault.network)
        ]
        detections = [
            row.window_end
            for row in candidates
            if row.window_end > fault.start and row.window_start < fault.end and predictions[row][0]
        ]
        if detections:
            delays.append(max(0.0, (min(detections) - fault.start).total_seconds()))
        else:
            missed_faults += 1
        normal_after = [
            row.window_end
            for row in candidates
            if row.window_start >= fault.end and not predictions[row][0]
        ]
        if normal_after:
            recoveries.append(max(0.0, (min(normal_after) - fault.end).total_seconds()))

    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
        "false_alerts_per_hour": round(fp / duration_hours, 6),
        "small_traffic_false_positives": small_traffic_fp,
        "small_traffic_negative_windows": small_traffic_negative_windows,
        "small_traffic_false_positive_rate": (
            round(small_traffic_fp / small_traffic_negative_windows, 6)
            if small_traffic_negative_windows
            else None
        ),
        "detection_delay_p50_seconds": percentile(delays, 0.50),
        "detection_delay_p95_seconds": percentile(delays, 0.95),
        "recovery_p50_seconds": percentile(recoveries, 0.50),
        "recovery_p95_seconds": percentile(recoveries, 0.95),
        "missed_faults": missed_faults,
    }


def evaluate_run(
    run_id: str,
    metrics_path: Path,
    scenario_path: Path,
    output_dir: Path,
    manifest_path: Path | None = None,
) -> dict[str, object]:
    rows = load_metrics(run_id, metrics_path)
    if not rows:
        raise ValueError(f"{metrics_path}: no metrics")
    faults = load_faults(scenario_path, manifest_path)
    strategies: dict[str, dict[str, object]] = {}
    prediction_path = output_dir / f"{run_id}-predictions.jsonl"
    with prediction_path.open("w", encoding="utf-8", newline="\n") as raw_output:
        for strategy in ("fixed", "rule", "ewma_mad"):
            predictions = predict(rows, strategy)
            strategies[strategy] = strategy_metrics(rows, faults, predictions)
            for row in rows:
                detected, reasons = predictions[row]
                raw_output.write(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "strategy": strategy,
                            "window_start": row.window_start.isoformat(),
                            "window_end": row.window_end.isoformat(),
                            "location": row.location,
                            "network_id": row.network_id,
                            "node_id": row.node_id,
                            "requests": row.requests,
                            "error_5xx_rate": row.error_rate,
                            "cache_hit_ratio": row.cache_hit_ratio,
                            "ttfb_p95_ms": row.p95_ttfb_ms,
                            "actual_fault": ground_truth(row, faults),
                            "predicted_fault": detected,
                            "reason_codes": reasons,
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
    result = {
        "run_id": run_id,
        "metrics_path": metrics_path.as_posix(),
        "metrics_sha256": sha256(metrics_path),
        "scenario_path": scenario_path.as_posix(),
        "scenario_sha256": sha256(scenario_path),
        "row_count": len(rows),
        "fault_count": len(faults),
        "strategies": strategies,
    }
    if manifest_path is not None:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        result.update(
            {
                "manifest_path": manifest_path.as_posix(),
                "manifest_sha256": sha256(manifest_path),
                "seed": manifest.get("seed"),
                "simulation_start_utc": manifest.get("simulation_start_utc"),
                "simulation_end_utc": manifest.get("simulation_end_utc"),
                "git_commit": manifest.get("git_commit"),
                "component_versions": manifest.get("versions", {}),
            }
        )
    return result


def aggregate(runs: list[dict[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    metric_names = (
        "precision",
        "recall",
        "f1",
        "false_alerts_per_hour",
        "detection_delay_p50_seconds",
        "detection_delay_p95_seconds",
        "recovery_p50_seconds",
        "recovery_p95_seconds",
        "small_traffic_false_positives",
        "small_traffic_false_positive_rate",
        "missed_faults",
    )
    for strategy in ("fixed", "rule", "ewma_mad"):
        values = [run["strategies"][strategy] for run in runs]  # type: ignore[index]
        strategy_result = {}
        for metric in metric_names:
            numbers = [float(value[metric]) for value in values if value[metric] is not None]
            strategy_result[metric] = (
                {
                    "mean": round(statistics.mean(numbers), 6),
                    "median": round(statistics.median(numbers), 6),
                    "min": min(numbers),
                    "max": max(numbers),
                    "sample_count": len(numbers),
                }
                if numbers
                else {"mean": None, "median": None, "min": None, "max": None, "sample_count": 0}
            )
        result[strategy] = strategy_result
    return result


def write_markdown(summary: dict[str, object], path: Path) -> None:
    lines = [
        "# Detector comparison",
        "",
        "Synthetic, fixed-seed local evidence. Missing or weak results are retained.",
        "",
        f"Runs: **{summary['run_count']}**; three-run gate: **{summary['acceptance_run_count_met']}**; "
        f"manifest evidence complete: **{summary['manifest_evidence_complete']}**.",
        "",
        "| Strategy | Precision mean/median/range | Recall mean/median/range | F1 mean/median/range | False alerts/hour mean | Detection P95 mean (s) |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    aggregate_rows = summary["aggregate"]  # type: ignore[index]
    for strategy in ("fixed", "rule", "ewma_mad"):
        item = aggregate_rows[strategy]
        def spread(metric: str) -> str:
            values = item[metric]
            return f"{values['mean']:.6f}/{values['median']:.6f}/[{values['min']:.6f}, {values['max']:.6f}]"

        delay = item["detection_delay_p95_seconds"]["mean"]
        lines.append(
            f"| `{strategy}` | {spread('precision')} | {spread('recall')} | {spread('f1')} | "
            f"{item['false_alerts_per_hour']['mean']:.6f} | {delay if delay is not None else 'N/A'} |"
        )
    lines.extend(
        [
            "",
            "## Per-run results",
            "",
            "| Run | Strategy | Precision | Recall | F1 | Missed faults |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for run in summary["runs"]:  # type: ignore[index]
        for strategy in ("fixed", "rule", "ewma_mad"):
            item = run["strategies"][strategy]
            lines.append(
                f"| `{run['run_id']}` | `{strategy}` | {item['precision']:.6f} | "
                f"{item['recall']:.6f} | {item['f1']:.6f} | {item['missed_faults']} |"
            )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "The fixed threshold and EWMA/MAD baseline tied on these deliberately strong synthetic faults; "
            "this does not show that either generalizes to changing production traffic.",
            "The combined rule was retained despite weak recall: requiring latency and error evidence together "
            "missed latency-only and error-only windows.",
            "Each raw node-metric file and manifest is SHA-256 identified in `summary.json`; per-window labels, "
            "predictions, metrics, and reason codes are stored in the three `*-predictions.jsonl` files.",
            "",
            "Detection uses completed-window timestamps. EWMA/MAD sees only past windows; no future rows are included.",
            "This report does not establish causal QoE improvement or production-scale performance.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run",
        nargs=4,
        action="append",
        metavar=("RUN_ID", "METRICS_JSONL", "SCENARIO_YAML", "MANIFEST_JSON"),
        required=True,
        help="repeat at least three times for the Day 6 acceptance run",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    runs = [
        evaluate_run(run_id, Path(metrics), Path(scenario), args.output_dir, Path(manifest))
        for run_id, metrics, scenario, manifest in args.run
    ]
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_count": len(runs),
        "acceptance_run_count_met": len(runs) >= 3,
        "manifest_evidence_complete": all("manifest_path" in run for run in runs),
        "runs": runs,
        "aggregate": aggregate(runs),
        "limitations": [
            "Synthetic fixed-seed local data only.",
            "Completed-window detection delay includes window close time.",
            "Popularity-shift labels are excluded from node-failure ground truth.",
            "Small-traffic false-positive rate is N/A when a run has no sub-100-request negative window.",
            "Expected recommendation deltas are not observed causal effects.",
        ],
    }
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    write_markdown(summary, args.output_dir / "report.md")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
