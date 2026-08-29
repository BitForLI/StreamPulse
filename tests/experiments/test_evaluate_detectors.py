import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "evaluate_detectors.py"
SPEC = importlib.util.spec_from_file_location("evaluate_detectors", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class DetectorEvaluationTests(unittest.TestCase):
    def test_parse_duration_supports_compound_scenario_values(self):
        self.assertEqual(MODULE.parse_duration("1m30s"), timedelta(seconds=90))
        self.assertEqual(MODULE.parse_duration("6m20s"), timedelta(seconds=380))
        self.assertEqual(MODULE.parse_duration("250ms"), timedelta(milliseconds=250))

    def test_fault_labels_overlap_and_respect_network(self):
        metric = MODULE.Metric(
            run_id="run-a",
            window_start=datetime(2026, 8, 29, 0, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 8, 29, 0, 1, tzinfo=timezone.utc),
            location="au-sydney",
            network_id="as-synthetic-1221",
            node_id="edge-a",
            requests=500,
            error_rate=0.1,
            cache_hit_ratio=0.5,
            p95_ttfb_ms=200,
        )
        matching = MODULE.Fault(
            "isp_node_degradation",
            metric.window_start + timedelta(seconds=30),
            metric.window_end + timedelta(seconds=30),
            "edge-a",
            "as-synthetic-1221",
        )
        other_network = MODULE.Fault(
            matching.fault_type,
            matching.start,
            matching.end,
            matching.target,
            "as-synthetic-7545",
        )
        self.assertTrue(MODULE.fault_matches(metric, matching))
        self.assertFalse(MODULE.fault_matches(metric, other_network))

    def test_ewma_mad_uses_only_past_rows(self):
        start = datetime(2026, 8, 29, 0, 0, tzinfo=timezone.utc)
        rows = []
        for index in range(6):
            rows.append(self.metric(start, index, 40 + index % 2, 0.001, 0.8))
        spike = self.metric(start, 6, 220, 0.1, 0.2)
        rows.append(spike)
        predictions = MODULE.predict(rows, "ewma_mad")
        self.assertFalse(any(predictions[row][0] for row in rows[:-1]))
        self.assertTrue(predictions[spike][0])
        self.assertIn("EWMA_MAD_LATENCY_ANOMALY", predictions[spike][1])

    def test_summary_preserves_weak_strategy_result_and_raw_predictions(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            metrics_path = root / "metrics.jsonl"
            scenario_path = root / "scenario.yaml"
            manifest_path = root / "manifest.json"
            output = root / "output"
            start = datetime(2026, 8, 29, 0, 0, tzinfo=timezone.utc)
            rows = [self.metric(start, index, 40, 0.001, 0.8) for index in range(5)]
            rows.append(self.metric(start, 5, 220, 0.1, 0.2))
            metrics_path.write_text(
                "".join(json.dumps(self.as_json(row)) + "\n" for row in rows), encoding="utf-8"
            )
            scenario_path.write_text(
                """seed: 1
start_time: "2026-08-29T00:00:00Z"
duration: 6m
scenarios:
  - {at: 5m, type: node_latency_spike, target: edge-a, duration: 1m}
""",
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "seed": 42,
                        "simulation_start_utc": "2026-08-29T00:00:00Z",
                        "simulation_end_utc": "2026-08-29T00:06:00Z",
                        "git_commit": "test-revision",
                        "versions": {"flink": "test"},
                        "expected_label_windows": [
                            {
                                "type": "node_latency_spike",
                                "start": "2026-08-29T00:05:00Z",
                                "end": "2026-08-29T00:06:00Z",
                                "target": "edge-a",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            output.mkdir()
            result = MODULE.evaluate_run(
                "run-a", metrics_path, scenario_path, output, manifest_path
            )
            self.assertIn("fixed", result["strategies"])
            self.assertIn("rule", result["strategies"])
            self.assertIn("ewma_mad", result["strategies"])
            self.assertTrue((output / "run-a-predictions.jsonl").exists())
            self.assertEqual(result["strategies"]["rule"]["recall"], 0.0)
            self.assertEqual(result["seed"], 42)
            self.assertEqual(result["git_commit"], "test-revision")
            self.assertEqual(len(result["metrics_sha256"]), 64)
            self.assertIsNone(
                result["strategies"]["fixed"]["small_traffic_false_positive_rate"]
            )

    @staticmethod
    def metric(start, index, p95, error_rate, hit_rate):
        return MODULE.Metric(
            run_id="run-a",
            window_start=start + timedelta(minutes=index),
            window_end=start + timedelta(minutes=index + 1),
            location="au-sydney",
            network_id="as-synthetic-1221",
            node_id="edge-a",
            requests=500,
            error_rate=error_rate,
            cache_hit_ratio=hit_rate,
            p95_ttfb_ms=p95,
        )

    @staticmethod
    def as_json(metric):
        return {
            "window_start": metric.window_start.isoformat(),
            "window_end": metric.window_end.isoformat(),
            "location": metric.location,
            "network_id": metric.network_id,
            "node_id": metric.node_id,
            "requests": metric.requests,
            "error_5xx_rate": metric.error_rate,
            "cache_hit_ratio": metric.cache_hit_ratio,
            "ttfb_p95_ms": metric.p95_ttfb_ms,
        }


if __name__ == "__main__":
    unittest.main()
