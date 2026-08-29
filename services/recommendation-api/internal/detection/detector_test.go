package detection

import (
	"testing"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
)

func TestRuleDetectorFlagsOnlyHighVolumeCombinedFailure(t *testing.T) {
	now := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	metrics := []domain.NodeMetric{
		metric(now, "edge-a", 300, 0.10, 0.70, 500),
		metric(now, "edge-b", 40, 0.00, 0.75, 500),
		metric(now, "edge-c", 45, 0.00, 0.75, 500),
	}
	detector := New(testConfig())

	signals := detector.Evaluate(metrics)
	if !signals["edge-a"].Anomalous {
		t.Fatal("expected edge-a to be anomalous")
	}
	if !contains(signals["edge-a"].ReasonCodes, "RULE_LATENCY_ERROR_ANOMALY") {
		t.Fatalf("missing rule reason: %#v", signals["edge-a"].ReasonCodes)
	}
	if signals["edge-b"].Anomalous || signals["edge-c"].Anomalous {
		t.Fatal("healthy peers must not be marked anomalous")
	}
}

func TestEWMAMADUsesPastWindowsAndDoesNotAbsorbSpike(t *testing.T) {
	start := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	var history []domain.NodeMetric
	for i := 0; i < 8; i++ {
		end := start.Add(time.Duration(i) * time.Minute)
		history = append(history,
			metric(end, "edge-a", 40+float64(i%2), 0.001, 0.80, 500),
			metric(end, "edge-b", 42+float64(i%2), 0.001, 0.80, 500),
		)
	}
	history = append(history,
		metric(start.Add(8*time.Minute), "edge-a", 210, 0.09, 0.25, 500),
		metric(start.Add(8*time.Minute), "edge-b", 43, 0.001, 0.80, 500),
	)

	signals := New(testConfig()).Evaluate(history)
	signal := signals["edge-a"]
	if !signal.Anomalous {
		t.Fatal("expected EWMA/MAD to detect the injected spike")
	}
	if signal.BaselineMS >= 60 {
		t.Fatalf("baseline absorbed spike: %.2f", signal.BaselineMS)
	}
	if !contains(signal.ReasonCodes, "EWMA_MAD_LATENCY_ANOMALY") ||
		!contains(signal.ReasonCodes, "EWMA_ERROR_RATE_ANOMALY") ||
		!contains(signal.ReasonCodes, "MAD_CACHE_MISS_ANOMALY") {
		t.Fatalf("missing explainable reasons: %#v", signal.ReasonCodes)
	}
	if signals["edge-b"].Anomalous {
		t.Fatal("normal peer must not be marked anomalous")
	}
}

func TestNormalHistoryDoesNotGenerateSignal(t *testing.T) {
	start := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	var history []domain.NodeMetric
	for i := 0; i < 10; i++ {
		end := start.Add(time.Duration(i) * time.Minute)
		history = append(history,
			metric(end, "edge-a", 40+float64(i%2), 0.001, 0.80, 500),
			metric(end, "edge-b", 42+float64(i%2), 0.001, 0.79, 500),
		)
	}

	for nodeID, signal := range New(testConfig()).Evaluate(history) {
		if signal.Anomalous {
			t.Fatalf("unexpected anomaly for %s: %#v", nodeID, signal)
		}
	}
}

func metric(end time.Time, nodeID string, p95, errorRate, hitRate float64, requests uint64) domain.NodeMetric {
	return domain.NodeMetric{
		WindowStart:  end.Add(-time.Minute),
		WindowEnd:    end,
		Scope:        domain.Scope{Location: "au-sydney", NetworkID: "as-synthetic-1221"},
		NodeID:       nodeID,
		Requests:     requests,
		ErrorRate:    errorRate,
		CacheHitRate: hitRate,
		P95TTFBMS:    p95,
		Healthy:      true,
	}
}

func testConfig() Config {
	return Config{
		MinimumSamples:   100,
		MinimumHistory:   5,
		EWMAAlpha:        0.20,
		ErrorThreshold:   0.02,
		LatencyMultiple:  2,
		RobustZThreshold: 3,
	}
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
