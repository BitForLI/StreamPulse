package scoring

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
)

func TestProposeAppliesMaximumWeightStepAndImprovesExpectedQuality(t *testing.T) {
	now := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	metrics := []domain.NodeMetric{
		nodeMetric(now, "edge-a", 220, 0.12, 0.30, 0.70),
		nodeMetric(now, "edge-b", 45, 0.002, 0.82, 0.40),
		nodeMetric(now, "edge-c", 50, 0.003, 0.78, 0.45),
	}
	current := map[string]float64{"edge-a": 1.0 / 3, "edge-b": 1.0 / 3, "edge-c": 1.0 / 3}
	signals := map[string]domain.Signal{
		"edge-a": {NodeID: "edge-a", Anomalous: true, Severity: 1, ReasonCodes: []string{"EWMA_MAD_LATENCY_ANOMALY"}},
	}

	result, err := New(testScorerConfig()).Propose(now, metrics, current, signals)
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	var sum float64
	for nodeID, proposed := range result.Proposed {
		sum += proposed
		if delta := math.Abs(proposed - result.Current[nodeID]); delta > 0.200000001 {
			t.Fatalf("weight step for %s exceeded 20%%: %.6f", nodeID, delta)
		}
	}
	if math.Abs(sum-1) > 1e-9 {
		t.Fatalf("weights do not sum to one: %.12f", sum)
	}
	if result.Proposed["edge-a"] >= result.Current["edge-a"] {
		t.Fatal("degraded node weight was not reduced")
	}
	if result.Expected.P95TTFBDeltaMS >= 0 || result.Expected.ErrorRateDelta >= 0 {
		t.Fatalf("expected quality did not improve: %#v", result.Expected)
	}
	if !hasReason(result.ReasonCodes, "MAX_WEIGHT_STEP_OK") ||
		!hasReason(result.ReasonCodes, "EWMA_MAD_LATENCY_ANOMALY") {
		t.Fatalf("missing guardrail/evidence reasons: %#v", result.ReasonCodes)
	}
}

func TestStaleMetricsAreRejected(t *testing.T) {
	now := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	metrics := []domain.NodeMetric{
		nodeMetric(now.Add(-5*time.Minute), "edge-a", 220, 0.12, 0.30, 0.70),
		nodeMetric(now.Add(-5*time.Minute), "edge-b", 45, 0.002, 0.82, 0.40),
	}
	_, err := New(testScorerConfig()).Propose(now, metrics, nil, nil)
	if !errors.Is(err, ErrStaleMetrics) {
		t.Fatalf("expected stale metrics error, got %v", err)
	}
}

func TestFutureMetricsAreRejected(t *testing.T) {
	now := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	metrics := []domain.NodeMetric{
		nodeMetric(now.Add(time.Minute), "edge-a", 220, 0.12, 0.30, 0.70),
		nodeMetric(now.Add(time.Minute), "edge-b", 45, 0.002, 0.82, 0.40),
	}
	_, err := New(testScorerConfig()).Propose(now, metrics, nil, nil)
	if !errors.Is(err, ErrFutureMetrics) {
		t.Fatalf("expected future metrics error, got %v", err)
	}
}

func TestCapacityFilterKeepsAtLeastTwoCandidates(t *testing.T) {
	now := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	metrics := []domain.NodeMetric{
		nodeMetric(now, "edge-a", 220, 0.12, 0.30, 0.95),
		nodeMetric(now, "edge-b", 45, 0.002, 0.82, 0.40),
	}
	_, err := New(testScorerConfig()).Propose(now, metrics, nil, nil)
	if !errors.Is(err, ErrInsufficientCandidates) {
		t.Fatalf("expected insufficient candidates, got %v", err)
	}
}

func nodeMetric(end time.Time, nodeID string, p95, errorRate, hitRate, saturation float64) domain.NodeMetric {
	return domain.NodeMetric{
		WindowStart:  end.Add(-time.Minute),
		WindowEnd:    end.Add(-30 * time.Second),
		Scope:        domain.Scope{Location: "au-sydney", NetworkID: "as-synthetic-1221"},
		NodeID:       nodeID,
		Requests:     500,
		ErrorRate:    errorRate,
		CacheHitRate: hitRate,
		OriginMS:     (1 - hitRate) * 500 * 80,
		P95TTFBMS:    p95,
		Saturation:   saturation,
		Healthy:      true,
	}
}

func testScorerConfig() Config {
	return Config{
		MinimumSamples:    100,
		StaleAfter:        2 * time.Minute,
		FutureTolerance:   5 * time.Second,
		SaturationLimit:   0.85,
		MaximumWeightStep: 0.20,
		BenefitMargin:     0.0001,
	}
}

func hasReason(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
