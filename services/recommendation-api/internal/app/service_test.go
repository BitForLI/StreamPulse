package app

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/scoring"
)

func TestEvaluateProducesShadowRecommendationWithTTLAndDwell(t *testing.T) {
	now := time.Date(2026, 8, 29, 9, 30, 0, 0, time.UTC)
	scope := domain.Scope{Location: "au-sydney", NetworkID: "as-synthetic-1221"}
	metrics := sampleMetrics(now, scope)
	publisher := &fakePublisher{}
	service := New(
		testServiceConfig(),
		&fakeRepository{history: metrics},
		fakeDetector{signals: anomalySignals()},
		fakeScorer{result: sampleProposal(now, metrics)},
		publisher,
		&fakeAudit{},
		func() time.Time { return now },
	)

	result, err := service.Evaluate(context.Background(), domain.EvaluationRequest{
		Scope:          scope,
		CurrentWeights: map[string]float64{"edge-a": 0.5, "edge-b": 0.5},
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if !result.Generated || result.Recommendation == nil {
		t.Fatalf("expected recommendation: %#v", result)
	}
	recommendation := result.Recommendation
	if recommendation.Mode != "shadow" || recommendation.Action != "adjust_node_weights" {
		t.Fatalf("unsafe mode/action: %#v", recommendation)
	}
	if recommendation.ValidUntil.Sub(recommendation.CreatedAt) != 2*time.Minute {
		t.Fatalf("unexpected TTL: %s", recommendation.ValidUntil.Sub(recommendation.CreatedAt))
	}
	if recommendation.SchemaVersion != 1 || recommendation.RecommendationID == "" ||
		recommendation.ModelVersion != "rule-ewma-mad-v1" || recommendation.ConfigHash != "config-1234" {
		t.Fatalf("missing audit/contract fields: %#v", recommendation)
	}
	for nodeID, proposed := range recommendation.Proposed {
		if delta := math.Abs(proposed - recommendation.Current[nodeID]); delta > 0.200000001 {
			t.Fatalf("weight step exceeded for %s: %.6f", nodeID, delta)
		}
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published %d recommendations, want 1", len(publisher.published))
	}

	second, err := service.Evaluate(context.Background(), domain.EvaluationRequest{Scope: scope})
	if err != nil {
		t.Fatalf("second evaluate: %v", err)
	}
	if second.Generated || !containsReason(second.ReasonCodes, "MINIMUM_DWELL_ACTIVE") {
		t.Fatalf("minimum dwell was not enforced: %#v", second)
	}
	if len(publisher.published) != 1 {
		t.Fatal("dwell-blocked evaluation was published")
	}
}

func TestEvaluateNormalTrafficDoesNotPublish(t *testing.T) {
	now := time.Date(2026, 8, 29, 9, 30, 0, 0, time.UTC)
	scope := domain.Scope{Location: "au-sydney", NetworkID: "as-synthetic-1221"}
	publisher := &fakePublisher{}
	service := New(
		testServiceConfig(),
		&fakeRepository{history: sampleMetrics(now, scope)},
		fakeDetector{signals: map[string]domain.Signal{"edge-a": {NodeID: "edge-a"}}},
		fakeScorer{},
		publisher,
		&fakeAudit{},
		func() time.Time { return now },
	)

	result, err := service.Evaluate(context.Background(), domain.EvaluationRequest{Scope: scope})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if result.Generated || !containsReason(result.ReasonCodes, "NO_ANOMALY") {
		t.Fatalf("normal traffic generated a recommendation: %#v", result)
	}
	if len(publisher.published) != 0 {
		t.Fatal("normal traffic was published")
	}
}

func TestDependencyFailureReturnsTypedUnavailableAndDoesNotPublish(t *testing.T) {
	scope := domain.Scope{Location: "au-sydney", NetworkID: "as-synthetic-1221"}
	publisher := &fakePublisher{}
	service := New(
		testServiceConfig(),
		&fakeRepository{err: errors.New("connection refused")},
		fakeDetector{},
		fakeScorer{},
		publisher,
		&fakeAudit{},
		time.Now,
	)

	_, err := service.Evaluate(context.Background(), domain.EvaluationRequest{Scope: scope})
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected dependency unavailable, got %v", err)
	}
	if len(publisher.published) != 0 {
		t.Fatal("recommendation was published during dependency failure")
	}
}

func TestPublisherFailureDoesNotStartDwell(t *testing.T) {
	now := time.Date(2026, 8, 29, 9, 30, 0, 0, time.UTC)
	scope := domain.Scope{Location: "au-sydney", NetworkID: "as-synthetic-1221"}
	metrics := sampleMetrics(now, scope)
	publisher := &fakePublisher{err: errors.New("broker unavailable")}
	service := New(
		testServiceConfig(),
		&fakeRepository{history: metrics},
		fakeDetector{signals: anomalySignals()},
		fakeScorer{result: sampleProposal(now, metrics)},
		publisher,
		&fakeAudit{},
		func() time.Time { return now },
	)

	_, err := service.Evaluate(context.Background(), domain.EvaluationRequest{Scope: scope})
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected dependency unavailable, got %v", err)
	}
	if _, exists := service.lastGeneration(scope); exists {
		t.Fatal("failed publish incorrectly started the dwell timer")
	}
}

func TestInvalidScopeIsRejectedBeforeRepositoryQuery(t *testing.T) {
	repository := &fakeRepository{err: errors.New("must not be called")}
	service := New(testServiceConfig(), repository, fakeDetector{}, fakeScorer{}, &fakePublisher{}, &fakeAudit{}, time.Now)

	_, err := service.Evaluate(context.Background(), domain.EvaluationRequest{
		Scope: domain.Scope{Location: "Sydney OR 1=1", NetworkID: "public-isp"},
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected invalid request, got %v", err)
	}
}

type fakeRepository struct {
	history []domain.NodeMetric
	latest  *domain.Recommendation
	err     error
}

func (f *fakeRepository) Ping(context.Context) error { return f.err }

func (f *fakeRepository) History(context.Context, domain.Scope, int) ([]domain.NodeMetric, error) {
	return f.history, f.err
}

func (f *fakeRepository) LatestRecommendation(context.Context, domain.Scope) (*domain.Recommendation, error) {
	return f.latest, f.err
}

type fakeDetector struct {
	signals map[string]domain.Signal
}

func (f fakeDetector) Evaluate([]domain.NodeMetric) map[string]domain.Signal { return f.signals }

type fakeScorer struct {
	result scoring.Result
	err    error
}

func (f fakeScorer) Propose(time.Time, []domain.NodeMetric, map[string]float64, map[string]domain.Signal) (scoring.Result, error) {
	return f.result, f.err
}

type fakePublisher struct {
	published []domain.Recommendation
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, recommendation domain.Recommendation) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, recommendation)
	return nil
}

type fakeAudit struct{}

func (*fakeAudit) Acknowledge(context.Context, domain.Acknowledgement) error { return nil }
func (*fakeAudit) Outcome(context.Context, domain.Outcome) error             { return nil }

func testServiceConfig() Config {
	return Config{
		HistoryWindows: 15,
		TTL:            2 * time.Minute,
		MinimumDwell:   time.Minute,
		QueryVersion:   "node-quality-v1",
		ModelVersion:   "rule-ewma-mad-v1",
		ConfigHash:     "config-1234",
	}
}

func sampleMetrics(now time.Time, scope domain.Scope) []domain.NodeMetric {
	return []domain.NodeMetric{
		{WindowStart: now.Add(-90 * time.Second), WindowEnd: now.Add(-30 * time.Second), Scope: scope, NodeID: "edge-a", Requests: 500, P95TTFBMS: 220, ErrorRate: 0.1, CacheHitRate: 0.3, Healthy: true},
		{WindowStart: now.Add(-90 * time.Second), WindowEnd: now.Add(-30 * time.Second), Scope: scope, NodeID: "edge-b", Requests: 500, P95TTFBMS: 45, ErrorRate: 0.001, CacheHitRate: 0.8, Healthy: true},
	}
}

func anomalySignals() map[string]domain.Signal {
	return map[string]domain.Signal{
		"edge-a": {NodeID: "edge-a", Anomalous: true, Severity: 1, ReasonCodes: []string{"EWMA_MAD_LATENCY_ANOMALY"}},
		"edge-b": {NodeID: "edge-b"},
	}
}

func sampleProposal(now time.Time, metrics []domain.NodeMetric) scoring.Result {
	evidence := make(map[string]domain.NodeMetric)
	for _, metric := range metrics {
		evidence[metric.NodeID] = metric
	}
	return scoring.Result{
		Current:     map[string]float64{"edge-a": 0.5, "edge-b": 0.5},
		Proposed:    map[string]float64{"edge-a": 0.3, "edge-b": 0.7},
		Expected:    domain.ExpectedDelta{P95TTFBDeltaMS: -35, ErrorRateDelta: -0.02, CostUnitsDelta: 0.001},
		ReasonCodes: []string{"MAX_WEIGHT_STEP_OK", "EWMA_MAD_LATENCY_ANOMALY"},
		Evidence:    evidence,
		WindowStart: now.Add(-90 * time.Second),
		WindowEnd:   now.Add(-30 * time.Second),
	}
}

func containsReason(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
