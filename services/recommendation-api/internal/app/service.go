package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/scoring"
)

var (
	ErrInvalidRequest         = errors.New("invalid request")
	ErrDependencyUnavailable  = errors.New("dependency unavailable")
	ErrRecommendationNotFound = errors.New("recommendation not found")
)

var (
	locationPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,127}$`)
	networkPattern  = regexp.MustCompile(`^(as-synthetic-[A-Za-z0-9._-]+|anon:[0-9a-f]{16,64}|UNKNOWN)$`)
)

type MetricRepository interface {
	Ping(context.Context) error
	History(context.Context, domain.Scope, int) ([]domain.NodeMetric, error)
	LatestRecommendation(context.Context, domain.Scope) (*domain.Recommendation, error)
}

type SignalDetector interface {
	Evaluate([]domain.NodeMetric) map[string]domain.Signal
}

type WeightScorer interface {
	Propose(time.Time, []domain.NodeMetric, map[string]float64, map[string]domain.Signal) (scoring.Result, error)
}

type Publisher interface {
	Publish(context.Context, domain.Recommendation) error
}

type AuditStore interface {
	Acknowledge(domain.Acknowledgement) error
	Outcome(domain.Outcome) error
}

type Config struct {
	HistoryWindows int
	TTL            time.Duration
	MinimumDwell   time.Duration
	QueryVersion   string
	ModelVersion   string
	ConfigHash     string
}

type Service struct {
	config     Config
	repository MetricRepository
	detector   SignalDetector
	scorer     WeightScorer
	publisher  Publisher
	audit      AuditStore
	now        func() time.Time

	mu            sync.Mutex
	lastGenerated map[string]time.Time
}

func New(
	config Config,
	repository MetricRepository,
	detector SignalDetector,
	scorer WeightScorer,
	publisher Publisher,
	audit AuditStore,
	now func() time.Time,
) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{
		config:        config,
		repository:    repository,
		detector:      detector,
		scorer:        scorer,
		publisher:     publisher,
		audit:         audit,
		now:           now,
		lastGenerated: make(map[string]time.Time),
	}
}

func (s *Service) Ready(ctx context.Context) error {
	if err := s.repository.Ping(ctx); err != nil {
		return fmt.Errorf("%w: clickhouse: %v", ErrDependencyUnavailable, err)
	}
	return nil
}

func (s *Service) Metrics(ctx context.Context, scope domain.Scope) ([]domain.NodeMetric, error) {
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	metrics, err := s.repository.History(ctx, scope, s.config.HistoryWindows)
	if err != nil {
		return nil, fmt.Errorf("%w: clickhouse history: %v", ErrDependencyUnavailable, err)
	}
	return latestByNode(metrics), nil
}

func (s *Service) Latest(ctx context.Context, scope domain.Scope) (*domain.Recommendation, error) {
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	recommendation, err := s.repository.LatestRecommendation(ctx, scope)
	if err != nil {
		return nil, fmt.Errorf("%w: clickhouse latest recommendation: %v", ErrDependencyUnavailable, err)
	}
	if recommendation == nil {
		return nil, ErrRecommendationNotFound
	}
	return recommendation, nil
}

func (s *Service) Evaluate(ctx context.Context, request domain.EvaluationRequest) (domain.EvaluationResult, error) {
	if err := validateScope(request.Scope); err != nil {
		return domain.EvaluationResult{}, err
	}
	history, err := s.repository.History(ctx, request.Scope, s.config.HistoryWindows)
	if err != nil {
		return domain.EvaluationResult{}, fmt.Errorf("%w: clickhouse history: %v", ErrDependencyUnavailable, err)
	}
	if len(history) == 0 {
		return domain.EvaluationResult{Generated: false, ReasonCodes: []string{"INSUFFICIENT_HISTORY"}}, nil
	}

	signals := s.detector.Evaluate(history)
	if !hasAnomaly(signals) {
		return domain.EvaluationResult{Generated: false, ReasonCodes: []string{"NO_ANOMALY"}}, nil
	}

	now := s.now().UTC()
	if last, ok := s.lastGeneration(request.Scope); ok && now.Sub(last) < s.config.MinimumDwell {
		return domain.EvaluationResult{Generated: false, ReasonCodes: []string{"MINIMUM_DWELL_ACTIVE"}}, nil
	}
	latest, err := s.repository.LatestRecommendation(ctx, request.Scope)
	if err != nil {
		return domain.EvaluationResult{}, fmt.Errorf("%w: clickhouse latest recommendation: %v", ErrDependencyUnavailable, err)
	}
	if latest != nil && now.Sub(latest.CreatedAt) < s.config.MinimumDwell {
		return domain.EvaluationResult{Generated: false, ReasonCodes: []string{"MINIMUM_DWELL_ACTIVE"}}, nil
	}

	proposal, err := s.scorer.Propose(now, history, request.CurrentWeights, signals)
	if err != nil {
		switch {
		case errors.Is(err, scoring.ErrStaleMetrics):
			return domain.EvaluationResult{Generated: false, ReasonCodes: []string{"STALE_METRICS"}}, nil
		case errors.Is(err, scoring.ErrFutureMetrics):
			return domain.EvaluationResult{Generated: false, ReasonCodes: []string{"FUTURE_METRICS"}}, nil
		case errors.Is(err, scoring.ErrInsufficientCandidates):
			return domain.EvaluationResult{Generated: false, ReasonCodes: []string{"INSUFFICIENT_HEALTHY_CANDIDATES"}}, nil
		case errors.Is(err, scoring.ErrNoMaterialBenefit):
			return domain.EvaluationResult{Generated: false, ReasonCodes: []string{"BENEFIT_MARGIN_NOT_MET"}}, nil
		default:
			return domain.EvaluationResult{}, err
		}
	}

	recommendation := domain.Recommendation{
		SchemaVersion:    1,
		RecommendationID: recommendationID(request.Scope, proposal.WindowEnd, s.config.ConfigHash),
		CreatedAt:        now,
		ValidUntil:       now.Add(s.config.TTL),
		Mode:             "shadow",
		Scope:            request.Scope,
		Action:           "adjust_node_weights",
		Current:          proposal.Current,
		Proposed:         proposal.Proposed,
		EvidenceWindow:   fmt.Sprintf("%dm", s.config.HistoryWindows),
		ReasonCodes:      sortedUnique(proposal.ReasonCodes),
		Expected:         proposal.Expected,
		Confidence:       confidence(signals, proposal.Evidence),
		ModelVersion:     s.config.ModelVersion,
		Revision:         1,
		InputWindowStart: proposal.WindowStart,
		InputWindowEnd:   proposal.WindowEnd,
		QueryVersion:     s.config.QueryVersion,
		ConfigHash:       s.config.ConfigHash,
		Evidence:         proposal.Evidence,
	}
	if recommendation.ValidUntil.Sub(recommendation.CreatedAt) != s.config.TTL {
		return domain.EvaluationResult{}, errors.New("internal TTL invariant failed")
	}
	if err := s.publisher.Publish(ctx, recommendation); err != nil {
		return domain.EvaluationResult{}, fmt.Errorf("%w: kafka publish: %v", ErrDependencyUnavailable, err)
	}
	s.recordGeneration(request.Scope, now)
	return domain.EvaluationResult{
		Generated:      true,
		ReasonCodes:    recommendation.ReasonCodes,
		Recommendation: &recommendation,
	}, nil
}

func (s *Service) Acknowledge(id string, acknowledgement domain.Acknowledgement) error {
	if id == "" || acknowledgement.Actor == "" || acknowledgement.Status == "" {
		return ErrInvalidRequest
	}
	acknowledgement.RecommendationID = id
	if acknowledgement.ObservedAt.IsZero() {
		acknowledgement.ObservedAt = s.now().UTC()
	}
	return s.audit.Acknowledge(acknowledgement)
}

func (s *Service) RecordOutcome(id string, outcome domain.Outcome) error {
	if id == "" {
		return ErrInvalidRequest
	}
	outcome.RecommendationID = id
	if outcome.ObservedAt.IsZero() {
		outcome.ObservedAt = s.now().UTC()
	}
	return s.audit.Outcome(outcome)
}

func (s *Service) lastGeneration(scope domain.Scope) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	last, ok := s.lastGenerated[scope.Key()]
	return last, ok
}

func (s *Service) recordGeneration(scope domain.Scope, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastGenerated[scope.Key()] = at
}

func validateScope(scope domain.Scope) error {
	if !locationPattern.MatchString(scope.Location) || !networkPattern.MatchString(scope.NetworkID) {
		return ErrInvalidRequest
	}
	return nil
}

func hasAnomaly(signals map[string]domain.Signal) bool {
	for _, signal := range signals {
		if signal.Anomalous {
			return true
		}
	}
	return false
}

func recommendationID(scope domain.Scope, windowEnd time.Time, configHash string) string {
	hash := sha256.Sum256([]byte(scope.Key() + "|" + windowEnd.UTC().Format(time.RFC3339Nano) + "|" + configHash))
	return "rec-" + hex.EncodeToString(hash[:12])
}

func confidence(signals map[string]domain.Signal, evidence map[string]domain.NodeMetric) float64 {
	var maxSeverity float64
	var samples uint64
	for nodeID, metric := range evidence {
		samples += metric.Requests
		maxSeverity = math.Max(maxSeverity, signals[nodeID].Severity)
	}
	sampleFactor := math.Min(1, float64(samples)/3000)
	return math.Round(math.Min(0.99, 0.45+0.35*maxSeverity+0.20*sampleFactor)*1000) / 1000
}

func latestByNode(metrics []domain.NodeMetric) []domain.NodeMetric {
	latest := make(map[string]domain.NodeMetric)
	for _, metric := range metrics {
		current, ok := latest[metric.NodeID]
		if !ok || metric.WindowEnd.After(current.WindowEnd) {
			latest[metric.NodeID] = metric
		}
	}
	result := make([]domain.NodeMetric, 0, len(latest))
	for _, metric := range latest {
		result = append(result, metric)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	return result
}

func sortedUnique(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}
