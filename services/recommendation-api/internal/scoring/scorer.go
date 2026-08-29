package scoring

import (
	"errors"
	"math"
	"sort"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
)

var (
	ErrInsufficientCandidates = errors.New("fewer than two eligible nodes")
	ErrStaleMetrics           = errors.New("all candidate metrics are stale")
	ErrNoMaterialBenefit      = errors.New("proposed weights do not clear the benefit margin")
)

type Config struct {
	MinimumSamples    uint64
	StaleAfter        time.Duration
	SaturationLimit   float64
	MaximumWeightStep float64
	BenefitMargin     float64
}

type Result struct {
	Current     map[string]float64
	Proposed    map[string]float64
	Expected    domain.ExpectedDelta
	ReasonCodes []string
	Evidence    map[string]domain.NodeMetric
	WindowStart time.Time
	WindowEnd   time.Time
}

type Scorer struct {
	config Config
}

func New(config Config) Scorer {
	return Scorer{config: config}
}

func (s Scorer) Propose(
	now time.Time,
	metrics []domain.NodeMetric,
	current map[string]float64,
	signals map[string]domain.Signal,
) (Result, error) {
	eligible := make([]domain.NodeMetric, 0, len(metrics))
	staleCount := 0
	for _, metric := range latestByNode(metrics) {
		if now.Sub(metric.WindowEnd) > s.config.StaleAfter {
			staleCount++
			continue
		}
		if !metric.Healthy || metric.Requests < s.config.MinimumSamples || metric.Saturation >= s.config.SaturationLimit {
			continue
		}
		eligible = append(eligible, metric)
	}
	if len(eligible) < 2 {
		if staleCount > 0 && staleCount == len(latestByNode(metrics)) {
			return Result{}, ErrStaleMetrics
		}
		return Result{}, ErrInsufficientCandidates
	}
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].NodeID < eligible[j].NodeID })

	current = normalizedCurrent(current, eligible)
	latency := normalized(eligible, func(metric domain.NodeMetric) float64 { return metric.P95TTFBMS })
	errorsFactor := normalized(eligible, func(metric domain.NodeMetric) float64 { return metric.ErrorRate })
	qoe := normalized(eligible, func(metric domain.NodeMetric) float64 { return 1 - metric.CacheHitRate })
	cost := normalized(eligible, func(metric domain.NodeMetric) float64 {
		if metric.Requests == 0 {
			return 0
		}
		return metric.OriginMS / float64(metric.Requests)
	})
	saturation := normalized(eligible, func(metric domain.NodeMetric) float64 { return metric.Saturation })

	target := make(map[string]float64, len(eligible))
	var targetSum float64
	for i, metric := range eligible {
		penalty := 0.35*latency[i] + 0.25*errorsFactor[i] + 0.15*qoe[i] + 0.15*cost[i] + 0.10*saturation[i]
		if signal, ok := signals[metric.NodeID]; ok && signal.Anomalous {
			penalty += 0.25 * signal.Severity
		}
		target[metric.NodeID] = 1 / (0.05 + penalty)
		targetSum += target[metric.NodeID]
	}
	for nodeID := range target {
		target[nodeID] /= targetSum
	}

	lambda := 1.0
	for nodeID, targetWeight := range target {
		delta := math.Abs(targetWeight - current[nodeID])
		if delta > s.config.MaximumWeightStep {
			lambda = math.Min(lambda, s.config.MaximumWeightStep/delta)
		}
	}
	proposed := make(map[string]float64, len(target))
	for nodeID, targetWeight := range target {
		proposed[nodeID] = current[nodeID] + lambda*(targetWeight-current[nodeID])
	}

	currentPenalty := weightedPenalty(eligible, current)
	proposedPenalty := weightedPenalty(eligible, proposed)
	if currentPenalty-proposedPenalty < s.config.BenefitMargin {
		return Result{}, ErrNoMaterialBenefit
	}

	result := Result{
		Current:     current,
		Proposed:    proposed,
		ReasonCodes: []string{"BOUNDED_MULTI_OBJECTIVE_SCORE", "MAX_WEIGHT_STEP_OK", "CAPACITY_HEADROOM_OK"},
		Evidence:    make(map[string]domain.NodeMetric, len(eligible)),
	}
	for _, metric := range eligible {
		result.Evidence[metric.NodeID] = metric
		if result.WindowStart.IsZero() || metric.WindowStart.Before(result.WindowStart) {
			result.WindowStart = metric.WindowStart
		}
		if metric.WindowEnd.After(result.WindowEnd) {
			result.WindowEnd = metric.WindowEnd
		}
		if signal, ok := signals[metric.NodeID]; ok && signal.Anomalous {
			result.ReasonCodes = appendUnique(result.ReasonCodes, signal.ReasonCodes...)
		}
	}
	result.Expected = expectedDelta(eligible, current, proposed)
	return result, nil
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
	return result
}

func normalizedCurrent(current map[string]float64, metrics []domain.NodeMetric) map[string]float64 {
	result := make(map[string]float64, len(metrics))
	var sum float64
	for _, metric := range metrics {
		if current[metric.NodeID] > 0 {
			result[metric.NodeID] = current[metric.NodeID]
			sum += current[metric.NodeID]
		}
	}
	if sum == 0 {
		weight := 1 / float64(len(metrics))
		for _, metric := range metrics {
			result[metric.NodeID] = weight
		}
		return result
	}
	for nodeID := range result {
		result[nodeID] /= sum
	}
	return result
}

func normalized(metrics []domain.NodeMetric, value func(domain.NodeMetric) float64) []float64 {
	result := make([]float64, len(metrics))
	minimum, maximum := math.Inf(1), math.Inf(-1)
	for i, metric := range metrics {
		result[i] = value(metric)
		minimum = math.Min(minimum, result[i])
		maximum = math.Max(maximum, result[i])
	}
	if math.Abs(maximum-minimum) < 1e-12 {
		for i := range result {
			result[i] = 0
		}
		return result
	}
	for i := range result {
		result[i] = (result[i] - minimum) / (maximum - minimum)
	}
	return result
}

func weightedPenalty(metrics []domain.NodeMetric, weights map[string]float64) float64 {
	var value float64
	for _, metric := range metrics {
		originPerRequest := metric.OriginMS / math.Max(float64(metric.Requests), 1)
		value += weights[metric.NodeID] * (0.35*metric.P95TTFBMS/1000 + 0.25*metric.ErrorRate +
			0.15*(1-metric.CacheHitRate) + 0.15*originPerRequest/1000 + 0.10*metric.Saturation)
	}
	return value
}

func expectedDelta(metrics []domain.NodeMetric, current, proposed map[string]float64) domain.ExpectedDelta {
	var currentP95, proposedP95, currentError, proposedError, currentCost, proposedCost float64
	for _, metric := range metrics {
		originPerRequest := metric.OriginMS / math.Max(float64(metric.Requests), 1)
		currentP95 += current[metric.NodeID] * metric.P95TTFBMS
		proposedP95 += proposed[metric.NodeID] * metric.P95TTFBMS
		currentError += current[metric.NodeID] * metric.ErrorRate
		proposedError += proposed[metric.NodeID] * metric.ErrorRate
		currentCost += current[metric.NodeID] * originPerRequest
		proposedCost += proposed[metric.NodeID] * originPerRequest
	}
	return domain.ExpectedDelta{
		P95TTFBDeltaMS: proposedP95 - currentP95,
		ErrorRateDelta: proposedError - currentError,
		CostUnitsDelta: proposedCost - currentCost,
	}
}

func appendUnique(values []string, additions ...string) []string {
	seen := make(map[string]bool, len(values)+len(additions))
	for _, value := range values {
		seen[value] = true
	}
	for _, value := range additions {
		if !seen[value] {
			values = append(values, value)
			seen[value] = true
		}
	}
	return values
}
