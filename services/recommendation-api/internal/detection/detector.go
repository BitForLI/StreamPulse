package detection

import (
	"math"
	"sort"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
)

type Config struct {
	MinimumSamples   uint64
	MinimumHistory   int
	EWMAAlpha        float64
	ErrorThreshold   float64
	LatencyMultiple  float64
	RobustZThreshold float64
}

type Detector struct {
	config Config
}

func New(config Config) Detector {
	return Detector{config: config}
}

func (d Detector) Evaluate(history []domain.NodeMetric) map[string]domain.Signal {
	byNode := make(map[string][]domain.NodeMetric)
	for _, metric := range history {
		byNode[metric.NodeID] = append(byNode[metric.NodeID], metric)
	}

	current := make([]domain.NodeMetric, 0, len(byNode))
	for nodeID := range byNode {
		sort.Slice(byNode[nodeID], func(i, j int) bool {
			return byNode[nodeID][i].WindowEnd.Before(byNode[nodeID][j].WindowEnd)
		})
		current = append(current, byNode[nodeID][len(byNode[nodeID])-1])
	}
	locationBaseline := median(metricValues(current, func(metric domain.NodeMetric) float64 {
		return metric.P95TTFBMS
	}))

	result := make(map[string]domain.Signal, len(byNode))
	for nodeID, samples := range byNode {
		latest := samples[len(samples)-1]
		signal := domain.Signal{NodeID: nodeID, BaselineMS: locationBaseline}
		if latest.Requests >= d.config.MinimumSamples &&
			latest.ErrorRate > d.config.ErrorThreshold &&
			locationBaseline > 0 && latest.P95TTFBMS > d.config.LatencyMultiple*locationBaseline {
			signal.Anomalous = true
			signal.Severity = clamp01(math.Max(
				latest.ErrorRate/d.config.ErrorThreshold-1,
				latest.P95TTFBMS/(d.config.LatencyMultiple*locationBaseline)-1,
			))
			signal.ReasonCodes = append(signal.ReasonCodes, "RULE_LATENCY_ERROR_ANOMALY")
		}

		if len(samples)-1 >= d.config.MinimumHistory {
			past := samples[:len(samples)-1]
			pastP95 := metricValues(past, func(metric domain.NodeMetric) float64 { return metric.P95TTFBMS })
			pastErrors := metricValues(past, func(metric domain.NodeMetric) float64 { return metric.ErrorRate })
			baseline := frozenEWMA(pastP95, d.config.EWMAAlpha)
			robustZ := robustZ(latest.P95TTFBMS, pastP95)
			errorBaseline := frozenEWMA(pastErrors, d.config.EWMAAlpha)
			errorDelta := latest.ErrorRate - errorBaseline
			signal.BaselineMS = baseline
			signal.RobustZ = robustZ

			latencyAnomaly := baseline > 0 &&
				latest.P95TTFBMS > 1.5*baseline && robustZ >= d.config.RobustZThreshold
			errorAnomaly := latest.Requests >= d.config.MinimumSamples &&
				latest.ErrorRate > d.config.ErrorThreshold && errorDelta > 0.01
			missAnomaly := latest.CacheHitRate < 0.35 &&
				latest.CacheHitRate < median(metricValues(past, func(metric domain.NodeMetric) float64 {
					return metric.CacheHitRate
				}))-0.20

			if latencyAnomaly {
				signal.Anomalous = true
				signal.Severity = math.Max(signal.Severity, clamp01((latest.P95TTFBMS/baseline-1)/2))
				signal.ReasonCodes = appendUnique(signal.ReasonCodes, "EWMA_MAD_LATENCY_ANOMALY")
			}
			if errorAnomaly {
				signal.Anomalous = true
				signal.Severity = math.Max(signal.Severity, clamp01(errorDelta/0.20))
				signal.ReasonCodes = appendUnique(signal.ReasonCodes, "EWMA_ERROR_RATE_ANOMALY")
			}
			if missAnomaly {
				signal.Anomalous = true
				signal.Severity = math.Max(signal.Severity, clamp01(1-latest.CacheHitRate))
				signal.ReasonCodes = appendUnique(signal.ReasonCodes, "MAD_CACHE_MISS_ANOMALY")
			}
		}

		result[nodeID] = signal
	}
	return result
}

func frozenEWMA(values []float64, alpha float64) float64 {
	if len(values) == 0 {
		return 0
	}
	baseline := values[0]
	accepted := []float64{values[0]}
	for _, value := range values[1:] {
		z := math.Abs(robustZ(value, accepted))
		if z <= 4 || len(accepted) < 5 {
			baseline = alpha*value + (1-alpha)*baseline
			accepted = append(accepted, value)
		}
	}
	return baseline
}

func robustZ(value float64, history []float64) float64 {
	if len(history) == 0 {
		return 0
	}
	center := median(history)
	deviations := make([]float64, 0, len(history))
	for _, sample := range history {
		deviations = append(deviations, math.Abs(sample-center))
	}
	mad := median(deviations)
	if mad < 1e-9 {
		if math.Abs(value-center) < 1e-9 {
			return 0
		}
		return math.Inf(1)
	}
	return math.Abs(0.6745 * (value - center) / mad)
}

func metricValues(metrics []domain.NodeMetric, value func(domain.NodeMetric) float64) []float64 {
	values := make([]float64, 0, len(metrics))
	for _, metric := range metrics {
		values = append(values, value(metric))
	}
	return values
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[middle-1] + sorted[middle]) / 2
	}
	return sorted[middle]
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func clamp01(value float64) float64 {
	return math.Max(0, math.Min(1, value))
}
