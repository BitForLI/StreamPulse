package domain

import "time"

type Scope struct {
	Location  string `json:"location"`
	NetworkID string `json:"network_id"`
}

func (s Scope) Key() string {
	return s.Location + "|" + s.NetworkID
}

type NodeMetric struct {
	WindowStart  time.Time `json:"window_start"`
	WindowEnd    time.Time `json:"window_end"`
	Scope        Scope     `json:"scope"`
	NodeID       string    `json:"node_id"`
	Requests     uint64    `json:"requests"`
	ErrorRate    float64   `json:"error_5xx_rate"`
	CacheHitRate float64   `json:"cache_hit_ratio"`
	OriginMS     float64   `json:"origin_ms_total"`
	P50TTFBMS    float64   `json:"ttfb_p50_ms"`
	P95TTFBMS    float64   `json:"ttfb_p95_ms"`
	P99TTFBMS    float64   `json:"ttfb_p99_ms"`
	Saturation   float64   `json:"saturation"`
	Healthy      bool      `json:"healthy"`
}

type Signal struct {
	NodeID      string   `json:"node_id"`
	Anomalous   bool     `json:"anomalous"`
	Severity    float64  `json:"severity"`
	ReasonCodes []string `json:"reason_codes"`
	BaselineMS  float64  `json:"baseline_ms"`
	RobustZ     float64  `json:"robust_z"`
}

type ExpectedDelta struct {
	P95TTFBDeltaMS float64 `json:"p95_ttfb_delta_ms"`
	ErrorRateDelta float64 `json:"error_rate_delta"`
	CostUnitsDelta float64 `json:"cost_units_delta"`
}

type Recommendation struct {
	SchemaVersion    int                   `json:"schema_version"`
	RecommendationID string                `json:"recommendation_id"`
	CreatedAt        time.Time             `json:"created_at"`
	ValidUntil       time.Time             `json:"valid_until"`
	Mode             string                `json:"mode"`
	Scope            Scope                 `json:"scope"`
	Action           string                `json:"action"`
	Current          map[string]float64    `json:"current"`
	Proposed         map[string]float64    `json:"proposed"`
	EvidenceWindow   string                `json:"evidence_window"`
	ReasonCodes      []string              `json:"reason_codes"`
	Expected         ExpectedDelta         `json:"expected"`
	Confidence       float64               `json:"confidence"`
	ModelVersion     string                `json:"model_version"`
	Revision         uint32                `json:"revision"`
	InputWindowStart time.Time             `json:"input_window_start"`
	InputWindowEnd   time.Time             `json:"input_window_end"`
	QueryVersion     string                `json:"query_version"`
	ConfigHash       string                `json:"config_hash"`
	Evidence         map[string]NodeMetric `json:"evidence"`
}

type EvaluationRequest struct {
	Scope          Scope              `json:"scope"`
	CurrentWeights map[string]float64 `json:"current_weights,omitempty"`
}

type EvaluationResult struct {
	Generated      bool            `json:"generated"`
	ReasonCodes    []string        `json:"reason_codes"`
	Recommendation *Recommendation `json:"recommendation,omitempty"`
}

type Acknowledgement struct {
	RecommendationID string    `json:"recommendation_id"`
	Actor            string    `json:"actor"`
	Status           string    `json:"status"`
	ObservedAt       time.Time `json:"observed_at"`
}

type Outcome struct {
	RecommendationID string        `json:"recommendation_id"`
	ObservedAt       time.Time     `json:"observed_at"`
	ObservedDelta    ExpectedDelta `json:"observed_delta"`
	Notes            string        `json:"notes,omitempty"`
}
