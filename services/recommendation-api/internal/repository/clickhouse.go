package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
)

type ClickHouseConfig struct {
	Endpoint string
	User     string
	Password string
	Timeout  time.Duration
}

type ClickHouse struct {
	endpoint string
	user     string
	password string
	client   *http.Client
}

func NewClickHouse(config ClickHouseConfig) *ClickHouse {
	return &ClickHouse{
		endpoint: strings.TrimRight(config.Endpoint, "/") + "/",
		user:     config.User,
		password: config.Password,
		client:   &http.Client{Timeout: config.Timeout},
	}
}

func (c *ClickHouse) Ping(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, strings.NewReader("SELECT 1"))
	if err != nil {
		return err
	}
	request.SetBasicAuth(c.user, c.password)
	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return clickHouseError(response)
	}
	return nil
}

func (c *ClickHouse) History(ctx context.Context, scope domain.Scope, windows int) ([]domain.NodeMetric, error) {
	if windows <= 0 {
		windows = 30
	}
	query := `
WITH recent AS
(
    SELECT window_start, window_end, node_id, requests, error_5xx_rate,
           cache_hit_ratio, origin_ms_total, ttfb_p50_ms, ttfb_p95_ms, ttfb_p99_ms
    FROM streampulse.node_metrics_1m FINAL
    WHERE location = {location:String} AND network_id = {network:String}
    ORDER BY window_start DESC
    LIMIT {windows:UInt32} BY node_id
), saturation_by_node AS
(
    SELECT node_id,
           max(synthetic_node_rps) / greatest(max(synthetic_capacity_rps), 1) AS saturation
    FROM streampulse.raw_delivery FINAL
    WHERE location = {location:String} AND network_id = {network:String}
      AND event_time >= (SELECT min(window_start) FROM recent)
    GROUP BY node_id
)
SELECT recent.window_start, recent.window_end, recent.node_id, recent.requests,
       recent.error_5xx_rate, recent.cache_hit_ratio, recent.origin_ms_total,
       recent.ttfb_p50_ms, recent.ttfb_p95_ms, recent.ttfb_p99_ms,
       least(1.0, greatest(0.0, ifNull(saturation_by_node.saturation, 0.0))) AS saturation
FROM recent
LEFT JOIN saturation_by_node USING node_id
ORDER BY window_start, node_id
FORMAT JSON`
	params := url.Values{
		"param_location": {scope.Location},
		"param_network":  {scope.NetworkID},
		"param_windows":  {fmt.Sprintf("%d", windows)},
	}
	var payload struct {
		Data []metricRow `json:"data"`
	}
	if err := c.queryJSON(ctx, query, params, &payload); err != nil {
		return nil, err
	}
	metrics := make([]domain.NodeMetric, 0, len(payload.Data))
	for _, row := range payload.Data {
		windowStart, err := parseClickHouseTime(row.WindowStart)
		if err != nil {
			return nil, fmt.Errorf("parse window_start: %w", err)
		}
		windowEnd, err := parseClickHouseTime(row.WindowEnd)
		if err != nil {
			return nil, fmt.Errorf("parse window_end: %w", err)
		}
		metrics = append(metrics, domain.NodeMetric{
			WindowStart:  windowStart,
			WindowEnd:    windowEnd,
			Scope:        scope,
			NodeID:       row.NodeID,
			Requests:     row.Requests,
			ErrorRate:    row.ErrorRate,
			CacheHitRate: row.CacheHitRate,
			OriginMS:     row.OriginMS,
			P50TTFBMS:    row.P50TTFBMS,
			P95TTFBMS:    row.P95TTFBMS,
			P99TTFBMS:    row.P99TTFBMS,
			Saturation:   row.Saturation,
			Healthy:      true,
		})
	}
	return metrics, nil
}

func (c *ClickHouse) LatestRecommendation(ctx context.Context, scope domain.Scope) (*domain.Recommendation, error) {
	query := `
SELECT recommendation_id, created_at, valid_until, revision, mode, action,
       current_json, proposed_json, evidence_window, reason_codes,
       expected_p95_ttfb_delta_ms, expected_error_rate_delta,
       expected_cost_units_delta, confidence, model_version
FROM streampulse.recommendations FINAL
WHERE location = {location:String} AND network_id = {network:String}
ORDER BY created_at DESC, revision DESC
LIMIT 1
FORMAT JSON`
	params := url.Values{
		"param_location": {scope.Location},
		"param_network":  {scope.NetworkID},
	}
	var payload struct {
		Data []recommendationRow `json:"data"`
	}
	if err := c.queryJSON(ctx, query, params, &payload); err != nil {
		return nil, err
	}
	if len(payload.Data) == 0 {
		return nil, nil
	}
	row := payload.Data[0]
	createdAt, err := parseClickHouseTime(row.CreatedAt)
	if err != nil {
		return nil, err
	}
	validUntil, err := parseClickHouseTime(row.ValidUntil)
	if err != nil {
		return nil, err
	}
	current := make(map[string]float64)
	proposed := make(map[string]float64)
	if err := json.Unmarshal([]byte(row.CurrentJSON), &current); err != nil {
		return nil, fmt.Errorf("decode current weights: %w", err)
	}
	if err := json.Unmarshal([]byte(row.ProposedJSON), &proposed); err != nil {
		return nil, fmt.Errorf("decode proposed weights: %w", err)
	}
	return &domain.Recommendation{
		SchemaVersion:    1,
		RecommendationID: row.RecommendationID,
		CreatedAt:        createdAt,
		ValidUntil:       validUntil,
		Revision:         row.Revision,
		Mode:             row.Mode,
		Scope:            scope,
		Action:           row.Action,
		Current:          current,
		Proposed:         proposed,
		EvidenceWindow:   row.EvidenceWindow,
		ReasonCodes:      row.ReasonCodes,
		Expected: domain.ExpectedDelta{
			P95TTFBDeltaMS: row.ExpectedP95,
			ErrorRateDelta: row.ExpectedError,
			CostUnitsDelta: row.ExpectedCost,
		},
		Confidence:   row.Confidence,
		ModelVersion: row.ModelVersion,
	}, nil
}

func (c *ClickHouse) queryJSON(ctx context.Context, query string, params url.Values, destination any) error {
	endpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return err
	}
	endpoint.RawQuery = params.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewBufferString(query))
	if err != nil {
		return err
	}
	request.SetBasicAuth(c.user, c.password)
	request.Header.Set("Content-Type", "text/plain")
	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return clickHouseError(response)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 8<<20))
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode ClickHouse response: %w", err)
	}
	return nil
}

func clickHouseError(response *http.Response) error {
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	return fmt.Errorf("ClickHouse status %d: %s", response.StatusCode, strings.TrimSpace(string(payload)))
}

func parseClickHouseTime(value string) (time.Time, error) {
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
	} {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, errors.New("unsupported ClickHouse time " + value)
}

type metricRow struct {
	WindowStart  string  `json:"window_start"`
	WindowEnd    string  `json:"window_end"`
	NodeID       string  `json:"node_id"`
	Requests     uint64  `json:"requests"`
	ErrorRate    float64 `json:"error_5xx_rate"`
	CacheHitRate float64 `json:"cache_hit_ratio"`
	OriginMS     float64 `json:"origin_ms_total"`
	P50TTFBMS    float64 `json:"ttfb_p50_ms"`
	P95TTFBMS    float64 `json:"ttfb_p95_ms"`
	P99TTFBMS    float64 `json:"ttfb_p99_ms"`
	Saturation   float64 `json:"saturation"`
}

type recommendationRow struct {
	RecommendationID string   `json:"recommendation_id"`
	CreatedAt        string   `json:"created_at"`
	ValidUntil       string   `json:"valid_until"`
	Revision         uint32   `json:"revision"`
	Mode             string   `json:"mode"`
	Action           string   `json:"action"`
	CurrentJSON      string   `json:"current_json"`
	ProposedJSON     string   `json:"proposed_json"`
	EvidenceWindow   string   `json:"evidence_window"`
	ReasonCodes      []string `json:"reason_codes"`
	ExpectedP95      float64  `json:"expected_p95_ttfb_delta_ms"`
	ExpectedError    float64  `json:"expected_error_rate_delta"`
	ExpectedCost     float64  `json:"expected_cost_units_delta"`
	Confidence       float64  `json:"confidence"`
	ModelVersion     string   `json:"model_version"`
}
