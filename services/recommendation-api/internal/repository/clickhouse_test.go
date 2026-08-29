package repository

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
)

func TestHistoryUsesClickHouseParametersAndParsesRows(t *testing.T) {
	scope := domain.Scope{Location: "au-sydney' OR 1=1 --", NetworkID: "as-synthetic-1221"}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if got := request.URL.Query().Get("param_location"); got != scope.Location {
			t.Fatalf("location parameter = %q", got)
		}
		if got := request.URL.Query().Get("param_network"); got != scope.NetworkID {
			t.Fatalf("network parameter = %q", got)
		}
		if got := request.URL.Query().Get("param_windows"); got != "15" {
			t.Fatalf("windows parameter = %q", got)
		}
		query, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read query: %v", err)
		}
		if got := strings.Count(string(query), "now64(3) + INTERVAL 5 SECOND"); got != 2 {
			t.Fatalf("future-window guard count = %d", got)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"data":[{"window_start":"2026-08-29 09:00:00.000","window_end":"2026-08-29 09:01:00.000","node_id":"edge-a","requests":500,"error_5xx_rate":0.01,"cache_hit_ratio":0.8,"origin_ms_total":1000,"ttfb_p50_ms":20,"ttfb_p95_ms":40,"ttfb_p99_ms":50,"saturation":0.6}]}`))
	}))
	defer server.Close()
	repository := NewClickHouse(ClickHouseConfig{Endpoint: server.URL, Timeout: time.Second})

	metrics, err := repository.History(context.Background(), scope, 15)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(metrics) != 1 || metrics[0].NodeID != "edge-a" || metrics[0].WindowEnd.Location() != time.UTC {
		t.Fatalf("unexpected metrics: %#v", metrics)
	}
	if !metrics[0].Healthy || metrics[0].Saturation != 0.6 {
		t.Fatalf("missing default health/saturation: %#v", metrics[0])
	}
}

func TestLatestRecommendationParsesStoredJSONWeights(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"data":[{"recommendation_id":"rec-12345678","created_at":"2026-08-29 09:00:00.000","valid_until":"2026-08-29 09:02:00.000","revision":1,"mode":"shadow","action":"adjust_node_weights","current_json":"{\"edge-a\":0.5,\"edge-b\":0.5}","proposed_json":"{\"edge-a\":0.3,\"edge-b\":0.7}","evidence_window":"15m","reason_codes":["MAX_WEIGHT_STEP_OK"],"expected_p95_ttfb_delta_ms":-20,"expected_error_rate_delta":-0.01,"expected_cost_units_delta":0.2,"confidence":0.8,"model_version":"rule-ewma-mad-v1"}]}`))
	}))
	defer server.Close()
	repository := NewClickHouse(ClickHouseConfig{Endpoint: server.URL, Timeout: time.Second})

	recommendation, err := repository.LatestRecommendation(context.Background(), domain.Scope{Location: "au-sydney", NetworkID: "as-synthetic-1221"})
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if recommendation == nil || recommendation.Current["edge-a"] != 0.5 || recommendation.Proposed["edge-b"] != 0.7 {
		t.Fatalf("unexpected recommendation: %#v", recommendation)
	}
	if recommendation.ValidUntil.Sub(recommendation.CreatedAt) != 2*time.Minute {
		t.Fatalf("unexpected persisted TTL: %s", recommendation.ValidUntil.Sub(recommendation.CreatedAt))
	}
}

func TestPingReturnsBoundedClickHouseError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, strings.Repeat("failure", 20), http.StatusServiceUnavailable)
	}))
	defer server.Close()
	repository := NewClickHouse(ClickHouseConfig{Endpoint: server.URL, Timeout: time.Second})

	err := repository.Ping(context.Background())
	if err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("unexpected ping error: %v", err)
	}
}
