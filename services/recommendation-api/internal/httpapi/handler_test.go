package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/app"
	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/scoring"
)

func TestHealthDoesNotDependOnClickHouseButReadinessDoes(t *testing.T) {
	service := app.New(
		app.Config{HistoryWindows: 15, TTL: 2 * time.Minute, MinimumDwell: time.Minute},
		failingRepository{},
		noSignals{},
		noScore{},
		noPublish{},
		noAudit{},
		time.Now,
	)
	handler := New(service)

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d", health.Code)
	}

	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d body=%s", ready.Code, ready.Body.String())
	}
}

func TestEvaluateRejectsUnknownAndMultipleJSONValues(t *testing.T) {
	service := app.New(
		app.Config{HistoryWindows: 15, TTL: 2 * time.Minute, MinimumDwell: time.Minute},
		failingRepository{}, noSignals{}, noScore{}, noPublish{}, noAudit{}, time.Now,
	)
	handler := New(service)
	for _, body := range []string{
		`{"scope":{"location":"au-sydney","network_id":"as-synthetic-1221"},"unknown":true}`,
		`{} {}`,
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/recommendations/evaluate", strings.NewReader(body)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body %q status = %d", body, response.Code)
		}
	}
}

type failingRepository struct{}

func (failingRepository) Ping(context.Context) error { return errors.New("clickhouse unavailable") }
func (failingRepository) History(context.Context, domain.Scope, int) ([]domain.NodeMetric, error) {
	return nil, errors.New("clickhouse unavailable")
}
func (failingRepository) LatestRecommendation(context.Context, domain.Scope) (*domain.Recommendation, error) {
	return nil, errors.New("clickhouse unavailable")
}

type noSignals struct{}

func (noSignals) Evaluate([]domain.NodeMetric) map[string]domain.Signal { return nil }

type noScore struct{}

func (noScore) Propose(time.Time, []domain.NodeMetric, map[string]float64, map[string]domain.Signal) (scoring.Result, error) {
	return scoring.Result{}, nil
}

type noPublish struct{}

func (noPublish) Publish(context.Context, domain.Recommendation) error { return nil }

type noAudit struct{}

func (noAudit) Acknowledge(domain.Acknowledgement) error { return nil }
func (noAudit) Outcome(domain.Outcome) error             { return nil }
