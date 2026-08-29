package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/app"
	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
)

type Handler struct {
	service *app.Service
}

func New(service *app.Service) http.Handler {
	handler := &Handler{service: service}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.health)
	mux.HandleFunc("GET /readyz", handler.ready)
	mux.HandleFunc("GET /v1/scopes/{location}/{network}/metrics", handler.metrics)
	mux.HandleFunc("GET /v1/scopes/{location}/{network}/recommendations/latest", handler.latest)
	mux.HandleFunc("POST /v1/recommendations/evaluate", handler.evaluate)
	mux.HandleFunc("POST /v1/recommendations/{id}/ack", handler.acknowledge)
	mux.HandleFunc("POST /v1/recommendations/{id}/outcome", handler.outcome)
	return mux
}

func (h *Handler) health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) ready(response http.ResponseWriter, request *http.Request) {
	ctx, cancel := contextWithTimeout(request, 2*time.Second)
	defer cancel()
	if err := h.service.Ready(ctx); err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handler) metrics(response http.ResponseWriter, request *http.Request) {
	metrics, err := h.service.Metrics(request.Context(), pathScope(request))
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"metrics": metrics})
}

func (h *Handler) latest(response http.ResponseWriter, request *http.Request) {
	recommendation, err := h.service.Latest(request.Context(), pathScope(request))
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, recommendation)
}

func (h *Handler) evaluate(response http.ResponseWriter, request *http.Request) {
	var evaluation domain.EvaluationRequest
	if err := decodeJSON(request, &evaluation); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	ctx, cancel := contextWithTimeout(request, 5*time.Second)
	defer cancel()
	result, err := h.service.Evaluate(ctx, evaluation)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (h *Handler) acknowledge(response http.ResponseWriter, request *http.Request) {
	var acknowledgement domain.Acknowledgement
	if err := decodeJSON(request, &acknowledgement); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if err := h.service.Acknowledge(request.PathValue("id"), acknowledgement); err != nil {
		writeError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (h *Handler) outcome(response http.ResponseWriter, request *http.Request) {
	var outcome domain.Outcome
	if err := decodeJSON(request, &outcome); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if err := h.service.RecordOutcome(request.PathValue("id"), outcome); err != nil {
		writeError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func pathScope(request *http.Request) domain.Scope {
	return domain.Scope{Location: request.PathValue("location"), NetworkID: request.PathValue("network")}
}

func decodeJSON(request *http.Request, destination any) error {
	defer request.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("multiple JSON values")
	}
	return nil
}

func writeError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, app.ErrInvalidRequest):
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, app.ErrRecommendationNotFound):
		writeJSON(response, http.StatusNotFound, map[string]string{"error": err.Error()})
	case errors.Is(err, app.ErrDependencyUnavailable):
		writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
	default:
		writeJSON(response, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}

func writeJSON(response http.ResponseWriter, status int, payload any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(payload)
}

func contextWithTimeout(request *http.Request, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(request.Context(), timeout)
}
