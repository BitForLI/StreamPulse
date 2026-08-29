package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/app"
	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/detection"
	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/httpapi"
	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/publisher"
	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/repository"
	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/scoring"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		runHealthcheck()
		return
	}
	detectorConfig := detection.Config{
		MinimumSamples:   uint64(envInt("MINIMUM_SAMPLES", 100)),
		MinimumHistory:   envInt("MINIMUM_HISTORY_WINDOWS", 5),
		EWMAAlpha:        envFloat("EWMA_ALPHA", 0.20),
		ErrorThreshold:   envFloat("ERROR_RATE_THRESHOLD", 0.02),
		LatencyMultiple:  envFloat("LATENCY_BASELINE_MULTIPLE", 2.0),
		RobustZThreshold: envFloat("ROBUST_Z_THRESHOLD", 3.0),
	}
	scorerConfig := scoring.Config{
		MinimumSamples:    uint64(envInt("MINIMUM_SAMPLES", 100)),
		StaleAfter:        envDuration("METRICS_STALE_AFTER", 2*time.Minute),
		FutureTolerance:   envDuration("METRICS_FUTURE_TOLERANCE", 5*time.Second),
		SaturationLimit:   envFloat("SATURATION_LIMIT", 0.85),
		MaximumWeightStep: envFloat("MAXIMUM_WEIGHT_STEP", 0.20),
		BenefitMargin:     envFloat("BENEFIT_MARGIN", 0.0001),
	}
	serviceConfig := app.Config{
		HistoryWindows: envInt("HISTORY_WINDOWS", 15),
		TTL:            envDuration("RECOMMENDATION_TTL", 2*time.Minute),
		MinimumDwell:   envDuration("MINIMUM_DWELL", time.Minute),
		QueryVersion:   "node-quality-v1",
		ModelVersion:   "rule-ewma-mad-v1",
	}
	serviceConfig.ConfigHash = configHash(detectorConfig, scorerConfig, serviceConfig)

	clickHouse := repository.NewClickHouse(repository.ClickHouseConfig{
		Endpoint: env("CLICKHOUSE_URL", "http://clickhouse:8123"),
		User:     env("CLICKHOUSE_USER", "streampulse"),
		Password: env("CLICKHOUSE_PASSWORD", "streampulse-local"),
		Timeout:  envDuration("CLICKHOUSE_TIMEOUT", 3*time.Second),
	})
	kafkaPublisher := publisher.NewKafka(
		splitNonEmpty(env("KAFKA_BROKERS", "kafka:9092")),
		env("KAFKA_RECOMMENDATION_TOPIC", "cdn.recommendations.v1"),
	)
	defer kafkaPublisher.Close()

	service := app.New(
		serviceConfig,
		clickHouse,
		detection.New(detectorConfig),
		scoring.New(scorerConfig),
		kafkaPublisher,
		repository.NewMemoryAuditStore(),
		time.Now,
	)
	server := &http.Server{
		Addr:              env("HTTP_ADDR", ":8090"),
		Handler:           httpapi.New(service),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	shutdownContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-shutdownContext.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("graceful shutdown: %v", err)
		}
	}()

	log.Printf("recommendation API listening on %s model=%s config_hash=%s", server.Addr, serviceConfig.ModelVersion, serviceConfig.ConfigHash)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func runHealthcheck() {
	address := env("HTTP_ADDR", ":8090")
	if strings.HasPrefix(address, ":") {
		address = "127.0.0.1" + address
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + address + "/healthz")
	if err != nil {
		log.Printf("healthcheck: %v", err)
		os.Exit(1)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		log.Printf("healthcheck status: %d", response.StatusCode)
		os.Exit(1)
	}
}

func configHash(values ...any) string {
	payload, err := json.Marshal(values)
	if err != nil {
		panic(err)
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:8])
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(env(name, strconv.Itoa(fallback)))
	if err != nil || value <= 0 {
		log.Fatalf("invalid %s", name)
	}
	return value
}

func envFloat(name string, fallback float64) float64 {
	value, err := strconv.ParseFloat(env(name, strconv.FormatFloat(fallback, 'f', -1, 64)), 64)
	if err != nil || value < 0 {
		log.Fatalf("invalid %s", name)
	}
	return value
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(env(name, fallback.String()))
	if err != nil || value <= 0 {
		log.Fatalf("invalid %s", name)
	}
	return value
}

func splitNonEmpty(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	if len(result) == 0 {
		log.Fatal("KAFKA_BROKERS must contain at least one broker")
	}
	return result
}
