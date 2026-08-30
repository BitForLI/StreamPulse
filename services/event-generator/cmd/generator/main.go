package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/BitForLI/StreamPulse/services/event-generator/internal/config"
	"github.com/BitForLI/StreamPulse/services/event-generator/internal/scenario"
	"github.com/BitForLI/StreamPulse/services/event-generator/internal/sink"
)

func main() {
	configPath := flag.String("config", "../../experiments/scenarios/baseline.yaml", "scenario YAML")
	output := flag.String("output", "jsonl", "jsonl or kafka")
	outputPath := flag.String("output-file", "-", "JSONL path, or - for stdout")
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	manifestPath := flag.String("manifest", "run-manifest.json", "generated run manifest")
	gitCommit := flag.String("git-commit", envOr("GIT_COMMIT", "uncommitted-or-unknown"), "source revision")
	startTime := flag.String("start-time", "", "optional RFC3339 simulation start override; recorded in the manifest")
	seed := flag.String("seed", "", "optional integer seed override; recorded in the manifest")
	deliveryTopic := flag.String("delivery-topic", "", "optional delivery topic override for isolated experiments")
	flag.Parse()

	cfg, _, err := config.Load(*configPath)
	check(err)
	overridden := false
	if *startTime != "" {
		cfg.StartTime = *startTime
		overridden = true
	}
	if *seed != "" {
		cfg.Seed, err = strconv.ParseInt(*seed, 10, 64)
		check(err)
		overridden = true
	}
	if *deliveryTopic != "" {
		cfg.DeliveryTopic = *deliveryTopic
		overridden = true
	}
	if overridden {
		check(cfg.Validate())
	}
	generator, err := scenario.New(cfg)
	check(err)

	var target sink.Sink
	switch *output {
	case "jsonl":
		target, err = sink.NewJSONLines(*outputPath)
	case "kafka":
		target, err = sink.NewKafka(*brokers)
	default:
		err = fmt.Errorf("unsupported output %q", *output)
	}
	check(err)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	manifest, runErr := generator.Run(ctx, *gitCommit, componentVersions(), target.Write)
	closeErr := target.Close()
	check(runErr)
	check(closeErr)
	check(writeJSON(*manifestPath, manifest))
}

func componentVersions() map[string]string {
	keys := []string{"FLINK_VERSION", "KAFKA_VERSION", "CLICKHOUSE_VERSION", "GRAFANA_VERSION", "VECTOR_VERSION"}
	versions := make(map[string]string, len(keys))
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			versions[key] = value
		}
	}
	return versions
}

func writeJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
		return err
	}
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "event-generator:", err)
		os.Exit(1)
	}
}
