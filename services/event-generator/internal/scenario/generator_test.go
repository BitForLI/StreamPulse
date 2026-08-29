package scenario

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/BitForLI/StreamPulse/services/event-generator/internal/config"
	"github.com/BitForLI/StreamPulse/services/event-generator/internal/events"
)

func TestSameSeedProducesSameSequence(t *testing.T) {
	cfg := testConfig()
	firstRecords, firstManifest := runCollect(t, cfg)
	secondRecords, secondManifest := runCollect(t, cfg)
	if !reflect.DeepEqual(firstRecords, secondRecords) {
		t.Fatal("same seed and config produced a different record sequence")
	}
	if !reflect.DeepEqual(firstManifest, secondManifest) {
		t.Fatal("same seed and config produced a different manifest")
	}
}

func TestDuplicateAndSchemaErrorRatesAreAuditable(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = config.Duration(time.Second)
	cfg.RatePerSecond = 4
	cfg.DuplicateRate = 1
	cfg.SchemaErrorRate = 1
	cfg.Scenarios = nil
	records, manifest := runCollect(t, cfg)
	if manifest.DuplicateRecords != 4 {
		t.Fatalf("duplicate records=%d, want 4", manifest.DuplicateRecords)
	}
	if manifest.SchemaErrorRecords != 8 {
		t.Fatalf("schema-error records=%d, want 8 including exact duplicates", manifest.SchemaErrorRecords)
	}
	for _, record := range records {
		if record.Topic != events.DeliveryTopic || !record.SchemaInvalid {
			continue
		}
		var value map[string]any
		if err := json.Unmarshal(record.Value, &value); err != nil {
			t.Fatal(err)
		}
		if value["cache_status"] != "HOT" {
			t.Fatalf("invalid fixture did not contain the controlled enum error: %v", value)
		}
	}
}

func TestAllFaultLabelsAppearInTheirExpectedWindows(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = config.Duration(5 * time.Second)
	cfg.RatePerSecond = 60
	cfg.OutOfOrderMax = 0
	cfg.Scenarios = []config.Scenario{
		{At: 0, Type: "node_latency_spike", Target: "edge-a", Duration: config.Duration(time.Second), AddedLatencyMS: 100},
		{At: config.Duration(time.Second), Type: "node_5xx_spike", Target: "edge-b", Duration: config.Duration(time.Second), ErrorRate: 1},
		{At: config.Duration(2 * time.Second), Type: "isp_node_degradation", Target: "edge-a", Network: "as-synthetic-1", Duration: config.Duration(time.Second), AddedLatencyMS: 80, ErrorRate: 0.5},
		{At: config.Duration(3 * time.Second), Type: "popularity_shift", Duration: config.Duration(time.Second), MissRate: 1},
		{At: config.Duration(4 * time.Second), Type: "capacity_pressure", Target: "edge-b", Duration: config.Duration(time.Second), AddedLatencyMS: 70, ErrorRate: 0.2},
	}
	records, manifest := runCollect(t, cfg)
	found := map[string]bool{}
	for _, record := range records {
		if record.Topic != events.DeliveryTopic || record.Duplicate {
			continue
		}
		var value struct {
			Labels []string `json:"synthetic_labels"`
		}
		if err := json.Unmarshal(record.Value, &value); err != nil {
			t.Fatal(err)
		}
		for _, label := range value.Labels {
			found[label] = true
		}
	}
	for _, expected := range manifest.ExpectedLabels {
		if !found[expected.Type] {
			t.Errorf("expected label %q never appeared", expected.Type)
		}
	}
}

func TestDeliveryKeysAreBucketedAcrossLocationNetworkSessions(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = config.Duration(10 * time.Second)
	cfg.RatePerSecond = 60
	cfg.DuplicateRate = 0
	cfg.SchemaErrorRate = 0
	cfg.Scenarios = nil
	_, manifest := runCollect(t, cfg)
	counts := make([]int64, 0)
	for _, count := range manifest.DeliveryRecordsByKey {
		counts = append(counts, count)
	}
	if len(counts) < 8 {
		t.Fatalf("only %d delivery keys were used", len(counts))
	}
	var total, maximum int64
	for _, count := range counts {
		total += count
		if count > maximum {
			maximum = count
		}
	}
	average := float64(total) / float64(len(counts))
	if float64(maximum) > 4*average {
		t.Fatalf("key distribution too skewed: max=%d average=%.2f", maximum, average)
	}
}

func runCollect(t *testing.T, cfg config.Config) ([]events.Record, Manifest) {
	t.Helper()
	generator, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var records []events.Record
	manifest, err := generator.Run(context.Background(), "test-commit", map[string]string{"KAFKA_VERSION": "test"}, func(_ context.Context, record events.Record) error {
		record.Value = append([]byte(nil), record.Value...)
		records = append(records, record)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return records, manifest
}

func testConfig() config.Config {
	return config.Config{
		Seed:            20260827,
		StartTime:       "2026-08-27T00:00:00Z",
		Duration:        config.Duration(2 * time.Second),
		RatePerSecond:   20,
		OutOfOrderMax:   config.Duration(30 * time.Second),
		DuplicateRate:   0.001,
		SchemaErrorRate: 0.0005,
		Locations:       []config.Location{{Name: "au-sydney", Networks: []string{"as-synthetic-1", "as-synthetic-2"}}},
		Nodes: []config.Node{
			{ID: "edge-a", BaseTTFBMS: 25, CapacityRPS: 100},
			{ID: "edge-b", BaseTTFBMS: 35, CapacityRPS: 100},
		},
		Content: config.Content{Count: 100, Popularity: "zipf"},
	}
}
