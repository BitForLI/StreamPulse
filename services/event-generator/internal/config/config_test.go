package config

import (
	"testing"
	"time"
)

func TestRejectsOutOfOrderBeyondContract(t *testing.T) {
	cfg := minimalConfig()
	cfg.OutOfOrderMax = Duration(31 * time.Second)
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected out_of_order_max above 30s to be rejected")
	}
}

func TestRejectsScenarioOutsideRun(t *testing.T) {
	cfg := minimalConfig()
	cfg.Scenarios = []Scenario{{
		At:       Duration(9 * time.Second),
		Type:     "node_latency_spike",
		Target:   "edge-a",
		Duration: Duration(2 * time.Second),
	}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected scenario extending beyond the run to be rejected")
	}
}

func TestRejectsInvalidStartTimeOverride(t *testing.T) {
	cfg := minimalConfig()
	cfg.StartTime = "now-minus-seven-minutes"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected non-RFC3339 start time to be rejected")
	}
}

func minimalConfig() Config {
	return Config{
		Seed:          1,
		StartTime:     "2026-08-27T00:00:00Z",
		Duration:      Duration(10 * time.Second),
		RatePerSecond: 10,
		OutOfOrderMax: Duration(time.Second),
		Locations:     []Location{{Name: "au-sydney", Networks: []string{"as-synthetic-1"}}},
		Nodes:         []Node{{ID: "edge-a", BaseTTFBMS: 20, CapacityRPS: 100}},
		Content:       Content{Count: 10, Popularity: "zipf"},
	}
}
