package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) Value() time.Duration { return time.Duration(d) }
func (d Duration) String() string       { return time.Duration(d).String() }

type Location struct {
	Name     string   `yaml:"name"`
	Networks []string `yaml:"networks"`
}

type Node struct {
	ID          string  `yaml:"id"`
	BaseTTFBMS  float64 `yaml:"base_ttfb_ms"`
	CapacityRPS int     `yaml:"capacity_rps"`
}

type Content struct {
	Count      int    `yaml:"count"`
	Popularity string `yaml:"popularity"`
}

type Scenario struct {
	At                Duration `yaml:"at"`
	Type              string   `yaml:"type"`
	Target            string   `yaml:"target"`
	Network           string   `yaml:"network"`
	Duration          Duration `yaml:"duration"`
	AddedLatencyMS    float64  `yaml:"added_latency_ms"`
	ErrorRate         float64  `yaml:"error_rate"`
	MissRate          float64  `yaml:"miss_rate"`
	TargetCapacityRPS int      `yaml:"target_capacity_rps"`
}

type Config struct {
	Seed            int64      `yaml:"seed"`
	StartTime       string     `yaml:"start_time"`
	Duration        Duration   `yaml:"duration"`
	RatePerSecond   int        `yaml:"rate_per_second"`
	OutOfOrderMax   Duration   `yaml:"out_of_order_max"`
	DuplicateRate   float64    `yaml:"duplicate_rate"`
	SchemaErrorRate float64    `yaml:"schema_error_rate"`
	DeliveryBuckets int        `yaml:"delivery_key_buckets"`
	DeliveryTopic   string     `yaml:"delivery_topic"`
	Locations       []Location `yaml:"locations"`
	Nodes           []Node     `yaml:"nodes"`
	Content         Content    `yaml:"content"`
	Scenarios       []Scenario `yaml:"scenarios"`
}

func Load(path string) (Config, []byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, nil, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, nil, err
	}
	return cfg, raw, nil
}

func (c Config) ParsedStartTime() (time.Time, error) {
	t, err := time.Parse(time.RFC3339, c.StartTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("start_time must be RFC3339: %w", err)
	}
	return t.UTC(), nil
}

func (c Config) Validate() error {
	if _, err := c.ParsedStartTime(); err != nil {
		return err
	}
	if c.Duration.Value() <= 0 || c.RatePerSecond <= 0 {
		return fmt.Errorf("duration and rate_per_second must be positive")
	}
	if c.OutOfOrderMax.Value() < 0 || c.OutOfOrderMax.Value() > 30*time.Second {
		return fmt.Errorf("out_of_order_max must be between 0 and 30s")
	}
	if !validRate(c.DuplicateRate) || !validRate(c.SchemaErrorRate) {
		return fmt.Errorf("duplicate_rate and schema_error_rate must be in [0,1]")
	}
	if c.DeliveryBuckets < 0 || c.DeliveryBuckets > 1024 {
		return fmt.Errorf("delivery_key_buckets must be between 1 and 1024 when set")
	}
	if len(c.Locations) == 0 || len(c.Nodes) == 0 || c.Content.Count < 2 {
		return fmt.Errorf("at least one location/node and two content objects are required")
	}
	for _, location := range c.Locations {
		if location.Name == "" || len(location.Networks) == 0 {
			return fmt.Errorf("each location requires a name and at least one network")
		}
	}
	for _, node := range c.Nodes {
		if node.ID == "" || node.BaseTTFBMS < 0 || node.CapacityRPS <= 0 {
			return fmt.Errorf("each node requires an id, non-negative base latency, and positive capacity")
		}
	}
	allowed := map[string]bool{
		"node_latency_spike":   true,
		"node_5xx_spike":       true,
		"isp_node_degradation": true,
		"popularity_shift":     true,
		"capacity_pressure":    true,
	}
	for _, scenario := range c.Scenarios {
		if !allowed[scenario.Type] || scenario.Duration.Value() <= 0 || scenario.At.Value() < 0 {
			return fmt.Errorf("invalid scenario type/timing: %q", scenario.Type)
		}
		if scenario.At.Value()+scenario.Duration.Value() > c.Duration.Value() {
			return fmt.Errorf("scenario %q exceeds run duration", scenario.Type)
		}
		if !validRate(scenario.ErrorRate) || !validRate(scenario.MissRate) {
			return fmt.Errorf("scenario rates must be in [0,1]")
		}
	}
	return nil
}

func (c Config) DeliveryKeyBucketCount() int {
	if c.DeliveryBuckets == 0 {
		return 16
	}
	return c.DeliveryBuckets
}

func (c Config) DeliveryTopicName() string {
	if c.DeliveryTopic == "" {
		return "cdn.delivery.v1"
	}
	return c.DeliveryTopic
}

func validRate(v float64) bool { return v >= 0 && v <= 1 }
