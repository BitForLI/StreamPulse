package scenario

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/BitForLI/StreamPulse/services/event-generator/internal/config"
	"github.com/BitForLI/StreamPulse/services/event-generator/internal/events"
)

type EmitFunc func(context.Context, events.Record) error

type LabelWindow struct {
	Type    string `json:"type"`
	Start   string `json:"start"`
	End     string `json:"end"`
	Target  string `json:"target,omitempty"`
	Network string `json:"network,omitempty"`
}

type Manifest struct {
	RunID                string            `json:"run_id"`
	Seed                 int64             `json:"seed"`
	GitCommit            string            `json:"git_commit"`
	SimulationStart      string            `json:"simulation_start_utc"`
	SimulationEnd        string            `json:"simulation_end_utc"`
	RatePerSecond        int               `json:"rate_per_second"`
	BaseRequests         int64             `json:"base_requests"`
	RecordsByTopic       map[string]int64  `json:"records_by_topic"`
	DeliveryRecordsByKey map[string]int64  `json:"delivery_records_by_key"`
	DuplicateRecords     int64             `json:"duplicate_records"`
	SchemaErrorRecords   int64             `json:"schema_error_records"`
	ExpectedLabels       []LabelWindow     `json:"expected_label_windows"`
	Versions             map[string]string `json:"versions"`
}

type Generator struct {
	cfg     config.Config
	rng     *rand.Rand
	start   time.Time
	zipf    *rand.Zipf
	nodeIDs []string
}

func New(cfg config.Config) (*Generator, error) {
	start, err := cfg.ParsedStartTime()
	if err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewSource(cfg.Seed))
	nodeIDs := make([]string, 0, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		nodeIDs = append(nodeIDs, node.ID)
	}
	return &Generator{
		cfg:     cfg,
		rng:     rng,
		start:   start,
		zipf:    rand.NewZipf(rng, 1.20, 1, uint64(cfg.Content.Count-1)),
		nodeIDs: nodeIDs,
	}, nil
}

func (g *Generator) Run(ctx context.Context, gitCommit string, versions map[string]string, emit EmitFunc) (Manifest, error) {
	total := int64(math.Round(g.cfg.Duration.Value().Seconds() * float64(g.cfg.RatePerSecond)))
	manifest := Manifest{
		RunID:                deterministicID(g.cfg.Seed, total, "run"),
		Seed:                 g.cfg.Seed,
		GitCommit:            gitCommit,
		SimulationStart:      events.Timestamp(g.start),
		SimulationEnd:        events.Timestamp(g.start.Add(g.cfg.Duration.Value())),
		RatePerSecond:        g.cfg.RatePerSecond,
		BaseRequests:         total,
		RecordsByTopic:       map[string]int64{},
		DeliveryRecordsByKey: map[string]int64{},
		ExpectedLabels:       g.labelWindows(),
		Versions:             versions,
	}

	for index := int64(0); index < total; index++ {
		if err := ctx.Err(); err != nil {
			return manifest, err
		}
		records, err := g.generateRequest(index)
		if err != nil {
			return manifest, err
		}
		for _, record := range records {
			if err := emit(ctx, record); err != nil {
				return manifest, err
			}
			manifest.RecordsByTopic[record.Topic]++
			if record.Topic == g.cfg.DeliveryTopicName() {
				manifest.DeliveryRecordsByKey[record.Key]++
			}
			if record.Duplicate {
				manifest.DuplicateRecords++
			}
			if record.SchemaInvalid {
				manifest.SchemaErrorRecords++
			}
		}
	}
	return manifest, nil
}

func (g *Generator) generateRequest(index int64) ([]events.Record, error) {
	step := time.Second / time.Duration(g.cfg.RatePerSecond)
	scheduled := g.start.Add(time.Duration(index) * step)
	jitter := time.Duration(0)
	if max := g.cfg.OutOfOrderMax.Value(); max > 0 {
		jitter = time.Duration(g.rng.Int63n(int64(max) + 1))
	}
	eventTime := scheduled.Add(-jitter)

	location := g.cfg.Locations[g.rng.Intn(len(g.cfg.Locations))]
	network := location.Networks[g.rng.Intn(len(location.Networks))]
	node := g.cfg.Nodes[index%int64(len(g.cfg.Nodes))]
	session := "sha256:" + hashText(fmt.Sprintf("session:%d:%d", g.cfg.Seed, index/8))
	requestID := deterministicID(g.cfg.Seed, index, "request")
	contentRank := int(g.zipf.Uint64())
	labels, latencyDelta, errorRate, missRate, popularityShift, pressureRPS := g.effects(eventTime.Sub(g.start), node.ID, network)
	if popularityShift {
		contentRank = g.cfg.Content.Count + g.rng.Intn(10)
	}
	contentID := fmt.Sprintf("content-%06d", contentRank)

	cacheStatus := "HIT"
	if g.rng.Float64() < maxFloat(0.08, missRate) {
		cacheStatus = "MISS"
	}
	httpStatus := 200
	if g.rng.Float64() < maxFloat(0.002, errorRate) {
		httpStatus = 503
	}
	ttfb := math.Max(0, node.BaseTTFBMS+g.rng.NormFloat64()*3+latencyDelta)
	origin := 0.0
	if cacheStatus != "HIT" {
		origin = math.Max(1, 35+g.rng.NormFloat64()*5+latencyDelta*0.4)
		ttfb += origin
	}

	routing := events.RoutingEvent{
		SchemaVersion:        1,
		EventID:              deterministicID(g.cfg.Seed, index, "routing"),
		EventTime:            events.Timestamp(eventTime),
		RequestID:            requestID,
		CandidateNodes:       append([]string(nil), g.nodeIDs...),
		SelectedNode:         node.ID,
		Policy:               "static-rendezvous",
		QualitySnapshotAgeMS: g.rng.Intn(1000),
		SelectedWeight:       1 / float64(len(g.cfg.Nodes)),
		FallbackLevel:        0,
		ReasonCodes:          []string{"SYNTHETIC_BASELINE"},
	}
	delivery := events.DeliveryEvent{
		SchemaVersion:        1,
		EventID:              deterministicID(g.cfg.Seed, index, "delivery"),
		EventTime:            events.Timestamp(eventTime),
		IngestTime:           events.Timestamp(scheduled),
		RequestID:            requestID,
		SessionIDHash:        session,
		ContentID:            contentID,
		ObjectType:           "segment",
		PathTemplate:         "/hls/{content_id}/{rendition}/{segment}.m4s",
		NodeID:               node.ID,
		Location:             location.Name,
		NetworkID:            network,
		CacheStatus:          cacheStatus,
		HTTPStatus:           httpStatus,
		BytesSent:            int64(180000 + g.rng.Intn(900000)),
		TTFBMS:               round3(ttfb),
		TransferMS:           round3(math.Max(1, 30+g.rng.NormFloat64()*5)),
		OriginMS:             round3(origin),
		SegmentDurationMS:    2000,
		BitrateBPS:           []int{800000, 1500000, 3000000}[g.rng.Intn(3)],
		SyntheticNodeRPS:     maxInt(g.cfg.RatePerSecond/len(g.cfg.Nodes), pressureRPS),
		SyntheticCapacityRPS: node.CapacityRPS,
		SyntheticLabels:      labels,
	}

	routingValue, err := events.Marshal(routing)
	if err != nil {
		return nil, err
	}
	deliveryValue, err := events.Marshal(delivery)
	if err != nil {
		return nil, err
	}
	schemaInvalid := g.rng.Float64() < g.cfg.SchemaErrorRate
	if schemaInvalid {
		var broken map[string]any
		if err := json.Unmarshal(deliveryValue, &broken); err != nil {
			return nil, err
		}
		broken["cache_status"] = "HOT"
		deliveryValue, err = json.Marshal(broken)
		if err != nil {
			return nil, err
		}
	}

	deliveryKey := location.Name + "|" + network + "|" + fmt.Sprintf("%02d", stableBucket(session, g.cfg.DeliveryKeyBucketCount()))
	records := []events.Record{
		{Topic: events.RoutingTopic, Key: requestID, Value: routingValue},
		{Topic: g.cfg.DeliveryTopicName(), Key: deliveryKey, Value: deliveryValue, SchemaInvalid: schemaInvalid},
	}
	if index%4 == 0 {
		rebuffer := (*float64)(nil)
		if ttfb > 120 {
			value := round3(ttfb - 120)
			rebuffer = &value
		}
		player := events.PlayerEvent{
			SchemaVersion:      1,
			EventID:            deterministicID(g.cfg.Seed, index, "player"),
			EventTime:          events.Timestamp(eventTime),
			SessionIDHash:      session,
			ContentID:          contentID,
			EventType:          "segment_complete",
			SegmentSequence:    int(index / 4),
			SelectedBitrateBPS: delivery.BitrateBPS,
			DownloadMS:         round3(ttfb + delivery.TransferMS),
			BufferMS:           round3(math.Max(0, 8000-ttfb)),
			RebufferMS:         rebuffer,
		}
		value, err := events.Marshal(player)
		if err != nil {
			return nil, err
		}
		records = append(records, events.Record{Topic: events.PlayerTopic, Key: session, Value: value})
	}
	if g.rng.Float64() < g.cfg.DuplicateRate {
		records = append(records, events.Record{
			Topic:         g.cfg.DeliveryTopicName(),
			Key:           deliveryKey,
			Value:         append([]byte(nil), deliveryValue...),
			Duplicate:     true,
			SchemaInvalid: schemaInvalid,
		})
	}
	return records, nil
}

func (g *Generator) effects(elapsed time.Duration, nodeID, network string) ([]string, float64, float64, float64, bool, int) {
	labels := make([]string, 0, 2)
	latencyDelta, errorRate, missRate := 0.0, 0.0, 0.0
	popularityShift := false
	pressureRPS := 0
	for _, item := range g.cfg.Scenarios {
		if elapsed < item.At.Value() || elapsed >= item.At.Value()+item.Duration.Value() {
			continue
		}
		applies := false
		switch item.Type {
		case "node_latency_spike":
			applies = item.Target == nodeID
			if applies {
				latencyDelta += item.AddedLatencyMS
			}
		case "node_5xx_spike":
			applies = item.Target == nodeID
			if applies {
				errorRate = maxFloat(errorRate, item.ErrorRate)
			}
		case "isp_node_degradation":
			applies = item.Target == nodeID && item.Network == network
			if applies {
				latencyDelta += item.AddedLatencyMS
				errorRate = maxFloat(errorRate, item.ErrorRate)
			}
		case "popularity_shift":
			applies = true
			popularityShift = true
			missRate = maxFloat(missRate, item.MissRate)
		case "capacity_pressure":
			applies = item.Target == nodeID
			if applies {
				latencyDelta += maxFloat(item.AddedLatencyMS, 60)
				errorRate = maxFloat(errorRate, item.ErrorRate)
				pressureRPS = item.TargetCapacityRPS
			}
		}
		if applies {
			labels = append(labels, item.Type)
		}
	}
	sort.Strings(labels)
	return labels, latencyDelta, errorRate, missRate, popularityShift, pressureRPS
}

func (g *Generator) labelWindows() []LabelWindow {
	result := make([]LabelWindow, 0, len(g.cfg.Scenarios))
	for _, item := range g.cfg.Scenarios {
		result = append(result, LabelWindow{
			Type:    item.Type,
			Start:   events.Timestamp(g.start.Add(item.At.Value())),
			End:     events.Timestamp(g.start.Add(item.At.Value() + item.Duration.Value())),
			Target:  item.Target,
			Network: item.Network,
		})
	}
	return result
}

func deterministicID(seed, index int64, kind string) string {
	return kind + "-" + hashText(fmt.Sprintf("%d:%d:%s", seed, index, kind))[:24]
}

func hashText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func stableBucket(value string, buckets int) int {
	sum := sha256.Sum256([]byte(value))
	return int(sum[0]) % buckets
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func round3(value float64) float64 { return math.Round(value*1000) / 1000 }
