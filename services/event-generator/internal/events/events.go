package events

import (
	"encoding/json"
	"time"
)

const (
	DeliveryTopic = "cdn.delivery.v1"
	RoutingTopic  = "cdn.routing.v1"
	PlayerTopic   = "cdn.player.v1"
)

type Record struct {
	Topic         string
	Key           string
	Value         []byte
	Duplicate     bool
	SchemaInvalid bool
}

type DeliveryEvent struct {
	SchemaVersion        int      `json:"schema_version"`
	EventID              string   `json:"event_id"`
	EventTime            string   `json:"event_time"`
	IngestTime           string   `json:"ingest_time"`
	RequestID            string   `json:"request_id"`
	SessionIDHash        string   `json:"session_id_hash"`
	ContentID            string   `json:"content_id"`
	ObjectType           string   `json:"object_type"`
	PathTemplate         string   `json:"path_template"`
	NodeID               string   `json:"node_id"`
	Location             string   `json:"location"`
	NetworkID            string   `json:"network_id"`
	CacheStatus          string   `json:"cache_status"`
	HTTPStatus           int      `json:"http_status"`
	BytesSent            int64    `json:"bytes_sent"`
	TTFBMS               float64  `json:"ttfb_ms"`
	TransferMS           float64  `json:"transfer_ms"`
	OriginMS             float64  `json:"origin_ms,omitempty"`
	SegmentDurationMS    int      `json:"segment_duration_ms,omitempty"`
	BitrateBPS           int      `json:"bitrate_bps,omitempty"`
	SyntheticNodeRPS     int      `json:"synthetic_node_rps,omitempty"`
	SyntheticCapacityRPS int      `json:"synthetic_capacity_rps,omitempty"`
	SyntheticLabels      []string `json:"synthetic_labels,omitempty"`
}

type RoutingEvent struct {
	SchemaVersion        int      `json:"schema_version"`
	EventID              string   `json:"event_id"`
	EventTime            string   `json:"event_time"`
	RequestID            string   `json:"request_id"`
	CandidateNodes       []string `json:"candidate_nodes"`
	SelectedNode         string   `json:"selected_node"`
	Policy               string   `json:"policy"`
	QualitySnapshotAgeMS int      `json:"quality_snapshot_age_ms"`
	SelectedWeight       float64  `json:"selected_weight"`
	FallbackLevel        int      `json:"fallback_level"`
	ReasonCodes          []string `json:"reason_codes"`
}

type PlayerEvent struct {
	SchemaVersion      int      `json:"schema_version"`
	EventID            string   `json:"event_id"`
	EventTime          string   `json:"event_time"`
	SessionIDHash      string   `json:"session_id_hash"`
	ContentID          string   `json:"content_id"`
	EventType          string   `json:"event_type"`
	SegmentSequence    int      `json:"segment_sequence"`
	SelectedBitrateBPS int      `json:"selected_bitrate_bps"`
	DownloadMS         float64  `json:"download_ms"`
	BufferMS           float64  `json:"buffer_ms"`
	RebufferMS         *float64 `json:"rebuffer_ms,omitempty"`
}

func Marshal(value any) ([]byte, error) { return json.Marshal(value) }

func Timestamp(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
