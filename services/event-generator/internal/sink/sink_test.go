package sink

import (
	"context"
	"testing"

	"github.com/BitForLI/StreamPulse/services/event-generator/internal/events"
	"github.com/segmentio/kafka-go"
)

type recordingWriter struct {
	batches [][]kafka.Message
	closed  bool
}

func (w *recordingWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	batch := append([]kafka.Message(nil), messages...)
	w.batches = append(w.batches, batch)
	return nil
}

func (w *recordingWriter) Close() error {
	w.closed = true
	return nil
}

func TestKafkaFlushesAtBoundedBatchSize(t *testing.T) {
	writer := &recordingWriter{}
	target := &Kafka{writer: writer}
	record := events.Record{Topic: events.DeliveryTopic, Key: "key", Value: []byte(`{"ok":true}`)}

	for i := 0; i < kafkaBatchSize-1; i++ {
		if err := target.Write(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	if len(writer.batches) != 0 {
		t.Fatalf("flushed early: got %d batches", len(writer.batches))
	}
	if err := target.Write(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(writer.batches) != 1 || len(writer.batches[0]) != kafkaBatchSize {
		t.Fatalf("expected one %d-message batch, got %#v", kafkaBatchSize, writer.batches)
	}
}

func TestKafkaCloseFlushesPartialBatch(t *testing.T) {
	writer := &recordingWriter{}
	target := &Kafka{writer: writer}
	record := events.Record{Topic: events.RoutingTopic, Key: "key", Value: []byte(`{"ok":true}`)}

	if err := target.Write(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if len(writer.batches) != 1 || len(writer.batches[0]) != 1 {
		t.Fatalf("expected one partial batch, got %#v", writer.batches)
	}
	if !writer.closed {
		t.Fatal("writer was not closed")
	}
}
