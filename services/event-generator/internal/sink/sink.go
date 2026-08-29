package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/BitForLI/StreamPulse/services/event-generator/internal/events"
	"github.com/segmentio/kafka-go"
)

type Sink interface {
	Write(context.Context, events.Record) error
	Close() error
}

type jsonEnvelope struct {
	Topic         string          `json:"topic"`
	Key           string          `json:"key"`
	Value         json.RawMessage `json:"value"`
	Duplicate     bool            `json:"duplicate,omitempty"`
	SchemaInvalid bool            `json:"schema_invalid,omitempty"`
}

type JSONLines struct {
	writer  io.Writer
	closer  io.Closer
	buffer  *bufio.Writer
	encoder *json.Encoder
}

func NewJSONLines(path string) (*JSONLines, error) {
	var writer io.Writer = os.Stdout
	var closer io.Closer
	if path != "" && path != "-" {
		file, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		writer, closer = file, file
	}
	buffer := bufio.NewWriter(writer)
	return &JSONLines{writer: writer, closer: closer, buffer: buffer, encoder: json.NewEncoder(buffer)}, nil
}

func (s *JSONLines) Write(_ context.Context, record events.Record) error {
	return s.encoder.Encode(jsonEnvelope{
		Topic:         record.Topic,
		Key:           record.Key,
		Value:         record.Value,
		Duplicate:     record.Duplicate,
		SchemaInvalid: record.SchemaInvalid,
	})
}

func (s *JSONLines) Close() error {
	flushErr := s.buffer.Flush()
	if s.closer != nil {
		if closeErr := s.closer.Close(); flushErr == nil {
			return closeErr
		}
	}
	return flushErr
}

const kafkaBatchSize = 500

type kafkaMessageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type Kafka struct {
	writer   kafkaMessageWriter
	messages []kafka.Message
}

func NewKafka(brokersCSV string) (*Kafka, error) {
	brokers := strings.Split(brokersCSV, ",")
	if len(brokers) == 0 || strings.TrimSpace(brokers[0]) == "" {
		return nil, fmt.Errorf("at least one Kafka broker is required")
	}
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	return &Kafka{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchSize:    kafkaBatchSize,
	}}, nil
}

func (s *Kafka) Write(ctx context.Context, record events.Record) error {
	s.messages = append(s.messages, kafka.Message{
		Topic: record.Topic,
		Key:   []byte(record.Key),
		Value: record.Value,
	})
	if len(s.messages) < kafkaBatchSize {
		return nil
	}
	return s.flush(ctx)
}

func (s *Kafka) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flushErr := s.flush(ctx)
	closeErr := s.writer.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

func (s *Kafka) flush(ctx context.Context) error {
	if len(s.messages) == 0 {
		return nil
	}
	if err := s.writer.WriteMessages(ctx, s.messages...); err != nil {
		return err
	}
	s.messages = s.messages[:0]
	return nil
}
