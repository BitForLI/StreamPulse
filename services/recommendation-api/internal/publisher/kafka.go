package publisher

import (
	"context"
	"encoding/json"
	"time"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
	"github.com/segmentio/kafka-go"
)

type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafka(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    100,
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}}
}

func (p *KafkaPublisher) Publish(ctx context.Context, recommendation domain.Recommendation) error {
	payload, err := json.Marshal(recommendation)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(recommendation.Scope.Key()),
		Value: payload,
		Time:  recommendation.CreatedAt,
	})
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
