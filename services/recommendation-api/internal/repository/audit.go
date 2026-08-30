package repository

import (
	"context"
	"sync"

	"github.com/BitForLI/StreamPulse/services/recommendation-api/internal/domain"
)

type MemoryAuditStore struct {
	mu       sync.RWMutex
	acks     map[string][]domain.Acknowledgement
	outcomes map[string][]domain.Outcome
}

func NewMemoryAuditStore() *MemoryAuditStore {
	return &MemoryAuditStore{
		acks:     make(map[string][]domain.Acknowledgement),
		outcomes: make(map[string][]domain.Outcome),
	}
}

func (s *MemoryAuditStore) Acknowledge(_ context.Context, ack domain.Acknowledgement) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acks[ack.RecommendationID] = append(s.acks[ack.RecommendationID], ack)
	return nil
}

func (s *MemoryAuditStore) Outcome(_ context.Context, outcome domain.Outcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outcomes[outcome.RecommendationID] = append(s.outcomes[outcome.RecommendationID], outcome)
	return nil
}
