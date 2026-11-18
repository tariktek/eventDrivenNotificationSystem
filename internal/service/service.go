// Package service implements the use cases behind the API: it validates
// input, renders templates, applies domain rules and persists the result.
// It knows nothing about HTTP; handlers translate to and from it.
package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/stats"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

// Service is safe for concurrent use.
type Service struct {
	store *postgres.Store
	now   func() time.Time
	opts  validation.Options
	stats *stats.Sampler
}

// New builds a Service. now is injected so tests control time.
func New(store *postgres.Store, now func() time.Time, opts validation.Options) *Service {
	return &Service{store: store, now: now, opts: opts, stats: stats.New(store, nil, 5*time.Second, slog.Default())}
}

// WithStats makes Metrics read from a sampler that runs in the
// background (and updates Prometheus gauges), instead of sampling on
// demand.
func (s *Service) WithStats(sampler *stats.Sampler) *Service {
	s.stats = sampler
	return s
}

// Metrics returns the latest delivery statistics.
func (s *Service) Metrics(ctx context.Context) (*stats.Snapshot, error) {
	return s.stats.Snapshot(ctx)
}

// RequestMeta is request context stored with what the request creates, so
// the worker can continue the same log and trace.
type RequestMeta struct {
	CorrelationID string
	TraceParent   string
}

func (s *Service) clock() time.Time { return s.now().UTC() }
