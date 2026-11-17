// Package service implements the use cases behind the API: it validates
// input, renders templates, applies domain rules and persists the result.
// It knows nothing about HTTP; handlers translate to and from it.
package service

import (
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

// Service is safe for concurrent use.
type Service struct {
	store *postgres.Store
	now   func() time.Time
	opts  validation.Options
}

// New builds a Service. now is injected so tests control time.
func New(store *postgres.Store, now func() time.Time, opts validation.Options) *Service {
	return &Service{store: store, now: now, opts: opts}
}

// RequestMeta is request context stored with what the request creates, so
// the worker can continue the same log and trace.
type RequestMeta struct {
	CorrelationID string
	TraceParent   string
}

func (s *Service) clock() time.Time { return s.now().UTC() }
