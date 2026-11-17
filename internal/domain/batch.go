package domain

import (
	"time"

	"github.com/google/uuid"
)

// MaxBatchSize is the largest number of notifications one batch request may
// create.
const MaxBatchSize = 1000

// Batch groups notifications created by one request.
type Batch struct {
	ID            uuid.UUID
	Total         int
	Metadata      map[string]any
	CorrelationID string
	CompletedAt   *time.Time
	CreatedAt     time.Time
}

// BatchSummary is a batch with per-status counts computed from its
// notifications.
type BatchSummary struct {
	Batch
	Counts map[Status]int
}

// Done is the number of notifications that reached a final status.
func (s BatchSummary) Done() int {
	done := 0
	for st, c := range s.Counts {
		if st.IsFinal() {
			done += c
		}
	}
	return done
}

// Progress is the share of notifications in a final status, from 0 to 1.
func (s BatchSummary) Progress() float64 {
	if s.Total == 0 {
		return 0
	}
	return float64(s.Done()) / float64(s.Total)
}
