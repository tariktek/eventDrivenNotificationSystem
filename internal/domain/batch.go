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
//
// Total is the number of items in the request. Items whose idempotency_key
// already existed were not created again and belong to their original
// batch, so the counts can sum to less than Total.
type BatchSummary struct {
	Batch
	Counts map[Status]int
}

// Created is the number of notifications this batch actually created.
func (s BatchSummary) Created() int {
	n := 0
	for _, c := range s.Counts {
		n += c
	}
	return n
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

// Progress is the share of the batch's notifications in a final status,
// from 0 to 1. A batch that created nothing (every item was a duplicate)
// has nothing left to do, so it is complete.
func (s BatchSummary) Progress() float64 {
	created := s.Created()
	if created == 0 {
		return 1
	}
	return float64(s.Done()) / float64(created)
}
