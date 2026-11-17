// Package queue defines the work queue contract the worker consumes. The
// PostgreSQL implementation lives in the repository package; keeping the
// contract here means the worker does not depend on how the queue is stored
// (a Kafka- or RabbitMQ-backed queue could implement the same interface).
package queue

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

// ClaimRequest asks for up to Limit due notifications of one channel.
type ClaimRequest struct {
	Channel  domain.Channel
	WorkerID string
	Limit    int
	Now      time.Time
	// Lease is how long the claim is valid without a heartbeat. A worker
	// that dies stops extending it, and the reaper returns the work.
	Lease time.Duration
	// Aging promotes a notification one priority level for every Aging it
	// has been due, so a steady stream of high-priority work cannot starve
	// low-priority work forever.
	Aging time.Duration
}

// ReapResult reports what the reaper did with expired leases.
type ReapResult struct {
	// Requeued notifications go back to pending for another worker.
	Requeued int64
	// Failed notifications had no attempts left: a message whose processing
	// keeps killing workers (a "poison pill") must not loop forever.
	Failed int64
}

// Queue is the worker's view of pending work.
type Queue interface {
	// Claim atomically moves due notifications to processing under a
	// lease owned by WorkerID, counting an attempt for each. Concurrent
	// callers never receive the same notification.
	Claim(ctx context.Context, req ClaimRequest) ([]*domain.Notification, error)
	// ExtendLeases is the heartbeat for long-running deliveries. It returns
	// how many leases were still held by workerID.
	ExtendLeases(ctx context.Context, workerID string, ids []uuid.UUID, until time.Time) (int64, error)
	// ReapExpiredLeases recovers work from workers that stopped
	// heartbeating (crashed, killed, network-partitioned).
	ReapExpiredLeases(ctx context.Context, now time.Time) (ReapResult, error)
}
