// Package outbox moves domain events from the outbox_events table to the
// event stream.
//
// Every worker runs a Relay, but only one publishes at a time: each round
// takes a transaction-scoped advisory lock, so if the publishing worker
// dies, another one takes over on its next round. One publisher at a time
// keeps events in order. A round reads the oldest unpublished events,
// appends them to the stream and marks them published, all in one
// transaction.
//
// If Redis is down, rounds fail and events wait in the table; nothing is
// lost, and they are published in order once Redis is back. If the relay
// dies between publishing and committing, the next round publishes the
// same events again: consumers deduplicate by event id.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
)

// Publisher appends events to the stream, in order.
type Publisher interface {
	Publish(ctx context.Context, evs []events.Event) error
}

// Config tunes a Relay.
type Config struct {
	BatchSize int
	// PollInterval is the pause between rounds when there is no backlog,
	// and so the usual publishing latency.
	PollInterval time.Duration
	// RetryEvery is the pause after a failed round.
	RetryEvery time.Duration
	// Retention is how long published events are kept in the table.
	Retention     time.Duration
	PruneInterval time.Duration
}

// Relay publishes outbox events.
type Relay struct {
	store *postgres.Store
	pub   Publisher
	cfg   Config
	log   *slog.Logger
	now   func() time.Time
}

// New returns a Relay.
func New(store *postgres.Store, pub Publisher, cfg Config, log *slog.Logger) *Relay {
	return &Relay{store: store, pub: pub, cfg: cfg, log: log, now: time.Now}
}

// Run publishes until ctx is done.
func (r *Relay) Run(ctx context.Context) {
	nextPrune := r.now().Add(r.cfg.PruneInterval)
	for ctx.Err() == nil {
		n, err := r.RelayOnce(ctx)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				r.log.WarnContext(ctx, "outbox relay round failed; events stay queued", "error", err)
				sleep(ctx, r.cfg.RetryEvery)
			}
			continue
		case n == r.cfg.BatchSize:
			continue // more backlog: go again right away
		}
		if r.now().After(nextPrune) {
			r.prune(ctx)
			nextPrune = r.now().Add(r.cfg.PruneInterval)
		}
		sleep(ctx, r.cfg.PollInterval)
	}
}

// RelayOnce runs one round and returns how many events it published
// (0 also when another relay holds the lock).
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	published := 0
	err := r.store.InTx(ctx, func(repo *postgres.Repo) error {
		leader, err := repo.TryOutboxLock(ctx)
		if err != nil || !leader {
			return err
		}
		evs, err := repo.UnpublishedEvents(ctx, r.cfg.BatchSize)
		if err != nil || len(evs) == 0 {
			return err
		}
		if err := r.pub.Publish(ctx, evs); err != nil {
			return err
		}
		ids := make([]int64, len(evs))
		for i := range evs {
			ids[i] = evs[i].ID
		}
		if err := repo.MarkEventsPublished(ctx, ids, r.now()); err != nil {
			return err
		}
		published = len(evs)
		return nil
	})
	return published, err
}

// prune deletes published events past retention, in chunks.
func (r *Relay) prune(ctx context.Context) {
	const chunk = 10000
	before := r.now().Add(-r.cfg.Retention)
	var total int64
	for ctx.Err() == nil {
		n, err := r.store.PrunePublishedEvents(ctx, before, chunk)
		if err != nil {
			r.log.WarnContext(ctx, "prune published outbox events", "error", err)
			return
		}
		total += n
		if n < chunk {
			break
		}
	}
	if total > 0 {
		r.log.InfoContext(ctx, "pruned published outbox events", "deleted", total)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
