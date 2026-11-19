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
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"

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
	// OnPublished, if set, sees every batch after it is committed as
	// published (e.g. to count status transitions).
	OnPublished func([]events.Event)
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
//
// Pruning runs on its own clock, beside publishing. It used to run only
// between rounds that found no backlog, so under sustained load (a
// campaign keeping every round full) it never ran and the table grew
// without bound, until one huge delete at the end.
func (r *Relay) Run(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.pruneLoop(ctx)
	}()
	defer func() { <-done }()

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
		sleep(ctx, r.cfg.PollInterval)
	}
}

// pruneLoop prunes every PruneInterval until ctx is done.
func (r *Relay) pruneLoop(ctx context.Context) {
	if r.cfg.PruneInterval <= 0 {
		return // pruning disabled
	}
	t := time.NewTicker(r.cfg.PruneInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.prune(ctx)
		}
	}
}

// RelayOnce runs one round and returns how many events it published
// (0 also when another relay holds the lock).
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	var published []events.Event
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
		// Wake the workers for channels that just got due work. Done here,
		// once per round, rather than in a trigger on every insert: NOTIFY
		// serializes the commits of all transactions that use it.
		if chans := dueChannels(evs, r.now()); len(chans) > 0 {
			if err := repo.NotifyDue(ctx, chans); err != nil {
				return err
			}
		}
		// Batch progress, in this transaction: a change is counted exactly
		// when its event is marked published, even if the relay crashes and
		// publishes the event again.
		if err := r.trackBatches(ctx, repo, evs); err != nil {
			return err
		}
		published = evs
		return nil
	})
	if err == nil && len(published) > 0 && r.cfg.OnPublished != nil {
		r.cfg.OnPublished(published)
	}
	return len(published), err
}

// finalStatuses are the statuses that count a batch item as finished.
var finalStatuses = map[string]bool{"sent": true, "failed": true, "cancelled": true}

// trackBatches updates the finished counts of the batches these events
// touch and emits batch.completed for those that just finished. The relay
// is the only writer of the counts and updates each batch once per round,
// so deliveries never wait for one another on a batch row.
//
// Events of one notification arrive in order, and a batch's created events
// (one insert, one transaction) all precede its items' later changes, so a
// batch cannot look finished before all of its items are. A batch is
// announced when its completed_at goes from empty to set, so it is
// announced once per completion (again only after a redrive reopened it).
func (r *Relay) trackBatches(ctx context.Context, repo *postgres.Repo, evs []events.Event) error {
	deltas := map[uuid.UUID]int32{}
	var order []uuid.UUID
	for i := range evs {
		if evs[i].Type != events.TypeNotificationStatusChanged {
			continue
		}
		var p events.NotificationPayload
		if json.Unmarshal(evs[i].Payload, &p) != nil || p.BatchID == nil || p.PreviousStatus == nil {
			continue
		}
		d := b2i(finalStatuses[p.Status]) - b2i(finalStatuses[*p.PreviousStatus])
		if d == 0 {
			continue
		}
		if _, ok := deltas[*p.BatchID]; !ok {
			order = append(order, *p.BatchID)
		}
		deltas[*p.BatchID] += d
	}
	if len(order) == 0 {
		return nil
	}
	ds := make([]int32, len(order))
	for i, id := range order {
		ds[i] = deltas[id]
	}
	now := r.now()
	rows, err := repo.AddBatchFinished(ctx, order, ds, now)
	if err != nil {
		return err
	}
	for _, b := range rows {
		if !b.WasOpen || b.CompletedAt == nil { // not just completed
			continue
		}
		counts, err := repo.BatchStatusCounts(ctx, b.ID)
		if err != nil {
			return err
		}
		payload, err := json.Marshal(map[string]any{
			"batch_id": b.ID, "total": b.Total, "created": b.CreatedCount,
			"counts": json.RawMessage(counts), "completed_at": b.CompletedAt, "occurred_at": now,
		})
		if err != nil {
			return err
		}
		// Published by the next round.
		if err := repo.InsertOutboxEvent(ctx, "batch", b.ID, events.TypeBatchCompleted, payload); err != nil {
			return err
		}
	}
	return nil
}

func b2i(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

// dueChannels returns the channels of notifications that these events made
// pending and due now: new ones, and ones handed back by a worker. Retries
// scheduled for later and scheduled sends are left to the workers' poll.
func dueChannels(evs []events.Event, now time.Time) []string {
	seen := map[string]bool{}
	var out []string
	for i := range evs {
		if evs[i].AggregateType != "notification" {
			continue
		}
		var p struct {
			Status        string     `json:"status"`
			Channel       string     `json:"channel"`
			NextAttemptAt *time.Time `json:"next_attempt_at"`
		}
		if json.Unmarshal(evs[i].Payload, &p) != nil || p.Status != "pending" || seen[p.Channel] {
			continue
		}
		if p.NextAttemptAt == nil || !p.NextAttemptAt.After(now) {
			seen[p.Channel] = true
			out = append(out, p.Channel)
		}
	}
	return out
}

// prune deletes published events past retention, in chunks, each its own
// short statement.
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
