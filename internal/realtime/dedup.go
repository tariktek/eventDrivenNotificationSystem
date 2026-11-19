package realtime

import (
	"sync"

	"github.com/google/uuid"
)

// dedup drops events a client has already been sent, or that are older
// than one it has been sent.
//
// Events reach the stream at least once: if the outbox relay publishes and
// then fails to commit (Redis accepted the events, PostgreSQL did not mark
// them published), it publishes them again. Without this a client would
// see a notification go to "sent" and then back to "pending".
//
// Event ids are global, but they only increase per aggregate (a
// notification's changes commit one after another under its row lock;
// across notifications, commit order and id order can differ). So the hub
// remembers the highest id forwarded per aggregate, for the most recently
// seen maxAggregates aggregates: a repeat arrives within seconds of the
// original, long before its aggregate is evicted.
type dedup struct {
	mu   sync.Mutex
	max  int
	last map[uuid.UUID]int64
	ring []uuid.UUID // insertion order, for eviction
	next int
}

func newDedup(maxAggregates int) *dedup {
	return &dedup{max: max(maxAggregates, 1), last: map[uuid.UUID]int64{}}
}

// fresh reports whether the event should be forwarded, and remembers it.
func (d *dedup) fresh(aggregate uuid.UUID, eventID int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if last, ok := d.last[aggregate]; ok {
		if eventID <= last {
			return false
		}
		d.last[aggregate] = eventID
		return true
	}
	if len(d.ring) < d.max {
		d.ring = append(d.ring, aggregate)
	} else {
		delete(d.last, d.ring[d.next])
		d.ring[d.next] = aggregate
		d.next = (d.next + 1) % d.max
	}
	d.last[aggregate] = eventID
	return true
}
