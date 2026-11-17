// Package ratelimit paces deliveries to at most Rate messages per second per
// key (channel), across every worker instance.
//
// # Why slots instead of a token bucket
//
// A token bucket that starts full lets 100 messages out at once and then
// refills at 100/s, so the first second carries ~200: a burst a provider
// enforcing "max 100/s" would reject. Instead each message is given its own
// send slot on a shared timeline, one emission interval (10 ms at 100/s)
// apart. A worker never sends before its slot, so no one-second window can
// contain more than Rate sends, however many workers there are.
//
// This is GCRA (the Generic Cell Rate Algorithm) used for scheduling: the
// only state per key is TAT, the theoretical arrival time of the next free
// slot. Workers reserve several slots per call, at most Horizon ahead, so
// the limiter costs one round trip per batch, not one per message.
package ratelimit

import (
	"context"
	"time"
)

// Reservation is a run of evenly spaced send slots, relative to the moment
// Reserve returned.
type Reservation struct {
	// Count is the number of slots granted (0 if none fit in the horizon).
	Count int
	// First is the delay until the first slot. With Count 0 it is how long
	// to wait before asking again.
	First time.Duration
	// Interval separates consecutive slots.
	Interval time.Duration
}

// Delay returns the delay until slot i (0-based).
func (r Reservation) Delay(i int) time.Duration {
	return r.First + time.Duration(i)*r.Interval
}

// Limiter reserves send slots.
type Limiter interface {
	// Reserve grants up to n slots for key.
	Reserve(ctx context.Context, key string, n int) (Reservation, error)
}

// Config describes a rate.
type Config struct {
	// Rate is the maximum sends per second per key.
	Rate int
	// Horizon is how far ahead slots may be reserved. It bounds both how
	// long a reserved message waits and how many slots one call can take
	// (Rate × Horizon).
	Horizon time.Duration
}

func (c Config) interval() time.Duration { return time.Second / time.Duration(c.Rate) }

// gcra is the reservation arithmetic shared by both implementations (the
// Lua script mirrors it). All values are offsets from now; tat <= now means
// the timeline is idle.
//
//	slots that fit:  k = floor((horizon - (tat - now)) / interval), capped to n
//	first slot:      tat - now  (or 0 if idle)
//	if k == 0, wait: (tat - now) + interval - horizon
func gcra(tatOffset, interval, horizon time.Duration, n int) (granted int, first, wait time.Duration) {
	tatOffset = max(tatOffset, 0)
	k := int((horizon - tatOffset) / interval)
	k = min(max(k, 0), n)
	if k == 0 {
		return 0, 0, tatOffset + interval - horizon
	}
	return k, tatOffset, 0
}
