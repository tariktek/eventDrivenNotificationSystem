// Package idempotency defines the contract between the HTTP idempotency
// middleware and its storage, so neither layer depends on the other.
package idempotency

import (
	"context"
	"errors"
	"time"
)

// Record is the stored state of a key that another request owns.
type Record struct {
	RequestHash string
	Status      int // 0 while the first request is still running
	Body        []byte
}

// Lease is one request's claim on a key. The token fences that request:
// once another request takes the key over, the old lease can no longer
// complete or release it.
type Lease struct {
	Key   string
	Token string
}

// ErrLeaseLost means the key was taken over by another request (because
// this one ran past the stale timeout) and the lease no longer owns it.
var ErrLeaseLost = errors.New("idempotency lease lost to another request")

// Store persists idempotency keys.
type Store interface {
	// Acquire claims key for a request with the given hash. It returns
	// acquired=true and a lease if the caller now owns the key: it was free,
	// expired, or held by a request with the same hash that has been running
	// for longer than staleAfter (its process presumably died). Otherwise it
	// returns the existing record.
	Acquire(ctx context.Context, key, hash string, now time.Time, ttl, staleAfter time.Duration) (lease Lease, rec Record, acquired bool, err error)
	// Complete stores the response for replay. It returns ErrLeaseLost if
	// the lease no longer owns the key.
	Complete(ctx context.Context, lease Lease, status int, body []byte) error
	// Release frees an unfinished key so the request can be retried. It is a
	// no-op if the lease no longer owns the key.
	Release(ctx context.Context, lease Lease) error
}
