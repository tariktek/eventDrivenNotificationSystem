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

// Response is what a request produced, as stored for replay.
type Response struct {
	Status int
	Body   []byte
}

// Store persists idempotency keys.
type Store interface {
	// Acquire claims key for a request with the given hash. It returns
	// acquired=true and a lease if the caller now owns the key: it was free,
	// expired, or held by a request with the same hash that has been running
	// for longer than staleAfter (its process presumably died). Otherwise it
	// returns the existing record.
	Acquire(ctx context.Context, key, hash string, now time.Time, ttl, staleAfter time.Duration) (lease Lease, rec Record, acquired bool, err error)
	// Execute runs the request (fn) and stores its response for replay as
	// one atomic unit: whatever fn writes to the database through the
	// context it is given commits in the same transaction as the stored
	// response, or not at all. So a key can never be left without its
	// response while what its request created exists (a crash or a failed
	// write in between would let a retry create it a second time), and a
	// request whose key was taken over cannot commit anything.
	//
	// fn reports whether its response should be kept; if not (a 5xx), its
	// writes are rolled back and the key stays unfinished, for Release.
	// Execute returns ErrLeaseLost, with fn's writes rolled back, if the
	// lease no longer owns the key.
	Execute(ctx context.Context, lease Lease, fn func(ctx context.Context) (resp Response, keep bool)) (kept bool, err error)
	// Release frees an unfinished key so the request can be retried. It is a
	// no-op if the lease no longer owns the key or its response was stored.
	Release(ctx context.Context, lease Lease) error
}
