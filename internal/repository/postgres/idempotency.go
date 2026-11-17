package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/idempotency"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// IdempotencyStore implements idempotency.Store. The primary key on
// idempotency_keys makes concurrent requests safe: exactly one INSERT wins.
// The lock_token makes takeovers safe: only the current owner can complete
// or release the key.
type IdempotencyStore struct {
	repo *Repo
}

var _ idempotency.Store = IdempotencyStore{}

// Idempotency returns the store's idempotency-key implementation.
func (s *Store) Idempotency() IdempotencyStore { return IdempotencyStore{repo: s.Repo} }

// Acquire implements idempotency.Store.
func (s IdempotencyStore) Acquire(ctx context.Context, key, hash string, now time.Time, ttl, staleAfter time.Duration,
) (idempotency.Lease, idempotency.Record, bool, error) {
	q := s.repo.q
	expires := now.Add(ttl)
	token, err := uuid.NewRandom()
	if err != nil {
		return idempotency.Lease{}, idempotency.Record{}, false, fmt.Errorf("generate lock token: %w", err)
	}
	lease := idempotency.Lease{Key: key, Token: token.String()}

	// Two attempts: if the row disappears between our INSERT and SELECT
	// (released by a failing owner), the second INSERT can win.
	for range 2 {
		_, err := q.InsertIdempotencyKey(ctx, sqlcgen.InsertIdempotencyKeyParams{
			Key: key, RequestHash: hash, LockToken: token, Now: now, ExpiresAt: expires,
		})
		if err == nil {
			return lease, idempotency.Record{}, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return idempotency.Lease{}, idempotency.Record{}, false, fmt.Errorf("insert idempotency key: %w", err)
		}

		_, err = q.TakeOverIdempotencyKey(ctx, sqlcgen.TakeOverIdempotencyKeyParams{
			Key: key, RequestHash: hash, LockToken: token, Now: now, ExpiresAt: expires, StaleBefore: now.Add(-staleAfter),
		})
		if err == nil {
			return lease, idempotency.Record{}, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return idempotency.Lease{}, idempotency.Record{}, false, fmt.Errorf("take over idempotency key: %w", err)
		}

		row, err := q.GetIdempotencyKey(ctx, key)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return idempotency.Lease{}, idempotency.Record{}, false, fmt.Errorf("get idempotency key: %w", err)
		}
		return idempotency.Lease{}, idempotency.Record{
			RequestHash: row.RequestHash,
			Status:      int(deref(row.ResponseStatus)),
			Body:        row.ResponseBody,
		}, false, nil
	}
	return idempotency.Lease{}, idempotency.Record{}, false, errors.New("idempotency key changed hands repeatedly")
}

// Complete implements idempotency.Store.
func (s IdempotencyStore) Complete(ctx context.Context, lease idempotency.Lease, status int, body []byte) error {
	token, err := uuid.Parse(lease.Token)
	if err != nil {
		return fmt.Errorf("complete idempotency key: %w", err)
	}
	st, err := narrow[int32](status, "response_status")
	if err != nil {
		return err
	}
	if body == nil {
		body = []byte{} // NULL means "in progress"; an empty body (204) is not
	}
	n, err := s.repo.q.CompleteIdempotencyKey(ctx, sqlcgen.CompleteIdempotencyKeyParams{
		Key: lease.Key, LockToken: token, ResponseStatus: &st, ResponseBody: body,
	})
	if err != nil {
		return fmt.Errorf("complete idempotency key: %w", err)
	}
	if n == 0 {
		return idempotency.ErrLeaseLost
	}
	return nil
}

// Release implements idempotency.Store.
func (s IdempotencyStore) Release(ctx context.Context, lease idempotency.Lease) error {
	token, err := uuid.Parse(lease.Token)
	if err != nil {
		return fmt.Errorf("release idempotency key: %w", err)
	}
	if err := s.repo.q.ReleaseIdempotencyKey(ctx, sqlcgen.ReleaseIdempotencyKeyParams{Key: lease.Key, LockToken: token}); err != nil {
		return fmt.Errorf("release idempotency key: %w", err)
	}
	return nil
}
