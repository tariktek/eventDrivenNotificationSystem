package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/idempotency"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// IdempotencyStore implements idempotency.Store. The primary key on
// idempotency_keys makes concurrent requests safe: exactly one INSERT wins.
// The lock_token makes takeovers safe: only the current owner can complete
// or release the key.
type IdempotencyStore struct {
	repo *Repo
	pool *pgxpool.Pool
}

var _ idempotency.Store = IdempotencyStore{}

// Idempotency returns the store's idempotency-key implementation.
func (s *Store) Idempotency() IdempotencyStore { return IdempotencyStore{repo: s.Repo, pool: s.pool} }

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

// Execute implements idempotency.Store. The writes fn makes through InTx
// join one transaction (a unit), which then stores the response under the
// lease's token: if another request took the key over meanwhile, the token
// no longer matches and everything rolls back.
//
// Storing the response takes the key's row lock, so a takeover that runs
// concurrently either happens first (this request then rolls back) or
// waits and finds the key completed (it then replays this response).
func (s IdempotencyStore) Execute(ctx context.Context, lease idempotency.Lease,
	fn func(ctx context.Context) (idempotency.Response, bool),
) (bool, error) {
	token, err := uuid.Parse(lease.Token)
	if err != nil {
		return false, fmt.Errorf("execute idempotent request: %w", err)
	}
	u := &unit{pool: s.pool}
	committed := false
	defer func() { // also runs when fn panics
		if u.tx != nil && !committed {
			rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), CleanupTimeout)
			defer cancel()
			_ = u.tx.Rollback(rbCtx)
		}
	}()

	resp, keep := fn(withUnit(ctx, u))
	if !keep {
		return false, nil
	}
	st, err := narrow[int32](resp.Status, "response_status")
	if err != nil {
		return false, err
	}
	body := resp.Body
	if body == nil {
		body = []byte{} // NULL means "in progress"; an empty body (204) is not
	}
	tx, err := u.begin(ctx) // fn may not have written anything (a 4xx)
	if err != nil {
		return false, err
	}
	n, err := sqlcgen.New(tx).CompleteIdempotencyKey(ctx, sqlcgen.CompleteIdempotencyKeyParams{
		Key: lease.Key, LockToken: token, ResponseStatus: &st, ResponseBody: body,
	})
	if err != nil {
		return false, fmt.Errorf("store idempotent response: %w", err)
	}
	if n == 0 {
		return false, idempotency.ErrLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit idempotent request: %w", err)
	}
	committed = true
	return true, nil
}

// CleanupTimeout bounds database work that must outlive a cancelled
// request (rolling back, releasing a key), so a stuck database cannot hold
// a request, or a shutdown, forever.
const CleanupTimeout = 5 * time.Second

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
