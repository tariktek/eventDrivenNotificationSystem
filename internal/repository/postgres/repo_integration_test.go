//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/idempotency"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/testutil/pgtest"
)

var (
	pool  *pgxpool.Pool
	store *postgres.Store
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	pg, err := pgtest.Start(ctx)
	if err == nil {
		err = pgtest.MigrateUp(pg.DSN)
	}
	if err == nil {
		pool, err = pgxpool.New(ctx, pg.DSN)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	store = postgres.NewStore(pool)

	code := m.Run()

	pool.Close()
	_ = pg.Terminate()
	os.Exit(code)
}

// reset gives each test an empty database.
func reset(t *testing.T) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `TRUNCATE notifications, batches, templates, notification_attempts, idempotency_keys, outbox_events, callback_deliveries`)
	require.NoError(t, err)
}

var baseTime = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func newSMS(t *testing.T, mutate ...func(p *domain.NewNotificationParams)) *domain.Notification {
	t.Helper()
	p := domain.NewNotificationParams{
		Channel:   domain.ChannelSMS,
		Recipient: "+905551234567",
		Content:   "hello",
		Segments:  1,
	}
	for _, m := range mutate {
		m(&p)
	}
	n, err := domain.NewNotification(p, baseTime)
	require.NoError(t, err)
	return n
}

func insertOne(t *testing.T, n *domain.Notification) *domain.Notification {
	t.Helper()
	res, err := store.InsertNotifications(t.Context(), []*domain.Notification{n})
	require.NoError(t, err)
	require.Len(t, res, 1)
	return res[0].Notification
}

func TestInsertNotifications_RoundTrip(t *testing.T) {
	reset(t)
	at := baseTime.Add(time.Hour)
	tplID := uuid.New()
	_, err := pool.Exec(t.Context(), `INSERT INTO templates (id, name, channel, subject, body) VALUES ($1, 'welcome', 'email', 'Hi', 'Hello')`, tplID)
	require.NoError(t, err)

	in, err := domain.NewNotification(domain.NewNotificationParams{
		Channel:         domain.ChannelEmail,
		Priority:        domain.PriorityHigh,
		Recipient:       "user@example.com",
		Subject:         "Welcome",
		Content:         "Hello Ada",
		TemplateID:      &tplID,
		TemplateVersion: 1,
		TemplateVars:    map[string]any{"name": "Ada"},
		IdempotencyKey:  "signup-42",
		MaxAttempts:     3,
		ScheduledAt:     &at,
		CallbackURL:     "https://hooks.example.com/cb",
		Metadata:        map[string]any{"campaign": "onboarding"},
		CorrelationID:   "corr-1",
		TraceParent:     "00-abc-def-01",
	}, baseTime)
	require.NoError(t, err)

	got := insertOne(t, in)
	reloaded, err := store.GetNotification(t.Context(), in.ID)
	require.NoError(t, err)

	for _, n := range []*domain.Notification{got, reloaded} {
		assert.Equal(t, in.ID, n.ID)
		assert.Equal(t, domain.ChannelEmail, n.Channel)
		assert.Equal(t, domain.PriorityHigh, n.Priority)
		assert.Equal(t, "Welcome", n.Subject)
		assert.Equal(t, domain.StatusScheduled, n.Status)
		assert.Equal(t, &tplID, n.TemplateID)
		assert.Equal(t, 1, n.TemplateVersion)
		assert.Equal(t, "Ada", n.TemplateVars["name"])
		assert.Equal(t, "signup-42", n.IdempotencyKey)
		assert.Equal(t, 3, n.MaxAttempts)
		assert.True(t, at.Equal(n.NextAttemptAt))
		require.NotNil(t, n.ScheduledAt)
		assert.True(t, at.Equal(*n.ScheduledAt))
		assert.Equal(t, "onboarding", n.Metadata["campaign"])
		assert.Equal(t, "corr-1", n.CorrelationID)
		assert.Equal(t, time.UTC, n.CreatedAt.Location())
	}
}

func TestInsertNotifications_OptionalFieldsStayEmpty(t *testing.T) {
	reset(t)
	got := insertOne(t, newSMS(t))

	assert.Nil(t, got.BatchID)
	assert.Nil(t, got.TemplateID)
	assert.Nil(t, got.TemplateVars)
	assert.Empty(t, got.Subject)
	assert.Empty(t, got.IdempotencyKey)
	assert.Equal(t, map[string]any{}, got.Metadata)
	assert.Equal(t, 1, got.Segments)
}

func TestInsertNotifications_IdempotencyKeyReturnsExisting(t *testing.T) {
	reset(t)
	withKey := func(p *domain.NewNotificationParams) { p.IdempotencyKey = "order-1" }

	first := insertOne(t, newSMS(t, withKey))
	res, err := store.InsertNotifications(t.Context(), []*domain.Notification{newSMS(t, withKey)})
	require.NoError(t, err)

	assert.True(t, res[0].Duplicate)
	assert.Equal(t, first.ID, res[0].Notification.ID, "the original notification is returned")

	var count int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM notifications`).Scan(&count))
	assert.Equal(t, 1, count)
}

// TestInsertNotifications_ConcurrentSameKey is the race that ON CONFLICT
// exists for: many requests with one key at the same moment.
func TestInsertNotifications_ConcurrentSameKey(t *testing.T) {
	reset(t)
	const workers = 20

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = map[uuid.UUID]int{}
		created int
	)
	for range workers {
		wg.Go(func() {
			n := newSMS(t, func(p *domain.NewNotificationParams) { p.IdempotencyKey = "race" })
			res, err := store.InsertNotifications(context.Background(), []*domain.Notification{n})
			if !assert.NoError(t, err) {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			ids[res[0].Notification.ID]++
			if !res[0].Duplicate {
				created++
			}
		})
	}
	wg.Wait()

	assert.Equal(t, 1, created, "exactly one request creates the notification")
	assert.Len(t, ids, 1, "every request sees the same notification")
}

func TestBatch_InsertAtomicAndSummary(t *testing.T) {
	reset(t)
	batch := &domain.Batch{ID: uuid.New(), Total: domain.MaxBatchSize, Metadata: map[string]any{"campaign": "bf"}, CreatedAt: baseTime}
	ns := make([]*domain.Notification, domain.MaxBatchSize)
	for i := range ns {
		ns[i] = newSMS(t, func(p *domain.NewNotificationParams) { p.BatchID = &batch.ID })
	}

	start := time.Now()
	err := store.InTx(t.Context(), func(r *postgres.Repo) error {
		if err := r.InsertBatch(t.Context(), batch); err != nil {
			return err
		}
		res, err := r.InsertNotifications(t.Context(), ns)
		if err != nil {
			return err
		}
		require.Len(t, res, len(ns))
		for i, x := range res {
			require.Equal(t, ns[i].ID, x.Notification.ID, "results keep input order")
		}
		return nil
	})
	require.NoError(t, err)
	t.Logf("inserted %d notifications in %s", len(ns), time.Since(start))

	s, err := store.GetBatchSummary(t.Context(), batch.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.MaxBatchSize, s.Total)
	assert.Equal(t, domain.MaxBatchSize, s.Counts[domain.StatusPending])
	assert.Equal(t, 0, s.Counts[domain.StatusSent], "every status is present, zero if unused")
	assert.Equal(t, "bf", s.Metadata["campaign"])
}

func TestBatch_RollbackLeavesNothing(t *testing.T) {
	reset(t)
	batch := &domain.Batch{ID: uuid.New(), Total: 2, CreatedAt: baseTime}

	err := store.InTx(t.Context(), func(r *postgres.Repo) error {
		require.NoError(t, r.InsertBatch(t.Context(), batch))
		_, err := r.InsertNotifications(t.Context(), []*domain.Notification{
			newSMS(t, func(p *domain.NewNotificationParams) { p.BatchID = &batch.ID }),
		})
		require.NoError(t, err)
		return errors.New("simulated failure after inserts")
	})
	require.Error(t, err)

	_, err = store.GetBatchSummary(t.Context(), batch.ID)
	assert.ErrorIs(t, err, domain.ErrNotFound)
	var count int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM notifications`).Scan(&count))
	assert.Zero(t, count)
}

func TestBatch_DuplicatesFlaggedInOrder(t *testing.T) {
	reset(t)
	existing := insertOne(t, newSMS(t, func(p *domain.NewNotificationParams) { p.IdempotencyKey = "k2" }))

	ns := []*domain.Notification{
		newSMS(t, func(p *domain.NewNotificationParams) { p.IdempotencyKey = "k1" }),
		newSMS(t, func(p *domain.NewNotificationParams) { p.IdempotencyKey = "k2" }),
		newSMS(t),
	}
	res, err := store.InsertNotifications(t.Context(), ns)
	require.NoError(t, err)

	assert.False(t, res[0].Duplicate)
	assert.True(t, res[1].Duplicate)
	assert.Equal(t, existing.ID, res[1].Notification.ID)
	assert.False(t, res[2].Duplicate)
}

func TestCancelNotification(t *testing.T) {
	reset(t)
	now := baseTime.Add(time.Minute)

	t.Run("pending is cancelled", func(t *testing.T) {
		n := insertOne(t, newSMS(t))
		got, err := store.CancelNotification(t.Context(), n.ID, now)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusCancelled, got.Status)
		require.NotNil(t, got.CancelledAt)
		assert.True(t, now.Equal(*got.CancelledAt))

		_, err = store.CancelNotification(t.Context(), n.ID, now)
		var te *domain.TransitionError
		require.True(t, errors.As(err, &te), "second cancel must be a transition error, got %v", err)
		assert.Equal(t, domain.StatusCancelled, te.From)
	})

	t.Run("processing cannot be cancelled", func(t *testing.T) {
		n := insertOne(t, newSMS(t))
		_, err := pool.Exec(t.Context(),
			`UPDATE notifications SET status='processing', locked_by='w1', locked_until=now() + interval '30s' WHERE id=$1`, n.ID)
		require.NoError(t, err)

		_, err = store.CancelNotification(t.Context(), n.ID, now)
		var te *domain.TransitionError
		require.True(t, errors.As(err, &te))
		assert.Equal(t, domain.StatusProcessing, te.From)
		assert.ErrorIs(t, err, domain.ErrInvalidTransition)
	})

	t.Run("unknown id", func(t *testing.T) {
		_, err := store.CancelNotification(t.Context(), uuid.New(), now)
		assert.ErrorIs(t, err, domain.ErrNotFound)
	})
}

func TestRetryNotification(t *testing.T) {
	reset(t)
	now := baseTime.Add(time.Hour)
	n := insertOne(t, newSMS(t))

	_, err := store.RetryNotification(t.Context(), n.ID, now)
	assert.ErrorIs(t, err, domain.ErrInvalidTransition, "pending cannot be retried")

	_, err = pool.Exec(t.Context(),
		`UPDATE notifications SET status='failed', failed_at=now(), attempts=5, last_error='503' WHERE id=$1`, n.ID)
	require.NoError(t, err)

	got, err := store.RetryNotification(t.Context(), n.ID, now)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, got.Status)
	assert.Equal(t, 0, got.Attempts, "fresh attempt budget")
	assert.Nil(t, got.FailedAt)
	assert.True(t, now.Equal(got.NextAttemptAt), "due immediately")
	assert.Equal(t, "503", got.LastError, "previous error kept for context")
}

func TestCancelBatch(t *testing.T) {
	reset(t)
	batch := &domain.Batch{ID: uuid.New(), Total: 3, CreatedAt: baseTime}
	inBatch := func(p *domain.NewNotificationParams) { p.BatchID = &batch.ID }
	require.NoError(t, store.InsertBatch(t.Context(), batch))
	_, err := store.InsertNotifications(t.Context(), []*domain.Notification{newSMS(t, inBatch), newSMS(t, inBatch), newSMS(t, inBatch)})
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE notifications SET status='sent', sent_at=now() WHERE id = (SELECT id FROM notifications LIMIT 1)`)
	require.NoError(t, err)

	n, err := store.CancelBatch(t.Context(), batch.ID, baseTime)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n, "sent notifications are left alone")

	s, err := store.GetBatchSummary(t.Context(), batch.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, s.Counts[domain.StatusCancelled])
	assert.Equal(t, 1, s.Counts[domain.StatusSent])
	assert.InDelta(t, 1.0, s.Progress(), 1e-9)

	_, err = store.CancelBatch(t.Context(), uuid.New(), baseTime)
	assert.ErrorIs(t, err, domain.ErrNotFound)
}

func TestListNotifications_PaginatesWithoutGapsOrRepeats(t *testing.T) {
	reset(t)
	const total = 23
	want := map[uuid.UUID]bool{}
	for i := range total {
		n, err := domain.NewNotification(domain.NewNotificationParams{
			Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "x",
		}, baseTime.Add(time.Duration(i%5)*time.Second)) // shared timestamps exercise the id tie-break
		require.NoError(t, err)
		insertOne(t, n)
		want[n.ID] = true
	}

	seen := map[uuid.UUID]bool{}
	cursor := ""
	var prev *domain.Notification
	pages := 0
	for {
		page, err := store.ListNotifications(t.Context(), postgres.NotificationFilter{Limit: 5, Cursor: cursor})
		require.NoError(t, err)
		pages++
		for _, n := range page.Items {
			assert.False(t, seen[n.ID], "row repeated across pages")
			seen[n.ID] = true
			if prev != nil {
				ordered := n.CreatedAt.Before(prev.CreatedAt) ||
					(n.CreatedAt.Equal(prev.CreatedAt) && n.ID.String() < prev.ID.String())
				assert.True(t, ordered, "rows must be newest first")
			}
			prev = n
		}
		if !page.HasMore {
			assert.Empty(t, page.NextCursor)
			break
		}
		cursor = page.NextCursor
	}
	assert.Equal(t, want, seen, "every row appears exactly once")
	assert.Equal(t, 5, pages)
}

func TestListNotifications_Filters(t *testing.T) {
	reset(t)
	batchID := uuid.New()
	require.NoError(t, store.InsertBatch(t.Context(), &domain.Batch{ID: batchID, Total: 1, CreatedAt: baseTime}))

	mk := func(ch domain.Channel, prio domain.Priority, at time.Time, inBatch bool) *domain.Notification {
		p := domain.NewNotificationParams{Channel: ch, Priority: prio, Recipient: "r", Content: "c"}
		if ch == domain.ChannelEmail {
			p.Subject = "s"
		}
		if inBatch {
			p.BatchID = &batchID
		}
		n, err := domain.NewNotification(p, at)
		require.NoError(t, err)
		return insertOne(t, n)
	}
	sms := mk(domain.ChannelSMS, domain.PriorityHigh, baseTime, false)
	email := mk(domain.ChannelEmail, domain.PriorityLow, baseTime.Add(time.Hour), true)
	push := mk(domain.ChannelPush, domain.PriorityNormal, baseTime.Add(2*time.Hour), false)
	_, err := store.CancelNotification(t.Context(), push.ID, baseTime)
	require.NoError(t, err)

	ids := func(f postgres.NotificationFilter) []uuid.UUID {
		page, err := store.ListNotifications(t.Context(), f)
		require.NoError(t, err)
		out := []uuid.UUID{}
		for _, n := range page.Items {
			out = append(out, n.ID)
		}
		return out
	}
	ptr := func(v time.Time) *time.Time { return &v }

	assert.Equal(t, []uuid.UUID{push.ID, email.ID, sms.ID}, ids(postgres.NotificationFilter{}))
	assert.Equal(t, []uuid.UUID{email.ID}, ids(postgres.NotificationFilter{Channel: new(domain.ChannelEmail)}))
	assert.Equal(t, []uuid.UUID{sms.ID}, ids(postgres.NotificationFilter{Priority: new(domain.PriorityHigh)}))
	assert.Equal(t, []uuid.UUID{push.ID}, ids(postgres.NotificationFilter{Status: new(domain.StatusCancelled)}))
	assert.Equal(t, []uuid.UUID{email.ID}, ids(postgres.NotificationFilter{BatchID: &batchID}))
	assert.Equal(t, []uuid.UUID{email.ID}, ids(postgres.NotificationFilter{
		CreatedFrom: ptr(baseTime.Add(time.Hour)), CreatedTo: ptr(baseTime.Add(2 * time.Hour)),
	}), "from is inclusive, to is exclusive")
	assert.Empty(t, ids(postgres.NotificationFilter{Channel: new(domain.ChannelSMS), Status: new(domain.StatusCancelled)}))
}

func TestListNotifications_InvalidCursor(t *testing.T) {
	_, err := store.ListNotifications(t.Context(), postgres.NotificationFilter{Cursor: "garbage!"})
	assert.ErrorIs(t, err, domain.ErrInvalidCursor)
}

func TestTemplates(t *testing.T) {
	reset(t)
	mk := func(name string, at time.Time) *domain.Template {
		tpl, err := store.InsertTemplate(t.Context(), &domain.Template{
			ID: uuid.New(), Name: name, Channel: domain.ChannelEmail, Subject: "Hi {{.name}}",
			Body: "Hello {{.name}}", Variables: []string{"name"}, CreatedAt: at,
		})
		require.NoError(t, err)
		return tpl
	}

	welcome := mk("welcome", baseTime)
	assert.Equal(t, 1, welcome.Version)
	assert.Equal(t, []string{"name"}, welcome.Variables)

	t.Run("duplicate name", func(t *testing.T) {
		_, err := store.InsertTemplate(t.Context(), &domain.Template{
			ID: uuid.New(), Name: "welcome", Channel: domain.ChannelSMS, Body: "x", CreatedAt: baseTime,
		})
		assert.ErrorIs(t, err, domain.ErrAlreadyExists)
	})

	t.Run("update bumps version", func(t *testing.T) {
		welcome.Body = "Welcome aboard, {{.name}}"
		got, err := store.UpdateTemplate(t.Context(), welcome)
		require.NoError(t, err)
		assert.Equal(t, 2, got.Version)
		assert.Equal(t, "Welcome aboard, {{.name}}", got.Body)
	})

	t.Run("list paginates newest first", func(t *testing.T) {
		mk("second", baseTime.Add(time.Minute))
		mk("third", baseTime.Add(2*time.Minute))

		page, err := store.ListTemplates(t.Context(), nil, 2, "")
		require.NoError(t, err)
		require.Len(t, page.Items, 2)
		assert.Equal(t, "third", page.Items[0].Name)
		assert.True(t, page.HasMore)

		page, err = store.ListTemplates(t.Context(), nil, 2, page.NextCursor)
		require.NoError(t, err)
		require.Len(t, page.Items, 1)
		assert.Equal(t, "welcome", page.Items[0].Name)
		assert.False(t, page.HasMore)

		page, err = store.ListTemplates(t.Context(), new(domain.ChannelSMS), 10, "")
		require.NoError(t, err)
		assert.Empty(t, page.Items)
	})

	t.Run("delete is soft and frees the name", func(t *testing.T) {
		require.NoError(t, store.DeleteTemplate(t.Context(), welcome.ID, baseTime))

		_, err := store.GetTemplate(t.Context(), welcome.ID)
		assert.ErrorIs(t, err, domain.ErrNotFound)
		assert.ErrorIs(t, store.DeleteTemplate(t.Context(), welcome.ID, baseTime), domain.ErrNotFound)
		_, err = store.UpdateTemplate(t.Context(), welcome)
		assert.ErrorIs(t, err, domain.ErrNotFound)

		mk("welcome", baseTime.Add(time.Hour)) // name is reusable
	})
}

func TestIdempotencyStore_LeaseFencing(t *testing.T) {
	reset(t)
	idem := store.Idempotency()
	now := baseTime

	a, _, ok, err := idem.Acquire(t.Context(), "c:k", "hash", now, time.Hour, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)

	_, rec, ok, err := idem.Acquire(t.Context(), "c:k", "hash", now.Add(30*time.Second), time.Hour, time.Minute)
	require.NoError(t, err)
	assert.False(t, ok, "in progress and not yet stale")
	assert.Zero(t, rec.Status)

	b, _, ok, err := idem.Acquire(t.Context(), "c:k", "hash", now.Add(2*time.Minute), time.Hour, time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "stale lock taken over")
	assert.NotEqual(t, a.Token, b.Token)

	// The old owner finishes late: it can neither release nor complete.
	require.NoError(t, idem.Release(t.Context(), a))
	assert.ErrorIs(t, complete(t, a, 201, []byte("stale")), idempotency.ErrLeaseLost)

	body := []byte("{\"z\":1,\"a\":2}\n")
	require.NoError(t, complete(t, b, 201, body))
	_, rec, ok, err = idem.Acquire(t.Context(), "c:k", "hash", now.Add(3*time.Minute), time.Hour, time.Minute)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, 201, rec.Status)
	assert.Equal(t, body, rec.Body, "replayed byte for byte")

	_, rec, _, err = idem.Acquire(t.Context(), "c:k", "other-hash", now.Add(3*time.Minute), time.Hour, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "hash", rec.RequestHash, "a different request sees the original hash")
}

func TestIdempotencyStore_ExpiredKeyIsReusable(t *testing.T) {
	reset(t)
	idem := store.Idempotency()
	l, _, _, err := idem.Acquire(t.Context(), "c:e", "h1", baseTime, time.Hour, time.Minute)
	require.NoError(t, err)
	require.NoError(t, complete(t, l, 201, []byte("{}")))

	_, _, ok, err := idem.Acquire(t.Context(), "c:e", "h2", baseTime.Add(2*time.Hour), time.Hour, time.Minute)
	require.NoError(t, err)
	assert.True(t, ok, "after expiry the key starts over, even for a different request")
}
