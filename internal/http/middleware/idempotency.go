package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/api"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/apierror"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/idempotency"
)

// Idempotency headers.
const (
	HeaderIdempotencyKey     = "Idempotency-Key"
	HeaderIdempotentReplayed = "Idempotent-Replayed"
	idempotencyKeyMaxLen     = 255
)

// IdempotencyConfig configures the Idempotency middleware.
type IdempotencyConfig struct {
	Store idempotency.Store
	TTL   time.Duration
	// StaleAfter is how long a key may stay in progress before a retry
	// can take it over (default DefaultIdempotencyStaleAfter).
	StaleAfter time.Duration
	Now        func() time.Time
	Log        *slog.Logger
	// Applies selects the requests the middleware handles; others pass through.
	Applies func(r *http.Request) bool
}

// Idempotency makes requests carrying an Idempotency-Key header safe to
// retry: the first request runs and its response is stored; a retry with the
// same key and body gets that response back instead of running again.
//
//   - same key, different request  → 422 idempotency_key_reused
//   - same key, first still running → 409 request_in_progress
//   - first request failed with 5xx or panicked → the key is released, so the
//     retry runs for real
//
// The request's database writes and its stored response commit in one
// transaction (idempotency.Store.Execute), and the client gets the response
// only after that commit. Whatever happens in between (a crash, a failed
// write, a dropped connection), either both exist or neither does, so a
// retry replays exactly what was created or creates it exactly once.
//
// It needs the buffered body (BufferBody) and the client identity
// (APIKeyAuth): keys are scoped per client, so two clients cannot collide.
func Idempotency(cfg IdempotencyConfig, writeErr ErrorWriter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get(HeaderIdempotencyKey)
			if key == "" || !cfg.Applies(r) {
				next.ServeHTTP(w, r)
				return
			}
			if len(key) > idempotencyKeyMaxLen || !printableASCII(key) {
				writeErr(w, r, apierror.BadRequest("Idempotency-Key must be 1-%d printable ASCII characters", idempotencyKeyMaxLen))
				return
			}

			ctx := r.Context()
			scoped := ClientID(ctx) + ":" + key
			body, _ := Body(ctx)
			hash := requestHash(r, body)

			lease, rec, acquired, err := cfg.Store.Acquire(ctx, scoped, hash, cfg.Now(), cfg.TTL, cfg.staleAfter())
			if err != nil {
				writeErr(w, r, fmt.Errorf("acquire idempotency key: %w", err))
				return
			}
			if !acquired {
				replayOrReject(w, r, rec, hash, writeErr)
				return
			}

			// We own the key. Release it unless its response was stored, so
			// a panic or 5xx never blocks the client's retry. (Release is a
			// no-op once the response is stored.) Cleanup must outlive a
			// cancelled request, but not wait forever on the database.
			kept := false
			defer func() {
				if kept {
					return
				}
				cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
				defer cancel()
				if err := cfg.Store.Release(cleanupCtx, lease); err != nil {
					cfg.Log.ErrorContext(cleanupCtx, "release idempotency key", "error", err)
				}
			}()

			// The response is buffered and reaches the client only after
			// it was stored, in the same transaction as whatever the
			// request created: the client never holds a response the key
			// does not, and a dropped connection cannot truncate the
			// stored copy.
			buf := newResponseBuffer()
			kept, err = cfg.Store.Execute(ctx, lease, func(txCtx context.Context) (idempotency.Response, bool) {
				next.ServeHTTP(buf, r.WithContext(txCtx))
				return idempotency.Response{Status: buf.Status(), Body: buf.body.Bytes()}, buf.Status() < http.StatusInternalServerError
			})
			switch {
			case errors.Is(err, idempotency.ErrLeaseLost):
				// A retry with the same key took over while this request ran
				// past the stale timeout; this one's work was rolled back,
				// so exactly one of them creates anything.
				cfg.Log.WarnContext(ctx, "idempotency key was taken over while the request ran; rolled back", "key", lease.Key)
				writeErr(w, r, apierror.New(http.StatusConflict, api.ErrorCodeRequestInProgress,
					"A retry with this Idempotency-Key took over this request; use the retry's response."))
			case err != nil:
				writeErr(w, r, fmt.Errorf("idempotent request: %w", err))
			default:
				buf.writeTo(w)
			}
		})
	}
}

// DefaultIdempotencyStaleAfter is how long a key may stay "in progress"
// before a retry with the same request may take it over (its first owner
// presumably died). It must exceed the longest a request can take: a
// takeover is safe even then (the slow request rolls back), but the slow
// request's client gets a 409 for nothing.
const DefaultIdempotencyStaleAfter = time.Minute

const cleanupTimeout = 5 * time.Second

func (cfg IdempotencyConfig) staleAfter() time.Duration {
	if cfg.StaleAfter > 0 {
		return cfg.StaleAfter
	}
	return DefaultIdempotencyStaleAfter
}

func replayOrReject(w http.ResponseWriter, r *http.Request, rec idempotency.Record, hash string, writeErr ErrorWriter) {
	switch {
	case rec.RequestHash != hash:
		writeErr(w, r, apierror.New(http.StatusUnprocessableEntity, api.ErrorCodeIdempotencyKeyReused,
			"This Idempotency-Key was already used for a different request."))
	case rec.Status == 0:
		writeErr(w, r, apierror.New(http.StatusConflict, api.ErrorCodeRequestInProgress,
			"A request with this Idempotency-Key is still being processed; retry shortly."))
	default:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(HeaderIdempotentReplayed, "true")
		w.WriteHeader(rec.Status)
		_, _ = w.Write(rec.Body)
	}
}

// requestHash fingerprints what the key protects: method, path and body.
func requestHash(r *http.Request, body []byte) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s %s\n", r.Method, r.URL.Path)
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func printableASCII(s string) bool {
	for i := range len(s) {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return s != ""
}
