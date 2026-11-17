package middleware

import (
	"bytes"
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
	HeaderIdempotencyKey      = "Idempotency-Key"
	HeaderIdempotentReplayed  = "Idempotent-Replayed"
	idempotencyKeyMaxLen      = 255
	idempotencyStaleAfterTime = time.Minute
)

// IdempotencyConfig configures the Idempotency middleware.
type IdempotencyConfig struct {
	Store idempotency.Store
	TTL   time.Duration
	Now   func() time.Time
	Log   *slog.Logger
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

			lease, rec, acquired, err := cfg.Store.Acquire(ctx, scoped, hash, cfg.Now(), cfg.TTL, idempotencyStaleAfterTime)
			if err != nil {
				writeErr(w, r, fmt.Errorf("acquire idempotency key: %w", err))
				return
			}
			if !acquired {
				replayOrReject(w, r, rec, hash, writeErr)
				return
			}

			// We own the key. Release it unless the request ran to a stored
			// (or deliberately kept) outcome, so a panic or 5xx never blocks
			// the client's retry. Cleanup must outlive a cancelled request.
			cleanupCtx := context.WithoutCancel(ctx)
			keep := false
			defer func() {
				if keep {
					return
				}
				if err := cfg.Store.Release(cleanupCtx, lease); err != nil {
					cfg.Log.ErrorContext(cleanupCtx, "release idempotency key", "error", err)
				}
			}()

			sw := &statusWriter{ResponseWriter: w, capture: &bytes.Buffer{}}
			next.ServeHTTP(sw, r)

			if sw.Status() >= http.StatusInternalServerError {
				return
			}
			// The client already has its response, so from here on the key
			// must never be released: a retry would execute the request a
			// second time. Store the response, retrying briefly on failure.
			keep = true
			complete(cleanupCtx, cfg, lease, sw.Status(), sw.capture.Bytes())
		})
	}
}

// completeAttempts and completeBackoff bound how hard we try to store a
// response before giving up and leaving the key locked (a retry then gets
// 409 until the stale timeout, which is safer than running twice).
const completeAttempts = 3

var completeBackoff = 50 * time.Millisecond

func complete(ctx context.Context, cfg IdempotencyConfig, lease idempotency.Lease, status int, body []byte) {
	var err error
	for attempt := range completeAttempts {
		if attempt > 0 {
			time.Sleep(completeBackoff << (attempt - 1))
		}
		err = cfg.Store.Complete(ctx, lease, status, body)
		if err == nil {
			return
		}
		if errors.Is(err, idempotency.ErrLeaseLost) {
			cfg.Log.WarnContext(ctx, "idempotency key was taken over while the request ran", "key", lease.Key)
			return
		}
	}
	cfg.Log.ErrorContext(ctx, "could not store idempotent response; key stays locked", "key", lease.Key, "error", err)
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
