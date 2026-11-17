package middleware

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/api"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/apierror"
)

// HeaderAPIKey carries the client's API key.
const HeaderAPIKey = "X-API-Key" //nolint:gosec // a header name, not a credential

type clientKey struct{}

// APIKeyAuth rejects requests without a valid X-API-Key with 401.
//
// Keys are compared as SHA-256 digests with subtle.ConstantTimeCompare, and
// every configured key is checked, so response timing reveals neither how
// much of a guess matched nor which key it was close to.
func APIKeyAuth(keys []string, writeErr ErrorWriter) func(http.Handler) http.Handler {
	digests := make([][32]byte, len(keys))
	for i, k := range keys {
		digests[i] = sha256.Sum256([]byte(k))
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get(HeaderAPIKey)
			if key == "" {
				writeErr(w, r, apierror.New(http.StatusUnauthorized, api.ErrorCodeUnauthorized, "Missing X-API-Key header."))
				return
			}
			got := sha256.Sum256([]byte(key))
			match := 0
			for _, d := range digests {
				match |= subtle.ConstantTimeCompare(got[:], d[:])
			}
			if match != 1 {
				writeErr(w, r, apierror.New(http.StatusUnauthorized, api.ErrorCodeUnauthorized, "Invalid API key."))
				return
			}
			// A short, non-reversible client identity for scoping
			// idempotency keys; the key itself is never stored.
			client := hex.EncodeToString(got[:8])
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), clientKey{}, client)))
		})
	}
}

// ClientID identifies the authenticated client, or "" before APIKeyAuth.
func ClientID(ctx context.Context) string {
	id, _ := ctx.Value(clientKey{}).(string)
	return id
}
