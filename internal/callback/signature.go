// Package callback delivers status callbacks: when a notification reaches a
// final status (sent, failed, cancelled) and was created with a
// callback_url, the client gets a signed POST.
//
// Flow: when a notification with a callback_url reaches a final status,
// the outbox trigger writes a callback_deliveries row in the same
// transaction (migration 19), so a callback exists exactly when its status
// change committed, and one notification's callbacks are created in order.
// A Dispatcher claims due rows, one notification's in event order, POSTs
// them and records the outcome, retrying with backoff. Delivery state lives
// in PostgreSQL, so retries survive restarts.
//
// Every request is signed so the receiver can check that it came from us
// and is not a replay. See Sign and Verify.
package callback

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Request headers.
const (
	HeaderSignature = "X-Signature"  // "sha256=" + hex(HMAC-SHA256(secret, timestamp + "." + body))
	HeaderTimestamp = "X-Timestamp"  // Unix seconds, part of the signed message
	HeaderEventID   = "X-Event-Id"   // stable across retries: the receiver's deduplication key
	HeaderEventType = "X-Event-Type" // e.g. notification.sent
)

const signaturePrefix = "sha256="

// Sign returns the X-Signature value for a body sent at timestamp ts.
// Signing the timestamp with the body means an attacker who captured a
// request cannot replay it later with a fresh timestamp.
func Sign(secret []byte, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// Verification errors.
var (
	ErrBadSignature = errors.New("callback signature does not match")
	ErrStale        = errors.New("callback timestamp outside the tolerance window")
)

// Verify checks a received callback: the signature must match and the
// timestamp must be within tolerance of now (5 minutes is typical). This
// is what a client's receiver runs; the mock provider uses it too.
func Verify(secret []byte, signature, timestamp string, body []byte, now time.Time, tolerance time.Duration) error {
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrBadSignature
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return ErrStale
	}
	if !strings.HasPrefix(signature, signaturePrefix) ||
		!hmac.Equal([]byte(signature), []byte(Sign(secret, ts, body))) { // constant time
		return ErrBadSignature
	}
	return nil
}
