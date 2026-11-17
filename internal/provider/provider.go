// Package provider delivers messages to the external notification provider
// over HTTP and classifies every outcome, which is what the retry logic
// decides on:
//
//	success    2xx with a valid {"messageId", "status":"accepted"} body
//	transient  worth retrying: timeouts, connection errors, 408, 429, 5xx,
//	           and 2xx bodies we cannot understand
//	permanent  retrying cannot help: other 4xx (bad recipient, rejected
//	           content, auth), redirects
package provider

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

// Message is one delivery request.
type Message struct {
	NotificationID uuid.UUID
	Channel        domain.Channel
	To             string
	Subject        string // email subject / push title; empty for sms
	Content        string
	CorrelationID  string
}

// Result is the classified outcome of one delivery attempt.
type Result struct {
	Outcome    domain.AttemptOutcome
	StatusCode int    // 0 when no HTTP response was received
	MessageID  string // the provider's id, on success
	// ErrorCode is a short, stable label for metrics and the attempts
	// table: "http_503", "timeout", "connection_error", ...
	ErrorCode string
	// Error describes the failure for humans; empty on success.
	Error string
	// RetryAfter is the provider's requested delay (Retry-After header),
	// zero if none was given.
	RetryAfter time.Duration
	Duration   time.Duration
}

// Sender delivers one message. Implementations never return a Go error:
// every failure is a classified Result, because "the provider is down" is
// an expected outcome the caller must handle, not an exceptional one.
type Sender interface {
	Send(ctx context.Context, msg *Message) Result
}
