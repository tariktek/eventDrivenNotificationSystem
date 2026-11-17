package domain

import (
	"time"

	"github.com/google/uuid"
)

// AttemptOutcome classifies the result of one delivery attempt.
type AttemptOutcome string

// Possible outcomes. A transient error is worth retrying (timeouts, 5xx,
// 429); a permanent one is not (e.g. 400 invalid recipient).
const (
	OutcomeSuccess        AttemptOutcome = "success"
	OutcomeTransientError AttemptOutcome = "transient_error"
	OutcomePermanentError AttemptOutcome = "permanent_error"
)

// Attempt records one call to the provider.
type Attempt struct {
	ID                int64
	NotificationID    uuid.UUID
	AttemptNo         int
	Outcome           AttemptOutcome
	StatusCode        int // HTTP status; 0 when the request never got a response
	ErrorCode         string
	Error             string
	ProviderMessageID string
	Duration          time.Duration
	CreatedAt         time.Time
}
