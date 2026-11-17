package domain

import (
	"errors"
	"fmt"
)

// Status is the lifecycle state of a notification.
//
//	pending/scheduled ──claim──▶ processing ──202──▶ sent
//	        ▲   │                  │
//	        │   └──▶ cancelled     ├──▶ pending   (transient error with attempts
//	        │                      │               left, or the lease expired)
//	        │                      └──▶ failed    (permanent error or attempts
//	        └──────── manual retry ◀──── failed     exhausted)
type Status string

// Possible statuses.
const (
	StatusPending    Status = "pending"
	StatusScheduled  Status = "scheduled"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// Statuses lists every status.
var Statuses = []Status{
	StatusPending, StatusScheduled, StatusProcessing,
	StatusSent, StatusFailed, StatusCancelled,
}

// transitions is the single source of truth for which status changes are
// legal. Repository UPDATEs mirror it in their WHERE clauses so a concurrent
// actor cannot break it either.
var transitions = map[Status][]Status{
	StatusPending:    {StatusProcessing, StatusCancelled},
	StatusScheduled:  {StatusProcessing, StatusCancelled},
	StatusProcessing: {StatusSent, StatusPending, StatusFailed},
	StatusFailed:     {StatusPending},
	StatusSent:       nil,
	StatusCancelled:  nil,
}

// ErrInvalidTransition is matched (via errors.Is) by every TransitionError.
var ErrInvalidTransition = errors.New("invalid status transition")

// TransitionError reports an illegal status change.
type TransitionError struct {
	From, To Status
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("invalid status transition %s -> %s", e.From, e.To)
}

// Is lets errors.Is(err, ErrInvalidTransition) match.
func (e *TransitionError) Is(target error) bool { return target == ErrInvalidTransition }

// ParseStatus converts external input (e.g. a list filter) into a Status.
func ParseStatus(s string) (Status, error) {
	st := Status(s)
	if !st.Valid() {
		return "", fmt.Errorf("unknown status %q", s)
	}
	return st, nil
}

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	_, ok := transitions[s]
	return ok
}

// CanTransitionTo reports whether moving from s to next is legal.
func (s Status) CanTransitionTo(next Status) bool {
	for _, allowed := range transitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// TransitionTo returns a *TransitionError if moving from s to next is illegal.
func (s Status) TransitionTo(next Status) error {
	if !s.CanTransitionTo(next) {
		return &TransitionError{From: s, To: next}
	}
	return nil
}

// IsFinal reports whether no further automatic processing will happen. A
// batch is complete once all of its notifications are final. (A failed
// notification can still be retried manually.)
func (s Status) IsFinal() bool {
	return s == StatusSent || s == StatusFailed || s == StatusCancelled
}

// IsCancellable reports whether a notification in this status can be
// cancelled, i.e. no worker has picked it up yet.
func (s Status) IsCancellable() bool {
	return s.CanTransitionTo(StatusCancelled)
}

func (s Status) String() string { return string(s) }
