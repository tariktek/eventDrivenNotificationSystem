package domain

import "fmt"

// Priority orders work within a channel's queue.
//
// It is string-backed on purpose: an integer type would make the zero value
// silently mean "high". Here the zero value is invalid and NewNotification
// maps it to the default (normal).
type Priority string

// Supported priorities.
const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// ParsePriority converts external input into a Priority. An empty string
// selects the default, PriorityNormal.
func ParsePriority(s string) (Priority, error) {
	if s == "" {
		return PriorityNormal, nil
	}
	p := Priority(s)
	if !p.Valid() {
		return "", fmt.Errorf("unknown priority %q: must be one of high, normal, low", s)
	}
	return p, nil
}

// Valid reports whether p is a supported priority.
func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	default:
		return false
	}
}

// Rank is the value stored in the database. The queue orders by it
// ascending, so a lower rank is served first.
func (p Priority) Rank() int16 {
	switch p {
	case PriorityHigh:
		return 0
	case PriorityLow:
		return 2
	default:
		return 1
	}
}

// PriorityFromRank is the inverse of Rank.
func PriorityFromRank(r int16) (Priority, error) {
	switch r {
	case 0:
		return PriorityHigh, nil
	case 1:
		return PriorityNormal, nil
	case 2:
		return PriorityLow, nil
	default:
		return "", fmt.Errorf("invalid priority rank %d", r)
	}
}

func (p Priority) String() string { return string(p) }
