package domain

import (
	"errors"
	"fmt"
)

// Errors shared across layers. Storage implementations translate their own
// errors into these so callers never depend on a specific database.
var (
	// ErrNotFound means the requested entity does not exist.
	ErrNotFound = errors.New("not found")
	// ErrAlreadyExists means a uniqueness rule was violated (e.g. a
	// template name already in use).
	ErrAlreadyExists = errors.New("already exists")
	// ErrInvalidCursor means a pagination cursor could not be decoded.
	ErrInvalidCursor = errors.New("invalid pagination cursor")
)

// IdempotencyKeyReusedError means a notification's idempotency_key already
// belongs to a notification created by a different request. Index is the
// position of the offending notification among those being created.
type IdempotencyKeyReusedError struct {
	Index int
	Key   string
	// Field is the request path of the key ("idempotency_key",
	// "items[3].idempotency_key"); set by the caller that knows the shape.
	Field string
}

func (e *IdempotencyKeyReusedError) Error() string {
	return fmt.Sprintf("idempotency_key %q was already used for a different notification", e.Key)
}
