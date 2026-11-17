package domain

import "errors"

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
