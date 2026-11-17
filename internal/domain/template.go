package domain

import (
	"time"

	"github.com/google/uuid"
)

// Template is a reusable message with {{.variable}} placeholders.
type Template struct {
	ID        uuid.UUID
	Name      string
	Channel   Channel
	Subject   string // email only; may contain placeholders
	Body      string
	Variables []string // placeholders the template expects
	Version   int
	DeletedAt *time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}
