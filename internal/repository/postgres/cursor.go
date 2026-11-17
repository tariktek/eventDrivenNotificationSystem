package postgres

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

// Pagination defaults.
const (
	DefaultPageSize = 50
	MaxPageSize     = 100
)

// cursor marks a position in a list ordered by (created_at DESC, id DESC).
// Keyset pagination stays fast at any depth, unlike OFFSET, which scans and
// discards every skipped row; it also cannot skip or repeat rows when new
// ones are inserted between page requests.
type cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// encode produces an opaque, URL-safe token. Clients must not parse it.
func (c cursor) encode() string {
	raw := strconv.FormatInt(c.CreatedAt.UnixMicro(), 10) + "|" + c.ID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (*cursor, error) {
	if s == "" {
		return nil, nil //nolint:nilnil // no cursor means "first page"
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: not base64", domain.ErrInvalidCursor)
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return nil, fmt.Errorf("%w: malformed", domain.ErrInvalidCursor)
	}
	micros, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: bad timestamp", domain.ErrInvalidCursor)
	}
	uid, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("%w: bad id", domain.ErrInvalidCursor)
	}
	return &cursor{CreatedAt: time.UnixMicro(micros).UTC(), ID: uid}, nil
}

// clampLimit applies the default and the maximum page size.
func clampLimit(n int) int {
	switch {
	case n <= 0:
		return DefaultPageSize
	case n > MaxPageSize:
		return MaxPageSize
	default:
		return n
	}
}

// Page is one page of a keyset-paginated list.
type Page[T any] struct {
	Items      []T
	NextCursor string // empty when HasMore is false
	HasMore    bool
}

// paginate trims the extra row fetched to detect a next page (queries ask
// for limit+1) and builds the cursor from the last row kept.
func paginate[T any](items []T, limit int, key func(T) cursor) Page[T] {
	p := Page[T]{Items: items}
	if len(items) > limit {
		p.Items = items[:limit]
		p.HasMore = true
		p.NextCursor = key(p.Items[limit-1]).encode()
	}
	if p.Items == nil {
		p.Items = []T{}
	}
	return p
}
