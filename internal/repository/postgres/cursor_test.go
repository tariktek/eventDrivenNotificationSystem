package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

func TestCursor_RoundTrip(t *testing.T) {
	c := cursor{CreatedAt: time.Date(2026, 11, 17, 12, 30, 45, 123456000, time.UTC), ID: uuid.New()}

	got, err := decodeCursor(c.encode())
	require.NoError(t, err)
	assert.Equal(t, c, *got)
}

func TestDecodeCursor_EmptyMeansFirstPage(t *testing.T) {
	got, err := decodeCursor("")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestDecodeCursor_Invalid(t *testing.T) {
	for _, s := range []string{
		"!!!",                 // not base64
		"bm8tc2VwYXJhdG9y",    // "no-separator"
		"eHx5",                // "x|y": bad timestamp
		"MTIzfG5vdC1hLXV1aWQ", // "123|not-a-uuid"
	} {
		_, err := decodeCursor(s)
		assert.ErrorIs(t, err, domain.ErrInvalidCursor, s)
	}
}

func TestClampLimit(t *testing.T) {
	assert.Equal(t, DefaultPageSize, clampLimit(0))
	assert.Equal(t, DefaultPageSize, clampLimit(-5))
	assert.Equal(t, 7, clampLimit(7))
	assert.Equal(t, MaxPageSize, clampLimit(10_000))
}

func TestPaginate(t *testing.T) {
	key := func(i int) cursor { return cursor{ID: uuid.UUID{byte(i)}} }

	p := paginate([]int{1, 2, 3}, 3, key)
	assert.Equal(t, []int{1, 2, 3}, p.Items)
	assert.False(t, p.HasMore)
	assert.Empty(t, p.NextCursor)

	p = paginate([]int{1, 2, 3, 4}, 3, key) // limit+1 rows fetched: there is more
	assert.Equal(t, []int{1, 2, 3}, p.Items)
	assert.True(t, p.HasMore)
	assert.Equal(t, key(3).encode(), p.NextCursor, "cursor points at the last row returned")

	p = paginate[int](nil, 3, key)
	assert.NotNil(t, p.Items, "empty page serialises as [] not null")
}

func TestNarrow(t *testing.T) {
	v, err := narrow[int16](6, "segments")
	require.NoError(t, err)
	assert.Equal(t, int16(6), v)

	_, err = narrow[int16](70000, "segments")
	assert.ErrorContains(t, err, "out of range", "must not wrap to 4464")

	_, err = narrow[int32](1<<40, "total")
	assert.Error(t, err)
}

// FuzzDecodeCursor: the cursor comes from the client, so any string must
// either decode to a cursor that encodes back to the same position or be
// rejected as ErrInvalidCursor (a 422), never panic or slip through as
// some other error (a 500).
func FuzzDecodeCursor(f *testing.F) {
	f.Add("")
	f.Add(cursor{CreatedAt: time.Unix(1_800_000_000, 0), ID: uuid.New()}.encode())
	f.Add("MTc4MDAwMDAwMDAwMDAwMHx4")
	f.Add("!!!")
	f.Fuzz(func(t *testing.T, s string) {
		c, err := decodeCursor(s)
		if err != nil {
			require.ErrorIs(t, err, domain.ErrInvalidCursor)
			return
		}
		if c == nil {
			require.Empty(t, s)
			return
		}
		again, err := decodeCursor(c.encode())
		require.NoError(t, err)
		require.Equal(t, c, again)
	})
}
