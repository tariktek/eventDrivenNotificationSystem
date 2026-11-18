package callback

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestTruncate_StorableInPostgres(t *testing.T) {
	long := strings.Repeat("a", 499) + "ş" + strings.Repeat("b", 100) // "ş" straddles byte 500
	got := truncate(long)
	assert.True(t, utf8.ValidString(got), "never cut a character in half")
	assert.LessOrEqual(t, len(got), 500)
	assert.Equal(t, strings.Repeat("a", 499), got)

	assert.Equal(t, "badbyte� and nul", truncate("bad\x00byte\xff and nul"))
	assert.Equal(t, "short", truncate("short"))
}
