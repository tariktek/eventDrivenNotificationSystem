package events

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// toMessage mimics what Redis returns: every value comes back a string.
func toMessage(e *Event) redis.XMessage {
	vals := map[string]any{}
	for k, v := range e.values() {
		if n, ok := v.(int64); ok {
			v = strconv.FormatInt(n, 10)
		}
		vals[k] = v
	}
	return redis.XMessage{ID: "1-0", Values: vals}
}

func TestDecode_RoundTrip(t *testing.T) {
	in := Event{
		ID: 42, Type: TypeNotificationStatusChanged, AggregateType: "notification", AggregateID: uuid.New(),
		Payload: json.RawMessage(`{"status":"sent"}`), CreatedAt: time.Date(2026, 11, 18, 10, 0, 0, 123, time.UTC),
	}
	out, err := Decode(toMessage(&in))
	require.NoError(t, err)
	in.StreamID = "1-0"
	assert.Equal(t, in, out)
}

func TestDecode_Malformed(t *testing.T) {
	good := Event{ID: 1, Type: TypeBatchCompleted, AggregateType: "batch", AggregateID: uuid.New(), Payload: json.RawMessage(`{}`), CreatedAt: time.Now()}
	for name, mutate := range map[string]func(map[string]any){
		"bad id":       func(v map[string]any) { v[fieldID] = "x" },
		"bad uuid":     func(v map[string]any) { v[fieldAggregateID] = "nope" },
		"bad time":     func(v map[string]any) { v[fieldCreatedAt] = "yesterday" },
		"no type":      func(v map[string]any) { delete(v, fieldType) },
		"invalid json": func(v map[string]any) { v[fieldPayload] = "{" },
	} {
		t.Run(name, func(t *testing.T) {
			m := toMessage(&good)
			mutate(m.Values)
			_, err := Decode(m)
			assert.ErrorIs(t, err, ErrMalformed)
		})
	}
}
