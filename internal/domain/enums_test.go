package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

func TestParseChannel(t *testing.T) {
	for _, c := range domain.Channels {
		got, err := domain.ParseChannel(string(c))
		require.NoError(t, err)
		assert.Equal(t, c, got)
	}
	for _, bad := range []string{"", "SMS", "whatsapp", " sms"} {
		_, err := domain.ParseChannel(bad)
		assert.Error(t, err, "%q must be rejected", bad)
	}
}

func TestParsePriority(t *testing.T) {
	tests := []struct {
		in      string
		want    domain.Priority
		wantErr bool
	}{
		{"high", domain.PriorityHigh, false},
		{"normal", domain.PriorityNormal, false},
		{"low", domain.PriorityLow, false},
		{"", domain.PriorityNormal, false}, // omitted means default
		{"urgent", "", true},
		{"HIGH", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := domain.ParsePriority(tt.in)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPriority_RankOrdersHighFirst(t *testing.T) {
	assert.Less(t, domain.PriorityHigh.Rank(), domain.PriorityNormal.Rank())
	assert.Less(t, domain.PriorityNormal.Rank(), domain.PriorityLow.Rank())
}

func TestPriority_RankRoundTrip(t *testing.T) {
	for _, p := range []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow} {
		got, err := domain.PriorityFromRank(p.Rank())
		require.NoError(t, err)
		assert.Equal(t, p, got)
	}
	_, err := domain.PriorityFromRank(3)
	assert.Error(t, err)
}
