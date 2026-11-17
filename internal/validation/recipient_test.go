package validation_test

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

func TestRecipient(t *testing.T) {
	tests := []struct {
		ch    domain.Channel
		in    string
		valid bool
	}{
		{domain.ChannelSMS, "+905551234567", true},
		{domain.ChannelSMS, "+14155552671", true},
		{domain.ChannelSMS, "+123456789012345", true}, // 15 digits, the E.164 maximum
		{domain.ChannelSMS, "+1234567890123456", false},
		{domain.ChannelSMS, "905551234567", false}, // missing +
		{domain.ChannelSMS, "+05551234567", false}, // country code cannot start with 0
		{domain.ChannelSMS, "+90 555 123 4567", false},
		{domain.ChannelSMS, "+1234567", false}, // too short
		{domain.ChannelSMS, "", false},

		{domain.ChannelEmail, "user@example.com", true},
		{domain.ChannelEmail, "first.last+tag@sub.example.co.uk", true},
		{domain.ChannelEmail, "Name <user@example.com>", false}, // display names not allowed
		{domain.ChannelEmail, "user@localhost", false},
		{domain.ChannelEmail, "user.example.com", false},
		{domain.ChannelEmail, " user@example.com", false},
		{domain.ChannelEmail, strings.Repeat("a", 250) + "@x.co", false},

		{domain.ChannelPush, "fcm:APA91bH-token_value", true},
		{domain.ChannelPush, strings.Repeat("t", 4096), true},
		{domain.ChannelPush, strings.Repeat("t", 4097), false},
		{domain.ChannelPush, "token with spaces", false},
		{domain.ChannelPush, "tok\nen", false},
	}
	for _, tt := range tests {
		name := string(tt.ch) + ":" + tt.in
		if len(name) > 60 {
			name = name[:60]
		}
		t.Run(name, func(t *testing.T) {
			err := validation.Recipient(tt.ch, tt.in, "")
			if tt.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestIsPublicIP(t *testing.T) {
	tests := map[string]bool{
		"8.8.8.8":            true,
		"2606:4700::1111":    true,
		"127.0.0.1":          false,
		"10.1.2.3":           false,
		"172.16.0.1":         false,
		"192.168.1.1":        false,
		"169.254.169.254":    false, // cloud metadata endpoint
		"100.64.0.1":         false, // carrier-grade NAT
		"0.0.0.0":            false,
		"::1":                false,
		"fd00::1":            false,
		"fe80::1":            false,
		"::ffff:10.0.0.1":    false, // IPv4-mapped private address
		"224.0.0.1":          false,
		"0.1.2.3":            false, // "this network"
		"198.18.0.1":         false, // benchmarking
		"192.0.0.8":          false,
		"203.0.113.9":        false, // documentation
		"240.0.0.1":          false, // reserved
		"255.255.255.255":    false,
		"64:ff9b::a9fe:a9fe": false, // NAT64 for 169.254.169.254
		"2002:7f00:1::":      false, // 6to4 for 127.0.0.1
		"2001:0:4136::1":     false, // Teredo
		"2001:db8::1":        false, // documentation
	}
	for in, want := range tests {
		t.Run(in, func(t *testing.T) {
			assert.Equal(t, want, validation.IsPublicIP(netip.MustParseAddr(in)))
		})
	}
}
