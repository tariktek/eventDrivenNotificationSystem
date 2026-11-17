package validation

import (
	"net/mail"
	"regexp"
	"strings"
	"unicode"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

const (
	emailMaxLen     = 254 // RFC 5321 path limit
	pushTokenMaxLen = 4096
)

// e164 is the international phone number format: "+", a non-zero country
// code digit, then 7 to 14 more digits.
var e164 = regexp.MustCompile(`^\+[1-9]\d{7,14}$`)

// Recipient validates the recipient address format for a channel.
func Recipient(ch domain.Channel, recipient, field string) error {
	c := newCollector(field)
	checkRecipient(c, ch, recipient)
	return c.err()
}

func checkRecipient(c collector, ch domain.Channel, r string) {
	if r == "" {
		c.add("recipient", "is required")
		return
	}
	if strings.TrimSpace(r) != r {
		c.add("recipient", "must not have leading or trailing whitespace")
		return
	}

	switch ch {
	case domain.ChannelSMS:
		if !e164.MatchString(r) {
			c.add("recipient", "must be a phone number in E.164 format, e.g. +905551234567")
		}
	case domain.ChannelEmail:
		if len(r) > emailMaxLen {
			c.add("recipient", "must be at most %d characters", emailMaxLen)
			return
		}
		addr, err := mail.ParseAddress(r)
		// ParseAddress also accepts `Name <a@b.c>`; only a bare address is allowed.
		if err != nil || addr.Address != r || !strings.Contains(r[strings.LastIndex(r, "@")+1:], ".") {
			c.add("recipient", "must be a valid email address, e.g. user@example.com")
		}
	case domain.ChannelPush:
		if len(r) > pushTokenMaxLen {
			c.add("recipient", "must be at most %d characters", pushTokenMaxLen)
			return
		}
		if strings.IndexFunc(r, func(x rune) bool { return unicode.IsSpace(x) || unicode.IsControl(x) }) >= 0 {
			c.add("recipient", "must be a device token without whitespace or control characters")
		}
	}
}
