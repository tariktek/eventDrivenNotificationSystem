package validation

import (
	"strings"
	"unicode/utf8"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

// Content limits per channel.
const (
	EmailSubjectMaxRunes = 255
	EmailBodyMaxBytes    = 100 * 1024
	PushTitleMaxRunes    = 256
	PushBodyMaxRunes     = 1024
)

// Content validates the final (already rendered) subject and content of a
// notification for its channel and returns the SMS segment count (0 for
// other channels). field prefixes error paths, e.g. "items[2]".
func Content(ch domain.Channel, subject, content, field string) (segments int, err error) {
	c := newCollector(field)
	segments = checkContent(c, ch, subject, content)
	return segments, c.err()
}

// checkContent checks subject and content independently, so every problem
// with either is reported in the same response.
func checkContent(c collector, ch domain.Channel, subject, content string) int {
	if checkText(c, "subject", subject) {
		checkSubject(c, ch, subject)
	}
	if !checkText(c, "content", content) {
		return 0
	}
	if content == "" {
		c.add("content", "is required")
		return 0
	}

	switch ch {
	case domain.ChannelSMS:
		info := AnalyzeSMS(content)
		if info.Segments > MaxSMSSegments {
			c.add("content", "is %d %s units long and needs %d SMS segments; the maximum is %d",
				info.Units, info.Encoding, info.Segments, MaxSMSSegments)
		}
		return info.Segments
	case domain.ChannelEmail:
		if len(content) > EmailBodyMaxBytes {
			c.add("content", "must be at most %d bytes", EmailBodyMaxBytes)
		}
	case domain.ChannelPush:
		if utf8.RuneCountInString(content) > PushBodyMaxRunes {
			c.add("content", "must be at most %d characters", PushBodyMaxRunes)
		}
	}
	return 0
}

func checkSubject(c collector, ch domain.Channel, subject string) {
	switch ch {
	case domain.ChannelSMS:
		if subject != "" {
			c.add("subject", "is not supported for sms")
		}
	case domain.ChannelEmail:
		switch {
		case subject == "":
			c.add("subject", "is required for email")
		case utf8.RuneCountInString(subject) > EmailSubjectMaxRunes:
			c.add("subject", "must be at most %d characters", EmailSubjectMaxRunes)
		case strings.ContainsAny(subject, "\r\n"):
			// A line break would let the subject inject extra mail headers.
			c.add("subject", "must not contain line breaks")
		}
	case domain.ChannelPush:
		if utf8.RuneCountInString(subject) > PushTitleMaxRunes {
			c.add("subject", "must be at most %d characters", PushTitleMaxRunes)
		}
	}
}

// checkText rejects strings PostgreSQL text columns cannot store or that are
// not valid UTF-8. It reports whether the value is usable.
func checkText(c collector, field, s string) bool {
	if !utf8.ValidString(s) {
		c.add(field, "must be valid UTF-8")
		return false
	}
	if strings.ContainsRune(s, 0) {
		c.add(field, "must not contain NUL characters")
		return false
	}
	return true
}

// containsNUL reports whether any key or string value inside a decoded JSON
// document contains U+0000, which PostgreSQL jsonb rejects.
func containsNUL(v any) bool {
	switch x := v.(type) {
	case string:
		return strings.ContainsRune(x, 0)
	case map[string]any:
		for k, val := range x {
			if strings.ContainsRune(k, 0) || containsNUL(val) {
				return true
			}
		}
	case []any:
		for _, val := range x {
			if containsNUL(val) {
				return true
			}
		}
	}
	return false
}
