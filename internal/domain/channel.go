// Package domain holds the core types and rules of the notification system.
// It depends on nothing but the standard library and uuid, so the rules can
// be tested in isolation and reused by the API, the worker and the database
// layer alike.
package domain

import "fmt"

// Channel is a delivery channel.
type Channel string

// Supported channels.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel; workers run one pool per entry.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

// ParseChannel converts external input into a Channel.
func ParseChannel(s string) (Channel, error) {
	c := Channel(s)
	if !c.Valid() {
		return "", fmt.Errorf("unknown channel %q: must be one of sms, email, push", s)
	}
	return c, nil
}

// Valid reports whether c is a supported channel.
func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	default:
		return false
	}
}

func (c Channel) String() string { return string(c) }
