package validation

import "unicode/utf16"

// SMSEncoding is the character encoding an SMS will be sent with.
type SMSEncoding string

// SMS encodings. GSM-7 packs 160 characters into one message; any character
// outside the GSM 03.38 alphabet (Turkish ş/ğ/ı/İ and lowercase ç, emoji, ...)
// forces UCS-2, which fits only 70.
const (
	EncodingGSM7 SMSEncoding = "GSM-7"
	EncodingUCS2 SMSEncoding = "UCS-2"
)

// Segment capacities. A multi-part message spends part of each segment on
// the concatenation header (UDH), so each part holds less than a single SMS.
const (
	gsmSingle  = 160
	gsmMulti   = 153
	ucs2Single = 70
	ucs2Multi  = 67

	// MaxSMSSegments is the longest message we accept. Operators charge per
	// segment and handsets reassemble only so many parts reliably.
	MaxSMSSegments = 6
)

// GSM 03.38 basic character set: each character costs one septet.
const gsmBasic = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?" +
	"¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"

// GSM 03.38 extension table: each character costs two septets (escape + char).
const gsmExtension = "\f^{}\\[~]|€"

var gsmWeights = func() map[rune]int {
	m := make(map[rune]int, len(gsmBasic)+len(gsmExtension))
	for _, r := range gsmBasic {
		m[r] = 1
	}
	for _, r := range gsmExtension {
		m[r] = 2
	}
	return m
}()

// SMSInfo describes how a message will be transmitted.
type SMSInfo struct {
	Encoding SMSEncoding
	Units    int // GSM-7 septets or UTF-16 code units
	Segments int
}

// AnalyzeSMS determines the encoding and segment count of a message.
//
// Segments are counted by actually packing the message, not by dividing:
// a two-unit character (a GSM escape pair or a UTF-16 surrogate pair) is
// never split across segments, so near the boundaries the true count can be
// one higher than ceil(units / capacity).
func AnalyzeSMS(s string) SMSInfo {
	gsmUnits, ucs2Units, isGSM := 0, 0, true
	for _, r := range s {
		if w, ok := gsmWeights[r]; ok {
			gsmUnits += w
		} else {
			isGSM = false
		}
		ucs2Units += ucs2Weight(r)
	}

	info := SMSInfo{Encoding: EncodingUCS2, Units: ucs2Units}
	single, multi := ucs2Single, ucs2Multi
	if isGSM {
		info = SMSInfo{Encoding: EncodingGSM7, Units: gsmUnits}
		single, multi = gsmSingle, gsmMulti
	}

	switch {
	case info.Units == 0:
		info.Segments = 0
	case info.Units <= single:
		info.Segments = 1
	default:
		info.Segments = 1
		used := 0
		for _, r := range s {
			w := ucs2Weight(r)
			if isGSM {
				w = gsmWeights[r]
			}
			if used+w > multi {
				info.Segments++
				used = 0
			}
			used += w
		}
	}
	return info
}

func ucs2Weight(r rune) int {
	if n := utf16.RuneLen(r); n > 0 {
		return n
	}
	return 1 // invalid rune; it is sent as U+FFFD
}
