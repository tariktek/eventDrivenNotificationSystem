package validation_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

func TestAnalyzeSMS(t *testing.T) {
	a := func(n int) string { return strings.Repeat("a", n) }
	tests := []struct {
		name     string
		in       string
		encoding validation.SMSEncoding
		units    int
		segments int
	}{
		{"empty", "", validation.EncodingGSM7, 0, 0},
		{"short ascii", "Hello", validation.EncodingGSM7, 5, 1},
		{"gsm single limit", a(160), validation.EncodingGSM7, 160, 1},
		{"gsm one over", a(161), validation.EncodingGSM7, 161, 2},
		{"gsm two full parts", a(306), validation.EncodingGSM7, 306, 2},
		{"gsm three parts", a(307), validation.EncodingGSM7, 307, 3},
		{"gsm six full parts", a(918), validation.EncodingGSM7, 918, 6},
		{"gsm seven parts", a(919), validation.EncodingGSM7, 919, 7},

		{"extension char costs two septets", "€", validation.EncodingGSM7, 2, 1},
		{"extension pushes over single limit", a(159) + "€", validation.EncodingGSM7, 161, 2},
		// 152 + [€] + 152 = 306 units: ceil(306/153) says 2, but the escape
		// pair cannot straddle the boundary, so it moves to part 2 and one
		// septet spills into part 3.
		{"escape pair not split across parts", a(152) + "€" + a(152), validation.EncodingGSM7, 306, 3},

		{"gsm accented letters stay gsm", "Çéüöä", validation.EncodingGSM7, 5, 1},
		// GSM 03.38 has capital Ç but no lowercase ç: most Turkish text,
		// even without ş/ğ/ı, falls back to UCS-2 and 70 characters.
		{"lowercase c-cedilla forces ucs2", "çab", validation.EncodingUCS2, 3, 1},
		{"turkish letters force ucs2", "Ş-code: 1234", validation.EncodingUCS2, 12, 1},
		{"ucs2 single limit", "ş" + a(69), validation.EncodingUCS2, 70, 1},
		{"ucs2 one over", "ş" + a(70), validation.EncodingUCS2, 71, 2},
		{"emoji is a surrogate pair", "😀", validation.EncodingUCS2, 2, 1},
		// 66 + [😀] + 66 = 134 units: ceil(134/67) says 2, but the surrogate
		// pair moves to part 2 and one unit spills into part 3.
		{"surrogate pair not split across parts", "ş" + a(65) + "😀" + a(66), validation.EncodingUCS2, 134, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := validation.AnalyzeSMS(tt.in)
			assert.Equal(t, tt.encoding, info.Encoding)
			assert.Equal(t, tt.units, info.Units)
			assert.Equal(t, tt.segments, info.Segments)
		})
	}
}

func FuzzAnalyzeSMS(f *testing.F) {
	for _, seed := range []string{"", "hello", "€€€", "Ş-code", "😀😀", strings.Repeat("a", 200), "\xff\xfe"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		info := validation.AnalyzeSMS(s)

		if s == "" {
			if info.Segments != 0 || info.Units != 0 {
				t.Fatalf("empty input: got %+v", info)
			}
			return
		}
		if info.Segments < 1 || info.Units < 1 {
			t.Fatalf("non-empty input %q: got %+v", s, info)
		}
		// Packing can need at most one extra part per boundary compared with
		// plain division, and never fewer parts than division.
		multi := 153
		if info.Encoding == validation.EncodingUCS2 {
			multi = 67
		}
		if info.Segments > 1 {
			lower := (info.Units + multi - 1) / multi
			if info.Segments < lower || info.Segments > 2*lower {
				t.Fatalf("implausible segment count %d for %d units", info.Segments, info.Units)
			}
		}
	})
}
