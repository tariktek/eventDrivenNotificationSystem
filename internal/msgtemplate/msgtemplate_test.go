package msgtemplate_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/msgtemplate"
)

func TestRender(t *testing.T) {
	tpl, err := msgtemplate.Parse(
		"Order {{.order_id}} shipped",
		"Hi {{.name}}, order {{.order_id}} ships via {{upper .carrier}}.",
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"carrier", "name", "order_id"}, tpl.Variables)

	subject, body, err := tpl.Render(map[string]any{"name": "Ada", "order_id": 42, "carrier": "ups"})
	require.NoError(t, err)
	assert.Equal(t, "Order 42 shipped", subject)
	assert.Equal(t, "Hi Ada, order 42 ships via UPS.", body)
}

func TestRender_NoSubject(t *testing.T) {
	tpl, err := msgtemplate.Parse("", "Code: {{.code}}")
	require.NoError(t, err)

	subject, body, err := tpl.Render(map[string]any{"code": "1234"})
	require.NoError(t, err)
	assert.Empty(t, subject)
	assert.Equal(t, "Code: 1234", body)
}

func TestRender_MissingVariablesListedTogether(t *testing.T) {
	tpl, err := msgtemplate.Parse("{{.a}}", "{{.b}} {{.c}}")
	require.NoError(t, err)

	_, _, err = tpl.Render(map[string]any{"b": 1})

	var missing *msgtemplate.MissingVariablesError
	require.True(t, errors.As(err, &missing))
	assert.Equal(t, []string{"a", "c"}, missing.Names)
}

func TestRender_NilVarsWithoutPlaceholders(t *testing.T) {
	tpl, err := msgtemplate.Parse("", "Static text")
	require.NoError(t, err)
	_, body, err := tpl.Render(nil)
	require.NoError(t, err)
	assert.Equal(t, "Static text", body)
}

func TestRender_Helpers(t *testing.T) {
	tpl, err := msgtemplate.Parse("", `{{lower .a}}|{{trim .b}}|{{default "there" .c}}|{{default "there" .d}}`)
	require.NoError(t, err)

	_, body, err := tpl.Render(map[string]any{"a": "LOUD", "b": "  x  ", "c": "", "d": "Ada"})
	require.NoError(t, err)
	assert.Equal(t, "loud|x|there|Ada", body)
}

func TestParse_VariablesInsideRangeAndWithAreNotTopLevel(t *testing.T) {
	tpl, err := msgtemplate.Parse("",
		`{{range .items}}{{.title}} {{end}}{{with .user}}{{.email}}{{end}}{{if .vip}}{{.perk}}{{end}}`)
	require.NoError(t, err)

	// .title and .email belong to range/with scopes; .perk is still top-level.
	assert.Equal(t, []string{"items", "perk", "user", "vip"}, tpl.Variables)

	_, body, err := tpl.Render(map[string]any{
		"items": []any{map[string]any{"title": "A"}, map[string]any{"title": "B"}},
		"user":  map[string]any{"email": "a@b.co"},
		"vip":   false,
		"perk":  "",
	})
	require.NoError(t, err)
	assert.Equal(t, "A B a@b.co", body)
}

func TestParse_DollarVariablesAreCollected(t *testing.T) {
	tpl, err := msgtemplate.Parse("", `{{range .items}}{{$.greeting}} {{.}}{{end}}`)
	require.NoError(t, err)
	assert.Equal(t, []string{"greeting", "items"}, tpl.Variables)

	_, _, err = tpl.Render(map[string]any{"items": []any{"a"}})
	var missing *msgtemplate.MissingVariablesError
	require.True(t, errors.As(err, &missing), "$.greeting must be required")
	assert.Equal(t, []string{"greeting"}, missing.Names)
}

func TestRender_NullVariableIsMissing(t *testing.T) {
	tpl, err := msgtemplate.Parse("", "Hi {{.name}}")
	require.NoError(t, err)

	_, _, err = tpl.Render(map[string]any{"name": nil})

	var missing *msgtemplate.MissingVariablesError
	require.True(t, errors.As(err, &missing), "null must not render as <no value>")
	assert.Equal(t, []string{"name"}, missing.Names)
}

func TestParse_AllowedBuiltins(t *testing.T) {
	tpl, err := msgtemplate.Parse("", `{{if and (gt (len .items) 0) (not .muted)}}{{len .items}} new{{end}}`)
	require.NoError(t, err)
	_, body, err := tpl.Render(map[string]any{"items": []any{1, 2}, "muted": false})
	require.NoError(t, err)
	assert.Equal(t, "2 new", body)
}

func TestParse_Rejects(t *testing.T) {
	tests := map[string]string{
		"syntax error":        "Hello {{.name",
		"unknown function":    "{{exec .cmd}}",
		"define":              `{{define "x"}}loop{{end}}hi`,
		"template call":       `{{template "body" .}}`,
		"template in if":      `{{if .a}}{{template "body" .}}{{end}}`,
		"template in range":   `{{range .a}}{{template "body" .}}{{end}}`,
		"template in else":    `{{if .a}}x{{else}}{{template "body" .}}{{end}}`,
		"unknown func in arg": `{{upper (shell .x)}}`,
		// Memory exhaustion: fmt builds the whole string before any write.
		"printf":   `{{printf "%0999999999d" 1}}`,
		"print":    `{{print .a}}`,
		"call":     `{{call .fn}}`,
		"index":    `{{index . "first-name"}}`,
		"slice":    `{{slice .a 1}}`,
		"html":     `{{html .a}}`,
		"urlquery": `{{urlquery .a}}`,
		// CPU exhaustion: loops that write nothing never hit the output cap.
		"nested range":           `{{range .a}}{{range .b}}{{end}}{{end}}`,
		"range in with in range": `{{range .a}}{{with .b}}{{range .c}}{{end}}{{end}}{{end}}`,
		"range over literal":     `{{range 1000000000}}{{end}}`,
		// The same billion iterations without a literal in the range.
		"range over declared variable":   `{{$n := 1000000000}}{{range $n}}{{end}}ok`,
		"range over function result":     `{{range len .a}}{{end}}`,
		"range over dot rebound by with": `{{with 1000000000}}{{range .}}{{end}}{{end}}`,
		"range over pipeline":            `{{range (.a)}}{{end}}`,
		"printf hidden in pipe":          `{{.a | printf "%v"}}`,
		"printf in if condition":         `{{if printf "x"}}y{{end}}`,
		"printf in range else":           `{{range .a}}{{else}}{{printf "x"}}{{end}}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := msgtemplate.Parse("", body)
			assert.Error(t, err)
		})
	}
}

func TestRender_OutputCapped(t *testing.T) {
	tpl, err := msgtemplate.Parse("", `{{range .rows}}{{.}}{{end}}`)
	require.NoError(t, err)

	rows := make([]string, 300)
	for i := range rows {
		rows[i] = strings.Repeat("x", 1024) // 300 KB total
	}
	_, _, err = tpl.Render(map[string]any{"rows": rows})
	assert.ErrorIs(t, err, msgtemplate.ErrOutputTooLarge)
}

func FuzzParseAndRender(f *testing.F) {
	for _, seed := range []string{
		"Hello {{.name}}", "{{range .x}}{{.}}{{end}}", "{{if .a}}{{.b}}{{else}}{{.c}}{{end}}",
		"{{", "}}", "{{define \"a\"}}{{end}}", "{{upper .a}}", "{{.a.b.c}}", "{{$x := .a}}{{$x}}",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, body string) {
		tpl, err := msgtemplate.Parse("", body)
		if err != nil {
			return
		}
		vars := make(map[string]any, len(tpl.Variables))
		for _, v := range tpl.Variables {
			vars[v] = "value"
		}
		// Must not panic; an execution error (e.g. ranging over a string) is fine.
		_, out, err := tpl.Render(vars)
		if err == nil && len(out) > msgtemplate.MaxOutputBytes {
			t.Fatalf("output of %d bytes exceeds the cap", len(out))
		}
	})
}

func TestParse_ErrorNamesThePart(t *testing.T) {
	_, err := msgtemplate.Parse("Hi {{.name", "fine")
	var pe *msgtemplate.ParseError
	require.True(t, errors.As(err, &pe))
	assert.Equal(t, "subject", pe.Part)

	_, err = msgtemplate.Parse("fine", "{{printf \"x\"}}")
	require.True(t, errors.As(err, &pe))
	assert.Equal(t, "body", pe.Part)
}

func TestParse_RangeOverSuppliedDataIsAllowed(t *testing.T) {
	for _, body := range []string{
		`{{range .items}}{{.}}{{end}}`,
		`{{range $i, $v := .items}}{{$i}}={{$v}} {{end}}`,
		`{{range $.items}}{{.}}{{end}}`,
		`{{range .order.lines}}{{.}}{{end}}`,
		`{{with .order}}{{range .lines}}{{.}}{{end}}{{end}}`,
	} {
		_, err := msgtemplate.Parse("", body)
		assert.NoError(t, err, body)
	}
}
