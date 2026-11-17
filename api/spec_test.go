package apispec_test

import (
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apispec "github.com/tariktek/eventDrivenNotificationSystem/api"
)

func load(t *testing.T) *openapi3.T {
	t.Helper()
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(apispec.YAML)
	require.NoError(t, err)
	return doc
}

// TestSpec_IsValid checks the document against the OpenAPI 3 schema,
// including that every example matches the schema it illustrates.
func TestSpec_IsValid(t *testing.T) {
	doc := load(t)
	require.NoError(t, doc.Validate(t.Context(), openapi3.EnableExamplesValidation()))
}

// TestSpec_Conventions enforces API-wide rules that the OpenAPI schema itself
// cannot express.
func TestSpec_Conventions(t *testing.T) {
	doc := load(t)
	seenIDs := map[string]bool{}

	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			name := method + " " + path
			t.Run(name, func(t *testing.T) {
				require.NotEmpty(t, op.OperationID, "every operation needs an operationId (it names the Go method)")
				assert.False(t, seenIDs[op.OperationID], "duplicate operationId %s", op.OperationID)
				seenIDs[op.OperationID] = true
				assert.NotEmpty(t, op.Summary)
				assert.NotEmpty(t, op.Tags)

				if !strings.HasPrefix(path, "/api/v1/") {
					return // ops endpoints: no auth, no error envelope
				}
				assert.Nil(t, op.Security, "API endpoints inherit the global API key requirement")
				for _, code := range []string{"401", "500"} {
					assert.NotNil(t, op.Responses.Value(code), "must document %s", code)
				}
				if op.RequestBody != nil {
					assert.NotNil(t, op.Responses.Value("400"), "malformed or unknown-field bodies return 400")
					assert.NotNil(t, op.Responses.Value("413"), "the body size limit applies to every body")
				}
				if strings.Contains(path, "{id}") {
					assert.NotNil(t, op.Responses.Value("400"), "a non-UUID id returns 400")
					assert.NotNil(t, op.Responses.Value("404"), "operations on a resource must document 404")
				}
				if method == http.MethodGet && op.Parameters.GetByInAndName("query", "cursor") != nil {
					assert.NotNil(t, op.Responses.Value("422"), "an invalid cursor or filter returns 422")
				}
			})
		}
	}
}

// TestSpec_EnumsMatchDomain guards against the API and the domain drifting
// apart: a status added in one place but not the other fails here.
func TestSpec_EnumsMatchDomain(t *testing.T) {
	doc := load(t)
	enum := func(schema string) []string {
		var out []string
		for _, v := range doc.Components.Schemas[schema].Value.Enum {
			out = append(out, v.(string))
		}
		slices.Sort(out)
		return out
	}

	assert.Equal(t, []string{"email", "push", "sms"}, enum("Channel"))
	assert.Equal(t, []string{"high", "low", "normal"}, enum("Priority"))
	assert.Equal(t, []string{"cancelled", "failed", "pending", "processing", "scheduled", "sent"}, enum("Status"))
}
