package handler

import (
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apispec "github.com/tariktek/eventDrivenNotificationSystem/api"
)

// TestStrictJSON_CoversEveryRequestBody fails when an operation with a JSON
// body is added to the spec without a strict decoder: its unknown fields
// would be silently dropped.
func TestStrictJSON_CoversEveryRequestBody(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(apispec.YAML)
	require.NoError(t, err)

	withBody := map[string]bool{}
	for _, item := range doc.Paths.Map() {
		for _, op := range item.Operations() {
			if op.RequestBody != nil {
				withBody[strings.ToUpper(op.OperationID[:1])+op.OperationID[1:]] = true
			}
		}
	}
	for id := range withBody {
		assert.Contains(t, requestBodies, id, "operation %s has a request body but no strict decoder", id)
	}
	for id := range requestBodies {
		assert.True(t, withBody[id], "requestBodies lists %s, which has no request body in the spec", id)
	}
}
