package util

import (
	"encoding/json"
	"testing"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/stretchr/testify/assert"
)

func strPtr(val string) *string {
	return &val
}

func TestLoadSchema(t *testing.T) {
	validator, err := NewSchemaValidator("./testdata")
	assert.NoError(t, err)
	assert.NotNil(t, validator)
	event := internal.DBChangeEvent{
		Operation:     "UPDATE",
		ID:            "999",
		LocationID:    strPtr("123"),
		CompanyID:     strPtr("456"),
		UserID:        strPtr("789"),
		Table:         "labor",
		Before:        json.RawMessage(JSONStringify(map[string]any{"completed": false, "orderId": "1", "serviceId": "2", "id": "xxx123"})),
		After:         json.RawMessage(JSONStringify(map[string]any{"completed": true, "orderId": "1", "serviceId": "2", "id": "xxx123"})),
		Diff:          []string{"completed"},
		MVCCTimestamp: "123456789",
	}
	found, valid, path, err := validator.Validate(event)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.True(t, valid)
	assert.Equal(t, "labor/received/labor_123456789_999.json", path)
}
