package util

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/shopmonkeyus/eds/internal"
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
		Before:        json.RawMessage(JSONStringify(map[string]any{"completed": false, "orderId": "1", "serviceId": "2", "id": "xxx123", "startDate": "2021-01-01T00:00:00Z"})),
		After:         json.RawMessage(JSONStringify(map[string]any{"completed": true, "orderId": "1", "serviceId": "2", "id": "xxx123", "startDate": "2021-01-01T00:00:00Z"})),
		Diff:          []string{"completed"},
		MVCCTimestamp: "123456789",
	}
	found, valid, path, err := validator.Validate(event)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.True(t, valid)
	assert.Equal(t, "labor/received/labor_123456789_999.json", path)
}

func TestLoadSchemaDateInvalid(t *testing.T) {
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
		Before:        json.RawMessage(JSONStringify(map[string]any{"completed": false, "orderId": "1", "serviceId": "2", "id": "xxx123", "startDate": "2021-01-01T00:00:00Z"})),
		After:         json.RawMessage(JSONStringify(map[string]any{"completed": true, "orderId": "1", "serviceId": "2", "id": "xxx123", "startDate": "x2021-01-01T00:00:00Z"})),
		Diff:          []string{"completed"},
		MVCCTimestamp: "123456789",
	}
	found, valid, path, err := validator.Validate(event)
	assert.Error(t, err)
	m, err := regexp.MatchString(`jsonschema: '\/after\/startDate' does not validate with file:\/\/(.*)?\/testdata\/labor_message\.json#\/anyOf\/0\/properties\/after\/allOf\/0\/\$ref\/properties\/startDate\/format: 'x2021-01-01T00:00:00Z' is not valid 'date-time'`, err.Error())
	assert.NoError(t, err)
	assert.True(t, m)
	assert.True(t, found)
	assert.False(t, valid)
	assert.Empty(t, path)
}
