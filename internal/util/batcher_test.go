package util

import (
	"encoding/json"
	"testing"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/stretchr/testify/assert"
)

func TestBatcher(t *testing.T) {
	b := NewBatcher()
	insertEvent := &internal.DBChangeEvent{
		Table:     "user",
		Key:       []string{"gcp-us-west1", "1"},
		Operation: "INSERT",
		Diff:      []string{},
		Before:    nil,
		After:     json.RawMessage(JSONStringify(map[string]interface{}{"id": "1", "name": "John", "age": 19, "salary": 9, "city": "New York"})),
	}
	b.Add(insertEvent)
	assert.NotEmpty(t, b.Records())
	assert.Equal(t, "John", b.Records()[0].Object["name"])

	updateEvent := &internal.DBChangeEvent{
		Table:     "user",
		Key:       []string{"gcp-us-west1", "2"},
		Operation: "UPDATE",
		Diff:      []string{"name", "age"},
		Before:    json.RawMessage(JSONStringify(map[string]interface{}{"id": "2", "age": 21, "name": "Foo"})),
		After:     json.RawMessage(JSONStringify(map[string]interface{}{"id": "2", "age": 22, "name": "Foo"})),
	}
	b.Add(updateEvent)
	assert.Equal(t, float64(22), b.Records()[1].Object["age"])

	deleteEvent := &internal.DBChangeEvent{
		Table:     "user",
		Key:       []string{"gcp-us-west1", "3"},
		Operation: "DELETE",
		Diff:      nil,
		Before:    json.RawMessage(JSONStringify(map[string]interface{}{"id": "3", "age": 56, "name": "Jim"})),
	}
	b.Add(deleteEvent)
	assert.Equal(t, "3", b.Records()[2].Id)
}
