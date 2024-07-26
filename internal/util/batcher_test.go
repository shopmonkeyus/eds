package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatcher(t *testing.T) {
	b := NewBatcher()
	b.Add("user", "1", "INSERT", []string{"name"}, map[string]interface{}{"id": "1", "name": "John"}, nil)
	assert.NotEmpty(t, b.Records())
	assert.Equal(t, `[{"table":"user","id":"1","operation":"INSERT","diff":["name"],"object":{"id":"1","name":"John"}}]`, JSONStringify(b.Records()))
	b.Add("user", "1", "UPDATE", []string{"name", "age"}, map[string]interface{}{"id": "1", "age": 21, "name": "Foo"}, nil)
	assert.NotEmpty(t, b.Records())
	assert.Equal(t, `[{"table":"user","id":"1","operation":"UPDATE","diff":["name","age"],"object":{"age":21,"id":"1","name":"Foo"}}]`, JSONStringify(b.Records()))
	b.Add("user", "1", "UPDATE", []string{"salary"}, map[string]interface{}{"id": "1", "salary": 10, "age": 21, "name": "Foo"}, nil)
	assert.NotEmpty(t, b.Records())
	assert.Equal(t, `[{"table":"user","id":"1","operation":"UPDATE","diff":["name","age","salary"],"object":{"age":21,"id":"1","name":"Foo","salary":10}}]`, JSONStringify(b.Records()))
	b.Add("user", "1", "DELETE", nil, nil, nil)
	assert.Equal(t, `[{"table":"user","id":"1","operation":"DELETE","diff":null,"object":null}]`, JSONStringify(b.Records()))
	assert.NotEmpty(t, b.Records())
}
