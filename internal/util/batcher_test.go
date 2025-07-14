package util

import (
	"encoding/json"
	"testing"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/stretchr/testify/assert"
)

func TestBatcher(t *testing.T) {
	b := NewBatcher()
	b.Add("user", "1", "INSERT", []string{"name"}, map[string]interface{}{"id": "1", "name": "John", "age": 19, "salary": 9, "city": "New York"}, nil)
	assert.NotEmpty(t, b.Records())
	insertRecord := `{"table":"user","id":"1","operation":"INSERT","diff":["name"],"object":{"age":19,"city":"New York","id":"1","name":"John","salary":9}}`
	assert.Equal(t, "["+insertRecord+"]", JSONStringify(b.Records()))
	b.Add("user", "2", "UPDATE", []string{"name", "age"}, map[string]interface{}{"id": "1", "age": 21, "name": "Foo"}, nil)
	updateRecord := `{"table":"user","id":"1","operation":"UPDATE","diff":["name","age"],"object":{"age":21,"id":"1","name":"Foo"}}`
	assert.NotEmpty(t, b.Records())
	assert.Equal(t, "["+insertRecord+","+updateRecord+"]", JSONStringify(b.Records())) // insert should not be combined with update
	b.Add("user", "3", "UPDATE", []string{"salary"}, map[string]interface{}{"id": "1", "salary": 10}, nil)
	assert.NotEmpty(t, b.Records())
	mergedUpdateRecord := `{"table":"user","id":"1","operation":"UPDATE","diff":["name","age","salary"],"object":{"age":21,"id":"1","name":"Foo","salary":10}}`
	assert.Equal(t, "["+insertRecord+","+mergedUpdateRecord+"]", JSONStringify(b.Records())) // update is merged and contains data from both updates
	b.Add("user", "4", "DELETE", nil, map[string]interface{}{"id": "1"}, nil)
	deleteRecord := `{"table":"user","id":"1","operation":"DELETE","diff":null,"object":null}`
	assert.Equal(t, "["+insertRecord+","+deleteRecord+"]", JSONStringify(b.Records())) // In case of delete, the previous insert record is removed
	assert.NotEmpty(t, b.Records())
}

func TestBatcherAddDontCombineInsert(t *testing.T) {
	b := NewBatcher()
	b.Add("user", "0", "INSERT", []string{"name"}, map[string]interface{}{"name": "John"}, nil)
	b.Add("user", "0", "UPDATE", []string{"name"}, map[string]interface{}{"name": "Bob"}, nil)
	assert.Equal(t, 2, b.Len())
	records := b.Records()
	assert.Equal(t, "INSERT", records[0].Operation)
}

func TestBatcherAddCombineUpdate(t *testing.T) {
	b := NewBatcher()
	b.Add("user", "0", "INSERT", []string{}, map[string]interface{}{"name": "Lucy", "age": 35}, nil)
	b.Add("user", "0", "UPDATE", []string{"name"}, map[string]interface{}{"name": "Sally"}, &internal.DBChangeEvent{After: json.RawMessage(`{"name":"Sally","age":35}`)})
	b.Add("user", "0", "UPDATE", []string{"age"}, map[string]interface{}{"age": 34}, &internal.DBChangeEvent{After: json.RawMessage(`{"name":"Sally","age":34}`)})
	assert.Equal(t, 2, b.Len())
	records := b.Records()
	assert.Equal(t, "UPDATE", records[1].Operation)
	assert.Equal(t, "Sally", records[1].Object["name"])
	assert.Equal(t, 34, records[1].Object["age"])
	assert.Equal(t, []string{"name", "age"}, records[1].Diff)
}
