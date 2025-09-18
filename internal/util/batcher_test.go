package util

import (
	"encoding/json"
	"testing"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/stretchr/testify/assert"
)

func TestBatcher(t *testing.T) {
	b := NewBatcher()
	event1 := &internal.DBChangeEvent{Table: "user", ID: "1", Operation: "INSERT", Diff: []string{"name"}, After: json.RawMessage(`{"id": "1", "name": "John", "age": 19, "salary": 9, "city": "New York"}`)}
	b.Add(event1)
	assert.NotEmpty(t, b.Records())
	insertRecord := `{"table":"user","id":"1","operation":"INSERT","diff":["name"],"object":{"age":19,"city":"New York","id":"1","name":"John","salary":9}}`
	assert.Equal(t, "["+insertRecord+"]", JSONStringify(b.Records()))
	event2 := &internal.DBChangeEvent{Table: "user", ID: "1", Operation: "UPDATE", Diff: []string{"name", "age"}, After: json.RawMessage(`{"id": "1", "age": 21, "name": "Foo"}`)}
	b.Add(event2)
	updateRecord := `{"table":"user","id":"1","operation":"UPDATE","diff":["name","age"],"object":{"age":21,"id":"1","name":"Foo"}}`
	assert.NotEmpty(t, b.Records())
	assert.Equal(t, "["+insertRecord+","+updateRecord+"]", JSONStringify(b.Records())) // insert should not be combined with update
	event3 := &internal.DBChangeEvent{Table: "user", ID: "1", Operation: "UPDATE", Diff: []string{"salary"}, After: json.RawMessage(`{"id": "1", "salary": 10}`)}
	b.Add(event3)
	assert.NotEmpty(t, b.Records())
	mergedUpdateRecord := `{"table":"user","id":"1","operation":"UPDATE","diff":["name","age","salary"],"object":{"age":21,"id":"1","name":"Foo","salary":10}}`
	assert.Equal(t, "["+insertRecord+","+mergedUpdateRecord+"]", JSONStringify(b.Records())) // update is merged and contains data from both updates
	event4 := &internal.DBChangeEvent{Table: "user", ID: "1", Operation: "DELETE", After: json.RawMessage(`{"id": "1"}`)}
	b.Add(event4)
	deleteRecord := `{"table":"user","id":"1","operation":"DELETE","diff":null,"object":null}`
	assert.Equal(t, "["+insertRecord+","+deleteRecord+"]", JSONStringify(b.Records())) // In case of delete, the previous insert record is removed
	assert.NotEmpty(t, b.Records())
}

func TestBatcherAddDontCombineInsert(t *testing.T) {
	b := NewBatcher()
	b.Add(&internal.DBChangeEvent{Table: "user", ID: "0", Operation: "INSERT", Diff: []string{"name"}, After: json.RawMessage(`{"name": "John"}`)})
	b.Add(&internal.DBChangeEvent{Table: "user", ID: "0", Operation: "UPDATE", Diff: []string{"name"}, After: json.RawMessage(`{"name": "Bob"}`)})
	assert.Equal(t, 2, b.Len())
	records := b.Records()
	assert.Equal(t, "INSERT", records[0].Operation)
}

func TestBatcherAddCombineUpdate(t *testing.T) {
	b := NewBatcher()
	b.Add(&internal.DBChangeEvent{Table: "user", ID: "0", Operation: "INSERT", Diff: []string{}, After: json.RawMessage(`{"name": "Lucy", "age": 35}`)})
	b.Add(&internal.DBChangeEvent{Table: "user", ID: "0", Operation: "UPDATE", Diff: []string{"name"}, After: json.RawMessage(`{"name": "Sally", "age": 35}`)})
	b.Add(&internal.DBChangeEvent{Table: "user", ID: "0", Operation: "UPDATE", Diff: []string{"age"}, After: json.RawMessage(`{"age": 34}`)})
	assert.Equal(t, 2, b.Len())
	records := b.Records()
	assert.Equal(t, "UPDATE", records[1].Operation)
	assert.Equal(t, "Sally", records[1].Object["name"])
	assert.Equal(t, 34.0, records[1].Object["age"])
	assert.Equal(t, []string{"name", "age"}, records[1].Diff)
}
