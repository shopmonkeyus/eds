package util

import (
	"encoding/json"
	"testing"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/stretchr/testify/assert"
)

func TestCombineRecordsUpdate(t *testing.T) {
	records := []*Record{}
	records = append(records, &Record{Table: "user", Id: "0", Operation: "UPDATE", Diff: []string{"name"}, Object: map[string]interface{}{"name": "Sally"}})
	records = append(records, &Record{Table: "user", Id: "1", Operation: "UPDATE", Diff: []string{"orders"}, Object: map[string]interface{}{"orders": 10}})
	records = append(records, &Record{Table: "user", Id: "0", Operation: "UPDATE", Diff: []string{"age"}, Object: map[string]interface{}{"age": 34}})
	records = append(records, &Record{Table: "user", Id: "1", Operation: "UPDATE", Diff: []string{"favoriteColor"}, Object: map[string]interface{}{"favoriteColor": "blue"}})
	records = append(records, &Record{Table: "user", Id: "0", Operation: "UPDATE", Diff: []string{"age"}, Object: map[string]interface{}{"age": 33}})
	records = CombineRecordsWithSamePrimaryKey(records)

	assert.Equal(t, 2, len(records))

	// CombineRecordsWithSamePrimaryKey doesn't guarantee the order of the records, so we need to find the records by Id.
	var recordSally, recordBob *Record
	for _, record := range records {
		switch record.Id {
		case "0":
			recordSally = record
		case "1":
			recordBob = record
		}
	}

	assert.NotNil(t, recordSally, "record with Id '0' should exist")
	assert.NotNil(t, recordBob, "record with Id '1' should exist")

	assert.Equal(t, "UPDATE", recordSally.Operation)
	assert.Equal(t, "Sally", recordSally.Object["name"])
	assert.Equal(t, 33, recordSally.Object["age"])
	assert.Equal(t, []string{"name", "age"}, recordSally.Diff)

	assert.Equal(t, []string{"orders", "favoriteColor"}, recordBob.Diff)
	assert.Equal(t, 10, recordBob.Object["orders"])
	assert.Equal(t, "blue", recordBob.Object["favoriteColor"])
}

func TestCombineRecordsDontCombineInsert(t *testing.T) {
	records := []*Record{}
	records = append(records, &Record{Table: "user", Id: "0", Operation: "INSERT", Diff: []string{"name"}, Object: map[string]interface{}{"name": "John"}, Event: nil})
	records = append(records, &Record{Table: "user", Id: "0", Operation: "UPDATE", Diff: []string{"name"}, Object: map[string]interface{}{"name": "Bob"}, Event: nil})
	records = CombineRecordsWithSamePrimaryKey(records)
	assert.Equal(t, 2, len(records))
	assert.Equal(t, "INSERT", records[0].Operation)
}

func TestCombineRecordsDelete(t *testing.T) {
	records := []*Record{}
	records = append(records, &Record{Table: "user", Id: "0", Operation: "INSERT", Diff: []string{}, Object: map[string]interface{}{"name": "John"}, Event: nil})
	records = append(records, &Record{Table: "user", Id: "0", Operation: "UPDATE", Diff: []string{"name"}, Object: map[string]interface{}{"name": "Tim"}, Event: nil})
	records = append(records, &Record{Table: "user", Id: "0", Operation: "DELETE", Diff: []string{}, Object: map[string]interface{}{"name": "Tim"}, Event: nil})
	records = CombineRecordsWithSamePrimaryKey(records)
	assert.Equal(t, 1, len(records))
}

func TestCombineRecordsDeleteEdgeCase(t *testing.T) {
	records := []*Record{}
	records = append(records, &Record{Table: "user", Id: "u0", Operation: "INSERT", Diff: []string{}, Object: map[string]interface{}{"name": "Lucy", "age": 35}, Event: nil})
	records = append(records, &Record{Table: "user", Id: "u1", Operation: "INSERT", Diff: []string{}, Object: map[string]interface{}{"name": "Sally", "age": 50}, Event: nil})
	records = append(records, &Record{Table: "vehicle", Id: "v0", Operation: "INSERT", Diff: []string{}, Object: map[string]interface{}{"type": "truck", "mileage": 99999}, Event: nil})
	records = append(records, &Record{Table: "user", Id: "u2", Operation: "UPDATE", Diff: []string{"age"}, Object: map[string]interface{}{"age": 7}, Event: &internal.DBChangeEvent{After: json.RawMessage(`{"id":"u2","age":7}`)}})
	records = append(records, &Record{Table: "user", Id: "u0", Operation: "DELETE", Diff: []string{}, Object: map[string]interface{}{"id": "u0"}, Event: nil})
	records = append(records, &Record{Table: "user", Id: "u1", Operation: "UPDATE", Diff: []string{"age"}, Object: map[string]interface{}{"age": 60}, Event: &internal.DBChangeEvent{After: json.RawMessage(`{"id":"u1","name":"Sally","age":60}`)}})
	records = append(records, &Record{Table: "vehicle", Id: "v0", Operation: "UPDATE", Diff: []string{"age"}, Object: map[string]interface{}{"mileage": 11111}, Event: &internal.DBChangeEvent{After: json.RawMessage(`{"id":"v0","mileage":11111}`)}})
	records = CombineRecordsWithSamePrimaryKey(records)

	for _, record := range records {
		_, hasMileage := record.Object["mileage"]
		_, hasName := record.Object["name"]
		dataIsBad := (record.Table == "user" && hasMileage) || (record.Table == "vehicle" && hasName)
		assert.False(t, dataIsBad, "record object contains incorrect data. failure when combining records in batch")
	}
}

func TestSortRecordsByMVCCTimestamp(t *testing.T) {
	records := []*Record{}
	records = append(records, &Record{Table: "user", Id: "A", Operation: "INSERT", Diff: []string{}, Object: map[string]interface{}{"name": "John"}, Event: &internal.DBChangeEvent{MVCCTimestamp: "1.0"}})
	records = append(records, &Record{Table: "user", Id: "B", Operation: "INSERT", Diff: []string{}, Object: map[string]interface{}{"name": "Sally"}, Event: &internal.DBChangeEvent{MVCCTimestamp: "3.0"}})
	records = append(records, &Record{Table: "user", Id: "C", Operation: "INSERT", Diff: []string{}, Object: map[string]interface{}{"name": "Tim"}, Event: &internal.DBChangeEvent{MVCCTimestamp: "2.0"}})
	records = SortRecordsByMVCCTimestamp(records)
	assert.Equal(t, "John", records[0].Object["name"])
	assert.Equal(t, "Tim", records[1].Object["name"])
	assert.Equal(t, "Sally", records[2].Object["name"])
}
