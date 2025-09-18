package util

import (
	"maps"

	"github.com/shopmonkeyus/eds/internal"
)

type Batcher struct {
	records []*Record
	pks     map[string]uint
}

type Record struct {
	Table     string                  `json:"table"`
	Id        string                  `json:"id"`
	Operation string                  `json:"operation"`
	Diff      []string                `json:"diff"`
	Object    map[string]any          `json:"object"`
	Event     *internal.DBChangeEvent `json:"-"`
}

func (r *Record) String() string {
	return JSONStringify(r)
}

// Records returns the array of records.
func (b *Batcher) Records() []*Record {
	return b.records
}

// Add will add a record to the batcher.
func (b *Batcher) Add(event *internal.DBChangeEvent) {
	table := event.Table
	primaryKey := event.GetPrimaryKey()
	operation := event.Operation
	diff := event.Diff
	payload, _ := event.GetObject()

	hashkey := table + primaryKey
	index, previousEventFound := b.pks[hashkey]

	appendRecord := func() {
		b.records = append(b.records, &Record{Table: table, Id: primaryKey, Operation: operation, Diff: diff, Object: payload, Event: event})
		b.pks[hashkey] = uint(len(b.records) - 1)
	}

	appendDeleteRecord := func() {
		b.records = append(b.records, &Record{Table: table, Id: primaryKey, Operation: operation, Event: event})
		b.pks[hashkey] = uint(len(b.records) - 1)
	}

	type batchCondition int
	const (
		withoutBatch batchCondition = iota
		updateWithBatch
		deleteWithBatch
		deleteWithoutBatch
	)

	var previousRecord *Record
	if previousEventFound {
		previousRecord = b.records[index]
	}

	batchType := withoutBatch
	switch operation {
	case "DELETE":
		if previousEventFound {
			batchType = deleteWithBatch
		} else {
			batchType = deleteWithoutBatch
		}
	case "UPDATE":
		if previousEventFound {
			switch previousRecord.Operation {
			case "INSERT":
				batchType = withoutBatch
			case "UPDATE":
				batchType = updateWithBatch
			}
		}
	}

	switch batchType {
	case withoutBatch:
		appendRecord()
	case updateWithBatch:
		for _, key := range diff {
			if !SliceContains(previousRecord.Diff, key) {
				previousRecord.Diff = append(previousRecord.Diff, key)
			}
		}
		if len(previousRecord.Diff) == 0 {
			previousRecord.Diff = diff
		}
		previousRecord.Event = event
		previousRecord.Operation = operation
		maps.Copy(previousRecord.Object, payload) // upsert the payload with the new update
	case deleteWithBatch:
		b.records = append(b.records[:index], b.records[index+1:]...)
		delete(b.pks, hashkey)
		appendDeleteRecord()
	case deleteWithoutBatch:
		appendDeleteRecord()
	}
}

// Clear will clear the batcher and reset the internal state.
func (b *Batcher) Clear() {
	b.records = nil
	b.pks = make(map[string]uint)
}

// Len will return the number of records pending in the batcher.
func (b *Batcher) Len() int {
	return len(b.records)
}

// NewBatcher creates a new batcher instance.
func NewBatcher() *Batcher {
	return &Batcher{
		pks: make(map[string]uint),
	}
}
