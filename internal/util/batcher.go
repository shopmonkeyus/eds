package util

import (
	"maps"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/go-common/logger"
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
func (b *Batcher) Add(logger logger.Logger, table string, id string, operation string, diff []string, payload map[string]any, event *internal.DBChangeEvent) {
	var primaryKey string
	if event != nil {
		primaryKey = event.GetPrimaryKey()
	} else {
		if val, ok := payload["id"].(string); ok {
			primaryKey = val
		} else {
			primaryKey = id
		}
	}
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
		if previousRecord.Id != payload["id"].(string) {
			logger.Error("id mismatch! record with id %s and payload id %s", previousRecord.Id, payload["id"].(string))
			logger.Error("current event key: %v. Previous event key: %v", event.Key, previousRecord.Event.Key)
			logger.Error("current table from event: %s. Previous table from event: %s", event.Table, previousRecord.Event.Table)
			logger.Error("current table from record: %s. Previous table from record: %s", table, previousRecord.Table)
			logger.Error("current hashkey: %s", hashkey)
			logger.Error("hashkey array: %v", b.pks)
			logger.Error("previous record payload: %s", JSONStringify(previousRecord.Object))
			logger.Error("new payload: %s", JSONStringify(payload))
			logger.Error("previous event raw: %s", JSONStringify(previousRecord.Event))
			logger.Error("new event raw: %s", JSONStringify(event))
		}
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
		logger.Debug("combined update with id %s into record with id %s", primaryKey, previousRecord.Id)
	case deleteWithBatch:
		b.records = append(b.records[:index], b.records[index+1:]...)
		delete(b.pks, hashkey)
		clear(b.pks)
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
