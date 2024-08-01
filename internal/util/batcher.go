package util

import (
	"github.com/shopmonkeyus/eds-server/internal"
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
func (b *Batcher) Add(table string, id string, operation string, diff []string, payload map[string]any, event *internal.DBChangeEvent) {
	var primaryKey string
	if val, ok := payload["id"].(string); ok {
		primaryKey = val
	} else {
		primaryKey = id
	}
	hashkey := table + id
	index, found := b.pks[hashkey]
	if operation == "DELETE" {
		if found {
			// if found, remove the old record (could be insert or update)
			b.records = append(b.records[:index], b.records[index+1:]...)
			delete(b.pks, hashkey)
		}
		b.records = append(b.records, &Record{Table: table, Id: primaryKey, Operation: operation, Event: event})
		b.pks[hashkey] = uint(len(b.records) - 1)
	} else {
		if found {
			entry := b.records[index]
			// merge the diff keys
			for _, key := range diff {
				if !SliceContains(entry.Diff, key) {
					entry.Diff = append(entry.Diff, key)
				}
			}
			if len(entry.Diff) == 0 {
				entry.Diff = diff
			}
			entry.Event = event
			entry.Operation = operation
			entry.Object = payload // replace the payload with the new update
		} else {
			// not found just add it
			b.records = append(b.records, &Record{Table: table, Id: primaryKey, Operation: operation, Diff: diff, Object: payload, Event: event})
			b.pks[hashkey] = uint(len(b.records) - 1)
		}
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
