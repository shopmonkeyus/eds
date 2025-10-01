package util

import (
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
	object, _ := event.GetObject() // error handled in consumer
	b.records = append(
		b.records,
		&Record{
			Table:     event.Table,
			Id:        event.GetPrimaryKey(),
			Operation: event.Operation,
			Diff:      event.Diff,
			Object:    object,
			Event:     event,
		},
	)
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
