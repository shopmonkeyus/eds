package datatypes

import "encoding/json"

type ChangeEventOperation string

const (
	ChangeEventInsert ChangeEventOperation = "INSERT"
	ChangeEventUpdate ChangeEventOperation = "UPDATE"
	ChangeEventDelete ChangeEventOperation = "DELETE"
)

type ChangeEventPayload interface {
	// GetID returns the event payload id
	GetID() string
	// GetTimestamp returns the timestamp in milliseconds when the event occurred
	GetTimestamp() int64
	// GetMvccTimestamp returns the mvcc timestamp in microseconds when the change occurred
	GetMvccTimestamp() string
	// GetTable returns the table name
	GetTable() string
	// GetKey returns an array of primary keys
	GetKey() []string
	// GetLocationID returns the location id or nil if not provided or relevant for this table
	GetLocationID() *string
	// GetCompanyID returns the company id or nil if not provided or relevant for this table
	GetCompanyID() *string
	// GetUserID returns the user id that made the change or nil if not provided or relevant for this table
	GetUserID() *string
	// GetVersion returns a monotonically increasing version number for the change version to this record
	GetVersion() int64
	// GetRegion returns the region where the change was processed
	GetRegion() string
	// GetOperation returns the ChangeEventOperation
	GetOperation() ChangeEventOperation
	// GetBefore returns the record as a ChangeEvent[T] before this change or nil if not provided
	GetBefore() any
	// GetAfter returns the record as a ChangeEvent[T] after this change or nil if not provided
	GetAfter() any
	// GetDiff returns an array of string keys of the properties that changed
	GetDiff() []string
}

type ChangeEvent[T any] struct {
	ID            string               `json:"id"`
	Timestamp     int64                `json:"timestamp"`
	MvccTimestamp string               `json:"mvccTimestamp"`
	Table         string               `json:"table"`
	Key           []string             `json:"key"`
	LocationID    *string              `json:"locationId,omitempty"`
	CompanyID     *string              `json:"companyId,omitempty"`
	UserID        *string              `json:"userId,omitempty"`
	SessionID     *string              `json:"sessionId,omitempty"`
	Version       int64                `json:"version"`
	Region        string               `json:"region"`
	Operation     ChangeEventOperation `json:"operation"`
	Before        *T                   `json:"before,omitempty"`
	After         *T                   `json:"after,omitempty"`
	Diff          []string             `json:"diff,omitempty"`
}

var _ ChangeEventPayload = (*ChangeEvent[any])(nil)

// String returns a JSON stringified version of the ChangeEvent
func (c ChangeEvent[T]) String() string {
	buf, err := json.Marshal(c)
	if err != nil {
		return err.Error()
	}
	return string(buf)
}

func (c ChangeEvent[T]) GetID() string {
	return c.ID
}

func (c ChangeEvent[T]) GetTimestamp() int64 {
	return c.Timestamp
}

func (c ChangeEvent[T]) GetMvccTimestamp() string {
	return c.MvccTimestamp
}

func (c ChangeEvent[T]) GetTable() string {
	return c.Table
}

func (c ChangeEvent[T]) GetKey() []string {
	return c.Key
}

func (c ChangeEvent[T]) GetLocationID() *string {
	return c.LocationID
}

func (c ChangeEvent[T]) GetCompanyID() *string {
	return c.CompanyID
}

func (c ChangeEvent[T]) GetUserID() *string {
	return c.UserID
}

func (c ChangeEvent[T]) GetVersion() int64 {
	return c.Version
}

func (c ChangeEvent[T]) GetRegion() string {
	return c.Region
}

func (c ChangeEvent[T]) GetOperation() ChangeEventOperation {
	return c.Operation
}

func (c ChangeEvent[T]) GetBefore() any {
	return c.Before
}

func (c ChangeEvent[T]) GetAfter() any {
	return c.After
}

func (c ChangeEvent[T]) GetDiff() []string {
	return c.Diff
}
