package internal

import (
	"encoding/json"
)

// DBChangeEvent represents a change record from the database.
type DBChangeEvent struct {
	Operation     string          `json:"operation"`
	ID            string          `json:"id"`
	Table         string          `json:"table"`
	Key           []string        `json:"key"`
	ModelVersion  string          `json:"modelVersion"`
	CompanyID     *string         `json:"companyId,omitempty"`
	LocationID    *string         `json:"locationId,omitempty"`
	Before        json.RawMessage `json:"before,omitempty"`
	After         json.RawMessage `json:"after,omitempty"`
	Diff          []string        `json:"diff,omitempty"`
	Timestamp     int64           `json:"timestamp"`
	MVCCTimestamp string          `json:"mvccTimestamp"`
}

func (c *DBChangeEvent) String() string {
	return "DBChangeEvent[op=" + c.Operation + ",table=" + c.Table + ",id=" + c.ID + "]"
}
