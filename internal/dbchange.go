package internal

import "encoding/json"

// DBChangeEvent represents a change record from the database.
type DBChangeEvent struct {
	Operation    string          `json:"operation"`
	ID           string          `json:"id"`
	Table        string          `json:"table"`
	Key          []string        `json:"key"`
	ModelVersion string          `json:"modelVersion"`
	CompanyID    *string         `json:"companyId,omitempty"`
	LocationID   *string         `json:"locationId,omitempty"`
	Before       json.RawMessage `json:"before,omitempty"`
	After        json.RawMessage `json:"after,omitempty"`
	Diff         []string        `json:"diff,omitempty"`
}

func (c *DBChangeEvent) String() string {
	return "DBChangeEvent[" + c.Operation + " " + c.Table + " " + c.ID + "]"
}
