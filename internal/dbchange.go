package internal

import (
	"encoding/json"

	"github.com/nats-io/nats.go/jetstream"
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
	UserID        *string         `json:"userId,omitempty"`
	Before        json.RawMessage `json:"before,omitempty"`
	After         json.RawMessage `json:"after,omitempty"`
	Diff          []string        `json:"diff,omitempty"`
	Timestamp     int64           `json:"timestamp"`
	MVCCTimestamp string          `json:"mvccTimestamp"`

	Imported bool          `json:"imported,omitempty"` // NOTE: this is not on the real dbchange but added during import
	NatsMsg  jetstream.Msg `json:"-"`                  // could be nil

	object map[string]any

	SchemaValidatedPath *string `json:"-"` // set by the schema validator if valid and the path is returned as non-empty and not nil
}

func (c *DBChangeEvent) String() string {
	return "DBChangeEvent[op=" + c.Operation + ",table=" + c.Table + ",id=" + c.ID + ",pk=" + c.GetPrimaryKey() + "]"
}

func (c *DBChangeEvent) GetPrimaryKey() string {
	if len(c.Key) >= 1 {
		return c.Key[len(c.Key)-1]
	}
	o, err := c.GetObject()
	if err == nil {
		if id, ok := o["id"].(string); ok {
			return id
		}
	}
	return ""
}

func (c *DBChangeEvent) GetObject() (map[string]any, error) {
	if c.After != nil {
		if c.object == nil {
			res := make(map[string]any)
			if err := json.Unmarshal(c.After, &res); err != nil {
				return nil, err
			}
			c.object = res
		}
		return c.object, nil
	} else if c.Before != nil {
		if c.object == nil {
			res := make(map[string]any)
			if err := json.Unmarshal(c.Before, &res); err != nil {
				return nil, err
			}
			c.object = res
		}
		return c.object, nil
	}
	return nil, nil
}
