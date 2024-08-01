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
	Before        json.RawMessage `json:"before,omitempty"`
	After         json.RawMessage `json:"after,omitempty"`
	Diff          []string        `json:"diff,omitempty"`
	Timestamp     int64           `json:"timestamp"`
	MVCCTimestamp string          `json:"mvccTimestamp"`

	NatsMsg jetstream.Msg `json:"-"` // could be nil
}

func (c *DBChangeEvent) String() string {
	return "DBChangeEvent[op=" + c.Operation + ",table=" + c.Table + ",id=" + c.ID + "]"
}

func (c *DBChangeEvent) GetPrimaryKey() string {
	key := c.Key[len(c.Key)-1]
	return key
}

func (c *DBChangeEvent) GetObject() (map[string]any, error) {
	if c.After != nil {
		res := make(map[string]any)
		if err := json.Unmarshal(c.After, &res); err != nil {
			return nil, err
		}
		return res, nil
	}
	return nil, nil
}
