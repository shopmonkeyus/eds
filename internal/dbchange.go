package internal

import (
	"encoding/json"
	"fmt"

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

// OmitProperties removes the specified properties from the object
func (c *DBChangeEvent) OmitProperties(props ...string) error {
	object, err := c.GetObject()
	if err != nil {
		return err
	}
	for _, prop := range props {
		delete(object, prop)
	}
	c.object = object
	return nil
}

func (c *DBChangeEvent) GetObject() (map[string]any, error) {
	if len(c.After) > 0 {
		if c.object == nil {
			res := make(map[string]any)
			if err := json.Unmarshal(c.After, &res); err != nil {
				return nil, err
			}
			c.object = res
		}
		return c.object, nil
	} else if len(c.Before) > 0 {
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

func DBChangeEventFromMessage(msg jetstream.Msg) (DBChangeEvent, error) {
	var evt DBChangeEvent
	buffer := msg.Data()
	md, _ := msg.Metadata()
	if err := json.Unmarshal(buffer, &evt); err != nil {
		return evt, fmt.Errorf("error unmarshalling message into DBChangeEvent: %s (seq:%d) raw message:\n%s", err, md.Sequence.Consumer, string(buffer))
	}

	if _, err := evt.GetObject(); err != nil {
		return evt, fmt.Errorf("error getting object (before/after is malformed): %s (seq:%d) raw message:\n%s", err, md.Sequence.Consumer, string(buffer))
	}

	pk := evt.GetPrimaryKey()
	if pk == "" {
		return evt, fmt.Errorf("primary key is empty: %s (seq:%d) raw message:\n%s", evt.ID, md.Sequence.Consumer, string(buffer))
	}

	evt.NatsMsg = msg // in case the driver wants to get specific information from it for logging, etc

	return evt, nil
}
