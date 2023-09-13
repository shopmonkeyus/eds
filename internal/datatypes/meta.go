package datatypes

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
)

type Meta struct {
	UserID       *string `json:"userId,omitempty"`
	SessionID    *string `json:"sessionId,omitempty"`
	Version      *int64  `json:"version,omitempty"`
	ModelVersion *string `json:"modelVersion,omitempty"`
}

// Value return Meta value, implement driver.Valuer interface
func (m *Meta) Value() (driver.Value, error) {
	if m == nil {
		return "{}", nil
	}
	buf, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return string(buf), nil
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (m *Meta) Scan(value interface{}) error {
	if value == nil {
		*m = Meta{}
		return nil
	}
	var bytes []byte
	switch v := value.(type) {
	case []byte:
		if len(v) > 0 {
			bytes = make([]byte, len(v))
			copy(bytes, v)
		}
	case string:
		bytes = []byte(v)
	default:
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	return json.Unmarshal(bytes, m)
}
