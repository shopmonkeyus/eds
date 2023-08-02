package datatypes

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
)

// JSON defined JSON data type, need to implements driver.Valuer, sql.Scanner interface
type JSON json.RawMessage

// NewJSONObject returns an empty JSON object
func NewJSONObject() (JSON, error) {
	return []byte(`{}`), nil
}

// NewJSONArray returns an empty JSON array
func NewJSONArray() (JSON, error) {
	return []byte(`[]`), nil
}

// NewJSON returns a JSON structure from a map of key/value pairs
func NewJSON(kv map[string]interface{}) (JSON, error) {
	buf, err := json.Marshal(kv)
	return buf, err
}

// Get will return the key value from the JSON object
func (j *JSON) Get(key string) (interface{}, error) {
	var kv map[string]interface{}
	if err := json.Unmarshal(*j, &kv); err != nil {
		return nil, err
	}
	return kv[key], nil
}

// Set will set the key and value in the JSON object
func (j *JSON) Set(key string, val interface{}) error {
	var kv map[string]interface{}
	if err := json.Unmarshal(*j, &kv); err != nil {
		return err
	}
	kv[key] = val
	newj, err := json.Marshal(kv)
	if err != nil {
		return err
	}
	*j = newj
	return nil
}

// Del will delete the key in the JSON object
func (j *JSON) Del(key string) error {
	var kv map[string]interface{}
	if err := json.Unmarshal(*j, &kv); err != nil {
		return err
	}
	delete(kv, key)
	newj, err := json.Marshal(kv)
	if err != nil {
		return err
	}
	*j = newj
	return nil
}

// Has returns true if the key exists in the JSON object
func (j *JSON) Has(key string) bool {
	var kv map[string]interface{}
	if err := json.Unmarshal(*j, &kv); err != nil {
		return false
	}
	return kv[key] != nil
}

// Value return json value, implement driver.Valuer interface
func (j JSON) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return string(j), nil
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (j *JSON) Scan(value interface{}) error {
	if value == nil {
		*j = JSON("null")
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

	result := json.RawMessage(bytes)
	*j = JSON(result)
	return nil
}

// MarshalJSON to output non base64 encoded []byte
func (j JSON) MarshalJSON() ([]byte, error) {
	return json.RawMessage(j).MarshalJSON()
}

// UnmarshalJSON to deserialize []byte
func (j *JSON) UnmarshalJSON(b []byte) error {
	result := json.RawMessage{}
	err := result.UnmarshalJSON(b)
	*j = JSON(result)
	return err
}

func (j JSON) String() string {
	return string(j)
}
