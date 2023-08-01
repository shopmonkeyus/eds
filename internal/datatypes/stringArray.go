package datatypes

import (
	"database/sql/driver"
	"encoding/json"
)

type StringArray []string

func (arr *StringArray) Scan(value interface{}) error {
	var bytes []byte
	switch v := value.(type) {
	case []byte:
		if len(v) > 0 {
			bytes = make([]byte, len(v))
			copy(bytes, v)
		}
	case string:
		bytes = []byte(v)
	}
	return json.Unmarshal(bytes, &arr)
}

var emptyStringArray = "[]"

func (arr StringArray) Value() (driver.Value, error) {
	if len(arr) == 0 {
		return emptyStringArray, nil
	}
	buf, err := json.Marshal(arr)
	if err != nil {
		return nil, err
	}
	return string(buf), nil
}

// MarshalJSON to output non base64 encoded []byte
func (arr StringArray) MarshalJSON() ([]byte, error) {
	sa := make([]string, len(arr))
	copy(sa, arr)
	return json.Marshal(sa)
}

// UnmarshalJSON to deserialize []byte
func (arr *StringArray) UnmarshalJSON(b []byte) error {
	sa := make([]string, 0)
	err := json.Unmarshal(b, &sa)
	if err != nil {
		return err
	}
	*arr = sa
	return nil
}

type NullableStringArray []string

func (arr *NullableStringArray) Scan(value interface{}) error {
	var bytes []byte
	switch v := value.(type) {
	case []byte:
		if len(v) > 0 {
			bytes = make([]byte, len(v))
			copy(bytes, v)
		}
	case string:
		bytes = []byte(v)
	}
	return json.Unmarshal(bytes, &arr)
}

func (arr NullableStringArray) Value() (driver.Value, error) {
	if len(arr) == 0 {
		return nil, nil
	}
	buf, err := json.Marshal(arr)
	if err != nil {
		return nil, err
	}
	return string(buf), nil
}
