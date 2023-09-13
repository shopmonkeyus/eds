package datatypes

import (
	"database/sql/driver"
	"fmt"
	"time"
)

type DateTime time.Time

func dequote(s string) string {
	if s[0:1] == `"` {
		return s[1 : len(s)-1]
	}
	return s
}

func (t *DateTime) UnmarshalJSON(b []byte) error {
	str := dequote(string(b))
	tv, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return err
	}
	*t = DateTime(tv)
	return nil
}

func (t *DateTime) MarshalJSON() ([]byte, error) {
	tTime := time.Time(*t)
	return []byte(`"` + tTime.Format(time.RFC3339) + `"`), nil
}

func (t DateTime) Value() (driver.Value, error) {
	tTime := time.Time(t)
	return tTime, nil
}

func (t *DateTime) Scan(v interface{}) error {
	if value, ok := v.(string); ok {
		v, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return err
		}
		*t = DateTime(v)
		return nil
	}
	if value, ok := v.(time.Time); ok {
		*t = DateTime(value)
		return nil
	}
	return fmt.Errorf("cannot convert %v to timestamp", v)
}
