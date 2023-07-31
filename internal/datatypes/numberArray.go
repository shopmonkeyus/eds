package datatypes

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	// "gorm.io/gorm"
	// "gorm.io/gorm/schema"
)

type NumberArray []float64

func (arr *NumberArray) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSON value:", value))
	}
	err := json.Unmarshal(bytes, &arr)
	return err
}

var emptyNumberArray = "[]"

func (arr NumberArray) Value() (driver.Value, error) {
	if len(arr) == 0 {
		return emptyNumberArray, nil
	}
	return json.Marshal(arr)
}

type NullableNumberArray []float64

func (arr *NullableNumberArray) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSON value:", value))
	}
	err := json.Unmarshal(bytes, &arr)
	return err
}

func (arr NullableNumberArray) Value() (driver.Value, error) {
	if len(arr) == 0 {
		return nil, nil
	}
	return json.Marshal(arr)
}

// // GormDBDataType gorm db data type
// func (NumberArray) GormDBDataType(db *gorm.DB, field *schema.Field) string {
// 	return getJSONDataType(db)
// }

// // GormDBDataType gorm db data type
// func (NullableNumberArray) GormDBDataType(db *gorm.DB, field *schema.Field) string {
// 	return getJSONDataType(db)
// }
