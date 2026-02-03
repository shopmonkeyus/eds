//go:build e2e
// +build e2e

package e2e

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type sqlDriverTransform interface {
	QuoteTable(table string) string
	QuoteColumn(column string) string
	QuoteValue(value string) string
}

type columnFormat func(string) string

// valueAsTime returns t if v can be interpreted as a time, otherwise zero, false.
func valueAsTime(v interface{}) (time.Time, bool) {
	if v == nil {
		return time.Time{}, false
	}
	switch x := v.(type) {
	case time.Time:
		return x, true
	case string:
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05.999999999Z07:00"} {
			if t, err := time.Parse(layout, x); err == nil {
				return t, true
			}
		}
		return time.Time{}, false
	default:
		return time.Time{}, false
	}
}

// valuesEqual compares expected (from event) and actual (from DB), normalizing date-time values.
func valuesEqual(expected, actual interface{}) bool {
	te, oke := valueAsTime(expected)
	ta, oka := valueAsTime(actual)
	if oke && oka {
		return te.Truncate(time.Millisecond).Equal(ta.Truncate(time.Millisecond))
	}
	// Fall back to JSON stringify for non-time values (and when one side isn't a time)
	return util.JSONStringify(expected) == util.JSONStringify(actual)
}

func validateSQLEventWithDB(logger logger.Logger, event internal.DBChangeEvent, db *sql.DB, format sqlDriverTransform) error {
	kv, err := event.GetObject()
	if err != nil {
		return fmt.Errorf("error getting object: %w", err)
	}
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = %s", format.QuoteTable(event.Table), format.QuoteColumn("id"), format.QuoteValue(event.GetPrimaryKey()))
	logger.Info("running query: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error running query: %w", err)
	}
	defer rows.Close()
	var count int
	columns, _ := rows.Columns()
	colcount := len(columns)
	values := make([]interface{}, colcount)
	valuePtrs := make([]interface{}, colcount)
	for rows.Next() {
		count++
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("error scanning row: %w", err)
		}
		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			b, ok := val.([]byte)
			var v interface{}
			if ok {
				v = string(b)
			} else {
				v = val
			}
			if ev, ok := kv[col]; ok {
				if !valuesEqual(ev, v) {
					return fmt.Errorf("%s value does not match, was: %v, expected: %v", col, v, ev)
				}
			} else {
				return fmt.Errorf("%s value %v was returned from db but was not expected", col, v)
			}
			row[col] = v
		}
		for k := range kv {
			if _, ok := row[k]; !ok {
				return fmt.Errorf("column %s was expected but not returned from db", k)
			}
		}
		logger.Info("row %d matched: %v", count-1, util.JSONStringify(row))
	}
	if event.Operation == "DELETE" {
		if count != 0 {
			return fmt.Errorf("expected 0 rows, got %d", count)
		}
	} else {
		if count != 1 {
			return fmt.Errorf("expected 1 row, got %d", count)
		}
	}
	return nil
}

func validateSQLEvent(logger logger.Logger, event internal.DBChangeEvent, driver string, url string, format sqlDriverTransform) error {
	db, err := sql.Open(driver, url)
	if err != nil {
		return fmt.Errorf("error opening database: %w", err)
	}
	defer db.Close()
	return validateSQLEventWithDB(logger, event, db, format)
}
