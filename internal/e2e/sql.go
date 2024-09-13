//go:build e2e
// +build e2e

package e2e

import (
	"database/sql"
	"fmt"
	"strings"

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

func validateSQLEvent(logger logger.Logger, event internal.DBChangeEvent, driver string, url string, format sqlDriverTransform) error {
	kv, err := event.GetObject()
	if err != nil {
		return fmt.Errorf("error getting object: %w", err)
	}
	db, err := sql.Open(driver, url)
	if err != nil {
		return fmt.Errorf("error opening database: %w", err)
	}
	defer db.Close()
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = %s", format.QuoteTable(event.Table), format.QuoteColumn("id"), format.QuoteValue(event.GetPrimaryKey()))
	logger.Info("running query: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error running query: %w", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
		if event.Table == "order" && event.Operation == "UPDATE" && strings.HasSuffix(event.ModelVersion, "-update") {
			var id string
			var name string
			var age float64
			if err := rows.Scan(&id, &name, &age); err != nil {
				return fmt.Errorf("error scanning row: %w", err)
			}
			logger.Info("row returned: %v, %v, %v", id, name, age)
			if id != kv["id"] {
				return fmt.Errorf("id values do not match, was: %s, expected: %s", id, kv["id"])
			}
			if name != kv["name"] {
				return fmt.Errorf("name values do not match, was: %s, expected: %s", name, kv["name"])
			}
			if age != kv["age"] {
				return fmt.Errorf("age values do not match, was: %v, expected: %v", age, kv["age"])
			}
		} else {
			var id string
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				return fmt.Errorf("error scanning row: %w", err)
			}
			logger.Info("row returned: %v, %v", id, name)
			if id != kv["id"] {
				return fmt.Errorf("id values do not match, was: %s, expected: %s", id, kv["id"])
			}
			if name != kv["name"] {
				return fmt.Errorf("name values do not match, was: %s, expected: %s", name, kv["name"])
			}
		}
		logger.Info("event validated: %s", util.JSONStringify(event))
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
