package util

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/go-common/logger"
)

// QuoteIdentifier quotes an identifier with double quotes
func QuoteIdentifier(name string) string {
	return `"` + name + `"`
}

// QuoteStringIdentifiers quotes a slice of identifiers with double quotes
func QuoteStringIdentifiers(vals []string) []string {
	res := make([]string, len(vals))
	for i, val := range vals {
		res[i] = QuoteIdentifier(val)
	}
	return res
}

// SQLExecuter returns a wrapper around a SQL database connection that can execute SQL statements or log them in dry-run mode
func SQLExecuter(ctx context.Context, log logger.Logger, db *sql.DB, dryRun bool) func(sql string) error {
	return func(sql string) error {
		if dryRun {
			log.Info("[dry-run] %s", sql)
			return nil
		}
		log.Debug("executing: %s", sql)
		if _, err := db.ExecContext(ctx, sql); err != nil {
			return err
		}
		return nil
	}
}

func IsJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}

// ToJSONStringVal returns a JSON string value checking for empty string and converting it to '{}'
func ToJSONStringVal(name string, val string, jsonb map[string]bool) string {
	if jsonb[name] && (val == "''" || val == "" || val == "null" || val == "[]") {
		return "'{}'"
	}
	return val
}

// ToMapOfJSONColumns returns a map of column names that are of type 'object'
func ToMapOfJSONColumns(model *internal.Schema) map[string]bool {
	jsonb := make(map[string]bool)
	for _, name := range model.Columns {
		property := model.Properties[name]
		if property.Type == "object" {
			jsonb[name] = true
		}
	}
	return jsonb
}
