package util

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/go-common/logger"
)

// GetCurrentDatabase returns the name of the selected database
func GetCurrentDatabase(ctx context.Context, db *sql.DB, fn string) (string, error) {
	var name string
	if err := db.QueryRowContext(ctx, "SELECT "+fn).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

// BuildDBSchemaFromInfoSchema builds a database schema from the information schema.
func BuildDBSchemaFromInfoSchema(ctx context.Context, logger logger.Logger, db *sql.DB, column string, value string, failIfEmpty bool) (internal.DatabaseSchema, error) {
	res := make(internal.DatabaseSchema)
	start := time.Now()
	q := fmt.Sprintf("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE %s = '%s'", column, value)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var tableName, columnName, dataType string
		if err := rows.Scan(&tableName, &columnName, &dataType); err != nil {
			return nil, err
		}
		if _, ok := res[tableName]; !ok {
			res[tableName] = make(map[string]string)
		}
		res[tableName][columnName] = dataType
	}
	if failIfEmpty && len(res) == 0 {
		return nil, fmt.Errorf("no tables found using %s = %s", column, value)
	}
	logger.Info("refreshed %d tables ddl in %v", len(res), time.Since(start))
	return res, nil
}
