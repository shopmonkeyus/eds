package util

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/go-common/logger"
)

// Selects a single value from the database using the provided function.
func QuerySingleValue(ctx context.Context, db *sql.DB, fn string) (string, error) {
	var value string
	if err := db.QueryRowContext(ctx, "SELECT "+fn).Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

type QueryConditions struct {
	Column string
	Value  string
}

// Builds a database schema from the information schema with additional conditions.
func BuildDBSchemaFromInfoSchemaWithConditions(ctx context.Context, logger logger.Logger, db *sql.DB, column string, value string, failIfEmpty bool, conditions ...QueryConditions) (internal.DatabaseSchema, error) {
	res := make(internal.DatabaseSchema)
	start := time.Now()
	q := fmt.Sprintf("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE %s = '%s'", column, value)
	for _, condition := range conditions {
		q += fmt.Sprintf(" AND %s = '%s'", condition.Column, condition.Value)
	}
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

func BuildDBSchemaFromInfoSchema(ctx context.Context, logger logger.Logger, db *sql.DB, column string, value string, failIfEmpty bool) (internal.DatabaseSchema, error) {
	return BuildDBSchemaFromInfoSchemaWithConditions(ctx, logger, db, column, value, failIfEmpty)
}
