package util

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/shopmonkeyus/eds/internal"
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
func BuildDBSchemaFromInfoSchema(ctx context.Context, db *sql.DB, catalog string) (internal.DatabaseSchema, error) {
	res := make(internal.DatabaseSchema)
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_catalog = '%s'", catalog))
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
	return res, nil
}
