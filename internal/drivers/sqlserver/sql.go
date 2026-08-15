package sqlserver

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const updatedDateColumn = "updatedDate"

func quoteIdentifier(val string) string {
	return "[" + val + "]"
}

func columnValueOrNull(name string, prop internal.SchemaProperty, object map[string]any) string {
	v := "NULL"
	if val, ok := object[name]; ok {
		v = quoteValue(val)
	}
	return util.ToJSONStringVal(name, v, prop, false)
}

func toSQLFromObject(model *internal.Schema, table string, object map[string]any) string {
	var updateValues []string
	var insertColumns []string
	var insertValues []string

	for _, name := range model.Columns() {
		v := columnValueOrNull(name, model.Properties[name], object)
		insertColumns = append(insertColumns, quoteIdentifier(name))
		insertValues = append(insertValues, v)
		if name != "id" {
			updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
		}
	}

	// the source row carries updatedDate so a matched row can reject events that are
	// older than what has already been written
	_, hasUpdatedDate := model.Properties[updatedDateColumn]

	var sql strings.Builder
	sql.WriteString("MERGE ")
	sql.WriteString(quoteIdentifier(table))
	sql.WriteString(" AS target")
	sql.WriteString(" USING (VALUES(")
	sql.WriteString(quoteValue(object["id"]))
	if hasUpdatedDate {
		sql.WriteString(",")
		sql.WriteString(quoteValue(object[updatedDateColumn]))
	}
	sql.WriteString(")) AS source (")
	sql.WriteString(quoteIdentifier("id"))
	if hasUpdatedDate {
		sql.WriteString(",")
		sql.WriteString(quoteIdentifier(updatedDateColumn))
	}
	sql.WriteString(") ON target.")
	sql.WriteString(quoteIdentifier("id"))
	sql.WriteString("=source.")
	sql.WriteString(quoteIdentifier("id"))
	if len(updateValues) > 0 {
		sql.WriteString(" WHEN MATCHED")
		if hasUpdatedDate {
			// a row that has never been stamped is always safe to overwrite
			sql.WriteString(fmt.Sprintf(
				" AND (target.%[1]s IS NULL OR source.%[1]s>target.%[1]s)",
				quoteIdentifier(updatedDateColumn),
			))
		}
		sql.WriteString(" THEN UPDATE SET ")
		sql.WriteString(strings.Join(updateValues, ","))
	}
	sql.WriteString(" WHEN NOT MATCHED THEN INSERT (")
	sql.WriteString(strings.Join(insertColumns, ","))
	sql.WriteString(") VALUES (")
	sql.WriteString(strings.Join(insertValues, ","))
	sql.WriteString(");") // must be terminated for merge to work

	return sql.String()
}

func toSQL(c internal.DBChangeEvent, model *internal.Schema) (string, error) {
	primaryKeys := model.PrimaryKeys
	if c.Operation == "DELETE" {
		var sql strings.Builder
		sql.WriteString("DELETE FROM ")
		sql.WriteString(quoteIdentifier(c.Table))
		sql.WriteString(" WHERE ")
		var predicate []string
		for i, pk := range primaryKeys {
			predicate = append(predicate, fmt.Sprintf("%s=%s", quoteIdentifier(pk), quoteValue(c.Key[i])))
		}
		sql.WriteString(strings.Join(predicate, " AND "))
		sql.WriteString(";\n")
		return sql.String(), nil
	} else {
		o := make(map[string]any)
		if err := json.Unmarshal(c.After, &o); err != nil {
			return "", err
		}
		return toSQLFromObject(model, c.Table, o), nil
	}
}

func propTypeToSQLType(property internal.SchemaProperty, isPrimaryKey bool) string {
	switch property.Type {
	case "string":
		if isPrimaryKey {
			return "VARCHAR(64)"
		}
		if property.Format == "date-time" {
			return "NVARCHAR(MAX)"
		}
		return "NVARCHAR(MAX)"
	case "integer":
		return "BIGINT"
	case "number":
		return "FLOAT"
	case "boolean":
		return "BIT"
	case "object":
		return "NVARCHAR(MAX)" // for JSON
	case "array":
		if property.Items != nil && property.Items.Enum != nil {
			return "VARCHAR(64)" // this is an enum but we want to represent it as a string
		}
		return "NVARCHAR(MAX)" // for JSON
	default:
		return "NVARCHAR(MAX)"
	}
}

func createSQL(s *internal.Schema) string {
	var sql strings.Builder
	sql.WriteString("DROP TABLE IF EXISTS ")
	sql.WriteString(quoteIdentifier(s.Table))
	sql.WriteString(";\n")
	sql.WriteString("CREATE TABLE ")
	sql.WriteString(quoteIdentifier(s.Table))
	sql.WriteString(" (\n")
	var columns []string
	for _, name := range s.Columns() {
		if util.SliceContains(s.PrimaryKeys, name) {
			continue
		}
		columns = append(columns, name)
	}
	sort.Strings(columns)
	columns = append(s.PrimaryKeys, columns...)
	for _, name := range columns {
		prop := s.Properties[name]
		isPrimaryKey := util.SliceContains(s.PrimaryKeys, name)
		sql.WriteString("\t")
		sql.WriteString(quoteIdentifier(name))
		sql.WriteString(" ")
		sql.WriteString(propTypeToSQLType(prop, isPrimaryKey))
		if !isPrimaryKey {
			sql.WriteString(" NULL")
		}
		sql.WriteString(",\n")
	}
	if len(s.PrimaryKeys) > 0 {
		sql.WriteString("\tPRIMARY KEY (")
		for i, pk := range s.PrimaryKeys {
			sql.WriteString(quoteIdentifier(pk))
			if i < len(s.PrimaryKeys)-1 {
				sql.WriteString(", ")
			}
		}
		sql.WriteString(")")
	}
	sql.WriteString("\n)")

	return sql.String()
}

func addNewColumnsSQL(logger logger.Logger, columns []string, s *internal.Schema, db internal.DatabaseSchema) []string {
	var res []string
	for _, column := range columns {
		if ok, _ := db.GetType(s.Table, column); ok {
			logger.Warn("skipping migration for column: %s for table: %s since it already exists", column, s.Table)
			continue
		}
		var sql strings.Builder
		prop := s.Properties[column]
		sql.WriteString("ALTER TABLE ")
		sql.WriteString(quoteIdentifier(s.Table))
		sql.WriteString(" ADD ")
		sql.WriteString(quoteIdentifier(column))
		sql.WriteString(" ")
		sql.WriteString(propTypeToSQLType(prop, false))
		sql.WriteString(" NULL;")
		res = append(res, sql.String())
	}
	return res
}

func ParseURLToDSN(urlstr string) (string, error) {
	// Example input: "sqlserver://sa:eds@localhost:11433/eds"
	// Desired output: "sqlserver://sa:eds@localhost:11433/database=eds?multiStatements=true"
	u, err := url.Parse(urlstr)
	if err != nil {
		return "", fmt.Errorf("error parsing url: %w", err)
	}
	vals := u.Query()

	if util.IsLocalhost(u.Host) && vals.Get("encrypt") == "" {
		vals.Set("encrypt", "disable")
	}

	if vals.Get("app name") == "" {
		vals.Set("app name", "eds")
	}

	// Start building the DSN string
	var dsn strings.Builder
	dsn.WriteString("sqlserver") // Add the scheme (e.g., "sqlserver")
	dsn.WriteString("://")

	if u.User != nil {
		dsn.WriteString(util.ToUserPass(u))
		dsn.WriteString("@")
	}

	dsn.WriteString(u.Host)

	if u.Path != "" {
		vals.Set("database", u.Path[1:])
		u.Path = ""
	}

	if encoded := vals.Encode(); encoded != "" {
		dsn.WriteString("?")
		dsn.WriteString(encoded)
	}

	return dsn.String(), nil
}
