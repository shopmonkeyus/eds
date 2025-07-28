package snowflake

import (
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

func quoteString(val string, fn string) string {
	if val == "NULL" {
		return val
	}
	// Escape backslashes and single quotes
	escapedVal := strings.ReplaceAll(val, "\\", "\\\\")
	escapedVal = strings.ReplaceAll(escapedVal, "'", "''")
	res := "'" + escapedVal + "'"
	if fn != "" {
		return fn + "(" + res + ")"
	}
	return res
}

func quoteValue(value any, fn string) string {
	var str string
	switch arg := value.(type) {
	case nil:
		str = "NULL"
	case int:
		str = strconv.FormatInt(int64(arg), 10)
	case int8:
		str = strconv.FormatInt(int64(arg), 10)
	case int16:
		str = strconv.FormatInt(int64(arg), 10)
	case int32:
		str = strconv.FormatInt(int64(arg), 10)
	case *int32:
		if arg == nil {
			str = "NULL"
		} else {
			str = strconv.FormatInt(int64(*arg), 10)
		}
	case int64:
		str = strconv.FormatInt(arg, 10)
	case *int64:
		if arg == nil {
			str = "NULL"
		} else {
			str = strconv.FormatInt(*arg, 10)
		}
	case float32:
		str = strconv.FormatFloat(float64(arg), 'f', -1, 32)
	case float64:
		str = strconv.FormatFloat(arg, 'f', -1, 64)
	case *float64:
		if arg == nil {
			str = "NULL"
		} else {
			str = strconv.FormatFloat(*arg, 'f', -1, 64)
		}
	case bool:
		str = strconv.FormatBool(arg)
	case *bool:
		if arg == nil {
			str = "NULL"
		} else {
			str = strconv.FormatBool(*arg)
		}
	case string:
		str = quoteString(arg, fn)
	case *time.Time:
		if arg == nil {
			str = "NULL"
		} else {
			str = (*arg).Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
		}
	case time.Time:
		str = arg.Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
	case map[string]interface{}:
		str = quoteString(util.JSONStringify(arg), fn)
	default:
		value := reflect.ValueOf(arg)
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				str = "NULL"
			} else {
				if value.Elem().Kind() == reflect.Struct {
					str = quoteString(util.JSONStringify(arg), fn)
				} else {
					str = quoteString(fmt.Sprintf("%v", value.Elem().Interface()), fn)
				}
			}
		} else {
			str = quoteString(util.JSONStringify(arg), fn)
		}
	}
	return str
}

func toDeleteSQL(record *util.Record) string {
	var sql strings.Builder
	sql.WriteString("DELETE FROM ")
	sql.WriteString(util.QuoteIdentifier(record.Table))
	sql.WriteString(" WHERE ")
	sql.WriteString(util.QuoteIdentifier("id"))
	sql.WriteString("=")
	sql.WriteString(quoteValue(record.Id, ""))
	sql.WriteString(";\n")
	return sql.String()
}

func toMergeSQL(record *util.Record, model *internal.Schema) string {
	var sql strings.Builder
	// Apply the whole after field if the updated date is later than the existing updated date
	// Use MERGE for upsert operations (insert if not exists, update if exists)
	// TODO Switch this to use MSVCC timestamp? Possibly separate PR?
	var updateValues []string
	var insertColumns []string
	var insertVals []string

	for _, name := range model.Columns() {
		insertColumns = append(insertColumns, util.QuoteIdentifier(name))
		if val, ok := record.Object[name]; ok {
			c := model.Properties[name]
			var fn string
			switch c.Type {
			case "object":
				fn = "PARSE_JSON"
			case "array":
				if c.Items != nil && (c.Items.Type == "object" || c.Items.Type == "string") {
					fn = "PARSE_JSON"
				} else {
					fn = "TO_VARIANT"
				}
			}
			v := quoteValue(val, fn)
			updateValues = append(updateValues, fmt.Sprintf("%s=%s", util.QuoteIdentifier(name), v))
			insertVals = append(insertVals, v)
		} else {
			// For missing values, use nullable defaults for insert
			c := model.Properties[name]
			v := nullableValue(c, true)
			insertVals = append(insertVals, v)
		}
	}

	after, _ := record.Event.GetObject() // ignore error because we assume after is present in an update event

	sql.WriteString("MERGE INTO ")
	sql.WriteString(util.QuoteIdentifier(record.Table))
	sql.WriteString(" AS target USING (SELECT ")
	sql.WriteString(quoteValue(record.Id, ""))
	sql.WriteString(" AS ")
	sql.WriteString(util.QuoteIdentifier("id"))
	sql.WriteString(", ")
	sql.WriteString(quoteValue(after["updatedDate"], ""))
	sql.WriteString(" AS ")
	sql.WriteString(util.QuoteIdentifier("updatedDate"))
	sql.WriteString(") AS source ON target.")
	sql.WriteString(util.QuoteIdentifier("id"))
	sql.WriteString(" = source.")
	sql.WriteString(util.QuoteIdentifier("id"))
	sql.WriteString(" ")

	// WHEN MATCHED clause - update if updatedDate is newer
	sql.WriteString("WHEN MATCHED AND source.")
	sql.WriteString(util.QuoteIdentifier("updatedDate"))
	sql.WriteString(" > target.")
	sql.WriteString(util.QuoteIdentifier("updatedDate"))
	sql.WriteString(" THEN UPDATE SET ")
	sql.WriteString(strings.Join(updateValues, ","))

	// WHEN NOT MATCHED clause - insert new record
	sql.WriteString(" WHEN NOT MATCHED THEN INSERT (")
	sql.WriteString(strings.Join(insertColumns, ","))
	sql.WriteString(") VALUES (")
	sql.WriteString(strings.Join(insertVals, ","))
	sql.WriteString(");\n")
	return sql.String()
}

func nullableValue(c internal.SchemaProperty, wrap bool) string {
	if c.Nullable {
		return "NULL"
	} else {
		switch c.Type {
		case "object":
			if wrap {
				return "PARSE_JSON('{}')"
			}
			return "'{}'"
		case "array":
			if wrap {
				return "PARSE_JSON('[]')"
			}
			return "'[]'"
		case "number", "integer":
			return "0"
		case "boolean":
			return "false"
		default:
			return "''"
		}
	}
}

func toSQL(record *util.Record, model *internal.Schema, exists bool, updateStrategy string) (string, int) {
	var sql strings.Builder
	var count int
	if exists || record.Operation == "DELETE" {
		sql.WriteString(toDeleteSQL(record))
		count++
	}
	if record.Operation != "DELETE" {
		if record.Operation == "INSERT" {
			var columns []string
			for _, name := range model.Columns() {
				columns = append(columns, util.QuoteIdentifier(name))
			}
			var insertVals []string
			for _, name := range model.Columns() {
				c := model.Properties[name]
				if val, ok := record.Object[name]; ok {
					var fn string
					switch c.Type {
					case "object":
						fn = "PARSE_JSON"
					case "array":
						if c.Items != nil && (c.Items.Type == "object" || c.Items.Type == "string") {
							fn = "PARSE_JSON"
						} else {
							fn = "TO_VARIANT"
						}
					}
					v := quoteValue(val, fn)
					insertVals = append(insertVals, v)
				} else {
					insertVals = append(insertVals, nullableValue(c, true))
				}
			}
			sql.WriteString("INSERT INTO ")
			sql.WriteString(util.QuoteIdentifier(record.Table))
			sql.WriteString(" (")
			sql.WriteString(strings.Join(columns, ","))
			sql.WriteString(") SELECT ")
			sql.WriteString(strings.Join(insertVals, ","))
			sql.WriteString(";\n")
		} else {
			// update
			switch updateStrategy {
			case "after":
				sql.WriteString(toMergeSQL(record, model))
			default: // "standard"
				// Standard is applying just the diffs in the order they are received
				var updateValues []string
				for _, name := range record.Diff {
					if !util.SliceContains(model.Columns(), name) {
						continue
					}
					if val, ok := record.Object[name]; ok {
						v := quoteValue(val, "")
						updateValues = append(updateValues, fmt.Sprintf("%s=%s", util.QuoteIdentifier(name), v))
					}
					// else shouldn't be possible
				}
				if len(updateValues) == 0 {
					return sql.String(), count // in case we skipped, just return
				}
				sql.WriteString("UPDATE ")
				sql.WriteString(util.QuoteIdentifier(record.Table))
				sql.WriteString(" SET ")
				sql.WriteString(strings.Join(updateValues, ","))
				sql.WriteString(" WHERE ")
				sql.WriteString(util.QuoteIdentifier("id"))
				sql.WriteString("=")
				sql.WriteString(quoteValue(record.Id, ""))
				sql.WriteString(";\n")
			}
		}
		count++
	}
	return sql.String(), count
}

func GetConnectionStringFromURL(urlString string) (string, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return "", fmt.Errorf("error parsing snowflake connection string from url: %w", err)
	}
	var str strings.Builder
	if u.User != nil {
		str.WriteString(u.User.String())
		str.WriteString("@")
	}
	str.WriteString(u.Host)
	if !strings.HasPrefix(u.Path, "/") {
		str.WriteString("/")
	}
	str.WriteString(u.Path)
	v := u.Query()
	v.Set("client_session_keep_alive", "true")
	v.Set("application", "eds")
	str.WriteString("?")
	str.WriteString(v.Encode())
	return str.String(), nil
}

func propTypeToSQLType(property internal.SchemaProperty) string {
	switch property.Type {
	case "string":
		if property.Format == "date-time" {
			return "TIMESTAMP_NTZ"
		}
		return "STRING"
	case "integer":
		return "INTEGER"
	case "number":
		return "FLOAT"
	case "boolean":
		return "BOOLEAN"
	case "object":
		return "STRING"
	case "array":
		if property.Items != nil && property.Items.Enum != nil {
			return "STRING" // this is an enum but we want to represent it as a string
		}
		return "VARIANT"
	default:
		return "STRING"
	}
}

func createSQL(s *internal.Schema) string {
	var sql strings.Builder
	sql.WriteString("CREATE OR REPLACE TABLE ")
	sql.WriteString(util.QuoteIdentifier((s.Table)))
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
		sql.WriteString("\t")
		sql.WriteString(util.QuoteIdentifier(name))
		sql.WriteString(" ")
		sql.WriteString(propTypeToSQLType(prop))
		if util.SliceContains(s.Required, name) && !prop.Nullable {
			sql.WriteString(" NOT NULL")
		}
		sql.WriteString(",\n")
	}
	if len(s.PrimaryKeys) > 0 {
		sql.WriteString("\tPRIMARY KEY (")
		for i, pk := range s.PrimaryKeys {
			sql.WriteString(util.QuoteIdentifier(pk))
			if i < len(s.PrimaryKeys)-1 {
				sql.WriteString(", ")
			}
		}
		sql.WriteString(")")
	}
	sql.WriteString("\n);\n")
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
		sql.WriteString(util.QuoteIdentifier(s.Table))
		sql.WriteString(" ADD COLUMN ")
		sql.WriteString(util.QuoteIdentifier(column))
		sql.WriteString(" ")
		sql.WriteString(propTypeToSQLType(prop))
		sql.WriteString(";")
		res = append(res, sql.String())
	}
	return res
}
