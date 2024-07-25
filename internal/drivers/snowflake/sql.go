package snowflake

import (
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
)

func quoteString(val string) string {
	return "'" + strings.ReplaceAll(val, "'", "''") + "'"
}

func quoteValue(value any) string {
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
		str = quoteString(arg)
	case *time.Time:
		if arg == nil {
			str = "NULL"
		} else {
			str = (*arg).Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
		}
	case time.Time:
		str = arg.Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
	case map[string]interface{}:
		str = quoteString(util.JSONStringify(arg))
	default:
		value := reflect.ValueOf(arg)
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				str = "NULL"
			} else {
				if value.Elem().Kind() == reflect.Struct {
					str = quoteString(util.JSONStringify(arg))
				} else {
					str = quoteString(fmt.Sprintf("%v", value.Elem().Interface()))
				}
			}
		} else {
			str = quoteString(util.JSONStringify(arg))
		}
	}
	return str
}

func toSQL(record *util.Record, schema internal.SchemaMap) (string, error) {
	var sql strings.Builder
	model := schema[record.Table]
	if record.Operation == "DELETE" {
		sql.WriteString("DELETE FROM ")
		sql.WriteString(util.QuoteIdentifier(record.Table))
		sql.WriteString(" WHERE ")
		sql.WriteString(util.QuoteIdentifier("id"))
		sql.WriteString("=")
		sql.WriteString(quoteValue(record.Id))
		sql.WriteString(";\n")
	} else {
		sql.WriteString("MERGE INTO ")
		sql.WriteString(util.QuoteIdentifier(record.Table))
		sql.WriteString(" USING (SELECT ")
		sql.WriteString(util.QuoteIdentifier("id"))
		sql.WriteString(" FROM ")
		sql.WriteString(util.QuoteIdentifier(record.Table))
		sql.WriteString(" WHERE ")
		sourcePredicate := fmt.Sprintf("%s=%s", util.QuoteIdentifier("id"), quoteValue(record.Id))
		sourceNullPredicate := fmt.Sprintf("NULL AS %s", util.QuoteIdentifier("id"))
		targetPredicate := fmt.Sprintf("source.%s=%s.%s", util.QuoteIdentifier("id"), util.QuoteIdentifier(record.Table), util.QuoteIdentifier("id"))
		var columns []string
		for _, name := range model.Columns {
			columns = append(columns, util.QuoteIdentifier(name))
		}
		var insertVals []string
		var updateValues []string
		if record.Operation == "UPDATE" {
			for _, name := range record.Diff {
				if !util.SliceContains(model.Columns, name) {
					continue
				}
				if val, ok := record.Object[name]; ok {
					v := quoteValue(val)
					updateValues = append(updateValues, fmt.Sprintf("%s=%s", util.QuoteIdentifier(name), v))
				} else {
					updateValues = append(updateValues, "NULL")
				}
			}
			for _, name := range model.Columns {
				if val, ok := record.Object[name]; ok {
					v := quoteValue(val)
					insertVals = append(insertVals, v)
				} else {
					insertVals = append(insertVals, "NULL")
				}
			}
		} else {
			for _, name := range model.Columns {
				if val, ok := record.Object[name]; ok {
					v := quoteValue(val)
					updateValues = append(updateValues, fmt.Sprintf("%s=%s", util.QuoteIdentifier(name), v))
					insertVals = append(insertVals, v)
				} else {
					updateValues = append(updateValues, "NULL")
					insertVals = append(insertVals, "NULL")
				}
			}
		}
		sql.WriteString(sourcePredicate)
		sql.WriteString(" UNION SELECT ")
		sql.WriteString(sourceNullPredicate)
		sql.WriteString(" LIMIT 1) AS source ON ")
		sql.WriteString(targetPredicate)
		sql.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		sql.WriteString(strings.Join(updateValues, ","))
		sql.WriteString(" WHEN NOT MATCHED THEN INSERT (")
		sql.WriteString(strings.Join(columns, ","))
		sql.WriteString(") VALUES (")
		sql.WriteString(strings.Join(insertVals, ","))
		sql.WriteString(");\n")
	}
	return sql.String(), nil
}

func getConnectionStringFromURL(urlString string) (string, error) {
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
	for _, name := range s.Columns {
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
