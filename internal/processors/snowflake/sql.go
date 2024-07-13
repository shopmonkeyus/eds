package snowflake

import (
	"encoding/json"
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

func toSQL(c internal.DBChangeEvent, schema internal.SchemaMap) (string, error) {
	var sql strings.Builder
	model := schema[c.Table]
	primaryKeys := model.PrimaryKeys
	if c.Operation == "DELETE" {
		sql.WriteString("DELETE FROM ")
		sql.WriteString(util.QuoteIdentifier(c.Table))
		sql.WriteString(" WHERE ")
		var predicate []string
		for i, pk := range primaryKeys {
			predicate = append(predicate, fmt.Sprintf("%s=%s", util.QuoteIdentifier(pk), quoteValue(c.Key[1+i])))
		}
		sql.WriteString(strings.Join(predicate, " AND "))
		sql.WriteString(";\n")
	} else {
		o := make(map[string]any)
		if err := json.Unmarshal(c.After, &o); err != nil {
			return "", err
		}
		sql.WriteString("MERGE INTO ")
		sql.WriteString(util.QuoteIdentifier(c.Table))
		sql.WriteString(" USING (SELECT ")
		sql.WriteString(strings.Join(util.QuoteStringIdentifiers(primaryKeys), ","))
		sql.WriteString(" FROM ")
		sql.WriteString(util.QuoteIdentifier(c.Table))
		sql.WriteString(" WHERE ")
		var sourcePredicates []string
		var sourceNullPredicates []string
		var targetPredicates []string
		for _, pk := range primaryKeys {
			val := o[pk]
			sourcePredicates = append(sourcePredicates, fmt.Sprintf("%s=%s", util.QuoteIdentifier(pk), quoteValue(val)))
			sourceNullPredicates = append(sourceNullPredicates, fmt.Sprintf("NULL AS %s", util.QuoteIdentifier(pk)))
			targetPredicates = append(targetPredicates, fmt.Sprintf("source.%s=%s.%s", util.QuoteIdentifier(pk), util.QuoteIdentifier(c.Table), util.QuoteIdentifier(pk)))
		}
		var columns []string
		for _, name := range model.Columns {
			columns = append(columns, util.QuoteIdentifier(name))
		}
		var insertVals []string
		var updateValues []string
		if c.Operation == "UPDATE" {
			for _, name := range c.Diff {
				if val, ok := o[name]; ok {
					v := quoteValue(val)
					updateValues = append(updateValues, fmt.Sprintf("%s=%s", util.QuoteIdentifier(name), v))
				} else {
					updateValues = append(updateValues, "NULL")
				}
			}
			for _, name := range model.Columns {
				if val, ok := o[name]; ok {
					v := quoteValue(val)
					insertVals = append(insertVals, v)
				} else {
					insertVals = append(insertVals, "NULL")
				}
			}
		} else {
			for _, name := range model.Columns {
				if val, ok := o[name]; ok {
					v := quoteValue(val)
					updateValues = append(updateValues, fmt.Sprintf("%s=%s", util.QuoteIdentifier(name), v))
					insertVals = append(insertVals, v)
				} else {
					updateValues = append(updateValues, "NULL")
					insertVals = append(insertVals, "NULL")
				}
			}
		}
		sql.WriteString(strings.Join(sourcePredicates, " AND "))
		sql.WriteString(" UNION SELECT ")
		sql.WriteString(strings.Join(sourceNullPredicates, " AND "))
		sql.WriteString(" LIMIT 1) AS source ON ")
		sql.WriteString(strings.Join(targetPredicates, " AND "))
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
	if u.Path[0:1] != "/" {
		str.WriteString("/")
	}
	str.WriteString(u.Path)
	return str.String(), nil
}

func propTypeToSQLType(propType string, format string) string {
	switch propType {
	case "string":
		if format == "date-time" {
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
		sql.WriteString(propTypeToSQLType(prop.Type, prop.Format))
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
