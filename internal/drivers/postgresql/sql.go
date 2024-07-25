package postgresql

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
)

const magicEscape = "$_H_$"

var safeCharacters = regexp.MustCompile(`^["/.,;:$%/@!#$%^&*(){}\[\]|\\<>?~a-zA-Z0-9_\- ]+$`)

var badCharacters = regexp.MustCompile(`\x00`) // in v1 we have the null character that show up in messages

func quoteString(str string) string {
	if len(str) != 0 && badCharacters.MatchString(str) {
		str = badCharacters.ReplaceAllString(str, "")
	}
	if util.IsJSON(str) {
		return str
	}
	if len(str) == 0 || safeCharacters.MatchString(str) {
		return `'` + str + `'`
	}
	return magicEscape + str + magicEscape
}

func quoteBytes(buf []byte) string {
	return `'\x` + hex.EncodeToString(buf) + "'"
}

func quoteValue(arg any) (str string) {
	switch arg := arg.(type) {
	case nil:
		return "null"
	case int:
		return strconv.FormatInt(int64(arg), 10)
	case int8:
		return strconv.FormatInt(int64(arg), 10)
	case int16:
		return strconv.FormatInt(int64(arg), 10)
	case int32:
		return strconv.FormatInt(int64(arg), 10)
	case *int32:
		if arg == nil {
			return "null"
		} else {
			return strconv.FormatInt(int64(*arg), 10)
		}
	case int64:
		return strconv.FormatInt(arg, 10)
	case *int64:
		if arg == nil {
			return "null"
		} else {
			return strconv.FormatInt(*arg, 10)
		}
	case float32:
		return strconv.FormatFloat(float64(arg), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(arg, 'f', -1, 64)
	case *float64:
		if arg == nil {
			return "null"
		} else {
			return strconv.FormatFloat(*arg, 'f', -1, 64)
		}
	case bool:
		return strconv.FormatBool(arg)
	case *bool:
		if arg == nil {
			return "null"
		} else {
			return strconv.FormatBool(*arg)
		}
	case []byte:

		return quoteBytes(arg)
	case *string:
		if arg == nil {
			return "null"
		}
		if util.IsJSON(*arg) {
			return *arg
		}
		return quoteString(*arg)

	case string:
		if util.IsJSON(arg) {
			return arg
		}
		return quoteString(arg)
	case *time.Time:
		if arg == nil {
			return "null"
		} else {
			return (*arg).Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
		}
	case time.Time:
		return arg.Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
	case []string:
		var ns []string
		for _, thes := range arg {
			ns = append(ns, pq.QuoteLiteral(thes))
		}
		return "ARRAY[" + strings.Join(ns, ",") + "]::JSONB]"
	case map[string]interface{}:
		if len(arg) == 0 {
			return "null"
		}

		return quoteString(util.JSONStringify(arg))

	case []interface{}:
		if len(arg) == 0 {
			str = "null"
		} else {
			var ns []string
			for _, thes := range arg {

				ns = append(ns, quoteValue(thes))
			}
			if util.IsJSON(ns[0]) {
				ns[0] = "'[" + ns[0]
				ns[len(ns)-1] = ns[len(ns)-1] + "]'"
			}
			return strings.Join(ns, ",")

		}
	default:
		value := reflect.ValueOf(arg)
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				return "null"
			} else {
				if value.Elem().Kind() == reflect.Struct {
					return quoteString(util.JSONStringify(arg))
				} else {
					return quoteString(fmt.Sprintf("%v", value.Elem().Interface()))
				}
			}
		} else {
			return quoteString(util.JSONStringify(arg))
		}
	}
	return str
}

func appendArraytype(val string, dataType string) string {
	switch dataType {
	case "date-time":
		return val + "::TIMESTAMP WITH TIME ZONE[]"
	case "string":
		return val + "::TEXT[]"
	case "integer":
		return val + "::INTEGER[]"
	case "number":
		return val + "::FLOAT[]"
	case "boolean":
		return val + "::BOOLEAN[]"
	case "object":
		return val + "::JSONB[]"
	default:
		return val + "::TEXT[]"
	}
}

var needsQuote = regexp.MustCompile(`[A-Z0-9_\s]`)
var keywords = regexp.MustCompile(`(?i)\b(USER|SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|GROUP BY|ORDER BY|HAVING|AND|OR|CREATE|DROP|ALTER|TABLE|INDEX|ON|INTO|VALUES|SET|AS|DISTINCT|TYPE|DEFAULT|ORDER|GROUP|LIMIT|SUM|TOTAL|START|END|BEGIN|COMMIT|ROLLBACK|PRIMARY|AUTHORIZATION)\b`)

func handleSchemaProperty(prop internal.SchemaProperty, v string) string {
	switch prop.Type {
	case "object":
		if prop.AdditionalProperties != nil && *prop.AdditionalProperties {
			if util.IsJSON(v) {
				//Double up on any single quotes
				v = strings.Replace(v, "'", "''", -1)
				return "'" + v + "'::JSONB"
			}
			return v + "::JSONB"

		} else {
			v = "ARRAY[" + v + "]"
			return v + "::JSONB[]"
		}
	case "array":
		if v == "null" && !prop.Nullable {
			v = "ARRAY[]"
		}
		if prop.Items != nil && prop.Items.Type != "enum" {
			v = "ARRAY[" + v + "]"
			dataType := prop.Items.Format
			return appendArraytype(v, dataType)
		}
		return v
	default:
		return v
	}
}

func quoteIdentifier(val string) string {
	if needsQuote.MatchString(val) || keywords.MatchString(val) {
		return pq.QuoteIdentifier(val)
	}
	return val
}

func toSQLFromObject(operation string, model *internal.Schema, table string, o map[string]any, diff []string) string {
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(quoteIdentifier(table))
	var columns []string
	jsonb := util.ToMapOfJSONColumns(model)
	for _, name := range model.Columns {
		columns = append(columns, quoteIdentifier(name))
	}
	sql.WriteString(" (")
	sql.WriteString(strings.Join(columns, ","))
	sql.WriteString(") VALUES (")
	var insertVals []string
	var updateValues []string
	if operation == "UPDATE" {
		for _, name := range diff {
			if !util.SliceContains(model.Columns, name) || name == "id" {
				continue
			}
			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), jsonb)
				updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
			} else {
				updateValues = append(updateValues, "NULL")
			}
		}
		for _, name := range model.Columns {
			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), jsonb)
				v = handleSchemaProperty(model.Properties[name], v)
				insertVals = append(insertVals, v)
			} else {
				insertVals = append(insertVals, "NULL")
			}
		}
	} else {
		for _, name := range model.Columns {
			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), jsonb)
				if name != "id" {
					v = handleSchemaProperty(model.Properties[name], v)
					updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
				}
				insertVals = append(insertVals, v)
			} else {
				updateValues = append(updateValues, "NULL")
				insertVals = append(insertVals, "NULL")
			}
		}
	}
	sql.WriteString(strings.Join(insertVals, ","))
	sql.WriteString(") ON CONFLICT (id) DO ")
	if len(updateValues) == 0 {
		sql.WriteString("NOTHING")
	} else {
		sql.WriteString("UPDATE SET ")
		sql.WriteString(strings.Join(updateValues, ","))
	}
	sql.WriteString(";\n")
	return sql.String()
}

func toSQL(c internal.DBChangeEvent, schema internal.SchemaMap) (string, error) {
	model := schema[c.Table]
	primaryKeys := model.PrimaryKeys
	if c.Operation == "DELETE" {
		var sql strings.Builder
		sql.WriteString("DELETE FROM ")
		sql.WriteString(quoteIdentifier(c.Table))
		sql.WriteString(" WHERE ")
		var predicate []string
		for i, pk := range primaryKeys {
			predicate = append(predicate, fmt.Sprintf("%s=%s", quoteIdentifier(pk), quoteValue(c.Key[1+i])))
		}
		sql.WriteString(strings.Join(predicate, " AND "))
		sql.WriteString(";\n")
		return sql.String(), nil
	} else {
		o := make(map[string]any)
		if err := json.Unmarshal(c.After, &o); err != nil {
			return "", err
		}
		return toSQLFromObject(c.Operation, model, c.Table, o, c.Diff), nil
	}
}

func propTypeToSQLType(property internal.SchemaProperty) string {
	switch property.Type {

	case "string":
		if property.Format == "date-time" {
			return "TIMESTAMP WITH TIME ZONE"
		}
		return "TEXT"
	case "integer":
		return "BIGINT"
	case "number":
		return "DOUBLE PRECISION"
	case "boolean":
		return "BOOLEAN"
	case "object":
		return "JSONB"
	case "array":
		if property.Items != nil {
			if property.Items.Enum != nil {
				return "VARCHAR(64)" // this is an enum but we want to represent it as a string
			}
			if property.Items.Format == "date-time" {
				return "TIMESTAMP WITH TIME ZONE[]"
			}
			if property.Items.Type == "string" {
				return "TEXT[]"
			}

		}

		return "JSONB"
	default:
		return "TEXT"
	}
}

func createSQL(s *internal.Schema) string {
	var sql strings.Builder
	sql.WriteString("DROP TABLE IF EXISTS ")
	sql.WriteString(quoteIdentifier((s.Table)))
	sql.WriteString(";\n")
	sql.WriteString("CREATE TABLE ")
	sql.WriteString(quoteIdentifier((s.Table)))
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
		sql.WriteString(quoteIdentifier(name))
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
			sql.WriteString(quoteIdentifier(pk))
			if i < len(s.PrimaryKeys)-1 {
				sql.WriteString(", ")
			}
		}
		sql.WriteString(")")
	}
	sql.WriteString("\n);\n")
	return sql.String()
}

func getConnectionStringFromURL(urlstr string) (string, error) {
	u, err := url.Parse(urlstr)
	if err != nil {
		return "", fmt.Errorf("error parsing postgres db url: %w", err)
	}
	u.Scheme = "postgresql"
	if u.Port() == "" {
		u.Host = u.Host + ":5432"
	}
	if !u.Query().Has("application_name") {
		q := u.Query()
		q.Set("application_name", "eds-server")
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}
