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
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const magicEscape = "$_H_$"

var safeCharacters = regexp.MustCompile(`^["/.,;:$%/@!#$%^&*(){}\[\]|\\<>?~a-zA-Z0-9_\- ]+$`)

var badCharacters = regexp.MustCompile(`\x00`) // in v1 we have the null character that show up in messages

func quoteString(str string) string {
	if len(str) != 0 && badCharacters.MatchString(str) {
		str = badCharacters.ReplaceAllString(str, "")
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
		str = "null"
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
			str = "null"
		} else {
			str = strconv.FormatInt(int64(*arg), 10)
		}
	case int64:
		str = strconv.FormatInt(arg, 10)
	case *int64:
		if arg == nil {
			str = "null"
		} else {
			str = strconv.FormatInt(*arg, 10)
		}
	case float32:
		str = strconv.FormatFloat(float64(arg), 'f', -1, 32)
	case float64:
		str = strconv.FormatFloat(arg, 'f', -1, 64)
	case *float64:
		if arg == nil {
			str = "null"
		} else {
			str = strconv.FormatFloat(*arg, 'f', -1, 64)
		}
	case bool:
		str = strconv.FormatBool(arg)
	case *bool:
		if arg == nil {
			str = "null"
		} else {
			str = strconv.FormatBool(*arg)
		}
	case []byte:
		str = quoteBytes(arg)
	case *string:
		if arg == nil {
			str = "null"
		} else {
			str = quoteString(*arg)
		}
	case string:
		str = quoteString(arg)
	case *time.Time:
		if arg == nil {
			str = "null"
		} else {
			str = (*arg).Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
		}
	case time.Time:
		str = arg.Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
	case []string:
		var ns []string
		for _, thes := range arg {
			ns = append(ns, pq.QuoteLiteral(thes))
		}
		str = quoteString(util.JSONStringify(ns))
	case []interface{}:
		str = quoteString(util.JSONStringify(arg))
	default:
		value := reflect.ValueOf(arg)
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				str = "null"
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

var needsQuote = regexp.MustCompile(`[A-Z0-9_\s]`)
var keywords = regexp.MustCompile(`(?i)\b(USER|SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|GROUP BY|ORDER BY|HAVING|AND|OR|CREATE|DROP|ALTER|TABLE|INDEX|ON|INTO|VALUES|SET|AS|DISTINCT|TYPE|DEFAULT|ORDER|GROUP|LIMIT|SUM|TOTAL|START|END|BEGIN|COMMIT|ROLLBACK|PRIMARY|AUTHORIZATION)\b`)

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
	for _, name := range model.Columns() {
		columns = append(columns, quoteIdentifier(name))
	}
	sql.WriteString(" (")
	sql.WriteString(strings.Join(columns, ","))
	sql.WriteString(") VALUES (")
	var insertVals []string
	var updateValues []string
	if operation == "UPDATE" {
		for _, name := range diff {
			if !util.SliceContains(model.Columns(), name) || name == "id" {
				continue
			}
			prop := model.Properties[name]
			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), prop, true)
				updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
			} else {
				v := util.ToJSONStringVal(name, "NULL", prop, true)
				updateValues = append(updateValues, v)
			}
		}
		for _, name := range model.Columns() {
			prop := model.Properties[name]
			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), prop, true)
				insertVals = append(insertVals, v)
			} else {
				v := util.ToJSONStringVal(name, "NULL", prop, true)
				insertVals = append(insertVals, v)
			}
		}
	} else {
		for _, name := range model.Columns() {
			prop := model.Properties[name]
			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), prop, true)
				if name != "id" {
					updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
				}
				insertVals = append(insertVals, v)
			} else {
				v := util.ToJSONStringVal(name, "NULL", prop, true)
				updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
				insertVals = append(insertVals, v)
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

func toSQL(c internal.DBChangeEvent, model *internal.Schema) (string, error) {
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
		if property.Items != nil && property.Items.Enum != nil {
			return "VARCHAR(64)" // this is an enum but we want to represent it as a string
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

func addNewColumnsSQL(logger logger.Logger, columns []string, s *internal.Schema, db internal.DatabaseSchema) []string {
	var res []string
	for _, column := range columns {
		if ok, _ := db.GetType(s.Table, column); ok {
			logger.Warn("skipping migration for column: %s for table: %s since it already exists", column, s.Table)
			continue
		}
		prop := s.Properties[column]
		var sql strings.Builder
		sql.WriteString("ALTER TABLE ")
		sql.WriteString(quoteIdentifier((s.Table)))
		sql.WriteString(" ADD COLUMN ")
		sql.WriteString(quoteIdentifier(column))
		sql.WriteString(" ")
		sql.WriteString(propTypeToSQLType(prop))
		sql.WriteString(";")
		res = append(res, sql.String())
	}
	return res
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
	var reencode bool
	q := u.Query()
	if !u.Query().Has("application_name") {
		q.Set("application_name", "eds")
		reencode = true
	}
	if util.IsLocalhost(u.Host) && !u.Query().Has("sslmode") {
		q.Set("sslmode", "disable")
		reencode = true
	}
	if reencode {
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}
