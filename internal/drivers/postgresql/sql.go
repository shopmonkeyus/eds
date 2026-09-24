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

func quoteIdentifier(val string) string {
	return pq.QuoteIdentifier(val)
}

// ledgerNotNewer is true when the ledger already holds a version at least as new
// as the incoming one, i.e. the incoming event is stale.
func ledgerNotNewer(table, pk, version string) string {
	return util.LedgerNotNewer(quoteIdentifier, quoteValue, table, pk, version)
}

// ledgerUpsertSQL advances the ledger high-water mark to the greatest of the
// stored and incoming versions, in the same transaction as the data write.
func ledgerUpsertSQL(table, pk, version string) string {
	return fmt.Sprintf(
		"INSERT INTO %[1]s (%[2]s,%[3]s,%[4]s,%[5]s) VALUES (%[6]s,%[7]s,%[8]s,now())"+
			" ON CONFLICT (%[2]s,%[3]s) DO UPDATE SET %[4]s=EXCLUDED.%[4]s,%[5]s=EXCLUDED.%[5]s WHERE EXCLUDED.%[4]s>%[1]s.%[4]s;\n",
		quoteIdentifier(util.LedgerTableName),
		quoteIdentifier("table_name"), quoteIdentifier("pk"), quoteIdentifier("mvcc"), quoteIdentifier("updated_at"),
		quoteValue(table), quoteValue(pk), quoteValue(version),
	)
}

// createLedgerTableSQL creates the side high-water-mark table if it does not
// already exist. The schema is fixed, so it is a static statement (no runtime
// values) rather than a formatted string.
const createLedgerTableSQL = `CREATE TABLE IF NOT EXISTS "_eds_row_version" ("table_name" VARCHAR(255) NOT NULL, "pk" VARCHAR(255) NOT NULL, "mvcc" VARCHAR(40) NOT NULL, "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL, PRIMARY KEY ("table_name","pk"));`

// conflictClause renders the ON CONFLICT resolution for the upsert.
func conflictClause(updateValues []string) string {
	if len(updateValues) == 0 {
		return " ON CONFLICT (id) DO NOTHING"
	}
	return " ON CONFLICT (id) DO UPDATE SET " + strings.Join(updateValues, ",")
}

// toSQLFromObject builds the upsert for a row. When version is non-empty the
// upsert is gated on the ledger (streaming apply); an empty version is the
// unguarded bulk-import path.
func toSQLFromObject(operation string, model *internal.Schema, table string, o map[string]any, diff []string, version string) string {
	guarded := version != ""
	insertVals, updateValues := util.ColumnValues(quoteIdentifier, quoteValue, operation, model, o, diff)
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(quoteIdentifier(table))
	sql.WriteString(" (")
	sql.WriteString(strings.Join(util.ColumnList(quoteIdentifier, model), ","))
	if guarded {
		sql.WriteString(") SELECT ")
	} else {
		sql.WriteString(") VALUES (")
	}
	sql.WriteString(strings.Join(insertVals, ","))
	if guarded {
		// gate the insert on the ledger so a hard-deleted row is not resurrected
		// by a late event and a stale event cannot clobber a newer row
		pk := util.LedgerPKFromObject(model.PrimaryKeys, o)
		sql.WriteString(" WHERE NOT ")
		sql.WriteString(ledgerNotNewer(table, pk, version))
	} else {
		sql.WriteString(")")
	}
	sql.WriteString(conflictClause(updateValues))
	sql.WriteString(";\n")
	if guarded {
		sql.WriteString(ledgerUpsertSQL(table, util.LedgerPKFromObject(model.PrimaryKeys, o), version))
	}
	return sql.String()
}

func toSQL(c internal.DBChangeEvent, model *internal.Schema) (string, error) {
	primaryKeys := model.PrimaryKeys
	version := util.EventVersion(&c)
	if c.Operation == "DELETE" {
		pk := util.LedgerPKFromKeys(primaryKeys, c.Key)
		var sql strings.Builder
		sql.WriteString("DELETE FROM ")
		sql.WriteString(quoteIdentifier(c.Table))
		sql.WriteString(" WHERE ")
		var predicate []string
		for i, pk := range primaryKeys {
			predicate = append(predicate, fmt.Sprintf("%s=%s", quoteIdentifier(pk), quoteValue(c.Key[i])))
		}
		sql.WriteString(strings.Join(predicate, " AND "))
		// only delete when no newer version has already been applied
		sql.WriteString(" AND NOT ")
		sql.WriteString(ledgerNotNewer(c.Table, pk, version))
		sql.WriteString(";\n")
		sql.WriteString(ledgerUpsertSQL(c.Table, pk, version))
		return sql.String(), nil
	} else {
		o := make(map[string]any)
		if err := json.Unmarshal(c.After, &o); err != nil {
			return "", err
		}
		return toSQLFromObject(c.Operation, model, c.Table, o, c.Diff, version), nil
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

func GetConnectionStringFromURL(urlstr string) (string, error) {
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
