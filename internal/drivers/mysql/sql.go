package mysql

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

func quoteIdentifier(val string) string {
	return "`" + val + "`"
}

// ledgerNotNewer is true when the ledger already holds a version at least as new
// as the incoming one, i.e. the incoming event is stale.
func ledgerNotNewer(table string, pk string, version string) string {
	return fmt.Sprintf(
		"EXISTS (SELECT 1 FROM %s l WHERE l.%s=%s AND l.%s=%s AND l.%s>=%s)",
		quoteIdentifier(util.LedgerTableName),
		quoteIdentifier("table_name"), quoteValue(table),
		quoteIdentifier("pk"), quoteValue(pk),
		quoteIdentifier("mvcc"), quoteValue(version),
	)
}

// ledgerUpsertSQL advances the ledger high-water mark to the greatest of the
// stored and incoming versions, in the same transaction as the data write.
func ledgerUpsertSQL(table string, pk string, version string) string {
	return fmt.Sprintf(
		"INSERT INTO %[1]s (%[2]s,%[3]s,%[4]s,%[5]s) VALUES (%[6]s,%[7]s,%[8]s,NOW())"+
			" ON DUPLICATE KEY UPDATE %[4]s=IF(VALUES(%[4]s)>%[4]s,VALUES(%[4]s),%[4]s),%[5]s=IF(VALUES(%[4]s)>%[4]s,VALUES(%[5]s),%[5]s);\n",
		quoteIdentifier(util.LedgerTableName),
		quoteIdentifier("table_name"), quoteIdentifier("pk"), quoteIdentifier("mvcc"), quoteIdentifier("updated_at"),
		quoteValue(table), quoteValue(pk), quoteValue(version),
	)
}

// createLedgerTableSQL creates the side high-water-mark table if it does not
// already exist.
func createLedgerTableSQL() string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %[1]s (%[2]s VARCHAR(255) NOT NULL, %[3]s VARCHAR(255) NOT NULL, %[4]s VARCHAR(40) NOT NULL, %[5]s TIMESTAMP NOT NULL, PRIMARY KEY (%[2]s,%[3]s)) CHARACTER SET=utf8mb4;",
		quoteIdentifier(util.LedgerTableName),
		quoteIdentifier("table_name"), quoteIdentifier("pk"), quoteIdentifier("mvcc"), quoteIdentifier("updated_at"),
	)
}

// toSQLFromObject builds the upsert for a row. When version is non-empty the
// upsert is gated on the ledger (streaming apply); an empty version is the
// unguarded bulk-import path.
func toSQLFromObject(operation string, model *internal.Schema, table string, event internal.DBChangeEvent, diff []string, version string) (string, error) {
	o, err := event.GetObject()
	if err != nil {
		return "", err
	}
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(quoteIdentifier(table))
	var columns []string
	for _, name := range model.Columns() {
		columns = append(columns, quoteIdentifier(name))
	}
	sql.WriteString(" (")
	sql.WriteString(strings.Join(columns, ","))
	guarded := version != ""
	if guarded {
		sql.WriteString(") SELECT ")
	} else {
		sql.WriteString(") VALUES (")
	}
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
	if guarded {
		// gate the insert on the ledger so a hard-deleted row is not resurrected
		// by a late event and a stale event cannot clobber a newer row
		pk := util.LedgerPKFromObject(model.PrimaryKeys, o)
		sql.WriteString(" FROM DUAL WHERE NOT ")
		sql.WriteString(ledgerNotNewer(table, pk, version))
	} else {
		sql.WriteString(")")
	}
	// REPLACE INTO gave no way to reject stale events; an upsert keyed on the row
	// lets ON DUPLICATE KEY UPDATE apply the newer values in place
	sql.WriteString(" ON DUPLICATE KEY UPDATE ")
	if len(updateValues) == 0 {
		sql.WriteString(fmt.Sprintf("%[1]s=%[1]s", quoteIdentifier("id")))
	} else {
		sql.WriteString(strings.Join(updateValues, ","))
	}
	sql.WriteString(";\n")
	if guarded {
		sql.WriteString(ledgerUpsertSQL(table, util.LedgerPKFromObject(model.PrimaryKeys, o), version))
	}
	return sql.String(), nil
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
		return toSQLFromObject(c.Operation, model, c.Table, c, c.Diff, version)
	}
}

func propTypeToSQLType(property internal.SchemaProperty, isPrimaryKey bool) string {
	switch property.Type {
	case "string":
		if isPrimaryKey {
			return "VARCHAR(64)"
		}
		if property.Format == "date-time" {
			return "TIMESTAMP"
		}
		return "TEXT"
	case "integer":
		return "BIGINT"
	case "number":
		return "FLOAT"
	case "boolean":
		return "BOOLEAN"
	case "object":
		return "JSON"
	case "array":
		if property.Items != nil && property.Items.Enum != nil {
			return "VARCHAR(64)" // this is an enum but we want to represent it as a string
		}
		return "JSON"
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
		sql.WriteString(propTypeToSQLType(prop, util.SliceContains(s.PrimaryKeys, name)))
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
	} else {
		sql.WriteString("\tPRIMARY KEY (id)")
	}
	sql.WriteString("\n) CHARACTER SET=utf8mb4;\n")
	return sql.String()
}

func addNewColumnsSQL(logger logger.Logger, columns []string, s *internal.Schema, db internal.DatabaseSchema) []string {
	var sqls []string
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
		sql.WriteString(propTypeToSQLType(prop, false))
		sql.WriteString(";")
		sqls = append(sqls, sql.String())
	}
	return sqls
}

func ParseURLToDSN(urlstr string) (string, error) {
	//username:password@protocol(address)/dbname?param=value
	u, err := url.Parse(urlstr)
	if err != nil {
		return "", fmt.Errorf("error parsing url: %w", err)
	}
	vals := u.Query()
	vals.Set("multiStatements", "true")
	var dsn strings.Builder
	if u.User != nil {
		dsn.WriteString(util.ToUserPass(u))
		dsn.WriteString("@")
	}
	dsn.WriteString("tcp(")
	dsn.WriteString(u.Host)
	dsn.WriteString(")")
	dsn.WriteString(u.Path)
	dsn.WriteString("?")
	dsn.WriteString(vals.Encode())
	return dsn.String(), nil
}
