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

// ledger renders this driver's guarded delete, row upsert, and stale-guard predicate.
var ledger = util.SQLLedger{
	QuoteIdentifier: quoteIdentifier,
	QuoteValue:      quoteValue,
	UpsertSQL:       ledgerUpsertSQL,
	GuardPrefix:     " FROM DUAL WHERE NOT ",
	ConflictClause:  duplicateKeyClause,
}

// ledgerUpsertSQL advances the ledger high-water mark to the greatest of the
// stored and incoming versions, in the same transaction as the data write.
func ledgerUpsertSQL(table, pk, version string) string {
	return fmt.Sprintf(
		"INSERT INTO %[1]s (%[2]s,%[3]s,%[4]s,%[5]s) VALUES (%[6]s,%[7]s,%[8]s,NOW())"+
			" ON DUPLICATE KEY UPDATE %[4]s=IF(VALUES(%[4]s)>%[4]s,VALUES(%[4]s),%[4]s),%[5]s=IF(VALUES(%[4]s)>%[4]s,VALUES(%[5]s),%[5]s);\n",
		quoteIdentifier(util.LedgerTableName),
		quoteIdentifier("table_name"), quoteIdentifier("pk"), quoteIdentifier("mvcc"), quoteIdentifier("updated_at"),
		quoteValue(table), quoteValue(pk), quoteValue(version),
	)
}

// createLedgerTableSQL creates the side high-water-mark table if it does not
// already exist. The schema is fixed, so it is a static statement (no runtime
// values) rather than a formatted string.
const createLedgerTableSQL = "CREATE TABLE IF NOT EXISTS `_eds_row_version` (`table_name` VARCHAR(255) NOT NULL, `pk` VARCHAR(255) NOT NULL, `mvcc` VARCHAR(40) NOT NULL, `updated_at` TIMESTAMP NOT NULL, PRIMARY KEY (`table_name`,`pk`)) CHARACTER SET=utf8mb4;"

// duplicateKeyClause renders the ON DUPLICATE KEY UPDATE resolution for the upsert.
func duplicateKeyClause(updateValues []string) string {
	if len(updateValues) == 0 {
		return fmt.Sprintf(" ON DUPLICATE KEY UPDATE %[1]s=%[1]s", quoteIdentifier("id"))
	}
	return " ON DUPLICATE KEY UPDATE " + strings.Join(updateValues, ",")
}

// toSQLFromObject builds the upsert for a row. Unlike postgres it must first pull
// the object off the event; the shared builder renders the rest. REPLACE INTO gave
// no way to reject stale events, so this uses an upsert keyed on the row whose
// ON DUPLICATE KEY UPDATE applies newer values in place.
func toSQLFromObject(operation string, model *internal.Schema, table string, event internal.DBChangeEvent, diff []string, version string) (string, error) {
	o, err := event.GetObject()
	if err != nil {
		return "", err
	}
	return ledger.RowUpsertSQL(operation, model, table, o, diff, version), nil
}

func toSQL(c internal.DBChangeEvent, model *internal.Schema) (string, error) {
	version := util.EventVersion(&c)
	if c.Operation == "DELETE" {
		return ledger.DeleteSQL(c.Table, model.PrimaryKeys, c.Key, version), nil
	}
	return toSQLFromObject(c.Operation, model, c.Table, c, c.Diff, version)
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
