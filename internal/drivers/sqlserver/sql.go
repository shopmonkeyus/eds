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

// ledger renders this driver's guarded delete and stale-guard predicate.
var ledger = util.SQLLedger{QuoteIdentifier: quoteIdentifier, QuoteValue: quoteValue, UpsertSQL: ledgerUpsertSQL}

// ledgerUpsertSQL advances the ledger high-water mark to the greatest of the
// stored and incoming versions, in the same transaction as the data write.
func ledgerUpsertSQL(table, pk, version string) string {
	return fmt.Sprintf(
		"MERGE %[1]s AS target USING (VALUES(%[2]s,%[3]s,%[4]s)) AS source (%[5]s,%[6]s,%[7]s)"+
			" ON target.%[5]s=source.%[5]s AND target.%[6]s=source.%[6]s"+
			" WHEN MATCHED AND source.%[7]s>target.%[7]s THEN UPDATE SET %[7]s=source.%[7]s,%[8]s=SYSUTCDATETIME()"+
			" WHEN NOT MATCHED THEN INSERT (%[5]s,%[6]s,%[7]s,%[8]s) VALUES (source.%[5]s,source.%[6]s,source.%[7]s,SYSUTCDATETIME());\n",
		quoteIdentifier(util.LedgerTableName),
		quoteValue(table), quoteValue(pk), quoteValue(version),
		quoteIdentifier("table_name"), quoteIdentifier("pk"), quoteIdentifier("mvcc"), quoteIdentifier("updated_at"),
	)
}

// mergeColumns renders the quoted column list, the INSERT values, and the UPDATE
// SET assignments (id is excluded from the latter) for the row.
func mergeColumns(model *internal.Schema, object map[string]any) (insertColumns []string, insertValues []string, updateValues []string) {
	for _, name := range model.Columns() {
		v := columnValueOrNull(name, model.Properties[name], object)
		insertColumns = append(insertColumns, quoteIdentifier(name))
		insertValues = append(insertValues, v)
		if name != "id" {
			updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
		}
	}
	return insertColumns, insertValues, updateValues
}

// mergeSource renders the USING source between "USING (" and the ON clause. When
// guarded it LEFT JOINs the ledger so the source always yields exactly one row
// (even with no ledger entry yet) and both merge branches can be gated on the
// stored version — this is what stops a hard-deleted row from being resurrected by
// a late event and a stale event from clobbering a newer row.
func mergeSource(model *internal.Schema, table string, object map[string]any, guarded bool) string {
	var sql strings.Builder
	if guarded {
		pk := util.LedgerPKFromObject(model.PrimaryKeys, object)
		sql.WriteString("SELECT s.")
		sql.WriteString(quoteIdentifier("id"))
		sql.WriteString(", l.")
		sql.WriteString(quoteIdentifier("mvcc"))
		sql.WriteString(" AS __mvcc FROM (VALUES(")
		sql.WriteString(quoteValue(object["id"]))
		sql.WriteString(")) AS s (")
		sql.WriteString(quoteIdentifier("id"))
		sql.WriteString(") LEFT JOIN ")
		sql.WriteString(quoteIdentifier(util.LedgerTableName))
		sql.WriteString(" l ON l.")
		sql.WriteString(quoteIdentifier("table_name"))
		sql.WriteString("=")
		sql.WriteString(quoteValue(table))
		sql.WriteString(" AND l.")
		sql.WriteString(quoteIdentifier("pk"))
		sql.WriteString("=")
		sql.WriteString(quoteValue(pk))
		sql.WriteString(") AS source")
	} else {
		sql.WriteString("VALUES(")
		sql.WriteString(quoteValue(object["id"]))
		sql.WriteString(")) AS source (")
		sql.WriteString(quoteIdentifier("id"))
		sql.WriteString(")")
	}
	return sql.String()
}

// toSQLFromObject builds the upsert for a row. When version is non-empty the
// upsert is gated on the ledger (streaming apply); an empty version is the
// unguarded bulk-import path.
func toSQLFromObject(model *internal.Schema, table string, object map[string]any, version string) string {
	insertColumns, insertValues, updateValues := mergeColumns(model, object)

	guarded := version != ""
	var guard string
	if guarded {
		guard = fmt.Sprintf(" AND (source.__mvcc IS NULL OR %s>source.__mvcc)", quoteValue(version))
	}

	var sql strings.Builder
	sql.WriteString("MERGE ")
	sql.WriteString(quoteIdentifier(table))
	sql.WriteString(" AS target USING (")
	sql.WriteString(mergeSource(model, table, object, guarded))
	sql.WriteString(" ON target.")
	sql.WriteString(quoteIdentifier("id"))
	sql.WriteString("=source.")
	sql.WriteString(quoteIdentifier("id"))
	if len(updateValues) > 0 {
		sql.WriteString(" WHEN MATCHED")
		sql.WriteString(guard)
		sql.WriteString(" THEN UPDATE SET ")
		sql.WriteString(strings.Join(updateValues, ","))
	}
	sql.WriteString(" WHEN NOT MATCHED")
	sql.WriteString(guard)
	sql.WriteString(" THEN INSERT (")
	sql.WriteString(strings.Join(insertColumns, ","))
	sql.WriteString(") VALUES (")
	sql.WriteString(strings.Join(insertValues, ","))
	sql.WriteString(");") // must be terminated for merge to work

	if guarded {
		sql.WriteString("\n")
		sql.WriteString(ledgerUpsertSQL(table, util.LedgerPKFromObject(model.PrimaryKeys, object), version))
	}

	return sql.String()
}

func toSQL(c internal.DBChangeEvent, model *internal.Schema) (string, error) {
	version := util.EventVersion(&c)
	if c.Operation == "DELETE" {
		return ledger.DeleteSQL(c.Table, model.PrimaryKeys, c.Key, version), nil
	}
	o := make(map[string]any)
	if err := json.Unmarshal(c.After, &o); err != nil {
		return "", err
	}
	return toSQLFromObject(model, c.Table, o, version), nil
}

// createLedgerTableSQL creates the side high-water-mark table if it does not
// already exist. The schema is fixed, so it is a static statement (no runtime
// values) rather than a formatted string.
const createLedgerTableSQL = "IF OBJECT_ID(N'[_eds_row_version]', N'U') IS NULL CREATE TABLE [_eds_row_version] ([table_name] VARCHAR(255) NOT NULL, [pk] VARCHAR(255) NOT NULL, [mvcc] VARCHAR(40) NOT NULL, [updated_at] DATETIME2 NOT NULL, PRIMARY KEY ([table_name],[pk]));"

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
