package util

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/go-common/logger"
)

// QuoteIdentifier quotes an identifier with double quotes
func QuoteIdentifier(name string) string {
	return `"` + name + `"`
}

// QuoteStringIdentifiers quotes a slice of identifiers with double quotes
func QuoteStringIdentifiers(vals []string) []string {
	res := make([]string, len(vals))
	for i, val := range vals {
		res[i] = QuoteIdentifier(val)
	}
	return res
}

// OffendingSQLLog is the log format used to surface the batch that failed to
// execute, so the exact SQL can be inspected in customer logs.
const OffendingSQLLog = "offending sql: %s"

// ExecPendingInTx runs a pre-built batch of statements in a single transaction,
// rolling back on any failure and logging the offending SQL. Consolidating this
// here keeps the streaming apply identical across every SQL driver.
func ExecPendingInTx(ctx context.Context, db *sql.DB, log logger.Logger, sql string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to start transaction: %w", err)
	}
	var success bool
	defer func() {
		if !success {
			tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, sql); err != nil {
		log.Error(OffendingSQLLog, sql)
		return fmt.Errorf("unable to execute sql: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}
	success = true
	return nil
}

// PendingWrite is a single buffered statement tagged with its mvcc version so a
// batch can be applied in version order.
type PendingWrite struct {
	Version string
	SQL     string
}

// FlushPendingWrites sorts the buffered writes by version, concatenates them and
// executes the batch in a single transaction. It is a no-op when there is
// nothing pending. Shared so the streaming apply stays identical across drivers.
func FlushPendingWrites(ctx context.Context, db *sql.DB, log logger.Logger, writes []PendingWrite) error {
	if len(writes) == 0 {
		return nil
	}
	// apply in version order so in-batch reorders resolve correctly; the
	// ledger still guards cross-batch and redelivery reordering
	slices.SortFunc(writes, func(a, b PendingWrite) int {
		return strings.Compare(a.Version, b.Version)
	})
	var pending strings.Builder
	for _, w := range writes {
		pending.WriteString(w.SQL)
	}
	return ExecPendingInTx(ctx, db, log, pending.String())
}

// SQLExecuter returns a wrapper around a SQL database connection that can execute SQL statements or log them in dry-run mode
func SQLExecuter(ctx context.Context, log logger.Logger, db *sql.DB, dryRun bool) func(sql string) error {
	return func(sql string) error {
		if dryRun {
			log.Info("[dry-run] %s", sql)
			return nil
		}
		log.Debug("executing: %s", strings.TrimRight(sql, "\n"))
		if _, err := db.ExecContext(ctx, sql); err != nil {
			return err
		}
		return nil
	}
}

func isEmptyVal(val string) bool {
	return val == "''" || val == "" || val == "NULL" || val == "null"
}

// ToJSONStringVal returns a JSON string value checking for empty string and converting it to '{}'
func ToJSONStringVal(name string, val string, prop internal.SchemaProperty, quoteScalar bool) string {
	if prop.IsArrayOrJSON() && prop.IsNotNull() && isEmptyVal(val) {
		switch prop.Type {
		case "array":
			return "'[]'"
		case "object":
			return "'{}'"
		}
	}
	if quoteScalar {
		return quoteJSONScalar(val, prop)
	}
	return val
}

// numbers and booleans must be quoted for JSON fields in certain databases
var scalarValue = regexp.MustCompile(`^([+-]?([0-9]*[.])?[0-9]+)|(true|false)$`)

// quoteJSONScalar will attempt to quote a JSON scalar value if it is a number or boolean
func quoteJSONScalar(val string, prop internal.SchemaProperty) string {
	if prop.Type == "object" && scalarValue.MatchString(val) {
		return "'" + val + "'"
	}
	return val
}

// ToUserPass returns a user:pass string from a URL
func ToUserPass(u *url.URL) string {
	var dsn strings.Builder
	user := u.User.Username()
	pass, ok := u.User.Password()
	dsn.WriteString(user)
	if ok {
		dsn.WriteString(":")
		dsn.WriteString(pass)
	}
	return dsn.String()
}

// DropTable drops the table if it exists
func DropTable(ctx context.Context, logger logger.Logger, db *sql.DB, table string) error {
	sql := "DROP TABLE IF EXISTS " + table
	if _, err := db.ExecContext(ctx, sql); err != nil {
		return err
	}
	return nil
}

// CreateLedgerTable creates the version high-water-mark table using the driver's
// own DDL (the schema is fixed per driver, so the DDL is a static const).
func CreateLedgerTable(ctx context.Context, db *sql.DB, ddl string) error {
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("unable to create version ledger table: %w", err)
	}
	return nil
}

// SQLLedger bundles a driver's quoting and its ledger-upsert renderer so the
// guarded DELETE and row-upsert paths — identical across drivers apart from
// those — live in one place. UpsertSQL stays per-driver because the ledger upsert
// dialect genuinely differs (ON CONFLICT / ON DUPLICATE KEY / MERGE).
//
// GuardPrefix and ConflictClause are only used by RowUpsertSQL (the postgres and
// mysql upsert shape); SQL Server builds its row upsert as a MERGE and leaves
// them unset.
type SQLLedger struct {
	QuoteIdentifier func(string) string
	QuoteValue      func(any) string
	UpsertSQL       func(table, pk, version string) string
	GuardPrefix     string
	ConflictClause  func(updateValues []string) string
}

// NotNewer renders the stale-guard predicate for this driver.
func (l SQLLedger) NotNewer(table, pk, version string) string {
	return LedgerNotNewer(l.QuoteIdentifier, l.QuoteValue, table, pk, version)
}

// RowUpsertSQL builds the row upsert for the postgres/mysql shape. When version
// is non-empty the insert is gated on the ledger (streaming apply) and the ledger
// high-water mark is advanced; an empty version is the unguarded bulk-import path.
func (l SQLLedger) RowUpsertSQL(operation string, model *internal.Schema, table string, o map[string]any, diff []string, version string) string {
	guarded := version != ""
	insertVals, updateValues := ColumnValues(l.QuoteIdentifier, l.QuoteValue, operation, model, o, diff)
	pk := LedgerPKFromObject(model.PrimaryKeys, o)
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(l.QuoteIdentifier(table))
	sql.WriteString(" (")
	sql.WriteString(strings.Join(ColumnList(l.QuoteIdentifier, model), ","))
	if guarded {
		sql.WriteString(") SELECT ")
	} else {
		sql.WriteString(") VALUES (")
	}
	sql.WriteString(strings.Join(insertVals, ","))
	if guarded {
		// gate the insert on the ledger so a hard-deleted row is not resurrected
		// by a late event and a stale event cannot clobber a newer row
		sql.WriteString(l.GuardPrefix)
		sql.WriteString(l.NotNewer(table, pk, version))
	} else {
		sql.WriteString(")")
	}
	sql.WriteString(l.ConflictClause(updateValues))
	sql.WriteString(";\n")
	if guarded {
		sql.WriteString(l.UpsertSQL(table, pk, version))
	}
	return sql.String()
}

// DeleteSQL builds a DELETE gated on the ledger, followed by the ledger
// high-water-mark upsert, in the same statement batch.
func (l SQLLedger) DeleteSQL(table string, primaryKeys, keys []string, version string) string {
	pk := LedgerPKFromKeys(primaryKeys, keys)
	var sql strings.Builder
	sql.WriteString("DELETE FROM ")
	sql.WriteString(l.QuoteIdentifier(table))
	sql.WriteString(" WHERE ")
	var predicate []string
	for i, name := range primaryKeys {
		predicate = append(predicate, fmt.Sprintf("%s=%s", l.QuoteIdentifier(name), l.QuoteValue(keys[i])))
	}
	sql.WriteString(strings.Join(predicate, " AND "))
	// only delete when no newer version has already been applied
	sql.WriteString(" AND NOT ")
	sql.WriteString(l.NotNewer(table, pk, version))
	sql.WriteString(";\n")
	sql.WriteString(l.UpsertSQL(table, pk, version))
	return sql.String()
}

// The row-building helpers below are shared by the SQL drivers whose upsert shape
// is identical apart from how identifiers and values are quoted (postgres, mysql).
// Each takes the driver's own quoting functions so the generated SQL is unchanged.

// ColumnList returns the quoted column identifiers for the model, in column order.
func ColumnList(quoteIdentifier func(string) string, model *internal.Schema) []string {
	var columns []string
	for _, name := range model.Columns() {
		columns = append(columns, quoteIdentifier(name))
	}
	return columns
}

// RowInsertValues renders every column's value for the INSERT tuple.
func RowInsertValues(quoteValue func(any) string, model *internal.Schema, o map[string]any) []string {
	var insertVals []string
	for _, name := range model.Columns() {
		prop := model.Properties[name]
		if val, ok := o[name]; ok {
			insertVals = append(insertVals, ToJSONStringVal(name, quoteValue(val), prop, true))
		} else {
			insertVals = append(insertVals, ToJSONStringVal(name, "NULL", prop, true))
		}
	}
	return insertVals
}

// UpdateDiffValues builds the SET assignments for an UPDATE from its diff list.
func UpdateDiffValues(quoteIdentifier func(string) string, quoteValue func(any) string, model *internal.Schema, o map[string]any, diff []string) []string {
	var updateValues []string
	for _, name := range diff {
		if !SliceContains(model.Columns(), name) || name == "id" {
			continue
		}
		prop := model.Properties[name]
		if val, ok := o[name]; ok {
			v := ToJSONStringVal(name, quoteValue(val), prop, true)
			updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
		} else {
			v := ToJSONStringVal(name, "NULL", prop, true)
			updateValues = append(updateValues, v)
		}
	}
	return updateValues
}

// InsertRowValues builds the INSERT tuple and the full SET assignments for a
// non-UPDATE upsert, where every column is written.
func InsertRowValues(quoteIdentifier func(string) string, quoteValue func(any) string, model *internal.Schema, o map[string]any) (insertVals []string, updateValues []string) {
	for _, name := range model.Columns() {
		prop := model.Properties[name]
		if val, ok := o[name]; ok {
			v := ToJSONStringVal(name, quoteValue(val), prop, true)
			if name != "id" {
				updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
			}
			insertVals = append(insertVals, v)
		} else {
			v := ToJSONStringVal(name, "NULL", prop, true)
			updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
			insertVals = append(insertVals, v)
		}
	}
	return insertVals, updateValues
}

// ColumnValues returns the INSERT tuple and SET assignments for the row.
func ColumnValues(quoteIdentifier func(string) string, quoteValue func(any) string, operation string, model *internal.Schema, o map[string]any, diff []string) (insertVals []string, updateValues []string) {
	if operation == "UPDATE" {
		return RowInsertValues(quoteValue, model, o), UpdateDiffValues(quoteIdentifier, quoteValue, model, o, diff)
	}
	return InsertRowValues(quoteIdentifier, quoteValue, model, o)
}
