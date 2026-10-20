package util

import (
	"fmt"
	"strings"
)

// LedgerTableName records the newest version applied to each row, so events
// that arrive out of order still end up with the same result. Customer tables
// are never touched.
const LedgerTableName = "_eds_row_version"

const ledgerPKSeparator = "|"

// LedgerPKFromKeys builds the ledger key from an event's key values.
func LedgerPKFromKeys(primaryKeys, keys []string) string {
	vals := make([]string, 0, len(primaryKeys))
	for i := range primaryKeys {
		if i < len(keys) {
			vals = append(vals, keys[i])
		}
	}
	return strings.Join(vals, ledgerPKSeparator)
}

// LedgerPKFromObject builds the ledger key from a row, so an upsert and the
// delete for the same row produce the same key.
func LedgerPKFromObject(primaryKeys []string, object map[string]any) string {
	vals := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		vals = append(vals, fmt.Sprintf("%v", object[pk]))
	}
	return strings.Join(vals, ledgerPKSeparator)
}

// LedgerNotNewer builds the check that is true when the ledger already holds a
// version as new as the incoming one. Quoting is passed in so this package does
// not import any driver.
func LedgerNotNewer(quoteIdentifier func(string) string, quoteValue func(any) string, table, pk, version string) string {
	return fmt.Sprintf(
		"EXISTS (SELECT 1 FROM %s l WHERE l.%s=%s AND l.%s=%s AND l.%s>=%s)",
		quoteIdentifier(LedgerTableName),
		quoteIdentifier("table_name"), quoteValue(table),
		quoteIdentifier("pk"), quoteValue(pk),
		quoteIdentifier("mvcc"), quoteValue(version),
	)
}
