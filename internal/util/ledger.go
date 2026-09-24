package util

import (
	"fmt"
	"strings"
)

// LedgerTableName is the side high-water-mark table that records the last-applied
// mvcc version per row so that apply can stay convergent regardless of the order
// events arrive in. Mirrored customer tables are never touched.
const LedgerTableName = "_eds_row_version"

const ledgerPKSeparator = "|"

// LedgerPKFromKeys builds the ledger primary-key string from an event's key
// tuple, matching the columns the DELETE predicate is built from.
func LedgerPKFromKeys(primaryKeys []string, keys []string) string {
	vals := make([]string, 0, len(primaryKeys))
	for i := range primaryKeys {
		if i < len(keys) {
			vals = append(vals, keys[i])
		}
	}
	return strings.Join(vals, ledgerPKSeparator)
}

// LedgerPKFromObject builds the ledger primary-key string from a row object so
// an upsert and its matching delete resolve to the same ledger key.
func LedgerPKFromObject(primaryKeys []string, object map[string]any) string {
	vals := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		vals = append(vals, fmt.Sprintf("%v", object[pk]))
	}
	return strings.Join(vals, ledgerPKSeparator)
}
