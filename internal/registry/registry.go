package registry

import (
	"sort"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
)

type tableToObjectNameMap map[string]string

func sortTable(tables internal.SchemaMap) (internal.SchemaMap, tableToObjectNameMap) {
	kv := make(internal.SchemaMap)
	otm := make(tableToObjectNameMap)
	for object, d := range tables {
		var columns []string
		for name := range d.Properties {
			if util.SliceContains(d.PrimaryKeys, name) {
				continue
			}
			columns = append(columns, name)
		}
		sort.Strings(columns)
		columns = append(d.PrimaryKeys, columns...)
		// d.Columns = columns
		otm[d.Table] = object
		kv[d.Table] = d
	}
	return kv, otm
}
