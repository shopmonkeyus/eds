package registry

import (
	"github.com/shopmonkeyus/eds/internal"
)

type tableToObjectNameMap map[string]string

func sortTable(tables internal.SchemaMap) (internal.SchemaMap, tableToObjectNameMap) {
	kv := make(internal.SchemaMap)
	otm := make(tableToObjectNameMap)
	for object, d := range tables {
		otm[d.Table] = object
		kv[d.Table] = d
	}
	return kv, otm
}
