package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
)

var skipFields = map[string]bool{"meta": true}

func sortTable(tables internal.SchemaMap) internal.SchemaMap {
	kv := make(internal.SchemaMap)
	for _, d := range tables {
		for key := range skipFields {
			delete(d.Properties, key)
		}
		var columns []string
		for name := range d.Properties {
			if util.SliceContains(d.PrimaryKeys, name) {
				continue
			}
			columns = append(columns, name)
		}
		sort.Strings(columns)
		columns = append(d.PrimaryKeys, columns...)
		d.Columns = columns
		kv[d.Table] = d
	}
	return kv
}

func save(filename string, schema internal.SchemaMap) error {
	of, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file %s: %w", filename, err)
	}
	defer of.Close()
	enc := json.NewEncoder(of)
	enc.SetIndent("", "  ")
	if err := enc.Encode(schema); err != nil {
		return fmt.Errorf("error encoding schema: %w", err)
	}
	return nil
}
