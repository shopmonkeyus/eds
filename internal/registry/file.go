package registry

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
)

type FileRegistry struct {
	schema internal.SchemaMap
}

var _ internal.SchemaRegistry = (*FileRegistry)(nil)

// GetLatestSchema returns the latest schema for all tables.
func (r *FileRegistry) GetLatestSchema() (internal.SchemaMap, error) {
	return r.schema, nil
}

// GetSchema returns the schema for a table at a specific version.
func (r *FileRegistry) GetSchema(table string, version string) (*internal.Schema, error) {
	return r.schema[table], nil
}

// Save the latest schema to a file.
func (r *FileRegistry) Save(filename string) error {
	return save(filename, r.schema)
}

// NewFileRegistry creates a new schema registry from a file. This implementation doesn't support versioning.
func NewFileRegistry(schemaFile string) (internal.SchemaRegistry, error) {
	if !util.Exists(schemaFile) {
		return nil, fmt.Errorf("schema file does not exist: %s", schemaFile)
	}
	of, err := os.Open(schemaFile)
	if err != nil {
		return nil, fmt.Errorf("error opening schema file: %w", err)
	}
	defer of.Close()
	var registry FileRegistry
	registry.schema = make(internal.SchemaMap)
	dec := json.NewDecoder(of)
	if err := dec.Decode(&registry.schema); err != nil {
		return nil, fmt.Errorf("error decoding schema file: %w", err)
	}
	of.Close()
	registry.schema = sortTable(registry.schema)
	return &registry, nil
}
