package internal

// SchemaProperty is the property metaedata of a schema.
type SchemaProperty struct {
	Type     string `json:"type"`
	Format   string `json:"format"`
	Nullable bool   `json:"nullable"`
}

// Schema is the schema metadata for a table.
type Schema struct {
	Properties  map[string]SchemaProperty `json:"properties"`
	Required    []string                  `json:"required"`
	PrimaryKeys []string                  `json:"primaryKeys"`
	Table       string                    `json:"table"`

	Columns []string `json:"-"`
}

// SchemaMap is a map of table names to schemas.
type SchemaMap map[string]*Schema

// SchemaRegistry is the interface for a schema registry.
type SchemaRegistry interface {

	// GetLatestSchema returns the latest schema for all tables.
	GetLatestSchema() (SchemaMap, error)

	// GetSchema returns the schema for a table at a specific version.
	GetSchema(table string, version string) (*Schema, error)

	// Save the latest schema to a file.
	Save(filename string) error
}
