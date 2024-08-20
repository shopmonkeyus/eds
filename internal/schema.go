package internal

type ItemsType struct {
	Type   string   `json:"type"`
	Enum   []string `json:"enum,omitempty"`
	Format string   `json:"format,omitempty"`
}

// SchemaProperty is the property metaedata of a schema.
type SchemaProperty struct {
	Type                 string     `json:"type"`
	Format               string     `json:"format,omitempty"`
	Nullable             bool       `json:"nullable,omitempty"`
	Items                *ItemsType `json:"items,omitempty"`
	AdditionalProperties *bool      `json:"additionalProperties,omitempty"`
	Comment              *string    `json:"$comment,omitempty"`
	Deprecated           *bool      `json:"deprecated,omitempty"`
}

// Schema is the schema metadata for a table.
type Schema struct {
	Properties   map[string]SchemaProperty `json:"properties"`
	Required     []string                  `json:"required"`
	PrimaryKeys  []string                  `json:"primaryKeys"`
	Table        string                    `json:"table"`
	ModelVersion string                    `json:"modelVersion"`

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

	// GetTableVersion gets the current version of the schema for a table.
	GetTableVersion(table string, version string) (bool, string, error)

	// SetTableVersion sets the version of a table to a specific version.
	SetTableVersion(table string, version string) error

	// Close will shutdown the schema optionally flushing any caches.
	Close() error
}

// SchemaValidator is the interface for a schema validator.
type SchemaValidator interface {
	// Validate the event against the schema. Returns true if a schema is found for the table, true if the event is valid, the path transformed and an error if one occurs.
	Validate(event DBChangeEvent) (bool, bool, string, error)
}
