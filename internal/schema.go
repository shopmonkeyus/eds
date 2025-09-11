package internal

import (
	"sort"
)

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

func (p SchemaProperty) IsNotNull() bool {
	return !p.Nullable || p.Type == "array"
}

func (p SchemaProperty) IsArrayOrJSON() bool {
	return p.Type == "object" || p.Type == "array"
}

// Schema is the schema metadata for a table.
type Schema struct {
	Properties   map[string]SchemaProperty `json:"properties"`
	Required     []string                  `json:"required"`
	PrimaryKeys  []string                  `json:"primaryKeys"`
	Table        string                    `json:"table"`
	ModelVersion string                    `json:"modelVersion"`

	columns []string
}

func sliceContains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

// Columns returns the columns for a given schema
func (s *Schema) Columns() []string {
	if s.columns != nil {
		return s.columns
	}
	var columns []string
	for name := range s.Properties {
		if sliceContains(s.PrimaryKeys, name) {
			continue
		}
		columns = append(columns, name)
	}
	sort.Strings(columns)
	columns = append(s.PrimaryKeys, columns...)
	s.columns = columns
	return s.columns
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
	GetTableVersion(table string) (bool, string, error)

	// SetTableVersion sets the version of a table to a specific version.
	SetTableVersion(table string, version string) error

	// GetLatestModelVersion returns the latest model version for a table.
	GetLatestModelVersion(table string) (string, error)

	// Close will shutdown the schema optionally flushing any caches.
	Close() error
}

// SchemaValidator is the interface for a schema validator.
type SchemaValidator interface {
	// Validate the event against the schema. Returns true if a schema is found for the table, true if the event is valid, the path transformed and an error if one occurs.
	Validate(event DBChangeEvent) (bool, bool, string, error)
}

// DatabaseSchema is a map of table names to a map of column names to column types.
type DatabaseSchema map[string]map[string]string

func (d DatabaseSchema) Columns(table string) []string {
	if t, ok := d[table]; ok {
		var columns []string
		for name := range t {
			columns = append(columns, name)
		}
		sort.Strings(columns)
		return columns
	}
	return []string{}
}

func (d DatabaseSchema) GetType(table string, column string) (bool, string) {
	if t, ok := d[table]; ok {
		if v, ok := t[column]; ok {
			return true, v
		}
	}
	return false, ""
}
