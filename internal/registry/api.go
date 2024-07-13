package registry

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/shopmonkeyus/eds-server/internal"
)

type APIRegistry struct {
	schema internal.SchemaMap
}

var _ internal.SchemaRegistry = (*APIRegistry)(nil)

// GetLatestSchema returns the latest schema for all tables.
func (r *APIRegistry) GetLatestSchema() (internal.SchemaMap, error) {
	return r.schema, nil
}

// GetSchema returns the schema for a table at a specific version.
func (r *APIRegistry) GetSchema(table string, version string) (*internal.Schema, error) {
	return r.schema[table], nil
}

// Save the latest schema to a file.
func (r *APIRegistry) Save(filename string) error {
	return save(filename, r.schema)
}

// NewAPIRegistry creates a new schema registry from the API. This implementation doesn't support versioning.
func NewAPIRegistry(apiURL string, apiKey string) (internal.SchemaRegistry, error) {
	var registry FileRegistry
	resp, err := http.Get(apiURL + "/v3/schema")
	if err != nil {
		return nil, fmt.Errorf("error fetching schema: %s", err)
	}
	defer resp.Body.Close()
	registry.schema = make(internal.SchemaMap)
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&registry.schema); err != nil {
		return nil, fmt.Errorf("error decoding schema: %s", err)
	}
	registry.schema = sortTable(registry.schema)
	return &registry, nil
}
