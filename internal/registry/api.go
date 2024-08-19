package registry

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
)

type APIRegistry struct {
	apiURL  string
	schema  internal.SchemaMap
	objects tableToObjectNameMap
	cache   map[string]*internal.Schema // table + version -> schema
	lock    sync.RWMutex
}

var _ internal.SchemaRegistry = (*APIRegistry)(nil)

// GetLatestSchema returns the latest schema for all tables.
func (r *APIRegistry) GetLatestSchema() (internal.SchemaMap, error) {
	return r.schema, nil
}

// GetSchema returns the schema for a table at a specific version.
func (r *APIRegistry) GetSchema(table string, version string) (*internal.Schema, error) {
	if entry, ok := r.schema[table]; ok {
		if entry.ModelVersion == version {
			return entry, nil
		}
		key := table + "-" + version // cache key
		r.lock.RLock()
		val := r.cache[key]
		r.lock.RUnlock()
		if val != nil {
			return val, nil
		}
		object := r.objects[table]
		resp, err := http.Get(r.apiURL + "/v3/schema/" + object + "/" + version)
		if err != nil {
			return nil, fmt.Errorf("error fetching schema: %s", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			buf, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("error fetching schema for table: %s, modelVersion: %s. status code was: %d, %s", table, version, resp.StatusCode, string(buf))
		}
		var schema internal.Schema
		dec := json.NewDecoder(resp.Body)
		if err := dec.Decode(&schema); err != nil {
			return nil, fmt.Errorf("error decoding schema for table: %s, modelVersion: %s: %s", table, version, err)
		}
		r.lock.Lock()
		r.cache[key] = &schema
		r.lock.Unlock()
		return &schema, nil
	}
	return r.schema[table], nil // worse case fall back to the latest
}

// Save the latest schema to a file.
func (r *APIRegistry) Save(filename string) error {
	return save(filename, r.schema)
}

type errorResponse struct {
	Message string `json:"message"`
}

// NewAPIRegistry creates a new schema registry from the API. This implementation doesn't support versioning.
func NewAPIRegistry(apiURL string) (internal.SchemaRegistry, error) {
	var registry APIRegistry
	req, err := http.NewRequest("GET", apiURL+"/v3/schema", nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %s", err)
	}
	retry := util.NewHTTPRetry(req)
	resp, err := retry.Do()
	if err != nil {
		return nil, fmt.Errorf("error fetching schema: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var errResponse errorResponse
		buf, _ := io.ReadAll(resp.Body)
		json.Unmarshal(buf, &errResponse)
		if errResponse.Message != "" {
			return nil, fmt.Errorf("error fetching schema: %s", errResponse.Message)
		}
		return nil, fmt.Errorf("error fetching schema: %d: %s", resp.StatusCode, string(buf))
	}
	registry.apiURL = apiURL
	registry.schema = make(internal.SchemaMap)
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&registry.schema); err != nil {
		return nil, fmt.Errorf("error decoding schema: %s", err)
	}
	registry.schema, registry.objects = sortTable(registry.schema)
	return &registry, nil
}
