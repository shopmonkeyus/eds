package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/tracker"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const (
	prefix               = "registry:"
	defaultCacheDuration = time.Hour * 24
)

type APIRegistry struct {
	logger  logger.Logger
	apiURL  string
	schema  internal.SchemaMap
	objects tableToObjectNameMap
	tracker *tracker.Tracker
	cache   util.Cache
	once    sync.Once
}

var _ internal.SchemaRegistry = (*APIRegistry)(nil)

func (r *APIRegistry) Close() error {
	r.logger.Trace("closing")
	r.once.Do(func() {
		if err := r.cache.Close(); err != nil {
			r.logger.Error("error closing cache: %s", err)
		}
	})
	r.logger.Trace("closed")
	return nil
}

func (r *APIRegistry) getSchemaCacheKey(table string, version string) string {
	return prefix + table + "-" + version
}

func (r *APIRegistry) getVersionCacheKey(table string) string {
	return prefix + table + ":version"
}

// GetLatestSchema returns the latest schema for all tables.
func (r *APIRegistry) GetLatestSchema() (internal.SchemaMap, error) {
	return r.schema, nil
}

// GetTableVersion gets the current version of the schema for a table.
func (r *APIRegistry) GetTableVersion(table string) (bool, string, error) {
	key := r.getVersionCacheKey(table)
	found, val, err := r.cache.Get(key)
	if err != nil {
		return false, "", fmt.Errorf("error fetching version from cache: %s", err)
	}
	if found {
		return true, val.(string), nil
	}
	if r.tracker != nil {
		found, val, err := r.tracker.GetKey(key)
		if err != nil {
			return false, "", fmt.Errorf("error fetching version from tracker: %s", err)
		}
		if found {
			if err := r.cache.Set(key, val, defaultCacheDuration); err != nil {
				return false, "", fmt.Errorf("error setting key %s in cache: %s", key, err)
			}
			return true, val, nil
		}
	}
	return false, "", nil
}

// SetTableVersion sets the version of a table to a specific version.
func (r *APIRegistry) SetTableVersion(table string, version string) error {
	key := r.getVersionCacheKey(table)
	if err := r.cache.Set(key, version, defaultCacheDuration); err != nil {
		return fmt.Errorf("error setting key %s in cache: %s", key, err)
	}
	if r.tracker != nil {
		if err := r.tracker.SetKey(key, version, 0); err != nil {
			return fmt.Errorf("error setting key %s in tracker: %s", key, err)
		}
	}
	r.logger.Trace("set table: %s version: %s", table, version)
	return nil
}

// GetSchema returns the schema for a table at a specific version.
func (r *APIRegistry) GetSchema(table string, version string) (*internal.Schema, error) {
	key := r.getSchemaCacheKey(table, version)

	// fast path is to check the in memory cache first
	found, val, err := r.cache.Get(key)

	if err != nil {
		return nil, fmt.Errorf("error fetching schema from cache: %s", err)
	}

	if found {
		return val.(*internal.Schema), nil
	}

	if r.tracker != nil {
		// now check the tracker for the data
		found, valstr, err := r.tracker.GetKey(key)

		if err != nil {
			return nil, fmt.Errorf("error fetching schema from tracker: %s", err)
		}

		if found {
			var schema internal.Schema
			if err := json.Unmarshal([]byte(valstr), &schema); err != nil {
				return nil, fmt.Errorf("error decoding schema for table: %s, modelVersion: %s: %s", table, version, err)
			}
			if err := r.cache.Set(key, &schema, defaultCacheDuration); err != nil {
				return nil, fmt.Errorf("error setting key %s in cache: %s", key, err)
			}
			return &schema, nil
		}
	}

	// we have to now fallback to the API to get the data
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

	// save it in the cache and tracker
	if err := r.cache.Set(key, &schema, defaultCacheDuration); err != nil {
		return nil, fmt.Errorf("error setting key %s in cache: %s", key, err)
	}
	if r.tracker != nil {
		if err := r.tracker.SetKey(key, util.JSONStringify(schema), 0); err != nil {
			return nil, fmt.Errorf("error setting key %s in tracker: %s", key, err)
		}
	}
	r.logger.Trace("get schema returned")

	return &schema, nil
}

type errorResponse struct {
	Message string `json:"message"`
}

// NewAPIRegistry creates a new schema registry from the API. This implementation doesn't support versioning.
func NewAPIRegistry(ctx context.Context, logger logger.Logger, apiURL string, tracker *tracker.Tracker) (internal.SchemaRegistry, error) {
	var registry APIRegistry
	req, err := http.NewRequest("GET", apiURL+"/v3/schema", nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %s", err)
	}
	retry := util.NewHTTPRetry(req, util.WithLogger(logger))
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
	registry.logger = logger.WithPrefix("[tracker]")
	registry.cache = util.NewCache(ctx, time.Hour)
	registry.tracker = tracker
	registry.apiURL = apiURL
	registry.schema = make(internal.SchemaMap)
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&registry.schema); err != nil {
		return nil, fmt.Errorf("error decoding schema: %s", err)
	}
	registry.schema, registry.objects = sortTable(registry.schema)
	// save all the data into the tracker and cache the latest
	for _, schema := range registry.schema {
		key := registry.getSchemaCacheKey(schema.Table, schema.ModelVersion)
		if tracker != nil {
			if err := tracker.SetKey(key, util.JSONStringify(schema), 0); err != nil {
				return nil, fmt.Errorf("error setting key %s in tracker: %s", key, err)
			}
		}
		if err := registry.cache.Set(key, schema, 0); err != nil {
			return nil, fmt.Errorf("error setting key %s in cache: %s", key, err)
		}
	}
	return &registry, nil
}
