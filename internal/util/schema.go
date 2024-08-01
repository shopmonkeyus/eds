package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"strings"

	js "github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/shopmonkeyus/eds-server/internal"
)

type SchemaValidator struct {
	compiler *js.Compiler
	rules    map[string]*SchemaValidationRule
}

type SchemaValidationRule struct {
	Schema string `json:"schema"`
	Path   string `json:"path"`

	// internal
	schema   *js.Schema
	template *template.Template
}

type SchemaDBChangeEvent struct {
	Operation     string   `json:"operation"`
	ID            string   `json:"id"`
	Table         string   `json:"table"`
	Key           []string `json:"key"`
	ModelVersion  string   `json:"modelVersion"`
	CompanyID     *string  `json:"companyId,omitempty"`
	LocationID    *string  `json:"locationId,omitempty"`
	UserID        *string  `json:"userId,omitempty"`
	Before        any      `json:"before,omitempty"`
	After         any      `json:"after,omitempty"`
	Diff          []string `json:"diff,omitempty"`
	Timestamp     int64    `json:"timestamp"`
	MVCCTimestamp string   `json:"mvccTimestamp"`
}

func toSchemaDBChangeEvent(event internal.DBChangeEvent) (*SchemaDBChangeEvent, error) {
	var before, after any
	if event.Before != nil {
		before = make(map[string]any)
		if err := json.Unmarshal(event.Before, &before); err != nil {
			return nil, fmt.Errorf("error unmarshalling before: %w", err)
		}
	}
	if event.After != nil {
		after = make(map[string]any)
		if err := json.Unmarshal(event.After, &after); err != nil {
			return nil, fmt.Errorf("error unmarshalling after: %w", err)
		}
	}
	return &SchemaDBChangeEvent{
		Operation:     event.Operation,
		ID:            event.ID,
		Table:         event.Table,
		Key:           event.Key,
		ModelVersion:  event.ModelVersion,
		CompanyID:     event.CompanyID,
		LocationID:    event.LocationID,
		UserID:        event.UserID,
		Before:        before,
		After:         after,
		Diff:          event.Diff,
		Timestamp:     event.Timestamp,
		MVCCTimestamp: event.MVCCTimestamp,
	}, nil
}

// Validate validates the given event against the schema for the table. Returns true if a schema is found for the table, true if the event is valid, the path transformed and an error if one occurs.
func (v *SchemaValidator) Validate(event internal.DBChangeEvent) (bool, bool, string, error) {
	if rule, ok := v.rules[event.Table]; ok {
		object, err := toSchemaDBChangeEvent(event)
		if err != nil {
			return true, false, "", fmt.Errorf("error converting to SchemaDBChangeEvent: %w", err)
		}
		o := make(map[string]any)
		if err := json.Unmarshal([]byte(JSONStringify(object)), &o); err != nil {
			return true, false, "", fmt.Errorf("error converting to SchemaDBChangeEvent: %w", err)
		}
		if err := rule.schema.Validate(o); err != nil {
			if _, ok := err.(*js.ValidationError); ok {
				return true, false, "", nil
			}
			return true, false, "", err
		}
		var path strings.Builder
		if err := rule.template.Execute(&path, o); err != nil {
			return true, false, "", fmt.Errorf("error executing template: %w for %s", err, JSONStringify(o))
		}
		return true, true, path.String(), nil
	}
	return false, false, "", nil
}

// NewSchemaValidator creates a new SchemaValidator from the schema files in the given directory.
func NewSchemaValidator(schemaDir string) (*SchemaValidator, error) {

	abs, err := filepath.Abs(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("error getting absolute path for %s: %w", schemaDir, err)
	}

	config := filepath.Join(abs, "config.json")
	if !Exists(config) {
		return nil, fmt.Errorf("config.json not found in schema directory: %s", abs)
	}

	of, err := os.Open(config)
	if err != nil {
		return nil, fmt.Errorf("error opening %s: %w", config, err)
	}
	defer of.Close()

	dec := json.NewDecoder(of)

	rules := make(map[string]*SchemaValidationRule)

	if err := dec.Decode(&rules); err != nil {
		return nil, fmt.Errorf("error decoding config.json: %w", err)
	}

	compiler := js.NewCompiler()

	files, err := ListDir(abs)
	if err != nil {
		return nil, fmt.Errorf("error listing files in %s: %w", abs, err)
	}

	// load up all of our schemas as resources so that they can be referenced
	for _, filename := range files {
		rel, _ := filepath.Rel(abs, filename)
		if rel == "config.json" {
			continue
		}
		buf, err := os.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("error reading: %s. %w", filename, err)
		}
		// load them in various url formats
		if err := compiler.AddResource("file://"+rel, bytes.NewReader(buf)); err != nil {
			return nil, fmt.Errorf("error adding schema: %s. %w", filename, err)
		}
		if err := compiler.AddResource("file:///"+rel, bytes.NewReader(buf)); err != nil {
			return nil, fmt.Errorf("error adding schema: %s. %w", filename, err)
		}
		if err := compiler.AddResource("file://"+filename, bytes.NewReader(buf)); err != nil {
			return nil, fmt.Errorf("error adding schema: %s. %w", filename, err)
		}
	}

	for table, rule := range rules {
		fn := filepath.Join(abs, rule.Schema)
		if !Exists(fn) {
			return nil, fmt.Errorf("schema file not found: %s for table: %s", fn, table)
		}
		schema, err := compiler.Compile("file://" + fn)
		if err != nil {
			return nil, fmt.Errorf("error compiling schema: %s for table: %s. %w", fn, table, err)
		}
		rule.schema = schema

		tmpl, err := template.New(fn).Parse(rule.Path)
		if err != nil {
			return nil, fmt.Errorf("error creating template: %s for table: %s. %w", fn, table, err)
		}
		rule.template = tmpl
	}

	return &SchemaValidator{
		compiler: compiler,
		rules:    rules,
	}, nil
}
