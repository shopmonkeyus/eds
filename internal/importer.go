package internal

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/shopmonkeyus/go-common/logger"
)

// ImporterConfig is the configuration for an importer.
type ImporterConfig struct {

	// Context for the importer.
	Context context.Context

	// URL for the importer.
	URL string

	// Logger to use for logging.
	Logger logger.Logger

	// SchemaRegistry is the schema registry to use for the importer.
	SchemaRegistry SchemaRegistry

	// MaxParallel is the maximum number of tables to import in parallel (if supported by the Importer).
	MaxParallel int

	// JobID is the current job id for the import session.
	JobID string

	// DataDir is the folder where all the data files are stored.
	DataDir string

	// DryRun is true if the importer should not actually import data.
	DryRun bool

	// Tables is the list of tables to import.
	Tables []string

	// Single is true if only a single row should be imported at a time vs batching.
	Single bool
}

// Importer is the interface that must be implemented by all importer implementations
type Importer interface {

	// Import is called to import data from the source.
	Import(config ImporterConfig) error
}

var importerRegistry = map[string]Importer{}
var importerAliasRegistry = map[string]string{}

// Register registers a importer for a given protocol.
func RegisterImporter(protocol string, importer Importer) {
	importerRegistry[protocol] = importer
	if p, ok := importer.(ProcessorAlias); ok {
		for _, alias := range p.Aliases() {
			importerAliasRegistry[alias] = protocol
		}
	}
}

// NewImporter creates a new importer for the given URL.
func NewImporter(ctx context.Context, logger logger.Logger, urlString string, registry SchemaRegistry) (Importer, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}
	importer := importerRegistry[u.Scheme]
	if importer == nil {
		protocol := importerAliasRegistry[u.Scheme]
		if protocol != "" {
			importer = importerRegistry[protocol]
		}
		if importer == nil {
			importers := []string{}
			for k := range importerRegistry {
				importers = append(importers, k)
			}
			return nil, fmt.Errorf("no importer registered for protocol %s. the following are supported: %s", u.Scheme, strings.Join(importers, ", "))
		}
	}
	return importer, nil
}
