package internal

import (
	"context"
	"fmt"
	"net/url"

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
}

// Importer is the interface that must be implemented by all importer implementations
type Importer interface {

	// Import is called to import data from the source.
	Import(config ImporterConfig) error
}

var importerRegistry = map[string]Importer{}

// Register registers a importer for a given protocol.
func RegisterImporter(protocol string, importer Importer) {
	importerRegistry[protocol] = importer
}

// NewImporter creates a new importer for the given URL.
func NewImporter(ctx context.Context, logger logger.Logger, urlString string, registry SchemaRegistry) (Importer, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}
	importer := importerRegistry[u.Scheme]
	if importer == nil {
		return nil, fmt.Errorf("no importer registered for protocol %s", u.Scheme)
	}
	return importer, nil
}
