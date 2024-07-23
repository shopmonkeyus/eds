package internal

import (
	"context"
	"fmt"
	"net/url"

	"github.com/shopmonkeyus/go-common/logger"
)

// ProcessorConfig is the configuration for a processor.
type ProcessorConfig struct {

	// Context for the processor.
	Context context.Context

	// URL for the processor.
	URL string

	// Logger to use for logging.
	Logger logger.Logger

	// SchemaRegistry is the schema registry to use for the processor.
	SchemaRegistry SchemaRegistry
}

// ProcessorSessionHandler is for processors that want to receive the session id
type ProcessorSessionHandler interface {
	SetSessionID(sessionID string)
}

// ProcessorLifecycle is the interface that must be implemented by all processor implementations.
type ProcessorLifecycle interface {

	// Start the processor. This is called once at the beginning of the processor's lifecycle.
	Start(config ProcessorConfig) error
}

// Processor is the interface that must be implemented by all processor implementations.q
// that implement handling data change events.
type Processor interface {

	// Stop the processor. This is called once at the end of the processor's lifecycle.
	Stop() error

	// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
	// Return -1 to indicate that there is no limit.
	MaxBatchSize() int

	// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the processor will NAK the event.
	Process(event DBChangeEvent) (bool, error)

	// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the processor will NAK all pending events.
	Flush() error
}

// ProcessorHelp is an interface that Processors implement for controlling the help system.
type ProcessorHelp interface {

	// Description is the description of the processor.
	Description() string

	// ExampleURL should return an example URL for configuring the processor.
	ExampleURL() string

	// Help should return a detailed help documentation for the processor.
	Help() string
}

type ProcessorMetadata struct {
	Name           string
	Description    string
	ExampleURL     string
	Help           string
	SupportsImport bool
}

var processorRegistry = map[string]Processor{}

// GetProcessorMetadata returns the metadata for all the registered processors.
func GetProcessorMetadata() []ProcessorMetadata {
	var res []ProcessorMetadata
	for name, processor := range processorRegistry {
		if help, ok := processor.(ProcessorHelp); ok {
			res = append(res, ProcessorMetadata{
				Name:           name,
				Description:    help.Description(),
				ExampleURL:     help.ExampleURL(),
				Help:           help.Help(),
				SupportsImport: importerRegistry[name] != nil,
			})
		}
	}
	return res
}

// Register registers a processor for a given protocol.
func RegisterProcessor(protocol string, processor Processor) {
	processorRegistry[protocol] = processor
}

// NewProcessor creates a new processor for the given URL.
func NewProcessor(ctx context.Context, logger logger.Logger, urlString string, registry SchemaRegistry) (Processor, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}
	processor := processorRegistry[u.Scheme]
	if processor == nil {
		return nil, fmt.Errorf("no processor registered for protocol %s", u.Scheme)
	}

	// start the processor if it implements the ProcessorLifecycle interface
	if p, ok := processor.(ProcessorLifecycle); ok {
		if err := p.Start(ProcessorConfig{
			Context:        ctx,
			URL:            urlString,
			Logger:         logger.WithPrefix(fmt.Sprintf("[%s]", u.Scheme)),
			SchemaRegistry: registry,
		}); err != nil {
			return nil, err
		}
	}

	return processor, nil
}
