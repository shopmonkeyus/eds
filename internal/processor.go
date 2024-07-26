package internal

import (
	"context"
	"fmt"
	"net/url"

	"github.com/shopmonkeyus/eds-server/internal/tracker"
	"github.com/shopmonkeyus/go-common/logger"
)

// ErrDriverStopped is returned when the driver has been stopped and a process or flush is called.
var ErrDriverStopped = fmt.Errorf("driver stopped")

// DriverConfig is the configuration for a driver.
type DriverConfig struct {

	// Context for the driver.
	Context context.Context

	// URL for the driver.
	URL string

	// Logger to use for logging.
	Logger logger.Logger

	// SchemaRegistry is the schema registry to use for the driver.
	SchemaRegistry SchemaRegistry

	// Tracker is the local (on disk) database for tracking stuff that the driver can use.
	Tracker *tracker.Tracker
}

// DriverSessionHandler is for drivers that want to receive the session id
type DriverSessionHandler interface {
	SetSessionID(sessionID string)
}

// DriverLifecycle is the interface that must be implemented by all driver implementations.
type DriverLifecycle interface {

	// Start the driver. This is called once at the beginning of the driver's lifecycle.
	Start(config DriverConfig) error
}

// Driver is the interface that must be implemented by all driver implementations.q
// that implement handling data change events.
type Driver interface {

	// Stop the driver. This is called once at the end of the driver's lifecycle.
	Stop() error

	// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
	// Return -1 to indicate that there is no limit.
	MaxBatchSize() int

	// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
	Process(event DBChangeEvent) (bool, error)

	// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
	Flush() error
}

// DriverAlias is an interface that Drivers implement for specifying additional protocol schemes for URLs that the driver can handle.
type DriverAlias interface {
	// Aliases returns a list of additional protocol schemes that the driver can handle (from the main protocol that was registered).
	Aliases() []string
}

// DriverHelp is an interface that Drivers implement for controlling the help system.
type DriverHelp interface {

	// Description is the description of the driver.
	Description() string

	// ExampleURL should return an example URL for configuring the driver.
	ExampleURL() string

	// Help should return a detailed help documentation for the driver.
	Help() string
}

type DriverMetadata struct {
	Name           string
	Description    string
	ExampleURL     string
	Help           string
	SupportsImport bool
}

var driverRegistry = map[string]Driver{}
var driverAliasRegistry = map[string]string{}

// GetDriverMetadata returns the metadata for all the registered drivers.
func GetDriverMetadata() []DriverMetadata {
	var res []DriverMetadata
	for name, driver := range driverRegistry {
		if help, ok := driver.(DriverHelp); ok {
			res = append(res, DriverMetadata{
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

// Register registers a driver for a given protocol.
func RegisterDriver(protocol string, driver Driver) {
	driverRegistry[protocol] = driver
	if p, ok := driver.(DriverAlias); ok {
		for _, alias := range p.Aliases() {
			driverAliasRegistry[alias] = protocol
		}
	}
}

// NewDriver creates a new driver for the given URL.
func NewDriver(ctx context.Context, logger logger.Logger, urlString string, registry SchemaRegistry, tracker *tracker.Tracker) (Driver, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}
	driver := driverRegistry[u.Scheme]
	if driver == nil {
		protocol := driverAliasRegistry[u.Scheme]
		if protocol != "" {
			driver = driverRegistry[protocol]
		}
		if driver == nil {
			return nil, fmt.Errorf("no driver registered for protocol %s", u.Scheme)
		}
	}

	// start the driver if it implements the DriverLifecycle interface
	if p, ok := driver.(DriverLifecycle); ok {
		if err := p.Start(DriverConfig{
			Context:        ctx,
			URL:            urlString,
			Logger:         logger.WithPrefix(fmt.Sprintf("[%s]", u.Scheme)),
			SchemaRegistry: registry,
			Tracker:        tracker,
		}); err != nil {
			return nil, err
		}
	}

	return driver, nil
}
