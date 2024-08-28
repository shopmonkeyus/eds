package internal

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/charmbracelet/x/ansi"
	"github.com/shopmonkeyus/eds/internal/tracker"
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

	// DataDir is the directory where the driver can store data.
	DataDir string
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
	Process(logger logger.Logger, event DBChangeEvent) (bool, error)

	// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
	Flush(logger logger.Logger) error

	// Test is called to test the drivers connectivity with the configured url. It should return an error if the test fails or nil if the test passes.
	Test(ctx context.Context, logger logger.Logger, url string) error

	// Configuration returns the configuration fields for the driver.
	Configuration() []DriverField

	// Validate validates the configuration and returns an error if the configuration is invalid or a valid url if the configuration is valid.
	Validate(map[string]any) (string, []FieldError)
}

// DriverAlias is an interface that Drivers implement for specifying additional protocol schemes for URLs that the driver can handle.
type DriverAlias interface {
	// Aliases returns a list of additional protocol schemes that the driver can handle (from the main protocol that was registered).
	Aliases() []string
}

type DriverType string
type DriverFormat string

const (
	DriverTypeString  DriverType = "string"
	DriverTypeNumber  DriverType = "number"
	DriverTypeBoolean DriverType = "boolean"
)

const (
	DriverFormatPassword DriverFormat = "password"
)

// DriverField is a field in the driver configuration.
type DriverField struct {
	Name        string       `json:"name"`
	Type        DriverType   `json:"type"`
	Format      DriverFormat `json:"format,omitempty"`
	Default     *string      `json:"default,omitempty"` // its a string for display purposes
	Description string       `json:"description"`
	Required    bool         `json:"required"`
}

// DriverHelp is an interface that Drivers implement for controlling the help system.
type DriverHelp interface {

	// Name is a unique name for the driver.
	Name() string

	// Description is the description of the driver.
	Description() string

	// ExampleURL should return an example URL for configuring the driver.
	ExampleURL() string

	// Help should return a detailed help documentation for the driver.
	Help() string
}

type DriverMetadata struct {
	Scheme            string `json:"scheme"`
	Name              string `json:"name"`
	Description       string `json:"description"`
	ExampleURL        string `json:"exampleURL"`
	Help              string `json:"help"`
	SupportsImport    bool   `json:"supportsImport"`
	SupportsMigration bool   `json:"supportsMigration"`
}

func driverSupportsMigration(driver Driver) bool {
	if _, ok := driver.(DriverMigration); ok {
		return true
	}
	return false
}

var driverRegistry = map[string]Driver{}
var driverAliasRegistry = map[string]string{}

// GetDriverMetadata returns the metadata for all the registered drivers.
func GetDriverMetadata() []DriverMetadata {
	var res []DriverMetadata
	for scheme, driver := range driverRegistry {
		if help, ok := driver.(DriverHelp); ok {
			res = append(res, DriverMetadata{
				Scheme:            scheme,
				Name:              help.Name(),
				Description:       help.Description(),
				ExampleURL:        help.ExampleURL(),
				Help:              help.Help(),
				SupportsImport:    importerRegistry[scheme] != nil,
				SupportsMigration: driverSupportsMigration(driver),
			})
		}
	}
	return res
}

type DriverConfigurator struct {
	Metadata DriverMetadata `json:"metadata"`
	Fields   []DriverField  `json:"fields"`
}

// GetDriverConfigurations returns the configurations for each supported driver.
func GetDriverConfigurations() map[string]DriverConfigurator {
	res := make(map[string]DriverConfigurator)
	for scheme, driver := range driverRegistry {
		metadata := DriverMetadata{
			Scheme: scheme,
			Name:   scheme,
		}
		if help, ok := driver.(DriverHelp); ok {
			metadata.Name = help.Name()
			metadata.Description = help.Description()
			metadata.ExampleURL = help.ExampleURL()
			metadata.Help = ansi.Strip(help.Help())
			metadata.SupportsImport = importerRegistry[scheme] != nil
			metadata.SupportsMigration = driverSupportsMigration(driver)
		}
		res[scheme] = DriverConfigurator{
			Metadata: metadata,
			Fields:   driver.Configuration(),
		}
	}
	return res
}

// GetDriverMetadataForURL returns the metadata for a specific url or nil if not supported.
func GetDriverMetadataForURL(urlString string) (*DriverMetadata, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}
	proto := u.Scheme
	for scheme, driver := range driverRegistry {
		if scheme == proto || driverAliasRegistry[proto] == scheme {
			if help, ok := driver.(DriverHelp); ok {
				return &DriverMetadata{
					Scheme:            scheme,
					Name:              help.Name(),
					Description:       help.Description(),
					ExampleURL:        help.ExampleURL(),
					Help:              help.Help(),
					SupportsImport:    importerRegistry[scheme] != nil,
					SupportsMigration: driverSupportsMigration(driver),
				}, nil
			} else {
				return &DriverMetadata{
					Scheme: scheme,
					Name:   scheme,
				}, nil
			}
		}
	}
	return nil, nil
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
func NewDriver(ctx context.Context, logger logger.Logger, urlString string, registry SchemaRegistry, tracker *tracker.Tracker, datadir string) (Driver, error) {
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
			DataDir:        datadir,
		}); err != nil {
			return nil, err
		}
	}

	return driver, nil
}

func NewDriverForImport(ctx context.Context, logger logger.Logger, urlString string, registry SchemaRegistry, tracker *tracker.Tracker, datadir string) (Driver, error) {
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
	return driver, nil
}

// Validate will validate a driver configuration and return the URL if the configuration is valid.
func Validate(schema string, values map[string]any) (string, []FieldError, error) {
	driver := driverRegistry[schema]
	if driver == nil {
		protocol := driverAliasRegistry[schema]
		if protocol != "" {
			driver = driverRegistry[protocol]
		}
		if driver == nil {
			return "", nil, fmt.Errorf("no driver registered for protocol %s", schema)
		}
	}
	url, errs := driver.Validate(values)
	return url, errs, nil
}

func RequiredStringField(name, description string, defval *string) DriverField {
	return DriverField{
		Name:        name,
		Type:        DriverTypeString,
		Description: description,
		Required:    true,
		Default:     defval,
	}
}

func OptionalStringField(name, description string, defval *string) DriverField {
	return DriverField{
		Name:        name,
		Type:        DriverTypeString,
		Description: description,
		Required:    false,
		Default:     defval,
	}
}

func OptionalPasswordField(name, description string, defval *string) DriverField {
	return DriverField{
		Name:        name,
		Type:        DriverTypeString,
		Format:      DriverFormatPassword,
		Description: description,
		Required:    false,
		Default:     defval,
	}
}

func OptionalNumberField(name, description string, defval *int) DriverField {
	var defstr *string
	if defval != nil {
		v := fmt.Sprintf("%d", *defval)
		defstr = &v
	}
	return DriverField{
		Name:        name,
		Type:        DriverTypeNumber,
		Description: description,
		Required:    false,
		Default:     defstr,
	}
}

func StringPointer(val string) *string {
	if val == "" {
		return nil
	}
	return &val
}

func IntPointer(val int) *int {
	return &val
}

type FieldError struct {
	Field   string `json:"field" msgpack:"field"`
	Message string `json:"error" msgpack:"error"`
}

func (f FieldError) Error() string {
	return f.Message
}

func NewFieldError(field, message string) FieldError {
	return FieldError{
		Field:   field,
		Message: message,
	}
}

func GetRequiredStringValue(name string, values map[string]any) string {
	if val, ok := values[name].(string); ok {
		return val
	}
	panic("required field " + name + " not found") // should not happen
}

func GetRequiredIntValue(name string, values map[string]any) int {
	if val, ok := values[name].(int); ok {
		return val
	}
	if val, ok := values[name].(int64); ok {
		return int(val)
	}
	panic("required field " + name + " not found") // should not happen
}

func GetOptionalStringValue(name string, def string, values map[string]any) string {
	if val, ok := values[name].(string); ok && val != "" {
		return val
	}
	return def
}

func GetOptionalIntValue(name string, def int, values map[string]any) int {
	fmt.Printf("Type of values[%s]: %T\n", name, values[name]) // Added line to print type
	 val, ok := values[name].(int); 
	 if ok {
		return val
	}
	if val, ok := values[name].(int64); ok {
		return int(val)
	}
	if val, ok := values[name].(string); ok { // Check if the value is a string
		if intVal, err := strconv.Atoi(val); err == nil { // Convert string to int
			return intVal
		}
	}
	return def
}

func URLFromDatabaseConfiguration(schema string, defport int, values map[string]any) string {
	hostname := GetRequiredStringValue("Hostname", values)
	username := GetOptionalStringValue("Username", "", values)
	password := GetOptionalStringValue("Password", "", values)
	port := GetOptionalIntValue("Port", defport, values)
	

	database := GetRequiredStringValue("Database", values)
	var u url.URL
	u.Scheme = schema
	if username != "" {
		u.User = url.UserPassword(username, password)
	}
	if defport > 0 {
		u.Host = fmt.Sprintf("%s:%d", hostname, port)
	} else {
		u.Host = hostname
	}
	u.Path = database
	urlString := u.String()
	unescapedUrl, _ := url.QueryUnescape(urlString)
	return unescapedUrl
}

func NewDatabaseConfiguration(defport int) []DriverField {
	fields := []DriverField{
		RequiredStringField("Database", "The database name to use", nil),
		OptionalStringField("Username", "The username to database", nil),
		OptionalPasswordField("Password", "The password to database", nil),
		RequiredStringField("Hostname", "The hostname or ip address to database", nil),
	}
	if defport > 0 {
		fields = append(fields, OptionalNumberField("Port", "The port to database", IntPointer(defport)))
	}
	return fields
}
