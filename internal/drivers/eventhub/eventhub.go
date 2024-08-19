package eventhub

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/importer"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const maxImportBatchSize = 100

type eventHubDriver struct {
	config       internal.DriverConfig
	logger       logger.Logger
	batcher      *util.Batcher
	producer     *azeventhubs.ProducerClient
	waitGroup    sync.WaitGroup
	once         sync.Once
	importConfig internal.ImporterConfig
	dryRun       bool
}

var _ internal.Driver = (*eventHubDriver)(nil)
var _ internal.DriverLifecycle = (*eventHubDriver)(nil)
var _ internal.DriverHelp = (*eventHubDriver)(nil)
var _ importer.Handler = (*eventHubDriver)(nil)
var _ internal.Importer = (*eventHubDriver)(nil)
var _ internal.ImporterHelp = (*eventHubDriver)(nil)

func (p *eventHubDriver) connect(urlString string) error {
	u, err := url.Parse(urlString)
	if err != nil {
		return fmt.Errorf("unable to parse url: %w", err)
	}
	u.Scheme = "sb"
	connectionString := "Endpoint=" + u.String()

	// create an Event Hubs producer client using a connection string to the namespace and the event hub
	producerClient, err := azeventhubs.NewProducerClientFromConnectionString(connectionString, "", nil)
	if err != nil {
		return fmt.Errorf("error connecting to eventhub: %w", err)
	}
	p.producer = producerClient
	return nil
}

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *eventHubDriver) Start(pc internal.DriverConfig) error {
	p.config = pc
	p.batcher = util.NewBatcher()
	p.logger = pc.Logger.WithPrefix("[eventhub]")

	if err := p.connect(pc.URL); err != nil {
		return err
	}

	p.logger.Info("started")
	return nil
}

// Stop the driver. This is called once at the end of the driver's lifecycle.
func (p *eventHubDriver) Stop() error {
	p.logger.Debug("stopping")
	p.once.Do(func() {
		p.Flush(p.logger)
		p.logger.Debug("waiting on waitgroup")
		p.waitGroup.Wait()
		p.logger.Debug("completed waitgroup")
		p.producer.Close(context.Background())
	})
	p.logger.Debug("stopped")
	return nil
}

// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
// Return -1 to indicate that there is no limit.
func (p *eventHubDriver) MaxBatchSize() int {
	return -1
}

func strWithDef(val *string, def string) string {
	if val == nil || *val == "" {
		return def
	}
	return *val
}

var contentType = "application/json"

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
func (p *eventHubDriver) Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	object, err := event.GetObject()
	if err != nil {
		return false, fmt.Errorf("error getting json object: %w", err)
	}
	p.batcher.Add(event.Table, event.GetPrimaryKey(), event.Operation, event.Diff, object, &event)
	return false, nil
}

func (p *eventHubDriver) getKeys(record *util.Record, companyId string, locationId string) (string, string) {
	key := fmt.Sprintf("dbchange.%s.%s.%s.%s.%s", record.Table, record.Operation, strWithDef(&companyId, "NONE"), strWithDef(&locationId, "NONE"), record.Id)
	partitionKey := fmt.Sprintf("%s.%s.%s.%s", record.Table, strWithDef(&companyId, "NONE"), strWithDef(&locationId, "NONE"), record.Id)
	return key, partitionKey
}

func (p *eventHubDriver) addEventToBatch(batch *azeventhubs.EventDataBatch, record *util.Record, key string) error {
	if err := batch.AddEventData(&azeventhubs.EventData{
		Body:        []byte(util.JSONStringify(record.Event)),
		MessageID:   &record.Event.ID,
		ContentType: &contentType,
		Properties:  map[string]any{"objectId": key},
	}, nil); err != nil {
		return fmt.Errorf("error adding event to batch: %w", err)
	}
	return nil
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
func (p *eventHubDriver) Flush(logger logger.Logger) error {
	logger.Debug("flush")
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	records := p.batcher.Records()
	count := p.batcher.Len()
	if count > 0 {
		p.batcher.Clear()
		var batches []*azeventhubs.EventDataBatch
		var pendingPartitionKey string
		var offset int
		for _, record := range records {
			var companyId string
			var locationId string
			if val, ok := record.Object["companyId"].(string); ok {
				companyId = val
			}
			if val, ok := record.Object["locationId"].(string); ok {
				locationId = val
			}
			key, partitionKey := p.getKeys(record, companyId, locationId)
			var batch *azeventhubs.EventDataBatch
			if pendingPartitionKey == partitionKey {
				batch = batches[len(batches)-1]
			} else {
				newBatchOptions := &azeventhubs.EventDataBatchOptions{
					PartitionKey: &partitionKey,
				}
				b, err := p.producer.NewEventDataBatch(p.config.Context, newBatchOptions)
				if err != nil {
					return fmt.Errorf("error creating new batch: %w", err)
				}
				batch = b
				pendingPartitionKey = partitionKey
				batches = append(batches, b)
			}
			if err := p.addEventToBatch(batch, record, key); err != nil {
				return err
			}
		}
		for _, batch := range batches {
			if p.dryRun {
				logger.Trace("would send batch (%03d/%03d) with count: %d, bytes: %d", 1+offset, count, batch.NumEvents(), batch.NumBytes())
			} else {
				logger.Trace("sending batch (%03d/%03d) with count: %d, bytes: %d", 1+offset, count, batch.NumEvents(), batch.NumBytes())
				if err := p.producer.SendEventDataBatch(p.config.Context, batch, nil); err != nil {
					return fmt.Errorf("error sending batch: %w", err)
				}
			}
			offset += int(batch.NumEvents())
		}
	}
	return nil
}

// Name is a unique name for the driver.
func (p *eventHubDriver) Name() string {
	return "Microsoft Azure EventHub"
}

// Description is the description of the driver.
func (p *eventHubDriver) Description() string {
	return "Supports streaming EDS messages to a Microsoft Azure EventHub."
}

// ExampleURL should return an example URL for configuring the driver.
func (p *eventHubDriver) ExampleURL() string {
	return "eventhub://my-eventhub.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=YXNkZmFzZGZhc2RmYXNkZmFzZGZhcwo=;EntityPath=my-eventhub"
}

// Help should return a detailed help documentation for the driver.
func (p *eventHubDriver) Help() string {
	var help strings.Builder
	help.WriteString(util.GenerateHelpSection("Partitioning", "The partition key is calculated automatically based on the number of partitions for the topic and the incoming message.\nThe partition key is in the format: [TABLE].[COMPANY_ID].[LOCATION_ID].[PRIMARY_KEY].\n"))
	help.WriteString("\n")
	help.WriteString(util.GenerateHelpSection("Message Value", "The message value is a JSON encoded value of the EDS DBChange event."))
	return help.String()
}

// CreateDatasource allows the handler to create the datasource before importing data.
func (p *eventHubDriver) CreateDatasource(schema internal.SchemaMap) error {
	return nil
}

// ImportEvent allows the handler to process the event.
func (p *eventHubDriver) ImportEvent(event internal.DBChangeEvent, schema *internal.Schema) error {
	object, err := event.GetObject()
	if err != nil {
		return fmt.Errorf("error getting json object: %w", err)
	}
	p.batcher.Add(event.Table, event.GetPrimaryKey(), event.Operation, event.Diff, object, &event)

	if p.batcher.Len() >= maxImportBatchSize {
		return p.Flush(p.logger)
	}
	return nil
}

// ImportCompleted is called when all events have been processed.
func (p *eventHubDriver) ImportCompleted() error {
	if p.batcher != nil {
		return p.Flush(p.logger)
	}
	p.producer.Close(context.Background())
	return nil
}

func (p *eventHubDriver) Import(config internal.ImporterConfig) error {
	if config.SchemaOnly {
		return nil
	}
	p.logger = config.Logger.WithPrefix("[eventhub]")
	if err := p.connect(config.URL); err != nil {
		return err
	}
	p.config.Context = config.Context
	p.dryRun = config.DryRun
	p.importConfig = config
	p.batcher = util.NewBatcher()
	return importer.Run(p.logger, config, p)
}

// SupportsDelete returns true if the importer supports deleting data.
func (p *eventHubDriver) SupportsDelete() bool {
	return false
}

// Test is called to test the drivers connectivity with the configured url. It should return an error if the test fails or nil if the test passes.
func (p *eventHubDriver) Test(ctx context.Context, logger logger.Logger, url string) error {
	if err := p.connect(url); err != nil {
		return err
	}
	return p.producer.Close(context.Background())
}

// Configuration returns the configuration fields for the driver.
func (p *eventHubDriver) Configuration() []internal.DriverField {
	return []internal.DriverField{
		internal.RequiredStringField("Connection String", "The connection string primary key from the Event Hub console.", nil),
	}
}

// Validate validates the configuration and returns an error if the configuration is invalid or a valid url if the configuration is valid.
func (p *eventHubDriver) Validate(values map[string]any) (string, []internal.FieldError) {
	val := internal.GetRequiredStringValue("Connection String", values)
	// example:
	// Endpoint=sb://shopmonkey-xx-test.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=x/x+x+x+x=;EntityPath=shopmonkey-eds-test
	if !strings.HasPrefix(val, "Endpoint=") {
		return "", []internal.FieldError{internal.NewFieldError("Connection String", "expected to start with the prefix Endpoint=")}
	}
	i := strings.Index(val, "://")
	if i < 0 {
		return "", []internal.FieldError{internal.NewFieldError("Connection String", "expected a url scheme after Endpoint= prefix")}
	}
	return "eventhub://" + val[i+3:], nil
}

func init() {
	internal.RegisterDriver("eventhub", &eventHubDriver{})
	internal.RegisterImporter("eventhub", &eventHubDriver{})
}
