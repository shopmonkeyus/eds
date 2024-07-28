package eventhub

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type eventHubDriver struct {
	config    internal.DriverConfig
	logger    logger.Logger
	batcher   *util.Batcher
	producer  *azeventhubs.ProducerClient
	waitGroup sync.WaitGroup
	once      sync.Once
}

var _ internal.Driver = (*eventHubDriver)(nil)
var _ internal.DriverLifecycle = (*eventHubDriver)(nil)
var _ internal.DriverHelp = (*eventHubDriver)(nil)

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *eventHubDriver) Start(pc internal.DriverConfig) error {
	p.config = pc
	p.batcher = util.NewBatcher()
	p.logger = pc.Logger.WithPrefix("[eventhub]")

	u, err := url.Parse(pc.URL)
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

	p.logger.Info("started")
	return nil
}

// Stop the driver. This is called once at the end of the driver's lifecycle.
func (p *eventHubDriver) Stop() error {
	p.logger.Debug("stopping")
	p.once.Do(func() {
		p.Flush()
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
func (p *eventHubDriver) Process(event internal.DBChangeEvent) (bool, error) {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	object, err := event.GetObject()
	if err != nil {
		return false, fmt.Errorf("error getting json object: %w", err)
	}
	p.batcher.Add(event.Table, event.GetPrimaryKey(), event.Operation, event.Diff, object, &event)
	return false, nil
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
func (p *eventHubDriver) Flush() error {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	records := p.batcher.Records()
	count := len(records)
	if count > 0 {
		p.batcher.Clear()
		var batches []*azeventhubs.EventDataBatch
		var pendingPartitionKey string
		for _, record := range records {
			var companyId string
			var locationId string
			if val, ok := record.Object["companyId"].(string); ok {
				companyId = val
			}
			if val, ok := record.Object["locationId"].(string); ok {
				locationId = val
			}
			key := fmt.Sprintf("dbchange.%s.%s.%s.%s.%s", record.Table, record.Operation, strWithDef(&companyId, "NONE"), strWithDef(&locationId, "NONE"), record.Id)
			partitionKey := fmt.Sprintf("%s.%s.%s", record.Table, strWithDef(&companyId, "NONE"), strWithDef(&locationId, "NONE"))
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
			if err := batch.AddEventData(&azeventhubs.EventData{
				Body:        []byte(util.JSONStringify(record.Event)),
				MessageID:   &record.Event.ID,
				ContentType: &contentType,
				Properties:  map[string]any{"objectId": key},
			}, nil); err != nil {
				return fmt.Errorf("error adding event to batch: %w", err)
			}
			for _, batch := range batches {
				p.logger.Trace("sending batch with count: %d, bytes: %d", batch.NumEvents(), batch.NumBytes())
				if err := p.producer.SendEventDataBatch(p.config.Context, batch, nil); err != nil {
					return fmt.Errorf("error sending batch: %w", err)
				}
			}
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
	help.WriteString(util.GenerateHelpSection("Partitioning", "The partition key is calculated automatically based on the number of partitions for the topic and the incoming message.\nThe partition key is in the format: [TABLE].[COMPANY_ID].[LOCATION_ID].\n"))
	help.WriteString("\n")
	help.WriteString(util.GenerateHelpSection("Message Value", "The message value is a JSON encoded value of the EDS DBChange event."))
	return help.String()
}

func init() {
	internal.RegisterDriver("eventhub", &eventHubDriver{})
}
