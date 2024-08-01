package kafka

import (
	"fmt"
	"net/url"
	"strings"
	"sync"

	gokafka "github.com/segmentio/kafka-go"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/importer"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const (
	edsPartitionKeyHeader = "eds-partitionkey"
	maxImportBatchSize    = 1_000
)

type messageBalancer struct {
}

func (b *messageBalancer) Balance(msg gokafka.Message, partitions ...int) int {
	if len(partitions) == 1 {
		return partitions[0]
	}
	for _, header := range msg.Headers {
		if header.Key == edsPartitionKeyHeader {
			return util.Modulo(util.Hash(string(header.Value)), len(partitions))
		}
	}
	return util.Modulo(util.Hash(string(msg.Key)), len(partitions))
}

type kafkaDriver struct {
	config       internal.DriverConfig
	logger       logger.Logger
	writer       *gokafka.Writer
	pending      []gokafka.Message
	waitGroup    sync.WaitGroup
	once         sync.Once
	importConfig internal.ImporterConfig
}

var _ internal.Driver = (*kafkaDriver)(nil)
var _ internal.DriverLifecycle = (*kafkaDriver)(nil)
var _ internal.DriverHelp = (*kafkaDriver)(nil)
var _ internal.Importer = (*kafkaDriver)(nil)
var _ importer.Handler = (*kafkaDriver)(nil)

func (p *kafkaDriver) connect(urlString string) error {
	u, err := url.Parse(urlString)
	if err != nil {
		return fmt.Errorf("unable to parse url: %w", err)
	}

	if u.Path == "" {
		return fmt.Errorf("kafka url requires a path which is the topic")
	}
	host := u.Host
	topic := u.Path[1:] // trim slash

	p.writer = &gokafka.Writer{
		Addr:                   gokafka.TCP(host),
		Topic:                  topic,
		Balancer:               &messageBalancer{},
		AllowAutoTopicCreation: true,
	}

	return nil
}

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *kafkaDriver) Start(pc internal.DriverConfig) error {
	p.config = pc
	p.logger = pc.Logger.WithPrefix("[kafka]")
	if err := p.connect(pc.URL); err != nil {
		return err
	}
	p.logger.Info("started")
	return nil
}

// Stop the driver. This is called once at the end of the driver's lifecycle.
func (p *kafkaDriver) Stop() error {
	p.logger.Debug("stopping")
	p.once.Do(func() {
		p.logger.Debug("waiting on waitgroup")
		p.waitGroup.Wait()
		p.logger.Debug("completed waitgroup")
		if p.writer != nil {
			p.logger.Debug("closing writer")
			p.writer.Close()
			p.writer = nil
			p.logger.Debug("closed writer")
		}
	})
	p.logger.Debug("stopped")
	return nil
}

// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
// Return -1 to indicate that there is no limit.
func (p *kafkaDriver) MaxBatchSize() int {
	return -1
}

func strWithDef(val *string, def string) string {
	if val == nil {
		return def
	}
	return *val
}

func (p *kafkaDriver) process(event internal.DBChangeEvent, dryRun bool) error {
	key := fmt.Sprintf("dbchange.%s.%s.%s.%s.%s", event.Table, event.Operation, strWithDef(event.CompanyID, "NONE"), strWithDef(event.LocationID, "NONE"), event.ID)
	pk := event.Key[len(event.Key)-1]
	partitionkey := fmt.Sprintf("%s.%s.%s.%s", event.Table, strWithDef(event.CompanyID, "NONE"), strWithDef(event.LocationID, "NONE"), pk)
	if dryRun {
		p.logger.Trace("would store key: %s, partition key: %s", key, partitionkey)
		return nil
	} else {
		p.pending = append(p.pending, gokafka.Message{
			Key:   []byte(key),
			Value: []byte(util.JSONStringify(event)),
			Headers: []gokafka.Header{
				{Key: edsPartitionKeyHeader, Value: []byte(partitionkey)},
			},
		})
	}
	if len(p.pending) >= maxImportBatchSize {
		return p.Flush()
	}
	return nil
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
func (p *kafkaDriver) Process(event internal.DBChangeEvent) (bool, error) {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	if err := p.process(event, false); err != nil {
		return false, err
	}
	return false, nil
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
func (p *kafkaDriver) Flush() error {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	if len(p.pending) > 0 {
		if err := p.writer.WriteMessages(p.config.Context, p.pending...); err != nil {
			return fmt.Errorf("error publishing message. %w", err)
		}
		p.pending = nil
	}
	return nil
}

// Name is a unique name for the driver.
func (p *kafkaDriver) Name() string {
	return "Kafka"
}

// Description is the description of the driver.
func (p *kafkaDriver) Description() string {
	return "Supports streaming EDS messages to a Kafka topic."
}

// ExampleURL should return an example URL for configuring the driver.
func (p *kafkaDriver) ExampleURL() string {
	return "kafka://kafka:9092/topic"
}

// Help should return a detailed help documentation for the driver.
func (p *kafkaDriver) Help() string {
	var help strings.Builder
	help.WriteString(util.GenerateHelpSection("Partitioning", "The partition key is calculated automatically based on the number of partitions for the topic and the incoming message.\nThe algorithm is to calculate a value (hash input) in the format: [TABLE].[COMPANY_ID].[LOCATION_ID].[PRIMARY_KEY]\nand use a hash function to generate a value modulo the number of topic partitions. This guarantees the correct ordering\nfor a given table and primary key while providing the ability to safely scale processing horizontally.\n"))
	help.WriteString("\n")
	help.WriteString(util.GenerateHelpSection("Message Key", "The message key is computed in the format: dbchange.[TABLE].[OPERATION].[COMPANY_ID].[LOCATION_ID].[MESSAGE_ID].\n"))
	help.WriteString("\n")
	help.WriteString(util.GenerateHelpSection("Message Value", "The message value is a JSON encoded value of the EDS DBChange event."))
	return help.String()
}

// CreateDatasource allows the handler to create the datasource before importing data.
func (p *kafkaDriver) CreateDatasource(schema internal.SchemaMap) error {
	return nil
}

// ImportEvent allows the handler to process the event.
func (p *kafkaDriver) ImportEvent(event internal.DBChangeEvent, schema *internal.Schema) error {
	return p.process(event, p.importConfig.DryRun)
}

// ImportCompleted is called when all events have been processed.
func (p *kafkaDriver) ImportCompleted() error {
	if err := p.Flush(); err != nil {
		return err
	}
	return p.writer.Close()
}

func (p *kafkaDriver) Import(config internal.ImporterConfig) error {
	p.logger = config.Logger.WithPrefix("[kafka]")
	p.importConfig = config
	if err := p.connect(config.URL); err != nil {
		return err
	}
	return importer.Run(p.logger, config, p)
}

func init() {
	internal.RegisterDriver("kafka", &kafkaDriver{})
	internal.RegisterImporter("kafka", &kafkaDriver{})
}
