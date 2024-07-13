package s3

import (
	"fmt"
	"net/url"
	"sync"

	gokafka "github.com/segmentio/kafka-go"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const edsPartitionKeyHeader = "eds-partitionkey"

type messageBalancer struct {
}

func (b *messageBalancer) Balance(msg gokafka.Message, partitions ...int) int {
	if len(partitions) == 1 {
		return 0
	}
	for _, header := range msg.Headers {
		if header.Key == edsPartitionKeyHeader {
			return util.Modulo(util.Hash(string(header.Value)), len(partitions))
		}
	}
	return util.Modulo(util.Hash(string(msg.Key)), len(partitions))
}

type kafkaProcessor struct {
	config    internal.ProcessorConfig
	logger    logger.Logger
	writer    *gokafka.Writer
	pending   []gokafka.Message
	waitGroup sync.WaitGroup
	once      sync.Once
}

var _ internal.Processor = (*kafkaProcessor)(nil)
var _ internal.ProcessorLifecycle = (*kafkaProcessor)(nil)

// Start the processor. This is called once at the beginning of the processor's lifecycle.
func (p *kafkaProcessor) Start(pc internal.ProcessorConfig) error {
	p.config = pc
	p.logger = pc.Logger.WithPrefix("[kafka]")

	u, err := url.Parse(pc.URL)
	if err != nil {
		return fmt.Errorf("unable to parse url: %w", err)
	}

	if u.Path == "" {
		return fmt.Errorf("kafka url requires a path which is the topic")
	}
	host := u.Host
	topic := u.Path[1:] // trim slash

	p.writer = &gokafka.Writer{
		Addr:     gokafka.TCP(host),
		Topic:    topic,
		Balancer: &messageBalancer{},
	}

	p.logger.Info("started")
	return nil
}

// Stop the processor. This is called once at the end of the processor's lifecycle.
func (p *kafkaProcessor) Stop() error {
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
func (p *kafkaProcessor) MaxBatchSize() int {
	return -1
}

func strWithDef(val *string, def string) string {
	if val == nil {
		return def
	}
	return *val
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the processor will NAK the event.
func (p *kafkaProcessor) Process(event internal.DBChangeEvent) (bool, error) {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	key := fmt.Sprintf("dbchange.%s.%s.%s.%s.%s", event.Table, event.Operation, strWithDef(event.CompanyID, "NONE"), strWithDef(event.LocationID, "NONE"), event.ID)
	pk := event.Key[len(event.Key)-1]
	partitionkey := fmt.Sprintf("%s.%s.%s.%s", event.Table, strWithDef(event.CompanyID, "NONE"), strWithDef(event.LocationID, "NONE"), pk)
	p.pending = append(p.pending, gokafka.Message{
		Key:   []byte(key),
		Value: []byte(util.JSONStringify(event)),
		Headers: []gokafka.Header{
			{Key: edsPartitionKeyHeader, Value: []byte(partitionkey)},
		},
	})
	return false, nil
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the processor will NAK all pending events.
func (p *kafkaProcessor) Flush() error {
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

func init() {
	internal.RegisterProcessor("kafka", &kafkaProcessor{})
}
