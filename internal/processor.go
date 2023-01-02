package internal

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/go-datamodel/datatypes"
	v3 "github.com/shopmonkeyus/go-datamodel/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type MessageProcessor struct {
	logger          Logger
	companyID       string
	db              *gorm.DB
	conn            *nats.Conn
	js              nats.JetStreamContext
	subs            []*nats.Subscription
	lock            sync.Mutex
	wg              *sync.WaitGroup
	dumpMessagesDir string
}

// MessageProcessorOpts is the options for the message processor
type MessageProcessorOpts struct {
	Logger          Logger
	CompanyID       string
	Database        *gorm.DB
	NatsConnection  *nats.Conn
	DumpMessagesDir string
	TraceNats       bool
}

// NewMessageProcessor will create a new processor for a given customer id
func NewMessageProcessor(opts MessageProcessorOpts) (*MessageProcessor, error) {
	js, err := opts.NatsConnection.JetStream(&nats.ClientTrace{
		RequestSent: func(subj string, payload []byte) {
			if opts.TraceNats {
				opts.Logger.Trace("nats tx: %s: %s", subj, string(payload))
			}
		},
		ResponseReceived: func(subj string, payload []byte, hdr nats.Header) {
			if opts.TraceNats {
				opts.Logger.Trace("nats rx: %s: %s", subj, string(payload))
			}
		},
	})
	if err != nil {
		return nil, err
	}
	if opts.DumpMessagesDir != "" {
		if _, err := os.Stat(opts.DumpMessagesDir); os.IsNotExist(err) {
			if err := os.MkdirAll(opts.DumpMessagesDir, 0755); err != nil {
				return nil, fmt.Errorf("couldn't create directory: %s", opts.DumpMessagesDir)
			}
		}
	}
	processor := &MessageProcessor{
		logger:          opts.Logger,
		companyID:       opts.CompanyID,
		db:              opts.Database,
		conn:            opts.NatsConnection,
		dumpMessagesDir: opts.DumpMessagesDir,
		js:              js,
		subs:            make([]*nats.Subscription, 0),
		wg:              &sync.WaitGroup{},
	}
	return processor, nil
}

func formatSubject(companyID string, table string) string {
	return fmt.Sprintf("dbchange.%s.*.%s.>", table, companyID)
}

func formatDurable(companyID string, table string) string {
	return fmt.Sprintf("dbchange_%s_%s", table, companyID)
}

func formatDescription(companyID string, table string) string {
	return fmt.Sprintf("EDS %s consumer for %s", table, companyID)
}

func formatStream(table string) string {
	return fmt.Sprintf("dbchange_%s", table)
}

// run will start processing messages
func (p *MessageProcessor) run(model string, sub *nats.Subscription) {
	p.wg.Add(1)
	defer p.wg.Done()
	dumpFiles := p.dumpMessagesDir != ""
	for {
		msgs, err := sub.Fetch(1)
		if err != nil {
			if strings.Contains(err.Error(), "timeout") {
				continue
			}
			if strings.Contains(err.Error(), "invalid subscription") {
				return
			}
			p.logger.Error("fetch error for model: %s. %s\n", model, err)
			continue
		}
		for _, msg := range msgs {
			encoding := msg.Header.Get("content-encoding")
			gzipped := encoding == "gzip/json"
			msgid := msg.Header.Get("Nats-Msg-Id")
			if dumpFiles {
				ext := ".json"
				if gzipped {
					ext += ".gz"
				}
				fn := path.Join(p.dumpMessagesDir, fmt.Sprintf("%s_%v_%s%s", model, time.Now().UnixNano(), msgid, ext))
				os.WriteFile(fn, msg.Data, 0644)
			}
			p.logger.Trace("received msgid: %s, subject: %s", msgid, msg.Subject)
			object, err := v3.NewFromChangeEvent(model, msg.Data, gzipped)
			if err != nil {
				p.logger.Error("decode error for change event: %s. %s", model, err)
				msg.AckSync()
				continue
			}
			intf := object.(datatypes.ChangeEventHelper)
			p.logger.Trace("decoded object: %s for msgid: %s", object, msgid)
			started := time.Now()
			switch intf.GetOperation() {
			case datatypes.ChangeEventInsert:
				{
					if err := p.db.Create(intf.GetAfter()).Error; err != nil {
						p.logger.Error("insert error for change event: %s. %s", object, err)
					} else {
						p.logger.Trace("inserted db record for msgid: %s, took %v", msgid, time.Since(started))
					}
				}
			case datatypes.ChangeEventUpdate:
				{
					if err := p.db.Clauses(clause.OnConflict{
						UpdateAll: true,
					}).Create(intf.GetAfter()).Error; err != nil {
						p.logger.Error("upsert error for change event: %s. %s", object, err)
					} else {
						p.logger.Trace("upserted db record for msgid: %s, took %v", msgid, time.Since(started))
					}
				}
			case datatypes.ChangeEventDelete:
				{
					if err := p.db.Delete(intf.GetBefore()).Error; err != nil {
						p.logger.Error("delete error for change event: %s. %s", object, err)
					} else {
						p.logger.Trace("deleted db record for msgid: %s, took %v", msgid, time.Since(started))
					}
				}
			}
			msg.AckSync()
		}
	}
}

// Start will start processing messages
func (p *MessageProcessor) Start() error {
	p.logger.Trace("message processor starting")
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, name := range v3.TableNames {
		stream := formatStream(name)
		subject := formatSubject(p.companyID, name)
		durable := formatDurable(p.companyID, name)
		description := formatDescription(p.companyID, name)
		p.logger.Trace("adding consumer for stream: %s, durable: %s, subject: %s", stream, durable, subject)
		_, err := p.js.AddConsumer(stream, &nats.ConsumerConfig{
			Durable:       durable,
			Description:   description,
			FilterSubject: subject,
			AckPolicy:     nats.AckExplicitPolicy,
			MaxAckPending: 1,
			MaxDeliver:    1,
		})
		if err != nil {
			if strings.Contains(err.Error(), "stream not found") {
				p.logger.Warn("stream not found error for stream: %s with subject: %s", stream, subject)
				continue
			}
			return fmt.Errorf("error: creating customer %s: %s", stream, err)
		}
		p.logger.Trace("creating pull subscriber: %s", subject)
		sub, err := p.js.PullSubscribe(subject,
			"",
			nats.Durable(durable),
			nats.MaxDeliver(1),
			nats.MaxAckPending(1),
			nats.AckExplicit(),
			nats.Description(description),
		)
		if err != nil {
			return fmt.Errorf("error: couldn't create pull subscriber for subject: %s. %s", subject, err)
		}
		p.logger.Trace("created pull subscriber: %s", subject)
		p.logger.Info("listening for messages for table: %s", name)
		p.subs = append(p.subs, sub)
		go p.run(name, sub)
	}
	p.logger.Trace("message processor started")
	return nil
}

// Stop will stop processing messages
func (p *MessageProcessor) Stop() error {
	p.logger.Trace("message processor stopping")
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, sub := range p.subs {
		sub.Unsubscribe()
		sub.Drain()
	}
	p.wg.Wait() // wait for all the run goroutines to complete
	p.subs = make([]*nats.Subscription, 0)
	p.logger.Trace("message processor stopped")
	return nil
}
