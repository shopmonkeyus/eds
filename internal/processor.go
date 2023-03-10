package internal

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/go-common/logger"
	snats "github.com/shopmonkeyus/go-common/nats"
	"github.com/shopmonkeyus/go-datamodel/datatypes"
	v3 "github.com/shopmonkeyus/go-datamodel/v3"
)

type MessageProcessor struct {
	logger          logger.Logger
	companyID       string
	provider        Provider
	conn            *nats.Conn
	js              nats.JetStreamContext
	subscriber      *snats.ExactlyOnceSubscriber
	dumpMessagesDir string
	consumerPrefix  string
	context         context.Context
	cancel          context.CancelFunc
	tableLookup     map[string]bool
}

// MessageProcessorOpts is the options for the message processor
type MessageProcessorOpts struct {
	Logger          logger.Logger
	CompanyID       string
	Provider        Provider
	NatsConnection  *nats.Conn
	DumpMessagesDir string
	TraceNats       bool
	ConsumerPrefix  string
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
	tableLookup := make(map[string]bool)
	for _, name := range v3.TableNames {
		tableLookup[name] = true
	}
	context, cancel := context.WithCancel(context.Background())
	processor := &MessageProcessor{
		logger:          opts.Logger,
		companyID:       opts.CompanyID,
		provider:        opts.Provider,
		conn:            opts.NatsConnection,
		dumpMessagesDir: opts.DumpMessagesDir,
		consumerPrefix:  opts.ConsumerPrefix,
		js:              js,
		tableLookup:     tableLookup,
		context:         context,
		cancel:          cancel,
	}
	return processor, nil
}

// callback processes db change events
func (p *MessageProcessor) callback(ctx context.Context, payload []byte, msg *nats.Msg) error {
	tok := strings.Split(msg.Subject, ".")
	model := tok[1]
	if !p.tableLookup[model] {
		// skip any non EDS models
		msg.AckSync()
		return nil
	}
	msgid := msg.Header.Get("Nats-Msg-Id")
	encoding := msg.Header.Get("content-encoding")
	gzipped := encoding == "gzip/json"
	p.logger.Trace("received msgid: %s, subject: %s", msgid, msg.Subject)
	object, err := v3.NewFromChangeEvent(model, msg.Data, gzipped)
	if err != nil {
		if gzipped {
			dec, _ := datatypes.Gunzip(msg.Data)
			p.logger.Error("decode error for change event: %s. %s", string(dec), err)
		} else {
			p.logger.Error("decode error for change event: %s. %s", string(msg.Data), err)
		}
		msg.AckSync()
		return nil
	}
	data := object.(datatypes.ChangeEventPayload)
	p.logger.Trace("decoded object: %s for msgid: %s", object, msgid)
	dumpFiles := p.dumpMessagesDir != ""
	if dumpFiles {
		ext := ".json"
		if gzipped {
			ext += ".gz"
		}
		fn := path.Join(p.dumpMessagesDir, fmt.Sprintf("%s_%v_%s%s", model, time.Now().UnixNano(), msgid, ext))
		os.WriteFile(fn, msg.Data, 0644)
	}
	if err := p.provider.Process(data); err != nil {
		p.logger.Error("error processing change event: %s. %s", object, err)
	}
	if err := msg.AckSync(); err != nil {
		p.logger.Error("error calling ack for message: %s. %s", object, err)
	}
	return nil
}

// Start will start processing messages
func (p *MessageProcessor) Start() error {
	p.logger.Trace("message processor starting")
	name := fmt.Sprintf("%seds-server-%s", p.consumerPrefix, p.companyID)
	description := fmt.Sprintf("EDS server consumer for %s", p.companyID)
	companyId := p.companyID
	if companyId == "" {
		companyId = "*"
	}
	c, err := snats.NewExactlyOnceConsumerWithConfig(snats.ExactlyOnceConsumerConfig{
		Context:             p.context,
		Logger:              p.logger,
		JetStream:           p.js,
		StreamName:          "dbchange",
		DurableName:         name,
		ConsumerDescription: description,
		FilterSubject:       "dbchange.*.*." + companyId + ".>",
		Handler:             p.callback,
		DeliverPolicy:       nats.DeliverAllPolicy,
		Deliver:             nats.DeliverAll(),
	})
	if err != nil {
		return err
	}
	p.subscriber = c
	p.logger.Trace("message processor started")
	return nil
}

// Stop will stop processing messages
func (p *MessageProcessor) Stop() error {
	p.logger.Trace("message processor stopping")
	p.cancel()
	if p.subscriber != nil {
		p.subscriber.Close()
	}
	p.logger.Trace("message processor stopped")
	return nil
}
