package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal/types"
	"github.com/shopmonkeyus/go-common/logger"
	snats "github.com/shopmonkeyus/go-common/nats"
)

type MessageProcessor struct {
	logger          logger.Logger
	companyID       string
	provider        Provider
	conn            *nats.Conn
	js              nats.JetStreamContext
	kv              nats.KeyValue
	subscriber      snats.Subscriber
	dumpMessagesDir string
	consumerPrefix  string
	context         context.Context
	cancel          context.CancelFunc
	tableLookup     map[string]interface{}
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

	// kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
	// 	Bucket: "datamodel-version",
	// })

	kv, err := js.KeyValue("datamodel-version")

	if err != nil {
		return nil, err
	}
	// model version cache
	tableLookup := make(map[string]interface{})

	context, cancel := context.WithCancel(context.Background())
	processor := &MessageProcessor{
		logger:          opts.Logger,
		companyID:       opts.CompanyID,
		provider:        opts.Provider,
		conn:            opts.NatsConnection,
		dumpMessagesDir: opts.DumpMessagesDir,
		consumerPrefix:  opts.ConsumerPrefix,
		js:              js,
		kv:              kv,
		context:         context,
		cancel:          cancel,
		tableLookup:     tableLookup,
	}
	return processor, nil
}

// callback processes db change events
func (p *MessageProcessor) callback(ctx context.Context, payload []byte, msg *nats.Msg) error {
	tok := strings.Split(msg.Subject, ".")
	model := tok[1]

	msgid := msg.Header.Get("Nats-Msg-Id")
	encoding := msg.Header.Get("content-encoding")
	gzipped := encoding == "gzip/json"
	p.logger.Trace("received msgid: %s, subject: %s", msgid, msg.Subject)

	/* TODO:
	- unpack data as json (moving away from go-datamodel)
	- lookup object version in object version kv
	- compare against current version and determine schema change needs
		- generate schema change object
	- send data item and schema change needs to provider
	*/

	// unpack as json based change  event
	data, err := types.FromChangeEvent(msg.Data, gzipped)
	if err != nil {
		if gzipped {
			dec, _ := types.Gunzip(msg.Data)
			p.logger.Error("decode error for change event: %s. %s", string(dec), err)
		} else {
			p.logger.Error("decode error for change event: %s. %s", string(msg.Data), err)
		}
		msg.AckSync()
		return nil
	}
	p.logger.Trace("decoded object: %s for msgid: %s", data, msgid)
	dumpFiles := p.dumpMessagesDir != ""
	if dumpFiles {
		ext := ".json"
		if gzipped {
			ext += ".gz"
		}
		fn := path.Join(p.dumpMessagesDir, fmt.Sprintf("%s_%v_%s%s", model, time.Now().UnixNano(), msgid, ext))
		os.WriteFile(fn, msg.Data, 0644)
	}

	// TODO: lookup schema in nats KV and send s
	incomingModelVersion := data.GetModelVersion()

	var schema map[string]interface{}

	currentModelVersion, found := p.tableLookup[model]

	if currentModelVersion != nil {
		schema = currentModelVersion.(map[string]interface{})
	}

	if !found || currentModelVersion != *incomingModelVersion {
		// lookup version in nats kv
		entry, err := p.kv.Get(fmt.Sprintf("%s.%s", model, *incomingModelVersion))

		if err != nil {
			p.logger.Error("error processing change event schema: %s. %s", data, err)
			return err
		}

		err = json.Unmarshal(entry.Value(), &schema)
		if err != nil {
			p.logger.Error("error processing change event schema: %s. %s", data, err)
			return err
		}

	}
	if err := p.provider.Process(data, schema); err != nil {
		p.logger.Error("error processing change event: %s. %s", data, err)
	}
	if err := msg.AckSync(); err != nil {
		p.logger.Error("error calling ack for message: %s. %s", data, err)
	}
	return nil
}

// Start will start processing messages
func (p *MessageProcessor) Start() error {
	p.logger.Trace("message processor starting")
	name := fmt.Sprintf("%seds-server-%s", p.consumerPrefix, p.companyID)
	companyId := p.companyID
	if companyId == "" {
		companyId = "*"
	}
	c, err := snats.NewExactlyOnceConsumer(p.logger, p.js, "dbchange", name, "dbchange.*.*."+companyId+".>", p.callback, snats.WithExactlyOnceContext(p.context))
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
