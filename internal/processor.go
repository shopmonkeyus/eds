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
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/types"
	"github.com/shopmonkeyus/go-common/logger"
	snats "github.com/shopmonkeyus/go-common/nats"
)

var emptyJSON = []byte("{}")

const modelRequestTimeout = time.Duration(time.Second * 30)

type MessageProcessor struct {
	logger            logger.Logger
	companyID         string
	provider          Provider
	conn              *nats.Conn
	js                nats.JetStreamContext
	subscriber        snats.Subscriber
	dumpMessagesDir   string
	consumerPrefix    string
	context           context.Context
	cancel            context.CancelFunc
	modelVersionCache map[string]dm.Model
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

	context, cancel := context.WithCancel(context.Background())
	processor := &MessageProcessor{
		logger:            opts.Logger.WithPrefix("[nats]"),
		companyID:         opts.CompanyID,
		provider:          opts.Provider,
		conn:              opts.NatsConnection,
		dumpMessagesDir:   opts.DumpMessagesDir,
		consumerPrefix:    opts.ConsumerPrefix,
		js:                js,
		context:           context,
		cancel:            cancel,
		modelVersionCache: make(map[string]dm.Model),
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

	var schema dm.Model

	modelVersion := data.ModelVersion
	modelVersionId := fmt.Sprintf("%s-%s", model, modelVersion)

	currentModelVersion, found := p.modelVersionCache[modelVersionId]

	if found {
		schema = currentModelVersion
		p.logger.Trace("found cached modelVersion for: %s for msgid: %s", modelVersionId, msgid)

	} else {
		// lookup version in nats kv
		p.logger.Trace("looking up modelVersion for: %s for msgid: %s", modelVersionId, msgid)

		entry, err := p.conn.Request(fmt.Sprintf("schema.%s.%s", model, modelVersion), emptyJSON, modelRequestTimeout)

		if err != nil {
			p.logger.Error("error fetching change event schema: %s. %s", data, err)
			return err
		}
		var foundSchema types.SchemaResponse
		err = json.Unmarshal(entry.Data, &foundSchema)
		if err != nil {
			p.logger.Error("error unmarshalling change event schema: %s. %s", string(entry.Data), err)
			return err
		}
		p.logger.Trace("got schema for: %s %v for msgid: %s", modelVersionId, foundSchema.Data, msgid)
		schema = foundSchema.Data
		p.modelVersionCache[modelVersionId] = schema
	}
	if err := p.provider.Process(data, schema); err != nil {
		p.logger.Error("error processing change event: %s. %s", data, err)
		return err
	}
	if err := msg.AckSync(); err != nil {
		p.logger.Error("error calling ack for message: %s. %s", data, err)
		return err
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
	c, err := snats.NewExactlyOnceConsumer(p.logger, p.js, "dbchange", name, "dbchange.*.*."+companyId+".*.PUBLIC.>", p.callback,
		snats.WithExactlyOnceContext(p.context),
		snats.WithExactlyOnceReplicas(1), // TODO: make configurable for testing
	)
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
		if err := p.subscriber.Close(); err != nil {
			return err
		}
	}
	p.logger.Trace("message processor stopped")
	return nil
}
