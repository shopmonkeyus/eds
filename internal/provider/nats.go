package provider

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/types"
	"github.com/shopmonkeyus/go-common/logger"
)

type NatsProvider struct {
	logger logger.Logger
	nc     *nats.Conn
	js     nats.JetStreamContext
	opts   *ProviderOpts
}

var _ internal.Provider = (*NatsProvider)(nil)

// NewFileProvider returns a provider that will stream files to a folder provided in the url
func NewNatsProvider(logger logger.Logger, urlstring string, opts *ProviderOpts) (internal.Provider, error) {
	streamConfigJSON, err := ioutil.ReadFile("stream.conf")
	if err != nil {
		return nil, fmt.Errorf("1/2: unable to find and open stream config with error: %s", err)
	}
	streamConfig := nats.StreamConfig{}
	err = json.Unmarshal([]byte(streamConfigJSON), &streamConfig)
	if err != nil {
		if e, ok := err.(*json.SyntaxError); ok {
			logger.Error("syntax error at byte offset %d", e.Offset)
		}
		return nil, fmt.Errorf("2/2: unable to parse stream config with error: %s", err)
	}

	nc, err := nats.Connect(urlstring)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to nats server: %s with error: %s", urlstring, err)
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("unable to configure Jetstream: %s", err)
	}

	_, err = js.AddStream(&streamConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to configure Jetstream: %s", err)
	}

	logger.Info("nats provider will publish to: %s", urlstring)
	return &NatsProvider{
		logger,
		nc,
		js,
		opts,
	}, nil
}

// Start the provider and return an error or nil if ok
func (p *NatsProvider) Start() error {
	return nil
}

// Stop the provider and return an error or nil if ok
func (p *NatsProvider) Stop() error {
	p.nc.Close()
	return nil
}

// Process data received and return an error or nil if processed ok
func (p *NatsProvider) Process(data types.ChangeEventPayload, schema map[string]interface{}) error {
	p.logger.Debug("Republish Message to: dbchange.%s.%s.%s", data.GetTable(), data.GetOperation(), data.GetLocationID())
	location := "NONE"
	if data.GetLocationID() != nil {
		location = *data.GetLocationID()
	}
	subject := fmt.Sprintf("dbchange.%s.%s.%s", data.GetTable(), data.GetOperation(), location)

	buf, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		return err
	}

	p.logger.Debug("Republish Message with Payload", string(buf))
	if p.opts != nil && p.opts.DryRun {
		p.logger.Info("[dry-run] would publish to: %s", subject)
		return nil
	}

	_, err = p.js.Publish(subject, buf)
	if err != nil {
		return err
	}

	return nil
}
