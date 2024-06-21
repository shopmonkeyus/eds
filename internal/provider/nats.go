package provider

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal/datatypes"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/go-common/logger"
)

type NatsProvider struct {
	logger logger.Logger
	nc     *nats.Conn
	js     nats.JetStreamContext
	opts   *ProviderOpts
}

func NewNatsProvider(logger logger.Logger, urlstring string, opts *ProviderOpts, remoteNc *nats.Conn) (*NatsProvider, error) {

	defaultStreamConfig := nats.StreamConfig{}
	defaultStreamConfig.Name = "dbchange"
	defaultStreamConfig.Subjects = []string{"dbchange.>"}
	defaultStreamConfig.Description = "dbchange stream for eds"
	defaultStreamConfig.MaxConsumers = -1
	defaultStreamConfig.MaxMsgs = -1
	defaultStreamConfig.MaxBytes = -1
	defaultStreamConfig.DiscardNewPerSubject = false
	defaultStreamConfig.MaxAge = 604800000000000
	defaultStreamConfig.MaxMsgsPerSubject = -1
	defaultStreamConfig.MaxMsgSize = -1
	defaultStreamConfig.Replicas = 1
	defaultStreamConfig.NoAck = false
	defaultStreamConfig.Duplicates = 86400000000000
	defaultStreamConfig.Sealed = false
	defaultStreamConfig.DenyDelete = false
	defaultStreamConfig.DenyPurge = false
	defaultStreamConfig.AllowRollup = false
	var natsFileStorageType nats.StorageType
	natsFileStorageType = nats.FileStorage
	defaultStreamConfig.Storage = natsFileStorageType

	streamConfig := nats.StreamConfig{}

	streamConfigJSON, err := os.ReadFile("stream.conf")
	if err != nil {
		if os.IsNotExist(err) {
			streamConfig = defaultStreamConfig
		} else {
			return nil, fmt.Errorf("issue reading stream config: %w", err)
		}
	} else {
		err = json.Unmarshal(streamConfigJSON, &streamConfig)
		if err != nil {
			if e, ok := err.(*json.SyntaxError); ok {
				logger.Error("syntax error at byte offset %d", e.Offset)
			}
			return nil, fmt.Errorf("unable to parse stream config: %w", err)
		}
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
func (p *NatsProvider) Process(data datatypes.ChangeEventPayload, schema dm.Model) error {
	location := "NONE"
	if data.GetLocationID() != nil {
		location = *data.GetLocationID()
	}
	companyId := *data.GetCompanyID()

	subject := fmt.Sprintf("dbchange.%s.%s.%s.%s.PUBLIC.%d.%s", data.GetTable(), data.GetOperation(), companyId, location, data.GetVersion(), data.GetID())
	p.logger.Debug(`Republish Message to: %s`, subject)

	buf, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		return err
	}
	msg := nats.NewMsg(subject)
	msg.Data = buf
	msg.Header.Set(nats.MsgIdHdr, data.GetID())

	_, err = p.js.PublishMsg(msg)
	if err != nil {
		return err
	}

	return nil
}

func (p *NatsProvider) Import(dataMap map[string]interface{}, tableName string, nc *nats.Conn) error {

	return nil
}

func (p *NatsProvider) GetNatsConn() *nats.Conn {
	return p.nc
}

func (p *NatsProvider) AddHealthCheck() error {
	_, err := p.nc.Subscribe("health", func(msg *nats.Msg) {
		p.nc.Publish(msg.Reply, []byte("I'm healthy"))
		p.logger.Debug("NATS server is healthy")
	})
	if err != nil {
		err = fmt.Errorf("error subscribing to health subject: %v", err)
		return err
	}
	p.logger.Info("NATS server is now listening for health check requests")
	return nil
}
