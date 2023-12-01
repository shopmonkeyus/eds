package internal

import (
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal/datatypes"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
)

type Provider interface {
	// Start the provider and return an error or nil if ok
	Start() error
	// Stop the provider and return an error or nil if ok
	Stop() error
	// Process data received and return an error or nil if processed ok
	Process(data datatypes.ChangeEventPayload, schema dm.Model) error
	// Import data received and return an error or nil if processed ok
	//TODO: Change this to be a MigrateEventPayload
	Import(data []byte, nc *nats.Conn) error
}
