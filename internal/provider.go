package internal

import "github.com/shopmonkeyus/go-datamodel/datatypes"

type Provider interface {
	// Start the provider and return an error or nil if ok
	Start() error
	// Stop the provider and return an error or nil if ok
	Stop() error
	// Process data received and return an error or nil if processed ok
	Process(data datatypes.ChangeEventPayload) error
	// Migrate will tell the provider to do any migration work and return an error or nil if ok
	Migrate() error
}
