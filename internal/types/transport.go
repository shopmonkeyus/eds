package types

import (
	dm "github.com/shopmonkeyus/eds-server/internal/model"
)

type Transport struct {
	DBChange ChangeEventPayload `json:"data"`
	Schema   dm.Model           `json:"schema"`
}

type SchemaResponse struct {
	Success bool     `json:"success"`
	Data    dm.Model `json:"data"`
}
