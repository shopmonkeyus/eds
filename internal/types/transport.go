package types

type Transport struct {
	DBChange ChangeEventPayload `json:"data"`
	Schema   Table              `json:"schema"`
}
