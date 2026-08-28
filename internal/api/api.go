package api

import (
	"errors"
	"strings"
)

type DriverMeta struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	URL         string `json:"url"` // this is masked since it can contain sensitive information
}

type SessionStart struct {
	Version    string      `json:"version"`
	Hostname   string      `json:"hostname"`
	IPAddress  string      `json:"ipAddress"`
	MachineId  string      `json:"machineId"`
	OsInfo     any         `json:"osinfo"`
	Driver     *DriverMeta `json:"driver,omitempty"`
	ServerID   string      `json:"serverId"`
	CompanyIDs []string    `json:"companyIds,omitempty"`
	UseEdsV4Prototype bool `json:"useEdsV4Prototype,omitempty"`
}

type EdsSession struct {
	SessionId      string                    `json:"sessionId"`
	Credential     *string                   `json:"credential"`
	EdsV4Prototype *EdsV4PrototypeConnection `json:"edsV4Prototype,omitempty"`
}

type SessionStartResponse struct {
	Success bool       `json:"success"`
	Message string     `json:"message"`
	Data    EdsSession `json:"data"`
}

type SessionEnd struct {
	Errored bool `json:"errored"`
}

type SessionEndURLs struct {
	URL      string `json:"url"`
	ErrorURL string `json:"errorUrl"`
}

type SessionEndResponse struct {
	Success bool           `json:"success"`
	Message string         `json:"message"`
	Data    SessionEndURLs `json:"data"`
}

type EnrollTokenData struct {
	Token    string `json:"token" toml:"token"`
	ServerID string `json:"serverId" toml:"server_id"`
	NatsURL  string `json:"natsUrl,omitempty" toml:"nats_url,omitempty"`
}

type EnrollResponse struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    EnrollTokenData `json:"data"`
}

const defaultNatsURL = "nats://connect.nats.shopmonkey.pub"

type environment struct {
	API  string
	NATS string
}

var environments = map[string]environment{
	"P": {API: "https://api.shopmonkey.cloud", NATS: defaultNatsURL},
	"S": {API: "https://sandbox-api.shopmonkey.cloud", NATS: "nats://connect.nats-sandbox.shopmonkey.pub"},
	"E": {API: "https://edge-api.shopmonkey.cloud", NATS: "nats://connect.nats-test.shopmonkey.pub"},
	"L": {API: "http://localhost:3101", NATS: "nats://localhost:4222"},
}

func GetAPIURL(firstLetter string) (*string, error) {
	env, err := getEnvironment(firstLetter)
	if err != nil {
		return nil, err
	}
	return &env.API, nil
}

func GetNatsURL(firstLetter string) (string, error) {
	env, err := getEnvironment(firstLetter)
	if err != nil {
		return "", err
	}
	return env.NATS, nil
}

func getEnvironment(code string) (environment, error) {
	code = strings.ToUpper(strings.TrimSpace(code))
	if code == "" {
		return environment{}, errors.New("invalid code")
	}
	code = code[0:1]
	env, exists := environments[code]
	if !exists {
		return environment{}, errors.New("invalid code")
	}
	return env, nil
}
