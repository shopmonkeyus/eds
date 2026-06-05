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
}

type EdsSession struct {
	SessionId  string  `json:"sessionId"`
	Credential *string `json:"credential"`
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
}

type EnrollResponse struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    EnrollTokenData `json:"data"`
}

const defaultNatsURL = "nats://connect.nats.shopmonkey.pub"

var apiToNatsURLs = map[string]string{
	"https://api.shopmonkey.cloud":      defaultNatsURL,
	"https://edge-api.shopmonkey.cloud": "nats://connect.nats-test.shopmonkey.pub",
	"http://localhost:3101":             "nats://localhost:4222",
}

func GetAPIURL(firstLetter string) (*string, error) {
	apiUrls := map[string]string{
		"P": "https://api.shopmonkey.cloud",
		"S": "https://sandbox-api.shopmonkey.cloud",
		"E": "https://edge-api.shopmonkey.cloud",
		"L": "http://localhost:3101",
	}

	if url, exists := apiUrls[firstLetter]; exists {
		return &url, nil
	}
	return nil, errors.New("invalid code")
}

// GetNatsURLFromAPIURL returns the NATS server URL for a Shopmonkey API URL.
func GetNatsURLFromAPIURL(apiURL string) string {
	apiURL = strings.TrimSuffix(apiURL, "/")
	if natsURL, ok := apiToNatsURLs[apiURL]; ok {
		return natsURL
	}
	return defaultNatsURL
}
