package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNatsURLFromAPIURL(t *testing.T) {
	tests := []struct {
		apiURL  string
		natsURL string
	}{
		{"https://api.shopmonkey.cloud", defaultNatsURL},
		{"https://api.shopmonkey.cloud/", defaultNatsURL},
		{"https://edge-api.shopmonkey.cloud", "nats://connect.nats-test.shopmonkey.pub"},
		{"http://localhost:3101", "nats://localhost:4222"},
		{"https://sandbox-api.shopmonkey.cloud", defaultNatsURL},
		{"https://unknown.example.com", defaultNatsURL},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.natsURL, GetNatsURLFromAPIURL(tt.apiURL), tt.apiURL)
	}
}
