package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]any
		expectedURL string
		expectError bool
	}{
		{
			name:        "minimal config",
			config:      map[string]any{"Hostname": "hostname", "Topic": "topic"},
			expectedURL: "kafka://hostname:9092/topic",
		},
		{
			name:        "with custom port",
			config:      map[string]any{"Hostname": "hostname", "Topic": "topic", "Port": 9999},
			expectedURL: "kafka://hostname:9999/topic",
		},
		{
			name:        "missing required field Topic",
			config:      map[string]any{"Hostname": "hostname"},
			expectError: true,
		},
	}

	var driver kafkaDriver
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url, errs := driver.Validate(tt.config)
			if tt.expectError {
				assert.GreaterOrEqual(t, len(errs), 1)
				assert.Equal(t, "", url)
			} else {
				assert.Empty(t, errs)
				assert.Equal(t, tt.expectedURL, url)
			}
		})
	}
}
