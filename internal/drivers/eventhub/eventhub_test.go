package eventhub

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
			name:        "valid connection string",
			config:      map[string]any{"Connection String": "Endpoint=sb://shopmonkey-xx-test.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=x/x+x+x+x=;EntityPath=shopmonkey-eds-test"},
			expectedURL: "eventhub://shopmonkey-xx-test.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=x/x+x+x+x=;EntityPath=shopmonkey-eds-test",
		},
		{
			name:        "missing required field Connection String",
			config:      map[string]any{"Format": "json"},
			expectError: true,
		},
	}

	var driver eventHubDriver
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
