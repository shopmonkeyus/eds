package file

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	tests := []struct {
		name        string
		config      map[string]any
		expectedURL string
		expectError bool
	}{
		{
			name:        "valid directory",
			config:      map[string]any{"Directory": tmpdir},
			expectedURL: "file://" + tmpdir,
		},
		{
			name:        "missing required field Directory",
			config:      map[string]any{"Format": "json"},
			expectError: true,
		},
	}

	var driver fileDriver
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
