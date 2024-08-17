package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	var driver kafkaDriver
	url, err := driver.Validate(map[string]any{
		"Hostname": "hostname",
		"Topic":    "topic",
	})
	assert.Empty(t, err)
	assert.Equal(t, "kafka://hostname:9092/topic", url)

	url, err = driver.Validate(map[string]any{
		"Hostname": "hostname",
		"Topic":    "topic",
		"Port":     9999,
	})
	assert.Empty(t, err)
	assert.Equal(t, "kafka://hostname:9999/topic", url)
}
