package eventhub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	var driver eventHubDriver

	url, errs := driver.Validate(map[string]any{
		"Connection String": "Endpoint=sb://shopmonkey-xx-test.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=x/x+x+x+x=;EntityPath=shopmonkey-eds-test",
	})
	assert.Empty(t, errs)
	assert.Equal(t, "eventhub://shopmonkey-xx-test.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=x/x+x+x+x=;EntityPath=shopmonkey-eds-test", url)
}
