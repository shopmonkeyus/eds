package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAPIURL(t *testing.T) {
	url, err := GetAPIURL("P")
	require.NoError(t, err)
	assert.Equal(t, "https://api.shopmonkey.cloud", *url)

	url, err = GetAPIURL("e")
	require.NoError(t, err)
	assert.Equal(t, "https://edge-api.shopmonkey.cloud", *url)

	_, err = GetAPIURL("X")
	assert.Error(t, err)
}

func TestGetNatsURL(t *testing.T) {
	for code, env := range environments {
		natsURL, err := GetNatsURL(code)
		require.NoError(t, err)
		assert.Equal(t, env.NATS, natsURL, code)

		natsURL, err = GetNatsURL(strings.ToLower(code))
		require.NoError(t, err)
		assert.Equal(t, env.NATS, natsURL, code)
	}

	_, err := GetNatsURL("X")
	assert.Error(t, err)
}
