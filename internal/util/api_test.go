package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAPIURLFromJWT(t *testing.T) {
	url, err := GetAPIURLFromJWT("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhcGkiLCJjaWQiOiI2Mjg3YTQxNTRkMWE3MmNjNWNlMDkxYmIiLCJpZCI6IjYyODdhNDA0NGQxYTcyM2IxMGUwOTFiOSIsImxpZCI6IjYyODdhNDA0NGQxYTcyM2IxMGVmZjFiMCIsIm9uIjo2LCJyaWQiOiJ1dzEiLCJzYWQiOjAsInNpZCI6IjM2MjczMzYwZWZkMDA1ZjgiLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjMxMDEiLCJpYXQiOjE3MjI0ODY4MzJ9.5fPgQJFBuZWBCaXsPN7uKXKsamfkxP5ssEBI3EECEv0")
	assert.NoError(t, err)
	assert.Equal(t, "http://localhost:3101", url)
}
