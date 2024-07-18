package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractCompanyIdFromSubscription(t *testing.T) {
	assert.Equal(t, "6287a4154d1a72cc5ce091bb", extractCompanyIdFromDBChangeSubscription("dbchange.*.*.6287a4154d1a72cc5ce091bb.*.PUBLIC.>"))
	assert.Equal(t, "", extractCompanyIdFromDBChangeSubscription("_INBOX.>"))
	assert.Equal(t, "", extractCompanyIdFromDBChangeSubscription("eds.notify.284e8bdb-9c18-45c3-9f18-844ad70610ef.>"))
	assert.Equal(t, "", extractCompanyIdFromDBChangeSubscription("eds.b"))
}

func TestExtractSessionIdFromEdsSubscription(t *testing.T) {
	assert.Equal(t, "", extractSessionIdFromEdsSubscription("dbchange.*.*.6287a4154d1a72cc5ce091bb.*.PUBLIC.>"))
	assert.Equal(t, "", extractSessionIdFromEdsSubscription("_INBOX.>"))
	assert.Equal(t, "284e8bdb-9c18-45c3-9f18-844ad70610ef", extractSessionIdFromEdsSubscription("eds.notify.284e8bdb-9c18-45c3-9f18-844ad70610ef.>"))
	assert.Equal(t, "", extractSessionIdFromEdsSubscription("eds.b"))
}
