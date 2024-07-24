package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToFileURI(t *testing.T) {
	fileURL := ToFileURI("/var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919", "*.ndjson.gz")
	assert.Equal(t, "file:///var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/*.ndjson.gz", fileURL)
	fileURL = ToFileURI("/var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/", "*.ndjson.gz")
	assert.Equal(t, "file:///var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/*.ndjson.gz", fileURL)
}

func TestParseCRDBExportFile(t *testing.T) {
	res, ok := ParseCRDBExportFile("202407131650522808024600000000000-c7274317e9a4a9cb-1-651-00000000-user-2.ndjson.gz")
	assert.True(t, ok)
	assert.Equal(t, "user", res.Table)

	res2, ok := ParseCRDBExportFile("202407131650522808024600000000001-c7274317e9a4a9cb-1-651-00000000-user-2.ndjson.gz")
	assert.True(t, ok)
	assert.Equal(t, "user", res.Table)
	assert.True(t, res.Less(res2))

	res, ok = ParseCRDBExportFile("202407131650522808024700000000000-c7274317e9a4a9cb-1-651-00000000-user-2.ndjson.gz")
	assert.True(t, ok)
	assert.Equal(t, "user", res.Table)

	res2, ok = ParseCRDBExportFile("202407131650522808024600000000000-c7274317e9a4a9cb-1-651-00000000-user-2.ndjson.gz")
	assert.True(t, ok)
	assert.Equal(t, "user", res.Table)
	assert.True(t, res2.Less(res))
}
