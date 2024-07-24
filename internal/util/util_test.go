package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestToFileURI(t *testing.T) {
	fileURL := ToFileURI("/var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919", "*.ndjson.gz")
	assert.Equal(t, "file:///var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/*.ndjson.gz", fileURL)
	fileURL = ToFileURI("/var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/", "*.ndjson.gz")
	assert.Equal(t, "file:///var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/*.ndjson.gz", fileURL)
}

func TestParsePreciseDate(t *testing.T) {
	date, err := parsePreciseDate("202407242003015854988560000000000")
	assert.NoError(t, err)
	if err != nil {
		assert.FailNow(t, "error parsing date")
	}
	assert.Equal(t, "2024-07-24T20:03:01.585498856Z", date.Format(time.RFC3339Nano))
}

func TestParseCRDBExportFile(t *testing.T) {
	table, ts, ok := ParseCRDBExportFile("202407131650522808024600000000000-c7274317e9a4a9cb-1-651-00000000-user-2.ndjson.gz")
	assert.True(t, ok)
	assert.Equal(t, "user", table)

	table2, ts2, ok := ParseCRDBExportFile("202407131650522808024800000000000-c7274317e9a4a9cb-1-651-00000000-user-2.ndjson.gz")
	assert.True(t, ok)
	assert.Equal(t, "user", table2)
	assert.True(t, ts.Before(ts2))

	table, ts, ok = ParseCRDBExportFile("202407131650522808024700000000000-c7274317e9a4a9cb-1-651-00000000-user-14a.ndjson.gz")
	assert.True(t, ok)
	assert.Equal(t, "user", table)

	table2, ts2, ok = ParseCRDBExportFile("202407131650522808024600000000000-c7274317e9a4a9cb-1-651-00000000-labor_rate-2.ndjson.gz")
	assert.True(t, ok)
	assert.Equal(t, "labor_rate", table2)
	assert.True(t, ts2.Before(ts))
}
