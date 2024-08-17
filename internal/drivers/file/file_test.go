package file

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	var driver fileDriver

	tmpdir, err := os.MkdirTemp("", "test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	url, errs := driver.Validate(map[string]any{
		"Directory": tmpdir,
	})
	assert.Empty(t, errs)
	assert.Equal(t, "file://"+tmpdir, url)
}
