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
	os.Remove(tmpdir) // remove initially
	defer os.RemoveAll(tmpdir)

	url, errs := driver.Validate(map[string]any{
		"Directory": tmpdir,
	})
	assert.Empty(t, errs)
	assert.Equal(t, "file://"+tmpdir, url)

	// again since we have created it
	url, errs = driver.Validate(map[string]any{
		"Directory": tmpdir,
	})
	assert.Empty(t, errs)
	assert.Equal(t, "file://"+tmpdir, url)
}
