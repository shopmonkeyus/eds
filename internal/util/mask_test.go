package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaskUrl(t *testing.T) {
	u, err := MaskURL("http://user:password@localhost:8080/path?query=1")
	assert.NoError(t, err)
	assert.Equal(t, "http://us**:pass****@localhost:8080/pa**?que****", u)

	u, err = MaskURL("snowflake://FOO:thisisapassword@TFLXCJY-LU41011/TEST/PUBLIC")
	assert.NoError(t, err)
	assert.Equal(t, "snowflake://F**:thisisa********@TFLXCJY-LU41011/TEST/******", u)
}
