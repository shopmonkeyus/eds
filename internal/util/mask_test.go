package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaskUrl(t *testing.T) {
	u, err := MaskURL("http://user:password@localhost:8080/path?query=1")
	assert.NoError(t, err)
	assert.Equal(t, "http://us**:pass****@localhost:8080/pa**?query=*", u)

	u, err = MaskURL("snowflake://FOO:thisisapassword@TFLXCJY-LU41011/TEST/PUBLIC")
	assert.NoError(t, err)
	assert.Equal(t, "snowflake://F**:thisisa********@TFLXCJY-LU41011/TEST/******", u)

	u, err = MaskURL("s3://bucket/folder?region=us-west-2&access-key-id=AKIAIOSFODNN7EXAMPLE&secret-access-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	assert.NoError(t, err)
	assert.Equal(t, "s3://bucket/fol***?access-key-id=AKIAIOSFOD**********&region=us-w*****&secret-access-key=wJalrXUtnFEMI/K7MDEN********************", u)
}
