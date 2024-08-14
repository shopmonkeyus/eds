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

func TestMaskArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "Mask URL",
			args: []string{"https://alice:bob@example.com/a/b?foo=bar", "http://user:password@localhost:8080/path?query=1", "s3://bucket/folder?region=us-west-2&access-key-id=AKIAIOSFODNN7EXAMPLE&secret-access", "mysql://user:password@localhost:3306/db?query=1"},
			want: []string{"https://al***:b**@example.com/a**?foo=b**", "http://us**:pass****@localhost:8080/pa**?query=*", "s3://bucket/fol***?access-key-id=AKIAIOSFOD**********&region=us-w*****&secret-access=", "mysql://us**:pass****@localhost:3306/d*?query=*"},
		},
		{
			name: "Mask Email",
			args: []string{"user@example.com", "another.user@example.com"},
			want: []string{"us**@exa****.com", "anothe******@exa****.com"},
		},
		{
			name: "Mask JWT",
			args: []string{"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"},
			want: []string{"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6Ikpv******************************************************************************"},
		},
		{
			name: "No Masking Needed",
			args: []string{"hello", "world"},
			want: []string{"hello", "world"},
		},
		{
			name: "Mixed Arguments",
			args: []string{"http://example.com", "user@example.com", "hello"},
			want: []string{"http://example.com", "us**@exa****.com", "hello"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MaskArguments(tt.args)
			assert.Equal(t, tt.want, got)
		})
	}

}
