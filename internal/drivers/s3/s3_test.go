package s3

import (
	"net/url"
	"testing"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/stretchr/testify/assert"
)

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func TestGetBucketInfoAWS(t *testing.T) {
	var url, bucket, prefix string
	url, bucket, prefix = getBucketInfo(mustParseURL("s3://bucket?region=us-east-1"), awsProvider)
	assert.Equal(t, "bucket", bucket)
	assert.Equal(t, "", prefix)
	assert.Equal(t, "", url)

	// // aws s3
	url, bucket, prefix = getBucketInfo(mustParseURL("s3://foo-shopmonkey/test?region=us-west-1"), awsProvider)
	assert.Equal(t, "foo-shopmonkey", bucket)
	assert.Equal(t, "test/", prefix)
	assert.Equal(t, "", url)

}

func TestGetBucketInfoLocalstack(t *testing.T) {

	// localstack
	url, bucket, prefix := getBucketInfo(mustParseURL("s3://localhost:4566/foo-shopmonkey/test?region=us-west-1"), localstackProvider)
	assert.Equal(t, "foo-shopmonkey", bucket)
	assert.Equal(t, "test/", prefix)
	assert.Equal(t, "http://localhost:4566", url)

	url, bucket, prefix = getBucketInfo(mustParseURL("s3://localhost:4566/foo-shopmonkey/test/?region=us-west-1"), localstackProvider)
	assert.Equal(t, "foo-shopmonkey", bucket)
	assert.Equal(t, "test/", prefix)
	assert.Equal(t, "http://localhost:4566", url)

	url, bucket, prefix = getBucketInfo(mustParseURL("s3://127.0.0.1:4566/foo-shopmonkey/test/?region=us-west-1"), localstackProvider)
	assert.Equal(t, "foo-shopmonkey", bucket)
	assert.Equal(t, "test/", prefix)
	assert.Equal(t, "http://127.0.0.1:4566", url)
}

func TestGetBucketInfoGCP(t *testing.T) {
	var url, bucket, prefix string
	// google cloud storage
	url, bucket, prefix = getBucketInfo(mustParseURL("s3://storage.googleapis.com/eds-import"), googleProvider)
	assert.Equal(t, "eds-import", bucket)
	assert.Equal(t, "", prefix)
	assert.Equal(t, "https://storage.googleapis.com", url)

	url, bucket, prefix = getBucketInfo(mustParseURL("s3://storage.googleapis.com/eds-import/withprefix"), googleProvider)
	assert.Equal(t, "eds-import", bucket)
	assert.Equal(t, "withprefix/", prefix)
	assert.Equal(t, "https://storage.googleapis.com", url)

	url, bucket, prefix = getBucketInfo(mustParseURL("s3://storage.googleapis.com/eds-import/with/prefix"), googleProvider)
	assert.Equal(t, "eds-import", bucket)
	assert.Equal(t, "with/prefix/", prefix)
	assert.Equal(t, "https://storage.googleapis.com", url)

	url, bucket, prefix = getBucketInfo(mustParseURL("s3://storage.googleapis.com/eds-import/with/prefix/"), googleProvider)
	assert.Equal(t, "eds-import", bucket)
	assert.Equal(t, "with/prefix/", prefix)
	assert.Equal(t, "https://storage.googleapis.com", url)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]any
		expectedURL string
		expectError bool
	}{
		{
			name:        "minimal config",
			config:      map[string]any{"Bucket": "bucket"},
			expectedURL: "s3://bucket",
		},
		{
			name:        "with prefix",
			config:      map[string]any{"Bucket": "bucket", "Prefix": "prefix"},
			expectedURL: "s3://bucket/prefix",
		},
		{
			name:        "with leading slash prefix",
			config:      map[string]any{"Bucket": "bucket", "Prefix": "/prefix"},
			expectedURL: "s3://bucket/prefix",
		},
		{
			name:        "with endpoint and prefix",
			config:      map[string]any{"Bucket": "bucket", "Prefix": "/prefix", "Endpoint": "storage.googleapis.com"},
			expectedURL: "s3://storage.googleapis.com/bucket/prefix",
		},
		{
			name:        "with endpoint prefix no slash",
			config:      map[string]any{"Bucket": "bucket", "Prefix": "prefix", "Endpoint": "storage.googleapis.com"},
			expectedURL: "s3://storage.googleapis.com/bucket/prefix",
		},
		{
			name:        "with endpoint only",
			config:      map[string]any{"Bucket": "bucket", "Endpoint": "storage.googleapis.com"},
			expectedURL: "s3://storage.googleapis.com/bucket",
		},
		{
			name:        "with access keys",
			config:      map[string]any{"Bucket": "bucket", "Access Key ID": "AKIAIOSFODNN7EXAMPLE", "Secret Access Key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
			expectedURL: "s3://bucket?access-key-id=AKIAIOSFODNN7EXAMPLE&secret-access-key=wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY",
		},
		{
			name:        "with region and access keys",
			config:      map[string]any{"Bucket": "bucket", "Region": "us-east-1", "Access Key ID": "AKIAIOSFODNN7EXAMPLE", "Secret Access Key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
			expectedURL: "s3://bucket?access-key-id=AKIAIOSFODNN7EXAMPLE&region=us-east-1&secret-access-key=wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY",
		},
		{
			name:        "full config",
			config:      map[string]any{"Bucket": "bucket", "Endpoint": "storage.googleapis.com", "Prefix": "/foo", "Region": "us-east-1", "Access Key ID": "AKIAIOSFODNN7EXAMPLE", "Secret Access Key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
			expectedURL: "s3://storage.googleapis.com/bucket/foo?access-key-id=AKIAIOSFODNN7EXAMPLE&region=us-east-1&secret-access-key=wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY",
		},
		{
			name:        "missing required field Bucket",
			config:      map[string]any{"Region": "us-east-1"},
			expectError: true,
		},
	}

	var driver s3Driver
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url, errs := driver.Validate(tt.config)
			if tt.expectError {
				assert.GreaterOrEqual(t, len(errs), 1)
				assert.Equal(t, "", url)
			} else {
				assert.Empty(t, errs)
				assert.Equal(t, tt.expectedURL, url)
			}
		})
	}
}

func TestSchemaValidationPath(t *testing.T) {
	logger := logger.NewTestLogger()
	_, _, prefix := getBucketInfo(mustParseURL("s3://storage.googleapis.com/eds-import/withprefix"), googleProvider)
	var s3 s3Driver
	s3.prefix = prefix
	s3.ch = make(chan job, 1)
	ok, err := s3.Process(logger, internal.DBChangeEvent{
		SchemaValidatedPath: internal.StringPointer("a/b/c"),
	})
	assert.False(t, ok)
	assert.NoError(t, err)
	job := <-s3.ch
	assert.Equal(t, "withprefix/a/b/c", job.key)
}

func TestEventPathWithPrefix(t *testing.T) {
	logger := logger.NewTestLogger()
	_, _, prefix := getBucketInfo(mustParseURL("s3://storage.googleapis.com/eds-import/withprefix"), googleProvider)
	var s3 s3Driver
	s3.prefix = prefix
	s3.ch = make(chan job, 1)
	ok, err := s3.Process(logger, internal.DBChangeEvent{
		Table:     "table",
		Key:       []string{"pk"},
		Timestamp: 500000000,
	})
	assert.False(t, ok)
	assert.NoError(t, err)
	job := <-s3.ch
	assert.Equal(t, "withprefix/table/500000-pk.json", job.key)
}

func TestEventPathNoPrefix(t *testing.T) {
	logger := logger.NewTestLogger()
	var s3 s3Driver
	s3.ch = make(chan job, 1)
	ok, err := s3.Process(logger, internal.DBChangeEvent{
		Table:     "table",
		Key:       []string{"pk"},
		Timestamp: 2000,
	})
	assert.False(t, ok)
	assert.NoError(t, err)
	job := <-s3.ch
	assert.Equal(t, "table/2-pk.json", job.key)
}
