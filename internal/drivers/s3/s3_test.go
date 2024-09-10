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
	var driver s3Driver
	url, err := driver.Validate(map[string]any{
		"Bucket": "bucket",
	})
	assert.Empty(t, err)
	assert.Equal(t, "s3://bucket", url)

	url, err = driver.Validate(map[string]any{
		"Bucket": "bucket",
		"Prefix": "prefix",
	})
	assert.Empty(t, err)
	assert.Equal(t, "s3://bucket/prefix", url)

	url, err = driver.Validate(map[string]any{
		"Bucket": "bucket",
		"Prefix": "/prefix",
	})
	assert.Empty(t, err)
	assert.Equal(t, "s3://bucket/prefix", url)

	url, err = driver.Validate(map[string]any{
		"Bucket":   "bucket",
		"Prefix":   "/prefix",
		"Endpoint": "storage.googleapis.com",
	})
	assert.Empty(t, err)
	assert.Equal(t, "s3://storage.googleapis.com/bucket/prefix", url)

	url, err = driver.Validate(map[string]any{
		"Bucket":   "bucket",
		"Prefix":   "prefix",
		"Endpoint": "storage.googleapis.com",
	})
	assert.Empty(t, err)
	assert.Equal(t, "s3://storage.googleapis.com/bucket/prefix", url)

	url, err = driver.Validate(map[string]any{
		"Bucket":   "bucket",
		"Endpoint": "storage.googleapis.com",
	})
	assert.Empty(t, err)
	assert.Equal(t, "s3://storage.googleapis.com/bucket", url)

	url, err = driver.Validate(map[string]any{
		"Bucket":            "bucket",
		"Access Key ID":     "AKIAIOSFODNN7EXAMPLE",
		"Secret Access Key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	})
	assert.Empty(t, err)
	assert.Equal(t, "s3://bucket?access-key-id=AKIAIOSFODNN7EXAMPLE&secret-access-key=wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY", url)

	url, err = driver.Validate(map[string]any{
		"Bucket":            "bucket",
		"Region":            "us-east-1",
		"Access Key ID":     "AKIAIOSFODNN7EXAMPLE",
		"Secret Access Key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	})
	assert.Empty(t, err)
	assert.Equal(t, "s3://bucket?access-key-id=AKIAIOSFODNN7EXAMPLE&region=us-east-1&secret-access-key=wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY", url)

	url, err = driver.Validate(map[string]any{
		"Bucket":            "bucket",
		"Endpoint":          "storage.googleapis.com",
		"Prefix":            "/foo",
		"Region":            "us-east-1",
		"Access Key ID":     "AKIAIOSFODNN7EXAMPLE",
		"Secret Access Key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	})
	assert.Empty(t, err)
	assert.Equal(t, "s3://storage.googleapis.com/bucket/foo?access-key-id=AKIAIOSFODNN7EXAMPLE&region=us-east-1&secret-access-key=wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY", url)
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
		Table: "table",
		Key:   []string{"pk"},
	})
	assert.False(t, ok)
	assert.NoError(t, err)
	job := <-s3.ch
	assert.Equal(t, "withprefix/table/pk.json", job.key)
}

func TestEventPathNoPrefix(t *testing.T) {
	logger := logger.NewTestLogger()
	var s3 s3Driver
	s3.ch = make(chan job, 1)
	ok, err := s3.Process(logger, internal.DBChangeEvent{
		Table: "table",
		Key:   []string{"pk"},
	})
	assert.False(t, ok)
	assert.NoError(t, err)
	job := <-s3.ch
	assert.Equal(t, "table/pk.json", job.key)
}
