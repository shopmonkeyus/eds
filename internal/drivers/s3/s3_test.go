package s3

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

//

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func TestGetBucketInfo(t *testing.T) {
	var host, bucket, prefix string
	// host, bucket, prefix = getBucketInfo(mustParseURL("s3://bucket/key?region=us-east-1"))
	// assert.Equal(t, "bucket", bucket)
	// assert.Equal(t, "key", prefix)
	// assert.Equal(t, "", host)

	// // aws s3
	host, bucket, prefix = getBucketInfo(mustParseURL("s3://foo-shopmonkey/test?region=us-west-1"))
	assert.Equal(t, "foo-shopmonkey", bucket)
	assert.Equal(t, "test", prefix)
	assert.Equal(t, "", host)

	// localstack
	host, bucket, prefix = getBucketInfo(mustParseURL("s3://localhost:4566/foo-shopmonkey/test?region=us-west-1"))
	assert.Equal(t, "foo-shopmonkey", bucket)
	assert.Equal(t, "test/", prefix)
	assert.Equal(t, "localhost:4566", host)

	// google cloud storage
	host, bucket, prefix = getBucketInfo(mustParseURL("s3://storage.googleapis.com/eds-import"))
	assert.Equal(t, "eds-import", bucket)
	assert.Equal(t, "", prefix)
	assert.Equal(t, "storage.googleapis.com", host)
}
