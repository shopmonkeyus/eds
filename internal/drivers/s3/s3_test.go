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

	// localstack
	url, bucket, prefix = getBucketInfo(mustParseURL("s3://localhost:4566/foo-shopmonkey/test?region=us-west-1"), localstackProvider)
	assert.Equal(t, "foo-shopmonkey", bucket)
	assert.Equal(t, "test/", prefix)
	assert.Equal(t, "http://localhost:4566", url)

	// google cloud storage
	url, bucket, prefix = getBucketInfo(mustParseURL("s3://storage.googleapis.com/eds-import"), googleProvider)
	assert.Equal(t, "eds-import", bucket)
	assert.Equal(t, "", prefix)
	assert.Equal(t, "https://storage.googleapis.com", url)
}
