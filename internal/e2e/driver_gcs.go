//go:build e2e
// +build e2e

// Google Cloud Storage (GCS) end-to-end test using the S3-compatible API.
//
// This test requires GCS HMAC keys to be set as environment variables:
//   - GCS_ACCESS_KEY_ID: Your GCS HMAC access key (starts with GOOG1...)
//   - GCS_SECRET_ACCESS_KEY: Your GCS HMAC secret key
//   - GCS_TEST_BUCKET (optional): Bucket name to use (defaults to "eds-test-upload")
// Use the credentials from the existing EDS GCS service account; ask a team member for the credentials.
//
// To run this test:
//   export GCS_ACCESS_KEY_ID=your_gcs_access_key
//   export GCS_SECRET_ACCESS_KEY=your_gcs_secret_key
//   export GCS_TEST_BUCKET=your-bucket-name  # optional
//   go run -tags e2e . e2e -v gcs
//
// The test will be skipped if credentials are not provided.

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shopmonkeyus/eds/internal"
	s3driver "github.com/shopmonkeyus/eds/internal/drivers/s3"
	"github.com/shopmonkeyus/go-common/logger"
)

type driverGCSTest struct {
}

var _ e2eTest = (*driverGCSTest)(nil)
var _ e2eTestDisabled = (*driverGCSTest)(nil)

func getGCSCredentials() (accessKeyID, secretAccessKey string) {
	accessKeyID = os.Getenv("GCS_ACCESS_KEY_ID")
	secretAccessKey = os.Getenv("GCS_SECRET_ACCESS_KEY")
	return
}

func (d *driverGCSTest) Name() string {
	return "gcs"
}

func (d *driverGCSTest) URL(dir string) string {
	bucket := os.Getenv("GCS_TEST_BUCKET")
	if bucket == "" {
		bucket = "eds-test-upload"
	}

	accessKeyID, secretAccessKey := getGCSCredentials()

	baseURL := fmt.Sprintf("s3://storage.googleapis.com/%s", bucket)
	params := url.Values{}
	params.Set("access-key-id", accessKeyID)
	params.Set("secret-access-key", secretAccessKey)

	return fmt.Sprintf("%s?%s", baseURL, params.Encode())
}

func (d *driverGCSTest) Validate(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error {
	client, bucket, prefix, _, _, err := s3driver.NewS3Client(context.Background(), logger, url)
	if err != nil {
		return fmt.Errorf("error creating gcs client: %w", err)
	}

	key := fmt.Sprintf("%s%s/%d-%s.json", prefix, event.Table, time.UnixMilli(event.Timestamp).Unix(), event.GetPrimaryKey())

	res, err := client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("error getting object from GCS (bucket: %s, key: %s): %w", bucket, key, err)
	}
	defer res.Body.Close()

	var event2 internal.DBChangeEvent
	if err := json.NewDecoder(res.Body).Decode(&event2); err != nil {
		return fmt.Errorf("error unmarshalling event: %w", err)
	}

	return dbchangeEventMatches(event, event2)
}

func (d *driverGCSTest) Disabled() bool {
	accessKeyID, secretAccessKey := getGCSCredentials()
	return accessKeyID == "" || secretAccessKey == ""
}

func init() {
	registerTest(&driverGCSTest{})
}
