//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shopmonkeyus/eds/internal"
	s3driver "github.com/shopmonkeyus/eds/internal/drivers/s3"
	"github.com/shopmonkeyus/go-common/logger"
)

func getEndpointResolver(url string) aws.EndpointResolverWithOptionsFunc {
	return func(_service, region string, _options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           url,
			SigningRegion: "us-east-1",
			SigningMethod: "v4",
		}, nil
	}
}

type driverS3Test struct {
}

var _ e2eTest = (*driverS3Test)(nil)

func (d *driverS3Test) Name() string {
	return "s3"
}

func (d *driverS3Test) URL(dir string) string {
	return fmt.Sprintf("s3://127.0.0.1:4566/%s?region=us-east-1&access-key-id=test&secret-access-key=eds", dbname)
}

func (d *driverS3Test) TestInsert(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error {
	client, _, _, _, _, err := s3driver.NewS3Client(context.Background(), logger, url)
	if err != nil {
		return fmt.Errorf("error creating s3 client: %w", err)
	}
	res, err := client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(dbname),
		Key:    aws.String(fmt.Sprintf("order/%s.json", event.GetPrimaryKey())),
	})
	if err != nil {
		return fmt.Errorf("error getting object: %w", err)
	}
	var event2 internal.DBChangeEvent
	if err := json.NewDecoder(res.Body).Decode(&event2); err != nil {
		return fmt.Errorf("error unmarshalling event: %w", err)
	}
	return dbchangeEventMatches(event, event2)
}

func init() {
	registerTest(&driverS3Test{})
}
