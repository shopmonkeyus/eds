//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds/internal"
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

func (d *driverS3Test) Test(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, url string) error {
	return runTest(logger, nc, js, func(event internal.DBChangeEvent) internal.DBChangeEvent {
		logger.Info("need to finish s3 validation") // FIXME
		provider := config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "eds", ""))
		cfg, err := config.LoadDefaultConfig(context.Background(),
			config.WithRegion("us-east-1"),
			config.WithEndpointResolverWithOptions(getEndpointResolver("http://127.0.0.1:4566")),
			provider,
		)
		if err != nil {
			panic(err)
		}
		s3 := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
			o.UsePathStyle = true
		})
		res, err := s3.GetObject(context.Background(), &awss3.GetObjectInput{
			Bucket: aws.String(dbname),
			Key:    aws.String(fmt.Sprintf("order/%s.json", event.GetPrimaryKey())),
		})
		if err != nil {
			logger.Error("error getting object: %s", err)
		}
		var event2 internal.DBChangeEvent
		if err := json.NewDecoder(res.Body).Decode(&event2); err != nil {
			logger.Fatal("error decoding event: %s", err)
		}
		return event2
	})
	return nil
}

func init() {
	registerTest(&driverS3Test{})
}
