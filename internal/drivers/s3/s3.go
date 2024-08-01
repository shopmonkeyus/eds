package s3

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/importer"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type RecalculateV4Signature struct {
	next   http.RoundTripper
	signer *v4.Signer
	cfg    aws.Config
}

func (lt *RecalculateV4Signature) RoundTrip(req *http.Request) (*http.Response, error) {
	// store for later use
	val := req.Header.Get("Accept-Encoding")

	// delete the header so the header doesn't account for in the signature
	req.Header.Del("Accept-Encoding")

	// sign with the same date
	timeString := req.Header.Get("X-Amz-Date")
	timeDate, _ := time.Parse("20060102T150405Z", timeString)

	creds, _ := lt.cfg.Credentials.Retrieve(req.Context())
	err := lt.signer.SignHTTP(req.Context(), creds, req, v4.GetPayloadHash(req.Context()), "s3", lt.cfg.Region, timeDate)
	if err != nil {
		return nil, err
	}
	// Reset Accept-Encoding if desired
	req.Header.Set("Accept-Encoding", val)

	// follows up the original round tripper
	return lt.next.RoundTrip(req)
}

type s3Driver struct {
	config       internal.DriverConfig
	logger       logger.Logger
	bucket       string
	prefix       string
	s3           *awss3.Client
	importConfig internal.ImporterConfig
}

var _ internal.Driver = (*s3Driver)(nil)
var _ internal.DriverLifecycle = (*s3Driver)(nil)
var _ internal.DriverHelp = (*s3Driver)(nil)
var _ internal.Importer = (*s3Driver)(nil)
var _ importer.Handler = (*s3Driver)(nil)

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *s3Driver) Start(pc internal.DriverConfig) error {
	p.config = pc
	p.logger = pc.Logger.WithPrefix("[s3]")

	u, err := url.Parse(pc.URL)
	if err != nil {
		return fmt.Errorf("unable to parse url: %w", err)
	}

	host := u.Host
	p.bucket = u.Path

	if u.Path == "" {
		p.bucket = u.Host
		host = ""
	} else {
		p.bucket = u.Path[1:] // trim off the forward slash

		tok := strings.Split(p.bucket, "/")
		if len(tok) > 1 {
			p.bucket = tok[0]
			p.prefix = strings.Join(tok[1:], "/")
			if !strings.HasSuffix(p.prefix, "/") {
				p.prefix += "/"
			}
		}
	}

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if host != "" {
			var url string
			if strings.Contains(host, "localhost") {
				url = "http://" + host
			} else {
				url = "https://" + host
			}
			if strings.Contains(url, "googleapis.com") {
				return aws.Endpoint{
					URL:               "https://storage.googleapis.com",
					SigningRegion:     "auto",
					Source:            aws.EndpointSourceCustom,
					HostnameImmutable: true,
				}, nil
			}
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           url,
				SigningRegion: region,
				SigningMethod: "v4",
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to its default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	region := os.Getenv("AWS_REGION")
	if u.Query().Get("region") != "" {
		region = u.Query().Get("region")
	} else if region == "" {
		region = "us-west-2"
	}

	var cfg aws.Config

	if strings.Contains(host, "googleapis.com") {
		cfg, err = config.LoadDefaultConfig(pc.Context,
			config.WithRegion("auto"),
			config.WithEndpointResolverWithOptions(customResolver))
		if err == nil {
			cfg.HTTPClient = &http.Client{Transport: &RecalculateV4Signature{http.DefaultTransport, v4.NewSigner(), cfg}}
		}
	} else {
		cfg, err = config.LoadDefaultConfig(pc.Context,
			config.WithRegion(region),
			config.WithEndpointResolverWithOptions(customResolver),
		)
	}

	if err != nil {
		return fmt.Errorf("unable to load AWS config: %w", err)
	}

	p.s3 = awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.UsePathStyle = true
	})

	return nil
}

// Stop the driver. This is called once at the end of the driver's lifecycle.
func (p *s3Driver) Stop() error {
	return nil
}

// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
// Return -1 to indicate that there is no limit.
func (p *s3Driver) MaxBatchSize() int {
	return 1
}

func (p *s3Driver) process(event internal.DBChangeEvent, dryRun bool) (bool, error) {
	key := fmt.Sprintf("%s%s/%s.json", p.prefix, event.Table, event.ID)
	if dryRun {
		p.logger.Trace("would store %s:%s", p.bucket, key)
	} else {
		buf := []byte(util.JSONStringify(event))
		_, err := p.s3.PutObject(p.config.Context, &awss3.PutObjectInput{
			Bucket:        aws.String(p.bucket),
			Key:           aws.String(key),
			ContentType:   aws.String("application/json"),
			Body:          bytes.NewReader(buf),
			ContentLength: aws.Int64(int64(len(buf))),
		})
		if err != nil {
			return true, fmt.Errorf("error storing s3 object to %s:%s: %w", p.bucket, key, err)
		}
		p.logger.Trace("stored %s:%s", p.bucket, key)
	}
	return false, nil
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
func (p *s3Driver) Process(event internal.DBChangeEvent) (bool, error) {
	return p.process(event, false)
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
func (p *s3Driver) Flush() error {
	return nil
}

// Name is a unique name for the driver.
func (p *s3Driver) Name() string {
	return "AWS S3"
}

// Description is the description of the driver.
func (p *s3Driver) Description() string {
	return "Supports streaming EDS messages to a AWS S3 compatible destination."
}

// ExampleURL should return an example URL for configuring the driver.
func (p *s3Driver) ExampleURL() string {
	return "s3://bucket/folder"
}

// Help should return a detailed help documentation for the driver.
func (p *s3Driver) Help() string {
	var help strings.Builder
	help.WriteString(util.GenerateHelpSection("AWS", "If using AWS, no special configuration is required and you can use the standard AWS environment variables to configure the access key, secret and region.\n"))
	help.WriteString("\n")
	help.WriteString(util.GenerateHelpSection("Google Cloud Storage", "To use GCS for storage, use the following url pattern: s3://storage.googleapis.com/bucket. See https://cloud.google.com/storage/docs/interoperability\n"))
	help.WriteString("\n")
	help.WriteString(util.GenerateHelpSection("LocalStack", "To use localstack for testing, use the following url pattern: s3://localhost:4566/bucket.\n"))
	return help.String()
}

// CreateDatasource allows the handler to create the datasource before importing data.
func (p *s3Driver) CreateDatasource(schema internal.SchemaMap) error {
	return nil
}

// ImportEvent allows the handler to process the event.
func (p *s3Driver) ImportEvent(event internal.DBChangeEvent, schema *internal.Schema) error {
	_, err := p.process(event, p.importConfig.DryRun)
	return err
}

// ImportCompleted is called when all events have been processed.
func (p *s3Driver) ImportCompleted() error {
	return nil
}

func (p *s3Driver) Import(config internal.ImporterConfig) error {
	p.logger = config.Logger.WithPrefix("[s3]")
	p.importConfig = config
	return importer.Run(p.logger, config, p)
}

func init() {
	internal.RegisterDriver("s3", &s3Driver{})
	internal.RegisterImporter("s3", &s3Driver{})
}
