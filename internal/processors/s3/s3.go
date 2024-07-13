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

type s3Processor struct {
	config internal.ProcessorConfig
	logger logger.Logger
	bucket string
	s3     *awss3.Client
}

var _ internal.Processor = (*s3Processor)(nil)
var _ internal.ProcessorLifecycle = (*s3Processor)(nil)

// Start the processor. This is called once at the beginning of the processor's lifecycle.
func (p *s3Processor) Start(pc internal.ProcessorConfig) error {
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

// Stop the processor. This is called once at the end of the processor's lifecycle.
func (p *s3Processor) Stop() error {
	return nil
}

// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
// Return -1 to indicate that there is no limit.
func (p *s3Processor) MaxBatchSize() int {
	return 1
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the processor will NAK the event.
func (p *s3Processor) Process(event internal.DBChangeEvent) (bool, error) {
	key := fmt.Sprintf("%s/%s.json", event.Table, event.ID)
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
	return false, nil
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the processor will NAK all pending events.
func (p *s3Processor) Flush() error {
	return nil
}

func init() {
	internal.RegisterProcessor("s3", &s3Processor{})
}
