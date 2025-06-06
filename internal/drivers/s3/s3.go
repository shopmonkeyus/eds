package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/importer"
	"github.com/shopmonkeyus/eds/internal/util"
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

type job struct {
	logger logger.Logger
	event  internal.DBChangeEvent
	key    string
}

type s3Driver struct {
	config       internal.DriverConfig
	logger       logger.Logger
	bucket       string
	prefix       string
	s3           *awss3.Client
	importConfig internal.ImporterConfig
	waitGroup    sync.WaitGroup
	jobWaitGroup sync.WaitGroup
	ch           chan job
	errors       chan error
	ctx          context.Context
	cancel       context.CancelFunc
}

var _ internal.Driver = (*s3Driver)(nil)
var _ internal.DriverLifecycle = (*s3Driver)(nil)
var _ internal.DriverHelp = (*s3Driver)(nil)
var _ internal.Importer = (*s3Driver)(nil)
var _ internal.ImporterHelp = (*s3Driver)(nil)
var _ importer.Handler = (*s3Driver)(nil)

func addFinalSlash(s string) string {
	if s == "" {
		return ""
	}
	if !strings.HasSuffix(s, "/") {
		return s + "/"
	}
	return s
}

// getBucketInfo returns the provider url, bucket and prefix
func getBucketInfo(u *url.URL, provider s3Provider) (string, string, string) {
	if provider == awsProvider {
		return "", u.Host, addFinalSlash(strings.TrimPrefix(u.Path, "/"))
	}
	var prefix, bucket string
	pathParts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(pathParts) > 0 {
		bucket = pathParts[0]
		prefix = addFinalSlash(strings.Join(pathParts[1:], "/"))
	}
	if provider == localstackProvider {
		return "http://" + u.Host, bucket, prefix
	}

	return "https://" + u.Host, bucket, prefix
}

func getEndpointResolver(url string, cloudProvider s3Provider) aws.EndpointResolverWithOptionsFunc {
	return func(_service, region string, _options ...interface{}) (aws.Endpoint, error) {
		if cloudProvider == googleProvider {
			return aws.Endpoint{
				URL:               "https://storage.googleapis.com",
				SigningRegion:     "auto",
				Source:            aws.EndpointSourceCustom,
				HostnameImmutable: true,
			}, nil
		}
		if url != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           url,
				SigningRegion: region,
				SigningMethod: "v4",
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to its default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	}
}

func parseBucketURL(u *url.URL, cloudProvider s3Provider) (aws.EndpointResolverWithOptionsFunc, string, string) {
	url, bucket, prefix := getBucketInfo(u, cloudProvider)
	return getEndpointResolver(url, cloudProvider), bucket, prefix
}

type s3Provider int

const (
	awsProvider s3Provider = iota
	googleProvider
	localstackProvider
)

func getCloudProvider(u *url.URL) s3Provider {
	if util.IsLocalhost(u.Host) {
		return localstackProvider
	}
	if strings.Contains(u.Host, "googleapis.com") {
		return googleProvider
	}
	return awsProvider
}

func NewS3Client(ctx context.Context, logger logger.Logger, urlString string) (*awss3.Client, string, string, int, int, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, "", "", 0, 0, fmt.Errorf("unable to parse url: %w", err)
	}
	cloudProvider := getCloudProvider(u)
	customResolver, bucket, prefix := parseBucketURL(u, cloudProvider)
	region := os.Getenv("AWS_REGION")
	if u.Query().Get("region") != "" {
		region = u.Query().Get("region")
	} else if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
		if region == "" {
			region = "us-west-2"
		}
	}

	var accessKeyID, secretAccessKey, sessionToken string
	var maxRetries int

	if u.Query().Has("region") {
		region = u.Query().Get("region")
	}
	if u.Query().Has("access-key-id") {
		accessKeyID = u.Query().Get("access-key-id")
	} else {
		accessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if u.Query().Has("secret-access-key") {
		secretAccessKey = u.Query().Get("secret-access-key")
	} else {
		secretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if u.Query().Has("session-token") {
		sessionToken = u.Query().Get("session-token")
	} else {
		sessionToken = os.Getenv("AWS_SESSION_TOKEN")
	}
	if u.Query().Has("max-retries") {
		max := u.Query().Get("max-retries")
		maxRetries, err = strconv.Atoi(max)
		if err != nil {
			logger.Warn("skipping max-retires value: %s. %d", max, err)
			maxRetries = 5
		}
	} else {
		maxRetries = 5
	}

	var cfg aws.Config

	provider := config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken))

	if cloudProvider == googleProvider {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion("auto"),
			config.WithEndpointResolverWithOptions(customResolver),
			provider)
		if err == nil {
			cfg.HTTPClient = &http.Client{Transport: &RecalculateV4Signature{http.DefaultTransport, v4.NewSigner(), cfg}}
		}
	} else {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(region),
			config.WithEndpointResolverWithOptions(customResolver),
			provider,
		)
	}

	if err != nil {
		return nil, "", "", 0, 0, fmt.Errorf("unable to load AWS config: %w", err)
	}

	customRetry := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = maxRetries
	})

	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.UsePathStyle = true
		o.Retryer = customRetry
	})

	maxBatchSize := 1_000 // maximum number of events to batch
	uploadTasks := 4      // number of concurrent upload tasks

	if u.Query().Get("maxBatchSize") != "" {
		maxBatchSize, err = strconv.Atoi(u.Query().Get("maxBatchSize"))
		if err != nil {
			return nil, "", "", 0, 0, fmt.Errorf("unable to parse maxBatchSize: %w", err)
		}
		if maxBatchSize <= 0 {
			maxBatchSize = 1_000
		}
	}
	if u.Query().Get("uploadTasks") != "" {
		uploadTasks, err = strconv.Atoi(u.Query().Get("uploadTasks"))
		if err != nil {
			return nil, "", "", 0, 0, fmt.Errorf("unable to parse uploadTasks: %w", err)
		}
		if uploadTasks <= 0 {
			uploadTasks = 4
		}
	}

	return client, bucket, prefix, maxBatchSize, uploadTasks, nil
}

func (p *s3Driver) connect(ctx context.Context, logger logger.Logger, urlString string, testonly bool) error {
	c, cancel := context.WithCancel(ctx)
	p.ctx = c
	p.cancel = cancel

	client, bucket, prefix, maxBatchSize, uploadTasks, err := NewS3Client(ctx, logger, urlString)
	if err != nil {
		return fmt.Errorf("unable to create s3 client: %w", err)
	}
	p.bucket = bucket
	p.prefix = prefix
	p.s3 = client

	if testonly {
		return nil
	}

	p.logger.Debug("setting maxBatchSize=%d uploadTasks=%d", maxBatchSize, uploadTasks)

	p.ch = make(chan job, maxBatchSize)
	p.errors = make(chan error, maxBatchSize)
	for i := 0; i < uploadTasks; i++ {
		p.waitGroup.Add(1)
		go p.run()
	}

	return nil
}

func (p *s3Driver) run() {
	defer p.waitGroup.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case job := <-p.ch:
			buf := []byte(util.JSONStringify(job.event))
			_, err := p.s3.PutObject(context.Background(), &awss3.PutObjectInput{
				Bucket:        aws.String(p.bucket),
				Key:           aws.String(job.key),
				ContentType:   aws.String("application/json"),
				Body:          bytes.NewReader(buf),
				ContentLength: aws.Int64(int64(len(buf))),
			})
			if err != nil {
				p.errors <- fmt.Errorf("error storing s3 object to %s:%s: %w", p.bucket, job.key, err)
			} else {
				job.logger.Trace("uploaded to %s:%s", p.bucket, job.key)
			}
			p.jobWaitGroup.Done()
		}
	}
}

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *s3Driver) Start(pc internal.DriverConfig) error {
	p.config = pc
	p.logger = pc.Logger.WithPrefix("[s3]")
	if err := p.connect(pc.Context, p.logger, pc.URL, false); err != nil {
		return err
	}
	return nil
}

// Stop the driver. This is called once at the end of the driver's lifecycle.
func (p *s3Driver) Stop() error {
	p.cancel()
	p.logger.Debug("stopping s3 driver")
	p.jobWaitGroup.Wait()
	if p.ch != nil {
		close(p.ch)
	}
	p.waitGroup.Wait()
	p.logger.Debug("stopped s3 driver")
	return nil
}

// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
// Return -1 to indicate that there is no limit.
func (p *s3Driver) MaxBatchSize() int {
	return 1_000
}

func (p *s3Driver) process(_ context.Context, logger logger.Logger, event internal.DBChangeEvent, dryRun bool) (bool, error) {
	var key string
	if event.SchemaValidatedPath != nil {
		key = path.Join(p.prefix, *event.SchemaValidatedPath)
	} else {
		key = path.Join(p.prefix, event.Table, fmt.Sprintf("%d-%s.json", time.UnixMilli(event.Timestamp).Unix(), event.GetPrimaryKey()))
	}
	if dryRun {
		logger.Trace("would store %s:%s", p.bucket, key)
	} else {
		p.jobWaitGroup.Add(1)
		p.ch <- job{logger, event, key}
	}
	return false, nil
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
func (p *s3Driver) Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
	return p.process(p.config.Context, logger, event, false)
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
func (p *s3Driver) Flush(logger logger.Logger) error {
	logger.Debug("flush called")
	p.jobWaitGroup.Wait()
	var errs []error
done:
	for {
		select {
		case err := <-p.errors:
			errs = append(errs, err)
		default:
			break done
		}
	}
	logger.Debug("flush finished")
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
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
	return "s3://bucket/folder?region=us-west-2&access-key-id=AKIAIOSFODNN7EXAMPLE&secret-access-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
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
	_, err := p.process(p.importConfig.Context, p.logger, event, p.importConfig.DryRun)
	return err
}

// ImportCompleted is called when all events have been processed.
func (p *s3Driver) ImportCompleted() error {
	return nil
}

func (p *s3Driver) Import(config internal.ImporterConfig) error {
	if config.SchemaOnly {
		return nil
	}
	p.logger = config.Logger.WithPrefix("[s3]")
	p.importConfig = config
	if err := p.connect(config.Context, p.logger, config.URL, false); err != nil {
		return err
	}
	return importer.Run(p.logger, config, p)
}

// SupportsDelete returns true if the importer supports deleting data.
func (p *s3Driver) SupportsDelete() bool {
	return false
}

// Test is called to test the drivers connectivity with the configured url. It should return an error if the test fails or nil if the test passes.
func (p *s3Driver) Test(ctx context.Context, logger logger.Logger, url string) error {
	if err := p.connect(ctx, logger, url, true); err != nil {
		return err
	}
	return nil
}

// Configuration returns the configuration fields for the driver.
func (p *s3Driver) Configuration() []internal.DriverField {
	return []internal.DriverField{
		internal.RequiredStringField("Bucket", "The bucket name", nil),
		internal.OptionalStringField("Prefix", "The prefix to prepend to the filename", nil),
		internal.OptionalStringField("Region", "The AWS region to use", nil),
		internal.OptionalPasswordField("Access Key ID", "The AWS AWS Key ID", nil),
		internal.OptionalPasswordField("Secret Access Key", "The AWS Secret Access Key", nil),
		internal.OptionalStringField("Endpoint", "The Endpoint hostname to override if using an AWS compatible provider", nil),
	}
}

// Validate validates the configuration and returns an error if the configuration is invalid or a valid url if the configuration is valid.
func (p *s3Driver) Validate(values map[string]any) (string, []internal.FieldError) {
	bucket := internal.GetRequiredStringValue("Bucket", values)
	prefix := internal.GetOptionalStringValue("Prefix", "", values)
	region := internal.GetOptionalStringValue("Region", "", values)
	accesskey := internal.GetOptionalStringValue("Access Key ID", "", values)
	secret := internal.GetOptionalStringValue("Secret Access Key", "", values)
	endpoint := internal.GetOptionalStringValue("Endpoint", "", values)
	var url url.URL
	url.Scheme = "s3"
	if endpoint != "" {
		url.Host = endpoint
		url.Path = bucket
	} else {
		url.Host = bucket
	}
	if prefix != "" {
		if url.Path == "" {
			url.Path = prefix
		} else {
			url.Path = path.Join(url.Path, prefix)
		}
	}
	q := url.Query()
	if region != "" {
		q.Set("region", region)
	}
	if accesskey != "" {
		q.Set("access-key-id", accesskey)
	}
	if secret != "" {
		q.Set("secret-access-key", secret)
	}
	url.RawQuery = q.Encode()
	return url.String(), nil
}

func init() {
	internal.RegisterDriver("s3", &s3Driver{})
	internal.RegisterImporter("s3", &s3Driver{})
}
