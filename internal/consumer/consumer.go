package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	transmissionv1 "github.com/shopmonkeyus/eds/pkg/transmission/v1"
	"github.com/shopmonkeyus/go-common/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultMaxCount     = 200
	defaultFetchTimeout = 500 * time.Millisecond
	extraFetchTimeout   = 500 * time.Millisecond

	ControlActionStart = "start"
	ControlActionPause = "pause"
)

// TODO: Make sure we handle rejecting connections to the transmission server from more than one client
var ErrConsumerAlreadyRunning = errors.New("consumer already running")

// Driver is a local interface which slims down the driver to only the methods we need to make it easier to test.
type Driver interface {
	Flush(logger logger.Logger) error
	Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error)
	MaxBatchSize() int
}

// TODO: Make sure we handle import table timestamps properly
// TODO: Determine if we need heartbeats

type ClientConfig struct {
	Context               context.Context
	Credentials           string
	Logger                logger.Logger
	Address               string
	EdsID                 string
	SessionID             string
	Driver                Driver
	Registry              internal.SchemaRegistry
	SchemaValidator       internal.SchemaValidator
	ExportTableTimestamps map[string]*time.Time
	FetchInterval         time.Duration
	MaxCount              int32
	FetchTimeout          time.Duration
}

// TODO: If we keep heartbeats, do we keep pause started and started?
// TODO: See if we can get Stop idempotency
type Client struct {
	ctx               context.Context
	cancel            context.CancelFunc
	logger            logger.Logger
	edsID             string
	sessionID         string
	driver            Driver
	registry          internal.SchemaRegistry
	validator         internal.SchemaValidator
	tableTimestamps   map[string]*time.Time
	maxCount          int32
	fetchTimeout      time.Duration
	supportsMigration bool

	conn   *grpc.ClientConn
	client transmissionv1.TransmissionServiceClient

	errCh        chan error
	disconnected chan bool
	waitGroup    sync.WaitGroup
	once         sync.Once
	lock         sync.Mutex
	paused       bool
}

// Disconnected returns a channel that will be closed when the client is disconnected from Transmission.
func (c *Client) Disconnected() <-chan bool {
	return c.disconnected
}

func (c *Client) Stop() error {
	c.cancel()
	c.waitGroup.Wait()
	c.conn.Close()
	return nil
}

func (c *Client) handleError(err error) {
	if c.ctx.Err() != nil {
		return
	}
	c.logger.Error("%s", err)
	select {
	case c.errCh <- err:
	default:
	}
}

func (c *Client) flush(logger logger.Logger) error {
	logger.Trace("flush")
	started := time.Now()
	if err := c.driver.Flush(logger); err != nil {
		return err
	}
	var count float64
	internal.FlushDuration.Observe(time.Since(started).Seconds())
	internal.FlushCount.Observe(count)
	return nil
}

func (c *Client) shouldSkip(evt *internal.DBChangeEvent) bool {
	if c.tableTimestamps != nil {
		eventTimestamp := time.UnixMilli(evt.Timestamp)
		// check if we have a timestamp for this table and only process if its newer
		if tableTimestamp := c.tableTimestamps[evt.Table]; tableTimestamp != nil {
			if eventTimestamp.Before(*tableTimestamp) {
				return true
			}
		}
	}
	if c.validator != nil {
		found, valid, path, err := c.validator.Validate(*evt)
		if err != nil {
			if errors.Is(err, util.ErrSchemaValidation) {
				// note we join these errors since they are separated by definition in errors.Join and we want to log them together
				c.logger.Debug("skipping %s, schema did not validate (%s) for event: %s", evt.Table, strings.TrimSpace(strings.Join(strings.Split(err.Error(), "\n"), " ")), util.JSONStringify(evt))
				return true
			}
			c.logger.Error("error validating schema: %s for event: %s", err, util.JSONStringify(evt))
			return true
		}
		if !found {
			c.logger.Trace("skipping %s, no schema found for event: %s", evt.Table, util.JSONStringify(evt))
			return true
		}
		if !valid {
			c.logger.Trace("skipping %s, schema did not validate for event: %s", evt.Table, util.JSONStringify(evt))
			return true
		}
		if path != "" {
			evt.SchemaValidatedPath = &path
			c.logger.Trace("schema validated %s", path)
		}
	}
	return false
}

func (c *Client) Error() <-chan error {
	return c.errCh
}

func (c *Client) handlePossibleMigration(event *internal.DBChangeEvent) error {
	found, version, err := c.registry.GetTableVersion(event.Table)
	if err != nil {
		return fmt.Errorf("error getting current table version for table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
	}
	if !found || version != event.ModelVersion {
		c.logger.Trace("%s found: %v, version: %v, model version: %v", event.Table, found, version, event.ModelVersion)
		newschema, err := c.registry.GetSchema(event.Table, event.ModelVersion)
		if err != nil {
			return fmt.Errorf("error getting new schema for table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
		}
		migration := c.driver.(internal.DriverMigration)
		if !found {
			c.logger.Debug("need to migrate new table: %s, model version: %s", event.Table, event.ModelVersion)
			if err := migration.MigrateNewTable(c.ctx, c.logger, newschema); err != nil {
				return fmt.Errorf("error migrating new table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
			}
			if err := c.registry.SetTableVersion(event.Table, event.ModelVersion); err != nil {
				return fmt.Errorf("error setting table version for table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
			}
			c.logger.Info("migrated new table: %s, model version: %s", event.Table, event.ModelVersion)
			return nil
		}
		oldschema, err := c.registry.GetSchema(event.Table, version)
		if err != nil {
			return fmt.Errorf("error getting current schema for table: %s, model version: %s: %w", event.Table, version, err)
		}
		// figure out which columns are new
		var columns []string
		for _, col := range newschema.Columns() {
			if !util.SliceContains(oldschema.Columns(), col) {
				columns = append(columns, col)
			}
		}
		// we only care about if there are new columns
		if len(columns) > 0 {
			c.logger.Debug("need to migrate table: %s, columns: %s, model version: %s", event.Table, strings.Join(columns, ","), event.ModelVersion)
			if err := migration.MigrateNewColumns(c.ctx, c.logger, newschema, columns); err != nil {
				return fmt.Errorf("error migrating new columns for table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
			}
			for _, col := range columns {
				if !util.SliceContains(event.Diff, col) {
					event.Diff = append(event.Diff, col) // add these new columns to the diff so that we can update them as part of the changeset
				}
			}
			if err := c.registry.SetTableVersion(event.Table, event.ModelVersion); err != nil {
				return fmt.Errorf("error setting table version for table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
			}
			c.logger.Info("migrated table: %s, columns: %s, model version: %s", event.Table, strings.Join(columns, ","), event.ModelVersion)
			return nil
		} else {
			c.logger.Info("new table: %s with different model version: %s but no new columns added", event.Table, event.ModelVersion)
		}
	}
	return nil
}

func (c *Client) processPayload(payload []byte) error {
	evt, err := internal.DBChangeEventFromPayload(payload)
	if err != nil {
		return err
	}
	if c.shouldSkip(&evt) {
		c.logger.Debug("skipping event")
		return nil
	}

	// check to see if we need to perform a migration
	if c.supportsMigration {
		err := c.handlePossibleMigration(&evt)
		if err != nil {
			return err
		}
	}

	// check to see if the schema matches the incoming object
	if evt.Operation != "DELETE" && c.registry != nil {
		schema, err := c.registry.GetSchema(evt.Table, evt.ModelVersion)
		if err != nil {
			return fmt.Errorf("error getting schema for table: %s, model version: %s: %w", evt.Table, evt.ModelVersion, err)
		}
		object, err := evt.GetObject()
		if err != nil {
			return fmt.Errorf("error getting object for table: %s, model version: %s: %w", evt.Table, evt.ModelVersion, err)
		}
		diff := util.JSONDiff(object, schema.Columns())
		if len(diff) > 0 {
			if err := evt.OmitProperties(diff...); err != nil {
				return fmt.Errorf("error omitting extra properties: %s properties for table: %s, model version: %s: %w", diff, evt.Table, evt.ModelVersion, err)
			}
		}
	}

	// ignore flush return value since v4 flushes after every fetch
	_, err = c.driver.Process(c.logger, evt)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Pause() {
	c.paused = true
	c.logger.Debug("paused")
}

func (c *Client) Unpause() {
	c.paused = false
	c.logger.Debug("unpaused")
}

func (c *Client) signalDisconnected() {
	select {
	case <-c.disconnected:
	default:
		close(c.disconnected)
	}
}

func (c *Client) isPaused() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.paused
}

func (c *Client) applyControlMessage(resp *transmissionv1.ControlResponse) {
	switch resp.GetAction() {
	case ControlActionStart:
		c.logger.Info("received start from transmission")
		c.Unpause()
	case ControlActionPause:
		c.logger.Info("received pause from transmission")
		c.Pause()
	}
}

func (c *Client) runFetch() {
	defer c.waitGroup.Done()

	for c.ctx.Err() == nil {
		if c.isPaused() {
			time.Sleep(c.fetchTimeout)
			continue
		}
		c.fetchOnce()
	}
}

// TODO: Move disconnect handling here

// resp, err := stream.Recv()
// if err != nil {
// 	if c.ctx.Err() != nil {
// 		return
// 	}
// 	if errors.Is(err, io.EOF) || status.Code(err) == codes.Unavailable {
// 		c.logger.Error("control stream interrupted: %s", err)
// 		c.signalDisconnected()
// 		return
// 	}
// 	c.handleError(fmt.Errorf("error reading control stream: %w", err))
// 	return

func (c *Client) fetchOnce() {
	fetchCtx, cancel := context.WithTimeout(c.ctx, c.fetchTimeout+extraFetchTimeout)
	defer cancel()

	c.logger.Debug("fetching (maxCount=%d timeout=%s)", c.maxCount, c.fetchTimeout)

	stream, err := c.client.Fetch(fetchCtx, &transmissionv1.FetchRequest{
		EdsId:     c.edsID,
		MaxCount:  c.maxCount,
		TimeoutMs: c.fetchTimeout.Milliseconds(),
	})
	if err != nil {
		if c.ctx.Err() != nil {
			return
		}
		c.handleError(fmt.Errorf("error calling fetch: %w", err))
		return
	}

	received := 0
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			if c.ctx.Err() != nil {
				return
			}
			c.handleError(fmt.Errorf("error receiving fetch response: %w", err))
			return
		}

		for _, msg := range resp.GetMessages() {
			if err := c.processPayload(msg.GetPayload()); err != nil {
				c.handleError(err)
				return
			}
			received++
		}
	}

	if received > 0 {
		if err := c.driver.Flush(c.logger); err != nil {
			c.handleError(fmt.Errorf("error flushing driver: %w", err))
			return
		}
	}

	c.logger.Debug("fetch completed (messages=%d)", received)
}

func (c *Client) start() error {
	c.waitGroup.Add(1)
	go c.runFetch()
	c.logger.Debug("started")
	return nil
}

// CreateConsumer creates a new nats consumer, but does not start it.
func CreateClient(config ClientConfig) (*Client, error) {
	ctx, cancel := context.WithCancel(config.Context)

	var credentials credentials.TransportCredentials
	if config.Credentials == "" {
		config.Logger.Warn("no credentials provided for NATS connection")
		credentials = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(config.Address, grpc.WithTransportCredentials(credentials))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("error dialing transmission at %s: %w", config.Address, err)
	}

	client := &Client{
		ctx:             ctx,
		cancel:          cancel,
		logger:          config.Logger.WithPrefix("[client]"),
		edsID:           config.EdsID,
		sessionID:       config.SessionID,
		driver:          config.Driver,
		registry:        config.Registry,
		validator:       config.SchemaValidator,
		tableTimestamps: config.ExportTableTimestamps,
		maxCount:        defaultMaxCount,
		fetchTimeout:    defaultFetchTimeout,
		conn:            conn,
		client:          transmissionv1.NewTransmissionServiceClient(conn),
		errCh:           make(chan error, 1),
		disconnected:    make(chan bool),
	}

	if _, ok := config.Driver.(internal.DriverMigration); ok {
		client.supportsMigration = true
	}

	var startAt *time.Time
	if config.ExportTableTimestamps != nil {
		client.tableTimestamps = config.ExportTableTimestamps
		// get the earliest timestamp
		for _, ts := range config.ExportTableTimestamps {
			if ts != nil && (startAt == nil || ts.Before(*startAt)) {
				startAt = ts
			}
		}
	}

	// TODO: Figure out how to send the start time to the transmission consumer

	if client.supportsMigration {
		if err := internal.UpdateDestinationSchema(ctx, client.logger, client.registry, config.Driver.(internal.DriverMigration)); err != nil {
			return nil, fmt.Errorf("error updating destination schema: %w", err)
		}
	}

	if config.Driver != nil {
		if p, ok := config.Driver.(internal.DriverSessionHandler); ok {
			p.SetSessionID(client.sessionID)
		}
	} else {
		config.Logger.Debug("no driver set")
	}

	client.disconnected = make(chan bool, 1)

	return client, nil
}

// NewConsumer creates and starts a new nats consumer
func NewClient(config ClientConfig) (*Client, error) {
	client, err := CreateClient(config)
	if err != nil {
		return nil, err
	}
	if err := client.start(); err != nil {
		return nil, err
	}
	return client, nil
}
