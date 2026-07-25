package transmission

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
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultFetchInterval = 2 * time.Second
	DefaultMaxCount      = 200
	DefaultFetchTimeout  = 500 * time.Millisecond

	controlActionStart = "start"
	controlActionPause = "pause"
)

// Driver is the subset of the destination driver used by the transmission poller.
type Driver interface {
	Flush(logger logger.Logger) error
	Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error)
	MaxBatchSize() int
}

// Config configures a transmission Client.
type Config struct {
	Context               context.Context
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

// Client connects to Transmission over gRPC, keeps a Control stream open, and
// polls Fetch on a fixed interval.
type Client struct {
	ctx             context.Context
	cancel          context.CancelFunc
	logger          logger.Logger
	edsID           string
	sessionID       string
	driver          Driver
	registry        internal.SchemaRegistry
	validator       internal.SchemaValidator
	tableTimestamps map[string]*time.Time
	fetchInterval   time.Duration
	maxCount        int32
	fetchTimeout    time.Duration
	maxBatch        int

	conn   *grpc.ClientConn
	client transmissionv1.TransmissionServiceClient

	errCh         chan error
	disconnected  chan bool
	waitGroup     sync.WaitGroup
	once          sync.Once
	lock          sync.Mutex
	paused        bool
	stopping      bool
	controlCancel context.CancelFunc
	ready         chan struct{}
	readyOnce     sync.Once
}

// NewClient dials Transmission and starts Control + Fetch loops.
func NewClient(config Config) (*Client, error) {
	if config.Address == "" {
		return nil, fmt.Errorf("transmission address is required")
	}
	if config.EdsID == "" {
		return nil, fmt.Errorf("eds id is required")
	}
	if config.Driver == nil {
		return nil, fmt.Errorf("driver is required")
	}

	ctx, cancel := context.WithCancel(config.Context)
	log := config.Logger.WithPrefix("[transmission]")

	interval := config.FetchInterval
	if interval <= 0 {
		interval = DefaultFetchInterval
	}
	maxCount := config.MaxCount
	if maxCount <= 0 {
		maxCount = DefaultMaxCount
	}
	fetchTimeout := config.FetchTimeout
	if fetchTimeout <= 0 {
		fetchTimeout = DefaultFetchTimeout
	}

	maxBatch := config.Driver.MaxBatchSize()
	if maxBatch <= 0 {
		maxBatch = int(maxCount)
	}

	conn, err := grpc.NewClient(config.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("error dialing transmission at %s: %w", config.Address, err)
	}

	c := &Client{
		ctx:             ctx,
		cancel:          cancel,
		logger:          log,
		edsID:           config.EdsID,
		sessionID:       config.SessionID,
		driver:          config.Driver,
		registry:        config.Registry,
		validator:       config.SchemaValidator,
		tableTimestamps: config.ExportTableTimestamps,
		fetchInterval:   interval,
		maxCount:        maxCount,
		fetchTimeout:    fetchTimeout,
		maxBatch:        maxBatch,
		conn:            conn,
		client:          transmissionv1.NewTransmissionServiceClient(conn),
		errCh:           make(chan error, 10),
		disconnected:    make(chan bool),
		ready:           make(chan struct{}),
	}

	if p, ok := config.Driver.(internal.DriverSessionHandler); ok && config.SessionID != "" {
		p.SetSessionID(config.SessionID)
	}

	c.waitGroup.Add(2)
	go c.runControl()
	go c.runFetch()

	log.Info("connected to transmission at %s (eds_id=%s)", config.Address, config.EdsID)
	return c, nil
}

// Error returns a channel that receives fatal client errors.
func (c *Client) Error() <-chan error {
	return c.errCh
}

// Disconnected returns a channel that is closed when the gRPC connection is lost.
func (c *Client) Disconnected() <-chan bool {
	return c.disconnected
}

// Pause stops fetching messages.
func (c *Client) Pause() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.paused = true
	c.logger.Debug("paused")
}

// Unpause resumes fetching messages.
func (c *Client) Unpause() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.paused = false
	c.logger.Debug("unpaused")
	return nil
}

// Stop shuts down the client and closes the gRPC connection.
func (c *Client) Stop() error {
	c.once.Do(func() {
		c.logger.Debug("stopping")
		c.lock.Lock()
		c.stopping = true
		c.lock.Unlock()
		if c.controlCancel != nil {
			c.controlCancel()
		}
		c.cancel()
		c.waitGroup.Wait()
		if c.conn != nil {
			c.conn.Close()
		}
		c.logger.Debug("stopped")
	})
	return nil
}

func (c *Client) isStopping() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stopping
}

func (c *Client) isPaused() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.paused
}

func (c *Client) markReady() {
	c.readyOnce.Do(func() {
		close(c.ready)
	})
}

func (c *Client) handleError(err error) {
	if c.isStopping() {
		return
	}
	c.logger.Error("%s", err)
	select {
	case c.errCh <- err:
	default:
	}
}

func (c *Client) signalDisconnected() {
	select {
	case <-c.disconnected:
	default:
		close(c.disconnected)
	}
}

func (c *Client) runControl() {
	defer c.waitGroup.Done()
	defer c.signalDisconnected()

	controlCtx, controlCancel := context.WithCancel(c.ctx)
	c.lock.Lock()
	c.controlCancel = controlCancel
	c.lock.Unlock()
	defer controlCancel()

	stream, err := c.client.Control(controlCtx, &transmissionv1.ControlRequest{EdsId: c.edsID})
	if err != nil {
		c.handleError(fmt.Errorf("error opening control stream: %w", err))
		return
	}
	c.logger.Debug("control stream attached")

	// Allow fetching immediately; Transmission may also send an explicit start.
	c.markReady()

	for {
		resp, err := stream.Recv()
		if err != nil {
			if c.isStopping() || c.ctx.Err() != nil {
				return
			}
			if err == io.EOF {
				c.handleError(fmt.Errorf("control stream closed"))
				return
			}
			c.handleError(fmt.Errorf("error reading control stream: %w", err))
			return
		}

		switch resp.GetAction() {
		case controlActionStart:
			c.logger.Info("received start from transmission")
			c.markReady()
			_ = c.Unpause()
		case controlActionPause:
			c.logger.Info("received pause from transmission")
			c.Pause()
		default:
			c.logger.Debug("ignoring control action: %s", resp.GetAction())
		}
	}
}

func (c *Client) runFetch() {
	defer c.waitGroup.Done()

	select {
	case <-c.ready:
	case <-c.ctx.Done():
		return
	}

	ticker := time.NewTicker(c.fetchInterval)
	defer ticker.Stop()

	// Fetch once immediately after becoming ready.
	c.fetchOnce()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if c.isPaused() || c.isStopping() {
				continue
			}
			c.fetchOnce()
		}
	}
}

func (c *Client) fetchOnce() {
	ctx, cancel := context.WithTimeout(c.ctx, c.fetchInterval+c.fetchTimeout)
	defer cancel()

	c.logger.Debug("fetching (maxCount=%d timeout=%s)", c.maxCount, c.fetchTimeout)

	stream, err := c.client.Fetch(ctx, &transmissionv1.FetchRequest{
		EdsId:     c.edsID,
		MaxCount:  c.maxCount,
		TimeoutMs: c.fetchTimeout.Milliseconds(),
	})
	if err != nil {
		if c.isStopping() || c.ctx.Err() != nil {
			return
		}
		c.handleError(fmt.Errorf("error calling fetch: %w", err))
		return
	}

	pending := 0
	received := 0
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			if c.isStopping() || c.ctx.Err() != nil {
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
			pending++
			received++
			if pending >= c.maxBatch {
				if err := c.driver.Flush(c.logger); err != nil {
					c.handleError(fmt.Errorf("error flushing driver: %w", err))
					return
				}
				pending = 0
			}
		}
	}

	if pending > 0 {
		if err := c.driver.Flush(c.logger); err != nil {
			c.handleError(fmt.Errorf("error flushing driver: %w", err))
			return
		}
	}

	c.logger.Debug("fetch completed (messages=%d)", received)
}

func (c *Client) shouldSkip(logger logger.Logger, evt *internal.DBChangeEvent) bool {
	if c.tableTimestamps != nil {
		eventTimestamp := time.UnixMilli(evt.Timestamp)
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
				logger.Debug("skipping %s, schema did not validate (%s) for event: %s", evt.Table, strings.TrimSpace(strings.Join(strings.Split(err.Error(), "\n"), " ")), util.JSONStringify(evt))
				return true
			}
			logger.Error("error validating schema: %s for event: %s", err, util.JSONStringify(evt))
			return true
		}
		if !found {
			logger.Trace("skipping %s, no schema found for event: %s", evt.Table, util.JSONStringify(evt))
			return true
		}
		if !valid {
			logger.Trace("skipping %s, schema did not validate for event: %s", evt.Table, util.JSONStringify(evt))
			return true
		}
		if path != "" {
			evt.SchemaValidatedPath = &path
			logger.Trace("schema validated %s", path)
		}
	}
	return false
}

func (c *Client) processPayload(payload []byte) error {
	evt, err := internal.DBChangeEventFromPayload(payload)
	if err != nil {
		return err
	}

	log := c.logger.With(map[string]any{
		"table":     evt.Table,
		"operation": evt.Operation,
		"id":        evt.ID,
	})

	if c.shouldSkip(log, &evt) {
		log.Debug("skipping event")
		return nil
	}

	if evt.Operation != "DELETE" && c.registry != nil {
		schema, err := c.registry.GetSchema(evt.Table, evt.ModelVersion)
		if err != nil {
			return fmt.Errorf("error getting schema for table %s version %s: %w", evt.Table, evt.ModelVersion, err)
		}
		object, err := evt.GetObject()
		if err != nil {
			return fmt.Errorf("error getting object: %w", err)
		}
		diff := util.JSONDiff(object, schema.Columns())
		if len(diff) > 0 {
			if err := evt.OmitProperties(diff...); err != nil {
				return fmt.Errorf("error omitting properties: %w", err)
			}
		}
	}

	flush, err := c.driver.Process(log, evt)
	if err != nil {
		return err
	}
	if flush {
		return c.driver.Flush(log)
	}
	return nil
}
