package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	cnats "github.com/shopmonkeyus/go-common/nats"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	defaultEmptyBufferPauseTime = time.Millisecond * 10 // time to wait when the buffer is empty to prevent CPU spinning
	DefaultMinPendingLatency    = time.Second * 2       // minimum accumulation period before flushing
	DefaultMaxPendingLatency    = time.Second * 30      // maximum accumulation period before flushing
	traceLogNatsProcessDetail   = true                  // turn on trace logging for nats processing
)

// Driver is a local interface which slims down the driver to only the methods we need to make it easier to test.
type Driver interface {
	Flush(logger logger.Logger) error
	Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error)
	MaxBatchSize() int
}

// ConsumerConfig is the configuration for the consumer.
type ConsumerConfig struct {

	// Context is the context for the consumer.
	Context context.Context

	// Logger is the logger for the consumer.
	Logger logger.Logger

	// URL to the nats server
	URL string

	// Credentials for the nats server
	Credentials string

	// CompanyIDs is the list of company IDs to listen for. If empty, all companies will be listened to.
	CompanyIDs []string

	// Suffix for the consumer name
	Suffix string

	// MaxAckPending is the maximum number of messages that can be in-flight at once.
	MaxAckPending int

	// MaxPendingBuffer is the maximum number of messages that can be buffered before the consumer starts dropping messages.
	MaxPendingBuffer int

	// Driver is the driver for the consumer.
	Driver Driver

	// ExportTableData is the map of table names to mvcc timestamps. This should be provided after an import to make sure the consumer doesnt double process data.
	ExportTableTimestamps map[string]*time.Time

	// DeliverAll will configure the consumer to read from the beginning of the stream, this only works if the consumer is new
	DeliverAll bool

	// SchemaValidator is the schema validator to use for the importer or nil if not needed.
	SchemaValidator internal.SchemaValidator

	// HeartbeatInterval is the interval to send heartbeats. Defaults to 1 minute.
	HeartbeatInterval time.Duration

	// MinPendingLatency is the minimum accumulation period before flushing.
	MinPendingLatency time.Duration

	// MaxPendingLatency is the maximum accumulation period before flushing.
	MaxPendingLatency time.Duration

	// EmptyBufferPauseTime is the time to wait when the buffer is empty to prevent CPU spinning.
	EmptyBufferPauseTime time.Duration

	// Registry returns the schema registry to use.
	Registry internal.SchemaRegistry

	sessionIDCallback func(id string) // only used in testing
}

type Consumer struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	max                  int
	driver               Driver
	conn                 *nats.Conn
	jsconn               jetstream.Consumer
	logger               logger.Logger
	subscriber           jetstream.ConsumeContext
	buffer               chan jetstream.Msg
	pending              []jetstream.Msg
	started              *time.Time
	pendingStarted       *time.Time
	pauseStarted         *time.Time
	waitGroup            sync.WaitGroup
	once                 sync.Once
	lock                 sync.Mutex
	stopping             bool
	subError             chan error
	sessionID            string
	tableTimestamps      map[string]*time.Time
	validator            internal.SchemaValidator
	heartbeatInterval    time.Duration
	minPendingLatency    time.Duration
	maxPendingLatency    time.Duration
	emptyBufferPauseTime time.Duration
	offset               int64
	supportsMigration    bool
	registry             internal.SchemaRegistry
}

// Stop the consumer and close the connection to the NATS server.
func (c *Consumer) Stop() error {
	c.logger.Debug("stopping consumer")
	c.once.Do(func() {
		c.logger.Debug("stopping bufferer")
		// set the consumer to stopping in a safe way since we have the goroutine running
		c.lock.Lock()
		c.stopping = true
		c.lock.Unlock()
		c.flush(c.logger)
		c.cancel()
		c.logger.Debug("waiting on bufferer")
		c.waitGroup.Wait()
		c.logger.Debug("stopped bufferer")

		// once we get here, the bufferer should be done and its safe to start shutting down

		c.nackEverything() // just be safe

		if c.subscriber != nil {
			c.logger.Debug("stopping subscriber")
			c.subscriber.Stop()
			c.logger.Debug("stopped subscriber")
		}
		if c.conn != nil {
			c.logger.Debug("stopping nats connection")
			c.conn.Close()
			c.logger.Debug("stopped nats connection")
		}
		close(c.buffer)
		c.subscriber = nil
		c.conn = nil
	})
	c.logger.Debug("stopped consumer")
	return nil
}

func (c *Consumer) nackEverything() {
	c.logger.Debug("nack everything")
	for _, m := range c.pending {
		if err := m.Nak(); err != nil {
			c.logger.Error("error nacking msg %s: %s", m.Headers().Get(nats.MsgIdHdr), err)
		}
	}
	c.pending = nil
	c.pendingStarted = nil
}

func (c *Consumer) handleError(err error) {
	c.logger.Error("error: %s", err)
	c.nackEverything()
	c.subError <- err
}

func (c *Consumer) flush(logger logger.Logger) bool {
	logger.Trace("flush")
	if c.driver == nil {
		return c.stopping
	}
	started := time.Now()
	c.lock.Lock()
	defer c.lock.Unlock()
	if err := c.driver.Flush(logger); err != nil {
		if errors.Is(err, internal.ErrDriverStopped) {
			c.nackEverything()
			return true
		}
		c.handleError(err)
		return true
	}
	var count float64
	for _, m := range c.pending {
		if err := m.Ack(); err != nil {
			internal.PendingEvents.Dec()
			logger.Error("error acking msg %s: %s", m.Headers().Get(nats.MsgIdHdr), err)
			c.nackEverything()
			return true
		}
		internal.PendingEvents.Dec()
		count++
	}
	if c.pendingStarted != nil {
		processingDuration := time.Since(*c.pendingStarted)
		internal.ProcessingDuration.Observe(processingDuration.Seconds())
	}
	internal.FlushDuration.Observe(time.Since(started).Seconds())
	internal.FlushCount.Observe(count)
	c.pending = nil
	c.pendingStarted = nil
	return c.stopping
}

func (c *Consumer) shouldSkip(logger logger.Logger, evt *internal.DBChangeEvent) bool {
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
			logger.Error("error validating schema: %s", err)
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

func (c *Consumer) Error() <-chan error {
	return c.subError
}

func (c *Consumer) handlePossibleMigration(ctx context.Context, logger logger.Logger, event *internal.DBChangeEvent) error {
	found, version, err := c.registry.GetTableVersion(event.Table)
	if err != nil {
		return fmt.Errorf("error getting current table version for table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
	}
	if !found || version != event.ModelVersion {
		newschema, err := c.registry.GetSchema(event.Table, event.ModelVersion)
		if err != nil {
			return fmt.Errorf("error getting new schema for table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
		}
		migration := c.driver.(internal.DriverMigration)
		if !found {
			logger.Debug("need to migrate new table: %s, model version: %s", event.Table, event.ModelVersion)
			if err := migration.MigrateNewTable(ctx, logger, newschema); err != nil {
				return fmt.Errorf("error migrating new table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
			}
			logger.Info("migrated new table: %s, model version: %s", event.Table, event.ModelVersion)
			if err := c.registry.SetTableVersion(event.Table, event.ModelVersion); err != nil {
				return fmt.Errorf("error setting table version for table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
			}
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
			logger.Debug("need to migrate table: %s, columns: %s, model version: %s", event.Table, strings.Join(columns, ","), event.ModelVersion)
			if err := migration.MigrateNewColumns(ctx, logger, newschema, columns); err != nil {
				return fmt.Errorf("error migrating new columns for table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
			}
			for _, col := range columns {
				if !util.SliceContains(event.Diff, col) {
					event.Diff = append(event.Diff, col) // add these new columns to the diff so that we can update them as part of the changeset
				}
			}
			logger.Info("migrated table: %s, columns: %s, model version: %s", event.Table, strings.Join(columns, ","), event.ModelVersion)
		} else {
			logger.Info("new table: %s with different model version: %s but no new columns added", event.Table, event.ModelVersion)
		}
		// we want to bring the table up to the new version in all cases
		if err := c.registry.SetTableVersion(event.Table, event.ModelVersion); err != nil {
			return fmt.Errorf("error setting table version for table: %s, model version: %s: %w", event.Table, event.ModelVersion, err)
		}
	}
	return nil
}

func (c *Consumer) bufferer() {
	c.logger.Trace("starting bufferer")
	c.waitGroup.Add(1)
	defer func() {
		c.waitGroup.Done()
		c.logger.Trace("stopped bufferer")
	}()
	for {
		select {
		case <-c.ctx.Done():
			c.nackEverything()
			return
		case msg := <-c.buffer:
			if msg == nil {
				return
			}
			m, err := msg.Metadata()
			if err != nil {
				c.handleError(err)
				return
			}
			log := c.logger.With(map[string]any{
				"msgId":   msg.Headers().Get(nats.MsgIdHdr),
				"subject": msg.Subject(),
				"seq":     m.Sequence.Consumer,
				"sid":     m.Sequence.Stream,
			})
			log.Trace("msg received - deliveries=%d,pending=%d", m.NumDelivered, len(c.pending))
			c.pending = append(c.pending, msg)
			buf := msg.Data()
			md, _ := msg.Metadata()
			var evt internal.DBChangeEvent
			if err := json.Unmarshal(buf, &evt); err != nil {
				internal.PendingEvents.Dec()
				log.Error("error unmarshalling: %s (seq:%d): %s", string(buf), md.Sequence.Consumer, err)
				c.handleError(err)
				return
			}
			if c.shouldSkip(log, &evt) {
				log.Debug("skipping event")
				if err := msg.Ack(); err != nil {
					// not much we can do here, just log it
					log.Error("error acking skipped msg: %s", err)
				}
				// remove from pending
				for i, m := range c.pending {
					if m == msg {
						c.pending = append(c.pending[:i], c.pending[i+1:]...)
						break
					}
				}
				internal.PendingEvents.Dec()
				continue
			}
			evt.NatsMsg = msg // in case the driver wants to get specific information from it for logging, etc

			// check to see if we need to perform a migration
			if c.supportsMigration {
				if err := c.handlePossibleMigration(c.ctx, log, &evt); err != nil {
					c.handleError(err)
					return
				}
			}

			flush, err := c.driver.Process(log, evt)
			if err != nil {
				internal.PendingEvents.Dec()
				c.handleError(err)
				return
			}
			maxsize := c.driver.MaxBatchSize()
			if maxsize <= 0 {
				maxsize = c.max
			}
			if traceLogNatsProcessDetail {
				log.Trace("process returned. flush=%v,pending=%d,max=%d", flush, len(c.pending), maxsize)
			}
			if flush || len(c.pending) >= maxsize {
				if traceLogNatsProcessDetail {
					log.Trace("flush 1 called. flush=%v,pending=%d,max=%d", flush, len(c.pending), maxsize)
				}
				if c.flush(log) {
					return
				}
				continue
			}
			if c.pendingStarted == nil {
				ts := time.Now()
				c.pendingStarted = &ts
			}
			if md.NumPending > uint64(c.max) && time.Since(*c.pendingStarted) < c.maxPendingLatency*2 {
				continue // if we have a large number, just keep going to try and catchup
			}
			if len(c.pending) >= c.max || time.Since(*c.pendingStarted) >= c.maxPendingLatency {
				if traceLogNatsProcessDetail {
					log.Trace("flush 2 called. flush=%v,pending=%d,max=%d,started=%v", flush, len(c.pending), maxsize, time.Since(*c.pendingStarted))
				}
				if c.flush(log) {
					return
				}
				continue
			}
		default:
			count := len(c.pending)
			if count > 0 && count < c.max && c.pendingStarted != nil && time.Since(*c.pendingStarted) >= c.minPendingLatency {
				if traceLogNatsProcessDetail {
					c.logger.Trace("flush 3 called. count=%d,max=%d,started=%v", count, c.max, time.Since(*c.pendingStarted))
				}
				if c.flush(c.logger) {
					return
				}
				continue
			}
			if count > 0 {
				continue
			}
			select {
			case <-c.ctx.Done():
				c.logger.Debug("context done")
				c.nackEverything()
				return
			default:
				time.Sleep(c.emptyBufferPauseTime)
			}
		}
	}
}

func (c *Consumer) process(msg jetstream.Msg) {
	internal.PendingEvents.Inc()
	internal.TotalEvents.Inc()
	c.buffer <- msg
}

type heartbeat struct {
	SessionId string               `json:"sessionId" msgpack:"sessionId"`
	Offset    int64                `json:"offset" msgpack:"offset"`
	Uptime    time.Duration        `json:"uptime" msgpack:"uptime"`
	Stats     internal.SystemStats `json:"stats" msgpack:"stats"`
	Paused    *time.Time           `json:"paused,omitempty" msgpack:"paused,omitempty"`
}

func (c *Consumer) heartbeat() error {
	stats, err := internal.GetSystemStats()
	if err != nil {
		return fmt.Errorf("error getting system stats: %w", err)
	}

	subject := fmt.Sprintf("eds.client.%s.heartbeat", c.sessionID)

	hb := heartbeat{
		SessionId: c.sessionID,
		Stats:     *stats,
		Uptime:    time.Duration(time.Since(*c.started).Seconds()),
		Paused:    c.pauseStarted,
		Offset:    c.offset,
	}

	c.offset++

	var buffer bytes.Buffer
	enc := msgpack.NewEncoder(&buffer)
	enc.SetCustomStructTag("json")
	if err := enc.Encode(hb); err != nil {
		return fmt.Errorf("error encoding heartbeat: %w", err)
	}
	msg := nats.NewMsg(subject)
	msgId := util.Hash(time.Now().UnixNano(), c.offset)
	msg.Header.Set(nats.MsgIdHdr, msgId)
	msg.Header.Set("content-encoding", "msgpack")
	msg.Data = buffer.Bytes()
	if err := c.conn.PublishMsg(msg); err != nil {
		return err
	}
	c.logger.Trace("heartbeat sent %s with: %v", msgId, util.JSONStringify(hb))
	return nil
}

// sendHeartbeats sends a heartbeat every minute
func (c *Consumer) sendHeartbeats() {

	// first heartbeat
	if err := c.heartbeat(); err != nil {
		c.logger.Error("error sending heartbeat: %s", err)
	}
	// we dont need the WG here since this doesnt need to gracefully complete
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			c.logger.Debug("context done, stopping heartbeat")
			return
		case <-ticker.C:
			if err := c.heartbeat(); err != nil {
				c.logger.Error("error sending heartbeat: %s", err)
			}
		}
	}
}

func (c *Consumer) Name() string {
	return c.jsconn.CachedInfo().Config.Durable
}

type CredentialInfo struct {
	companyIDs []string
	serverID   string
	sessionID  string
}

func NewNatsConnection(logger logger.Logger, url string, creds string) (*nats.Conn, *CredentialInfo, error) {
	var natsCredentials nats.Option
	var info *CredentialInfo

	if util.IsLocalhost(url) || creds == "" {
		info = &CredentialInfo{
			companyIDs: []string{"*"},
			serverID:   "dev",
			sessionID:  uuid.NewString(),
		}
		logger.Debug("using localhost nats server")
	} else {
		var err error
		natsCredentials, info, err = getNatsCreds(creds)
		if err != nil {
			return nil, nil, err
		}
	}

	// Nats connection to main NATS server
	nc, err := cnats.NewNats(logger, "eds-"+info.serverID, url, natsCredentials)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating nats connection: %w", err)
	}

	return nc, info, nil
}

func (c *Consumer) Pause() {
	c.logger.Debug("pausing")
	c.subscriber.Drain()
	c.subscriber = nil
	t := time.Now()
	c.pauseStarted = &t
	c.logger.Debug("paused")
}

func (c *Consumer) Unpause() error {
	if c.subscriber != nil {
		return fmt.Errorf("consumer already started")
	}
	// start consuming messages
	sub, err := c.jsconn.Consume(
		c.process,
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			c.logger.Warn("consumer error: %s", err)
		}),
		jetstream.PullExpiry(time.Minute),
		jetstream.PullMaxMessages(4_096),
	)
	if err != nil {
		c.conn.Close()
		return fmt.Errorf("error starting jetstream consumer: %w", err)
	}
	c.subscriber = sub
	c.pauseStarted = nil
	return nil
}

func (c *Consumer) start() error {
	if c.subscriber != nil {
		return fmt.Errorf("consumer already started")
	}

	if err := c.Unpause(); err != nil {
		return err
	}

	// start the background processor
	go c.bufferer()

	// start the heartbeat
	go c.sendHeartbeats()

	c.logger.Debug("started")
	return nil
}

// CreateConsumer creates a new nats consumer, but does not start it.
func CreateConsumer(config ConsumerConfig) (*Consumer, error) {
	nc, info, err := NewNatsConnection(config.Logger, config.URL, config.Credentials)
	if err != nil {
		return nil, err
	}

	// for unit testing only
	if config.sessionIDCallback != nil {
		config.sessionIDCallback(info.sessionID)
	}

	ctx, cancel := context.WithCancel(config.Context)

	if config.MaxAckPending <= 0 {
		config.MaxAckPending = 25_000
	}

	var startAt *time.Time
	var consumer Consumer
	started := time.Now()
	consumer.started = &started
	consumer.max = config.MaxAckPending
	consumer.ctx = ctx
	consumer.cancel = cancel
	consumer.conn = nc
	consumer.driver = config.Driver
	consumer.buffer = make(chan jetstream.Msg, config.MaxAckPending)
	consumer.pending = make([]jetstream.Msg, 0)
	consumer.subError = make(chan error, 10)
	consumer.sessionID = info.sessionID
	consumer.validator = config.SchemaValidator
	consumer.registry = config.Registry
	if _, ok := config.Driver.(internal.DriverMigration); ok {
		consumer.supportsMigration = true
	}
	consumer.heartbeatInterval = config.HeartbeatInterval
	if consumer.heartbeatInterval == 0 {
		consumer.heartbeatInterval = time.Minute
	}
	consumer.minPendingLatency = config.MinPendingLatency
	if consumer.minPendingLatency == 0 {
		consumer.minPendingLatency = DefaultMinPendingLatency
	}
	consumer.maxPendingLatency = config.MaxPendingLatency
	if consumer.maxPendingLatency == 0 {
		consumer.maxPendingLatency = DefaultMaxPendingLatency
	}
	consumer.emptyBufferPauseTime = config.EmptyBufferPauseTime
	if consumer.emptyBufferPauseTime == 0 {
		consumer.emptyBufferPauseTime = defaultEmptyBufferPauseTime
	}

	consumer.logger = config.Logger.WithPrefix("[consumer]")
	if config.ExportTableTimestamps != nil {
		consumer.tableTimestamps = config.ExportTableTimestamps
		// get the earliest timestamp
		for _, ts := range config.ExportTableTimestamps {
			if ts != nil && (startAt == nil || ts.Before(*startAt)) {
				startAt = ts
			}
		}
	}

	// set company ID overrides
	if len(config.CompanyIDs) > 0 {
		var newCompanyIDs []string
		for _, companyID := range config.CompanyIDs {
			if !util.SliceContains(info.companyIDs, companyID) {
				return nil, fmt.Errorf("provided company ID %s not in credentials", companyID)
			}
			newCompanyIDs = append(newCompanyIDs, companyID)
		}
		if len(newCompanyIDs) == 0 {
			return nil, fmt.Errorf("no valid company IDs provided")
		}
		info.companyIDs = newCompanyIDs
		consumer.logger.Debug("using override company IDs: %v", newCompanyIDs)
	}

	if config.Driver != nil {
		if p, ok := config.Driver.(internal.DriverSessionHandler); ok {
			p.SetSessionID(consumer.sessionID)
		}
	} else {
		config.Logger.Debug("no driver set")
	}

	natsLogger := config.Logger.WithPrefix("[nats]")
	js, err := jetstream.New(nc,
		jetstream.WithClientTrace(
			&jetstream.ClientTrace{
				RequestSent: func(subj string, payload []byte) {
					natsLogger.Trace("tx: %s: %s", subj, string(payload))
				},
				ResponseReceived: func(subj string, payload []byte, hdr nats.Header) {
					natsLogger.Trace("rx: %s: %s", subj, string(payload))
				},
			},
		),
	)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("error creating jetstream connection: %w", err)
	}

	consumer.logger.Info("using info from credentials, server: %s companies: %s, session: %s", info.serverID, info.companyIDs, info.sessionID)

	var suffix string
	if config.Suffix != "" {
		suffix = "-" + config.Suffix
	}
	name := fmt.Sprintf("eds-%s%s", info.serverID, suffix)
	var subjects []string
	for _, companyID := range info.companyIDs {
		subject := "dbchange.*.*." + companyID + ".*.PUBLIC.>"
		subjects = append(subjects, subject)
	}

	jsConfig := jetstream.ConsumerConfig{
		Durable:           name,
		MaxAckPending:     config.MaxAckPending,
		MaxDeliver:        20,
		AckWait:           time.Minute * 5,
		MaxRequestBatch:   config.MaxPendingBuffer,
		FilterSubjects:    subjects,
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: time.Hour * 24 * 3, // expire if unused 3 days from first creating
		MaxWaiting:        1,                  // only 1 consumer allowed
	}

	// create a context with a longer deadline for creating the consumer
	configConsumerCtx, cancelConfig := context.WithDeadline(config.Context, time.Now().Add(time.Minute*10))
	defer cancelConfig()

	// setup the consumer
	c, err := js.Consumer(configConsumerCtx, "dbchange", jsConfig.Durable)
	if err != nil {
		if !errors.Is(err, jetstream.ErrConsumerNotFound) {
			nc.Close()
			return nil, fmt.Errorf("error getting jetstream consumer: %w", err)
		}
		// consumer not found, create it

		// only set the deliver policy if we are creating a new consumer, it will error if we try to update it
		if config.DeliverAll {
			jsConfig.DeliverPolicy = jetstream.DeliverAllPolicy
		} else if startAt != nil {
			jsConfig.DeliverPolicy = jetstream.DeliverByStartTimePolicy
			jsConfig.OptStartTime = startAt
		} else {
			jsConfig.DeliverPolicy = jetstream.DeliverNewPolicy
		}

		c, err = js.CreateConsumer(configConsumerCtx, "dbchange", jsConfig)
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("error creating jetstream consumer: %w", err)
		}
	} else {

		jsConfig.DeliverPolicy = c.CachedInfo().Config.DeliverPolicy
		jsConfig.OptStartTime = c.CachedInfo().Config.OptStartTime
		jsConfig.MaxWaiting = c.CachedInfo().Config.MaxWaiting
		consumer.logger.Debug("consumer found, setting delivery policy to %v and start time to %v", jsConfig.DeliverPolicy, jsConfig.OptStartTime)

		// consumer found, update it
		// TODO: we should check if the consumer is already in the correct state and skip this
		c, err = js.UpdateConsumer(configConsumerCtx, "dbchange", jsConfig)
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("error updating jetstream consumer: %w", err)
		}
	}
	cancelConfig()

	consumer.jsconn = c

	return &consumer, nil
}

// NewConsumer creates and starts a new nats consumer
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	consumer, err := CreateConsumer(config)
	if err != nil {
		return nil, err
	}
	if err := consumer.start(); err != nil {
		return nil, err
	}
	return consumer, nil
}
