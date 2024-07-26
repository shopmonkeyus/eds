package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	cnats "github.com/shopmonkeyus/go-common/nats"
)

const (
	emptyBufferPauseTime      = time.Millisecond * 50 // time to wait when the buffer is empty to prevent CPU spinning
	minPendingLatency         = time.Second * 2       // minimum accumulation period before flushing
	maxPendingLatency         = time.Second * 30      // maximum accumulation period before flushing
	traceLogNatsProcessDetail = true                  // turn on trace logging for nats processing
)

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

	// Suffix for the consumer name
	Suffix string

	// MaxAckPending is the maximum number of messages that can be in-flight at once.
	MaxAckPending int

	// MaxPendingBuffer is the maximum number of messages that can be buffered before the consumer starts dropping messages.
	MaxPendingBuffer int

	// Driver is the driver for the consumer.
	Driver internal.Driver

	// ExportTableData is the map of table names to mvcc timestamps. This should be provided after an import to make sure the consumer doesnt double process data.
	ExportTableTimestamps map[string]*time.Time

	// Restart the consumer from the beginning of the stream
	Restart bool
}

type Consumer struct {
	ctx             context.Context
	cancel          context.CancelFunc
	max             int
	driver          internal.Driver
	conn            *nats.Conn
	jsconn          jetstream.Consumer
	logger          logger.Logger
	subscriber      jetstream.ConsumeContext
	buffer          chan jetstream.Msg
	pending         []jetstream.Msg
	pendingStarted  *time.Time
	waitGroup       sync.WaitGroup
	once            sync.Once
	lock            sync.Mutex
	stopping        bool
	subError        chan error
	sessionID       string
	tableTimestamps map[string]*time.Time
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
		c.flush()
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

func (c *Consumer) flush() bool {
	c.logger.Trace("flush")
	if c.driver == nil {
		return c.stopping
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if err := c.driver.Flush(); err != nil {
		if errors.Is(err, internal.ErrDriverStopped) {
			c.nackEverything()
			return true
		}
		c.handleError(err)
		return true
	}
	for _, m := range c.pending {
		if err := m.Ack(); err != nil {
			c.logger.Error("error acking msg %s: %s", m.Headers().Get(nats.MsgIdHdr), err)
			c.nackEverything()
			return true
		}
	}
	c.pending = nil
	c.pendingStarted = nil
	return c.stopping
}
func (c *Consumer) shouldSkip(evt *internal.DBChangeEvent) bool {
	if c.tableTimestamps == nil {
		return false
	}
	eventTimestamp := time.UnixMilli(evt.Timestamp)
	// check if we have a timestamp for this table and only process if its newer
	if tableTimestamp := c.tableTimestamps[evt.Table]; tableTimestamp != nil {
		return eventTimestamp.Before(*tableTimestamp)
	}
	return false
}

func (c *Consumer) Error() <-chan error {
	return c.subError
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
			log := c.logger.With(map[string]any{
				"msgId":   msg.Headers().Get(nats.MsgIdHdr),
				"subject": msg.Subject(),
			})
			if m, err := msg.Metadata(); err == nil {
				log.Trace("msg received - deliveries=%d,consumer=%d,stream=%d,pending=%d", m.NumDelivered, m.Sequence.Consumer, m.Sequence.Stream, len(c.pending))
			}
			c.pending = append(c.pending, msg)
			buf := msg.Data()
			md, _ := msg.Metadata()
			var evt internal.DBChangeEvent
			if err := json.Unmarshal(buf, &evt); err != nil {
				log.Error("error unmarshalling: %s (seq:%d): %s", string(buf), md.Sequence.Consumer, err)
				c.handleError(err)
				return
			}
			if c.shouldSkip(&evt) {
				log.Trace("skipping event due to timestamp %d", evt.Timestamp)
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
				continue
			}
			flush, err := c.driver.Process(evt)
			if err != nil {
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
				if c.flush() {
					return
				}
				continue
			}
			if c.pendingStarted == nil {
				ts := time.Now()
				c.pendingStarted = &ts
			}
			if md.NumPending > uint64(c.max) && time.Since(*c.pendingStarted) < maxPendingLatency*2 {
				continue // if we have a large number, just keep going to try and catchup
			}
			if len(c.pending) >= c.max || time.Since(*c.pendingStarted) >= maxPendingLatency {
				if traceLogNatsProcessDetail {
					log.Trace("flush 2 called. flush=%v,pending=%d,max=%d,started=%v", flush, len(c.pending), maxsize, time.Since(*c.pendingStarted))
				}
				if c.flush() {
					return
				}
				continue
			}
		default:
			count := len(c.pending)
			if count > 0 && count < c.max && time.Since(*c.pendingStarted) >= minPendingLatency {
				if traceLogNatsProcessDetail {
					c.logger.Trace("flush 3 called.count=%d,max=%d,started=%v", count, c.max, time.Since(*c.pendingStarted))
				}
				if c.flush() {
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
				time.Sleep(emptyBufferPauseTime)
			}
		}
	}
}

func (c *Consumer) process(msg jetstream.Msg) {
	c.buffer <- msg
}

type heartbeat struct {
	SessionId string `json:"sessionId" msgpack:"sessionId"`
}

func (c *Consumer) heartbeat(subject string, payload []byte) error {
	msg := nats.NewMsg(subject)
	msgId := util.Hash(time.Now().UnixNano())
	msg.Header.Set(nats.MsgIdHdr, msgId)
	msg.Data = payload
	if err := c.conn.PublishMsg(msg); err != nil {
		return err
	}
	c.logger.Trace("heartbeat sent %s", msgId)
	return nil
}

// sendHeartbeats sends a heartbeat every minute
func (c *Consumer) sendHeartbeats() {
	// payload never changes, so we can just create it once
	heartbeatSubject := fmt.Sprintf("eds.heartbeat.%s", c.sessionID)
	heartbeatPayload := []byte(util.JSONStringify(heartbeat{SessionId: c.sessionID}))
	// first heartbeat
	if err := c.heartbeat(heartbeatSubject, heartbeatPayload); err != nil {
		c.logger.Error("error sending heartbeat: %s", err)
	}
	// we dont need the WG here since this doesnt need to gracefully complete
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			c.logger.Debug("context done, stopping heartbeat")
			return
		case <-ticker.C:
			if err := c.heartbeat(heartbeatSubject, heartbeatPayload); err != nil {
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
	companyID  string
	sessionID  string
}

func NewNatsConnection(logger logger.Logger, url string, creds string) (*nats.Conn, *CredentialInfo, error) {
	var natsCredentials nats.Option
	var info *CredentialInfo

	if util.IsLocalhost(url) || creds == "" {
		info = &CredentialInfo{
			companyIDs: []string{"*"},
			companyID:  "dev",
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
	nc, err := cnats.NewNats(logger, "eds-server-"+info.companyID, url, natsCredentials)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating nats connection: %w", err)
	}

	return nc, info, nil
}

func (c *Consumer) Start() error {
	if c.subscriber != nil {
		return fmt.Errorf("consumer already started")
	}
	// start consuming messages
	sub, err := c.jsconn.Consume(c.process)
	if err != nil {
		c.conn.Close()
		return fmt.Errorf("error starting jetstream consumer: %w", err)
	}
	c.subscriber = sub

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

	ctx, cancel := context.WithCancel(config.Context)

	var consumer Consumer
	consumer.max = config.MaxAckPending
	consumer.ctx = ctx
	consumer.cancel = cancel
	consumer.conn = nc
	consumer.driver = config.Driver
	consumer.buffer = make(chan jetstream.Msg, config.MaxAckPending)
	consumer.pending = make([]jetstream.Msg, 0)
	consumer.subError = make(chan error, 10)
	consumer.sessionID = info.sessionID
	consumer.tableTimestamps = config.ExportTableTimestamps
	consumer.logger = config.Logger.WithPrefix("[consumer]")

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
					natsLogger.Trace("nats tx: %s: %s", subj, string(payload))
				},
				ResponseReceived: func(subj string, payload []byte, hdr nats.Header) {
					natsLogger.Trace("nats rx: %s: %s", subj, string(payload))
				},
			},
		),
	)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("error creating jetstream connection: %w", err)
	}

	consumer.logger.Info("using info from credentials, name: %s companies: %s, session %s", info.companyID, info.companyIDs, info.sessionID)

	var suffix string
	if config.Suffix != "" {
		suffix = "-" + config.Suffix
	}
	name := fmt.Sprintf("eds-server-%s%s", info.companyID, suffix)
	var subjects []string
	for _, companyID := range info.companyIDs {
		subject := "dbchange.*.*." + companyID + ".*.PUBLIC.>"
		subjects = append(subjects, subject)
	}

	jsConfig := jetstream.ConsumerConfig{
		Durable:           name,
		MaxAckPending:     config.MaxAckPending,
		MaxDeliver:        1_000,
		AckWait:           time.Minute * 5,
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		MaxRequestBatch:   config.MaxPendingBuffer,
		FilterSubjects:    subjects,
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: time.Hour * 24 * 3, // expire if unused 3 days from first creating
	}
	if config.Restart {
		jsConfig.DeliverPolicy = jetstream.DeliverAllPolicy
	}
	createConsumerContext, cancelCreate := context.WithDeadline(config.Context, time.Now().Add(time.Minute*10))
	defer cancelCreate()
	c, err := js.CreateOrUpdateConsumer(createConsumerContext, "dbchange", jsConfig)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("error creating jetstream consumer: %w", err)
	}
	cancelCreate()

	consumer.jsconn = c

	return &consumer, nil
}

// NewConsumer creates and starts a new nats consumer
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	consumer, err := CreateConsumer(config)
	if err != nil {
		return nil, err
	}
	if err := consumer.Start(); err != nil {
		return nil, err
	}
	return consumer, nil
}
