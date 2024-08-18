package consumer

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack"
)

func runNatsTestServer(fn func(natsurl string, nc *nats.Conn, js jetstream.JetStream)) {
	port, err := util.GetFreePort()
	if err != nil {
		panic(err)
	}
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.Cluster.Name = "testing"
	opts.JetStream = true
	srv := natsserver.RunServer(&opts)
	defer srv.Shutdown()
	url := fmt.Sprintf("nats://localhost:%d", port)
	nc, err := nats.Connect(url)
	if err != nil {
		panic(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:         "dbchange",
		Subjects:     []string{"dbchange.>"},
		MaxConsumers: 1,
		Storage:      jetstream.MemoryStorage,
	}); err != nil {
		panic(err)
	}
	fn(url, nc, js)
}

type mockDriver struct {
	maxBatchSize int
	flush        func(logger logger.Logger) error
	process      func(logger logger.Logger, event internal.DBChangeEvent) (bool, error)
}

func (m *mockDriver) Flush(logger logger.Logger) error {
	if m.flush != nil {
		return m.flush(logger)
	}
	return nil
}
func (m *mockDriver) Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
	if m.process != nil {
		return m.process(logger, event)
	}
	return false, nil
}

func (m *mockDriver) MaxBatchSize() int {
	return m.maxBatchSize
}

func TestStartStop(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {
		mockDriver := &mockDriver{}
		consumer, err := NewConsumer(ConsumerConfig{
			Context: context.Background(),
			Logger:  logger.NewTestLogger(),
			Driver:  mockDriver,
			URL:     natsurl,
		})
		assert.NoError(t, err)
		assert.NoError(t, consumer.Stop())
	})
}

func TestSingleMessage(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {
		var testEvent *internal.DBChangeEvent
		var flushed bool

		mockDriver := &mockDriver{
			process: func(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
				testEvent = &event
				return false, nil
			},
			flush: func(logger logger.Logger) error {
				flushed = true
				return nil
			},
		}

		consumer, err := NewConsumer(ConsumerConfig{
			Context: context.Background(),
			Logger:  logger.NewTestLogger(),
			Driver:  mockDriver,
			URL:     natsurl,
		})

		assert.NoError(t, err)

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.LID.PUBLIC.1", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assert.NoError(t, consumer.Stop())
		assert.NotNil(t, testEvent)
		assert.True(t, flushed)

		assert.Equal(t, sendEvent.Table, testEvent.Table)
		assert.Equal(t, sendEvent.Operation, testEvent.Operation)
		assert.Equal(t, sendEvent.Timestamp, testEvent.Timestamp)
		assert.Equal(t, sendEvent.MVCCTimestamp, testEvent.MVCCTimestamp)
	})
}

func TestSingleMessageWithFlush(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {

		var testEvent *internal.DBChangeEvent
		var flushed bool

		mockDriver := &mockDriver{
			maxBatchSize: 2,
			process: func(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
				testEvent = &event
				return true, nil
			},
			flush: func(logger logger.Logger) error {
				flushed = true
				return nil
			},
		}

		consumer, err := NewConsumer(ConsumerConfig{
			Context: context.Background(),
			Logger:  logger.NewTestLogger(),
			Driver:  mockDriver,
			URL:     natsurl,
		})

		assert.NoError(t, err)

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.LID.PUBLIC.1", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assert.NotNil(t, testEvent)
		assert.True(t, flushed)
		assert.Equal(t, sendEvent.Table, testEvent.Table)
		assert.Equal(t, sendEvent.Operation, testEvent.Operation)
		assert.Equal(t, sendEvent.Timestamp, testEvent.Timestamp)
		assert.Equal(t, sendEvent.MVCCTimestamp, testEvent.MVCCTimestamp)

		assert.NoError(t, consumer.Stop())
	})
}

func TestMultipleMessagesWithFlushAndMaxBatchSize(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {

		var testEvents []internal.DBChangeEvent
		var flushed int

		mockDriver := &mockDriver{
			maxBatchSize: 2,
			process: func(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
				testEvents = append(testEvents, event)
				return false, nil
			},
			flush: func(logger logger.Logger) error {
				flushed++
				return nil
			},
		}

		consumer, err := NewConsumer(ConsumerConfig{
			Context: context.Background(),
			Logger:  logger.NewTestLogger(),
			Driver:  mockDriver,
			URL:     natsurl,
		})

		assert.NoError(t, err)

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.1.PUBLIC.1", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.2.PUBLIC.1", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assert.Len(t, testEvents, 2)
		assert.Equal(t, 1, flushed)

		assert.NoError(t, consumer.Stop())
	})
}

func TestMultipleMessagesWithFlushUsingProcess(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {

		var testEvents []internal.DBChangeEvent
		var flushed int

		mockDriver := &mockDriver{
			maxBatchSize: -1,
			process: func(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
				testEvents = append(testEvents, event)
				return len(testEvents) == 2, nil
			},
			flush: func(logger logger.Logger) error {
				flushed++
				return nil
			},
		}

		consumer, err := NewConsumer(ConsumerConfig{
			Context: context.Background(),
			Logger:  logger.NewTestLogger(),
			Driver:  mockDriver,
			URL:     natsurl,
		})

		assert.NoError(t, err)

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.1.PUBLIC.1", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.2.PUBLIC.1", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assert.Len(t, testEvents, 2)
		assert.Equal(t, 1, flushed)

		assert.NoError(t, consumer.Stop())
	})
}

func TestMultipleMessagesWithMaxAckPending(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {

		var testEvents []internal.DBChangeEvent
		var flushed int

		max := 100

		mockDriver := &mockDriver{
			maxBatchSize: -1,
			process: func(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
				testEvents = append(testEvents, event)
				return false, nil
			},
			flush: func(logger logger.Logger) error {
				flushed++
				return nil
			},
		}

		consumer, err := NewConsumer(ConsumerConfig{
			Context:       context.Background(),
			Logger:        logger.NewTestLogger(),
			Driver:        mockDriver,
			URL:           natsurl,
			MaxAckPending: max,
		})

		assert.NoError(t, err)

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		for i := 0; i < max; i++ {
			_, err = js.Publish(context.Background(), fmt.Sprintf("dbchange.order.INSERT.CID.%d.PUBLIC.1", i+1), []byte(util.JSONStringify(sendEvent)))
			assert.NoError(t, err)
		}

		time.Sleep(time.Millisecond * 100)

		assert.Len(t, testEvents, max)
		assert.Equal(t, 1, flushed)

		assert.NoError(t, consumer.Stop())
	})
}

func TestHeartbeats(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {

		internal.MetricsReset() // force a collection to reset

		mockDriver := &mockDriver{}

		var received []*nats.Msg
		var subscription *nats.Subscription

		consumer, err := NewConsumer(ConsumerConfig{
			Context:           context.Background(),
			Logger:            logger.NewTestLogger(),
			Driver:            mockDriver,
			URL:               natsurl,
			HeartbeatInterval: time.Second,
			sessionIDCallback: func(id string) {
				subscription, _ = nc.Subscribe("eds.client."+id+".heartbeat", func(msg *nats.Msg) {
					received = append(received, msg)
				})
			},
		})

		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 150)

		assert.Len(t, received, 1)

		msg := received[0]

		assert.Equal(t, "msgpack", msg.Header.Get("content-encoding"))

		var payload heartbeat
		assert.NoError(t, msgpack.NewDecoder(bytes.NewReader(msg.Data)).UseJSONTag(true).Decode(&payload))

		assert.NotEmpty(t, payload.SessionId)
		assert.Equal(t, consumer.sessionID, payload.SessionId)
		assert.Equal(t, int64(0), payload.Offset)
		assert.Equal(t, time.Duration(0), payload.Uptime)
		assert.Equal(t, float64(0), payload.Stats.Metrics.TotalEvents)
		assert.Equal(t, float64(0), payload.Stats.Metrics.FlushCount)
		assert.Equal(t, float64(0), payload.Stats.Metrics.FlushDuration)
		assert.Equal(t, float64(0), payload.Stats.Metrics.ProcessingDuration)
		assert.Equal(t, float64(0), payload.Stats.Metrics.PendingEvents)

		internal.MetricsReset() // force a collection to reset

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.1.PUBLIC.1", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Second + time.Millisecond*100)

		assert.Len(t, received, 2)

		subscription.Drain()
		subscription.Unsubscribe()

		assert.NoError(t, consumer.Stop())

		msg = received[1]
		var payload2 heartbeat
		assert.Equal(t, "msgpack", msg.Header.Get("content-encoding"))
		assert.NoError(t, msgpack.NewDecoder(bytes.NewReader(msg.Data)).UseJSONTag(true).Decode(&payload2))
		assert.NotEmpty(t, payload2.SessionId)
		assert.Equal(t, consumer.sessionID, payload2.SessionId)
		assert.Equal(t, int64(1), payload2.Offset)
		assert.Greater(t, payload2.Uptime, time.Duration(0))
		assert.Equal(t, float64(1), payload2.Stats.Metrics.TotalEvents)
		assert.Equal(t, float64(0), payload2.Stats.Metrics.FlushCount)
		assert.Equal(t, float64(0), payload2.Stats.Metrics.FlushDuration)
		assert.Equal(t, float64(0), payload2.Stats.Metrics.ProcessingDuration)
		assert.Equal(t, float64(0), payload2.Stats.Metrics.PendingEvents)
	})
}

func TestMultipleMessagesWithMultipleBatches(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {

		var testEvents []internal.DBChangeEvent
		var flushed int

		max := 100

		mockDriver := &mockDriver{
			maxBatchSize: 10,
			process: func(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
				testEvents = append(testEvents, event)
				return false, nil
			},
			flush: func(logger logger.Logger) error {
				flushed++
				return nil
			},
		}

		consumer, err := NewConsumer(ConsumerConfig{
			Context:       context.Background(),
			Logger:        logger.NewTestLogger(),
			Driver:        mockDriver,
			URL:           natsurl,
			MaxAckPending: max,
		})

		assert.NoError(t, err)

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		for i := 0; i < max; i++ {
			_, err = js.Publish(context.Background(), fmt.Sprintf("dbchange.order.INSERT.CID.%d.PUBLIC.1", i+1), []byte(util.JSONStringify(sendEvent)))
			assert.NoError(t, err)
		}

		time.Sleep(time.Millisecond * 100)

		assert.Len(t, testEvents, max)
		assert.Equal(t, 10, flushed)

		assert.NoError(t, consumer.Stop())
	})
}

func TestMultipleMessagesWithIdleDelayFlush(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {

		var testEvents []internal.DBChangeEvent
		var flushed int

		max := 6

		mockDriver := &mockDriver{
			maxBatchSize: -1,
			process: func(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
				testEvents = append(testEvents, event)
				return false, nil
			},
			flush: func(logger logger.Logger) error {
				flushed++
				return nil
			},
		}

		consumer, err := NewConsumer(ConsumerConfig{
			Context:           context.Background(),
			Logger:            logger.NewTestLogger(),
			Driver:            mockDriver,
			URL:               natsurl,
			MaxAckPending:     max,
			MinPendingLatency: time.Millisecond * 500,
			MaxPendingLatency: time.Millisecond * 500,
		})

		assert.NoError(t, err)

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		for i := 0; i < max; i++ {
			_, err = js.Publish(context.Background(), fmt.Sprintf("dbchange.order.INSERT.CID.%d.PUBLIC.1", i+1), []byte(util.JSONStringify(sendEvent)))
			assert.NoError(t, err)
			if i%2 == 0 {
				time.Sleep(time.Millisecond * 600)
			}
		}

		time.Sleep(time.Millisecond * 500)

		assert.Len(t, testEvents, max)
		assert.Equal(t, 3, flushed)

		assert.NoError(t, consumer.Stop())
	})
}

func TestPause(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {
		var testEvent *internal.DBChangeEvent
		var flushed bool

		mockDriver := &mockDriver{
			process: func(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
				testEvent = &event
				return false, nil
			},
			flush: func(logger logger.Logger) error {
				flushed = true
				return nil
			},
		}

		consumer, err := NewConsumer(ConsumerConfig{
			Context: context.Background(),
			Logger:  logger.NewTestLogger(),
			Driver:  mockDriver,
			URL:     natsurl,
		})

		assert.NoError(t, err)

		consumer.Pause()

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.LID.PUBLIC.1", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assert.Nil(t, testEvent)
		assert.False(t, flushed)

		consumer.Unpause()

		time.Sleep(time.Millisecond * 100)

		assert.NoError(t, consumer.Stop())
		assert.NotNil(t, testEvent)
		assert.True(t, flushed)

		assert.Equal(t, sendEvent.Table, testEvent.Table)
		assert.Equal(t, sendEvent.Operation, testEvent.Operation)
		assert.Equal(t, sendEvent.Timestamp, testEvent.Timestamp)
		assert.Equal(t, sendEvent.MVCCTimestamp, testEvent.MVCCTimestamp)
	})
}

func TestTableSkipOldEvents(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {
		var testEvent *internal.DBChangeEvent
		var flushed bool

		mockDriver := &mockDriver{
			process: func(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
				testEvent = &event
				return false, nil
			},
			flush: func(logger logger.Logger) error {
				flushed = true
				return nil
			},
		}

		oldtv := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		tv := time.Now()

		consumer, err := NewConsumer(ConsumerConfig{
			Context:               context.Background(),
			Logger:                logger.NewTestLogger(),
			Driver:                mockDriver,
			URL:                   natsurl,
			ExportTableTimestamps: map[string]*time.Time{"order": &tv},
		})

		assert.NoError(t, err)

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = oldtv.UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", oldtv.UnixNano())

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.LID.PUBLIC.1", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assert.Nil(t, testEvent)
		assert.False(t, flushed)

		time.Sleep(time.Millisecond * 100)
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.LID.PUBLIC.2", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assert.NotNil(t, testEvent)

		testEvent = nil

		// change to validate that a new table not in the table map will still process
		sendEvent.Table = "user"
		sendEvent.Timestamp = oldtv.UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", oldtv.UnixNano())

		_, err = js.Publish(context.Background(), "dbchange.user.INSERT.CID.LID.PUBLIC.2", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assert.NotNil(t, testEvent)

		assert.NoError(t, consumer.Stop())
	})
}

type mockValidator struct {
	validator func(event internal.DBChangeEvent) (bool, bool, string, error)
}

func (v *mockValidator) Validate(event internal.DBChangeEvent) (bool, bool, string, error) {
	if v.validator != nil {
		return v.validator(event)
	}
	return false, false, "", nil
}

func TestTableSchemaValidator(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, js jetstream.JetStream) {
		var testEvent *internal.DBChangeEvent

		mockDriver := &mockDriver{
			process: func(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
				testEvent = &event
				return false, nil
			},
		}

		var valid bool

		mockValidator := &mockValidator{
			validator: func(event internal.DBChangeEvent) (bool, bool, string, error) {
				return true, valid, "", nil
			},
		}

		consumer, err := NewConsumer(ConsumerConfig{
			Context:         context.Background(),
			Logger:          logger.NewTestLogger(),
			Driver:          mockDriver,
			URL:             natsurl,
			SchemaValidator: mockValidator,
		})

		assert.NoError(t, err)

		var sendEvent internal.DBChangeEvent
		sendEvent.Table = "order"
		sendEvent.Operation = "INSERT"
		sendEvent.Timestamp = time.Now().UnixMilli()
		sendEvent.MVCCTimestamp = fmt.Sprintf("%v", time.Now().UnixNano())

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.LID.PUBLIC.1", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assert.Nil(t, testEvent)

		valid = true

		_, err = js.Publish(context.Background(), "dbchange.order.INSERT.CID.LID.PUBLIC.2", []byte(util.JSONStringify(sendEvent)))
		assert.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assert.NotNil(t, testEvent)

		assert.NoError(t, consumer.Stop())
	})
}
