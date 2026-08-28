package notification

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/stretchr/testify/assert"

	_ "github.com/shopmonkeyus/eds/internal/drivers/eventhub" // this comment on this blank import is needed because Sonarqube doesn't understand Go modules
	_ "github.com/shopmonkeyus/eds/internal/drivers/file"
	_ "github.com/shopmonkeyus/eds/internal/drivers/kafka"
	_ "github.com/shopmonkeyus/eds/internal/drivers/mysql"
	_ "github.com/shopmonkeyus/eds/internal/drivers/postgresql"
	_ "github.com/shopmonkeyus/eds/internal/drivers/s3"
	_ "github.com/shopmonkeyus/eds/internal/drivers/snowflake"
	_ "github.com/shopmonkeyus/eds/internal/drivers/sqlserver"
)

func runNatsTestServer(fn func(natsurl string, nc *nats.Conn, srv *server.Server)) {
	port, err := util.GetFreePort()
	if err != nil {
		panic(err)
	}
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.Cluster.Name = "testing"
	srv := natsserver.RunServer(&opts)
	defer srv.Shutdown()
	url := fmt.Sprintf("nats://localhost:%d", port)
	nc, err := nats.Connect(url)
	if err != nil {
		panic(err)
	}
	defer nc.Close()
	fn(url, nc, srv)
}

func TestAllDriversReturnFieldErrorsOnEmptyConfig(t *testing.T) {
	runNatsTestServer(func(natsurl string, nc *nats.Conn, srv *server.Server) {
		sessionID := "test-session-all-drivers"

		handler := NotificationHandler{
			Validate: func(driver string, values map[string]any) *ValidateResponse {
				url, fieldErrs, err := internal.Validate(driver, values)
				var msg string
				if err != nil {
					msg = err.Error()
				}
				return &ValidateResponse{
					Success:     err == nil && url != "",
					SessionID:   sessionID,
					FieldErrors: fieldErrs,
					URL:         url,
					Message:     msg,
				}
			},
		}

		subject := fmt.Sprintf("eds.notify.%s.validate", sessionID)
		sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
			var notification Notification
			if err := util.DecodeNatsMsg(m, &notification); err != nil {
				t.Errorf("failed to decode notification: %s", err)
				return
			}

			var driver string
			var config map[string]any
			if v, ok := notification.Data["driver"].(string); ok {
				driver = v
			}
			if v, ok := notification.Data["config"].(map[string]any); ok {
				config = v
			}

			response := handler.Validate(driver, config)
			respData, _ := json.Marshal(response)
			m.Respond(respData)
		})
		assert.NoError(t, err)
		defer sub.Unsubscribe()

		// Get all registered drivers and test each one
		drivers := internal.GetDriverConfigurations()

		for driverName := range drivers {
			t.Run(driverName+"_empty_config", func(t *testing.T) {
				notification := Notification{
					Action: "validate",
					Data: map[string]any{
						"driver": driverName,
						"config": map[string]any{},
					},
				}

				resp, err := nc.Request(subject, []byte(util.JSONStringify(notification)), 5*time.Second)
				assert.NoError(t, err)

				var validateResp ValidateResponse
				err = json.Unmarshal(resp.Data, &validateResp)
				assert.NoError(t, err)

				assert.False(t, validateResp.Success, "%s: validation should fail with empty config", driverName)
				assert.GreaterOrEqual(t, len(validateResp.FieldErrors), 1, "%s: should return at least one field error", driverName)
				assert.Equal(t, "", validateResp.URL, "%s: URL should be empty on validation failure", driverName)
			})
		}
	})
}

func TestImportActionReturnsErrorOnInitFailure(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	srv := natsserver.RunServer(&opts)
	defer srv.Shutdown()

	nc, _ := nats.Connect(srv.ClientURL())
	defer nc.Close()

	errMsg := "init failed"
	consumer := &NotificationConsumer{
		nc:     nc,
		logger: logger.NewTestLogger(),
		handler: NotificationHandler{
			BackfillInit: func(*InitBackfillRequest) *InitBackfillResponse {
				return &InitBackfillResponse{Success: false, Message: errMsg}
			},
		},
	}

	nc.Subscribe("test.import", func(msg *nats.Msg) {
		consumer.importaction(&ImportRequest{Backfill: true}, msg)
	})

	reply, err := nc.Request("test.import", nil, time.Second)
	assert.NoError(t, err)

	var resp InitBackfillResponse
	json.Unmarshal(reply.Data, &resp)
	assert.False(t, resp.Success)
	assert.Equal(t, "init failed", resp.Message)
}

func TestImportActionReturnsSuccessOnInitSuccess(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	srv := natsserver.RunServer(&opts)
	defer srv.Shutdown()

	nc, _ := nats.Connect(srv.ClientURL())
	defer nc.Close()

	consumer := &NotificationConsumer{
		nc:     nc,
		logger: logger.NewTestLogger(),
		handler: NotificationHandler{
			BackfillInit: func(*InitBackfillRequest) *InitBackfillResponse {
				return &InitBackfillResponse{Success: true, JobID: "job-123", SessionID: "session-456"}
			},
			Import: func(*ImportRequest) *ImportResponse {
				return &ImportResponse{Success: true, SessionID: "session-456"}
			},
		},
	}

	nc.Subscribe("test.import", func(msg *nats.Msg) {
		consumer.importaction(&ImportRequest{Backfill: true}, msg)
	})

	reply, err := nc.Request("test.import", nil, time.Second)
	assert.NoError(t, err)

	var resp InitBackfillResponse
	json.Unmarshal(reply.Data, &resp)
	assert.True(t, resp.Success)
	assert.Equal(t, "job-123", resp.JobID)
}
