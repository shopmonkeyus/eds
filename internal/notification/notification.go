package notification

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/consumer"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/vmihailenco/msgpack/v5"
)

// NotificationHandler is an interface that is used to handle notification callbacks.
type NotificationHandler struct {

	// Restart action is called to restart the child process.
	Restart func()

	// Renew action is called to renew session credentials.
	Renew func()

	// Shutdown action is called to shutdown the server.
	Shutdown func(message string)

	// Pause action is called to pause the driver from processing.
	Pause func() error

	// Unpause action is called to unpause the driver from processing.
	Unpause func() error

	// Upgrade action is called to upgrade the server version.
	Upgrade func(version string) UpgradeResponse

	// SendLogs action is called to send logs to the server, should return the storage path.
	SendLogs func() *SendLogsResponse

	// Configure action is called to configure the server with a driver.
	Configure func(config *ConfigureRequest) *ConfigureResponse

	// Import action is called to import data using the driver.
	Import func(*ImportRequest) *ImportResponse

	// DriverConfig action is called to get the driver configurations.
	DriverConfig func() *DriverConfigResponse

	// Validate action is called to validate the driver configurations.
	Validate func(driver string, values map[string]any) *ValidateResponse
}

type SendLogsResponse struct {
	Path      string `json:"path" msgpack:"path"`
	SessionID string `json:"sessionId" msgpack:"sessionId"`
}

type ImportRequest struct {
	Backfill bool `json:"backfill" msgpack:"backfill"`
}

type ImportResponse struct {
	Success   bool    `json:"success" msgpack:"success"`
	Message   *string `json:"message,omitempty" msgpack:"message,omitempty"`
	SessionID string  `json:"sessionId" msgpack:"sessionId"`
	LogPath   *string `json:"-" msgpack:"-"`
}

type genericResponse struct {
	Success   bool    `json:"success" msgpack:"success"`
	Message   *string `json:"message,omitempty" msgpack:"message,omitempty"`
	SessionID string  `json:"sessionId" msgpack:"sessionId"`
	Action    string  `json:"action" msgpack:"action"`
}

type UpgradeResponse struct {
	Success   bool    `json:"success" msgpack:"success"`
	Message   string  `json:"message,omitempty" msgpack:"message,omitempty"`
	SessionID string  `json:"sessionId" msgpack:"sessionId"`
	LogPath   *string `json:"-" msgpack:"-"`
	Version   string  `json:"version" msgpack:"version"`
}

type ConfigureRequest struct {
	URL string `json:"url" msgpack:"url"`
	// Backfill is a flag to indicate if the driver should backfill data.
	// Configure does not perform a backfill, it is returned in the response so the next action can perform the backfill.
	Backfill bool `json:"backfill" msgpack:"backfill"`
}

type ConfigureResponse struct {
	Success   bool    `json:"success" msgpack:"success"`
	Message   *string `json:"message,omitempty" msgpack:"message,omitempty"`
	MaskedURL *string `json:"maskedURL,omitempty" msgpack:"maskedURL,omitempty"`
	SessionID string  `json:"sessionId" msgpack:"sessionId"`
	Backfill  bool    `json:"backfill" msgpack:"backfill"`
	LogPath   *string `json:"-" msgpack:"-"`
}

type DriverConfigResponse struct {
	Drivers   map[string]internal.DriverConfigurator `json:"drivers" msgpack:"drivers"`
	SessionID string                                 `json:"sessionId" msgpack:"sessionId"`
}

type ValidateResponse struct {
	Success     bool                  `json:"success" msgpack:"success"`
	Message     string                `json:"messsage,omitempty" msgpack:"message,omitempty"`
	FieldErrors []internal.FieldError `json:"field_errors,omitempty" msgpack:"field_errors,omitempty"`
	SessionID   string                `json:"sessionId" msgpack:"sessionId"`
	URL         string                `json:"url,omitempty" msgpack:"url,omitempty"`
}

type Notification struct {
	Action string         `json:"action" msgpack:"action"`
	Data   map[string]any `json:"data,omitempty" msgpack:"data,omitempty"`
}

func (n *Notification) String() string {
	return util.JSONStringify(n)
}

type NotificationConsumer struct {
	nc        *nats.Conn
	sub       *nats.Subscription
	logger    logger.Logger
	natsurl   string
	handler   NotificationHandler
	wg        sync.WaitGroup
	sessionID string
}

// New will create a new NotificationConsumer.
func New(logger logger.Logger, natsurl string, handler NotificationHandler) *NotificationConsumer {
	return &NotificationConsumer{
		logger:  logger.WithPrefix("[notification]"),
		natsurl: natsurl,
		handler: handler,
	}
}

// Start will start the consumer.
func (c *NotificationConsumer) Start(sessionId string, credsFile string) error {
	var err error
	c.nc, _, err = consumer.NewNatsConnection(c.logger, c.natsurl, credsFile)
	if err != nil {
		return fmt.Errorf("failed to create nats connection: %w", err)
	}
	subject := fmt.Sprintf("eds.notify.%s.>", sessionId)
	c.sub, err = c.nc.Subscribe(subject, c.callback)
	if err != nil {
		return fmt.Errorf("failed to subscribe to eds.notify: %w", err)
	}
	c.logger.Debug("subscribed to: %s", subject)
	c.sessionID = sessionId
	return nil
}

// Stop will stop the consumer.
func (c *NotificationConsumer) Stop() {
	if c.sub != nil {
		if err := c.sub.Unsubscribe(); err != nil {
			c.logger.Error("failed to unsubscribe from nats: %s", err)
		}
		c.sub = nil
	}
	if c.nc != nil {
		c.nc.Close()
		c.nc = nil
	}
	c.wg.Wait()
}

// Restart will stop the consumer and start it again.
func (c *NotificationConsumer) Restart(sessionId string, credsFile string) error {
	c.Stop()
	return c.Start(sessionId, credsFile)
}

func (c *NotificationConsumer) publishResponse(sessionId string, action string, v any) error {
	return c.publish(sessionId, action, "response", v)
}

func (c *NotificationConsumer) publishStatus(sessionId string, action string, v any) error {
	return c.publish(sessionId, action, "status", v)
}

func (c *NotificationConsumer) publish(sessionId string, action string, actionMod string, v any) error {
	data, err := msgpack.Marshal(v)
	if err != nil {
		return fmt.Errorf("error marshaling response: %w", err)
	}
	msg := nats.NewMsg(fmt.Sprintf("eds.client.%s.%s-%s", sessionId, action, actionMod))
	msg.Data = data
	msg.Header.Add(nats.MsgIdHdr, uuid.NewString())
	msg.Header.Add("content-encoding", "msgpack")
	c.logger.Trace("sending response: %s", msg.Subject)
	if err := c.nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("error sending response: %w", err)
	}
	return nil
}

func (c *NotificationConsumer) publishSimpleStatus(action string, errMsg string) {
	if err := c.publishStatus(c.sessionID, action, genericResponse{
		Success:   errMsg == "",
		Message:   &errMsg,
		SessionID: c.sessionID,
		Action:    action,
	}); err != nil {
		c.logger.Error("failed to send %s status: %s", action, err)
	}
}

func (c *NotificationConsumer) PublishSendLogsResponse(response *SendLogsResponse) error {
	return c.publishResponse(response.SessionID, "sendlogs", response)
}

func (c *NotificationConsumer) CallSendLogs() {
	response := c.handler.SendLogs()
	if response == nil {
		c.logger.Warn("sendlogs handler returned nothing")
		return
	}
	if err := c.PublishSendLogsResponse(response); err != nil {
		c.logger.Error("failed to send sendlogs response: %s", err)
	}
}

func (c *NotificationConsumer) configure(config ConfigureRequest) {
	response := c.handler.Configure(&config)
	if err := c.publishResponse(response.SessionID, "configure", response); err != nil {
		c.logger.Error("failed to send configure response: %s", err)
	} else if response.LogPath != nil {
		if err := c.PublishSendLogsResponse(&SendLogsResponse{Path: *response.LogPath, SessionID: response.SessionID}); err != nil {
			c.logger.Error("failed to publish send logs response during configure: %s", err)
		}
	}
}

func (c *NotificationConsumer) upgrade(version string) {
	response := c.handler.Upgrade(version)
	if err := c.publishResponse(response.SessionID, "upgrade", response); err != nil {
		c.logger.Error("failed to send upgrade response: %s", err)
	} else if response.LogPath != nil {
		if err := c.PublishSendLogsResponse(&SendLogsResponse{Path: *response.LogPath, SessionID: response.SessionID}); err != nil {
			c.logger.Error("failed to publish send logs response during upgrade: %s", err)
		}
	}
}

func (c *NotificationConsumer) importaction(req *ImportRequest) {
	c.wg.Add(1)
	// NOTE: we're going to run this on a background goroutine so we can return the response immediately and allow
	// other commands (like restart) to be processed while the import is running since the import could take a long time.
	go func() {
		defer c.wg.Done()
		response := c.handler.Import(req)
		if err := c.publishResponse(response.SessionID, "import", response); err != nil {
			c.logger.Error("failed to send import response: %s", err)
		} else if response.LogPath != nil {
			if err := c.PublishSendLogsResponse(&SendLogsResponse{Path: *response.LogPath, SessionID: response.SessionID}); err != nil {
				c.logger.Error("failed to publish send logs response during import: %s", err)
			}
		}
	}()
}

func (c *NotificationConsumer) driverconfig(m *nats.Msg) {
	response := c.handler.DriverConfig()
	if err := m.Respond([]byte(util.JSONStringify(response))); err != nil {
		c.logger.Error("failed to send driverconfig response: %s", err)
	}
}

func (c *NotificationConsumer) validate(driver string, vals map[string]any, m *nats.Msg) {
	response := c.handler.Validate(driver, vals)
	if err := m.Respond([]byte(util.JSONStringify(response))); err != nil {
		c.logger.Error("failed to send validate response: %s", err)
	}
}

func getBool(val any) bool {
	if v, ok := val.(bool); ok {
		return v
	}
	if v, ok := val.(string); ok {
		return v == "true"
	}
	return false
}

func (c *NotificationConsumer) callback(m *nats.Msg) {
	c.wg.Add(1)
	defer c.wg.Done()
	var notification Notification
	if err := util.DecodeNatsMsg(m, &notification); err != nil {
		c.logger.Error("failed to decode notification message: %s", err)
		return
	}
	c.logger.Trace("received message: %s", notification.String())

	respondGenerically := func(err error) {
		var errmsg *string
		if err != nil {
			c.logger.Error("failed to %s: %s", notification.Action, err)
			e := err.Error()
			errmsg = &e
		}
		if err := c.publishResponse(c.sessionID, notification.Action, genericResponse{
			Success:   errmsg == nil,
			Message:   errmsg,
			SessionID: c.sessionID,
			Action:    notification.Action,
		}); err != nil {
			c.logger.Error("failed to send pause response: %s", err)
		}
	}

	switch notification.Action {
	case "restart":
		c.publishSimpleStatus("restart", "")
		c.handler.Restart()
		respondGenerically(nil)
	case "renew":
		c.handler.Renew()
	case "ping":
		if subject, ok := notification.Data["subject"].(string); ok {
			c.logger.Trace("received ping notification, replying to: %s", subject)
			if err := c.nc.Publish(subject, []byte("pong")); err != nil {
				c.logger.Error("error sending ping response: %s", err)
			}
		} else {
			c.logger.Warn("invalid ping notification. missing subject for: %s", notification.String())
		}
	case "shutdown":
		if message, ok := notification.Data["message"].(string); ok {
			c.handler.Shutdown(message)
		} else {
			c.logger.Warn("invalid shutdown notification. missing message for: %s", notification.String())
		}
	case "pause":
		respondGenerically(c.handler.Pause())
	case "unpause":
		respondGenerically(c.handler.Unpause())
	case "upgrade":
		var version string
		if v, ok := notification.Data["version"].(string); ok {
			version = v
		} else {
			msg := fmt.Sprintf("invalid upgrade notification. missing version for: %s", notification.String())
			c.logger.Warn(msg)
			c.publishSimpleStatus("upgrade", msg)
			return
		}
		c.publishSimpleStatus("upgrade", "")
		c.upgrade(version)
	case "sendlogs":
		c.CallSendLogs()
	case "configure":
		var req ConfigureRequest
		if v, ok := notification.Data["url"].(string); ok {
			req.URL = v
		}
		req.Backfill = getBool(notification.Data["backfill"])
		c.configure(req)
	case "import":
		var req ImportRequest
		req.Backfill = getBool(notification.Data["backfill"])
		c.publishSimpleStatus("import", "")
		c.importaction(&req)
	case "driverconfig":
		c.driverconfig(m)
	case "validate":
		var driver string
		var config map[string]any
		if v, ok := notification.Data["driver"].(string); ok {
			driver = v
		} else {
			c.logger.Error("invalid validate notification. missing driver for: %s", notification.String())
			return
		}
		if v, ok := notification.Data["config"].(map[string]any); ok {
			config = v
		} else {
			c.logger.Error("invalid validate notification. missing config for: %s", notification.String())
			return
		}
		c.validate(driver, config, m)
	default:
		c.logger.Warn("unknown action: %s", notification.Action)
	}
}
