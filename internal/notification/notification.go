package notification

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal/consumer"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
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
	Pause func()

	// Unpause action is called to unpause the driver from processing.
	Unpause func()

	// Upgrade action is called to upgrade the server version.
	Upgrade func(version string, url string)

	// SendLogs action is called to send logs to the server, should return the storage path.
	SendLogs func() *SendLogsResponse
}

type SendLogsResponse struct {
	Path      string `json:"path"`
	SessionId string `json:"sessionId"`
}

type Notification struct {
	Action string         `json:"action" msgpack:"action"`
	Data   map[string]any `json:"data,omitempty" msgpack:"data,omitempty"`
}

func (n *Notification) String() string {
	return util.JSONStringify(n)
}

type NotificationConsumer struct {
	nc      *nats.Conn
	sub     *nats.Subscription
	logger  logger.Logger
	natsurl string
	handler NotificationHandler
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
}

// Restart will stop the consumer and start it again.
func (c *NotificationConsumer) Restart(sessionId string, credsFile string) error {
	c.Stop()
	return c.Start(sessionId, credsFile)
}

func (c *NotificationConsumer) publishResponse(sessionId string, action string, data []byte) error {
	msg := nats.NewMsg(fmt.Sprintf("eds.client.%s.%s-response", sessionId, action))
	msg.Data = data
	msg.Header.Add(nats.MsgIdHdr, uuid.NewString())
	c.logger.Trace("sending response: %s", msg.Subject)
	if err := c.nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("error sending response: %w", err)
	}
	return nil
}

func (c *NotificationConsumer) CallSendLogs() {
	response := c.handler.SendLogs()
	if response == nil {
		c.logger.Warn("sendlogs handler returned nothing")
		return
	}
	if err := c.publishResponse(response.SessionId, "sendlogs", []byte(util.JSONStringify(response))); err != nil {
		c.logger.Error("failed to send sendlogs response: %s", err)
	}
}

func (c *NotificationConsumer) callback(m *nats.Msg) {
	var notification Notification
	if err := util.DecodeNatsMsg(m, &notification); err != nil {
		c.logger.Error("failed to decode notification message: %s", err)
		return
	}
	c.logger.Trace("received message: %s", notification.String())
	switch notification.Action {
	case "restart":
		c.handler.Restart()
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
		c.handler.Pause()
	case "unpause":
		c.handler.Unpause()
	case "upgrade":
		var url, version string
		if v, ok := notification.Data["url"].(string); ok {
			url = v
		} else {
			c.logger.Warn("invalid upgrade notification. missing url for: %s", notification.String())
			return
		}
		if v, ok := notification.Data["version"].(string); ok {
			version = v
		} else {
			c.logger.Warn("invalid upgrade notification. missing version for: %s", notification.String())
			return
		}
		c.handler.Upgrade(version, url)
	case "sendlogs":
		c.CallSendLogs()
	default:
		c.logger.Warn("unknown action: %s", notification.Action)
	}
}
