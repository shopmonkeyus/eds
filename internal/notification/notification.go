package notification

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	transmissionv1 "github.com/shopmonkeyus/eds/pkg/transmission/v1"
	"github.com/shopmonkeyus/go-common/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// NotificationHandler is an interface that is used to handle notification callbacks.
type NotificationHandler struct {

	// Restart action is called to restart the child process.
	Restart func()

	// Shutdown action is called to shutdown the server.
	Shutdown func(message string, deleted bool)

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

	// BackfillInit is called to initialize backfill and create an export job.
	BackfillInit func(*InitBackfillRequest) *InitBackfillResponse

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
	Backfill bool   `json:"backfill" msgpack:"backfill"`
	JobID    string `json:"jobId" msgpack:"jobId"`
}

type InitBackfillRequest struct {
	Backfill bool `json:"backfill" msgpack:"backfill"`
}

type InitBackfillResponse struct {
	Success   bool    `json:"success" msgpack:"success"`
	Message   *string `json:"message,omitempty" msgpack:"message,omitempty"`
	SessionID string  `json:"sessionId" msgpack:"sessionId"`
	JobID     string  `json:"jobId" msgpack:"jobId"`
}

type ImportResponse struct {
	Success   bool    `json:"success" msgpack:"success"`
	Message   *string `json:"message,omitempty" msgpack:"message,omitempty"`
	SessionID string  `json:"sessionId" msgpack:"sessionId"`
	LogPath   *string `json:"-" msgpack:"-"`
	JobID     string  `json:"jobId" msgpack:"jobId"`
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
	ctx            context.Context
	cancel         context.CancelFunc
	gRPCAddress    string
	gRPCConnection *grpc.ClientConn
	logger         logger.Logger
	handler        NotificationHandler
	wg             sync.WaitGroup
	edsID          string
	sessionID      string
	client         transmissionv1.TransmissionServiceClient
}

// New will create a new NotificationConsumer.
func New(logger logger.Logger, gRPCAddress string, handler NotificationHandler) *NotificationConsumer {
	return &NotificationConsumer{
		logger:      logger.WithPrefix("[notification]"),
		gRPCAddress: gRPCAddress,
		handler:     handler,
	}
}

// Start will start the consumer.
func (c *NotificationConsumer) Start(ctx context.Context, conn *grpc.ClientConn, sessionID string, edsID string) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.sessionID = sessionID
	c.edsID = edsID
	c.client = transmissionv1.NewTransmissionServiceClient(conn)
	stream, err := c.client.Control(c.ctx)
	if err != nil {
		return fmt.Errorf("error opening control stream: %w", err)
	}
	if err := stream.Send(&transmissionv1.ControlRequest{EdsId: edsID, SessionId: sessionID}); err != nil {
		return fmt.Errorf("error sending control request: %w", err)
	}
	c.logger.Debug("control stream attached")

	c.client.Log(c.ctx, &transmissionv1.LogRequest{
		EdsId: edsID,
		Json:  []byte(util.JSONStringify(map[string]any{"message": "test", "severity": "info"})),
	})
	if err != nil {
		return fmt.Errorf("error sending log: %w", err)
	}
	c.logger.Debug("log sent")

	c.wg.Add(1)
	go c.runControl(stream, c.callback)

	return nil
}

// Stop will stop the consumer.
func (c *NotificationConsumer) Stop() {
	c.logger.Debug("stopping notification handler")
	c.cancel()
	c.wg.Wait()
	c.logger.Debug("notification handler stopped")
}

func (c *NotificationConsumer) runControl(
	stream transmissionv1.TransmissionService_ControlClient,
	controlCallback func(*transmissionv1.ControlResponse) (string, *transmissionv1.ControlRequest),
) {
	defer c.wg.Done()

	for {
		if c.ctx.Err() != nil {
			return
		}
		resp, err := stream.Recv()
		if err != nil {
			if status.Code(err) == codes.Canceled {
				return
			}
			c.logger.Error("error receiving command: %s", err)
			// TODO: figure out how/where we handle the reconnect behavior
			time.Sleep(5 * time.Second)
			continue
		}

		_, commandReply := controlCallback(resp)
		if commandReply != nil {
			if err := stream.Send(commandReply); err != nil {
				c.logger.Error("error sending command reply: %s", err)
			}
		}
	}
}

func goStructToStructpb(v any) (*structpb.Struct, error) {
	var payload structpb.Struct
	err := protojson.Unmarshal([]byte(util.JSONStringify(v)), &payload)
	if err != nil {
		return nil, fmt.Errorf("error converting struct to protobuf: %w", err)
	}

	return &payload, nil
}

func (c *NotificationConsumer) publishResponse(action string, v any) *transmissionv1.ControlRequest {
	payload, err := goStructToStructpb(v)
	if err != nil {
		c.logger.Error("error converting response to protobuf: %s", err)
		return nil
	}
	return &transmissionv1.ControlRequest{
		SessionId: c.sessionID,
		Payload:   payload,
	}
}

func (c *NotificationConsumer) publishSimpleStatus(action string, errMsg string) {
	r := genericResponse{
		Success:   errMsg == "",
		Message:   &errMsg,
		SessionID: c.sessionID,
		Action:    action,
	}
	payload, err := goStructToStructpb(r)
	if err != nil {
		c.logger.Error("error converting generic response to protobuf: %s", err)
		return
	}
	_, err = c.client.Status(c.ctx, &transmissionv1.StatusRequest{
		SessionId: c.sessionID,
		Action:    action,
		Payload:   payload,
	})
	if err != nil {
		c.logger.Error("failed to send %s status: %s", action, err)
	}
}

// Need to notify Depot to ingest logs into the monitoring CH database whenever logs
// are uploaded via the backend API. The code for this is very messy and logs are uploaded
// in lots of different places, e.g. on an import failure or shutdown. We are wantint
// to remove the Depot ingestion of logs and log over Transmission (working name for
// the EDSv4 backend servie) instead
func (c *NotificationConsumer) PublishSendLogsResponse(logPath string) error {
	if _, err := c.client.SendLogs(c.ctx, &transmissionv1.SendLogsRequest{
		EdsId:     c.edsID,
		SessionId: c.sessionID,
		Path:      logPath,
	}); err != nil {
		return fmt.Errorf("failed to send import logs: %w", err)
	}
	return nil
}

func (c *NotificationConsumer) CallSendLogs() {
	response := c.handler.SendLogs()
	if response == nil {
		c.logger.Warn("sendlogs handler returned nothing")
		return
	}
	if err := c.PublishSendLogsResponse(response.Path); err != nil {
		c.logger.Error("failed to send sendlogs response: %s", err)
	}
}

func (c *NotificationConsumer) configure(config ConfigureRequest) *ConfigureResponse {
	response := c.handler.Configure(&config)
	if response.LogPath != nil {
		if err := c.PublishSendLogsResponse(*response.LogPath); err != nil {
			c.logger.Error("failed to publish send logs response during configure: %s", err)
		}
	}
	return response
}

func (c *NotificationConsumer) upgrade(version string) UpgradeResponse {
	response := c.handler.Upgrade(version)
	if response.LogPath != nil {
		if err := c.PublishSendLogsResponse(*response.LogPath); err != nil {
			c.logger.Error("failed to publish send logs response during upgrade: %s", err)
		}
	}
	return response
}

// This is an odd one because it needs to return whether the backfill initialized successfully
// The result of the import is returned much later in a separate message
func (c *NotificationConsumer) importaction(req *ImportRequest) *InitBackfillResponse {
	initResponse := c.handler.BackfillInit(&InitBackfillRequest{Backfill: req.Backfill})

	if !initResponse.Success {
		return initResponse
	}
	req.JobID = initResponse.JobID
	c.publishSimpleStatus("import", "")

	c.wg.Add(1)
	// NOTE: we're going to run this on a background goroutine so we can return the response immediately and allow
	// other commands (like restart) to be processed while the import is running since the import could take a long time.
	go func() {
		defer c.wg.Done()
		response := c.handler.Import(req)
		var message string
		if response.Message != nil {
			message = *response.Message
		}
		if _, err := c.client.ImportResult(c.ctx, &transmissionv1.ImportResultRequest{
			SessionId: c.sessionID,
			Success:   response.Success,
			Message:   message,
			JobId:     response.JobID,
		}); err != nil {
			c.logger.Error("failed to send import result: %s", err)
			return
		} else if response.LogPath != nil {
			c.PublishSendLogsResponse(*response.LogPath)
		}
	}()
	return initResponse
}

func (c *NotificationConsumer) driverconfig() *DriverConfigResponse {
	return c.handler.DriverConfig()
}

func (c *NotificationConsumer) validate(driver string, vals map[string]any) *ValidateResponse {
	return c.handler.Validate(driver, vals)
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

func (c *NotificationConsumer) callback(command *transmissionv1.ControlResponse) (string, *transmissionv1.ControlRequest) {
	c.wg.Add(1)
	defer c.wg.Done()
	action := command.Action
	data := command.Payload.AsMap()
	c.logger.Trace("received message: %s", util.JSONStringify(command))

	respondGenerically := func(err error) *transmissionv1.ControlRequest {
		var errmsg *string
		if err != nil {
			c.logger.Error("failed to %s: %s", action, err)
			e := err.Error()
			errmsg = &e
		}
		payload, err := goStructToStructpb(genericResponse{
			Success:   errmsg == nil,
			Message:   errmsg,
			SessionID: c.sessionID,
			Action:    action,
		})
		if err != nil {
			return nil
		}
		return &transmissionv1.ControlRequest{
			SessionId: c.sessionID,
			Payload:   payload,
		}
	}

	var commandReply *transmissionv1.ControlRequest
	switch action {
	case "restart":
		c.publishSimpleStatus("restart", "")
		c.handler.Restart()
		commandReply = respondGenerically(nil)
	case "ping":
		c.logger.Trace("received ping notification")
		commandReply = &transmissionv1.ControlRequest{
			SessionId: c.sessionID,
		}
	case "shutdown":
		deleted := getBool(data["deleted"])
		if message, ok := data["message"].(string); ok {
			c.handler.Shutdown(message, deleted)
			commandReply = &transmissionv1.ControlRequest{
				SessionId: c.sessionID,
			}
		} else {
			c.logger.Warn("invalid shutdown notification. missing message for: %s", util.JSONStringify(data))
		}
	case "pause":
		commandReply = respondGenerically(c.handler.Pause())
	case "unpause":
		commandReply = respondGenerically(c.handler.Unpause())
	case "upgrade":
		var version string
		if v, ok := data["version"].(string); ok {
			version = v
		} else {
			msg := fmt.Sprintf("invalid upgrade notification. missing version for: %s", util.JSONStringify(data))
			c.logger.Warn(msg)
			c.publishSimpleStatus("upgrade", msg)
		}
		c.publishSimpleStatus("upgrade", "")
		c.upgrade(version)
		commandReply = respondGenerically(nil)
	case "sendlogs":
		c.CallSendLogs()
		commandReply = respondGenerically(nil)
	case "configure":
		var req ConfigureRequest
		if v, ok := data["url"].(string); ok {
			req.URL = v
		}
		req.Backfill = getBool(data["backfill"])
		commandReply = c.publishResponse("configure", c.configure(req))
	case "import":
		var req ImportRequest
		req.Backfill = getBool(data["backfill"])
		commandReply = c.publishResponse("import", c.importaction(&req))

	case "driverconfig":
		commandReply = c.publishResponse("driverconfig", c.driverconfig())
	case "validate":
		var driver string
		var config map[string]any
		if v, ok := data["driver"].(string); ok {
			driver = v
		} else {
			err := fmt.Errorf("invalid validate notification. missing driver for: %s", util.JSONStringify(data))
			c.logger.Error(err.Error())
			commandReply = respondGenerically(err)
		}
		if v, ok := data["config"].(map[string]any); ok {
			config = v
		} else {
			err := fmt.Errorf("invalid validate notification. missing config for: %s", util.JSONStringify(data))
			c.logger.Error(err.Error())
			commandReply = respondGenerically(err)
		}
		commandReply = c.publishResponse("validate", c.validate(driver, config))
	default:
		c.logger.Warn("unknown action: %s", action)
	}
	return action, commandReply
}
