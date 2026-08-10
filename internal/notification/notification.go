package notification

import (
	"context"
	"fmt"
	"sync"
	"time"

	tv1 "eds-v4-prototype/pkg/ads/v1"

	"github.com/shopmonkeyus/eds/internal/util"
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

	// Configure action is called to configure the server with a driver.
	Configure func(config *tv1.Configure) *tv1.ConfigureReply

	// Import action is called to import data using the driver.
	Import func() *tv1.ImportReply

	// DriverConfig action is called to get the driver configurations.
	DriverConfig func() *tv1.DriverConfigReply

	// Validate action is called to validate the driver configurations.
	Validate func(driver string, values map[string]any) *tv1.ValidateReply
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
	client         tv1.AdsServiceClient
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
	c.client = tv1.NewAdsServiceClient(conn)
	stream, err := c.client.Control(c.ctx)
	if err != nil {
		return fmt.Errorf("error opening control stream: %w", err)
	}
	if err := stream.Send(&tv1.ControlReply{
		EdsId:     edsID,
		SessionId: sessionID,
		Reply:     &tv1.ControlReply_StreamAttach{StreamAttach: &tv1.StreamAttach{}},
	}); err != nil {
		return fmt.Errorf("error sending control request: %w", err)
	}
	c.logger.Debug("control stream attached")

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
	stream tv1.AdsService_ControlClient,
	controlCallback func(*tv1.ControlCommand) *tv1.ControlReply,
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

		controlReply := controlCallback(resp)
		if controlReply != nil {
			if err := stream.Send(controlReply); err != nil {
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

func (c *NotificationConsumer) publishSimpleStatus(action tv1.StatusAction, errMsg string) {
	_, err := c.client.SendStatus(c.ctx, &tv1.SendStatusRequest{
		EdsId:     c.edsID,
		SessionId: c.sessionID,
		Action:    action,
		Success:   errMsg == "",
		Message:   errMsg,
	})
	if err != nil {
		c.logger.Error("failed to send %s status: %s", action, err)
	}
}

func (c *NotificationConsumer) importaction(_ *tv1.Import) *tv1.ImportReply {

	// This is only here for testing. We removed the backfill flag so in order to avoid the full import the following code was commented out.

	// c.publishSimpleStatus(tv1.StatusAction_STATUS_ACTION_IMPORT, "")
	// c.wg.Add(1)
	// go func() {
	// 	defer c.wg.Done()
	// 	c.handler.Import()
	// }()
	return &tv1.ImportReply{Success: true}
}

func (c *NotificationConsumer) driverconfig() *tv1.DriverConfigReply {
	return c.handler.DriverConfig()
}

func (c *NotificationConsumer) validate(driver string, vals map[string]any) *tv1.ValidateReply {
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

func (c *NotificationConsumer) callback(command *tv1.ControlCommand) *tv1.ControlReply {
	c.wg.Add(1)
	defer c.wg.Done()
	action := command.GetCommand()
	c.logger.Trace("received message: %s", util.JSONStringify(command))

	commandReply := &tv1.ControlReply{EdsId: c.edsID, SessionId: c.sessionID}
	switch action.(type) {
	case *tv1.ControlCommand_Restart:
		c.publishSimpleStatus(tv1.StatusAction_STATUS_ACTION_RESTART, "")
		c.handler.Restart()
		commandReply.Reply = &tv1.ControlReply_Restart{}
	case *tv1.ControlCommand_Shutdown:
		commandReply.Reply = &tv1.ControlReply_Shutdown{}
		c.handler.Shutdown(command.GetShutdown().GetMessage(), command.GetShutdown().GetDeleted())
	case *tv1.ControlCommand_Pause:
		err := c.handler.Pause()
		var message string
		if err != nil {
			message = err.Error()
		}
		commandReply.Reply = &tv1.ControlReply_Pause{Pause: &tv1.PauseReply{
			Success: err == nil,
			Message: message,
		}}
	case *tv1.ControlCommand_Unpause:
		err := c.handler.Unpause()
		var message string
		if err != nil {
			message = err.Error()
		}
		commandReply.Reply = &tv1.ControlReply_Unpause{Unpause: &tv1.UnpauseReply{
			Success: err == nil,
			Message: message,
		}}
	case *tv1.ControlCommand_Configure:
		commandReply.Reply = &tv1.ControlReply_Configure{
			Configure: c.handler.Configure(command.GetConfigure())}
	case *tv1.ControlCommand_Import:
		commandReply.Reply = &tv1.ControlReply_Import{Import: c.importaction(command.GetImport())}
	case *tv1.ControlCommand_DriverConfig:
		commandReply.Reply = &tv1.ControlReply_DriverConfig{DriverConfig: c.driverconfig()}
	case *tv1.ControlCommand_Validate:
		validateCommand := command.GetValidate()
		config := validateCommand.Config.AsMap()
		if validateCommand.Driver == "" {
			err := fmt.Errorf("invalid validate notification. missing driver for: %s", validateCommand.Driver)
			c.logger.Error(err.Error())
			commandReply.Reply = &tv1.ControlReply_Validate{}
		}
		if v, ok := config["config"].(map[string]any); ok {
			config = v
		} else {
			err := fmt.Errorf("invalid validate notification. missing config for: %s", util.JSONStringify(config))
			c.logger.Error(err.Error())
			commandReply.Reply = &tv1.ControlReply_Validate{}
		}
		commandReply.Reply = &tv1.ControlReply_Validate{Validate: c.validate(validateCommand.Driver, config)}
	default:
		c.logger.Warn("unknown action: %s", action)
	}
	return commandReply
}
