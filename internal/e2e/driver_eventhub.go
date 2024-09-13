//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/drivers/eventhub"
	"github.com/shopmonkeyus/go-common/logger"
)

// NOTE: right now this is disabled for validation because it requires a premium azure subscription
// to read from the eventhub.

var (
	eventHubEntityPath          = "shopmonkey-eds-test"
	eventHubServiceBusNamespace = eventHubEntityPath + ".servicebus.windows.net"
	eventHubSharedAccessKeyName = "sendreceive"
	eventHubSharedAccessKey     = os.Getenv("SM_EVENTHUB_SHARED_ACCESS_KEY")
	eventHubEnabled             = false
)

type driverEventHubTest struct {
}

var _ e2eTest = (*driverEventHubTest)(nil)
var _ e2eTestDisabled = (*driverEventHubTest)(nil)

func (d *driverEventHubTest) Disabled() bool {
	return eventHubSharedAccessKey == ""
}

func (d *driverEventHubTest) Name() string {
	return "eventhub"
}

func (d *driverEventHubTest) URL(dir string) string {
	return fmt.Sprintf("eventhub://%s/;SharedAccessKeyName=%s;SharedAccessKey=%s;EntityPath=%s", eventHubServiceBusNamespace, eventHubSharedAccessKeyName, eventHubSharedAccessKey, eventHubEntityPath)
}

func (d *driverEventHubTest) TestInsert(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error {
	if !eventHubEnabled {
		return nil
	}
	connectionString, err := eventhub.ParseConnectionString(url)
	if err != nil {
		return fmt.Errorf("error parsing connection string: %w", err)
	}
	consumer, err := azeventhubs.NewConsumerClientFromConnectionString(connectionString, "", "", nil)
	if err != nil {
		return fmt.Errorf("error creating consumer client: %w", err)
	}
	defer consumer.Close(context.Background())
	partitionKey := eventhub.NewPartitionKey(event.Table, event.CompanyID, event.LocationID, event.GetPrimaryKey())
	client, err := consumer.NewPartitionClient(partitionKey, nil)
	if err != nil {
		return fmt.Errorf("error creating partition client: %w", err)
	}
	defer client.Close(context.Background())
	data, err := client.ReceiveEvents(context.Background(), 1, nil)
	if err != nil {
		return fmt.Errorf("error receiving events: %w", err)
	}
	var event2 internal.DBChangeEvent
	if err := json.Unmarshal(data[0].Body, &event2); err != nil {
		return fmt.Errorf("error unmarshalling event: %w", err)
	}
	return dbchangeEventMatches(event, event2)
}

func init() {
	registerTest(&driverEventHubTest{})
}
