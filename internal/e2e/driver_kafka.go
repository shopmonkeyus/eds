//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	gokafka "github.com/segmentio/kafka-go"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/go-common/logger"
)

type driverKafkaTest struct {
}

var _ e2eTest = (*driverKafkaTest)(nil)

func (d *driverKafkaTest) Name() string {
	return "kafka"
}

func (d *driverKafkaTest) URL(dir string) string {
	return fmt.Sprintf("kafka://127.0.0.1:29092/%s", dbname)
}

func (d *driverKafkaTest) TestInsert(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error {
	reader := gokafka.NewReader(gokafka.ReaderConfig{
		Brokers:        []string{"127.0.0.1:29092"},
		Topic:          "eds",
		CommitInterval: 0,
		GroupID:        "eds",
	})
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	msg, err := reader.FetchMessage(ctx)
	if err != nil {
		logger.Fatal("error fetching message: %s", err)
	}
	if err := reader.CommitMessages(ctx, msg); err != nil {
		logger.Fatal("error committing message: %s", err)
	}
	var event2 internal.DBChangeEvent
	if err := json.Unmarshal(msg.Value, &event2); err != nil {
		logger.Fatal("error decoding event: %s", err)
	}
	return dbchangeEventMatches(event, event2)
}

func init() {
	registerTest(&driverKafkaTest{})
}
