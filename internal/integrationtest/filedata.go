package integrationtest

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/registry"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/shopmonkeyus/go-common/slice"
)

func getPublicTables(log logger.Logger) []string {
	registry, err := registry.NewAPIRegistry(context.Background(), log, "http://api.shopmonkey.cloud", "test", nil)
	if err != nil {
		panic(err)
	}
	schema, err := registry.GetLatestSchema()
	if err != nil {
		panic(err)
	}
	publicTables := []string{}
	for table := range schema {
		publicTables = append(publicTables, table)
	}
	return publicTables
}

// increase timestamp to today to keep working with recent EDS servers
func makeEventTimestampRecent(timestamp int64) int64 {
	daysInMillis := int64(86400000)
	originalDays := timestamp / daysInMillis
	todayDays := time.Now().UnixMilli() / daysInMillis
	daysDiff := todayDays - originalDays
	return timestamp + daysDiff*daysInMillis
}

type TestDataGenerator struct {
	scanner      *bufio.Scanner
	file         *os.File
	gzr          *gzip.Reader
	companyId    string //should match destination test shop
	locationId   string
	userId       string
	publicTables []string
}

func NewTestDataGenerator(filename string, publicTables []string) *TestDataGenerator {
	var scanner *bufio.Scanner
	var file *os.File
	var gzr *gzip.Reader

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	gzr, err = gzip.NewReader(file)
	if err != nil {
		file.Close()
		panic(err)
	}

	tr := tar.NewReader(gzr)
	_, err = tr.Next() // skip to first file
	if err != nil {
		gzr.Close()
		file.Close()
		panic(err)
	}
	_, _ = tr.Next() // skip the header

	scanner = bufio.NewScanner(tr)

	cid := "28a6712e-83a0-4ede-97cb-c3f5201068dc" //this is multi shop corp in local seed data
	lid := "5b7a05d6-c971-4f77-8792-9c12744a811d" //multi shop llc main street location
	uid := "test-user-789"
	return &TestDataGenerator{
		scanner:      scanner,
		file:         file,
		gzr:          gzr,
		companyId:    cid,
		locationId:   lid,
		userId:       uid,
		publicTables: publicTables,
	}
}

func (t *TestDataGenerator) NextEvent() *internal.DBChangeEvent {
	for {
		if !t.scanner.Scan() {
			return nil
		}
		var event internal.DBChangeEvent
		if err := json.Unmarshal(t.scanner.Bytes(), &event); err != nil {
			panic(err)
		}
		if !slice.Contains(t.publicTables, strings.ToLower(event.Table)) {
			continue
		}

		event.Timestamp = makeEventTimestampRecent(event.Timestamp)

		//replace with testing ids
		event.CompanyID = &t.companyId
		event.LocationID = &t.locationId
		event.UserID = &t.userId

		return &event
	}
}

func (t *TestDataGenerator) Close() error {
	var err error
	if t.gzr != nil {
		if gzErr := t.gzr.Close(); gzErr != nil {
			err = gzErr
		}
	}
	if t.file != nil {
		if fileErr := t.file.Close(); fileErr != nil && err == nil {
			err = fileErr
		}
	}
	return err
}

type natsMessage struct {
	Message []byte
	Subject string
	MsgID   string
}

func NatsMessageFromEvent(event *internal.DBChangeEvent) natsMessage {
	buf, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	subject := fmt.Sprintf("dbchange.customer.UPDATE.%s.1.PUBLIC.2", *event.CompanyID)
	msgID := util.Hash(event)
	return natsMessage{Message: buf, Subject: subject, MsgID: msgID}
}

func PublishTestData(path string, count int, js jetstream.JetStream, log logger.Logger) int {
	publicTables := getPublicTables(log)
	testDataGenerator := NewTestDataGenerator("../eds_test_data/output.jsonl.tar.gz", publicTables)
	defer testDataGenerator.Close()

	delivered := 0
	for {
		if delivered >= count {
			break
		}
		event := testDataGenerator.NextEvent()
		if event == nil {
			break
		}
		message := NatsMessageFromEvent(event)
		js.Publish(context.Background(), message.Subject, message.Message, jetstream.WithMsgID(message.MsgID))

		log.Info("sent message with id: %s", message.MsgID)

		delivered++
	}
	return delivered
}
