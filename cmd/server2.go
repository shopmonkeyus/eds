package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	cnats "github.com/shopmonkeyus/go-common/nats"
	csys "github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
)

const (
	traceInsertQueries = false // if we should log insert sql
	traceInserts       = true  // if we should log inserts

	maxAckPending        = 25_000                 // this is currently our system max
	maxBatchSize         = 100_000                // maximum number of messages to batch insert at a time
	maxPendingBuffer     = 4_096                  // maximum number of messages to pull from nats to buffer
	minPendingLatency    = time.Second            // minimum accumulation period before flushing
	maxPendingLatency    = time.Second * 30       // maximum accumulation period before flushing
	emptyBufferPauseTime = time.Millisecond * 100 // time to wait when the buffer is empty to prevent CPU spinning
)

func getNatsCreds(creds string) (nats.Option, []string, string, error) {
	if _, err := os.Stat(creds); os.IsNotExist(err) {
		return nil, nil, "", fmt.Errorf("error: missing file: %s", creds)
	}
	buf, err := os.ReadFile(creds)
	if err != nil {
		return nil, nil, "", fmt.Errorf("error: reading credentials file: %w", err)

	}
	natsCredentials := nats.UserCredentials(creds)

	natsJWT, err := jwt.ParseDecoratedJWT(buf)
	if err != nil {
		return nil, nil, "", fmt.Errorf("error: parsing valid JWT: %s", err)

	}
	claim, err := jwt.DecodeUserClaims(natsJWT)
	if err != nil {
		return nil, nil, "", fmt.Errorf("error: decoding JWT claims: %s", err)

	}
	var companyIDs []string
	allowedSubs := claim.Sub.Allow
	for _, sub := range allowedSubs {
		companyID := util.ExtractCompanyIdFromSubscription(sub)
		if companyID != "" {
			companyIDs = append(companyIDs, companyID)
		}
	}
	if len(companyIDs) == 0 {
		return nil, nil, "", fmt.Errorf("error: issue parsing company ID from JWT claims. Ensure the JWT has the correct permissions.")
	}
	companyName := claim.Name
	if companyName == "" {
		companyName = "unknown"
	}

	return natsCredentials, companyIDs, companyName, nil

}

type DBChangeEvent struct {
	Operation    string          `json:"operation"`
	ID           string          `json:"id"`
	Table        string          `json:"table"`
	Key          []string        `json:"key"`
	ModelVersion string          `json:"modelVersion"`
	CompanyID    *string         `json:"companyId,omitempty"`
	LocationID   *string         `json:"locationId,omitempty"`
	Before       json.RawMessage `json:"before,omitempty"`
	After        json.RawMessage `json:"after,omitempty"`
	Diff         []string        `json:"diff,omitempty"`
}

func quoteString(val string) string {
	return "'" + strings.ReplaceAll(val, "'", "''") + "'"
}

func quoteValue(value any) string {
	var str string
	switch arg := value.(type) {
	case nil:
		str = "NULL"
	case int:
		str = strconv.FormatInt(int64(arg), 10)
	case int8:
		str = strconv.FormatInt(int64(arg), 10)
	case int16:
		str = strconv.FormatInt(int64(arg), 10)
	case int32:
		str = strconv.FormatInt(int64(arg), 10)
	case *int32:
		if arg == nil {
			str = "NULL"
		} else {
			str = strconv.FormatInt(int64(*arg), 10)
		}
	case int64:
		str = strconv.FormatInt(arg, 10)
	case *int64:
		if arg == nil {
			str = "NULL"
		} else {
			str = strconv.FormatInt(*arg, 10)
		}
	case float32:
		str = strconv.FormatFloat(float64(arg), 'f', -1, 32)
	case float64:
		str = strconv.FormatFloat(arg, 'f', -1, 64)
	case *float64:
		if arg == nil {
			str = "NULL"
		} else {
			str = strconv.FormatFloat(*arg, 'f', -1, 64)
		}
	case bool:
		str = strconv.FormatBool(arg)
	case *bool:
		if arg == nil {
			str = "NULL"
		} else {
			str = strconv.FormatBool(*arg)
		}
	case string:
		str = quoteString(arg)
	case *time.Time:
		if arg == nil {
			str = "NULL"
		} else {
			str = (*arg).Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
		}
	case time.Time:
		str = arg.Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
	case map[string]interface{}:
		str = quoteString(util.JSONStringify(arg))
	default:
		value := reflect.ValueOf(arg)
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				str = "NULL"
			} else {
				if value.Elem().Kind() == reflect.Struct {
					str = quoteString(util.JSONStringify(arg))
				} else {
					str = quoteString(fmt.Sprintf("%v", value.Elem().Interface()))
				}
			}
		} else if value.Kind() == reflect.Struct {
			str = quoteString(util.JSONStringify(arg))
		} else {
			str = quoteString(fmt.Sprintf("%v", arg))
		}
	}
	return str
}

// func quoteStringValues(vals []string) []string {
// 	res := make([]string, len(vals))
// 	for i, val := range vals {
// 		res[i] = quoteValue(val)
// 	}
// 	return res
// }

func quoteStringIdentifiers(vals []string) []string {
	res := make([]string, len(vals))
	for i, val := range vals {
		res[i] = quoteIdentifier(val)
	}
	return res
}

func (c *DBChangeEvent) ToSQL(schema map[string]*schema) (string, error) {
	var sql strings.Builder
	model := schema[c.Table]
	primaryKeys := model.PrimaryKeys
	if c.Operation == "DELETE" {
		sql.WriteString("DELETE FROM ")
		sql.WriteString(quoteIdentifier(c.Table))
		sql.WriteString(" WHERE ")
		predicate := make([]string, 0)
		for i, pk := range primaryKeys {
			predicate = append(predicate, fmt.Sprintf("%s=%s", quoteIdentifier(pk), quoteValue(c.Key[i])))
		}
		sql.WriteString(strings.Join(predicate, " AND "))
		sql.WriteString(";\n")
	} else {
		o := make(map[string]any)
		if err := json.Unmarshal(c.After, &o); err != nil {
			return "", err
		}
		sql.WriteString("MERGE INTO ")
		sql.WriteString(quoteIdentifier(c.Table))
		sql.WriteString(" USING (SELECT ")
		sql.WriteString(strings.Join(quoteStringIdentifiers(primaryKeys), ","))
		sql.WriteString(" FROM ")
		sql.WriteString(quoteIdentifier(c.Table))
		sql.WriteString(" WHERE ")
		sourcePredicates := make([]string, 0)
		sourceNullPredicates := make([]string, 0)
		targetPredicates := make([]string, 0)
		for _, pk := range primaryKeys {
			val := o[pk]
			sourcePredicates = append(sourcePredicates, fmt.Sprintf("%s=%s", quoteIdentifier(pk), quoteValue(val)))
			sourceNullPredicates = append(sourceNullPredicates, fmt.Sprintf("NULL AS %s", quoteIdentifier(pk)))
			targetPredicates = append(targetPredicates, fmt.Sprintf("source.%s=%s.%s", quoteIdentifier(pk), quoteIdentifier(c.Table), quoteIdentifier(pk)))
		}
		columns := make([]string, 0)
		columnNames := make([]string, 0)
		for name := range model.Properties {
			if !skipFields[name] {
				columns = append(columns, quoteIdentifier(name))
				columnNames = append(columnNames, name)
			}
		}
		sort.Strings(columns)     // TODO: optimize
		sort.Strings(columnNames) // TODO: optimize
		insertVals := make([]string, 0)
		updateValues := make([]string, 0)
		if c.Operation == "UPDATE" {
			for _, name := range c.Diff {
				if val, ok := o[name]; ok {
					v := quoteValue(val)
					updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
				} else {
					updateValues = append(updateValues, "NULL")
				}
			}
			for _, name := range columnNames {
				if val, ok := o[name]; ok {
					v := quoteValue(val)
					insertVals = append(insertVals, v)
				} else {
					insertVals = append(insertVals, "NULL")
				}
			}
		} else {
			for _, name := range columnNames {
				if val, ok := o[name]; ok {
					v := quoteValue(val)
					updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
					insertVals = append(insertVals, v)
				} else {
					updateValues = append(updateValues, "NULL")
					insertVals = append(insertVals, "NULL")
				}
			}
		}
		sql.WriteString(strings.Join(sourcePredicates, " AND "))
		sql.WriteString(" UNION SELECT ")
		sql.WriteString(strings.Join(sourceNullPredicates, " AND "))
		sql.WriteString(" LIMIT 1) AS source ON ")
		sql.WriteString(strings.Join(targetPredicates, " AND "))
		sql.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		sql.WriteString(strings.Join(updateValues, " AND "))
		sql.WriteString(" WHEN NOT MATCHED THEN INSERT (")
		sql.WriteString(strings.Join(columns, ","))
		sql.WriteString(") VALUES (")
		sql.WriteString(strings.Join(insertVals, ","))
		sql.WriteString(");\n")
	}
	return sql.String(), nil
}

var server2Cmd = &cobra.Command{
	Use:   "server2",
	Short: "Run the server",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		log := newLogger(logger.LevelTrace)

		natsurl, _ := cmd.Flags().GetString("server")
		dbUrl := mustFlagString(cmd, "db-url", true)
		creds, _ := cmd.Flags().GetString("creds")
		apiURL, _ := cmd.Flags().GetString("api-url")

		var schema map[string]*schema
		var err error

		schema, err = loadSchema(apiURL, true)
		if err != nil {
			log.Error("error loading schema: %s", err)
			os.Exit(1)
		}

		var natsCredentials nats.Option
		var companyIDs []string
		companyName := "unknown"
		// FIXME: remove
		companyIDs = []string{"6287a4154d1a72cc5ce091bb"}

		if natsurl != "" {
			natsCredentials, companyIDs, companyName, err = getNatsCreds(creds)
			if err != nil {
				log.Error("error: %s", err)
				os.Exit(1)
			}
		}

		// Nats connection to main NATS server
		nc, err := cnats.NewNats(log, "eds-server-"+companyName, natsurl, natsCredentials)
		if err != nil {
			log.Error("nats: %s", err)
			os.Exit(1)
		}
		defer nc.Close()

		db, err := connect2DB(ctx, dbUrl)
		if err != nil {
			log.Error("error connecting to db: %s", err)
			os.Exit(1)
		}
		defer db.Close()

		createHandler := func(logger logger.Logger) (cnats.Handler, error) {
			buffer := make(chan *nats.Msg, maxAckPending)
			pending := make([]*nats.Msg, 0)
			var pendingStarted *time.Time
			var builder strings.Builder
			flush := func() {
				q := builder.String()
				if traceInsertQueries {
					logger.Trace("running %s", q)
				}
				tv := time.Now()

				if _, err := db.ExecContext(ctx, q); err != nil {
					// TODO: nack all the messages
					logger.Error("error running %s: %s", err, q)
					os.Exit(1)
				}

				// ack all the messages only after inserted
				for _, m := range pending {
					if err := m.Ack(); err != nil {
						logger.Error("error acking %s: %s", err, q)
						os.Exit(1)
					}
				}
				latency := time.Since(tv)
				if traceInserts {
					first := pending[0]
					last := pending[len(pending)-1]
					fmd, _ := first.Metadata()
					lmd, _ := last.Metadata()
					logger.Trace("flush (first:%v,last:%v,total:%d,pending:%d) completed in %v", fmd.Sequence.Consumer, lmd.Sequence.Consumer, len(pending), lmd.NumPending, latency)
				}
				pending = pending[:0]
				builder.Reset()
				pendingStarted = nil
			}
			go func() {
				for {
					select {
					case msg := <-buffer:
						pending = append(pending, msg)
						md, _ := msg.Metadata()
						var evt DBChangeEvent
						if err := json.Unmarshal(msg.Data, &evt); err != nil {
							msg.Ack()
							log.Trace("record error: %s", string(msg.Data))
							log.Error("error unmarshalling %s (subject:%s,seq:%d,msgid:%v): %s", "dbchange", msg.Subject, md.Sequence.Consumer, msg.Header.Get(nats.MsgIdHdr), err)
							os.Exit(1)
						}
						sql, err := evt.ToSQL(schema)
						if err != nil {
							log.Error("error creating sql for %s: %s", string(msg.Data), err)
							os.Exit(1)
						}
						if _, err := builder.WriteString(sql); err != nil {
							log.Error("error writing to buffer: %s", err)
							os.Exit(1)
						}
						if pendingStarted == nil {
							ts := time.Now()
							pendingStarted = &ts
						}
						if md.NumPending > maxBatchSize && time.Since(*pendingStarted) < maxPendingLatency*2 {
							continue // if we have a large number, just keep going to try and catchup
						}
						if len(pending) >= maxBatchSize || time.Since(*pendingStarted) >= maxPendingLatency {
							flush()
						}
					default:
						count := len(pending)
						if count > 0 && count < maxBatchSize && time.Since(*pendingStarted) >= minPendingLatency {
							flush()
							continue
						}
						if count > 0 {
							continue
						}
						select {
						case <-ctx.Done():
							return
						default:
							time.Sleep(emptyBufferPauseTime)
						}
					}
				}
			}()

			return func(ctx context.Context, buf []byte, msg *nats.Msg) error {
				msg.Data = buf // buf will be different if encoding is gzip etc
				buffer <- msg
				return nil
			}, nil
		}

		replicas := 1

		jslogger := log.WithPrefix("[nats-js]")
		js, err := nc.JetStream(&nats.ClientTrace{
			RequestSent: func(subj string, payload []byte) {
				jslogger.Trace("nats tx: %s: %s", subj, string(payload))
			},
			ResponseReceived: func(subj string, payload []byte, hdr nats.Header) {
				jslogger.Trace("nats rx: %s: %s", subj, string(payload))
			},
		})
		if err != nil {
			log.Error("error creating jetstream: %v", err)
			os.Exit(1)
		}
		subscribers := make([]cnats.Subscriber, 0)

		consumerPrefix := "robin-is-testing"

		for _, companyID := range companyIDs {

			name := fmt.Sprintf("%seds-server-%s", consumerPrefix, companyID)
			subject := "dbchange.*.*." + companyID + ".*.PUBLIC.>"

			log.Info("starting consumer %s on %s", name, subject)
			handler, err := createHandler(log)
			if err != nil {
				log.Error("error creating handler: %v", err)
				os.Exit(1)
			}
			sub, err := cnats.NewQueueConsumer(log, js, "dbchange", name, subject, handler,
				cnats.WithQueueContext(ctx),
				cnats.WithQueueReplicas(replicas),
				cnats.WithQueueDelivery(nats.DeliverNewPolicy),
				cnats.WithQueueMaxDeliver(1_000),
				cnats.WithQueueAckWait(time.Minute*5),
				cnats.WithQueueMaxAckPending(maxAckPending),
				cnats.WithQueueMaxRequestBatch(maxPendingBuffer),
				cnats.WithQueueDisableSubscriberLogging(),
			)
			if err != nil {
				log.Error("error creating consumer: %v", err)
				os.Exit(1)
			}
			subscribers = append(subscribers, sub)
		}
		log.Info("server is running")
		<-csys.CreateShutdownChannel()
		log.Info("shutting down")

		cancel()

		for _, sub := range subscribers {
			sub.Close()
		}

		log.Info("👋 Bye")
	},
}

func init() {
	rootCmd.AddCommand(server2Cmd)
	server2Cmd.Flags().String("creds", "", "the server credentials file provided by Shopmonkey")
	server2Cmd.Flags().String("server", "nats://connect.nats.shopmonkey.pub", "the nats server url, could be multiple comma separated")
	server2Cmd.Flags().String("db-url", "", "Snowflake Database connection string")
	server2Cmd.Flags().String("api-url", "https://api.shopmonkey.cloud", "url to shopmonkey api")
}
