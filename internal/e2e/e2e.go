//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const (
	sessionId    = "4641e7c5-0f86-4576-89ca-15db401aea56"
	companyId    = "1234567890"
	enrollToken  = "1234567890"
	modelVersion = "fff000110"
	serverID     = "1"
	dbuser       = "eds"
	dbpass       = "Asdf1234!"
	dbname       = "eds"
	apikey       = "apikey"
)

type e2eTest interface {
	Name() string
	URL(dir string) string
	TestInsert(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error
}

type e2eTestDisabled interface {
	Disabled() bool
}

var tests = make([]e2eTest, 0)

func registerTest(t e2eTest) {
	if tv, ok := t.(e2eTestDisabled); ok {
		if tv.Disabled() {
			return
		}
	}
	tests = append(tests, t)
}

type runCallbackFunc func(*exec.Cmd)

func run(cmd string, args []string, cb runCallbackFunc) {
	c := exec.Command(util.GetExecutable(), append([]string{cmd}, args...)...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if cb != nil {
		go cb(c)
	}
	if err := c.Run(); err != nil {
		panic(err)
	}
}

type checkValidEvent func(event internal.DBChangeEvent) error

func dbchangeEventMatches(event internal.DBChangeEvent, event2 internal.DBChangeEvent) error {
	event2.Imported = false // remove for comparison
	if util.JSONStringify(event) != util.JSONStringify(event2) {
		return fmt.Errorf("events do not match. sent: %s, received: %s", util.JSONStringify(event), util.JSONStringify(event2))
	}
	return nil
}

func runDBChangeInsertTest(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult checkValidEvent) error {
	var event internal.DBChangeEvent
	event.ID = util.Hash(time.Now())
	event.Operation = "INSERT"
	event.Table = "order"
	event.Key = []string{"12345"}
	event.ModelVersion = modelVersion
	event.Timestamp = time.Now().UnixMilli()
	event.MVCCTimestamp = fmt.Sprintf("%d", time.Now().Nanosecond())
	event.After = json.RawMessage(`{"id":"12345","name":"test"}`)
	event.Imported = false
	buf, err := json.Marshal(event)
	if err != nil {
		return err
	}
	subject := "dbchange.order.INSERT." + companyId + ".1.PUBLIC.2"
	logger.Info("publishing event: %s with timestamp: %d", subject, event.Timestamp)
	msgId := util.Hash(event)
	if _, err := js.Publish(context.Background(), subject, buf, jetstream.WithMsgID(msgId)); err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	return readResult(event)
}

func RunTests(logger logger.Logger, only []string) error {
	tmpdir, err := os.MkdirTemp("", "e2e")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpdir)
	healthPort, err := util.GetFreePort()
	if err != nil {
		panic(err)
	}
	runNatsTestServer(func(nc *nats.Conn, js jetstream.JetStream, srv *server.Server, userCreds string) {
		logger.Trace("creds: %s", userCreds)
		httpport, shutdown := setupServer(logger, userCreds)
		apiurl := fmt.Sprintf("http://127.0.0.1:%d", httpport)
		run("enroll", []string{"--api-url", apiurl, "-v", "-d", tmpdir, "1234"}, nil)
		for _, test := range tests {
			if len(only) > 0 && !util.SliceContains(only, test.Name()) {
				continue
			}
			var wg sync.WaitGroup
			wg.Add(1)
			ts := time.Now()
			name := test.Name()
			url := test.URL(tmpdir)
			logger.Info("running test: %s", name)
			run("import", []string{"--api-url", apiurl, "-v", "-d", tmpdir, "--no-confirm", "--schema-only", "--api-key", apikey, "--url", url, "--log-label", name}, nil)
			run("server", []string{"--api-url", apiurl, "--url", url, "-v", "-d", tmpdir, "--server", srv.ClientURL(), "--port", strconv.Itoa(healthPort), "--log-label", name}, func(c *exec.Cmd) {
				time.Sleep(2 * time.Second)
				_logger := logger.WithPrefix("[" + name + "]")
				_logger.Info("ready to test")
				pingStarted := time.Now()
				var resp *http.Response
				var err error
				for time.Since(pingStarted) < 30*time.Second {
					resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d/", healthPort))
					if err != nil {
						if strings.Contains(err.Error(), "connection refused") {
							time.Sleep(time.Second)
							continue
						}
					}
					break
				}
				if err != nil {
					panic(err)
				}
				_logger.Info("health check: %s", resp.Status)
				if resp.StatusCode != http.StatusOK {
					panic("health check failed")
				}
				if err := runDBChangeInsertTest(logger, nc, js, func(event internal.DBChangeEvent) error {
					return test.TestInsert(_logger, tmpdir, url, event)
				}); err != nil {
					logger.Error("🔴 dbchange insert test: %s failed: %s", name, err)
				} else {
					logger.Info("✅ dbchange insert: %s succeeded in %s", name, time.Since(ts))
				}
				c.Process.Signal(os.Interrupt)
				wg.Done()
			})
			wg.Wait()
			logger.Info("test: %s completed in %s", name, time.Since(ts))
		}
		logger.Info("shutting down server")
		shutdown()
	})
	return nil
}
