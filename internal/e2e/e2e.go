//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const (
	sessionId         = "4641e7c5-0f86-4576-89ca-15db401aea56"
	companyId         = "1234567890"
	enrollToken       = "1234567890"
	modelVersion      = "fff000111"
	modelVersion2     = "fff000112-update"
	modelVersion3     = "fff000113-update"
	serverID          = "1"
	dbuser            = "eds"
	dbpass            = "Asdf1234!"
	dbname            = "eds"
	apikey            = "apikey"
	defaultPayload    = `{"id":"12345","name":"test"}`
	defaultPayload2   = `{"id":"12345","name":"test","age":1}`
	defaultPayload3   = `{"id":"12345","name":"test","foo":1}`
	eventDeliverDelay = time.Millisecond * 150
)

type e2eTest interface {
	Name() string
	URL(dir string) string
	Validate(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error
}

type e2eTestDisabled interface {
	Disabled() bool
}

type e2eTestReady interface {
	WaitForReady(timeout time.Duration) error
}

var tests = make([]e2eTest, 0)

func registerTest(t e2eTest) {
	tests = append(tests, t)
}

type runCallbackFunc func(*exec.Cmd)
type exitCallbackFunc func(int) bool // return true to not exit

func run(cmd string, args []string, cb runCallbackFunc, exitCallback exitCallbackFunc) {
	c := exec.Command(util.GetExecutable(), append([]string{cmd}, args...)...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if cb != nil {
		go cb(c)
	}
	if err := c.Run(); err != nil {
		ec := 1
		if c.ProcessState != nil {
			ec = c.ProcessState.ExitCode()
		}
		if exitCallback != nil {
			if exitCallback(ec) {
				return
			}
		}
		os.Exit(ec)
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

func publishDBChangeEvent(logger logger.Logger, js jetstream.JetStream, table string, operation string, modelVersion string, payload string) (*internal.DBChangeEvent, error) {
	var event internal.DBChangeEvent
	event.ID = util.Hash(time.Now())
	event.Operation = operation
	event.Table = table
	event.Key = []string{"12345"}
	event.ModelVersion = modelVersion
	event.Timestamp = time.Now().UnixMilli()
	event.MVCCTimestamp = fmt.Sprintf("%d", time.Now().Nanosecond())
	if operation == "DELETE" {
		event.Before = json.RawMessage([]byte(payload))
	} else {
		event.After = json.RawMessage([]byte(payload))
		if event.Operation == "UPDATE" {
			event.Diff = []string{"name"}
		}
	}
	event.Imported = false
	buf, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("error marshalling event: %w", err)
	}
	subject := fmt.Sprintf("dbchange.%s.%s."+companyId+".1.PUBLIC.2", table, operation)
	logger.Info("publishing event: %s => %v", subject, util.JSONStringify(event))
	msgId := util.Hash(event)
	if _, err := js.Publish(context.Background(), subject, buf, jetstream.WithMsgID(msgId)); err != nil {
		return nil, fmt.Errorf("error publishing event: %w", err)
	}
	return &event, nil
}

func runDBChangeNewTableTest(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult checkValidEvent) error {
	event, err := publishDBChangeEvent(logger, js, "customer", "INSERT", modelVersion, defaultPayload)
	if err != nil {
		return err
	}
	time.Sleep(eventDeliverDelay)
	return readResult(*event)
}

func runDBChangeNewTableTest2(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult checkValidEvent) error {
	event, err := publishDBChangeEvent(logger, js, "customer", "INSERT", modelVersion2, defaultPayload)
	if err != nil {
		return err
	}
	time.Sleep(eventDeliverDelay)
	return readResult(*event)
}

func runDBChangeNewColumnTest(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult checkValidEvent) error {
	event, err := publishDBChangeEvent(logger, js, "order", "INSERT", modelVersion2, defaultPayload2)
	if err != nil {
		return err
	}
	time.Sleep(eventDeliverDelay)
	return readResult(*event)
}

func runDBChangeNewColumnTest2(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult checkValidEvent) error {
	event, err := publishDBChangeEvent(logger, js, "order", "UPDATE", modelVersion3, defaultPayload2)
	if err != nil {
		return err
	}
	time.Sleep(eventDeliverDelay)
	return readResult(*event)
}

func runDBChangeInsertTest(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult checkValidEvent) error {
	event, err := publishDBChangeEvent(logger, js, "order", "INSERT", modelVersion, defaultPayload)
	if err != nil {
		return err
	}
	time.Sleep(eventDeliverDelay)
	return readResult(*event)
}

func runDBChangeInsertTest2(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult checkValidEvent) error {
	event, err := publishDBChangeEvent(logger, js, "customer", "INSERT", modelVersion2, defaultPayload)
	if err != nil {
		return err
	}
	time.Sleep(eventDeliverDelay)
	return readResult(*event)
}

func runDBChangeUpdateTest(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult checkValidEvent) error {
	event, err := publishDBChangeEvent(logger, js, "order", "UPDATE", modelVersion, defaultPayload)
	if err != nil {
		return err
	}
	time.Sleep(eventDeliverDelay)
	return readResult(*event)
}

func runDBChangeDeleteTest(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult checkValidEvent) error {
	event, err := publishDBChangeEvent(logger, js, "order", "DELETE", modelVersion, defaultPayload)
	if err != nil {
		return err
	}
	time.Sleep(eventDeliverDelay)
	return readResult(*event)
}

func runDBChangeSchemaMismatchTest(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult checkValidEvent) error {
	event, err := publishDBChangeEvent(logger, js, "order", "INSERT", modelVersion, defaultPayload3)
	if err != nil {
		return err
	}
	time.Sleep(eventDeliverDelay)
	return readResult(*event)
}

func RunTests(logger logger.Logger, only []string) (bool, error) {
	tmpdir, err := os.MkdirTemp("", "e2e")
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(tmpdir)
	healthPort, err := util.GetFreePort()
	if err != nil {
		return false, err
	}

	var pass, fail, skipped uint32
	runNatsTestServer(func(nc *nats.Conn, js jetstream.JetStream, srv *server.Server, userCreds string) {
		logger.Trace("creds: %s", userCreds)
		httpport, shutdown := setupServer(logger, userCreds)
		apiurl := fmt.Sprintf("http://127.0.0.1:%d", httpport)
		run("enroll", []string{"--api-url", apiurl, "-v", "-d", tmpdir, "1234"}, nil, nil)
		for _, test := range tests {
			if tv, ok := test.(e2eTestDisabled); ok {
				if tv.Disabled() {
					skipped++
					logger.Warn("skipping test: %s", test.Name())
					continue
				}
			}
			if len(only) > 0 && !util.SliceContains(only, test.Name()) {
				skipped++
				logger.Warn("skipping test: %s", test.Name())
				continue
			}
			if testReady, ok := test.(e2eTestReady); ok {
				if err := testReady.WaitForReady(2 * time.Second); err != nil {
					logger.Error("%s readiness check failed: %s", test.Name(), err)
					atomic.AddUint32(&fail, 1)
					continue
				}
			}
			os.Remove(filepath.Join(tmpdir, "eds-data.db"))
			var wg sync.WaitGroup
			wg.Add(1)
			ts := time.Now()
			name := test.Name()
			url := test.URL(tmpdir)
			var lookingForExitCode int
			// var foundExitCode bool
			logger.Info("running test: %s", name)
			run("import", []string{"--api-url", apiurl, "-v", "-d", tmpdir, "--no-confirm", "--schema-only", "--api-key", apikey, "--url", url, "--log-label", name}, nil, nil)
			run("server", []string{"--api-url", apiurl, "--url", url, "-v", "-d", tmpdir, "--server", srv.ClientURL(), "--port", strconv.Itoa(healthPort), "--log-label", name, "--minPendingLatency", "1ms", "--maxPendingLatency", "1ms", "--no-restart"}, func(c *exec.Cmd) {
				defer func() {
					if c.Process != nil {
						c.Process.Signal(os.Interrupt)
					}
					wg.Done()
				}()
				time.Sleep(time.Second)
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
					logger.Error("🔴 ping failed for test: %s. %s", name, err)
					atomic.AddUint32(&fail, 1)
					return
				}
				_logger.Info("health check: %s", resp.Status)
				if resp.StatusCode != http.StatusOK {
					logger.Error("🔴 health check failed for test: %s. %s", name, resp.Status)
					atomic.AddUint32(&fail, 1)
					return
				}
				testStarted := time.Now()
				if err := runDBChangeInsertTest(logger, nc, js, func(event internal.DBChangeEvent) error {
					return test.Validate(_logger, tmpdir, url, event)
				}); err != nil {
					logger.Error("🔴 dbchange insert test: %s failed: %s", name, err)
					atomic.AddUint32(&fail, 1)
				} else {
					atomic.AddUint32(&pass, 1)
					logger.Info("✅ dbchange insert test: %s succeeded in %s", name, time.Since(testStarted))
				}
				testStarted = time.Now()
				if err := runDBChangeNewTableTest(logger, nc, js, func(event internal.DBChangeEvent) error {
					return test.Validate(_logger, tmpdir, url, event)
				}); err != nil {
					atomic.AddUint32(&fail, 1)
					logger.Error("🔴 dbchange new table test: %s failed: %s", name, err)
				} else {
					atomic.AddUint32(&pass, 1)
					logger.Info("✅ dbchange new table test: %s succeeded in %s", name, time.Since(testStarted))
				}
				testStarted = time.Now()
				if err := runDBChangeUpdateTest(logger, nc, js, func(event internal.DBChangeEvent) error {
					return test.Validate(_logger, tmpdir, url, event)
				}); err != nil {
					atomic.AddUint32(&fail, 1)
					logger.Error("🔴 dbchange update test: %s failed: %s", name, err)
				} else {
					atomic.AddUint32(&pass, 1)
					logger.Info("✅ dbchange update test: %s succeeded in %s", name, time.Since(testStarted))
				}
				testStarted = time.Now()
				if err := runDBChangeDeleteTest(logger, nc, js, func(event internal.DBChangeEvent) error {
					return test.Validate(_logger, tmpdir, url, event)
				}); err != nil {
					atomic.AddUint32(&fail, 1)
					logger.Error("🔴 dbchange delete test: %s failed: %s", name, err)
				} else {
					atomic.AddUint32(&pass, 1)
					logger.Info("✅ dbchange delete test: %s succeeded in %s", name, time.Since(testStarted))
				}
				testStarted = time.Now()
				if err := runDBChangeNewTableTest2(logger, nc, js, func(event internal.DBChangeEvent) error {
					return test.Validate(_logger, tmpdir, url, event)
				}); err != nil {
					atomic.AddUint32(&fail, 1)
					logger.Error("🔴 dbchange new table (#2) test: %s failed: %s", name, err)
				} else {
					atomic.AddUint32(&pass, 1)
					logger.Info("✅ dbchange new table (#2) test: %s succeeded in %s", name, time.Since(testStarted))
				}
				testStarted = time.Now()
				if err := runDBChangeNewColumnTest(logger, nc, js, func(event internal.DBChangeEvent) error {
					return test.Validate(_logger, tmpdir, url, event)
				}); err != nil {
					atomic.AddUint32(&fail, 1)
					logger.Error("🔴 dbchange new column test: %s failed: %s", name, err)
				} else {
					atomic.AddUint32(&pass, 1)
					logger.Info("✅ dbchange new column: %s succeeded in %s", name, time.Since(testStarted))
				}
				testStarted = time.Now()
				if err := runDBChangeNewColumnTest2(logger, nc, js, func(event internal.DBChangeEvent) error {
					return test.Validate(_logger, tmpdir, url, event)
				}); err != nil {
					atomic.AddUint32(&fail, 1)
					logger.Error("🔴 dbchange new column (#2) test: %s failed: %s", name, err)
				} else {
					atomic.AddUint32(&pass, 1)
					logger.Info("✅ dbchange new column (#2) test: %s succeeded in %s", name, time.Since(testStarted))
				}
				testStarted = time.Now()
				if err := runDBChangeInsertTest2(logger, nc, js, func(event internal.DBChangeEvent) error {
					return test.Validate(_logger, tmpdir, url, event)
				}); err != nil {
					logger.Error("🔴 dbchange insert (#2) test: %s failed: %s", name, err)
					atomic.AddUint32(&fail, 1)
				} else {
					atomic.AddUint32(&pass, 1)
					logger.Info("✅ dbchange insert (#2) test: %s succeeded in %s", name, time.Since(testStarted))
				}
			}, func(ec int) bool {
				if ec == lookingForExitCode {
					// foundExitCode = true  --> leaving this in for now
					return true
				}
				return false
			})
			wg.Wait()
			// if the test acquires a resource, close it
			if closer, ok := test.(io.Closer); ok {
				if err := closer.Close(); err != nil {
					logger.Error("error closing test: %s: %s", name, err)
				}
			}
			logger.Info("test: %s completed in %s", name, time.Since(ts))
		}
		logger.Info("shutting down server")
		shutdown()
	})
	logger.Info("✅ %d passed 🔴 %d failed 🚧 %d skipped", pass, fail, skipped)
	return fail == 0, nil
}
