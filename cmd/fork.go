package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/consumer"
	"github.com/shopmonkeyus/eds/internal/registry"
	"github.com/shopmonkeyus/eds/internal/tracker"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
)

const (
	defaultMaxAckPending    = 25_000 // this is currently our system max
	defaultMaxPendingBuffer = 4_096  // maximum number of messages to pull from nats to buffer

	exitCodeIncorrectUsage = 3
	exitCodeRestart        = 4
	exitCodeDisconnected   = 5
)

func runHealthCheckServerFork(logger logger.Logger, port int) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		defer util.RecoverPanic(logger)
		if err := http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port), nil); err != nil && err != http.ErrServerClosed {
			logger.Fatal("failed to start health check server: %s", err)
		}
	}()
}

var forkCmd = &cobra.Command{
	Use:    "fork",
	Short:  "Run the server",
	Args:   cobra.NoArgs,
	Hidden: true, // don't expose this since its only called by the main server process in the wrapper
	Run: func(cmd *cobra.Command, args []string) {
		serverStarted := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		logger := newLogger(cmd)
		datadir := mustFlagString(cmd, "data-dir", true)
		logDir := mustFlagString(cmd, "logs-dir", true)
		sink, err := newLogFileSink(logDir)
		if err != nil {
			logger.Error("error creating log file sink: %s", err)
			os.Exit(exitCodeIncorrectUsage)
		}
		defer sink.Close()
		logger.Trace("using log file sink: %s", logDir)
		logger = newLoggerWithSink(logger, sink).WithPrefix("[fork]")

		defer util.RecoverPanic(logger)

		// natsurl := mustFlagString(cmd, "server", true)
		url := mustFlagString(cmd, "url", true)
		// creds := mustFlagString(cmd, "creds", !util.IsLocalhost(natsurl))
		port := mustFlagInt(cmd, "port", false)
		transmissionAddress, _ := cmd.Flags().GetString("transmission-address")

		// TODO: get Transmission creds from backend
		transmissionCreds := ""

		edsID, _ := cmd.Flags().GetString("eds-id")
		sessionID, _ := cmd.Flags().GetString("session-id")

		// check to see if there's a schema validator and if so load it
		validator, err := loadSchemaValidator(cmd)
		if err != nil {
			logger.Fatal("error loading validator: %s", err)
		}

		tracker, err := tracker.NewTracker(tracker.TrackerConfig{
			Logger:  logger,
			Context: ctx,
			Dir:     datadir,
		})
		if err != nil {
			logger.Error("error creating tracker db: %s", err)
			os.Exit(exitCodeIncorrectUsage)
		}
		defer tracker.Close()

		apiUrl := mustFlagString(cmd, "api-url", true)
		schemaRegistry, err := registry.NewAPIRegistry(ctx, logger, apiUrl, Version, tracker)
		if err != nil {
			logger.Error("error creating registry: %s", err)
			os.Exit(exitCodeIncorrectUsage)
		}

		tableData, err := loadTableExportInfo(tracker)
		if err != nil {
			logger.Error("error loading table export data: %s", err)
			os.Exit(exitCodeIncorrectUsage)
		}

		exportTableTimestamps := make(map[string]*time.Time)
		for _, data := range tableData {
			exportTableTimestamps[data.Table] = &data.Timestamp
		}

		// note: don't use ctx here because we want the driver to continue running during shutdown so we can control the flush
		driver, err := internal.NewDriver(context.Background(), logger, url, schemaRegistry, tracker, datadir)
		if err != nil {
			logger.Error("error creating driver: %s", err)
			os.Exit(exitCodeIncorrectUsage)
		}

		defer driver.Stop()

		runHealthCheckServerFork(logger, port)

		// create a channel to listen for signals to control the process
		restart := make(chan os.Signal, 1)
		signal.Notify(restart, syscall.SIGHUP)

		var wg sync.WaitGroup
		wg.Add(1)

		// the ability to control the process from HTTP control channel
		pauseCh := make(chan bool)
		http.HandleFunc("/control/pause", func(w http.ResponseWriter, r *http.Request) {
			pauseCh <- true
			w.WriteHeader(http.StatusOK)
		})
		http.HandleFunc("/control/unpause", func(w http.ResponseWriter, r *http.Request) {
			pauseCh <- false
			w.WriteHeader(http.StatusOK)
		})
		http.HandleFunc("/control/restart", func(w http.ResponseWriter, r *http.Request) {
			restart <- syscall.SIGHUP
			w.WriteHeader(http.StatusOK)
		})
		http.HandleFunc("/control/shutdown", func(w http.ResponseWriter, r *http.Request) {
			restart <- syscall.SIGTERM
			w.WriteHeader(http.StatusOK)
		})
		http.HandleFunc("/control/logfile", func(w http.ResponseWriter, r *http.Request) {
			fn, err := sink.Rotate()
			if err != nil {
				logger.Error("error rotating log file: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fn))
		})

		var exitCode int
		go func() {
			defer util.RecoverPanic(logger)
			defer func() {
				cancel()
				wg.Done()
			}()
			var completed bool
			var paused bool
			var client *consumer.Client
			var err error
			for !completed {
				if !paused && client == nil {
					client, err = consumer.NewClient(consumer.ClientConfig{
						Context:               ctx,
						Logger:                logger,
						Address:               transmissionAddress,
						EdsID:                 edsID,
						SessionID:             sessionID,
						Driver:                driver,
						ExportTableTimestamps: exportTableTimestamps,
						SchemaValidator:       validator,
						Registry:              schemaRegistry,
						Credentials:           transmissionCreds,
					})
					if err != nil {
						logger.Error("error creating transmission client: %s", err)
						os.Exit(1)
					}

					if client != nil {
						go func(src *consumer.Client) {
							select {
							case <-src.Disconnected():
								logger.Warn("transmission disconnected")
								os.Exit(exitCodeDisconnected)
							case <-ctx.Done():
								return
							}
						}(client)
					}
				}
				select {
				case <-ctx.Done():
					completed = true
					client.Stop()
				case err := <-client.Error():
					logger.Error("error from message source: %s", err)
					if err := client.Stop(); err != nil {
						logger.Error("error stopping message source: %s", err)
					}
					exitCode = 1
					return
				case sig := <-restart:
					switch sig {
					case syscall.SIGHUP:
						logger.Debug("restarting message source")
						completed = true
						exitCode = exitCodeRestart // this is a special code to indicate an intentional restart
					case syscall.SIGTERM:
						logger.Debug("shutting down")
						completed = true
					}
					if err := client.Stop(); err != nil {
						logger.Error("error stopping message source: %s", err)
					}
					client = nil
				case pause := <-pauseCh:
					if pause {
						if !paused {
							paused = true
							logger.Debug("pausing")
							client.Pause()
						}
					} else {
						if paused {
							logger.Debug("unpausing")
							paused = false
							client.Unpause()
						}
					}
				}
			}
		}()

		logger.Info("server is running version: %v", Version)

		// wait for shutdown or cancel
		select {
		case <-ctx.Done():
		case <-sys.CreateShutdownChannel():
		}

		logger.Debug("server is stopping")

		cancel()
		wg.Wait()
		driver.Stop()
		tracker.Close()

		logger.Trace("server was up for %v", time.Since(serverStarted))
		logger.Info("👋 Bye")
		os.Exit(exitCode)
	},
}

func init() {
	rootCmd.AddCommand(forkCmd)

	// NOTE: sync these with serverCmd
	// these flags are altered by the server
	forkCmd.Flags().String("logs-dir", "", "the directory for storing logs")
	forkCmd.Flags().String("creds", "", "the server credentials file provided by Shopmonkey")
	forkCmd.Flags().String("url", "", "driver connection string")
	forkCmd.Flags().String("api-url", "", "url to shopmonkey api")
	forkCmd.Flags().String("transmission-address", "", "gRPC address for transmission")
	forkCmd.Flags().String("eds-id", "", "the EDS server ID used with transmission")
	forkCmd.Flags().String("session-id", "", "the session ID used with transmission")

	// NOTE: sync these with serverCmd
	// these flags are passed through from the server
	forkCmd.Flags().Int("port", 0, "the port to listen for health checks and metrics")
	forkCmd.Flags().String("server", "", "the nats server url, could be multiple comma separated")
}
