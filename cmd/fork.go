package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
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
		companyIds, _ := cmd.Flags().GetStringSlice("company-id")
		datadir := mustFlagString(cmd, "data-dir", true)
		logDir := mustFlagString(cmd, "logs-dir", true)
		sink, err := newLogFileSink(logDir)
		if err != nil {
			logger.Error("error creating log file sink: %s", err)
			os.Exit(3)
		}
		defer sink.Close()
		logger.Trace("using log file sink: %s", logDir)
		logger = newLoggerWithSink(logger, sink).WithPrefix("[fork]")

		defer util.RecoverPanic(logger)

		natsurl := mustFlagString(cmd, "server", true)
		url := mustFlagString(cmd, "url", true)
		creds := mustFlagString(cmd, "creds", !util.IsLocalhost(natsurl))
		consumerSuffix := mustFlagString(cmd, "consumer-suffix", false)
		maxAckPending := mustFlagInt(cmd, "maxAckPending", false)
		maxPendingBuffer := mustFlagInt(cmd, "maxPendingBuffer", false)
		port := mustFlagInt(cmd, "port", false)

		schemaFile, tablesFile := getSchemaAndTableFiles(datadir)

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
			os.Exit(3)
		}
		defer tracker.Close()

		var schemaRegistry internal.SchemaRegistry

		if util.Exists(schemaFile) {
			schemaRegistry, err = registry.NewFileRegistry(schemaFile)
			if err != nil {
				logger.Error("error creating registry: %s", err)
				os.Exit(3)
			}
		} else {
			apiUrl := mustFlagString(cmd, "api-url", true)
			schemaRegistry, err = registry.NewAPIRegistry(apiUrl)
			if err != nil {
				logger.Error("error creating registry: %s", err)
				os.Exit(3)
			}
		}

		// TODO: move these into the tracker
		var exportTableTimestamps map[string]*time.Time
		if exportTableData, err := loadTablesJSON(tablesFile); err != nil {
			if cmd.Flags().Changed("tables") {
				logger.Error("provided tables file %s not found!", tablesFile)
				os.Exit(3)
			}
			if errors.Is(err, os.ErrNotExist) {
				logger.Info("tables file %s not found", tablesFile)
				// this is okay
			} else {
				logger.Error("error loading tables: %s", err)
				os.Exit(3)
			}
		} else {
			exportTableTimestamps = make(map[string]*time.Time)
			for _, data := range exportTableData {
				exportTableTimestamps[data.Table] = &data.Timestamp
			}
		}

		driver, err := internal.NewDriver(ctx, logger, url, schemaRegistry, tracker, datadir)
		if err != nil {
			logger.Error("error creating driver: %s", err)
			os.Exit(3)
		}

		defer driver.Stop()

		runHealthCheckServerFork(logger, port)

		// create a channel to listen for signals to control the process
		restart := make(chan os.Signal, 1)
		signal.Notify(restart, syscall.SIGHUP)

		var wg sync.WaitGroup
		wg.Add(1)

		restartFlag, _ := cmd.Flags().GetBool("restart")

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
			var localConsumer *consumer.Consumer
			var err error
			for !completed {
				if !paused && localConsumer == nil {
					localConsumer, err = consumer.NewConsumer(consumer.ConsumerConfig{
						Context:               ctx,
						Logger:                logger,
						URL:                   natsurl,
						Credentials:           creds,
						Suffix:                consumerSuffix,
						MaxAckPending:         maxAckPending,
						MaxPendingBuffer:      maxPendingBuffer,
						Driver:                driver,
						ExportTableTimestamps: exportTableTimestamps,
						DeliverAll:            restartFlag,
						SchemaValidator:       validator,
						CompanyIDs:            companyIds,
					})
					if err != nil {
						logger.Error("error creating consumer: %s", err)
						os.Exit(3)
					}
				}
				select {
				case <-ctx.Done():
					completed = true
					if localConsumer != nil {
						localConsumer.Stop()
						localConsumer = nil
					}
				case err := <-localConsumer.Error():
					if errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrDisconnected) {
						logger.Warn("nats server / consumer needs reconnection: %s", err)
					} else {
						logger.Error("error from consumer: %s", err)
					}
					if err := localConsumer.Stop(); err != nil {
						logger.Error("error stopping consumer: %s", err)
					}
					exitCode = 1
					return
				case sig := <-restart:
					switch sig {
					case syscall.SIGHUP:
						logger.Debug("restarting consumer")
						paused = false
					case syscall.SIGTERM:
						logger.Debug("shutting down")
						completed = true
					}
					if err := localConsumer.Stop(); err != nil {
						logger.Error("error stopping consumer: %s", err)
					}
					localConsumer = nil
				case pause := <-pauseCh:
					if pause {
						if !paused {
							paused = true
							logger.Debug("pausing")
							localConsumer.Pause()
						}
					} else {
						if paused {
							logger.Debug("unpausing")
							paused = false
							if err := localConsumer.Unpause(); err != nil {
								logger.Error("error unpausing: %s", err)
								return
							}
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

		driver.Stop()
		cancel()
		wg.Wait()
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
	forkCmd.Flags().Int("maxAckPending", defaultMaxAckPending, "the number of max ack pending messages")
	forkCmd.Flags().Int("maxPendingBuffer", defaultMaxPendingBuffer, "the maximum number of messages to pull from nats to buffer")
	forkCmd.Flags().Duration("minPendingLatency", 0, "the minimum accumulation period before flushing (0 uses default)")
	forkCmd.Flags().Duration("maxPendingLatency", 0, "the maximum accumulation period before flushing (0 uses default)")
	forkCmd.Flags().Bool("restart", false, "restart the consumer from the beginning (only works on new consumers)")

	// NOTE: sync these with serverCmd
	// these flags are passed through from the server
	forkCmd.Flags().Int("port", 0, "the port to listen for health checks and metrics")
	forkCmd.Flags().StringSlice("companyIds", nil, "restrict to a specific company ID or multiple")
	forkCmd.Flags().String("consumer-suffix", "", "a suffix to use for the consumer group name")
	forkCmd.Flags().String("server", "", "the nats server url, could be multiple comma separated")
}
