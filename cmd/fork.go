package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/consumer"
	"github.com/shopmonkeyus/eds-server/internal/registry"
	"github.com/shopmonkeyus/eds-server/internal/tracker"
	"github.com/shopmonkeyus/eds-server/internal/util"
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
	Use:   "fork",
	Short: "Run the server",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		logger, closer := newLogger(cmd)
		defer closer()
		logger = logger.WithPrefix("[fork]")

		defer util.RecoverPanic(logger)

		natsurl := mustFlagString(cmd, "server", true)
		url := mustFlagString(cmd, "url", true)
		creds := mustFlagString(cmd, "creds", !util.IsLocalhost(natsurl))
		datadir := mustFlagString(cmd, "data-dir", true)
		_schemaFile := mustFlagString(cmd, "schema", true)
		_tablesFile := mustFlagString(cmd, "tables", true)
		consumerSuffix := mustFlagString(cmd, "consumer-suffix", false)
		maxAckPending := mustFlagInt(cmd, "maxAckPending", false)
		maxPendingBuffer := mustFlagInt(cmd, "maxPendingBuffer", false)
		port := mustFlagInt(cmd, "port", false)

		serverStarted := time.Now()

		// assume these are default in the same directory as the data-dir
		schemaFile := filepath.Join(datadir, _schemaFile)
		if util.Exists(_schemaFile) {
			schemaFile = _schemaFile
		}
		tablesFile := filepath.Join(datadir, _tablesFile)
		if util.Exists(tablesFile) {
			tablesFile = _tablesFile
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

		registry, err := registry.NewFileRegistry(schemaFile)
		if err != nil {
			logger.Error("error creating registry: %s", err)
			os.Exit(3)
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

		driver, err := internal.NewDriver(ctx, logger, url, registry, tracker)
		if err != nil {
			logger.Error("error creating driver: %s", err)
			os.Exit(3)
		}

		defer driver.Stop()

		runHealthCheckServerFork(logger, port)

		// create a channel to listen for SIGHUP to restart the consumer
		restart := make(chan os.Signal, 1)
		signal.Notify(restart, syscall.SIGHUP)

		var wg sync.WaitGroup
		wg.Add(1)

		restartFlag, _ := cmd.Flags().GetBool("restart")

		// the ability to pause and unpause the consumer from HTTP control
		pauseCh := make(chan bool)
		http.HandleFunc("/control/pause", func(w http.ResponseWriter, r *http.Request) {
			pauseCh <- true
			w.WriteHeader(http.StatusOK)
		})
		http.HandleFunc("/control/unpause", func(w http.ResponseWriter, r *http.Request) {
			pauseCh <- false
			w.WriteHeader(http.StatusOK)
		})

		go func() {
			defer util.RecoverPanic(logger)
			defer wg.Done()
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
						Restart:               restartFlag,
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
					driver.Stop()
					cancel()
					closer()
					tracker.Close()
					os.Exit(1)
				case <-restart:
					logger.Debug("restarting consumer on SIGHUP")
					if err := localConsumer.Stop(); err != nil {
						logger.Error("error stopping consumer: %s", err)
					}
					localConsumer = nil
					paused = false
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
								driver.Stop()
								cancel()
								closer()
								tracker.Close()
								os.Exit(1)
							}
						}
					}
				}
			}
		}()

		logger.Info("server is running version: %v", Version)

		// wait for shutdown or error
		<-sys.CreateShutdownChannel()

		logger.Debug("server is stopping")

		driver.Stop()
		cancel()
		wg.Wait()
		tracker.Close()

		logger.Trace("server was up for %v", time.Since(serverStarted))
		logger.Info("👋 Bye")
	},
}

func init() {
	rootCmd.AddCommand(forkCmd)
	forkCmd.Hidden = true // don't expose this since its only called by the main server process in the wrapper
	// NOTE: sync these with serverCmd
	forkCmd.Flags().String("data-dir", "", "the data directory for storing logs and other data")
	forkCmd.Flags().String("consumer-suffix", "", "a suffix to use for the consumer group name")
	forkCmd.Flags().String("creds", "", "the server credentials file provided by Shopmonkey")
	forkCmd.Flags().String("server", "nats://connect.nats.shopmonkey.pub", "the nats server url, could be multiple comma separated")
	forkCmd.Flags().String("url", "", "Snowflake Database connection string")
	forkCmd.Flags().String("schema", "schema.json", "the Shopmonkey schema file")
	forkCmd.Flags().String("tables", "tables.json", "the Shopmonkey tables file")
	forkCmd.Flags().Int("maxAckPending", defaultMaxAckPending, "the number of max ack pending messages")
	forkCmd.Flags().Int("maxPendingBuffer", defaultMaxPendingBuffer, "the maximum number of messages to pull from nats to buffer")
	forkCmd.Flags().Bool("restart", false, "restart the consumer from the beginning (only works on new consumers)")
	forkCmd.Flags().Int("port", 0, "the port to listen for health checks and metrics")
}
