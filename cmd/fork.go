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

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/consumer"
	"github.com/shopmonkeyus/eds-server/internal/registry"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	csys "github.com/shopmonkeyus/go-common/sys"
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
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil && err != http.ErrServerClosed {
			logger.Fatal("failed to start health check server: %s", err)
		}
	}()
}

func getReplicas(logger logger.Logger, cmd *cobra.Command, natsurl string) int {
	replicas := mustFlagInt(cmd, "replicas", false)

	// dynamically set based on nats server if not set
	if replicas <= 0 {
		if util.IsLocalhost(natsurl) {
			return 1
		}
		return 3
	}
	if replicas > 3 {
		logger.Error("replicas must be between 1-3")
		os.Exit(2)
	}
	return replicas
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

		natsurl := mustFlagString(cmd, "server", true)
		url := mustFlagString(cmd, "url", true)
		creds := mustFlagString(cmd, "creds", !util.IsLocalhost(natsurl))
		schemaFile := mustFlagString(cmd, "schema", true)
		consumerSuffix := mustFlagString(cmd, "consumer-suffix", false)
		maxAckPending := mustFlagInt(cmd, "maxAckPending", false)
		maxPendingBuffer := mustFlagInt(cmd, "maxPendingBuffer", false)
		healthPort := mustFlagInt(cmd, "health-port", false)
		serverStarted := time.Now()

		replicas := getReplicas(logger, cmd, natsurl)

		registry, err := registry.NewFileRegistry(schemaFile)
		if err != nil {
			logger.Error("error creating registry: %s", err)
			os.Exit(2)
		}

		processor, err := internal.NewProcessor(ctx, logger, url, registry)
		if err != nil {
			logger.Error("error creating processor: %s", err)
			os.Exit(2)
		}

		defer processor.Stop()

		runHealthCheckServerFork(logger, healthPort)

		// create a channel to listen for SIGHUP to restart the consumer
		restart := make(chan os.Signal, 1)
		signal.Notify(restart, syscall.SIGHUP)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			var completed bool
			for !completed {
				consumer, err := consumer.NewConsumer(consumer.ConsumerConfig{
					Context:          ctx,
					Logger:           logger,
					URL:              natsurl,
					Credentials:      creds,
					Suffix:           consumerSuffix,
					MaxAckPending:    maxAckPending,
					MaxPendingBuffer: maxPendingBuffer,
					Replicas:         replicas,
					Processor:        processor,
				})
				if err != nil {
					logger.Error("error creating consumer: %s", err)
					os.Exit(2)
				}
				select {
				case <-ctx.Done():
					completed = true
				case <-restart:
					logger.Debug("restarting consumer on SIGHUP")
				}
				if err := consumer.Stop(); err != nil {
					logger.Error("error stopping consumer: %s", err)
				}
			}
		}()

		logger.Info("server is running")

		// wait for shutdown or error
		<-csys.CreateShutdownChannel()

		logger.Debug("server is stopping")

		processor.Stop()
		cancel()
		wg.Wait()

		logger.Trace("server was up for %v", time.Since(serverStarted))
		logger.Info("👋 Bye")
	},
}

func init() {
	rootCmd.AddCommand(forkCmd)
	forkCmd.Hidden = true // don't expose this since its only called by the main server process in the wrapper
	// NOTE: sync these with serverCmd
	forkCmd.Flags().String("consumer-suffix", "", "a suffix to use for the consumer group name")
	forkCmd.Flags().String("creds", "", "the server credentials file provided by Shopmonkey")
	forkCmd.Flags().String("server", "nats://connect.nats.shopmonkey.pub", "the nats server url, could be multiple comma separated")
	forkCmd.Flags().String("url", "", "Snowflake Database connection string")
	forkCmd.Flags().String("schema", "schema.json", "the Shopmonkey schema file")
	forkCmd.Flags().Int("replicas", -1, "the number of consumer replicas")
	forkCmd.Flags().Int("maxAckPending", defaultMaxAckPending, "the number of max ack pending messages")
	forkCmd.Flags().Int("maxPendingBuffer", defaultMaxPendingBuffer, "the maximum number of messages to pull from nats to buffer")
	forkCmd.Flags().Int("health-port", 0, "the port to listen for health checks")
}
