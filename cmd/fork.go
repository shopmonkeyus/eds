package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/consumer"
	"github.com/shopmonkeyus/eds-server/internal/registry"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	csys "github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"

	_ "github.com/shopmonkeyus/eds-server/internal/processors/postgresql"
	_ "github.com/shopmonkeyus/eds-server/internal/processors/snowflake"
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
		replicas := mustFlagInt(cmd, "replicas", true)
		healthPort := mustFlagInt(cmd, "health-port", false)
		serverStarted := time.Now()

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

		runHealthCheckServerFork(logger, healthPort)

		logger.Info("server is running")

		// wait for shutdown or error
		<-csys.CreateShutdownChannel()

		logger.Debug("server is stopping")

		consumer.Stop()
		processor.Stop()

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
	forkCmd.Flags().String("schema", "schema.json", "the shopmonkey schema file")
	forkCmd.Flags().Int("replicas", 1, "the number of consumer replicas")
	forkCmd.Flags().Int("maxAckPending", defaultMaxAckPending, "the number of max ack pending messages")
	forkCmd.Flags().Int("maxPendingBuffer", defaultMaxPendingBuffer, "the maximum number of messages to pull from nats to buffer")
	forkCmd.Flags().Int("health-port", 0, "the port to listen for health checks")
}
