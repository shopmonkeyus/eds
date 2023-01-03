package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the server",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger.Trace("connecting to nats server")
		nc, err := nats.Connect(nats.DefaultURL)
		if err != nil {
			logger.Error("error: nats connection: %s", err)
			os.Exit(1)
		}
		logger.Trace("connected to nats server")
		defer nc.Close()
		url := mustFlagString(cmd, "url", true)
		dryRun := mustFlagBool(cmd, "dry-run", false)
		runProvider(logger, url, dryRun, func(provider internal.Provider) error {
			logger.Trace("creating message processor")
			processor, err := internal.NewMessageProcessor(internal.MessageProcessorOpts{
				Logger:          logger,
				CompanyID:       "6287a4154d1a72cc5ce091bb", // FIXME: update once we have configuration loading
				Provider:        provider,
				NatsConnection:  nc,
				TraceNats:       mustFlagBool(cmd, "trace-nats", false),
				DumpMessagesDir: mustFlagString(cmd, "dump-dir", false),
			})
			if err != nil {
				return err
			}
			defer processor.Stop()
			logger.Trace("starting message processor")
			if err := processor.Start(); err != nil {
				return fmt.Errorf("processor start: %s", err)
			}
			logger.Info("started message processor")
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
			<-c
			logger.Info("stopped message processor")
			return nil
		})
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
	startCmd.Flags().Bool("trace-nats", false, "turn on lower level nats tracing")
	startCmd.Flags().String("dump-dir", "", "write each incoming message to this directory")
	startCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make db changes")
}
