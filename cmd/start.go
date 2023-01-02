package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start the server",
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
		logger.Trace("connecting to database")
		glogger := internal.NewGormLogAdapter(logger)
		db := loadDatabase(cmd, &gorm.Config{Logger: glogger})
		logger.Trace("connected to database")
		logger.Trace("creating message processor")
		processor, err := internal.NewMessageProcessor(internal.MessageProcessorOpts{
			Logger:          logger,
			CompanyID:       "6287a4154d1a72cc5ce091bb", // FIXME: update once we have configuration loading
			Database:        db,
			NatsConnection:  nc,
			TraceNats:       mustFlagBool(cmd, "trace-nats", false),
			DumpMessagesDir: mustFlagString(cmd, "dump-dir", false),
		})
		if err != nil {
			logger.Error("error: processor: %s", err)
			os.Exit(1)
		}
		defer processor.Stop()
		logger.Trace("starting message processor")
		if err := processor.Start(); err != nil {
			logger.Error("error: processor start: %s", err)
			os.Exit(1)
		}
		logger.Info("started message processor")
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
		<-c
		logger.Info("stopped message processor")
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
	startCmd.Flags().Bool("trace-nats", false, "turn on lower level nats tracing")
	startCmd.Flags().String("dump-dir", "", "write each incoming message to this directory")
}
