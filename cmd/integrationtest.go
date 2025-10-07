package cmd

import (
	"github.com/shopmonkeyus/eds/internal/integrationtest"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"
)

func RunWithLogAndRecover(fn func(cmd *cobra.Command, args []string, log logger.Logger)) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		log := newLogger(cmd)
		defer func() {
			if err := recover(); err != nil {
				log.Error("error running integration test: %s", err)
			}
		}()
		fn(cmd, args, log)
	}
}

var integrationtestCmd = &cobra.Command{
	Use:   "integrationtest",
	Short: "Run integration tests",
	Long:  "Run various integration tests for EDS components",
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		integrationtest.GlobalConnectionHandler.DisconnectAll()
	},
}

var loadTestRandomCmd = &cobra.Command{
	Use:   "loadtest-random",
	Short: "Send random messages to nats",
	Long:  "Send random messages to nats for integration testing. Currently sends customer update messages",
	Run: RunWithLogAndRecover(func(cmd *cobra.Command, args []string, log logger.Logger) {
		log.Info("Starting load-random integration test")
		count, _ := cmd.Flags().GetInt("count")

		js := integrationtest.NewConnection(&integrationtest.JetstreamConnection{})

		log.Info("Sending %d customer messages", count)

		delivered := integrationtest.PublishRandomMessages(js, count, log)

		log.Info("Completed sending %d customer messages to NATS", delivered)
	}),
}

func init() {
	loadTestRandomCmd.Flags().Int("count", 1, "Number of customer messages to send (default: 1)")

	integrationtestCmd.AddCommand(loadTestRandomCmd)
	rootCmd.AddCommand(integrationtestCmd)
}
