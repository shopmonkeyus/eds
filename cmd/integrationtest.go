package cmd

import (
	"fmt"

	"github.com/shopmonkeyus/eds/internal/integrationtest"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"
)

func RunWithLogAndRecover(fn func(cmd *cobra.Command, args []string, log logger.Logger)) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		log := newLogger(cmd)
		log.Info(fmt.Sprintf("starting integration test: %s", cmd.Name()))
		defer func() {
			if err := recover(); err != nil {
				log.Error("error running integration test: %s", err)
			}
		}()
		fn(cmd, args, log)
		log.Info(fmt.Sprintf("completed integration test: %s", cmd.Name()))
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
		count, _ := cmd.Flags().GetInt("count")

		js := integrationtest.NewConnection(&integrationtest.JetstreamConnection{})

		log.Info("sending %d customer messages", count)

		delivered := integrationtest.PublishRandomMessages(js, count, log)

		log.Info("sent %d customer messages to NATS", delivered)
	}),
}

var publishFileDataCmd = &cobra.Command{
	Use:   "publish-file-data",
	Short: "Publish test data to NATS from file",
	Long:  "Publishes dbchange events from JSON file to NATS\nTest data should be at path '..eds_test_data/output.jsonl.tar.gz'\n@jdavenport for details",
	Run: RunWithLogAndRecover(func(cmd *cobra.Command, args []string, log logger.Logger) {
		count, _ := cmd.Flags().GetInt("count")

		js := integrationtest.NewConnection(&integrationtest.JetstreamConnection{})

		path := "../eds_test_data/output.jsonl.tar.gz"
		delivered := integrationtest.PublishTestData(path, count, js, log)

		log.Info("sent %d messages", delivered)
	}),
}

func init() {
	loadTestRandomCmd.Flags().Int("count", 1, "Number of customer messages to send (default: 1)")

	publishFileDataCmd.Flags().Int("count", 1, "Number of messages to send (default: 1)")

	integrationtestCmd.AddCommand(loadTestRandomCmd)
	integrationtestCmd.AddCommand(publishFileDataCmd)

	rootCmd.AddCommand(integrationtestCmd)
}
