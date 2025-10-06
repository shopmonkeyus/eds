package cmd

import (
<<<<<<< HEAD
=======
	"fmt"

>>>>>>> e034a66 (integration testing staging branch)
	"github.com/shopmonkeyus/eds/internal/integrationtest"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"
)

func RunWithLogAndRecover(fn func(cmd *cobra.Command, args []string, log logger.Logger)) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		log := newLogger(cmd)
<<<<<<< HEAD
=======
		log.Info(fmt.Sprintf("starting integration test: %s", cmd.Name()))
>>>>>>> e034a66 (integration testing staging branch)
		defer func() {
			if err := recover(); err != nil {
				log.Error("error running integration test: %s", err)
			}
		}()
		fn(cmd, args, log)
<<<<<<< HEAD
=======
		log.Info(fmt.Sprintf("completed integration test: %s", cmd.Name()))
>>>>>>> e034a66 (integration testing staging branch)
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

<<<<<<< HEAD
=======
var generateDatamodelApiCmd = &cobra.Command{
	Use:   "generate-datamodel-api",
	Short: "generate the datamodel from the shopmonkey api",
	Long:  "uses the shopmonkey api to generate go files containing the data model. This is der",
	Run: RunWithLogAndRecover(func(cmd *cobra.Command, args []string, log logger.Logger) {
		integrationtest.GenerateDatamodelFromApi(log)
	}),
}

var generateDatamodelDbCmd = &cobra.Command{
	Use:   "generate-datamodel-db",
	Short: "generate the datamodel from the database",
	Long:  "uses the database to generate go files containing the data model",
	Run: RunWithLogAndRecover(func(cmd *cobra.Command, args []string, log logger.Logger) {
		log.Info("Starting generate-datamodel-db")
		db := integrationtest.NewConnection(&integrationtest.CRDBConnection{})
		integrationtest.GenerateDatamodelFromDb(log, db)
		log.Info("Completed generating datamodel from database")
	}),
}

>>>>>>> e034a66 (integration testing staging branch)
var loadTestRandomCmd = &cobra.Command{
	Use:   "loadtest-random",
	Short: "Send random messages to nats",
	Long:  "Send random messages to nats for integration testing. Currently sends customer update messages",
	Run: RunWithLogAndRecover(func(cmd *cobra.Command, args []string, log logger.Logger) {
<<<<<<< HEAD
		log.Info("Starting load-random integration test")
=======
>>>>>>> e034a66 (integration testing staging branch)
		count, _ := cmd.Flags().GetInt("count")

		js := integrationtest.NewConnection(&integrationtest.JetstreamConnection{})

<<<<<<< HEAD
		log.Info("Sending %d customer messages", count)

		delivered := integrationtest.PublishRandomMessages(js, count, log)

		log.Info("Completed sending %d customer messages to NATS", delivered)
=======
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
>>>>>>> e034a66 (integration testing staging branch)
	}),
}

func init() {
<<<<<<< HEAD
	loadTestRandomCmd.Flags().Int("count", 1, "Number of customer messages to send (default: 1)")

	integrationtestCmd.AddCommand(loadTestRandomCmd)
=======
	loadTestRandomCmd.Flags().String("nats-url", "", "NATS server URL (default: nats://localhost:4222)")
	loadTestRandomCmd.Flags().Int("count", 1, "Number of customer messages to send (default: 1)")

	publishFileDataCmd.Flags().Int("count", 1, "Number of messages to send (default: 1)")

	integrationtestCmd.AddCommand(loadTestRandomCmd)
	integrationtestCmd.AddCommand(generateDatamodelApiCmd)
	integrationtestCmd.AddCommand(publishFileDataCmd)
	integrationtestCmd.AddCommand(generateDatamodelDbCmd)

>>>>>>> e034a66 (integration testing staging branch)
	rootCmd.AddCommand(integrationtestCmd)
}
