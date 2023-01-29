package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
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

		companyID, _ := cmd.Flags().GetString("company-id")
		if companyID == "" {
			logger.Error("error: missing required company id. use --company-id and specify the id for the company associated with the credentials")
			os.Exit(1)
		}
		hosts, _ := cmd.Flags().GetStringSlice("nats-server")
		creds, _ := cmd.Flags().GetString("nats-creds")

		if len(hosts) > 0 && creds == "" && strings.Contains(hosts[0], "nats.shopmonkey.cloud") {
			logger.Error("error: missing required credentials file. use --creds and specify the location of your credentials file")
			os.Exit(1)
		}

		if creds != "" {
			if _, err := os.Stat(creds); os.IsNotExist(err) {
				logger.Error("error: invalid credential file: %s", creds)
				os.Exit(1)
			}
		}

		natsurl := strings.Join(hosts, ",")
		natsOpts := make([]nats.Option, 0)
		natsOpts = append(natsOpts, nats.Name("customer-"+companyID))
		if creds != "" {
			natsOpts = append(natsOpts, nats.UserCredentials(creds))
		}
		nc, err := nats.Connect(natsurl, natsOpts...)
		if err != nil {
			logger.Error("error: nats connection: %s", err)
			os.Exit(1)
		}
		logger.Trace("connected to nats server")
		d, err := nc.RTT()
		if err != nil {
			nc.Close()
			logger.Error("error: nats connection rtt: %s", err)
			os.Exit(1)
		}
		logger.Info("server ping rtt: %vms, host: %s (%s)", d, nc.ConnectedUrl(), nc.ConnectedServerName())
		defer nc.Close()
		url := mustFlagString(cmd, "url", true)
		dryRun := mustFlagBool(cmd, "dry-run", false)
		runProvider(logger, url, dryRun, func(provider internal.Provider) error {
			logger.Trace("creating message processor")
			processor, err := internal.NewMessageProcessor(internal.MessageProcessorOpts{
				Logger:          logger,
				CompanyID:       companyID,
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
	startCmd.Flags().String("company-id", "", "the customer id") // TODO: remove this once we can encode this
	startCmd.Flags().Bool("trace-nats", false, "turn on lower level nats tracing")
	startCmd.Flags().String("dump-dir", "", "write each incoming message to this directory")
	startCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make db changes")
	startCmd.Flags().StringSlice("nats-server", []string{"nats://nats.shopmonkey.cloud:4222"}, "the nats server url")
	startCmd.Flags().String("nats-creds", "", "the nats server credentials file")
}
