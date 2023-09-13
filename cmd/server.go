package cmd

import (
	"fmt"
	"os"
	"strings"
	"sync"

	glog "log"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	snats "github.com/shopmonkeyus/go-common/nats"
	csys "github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the server",
	Run: func(cmd *cobra.Command, args []string) {
		hosts, _ := cmd.Flags().GetStringSlice("server")
		creds, _ := cmd.Flags().GetString("creds")
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		verbose, _ := cmd.Flags().GetBool("verbose")
		timestamp, _ := cmd.Flags().GetBool("timestamp")

		if !timestamp {
			glog.SetFlags(0)
		}

		logger := newLogger(cmd)

		if len(hosts) > 0 && creds == "" && strings.Contains(hosts[0], "connect.nats.shopmonkey.pub") {
			logger.Error("error: missing required credentials file. use --creds and specify the location of your credentials file")
			os.Exit(1)
		}

		natsurl := strings.Join(hosts, ",")

		var natsCredentials nats.Option
		companyID, _ := cmd.Flags().GetString("company-id")

		if creds != "" {
			if _, err := os.Stat(creds); os.IsNotExist(err) {
				logger.Error("error: invalid credential file: %s", creds)
				os.Exit(1)
			}
			buf, err := os.ReadFile(creds)
			if err != nil {
				logger.Error("error: reading credentials file: %s", err)
				os.Exit(1)
			}
			natsCredentials = nats.UserCredentials(creds)

			natsJWT, err := jwt.ParseDecoratedJWT(buf)
			if err != nil {
				logger.Error("error: parsing valid JWT: %s", err)
				os.Exit(1)
			}
			claim, err := jwt.DecodeUserClaims(natsJWT)
			if err != nil {
				logger.Error("error: decoding JWT claims: %s", err)
				os.Exit(1)
			}
			companyID = claim.Audience
			if companyID == "" {
				logger.Error("error: invalid JWT claim. missing audience")
				os.Exit(1)
			}
		}

		nc, err := snats.NewNats(logger, "eds-server-"+companyID, natsurl, natsCredentials)
		if err != nil {
			logger.Error("nats: %s", err)
			os.Exit(1)
		}
		defer nc.Close()

		var runProviderCallback func(internal.Provider) error = func(provider internal.Provider) error {
			logger.Trace("creating message processor")
			processor, err := internal.NewMessageProcessor(internal.MessageProcessorOpts{
				Logger:          logger,
				CompanyID:       companyID,
				Provider:        provider,
				NatsConnection:  nc,
				TraceNats:       mustFlagBool(cmd, "trace-nats", false),
				DumpMessagesDir: mustFlagString(cmd, "dump-dir", false),
				ConsumerPrefix:  mustFlagString(cmd, "consumer-prefix", false),
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
			<-csys.CreateShutdownChannel()
			logger.Info("stopped message processor")
			return nil
		}

		var wg sync.WaitGroup
		for _, url := range args {
			wg.Add(1)
			go func(url string) {
				defer wg.Done()
				runProvider(logger, url, dryRun, verbose, runProviderCallback)
			}(url)
		}
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.Flags().String("company-id", "", "the customer id")
	serverCmd.Flags().MarkHidden("company-id") // only for testing, otherwise should come from the credentials file
	serverCmd.Flags().Bool("trace-nats", false, "turn on lower level nats tracing")
	serverCmd.Flags().String("dump-dir", "", "write each incoming message to this directory")
	serverCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make db changes")
	serverCmd.Flags().StringSlice("server", []string{"nats://connect.nats.shopmonkey.pub"}, "the nats server url")
	serverCmd.Flags().String("creds", "", "the server credentials file provided by Shopmonkey")
	serverCmd.Flags().String("consumer-prefix", "", "a consumer group prefix to add to the name")
	serverCmd.Flags().Bool("timestamp", false, "Add timestamps to logging")
}
