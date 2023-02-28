package cmd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	snats "github.com/shopmonkeyus/go-common/nats"
	"github.com/spf13/cobra"
)

// Credentials file contains the company id and the nats credentials encoded as base64
type Credentials struct {
	CompanyID   string `json:"companyId"`
	Credentials string `json:"creds"`
}

func (c *Credentials) Load(fn string) error {
	of, err := os.Open(fn)
	if err != nil {
		return fmt.Errorf("error: open %s. %s", fn, err)
	}
	defer of.Close()
	dec := json.NewDecoder(of)
	return dec.Decode(c)
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the server",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger.Trace("connecting to nats server")

		hosts, _ := cmd.Flags().GetStringSlice("server")
		creds, _ := cmd.Flags().GetString("creds")

		if len(hosts) > 0 && creds == "" && strings.Contains(hosts[0], "nats.shopmonkey.cloud") {
			logger.Error("error: missing required credentials file. use --creds and specify the location of your credentials file")
			os.Exit(1)
		}

		natsurl := strings.Join(hosts, ",")

		var credentials Credentials
		var tmpfn string
		var natsCredentials nats.Option
		companyID, _ := cmd.Flags().GetString("company-id")

		if creds != "" {
			if _, err := os.Stat(creds); os.IsNotExist(err) {
				logger.Error("error: invalid credential file: %s", creds)
				os.Exit(1)
			}
			if err := credentials.Load(creds); err != nil {
				logger.Error("%s", err)
				os.Exit(1)
			}
			companyID = credentials.CompanyID
			sDec, err := base64.StdEncoding.DecodeString(credentials.Credentials)
			if err != nil {
				logger.Error("error: invalid credential file: %s. couldn't decoded credentials: %s", creds, err)
				os.Exit(1)
			}
			tmpfn = path.Join(os.TempDir(), fmt.Sprintf(".nats-creds-%v", time.Now().Unix()))
			if err := os.WriteFile(tmpfn, sDec, 0444); err != nil {
				logger.Error("error: writing temporary credentials file: %s", err)
				os.Exit(1)
			}
			fmt.Println(string(sDec))
			natsCredentials = nats.UserCredentials(tmpfn)
			defer os.Remove(tmpfn)
		}

		nc, err := snats.NewNats(logger, "eds-server-"+companyID, natsurl, natsCredentials)
		if err != nil {
			logger.Error("nats: %s", err)
			os.Exit(1)
		}
		if tmpfn != "" {
			os.Remove(tmpfn) // remove as soon as possible
		}
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
	startCmd.Flags().String("company-id", "", "the customer id")
	startCmd.Flags().MarkHidden("company-id") // only for testing, otherwise should come from the credentials file
	startCmd.Flags().Bool("trace-nats", false, "turn on lower level nats tracing")
	startCmd.Flags().String("dump-dir", "", "write each incoming message to this directory")
	startCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make db changes")
	startCmd.Flags().StringSlice("server", []string{"nats://nats.shopmonkey.cloud"}, "the nats server url")
	startCmd.Flags().String("creds", "", "the server credentials file provided by Shopmonkey")
	startCmd.Flags().String("consumer-prefix", "", "a consumer group prefix to add to the name")
}
