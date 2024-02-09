package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	glog "log"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/provider"
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
		importer, _ := cmd.Flags().GetString("importer")
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
		var companyIDs []string

		companyName := "unknown"

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
			companyIDs = strings.Split(claim.Audience, ",")
			if len(companyIDs) == 0 {
				logger.Error("error: invalid JWT claim. missing audience")
				os.Exit(1)
			}
			companyName = claim.Name

		}
		//Nats connection to main NATS server
		nc, err := snats.NewNats(logger, "eds-server-"+companyName, natsurl, natsCredentials)
		if err != nil {
			logger.Error("nats: %s", err)
			os.Exit(1)
		}
		defer nc.Close()

		serverConfigJSON, err := os.ReadFile("server.conf")
		if err != nil {
			panic(err)
		}
		serverConfig := server.Options{}

		err = json.Unmarshal([]byte(serverConfigJSON), &serverConfig)
		if err != nil {
			panic(err)
		}

		ns, err := server.NewServer(&serverConfig)

		if err != nil {
			panic(err)
		}

		go ns.Start()

		for !ns.ReadyForConnections(4 * time.Second) {
			logger.Info("Waiting for nats server to start...")
		}

		logger.Info("Nats server started at url: %s", ns.ClientURL())
		//Create our own NATs server for the providers to read from
		opts := &provider.ProviderOpts{
			DryRun:   dryRun,
			Verbose:  verbose,
			Importer: importer,
		}
		natsProvider, err := provider.NewNatsProvider(logger, ns.ClientURL(), opts, nc)
		if err != nil {
			logger.Error("error creating nats provider: %s", err)
			os.Exit(1)
		}

		//Create a local NATs server (leaf-node) for the providers to read from
		localNatsServerConnection := natsProvider.GetNatsConn()
		modelVersionCache := make(map[string]dm.Model)
		var runLocalNatsCallback func([]internal.Provider) error = func(providers []internal.Provider) error {
			logger.Trace("creating message processor")
			processor, err := internal.NewMessageProcessor(internal.MessageProcessorOpts{
				Logger:            logger,
				CompanyID:         companyIDs,
				Providers:         providers,
				NatsConnection:    nc,
				TraceNats:         mustFlagBool(cmd, "trace-nats", false),
				DumpMessagesDir:   mustFlagString(cmd, "dump-dir", false),
				ConsumerPrefix:    mustFlagString(cmd, "consumer-prefix", false),
				ModelVersionCache: &modelVersionCache,
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
		go runLocalProvider(logger, natsProvider, runLocalNatsCallback, nc)

		var runProvidersCallback func([]internal.Provider) error = func(providers []internal.Provider) error {
			logger.Trace("creating message processor")
			processor, err := internal.NewMessageProcessor(internal.MessageProcessorOpts{
				Logger:            logger,
				CompanyID:         companyIDs,
				Providers:         providers,
				NatsConnection:    localNatsServerConnection,
				TraceNats:         mustFlagBool(cmd, "trace-nats", false),
				DumpMessagesDir:   mustFlagString(cmd, "dump-dir", false),
				ConsumerPrefix:    mustFlagString(cmd, "consumer-prefix", false),
				ModelVersionCache: &modelVersionCache,
			})
			if err != nil {
				return err
			}
			defer processor.Stop()

			logger.Trace("starting message processor to read from local nats")
			if err := processor.Start(); err != nil {
				return fmt.Errorf("processor start: %s", err)
			}
			logger.Info("started message processor")
			<-csys.CreateShutdownChannel()
			logger.Info("stopped message processor")
			return nil
		}

		urls := []string{}
		for _, url := range args {
			urls = append(urls, url)
		}
		if len(urls) == 0 {
			logger.Error("error: missing required url argument")
			os.Exit(1)
		}
		defer natsProvider.Stop()
		if err := natsProvider.Start(); err != nil {
			logger.Error("error starting nats provider: %s", err)
			os.Exit(1)
		}
		logger.Info("started nats provider")

		runProviders(logger, urls, dryRun, verbose, importer, runProvidersCallback, nc)
		<-csys.CreateShutdownChannel()
		logger.Info("stopped nats provider")
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.Flags().Bool("trace-nats", false, "turn on lower level nats tracing")
	serverCmd.Flags().String("dump-dir", "", "write each incoming message to this directory")
	serverCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make db changes")
	serverCmd.Flags().StringSlice("server", []string{"nats://connect.nats.shopmonkey.pub"}, "the nats server url")
	serverCmd.Flags().String("creds", "", "the server credentials file provided by Shopmonkey")
	serverCmd.Flags().String("consumer-prefix", "", "a consumer group prefix to add to the name")
	serverCmd.Flags().Bool("timestamp", false, "Add timestamps to logging")
	serverCmd.Flags().String("importer", "", "migrate data from your shopmonkey instance to your external database")
}
