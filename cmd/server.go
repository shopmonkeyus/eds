package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	glog "log"

	"github.com/gorilla/mux"
	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/provider"
	"github.com/shopmonkeyus/eds-server/internal/util"
	snats "github.com/shopmonkeyus/go-common/nats"
	csys "github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the server",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts, _ := cmd.Flags().GetStringSlice("server")
		creds, _ := cmd.Flags().GetString("creds")
		importer, _ := cmd.Flags().GetString("importer")
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		verbose, _ := cmd.Flags().GetBool("verbose")
		timestamp, _ := cmd.Flags().GetBool("timestamp")
		onlyRunNatsProvider, _ := cmd.Flags().GetBool("nats-provider")
		localNatsPort, _ := cmd.Flags().GetInt("port")
		healthCheckPort, _ := cmd.Flags().GetInt("health-port")
		duration, _ := cmd.Flags().GetString("consumer-start-time")

		if !timestamp {
			glog.SetFlags(0)
		}

		logger := newLogger(cmd)

		var (
			consumerStartTime time.Duration
			err               error
		)

		logger.Trace("consumer-start-time: %s", duration)
		if duration != "" {
			consumerStartTime, err = time.ParseDuration(duration)
			if err != nil {
				logger.Error("error: invalid duration: %s", err)
				os.Exit(1)
			}
			if consumerStartTime > time.Hour*168 {
				logger.Error("error: invalid duration. Max value is 168h")
				os.Exit(1)
			}
			logger.Trace("consumer-start-time parsed: %s", consumerStartTime)
		}
		logger.Trace("consumer-start-time: %s", consumerStartTime)

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

		defaultServerConfig := &server.Options{} // used for setting any defaults
		defaultServerConfig.Port = localNatsPort
		defaultServerConfig.MaxConn = -1
		defaultServerConfig.JetStream = true
		defaultServerConfig.StoreDir = "/var/lib/shopmonkey/eds-server"
		defaultServerConfig.JetStreamDomain = "leaf"
		//Create the store dir if it doesn't exist
		if _, err := os.Stat(defaultServerConfig.StoreDir); os.IsNotExist(err) {
			err = os.MkdirAll(defaultServerConfig.StoreDir, 0755)
			if err != nil {
				panic(err)
			}
		}
		serverConfig := &server.Options{}
		serverConfig, err = server.ProcessConfigFile("server.conf")
		if err != nil {
			serverConfig = defaultServerConfig
		}
		ns, err := server.NewServer(serverConfig)

		if err != nil {
			panic(err)
		}

		go ns.Start()
		readyForConnectionCounter := 0
		for !ns.ReadyForConnections(4 * time.Second) {
			logger.Info("Waiting for nats server to start...")
			readyForConnectionCounter++
			if readyForConnectionCounter > 10 {
				logger.Error("Local Nats server failed to start. Check to see if another instance is already running, and verify your server.conf file is configured properly. Exiting...")
				os.Exit(1)
			}
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

		err = natsProvider.AddHealthCheck()
		if err != nil {
			logger.Error("error adding health check: %s", err)
			os.Exit(1)
		}

		schemaModelVersionCache := make(map[string]dm.Model)

		//If we're importing data, we'll go ahead and pre-populate the schemas for all the files in the importer directory
		if importer != "" {
			files, err := util.ListDir(importer)
			if err != nil {
				logger.Error("error reading directory: %s", err)
				os.Exit(1)
			}
			for _, file := range files {
				tableName, err := util.GetTableNameFromPath(file)
				if err != nil {
					logger.Error("error getting table name from path: %s", err)
					os.Exit(1)
				}
				latestSchema, err := provider.GetLatestSchema(logger, nc, tableName)
				if err != nil {
					logger.Error("error getting latest schema: %s", err)
					os.Exit(1)
				}
				schemaModelVersionCache[tableName] = latestSchema
			}
		}
		var runLocalNatsCallback func([]internal.Provider) error = func(providers []internal.Provider) error {
			logger.Trace("creating message processor")
			processor, err := internal.NewMessageProcessor(internal.MessageProcessorOpts{
				Logger:                   logger,
				CompanyID:                companyIDs,
				Providers:                providers,
				NatsConnection:           nc,
				MainNatsConnection:       nc,
				TraceNats:                mustFlagBool(cmd, "trace-nats", false),
				DumpMessagesDir:          mustFlagString(cmd, "dump-dir", false),
				ConsumerPrefix:           mustFlagString(cmd, "consumer-prefix", false),
				ConsumerLookbackDuration: consumerStartTime,
				SchemaModelVersionCache:  &schemaModelVersionCache,
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
				Logger:                  logger,
				CompanyID:               companyIDs,
				Providers:               providers,
				NatsConnection:          localNatsServerConnection,
				MainNatsConnection:      nc,
				TraceNats:               mustFlagBool(cmd, "trace-nats", false),
				DumpMessagesDir:         mustFlagString(cmd, "dump-dir", false),
				ConsumerPrefix:          mustFlagString(cmd, "consumer-prefix", false),
				SchemaModelVersionCache: &schemaModelVersionCache,
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
		if len(urls) == 0 && !onlyRunNatsProvider {
			logger.Error("error: missing required url argument")
			os.Exit(1)
		}
		defer natsProvider.Stop()
		if err := natsProvider.Start(); err != nil {
			logger.Error("error starting nats provider: %s", err)
			os.Exit(1)
		}
		logger.Info("started nats provider")

		rtr := mux.NewRouter()
		port := healthCheckPort
		srv := &http.Server{
			Addr:           fmt.Sprintf(":%d", port),
			Handler:        rtr,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
			IdleTimeout:    15 * time.Minute,
			MaxHeaderBytes: 1 << 20,
		}
		srv.SetKeepAlivesEnabled(true)

		// health check route
		rtr.HandleFunc("/status/health", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("error starting server: %s", err)
				os.Exit(1)
			}
		}()

		runProviders(logger, urls, &schemaModelVersionCache, dryRun, verbose, importer, runProvidersCallback, nc)

		<-csys.CreateShutdownChannel()
		logger.Info("stopped nats provider")

		sctx, scancel := context.WithTimeout(ctx, time.Second*15)
		defer scancel()
		srv.Shutdown(sctx)
		wg.Wait()
		logger.Info("👋 Bye")
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
	serverCmd.Flags().Int("port", 4223, "the port to run the local NATS server on")
	serverCmd.Flags().Int("health-port", 8080, "the port to run the health check server on")
	serverCmd.Flags().String("consumer-start-time", "", "A duration string with unit suffix. Example: 1h45m. Max value is 168h")
}
