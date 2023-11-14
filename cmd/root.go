package cmd

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"net/http"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/provider"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"
)

func mustFlagBool(cmd *cobra.Command, name string, required bool) bool {
	val, err := cmd.Flags().GetBool(name)
	if required && err != nil {
		fmt.Printf("error: %s\n", err)
		os.Exit(1)
	}
	return val
}

func mustFlagString(cmd *cobra.Command, name string, required bool) string {
	val, err := cmd.Flags().GetString(name)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		os.Exit(1)
	}
	if required && val == "" {
		fmt.Printf("error: required flag --%s missing\n", name)
		os.Exit(1)
	}
	return val
}

type ProviderFunc func(p internal.Provider) error

func runProvider(logger logger.Logger, url string, dryRun bool, verbose bool, importer string, fn ProviderFunc, nc *nats.Conn) {
	opts := &provider.ProviderOpts{
		DryRun:   dryRun,
		Verbose:  verbose,
		Importer: importer,
	}
	provider, err := provider.NewProviderForURL(logger, url, opts)
	if err != nil {
		logger.Error("error creating provider: %s", err)
		os.Exit(1)
	}
	if err := provider.Start(); err != nil {
		logger.Error("error starting provider: %s", err)
		os.Exit(1)
	}
	if importer != "" {
		resp, err := http.Get(opts.Importer)
		if err != nil {
			logger.Error("error from importer url request: %s", err)
			os.Exit(1)
		}
		if resp.StatusCode != http.StatusOK {
			logger.Error("invalid status code from importer url: %s", opts.Importer)
			os.Exit(1)
		}
		defer resp.Body.Close()

		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			logger.Error("error creating gzip reader: %s", err)
			os.Exit(1)
		}
		scanner := bufio.NewScanner(gzReader)
		const maxCapacity = 1024 * 1024
		scanner.Buffer(make([]byte, maxCapacity), maxCapacity)

		for scanner.Scan() {
			data := scanner.Bytes()

			err = provider.Import(data, nc)
			if err != nil {
				logger.Error("error importing data: %s", err)
				os.Exit(1)
			}
		}

		logger.Info("Importing data instead of streaming")
		os.Exit(1)
	}
	ferr := fn(provider)
	if ferr != nil {
		provider.Stop()
		logger.Error("error: %s", ferr)
		os.Exit(1)
	}
	if err := provider.Stop(); err != nil {
		logger.Error("error stopping provider: %s", err)
		os.Exit(1)
	}
}

func newLogger(cmd *cobra.Command) logger.Logger {
	return logger.NewConsoleLogger()
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:  "eds-server",
	Long: "Shopmonkey Enterprise Data Streaming server",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().String("url", "", "the connection string")
	rootCmd.PersistentFlags().Bool("verbose", false, "turn on verbose logging")
	rootCmd.PersistentFlags().Bool("silent", false, "turn off all logging")
}
