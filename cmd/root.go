package cmd

import (
	"fmt"
	"os"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/provider"
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

func runProvider(cmd *cobra.Command, logger internal.Logger, fn ProviderFunc) {
	url := mustFlagString(cmd, "url", true)
	provider, err := provider.NewProviderForURL(logger, url)
	if err != nil {
		logger.Error("error creating provider: %s", err)
		os.Exit(1)
	}
	if err := provider.Start(); err != nil {
		logger.Error("error starting provider: %s", err)
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

func newLogger(cmd *cobra.Command) internal.Logger {
	var logger internal.Logger = internal.Default
	if mustFlagBool(cmd, "verbose", false) {
		logger = internal.Tracer
	} else if mustFlagBool(cmd, "silent", false) {
		logger = internal.Discard
	}
	return logger
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "eds-server",
	Short: "",
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
