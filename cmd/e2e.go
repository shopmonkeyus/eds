//go:build e2e
// +build e2e

package cmd

import (
	"time"

	"github.com/shopmonkeyus/eds/internal/e2e"
	"github.com/spf13/cobra"

	_ "github.com/shopmonkeyus/eds/internal/drivers/eventhub"
	_ "github.com/shopmonkeyus/eds/internal/drivers/file"
	_ "github.com/shopmonkeyus/eds/internal/drivers/kafka"
	_ "github.com/shopmonkeyus/eds/internal/drivers/mysql"
	_ "github.com/shopmonkeyus/eds/internal/drivers/postgresql"
	_ "github.com/shopmonkeyus/eds/internal/drivers/s3"
	_ "github.com/shopmonkeyus/eds/internal/drivers/snowflake"
	_ "github.com/shopmonkeyus/eds/internal/drivers/sqlserver"
)

var e2eCmd = &cobra.Command{
	Use:   "e2e",
	Short: "Run the end to end test suite",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger = logger.WithPrefix("[e2e]")
		started := time.Now()
		if err := e2e.RunTests(logger, args); err != nil {
			logger.Fatal("error running tests: %s", err)
		}
		logger.Info("tests completed in %s", time.Since(started))
	},
}

func init() {
	rootCmd.AddCommand(e2eCmd)
}
