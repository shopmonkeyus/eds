package cmd

import (
	"fmt"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/spf13/cobra"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "migrate to the latest datamodel schema changes",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		started := time.Now()
		runProvider(cmd, logger, func(provider internal.Provider) error {
			if err := provider.Migrate(); err != nil {
				return fmt.Errorf("error running migrate: %s", err)
			}
			return nil
		})
		logger.Info("migration completed, took %v", time.Since(started))
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
}
