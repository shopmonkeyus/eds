package cmd

import (
	"os"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "migrate the database to the latest datamodel schema changes",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		glogger := internal.NewGormLogAdapter(logger)
		db := loadDatabase(cmd, &gorm.Config{Logger: glogger})
		started := time.Now()
		if err := internal.Migrate(glogger, db); err != nil {
			logger.Error("error running migrate: %s", err)
			os.Exit(1)
		}
		logger.Info("database schema is up-to-date, took %v", time.Since(started))
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
}
