package cmd

import (
	"fmt"
	"os"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/spf13/cobra"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
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

func loadDatabase(cmd *cobra.Command, config *gorm.Config) *gorm.DB {
	driver := mustFlagString(cmd, "driver", true)
	dsn := mustFlagString(cmd, "dsn", true)
	var dialector gorm.Dialector
	// TODO: implement the other databases
	switch driver {
	case "postgres":
		dialector = postgres.Open(dsn)
	default:
		fmt.Printf("error: unsupported driver: %s\n", driver)
		os.Exit(1)
	}
	db, err := gorm.Open(dialector, config)
	if err != nil {
		fmt.Printf("error: connection to db: %s\n", err)
		os.Exit(1)
	}
	return db
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
	rootCmd.PersistentFlags().String("dsn", "", "the database connection settings")
	rootCmd.PersistentFlags().String("driver", "", "the database name")
	rootCmd.PersistentFlags().Bool("verbose", false, "turn on verbose logging")
	rootCmd.PersistentFlags().Bool("silent", false, "turn off all logging")
}
