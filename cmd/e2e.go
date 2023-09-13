//go:build e2e

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var allDrivers = []string{"postgresql", "cockroach", "sqlserver", "file"}

var e2eCmd = &cobra.Command{
	Use:   "e2e [driver]",
	Short: "Run the end-to-end test suite",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("not yet implemented")
	},
}

func init() {
	rootCmd.AddCommand(e2eCmd)
}
