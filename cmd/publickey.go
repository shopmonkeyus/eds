package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var publicKeyCmd = &cobra.Command{
	Use:   "publickey",
	Short: "Print the Shopmonkey Public PGP Key",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(ShopmonkeyPublicPGPKey)
	},
}

func init() {
	rootCmd.AddCommand(publicKeyCmd)
}
