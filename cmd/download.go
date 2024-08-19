package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/shirou/gopsutil/v4/host"
	"github.com/shopmonkeyus/eds-server/internal/upgrade"
	"github.com/spf13/cobra"
)

var downloadCmd = &cobra.Command{
	Use:   "download [version] [filename]",
	Short: "Download a new version of the server from the GitHub releases page",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger = logger.WithPrefix("[download]")
		Host, err := host.Info()
		if err != nil {
			logger.Fatal("error getting host information: %s", err)
		}
		version := args[0]
		filename := args[1]
		if !strings.HasPrefix(version, "v") {
			version = "v" + version
		}
		platform := strings.ToUpper(Host.Platform[0:1]) + Host.Platform[1:]
		arch := Host.KernelArch
		ext := "tar.gz"
		if Host.Platform == "windows" {
			ext = "zip"
		}
		binaryURL := fmt.Sprintf("https://github.com/shopmonkeyus/eds-server/releases/download/%s/eds-server_%s_%s.%s", version, platform, arch, ext)
		signatureURL := fmt.Sprintf("%s.sig", binaryURL)
		if err := upgrade.Upgrade(upgrade.UpgradeConfig{
			Logger:       logger,
			Context:      context.Background(),
			BinaryURL:    binaryURL,
			SignatureURL: signatureURL,
			Filename:     filename,
			PublicKey:    ShopmonkeyPublicPGPKey,
		}); err != nil {
			logger.Fatal("%s", err)
		}
		logger.Info("version %s download successful, saved to %s", version, filename)
	},
}

func init() {
	rootCmd.AddCommand(downloadCmd)
}
