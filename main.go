package main

import (
	_ "embed"
	"os"

	"github.com/shopmonkeyus/eds/cmd"
)

var version = "dev"

//go:embed shopmonkey.asc
var shopmonkeyPublicPGPKey string

func main() {
	if version == "dev" {
		if v, ok := os.LookupEnv("GIT_SHA"); ok && v != "" {
			version = v
		}
	}
	cmd.Version = version
	cmd.ShopmonkeyPublicPGPKey = shopmonkeyPublicPGPKey
	cmd.Execute()
}
