package main

import (
	"os"

	"github.com/shopmonkeyus/eds-server/cmd"
)

var version = "dev"

func main() {
	if version == "dev" {
		if v, ok := os.LookupEnv("GIT_SHA"); ok && v != "" {
			version = v
		}
	}
	cmd.Version = version
	cmd.Execute()
}
