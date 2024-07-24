package main

import "github.com/shopmonkeyus/eds-server/cmd"

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	cmd.Version = version
	cmd.Execute()
}
