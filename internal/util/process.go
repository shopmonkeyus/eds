package util

import "os"

// GetExecutable returns the path to the executable that is running the current process
func GetExecutable() string {
	ex, err := os.Executable()
	if err != nil {
		ex = os.Args[0]
	}
	return ex
}
