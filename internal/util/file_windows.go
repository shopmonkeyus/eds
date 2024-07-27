//go:build windows
// +build windows

package util

import (
	"fmt"
	"os"
)

// IsDirWritable checks if the directory is writable by the user
func IsDirWritable(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("stat: %w", err)
	}

	if !info.IsDir() {
		return false, fmt.Errorf("%s is not a directory", path)
	}

	// Check if the user bit is enabled in file permission
	if info.Mode().Perm()&(1<<(uint(7))) == 0 {
		return false, fmt.Errorf("write permission bit is not set for this user for %s", path)
	}

	return true, nil
}
