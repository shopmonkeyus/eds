//go:build !windows
// +build !windows

package util

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"syscall"
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

	var stat syscall.Stat_t
	if err = syscall.Stat(path, &stat); err != nil {
		return false, fmt.Errorf("sysstat: %w", err)
	}

	err = nil
	if uint32(os.Geteuid()) != stat.Uid {
		return false, fmt.Errorf("user doesn't have permission to write to %s", path)
	}

	return true, nil
}

func GzipFile(filepath string) error {
	infile, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer infile.Close()

	outfile, err := os.Create(filepath + ".gz")
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer outfile.Close()

	zr := gzip.NewWriter(outfile)
	defer zr.Close()
	_, err = io.Copy(zr, infile)
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return nil
}
