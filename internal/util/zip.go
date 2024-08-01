package util

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
)

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
