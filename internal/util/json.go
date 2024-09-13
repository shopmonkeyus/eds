package util

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type JSONDecoder interface {
	Decode(v any) error
	// More returns true if there are more items in the stream
	More() bool
	// Count returns the number of records read
	Count() int
	// Close a stream
	Close() error
}

type ndjsonReader struct {
	in    *os.File
	gr    *gzip.Reader
	dec   *json.Decoder
	count int
}

var _ JSONDecoder = (*ndjsonReader)(nil)

func (n *ndjsonReader) Count() int {
	return n.count
}

func (n *ndjsonReader) Close() error {
	if n.gr != nil {
		n.gr.Close()
		n.gr = nil
	}
	if n.in != nil {
		n.in.Close()
		n.in = nil
	}
	return nil
}

func (n *ndjsonReader) More() bool {
	return n.dec.More()
}

func (n *ndjsonReader) Decode(v any) error {
	if err := n.dec.Decode(v); err != nil {
		return err
	}
	n.count++
	return nil
}

// NewNDJSONDecoder returns a decoder which can be used to read JSON new line delimited files
func NewNDJSONDecoder(fn string) (JSONDecoder, error) {
	in, err := os.Open(fn)
	if err != nil {
		return nil, fmt.Errorf("error opening: %s. %w", fn, err)
	}
	var i io.Reader = in
	var gr *gzip.Reader
	if filepath.Ext(fn) == ".gz" {
		var err error
		gr, err = gzip.NewReader(in)
		if err != nil {
			return nil, fmt.Errorf("gzip: error opening: %s. %w", fn, err)
		}
		i = gr
	}
	je := json.NewDecoder(i)
	return &ndjsonReader{
		in:  in,
		gr:  gr,
		dec: je,
	}, nil
}

// JSONDiff returns the keys that are in obj but not in found in the slice
func JSONDiff(obj map[string]any, found []string) []string {
	diff := make([]string, 0)
	for key := range obj {
		if !SliceContains(found, key) {
			diff = append(diff, key)
		}
	}
	return diff
}
