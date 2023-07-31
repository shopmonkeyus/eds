package datatypes

import (
	"bytes"
	"compress/gzip"
	"io"
)

// Gunzip will unzip data and return buffer inline
func Gunzip(data []byte) (resData []byte, err error) {
	b := bytes.NewBuffer(data)

	var r io.Reader
	r, err = gzip.NewReader(b)
	if err != nil {
		return
	}

	var resB bytes.Buffer
	_, err = resB.ReadFrom(r)
	if err != nil {
		return
	}

	resData = resB.Bytes()
	return
}
