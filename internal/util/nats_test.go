package util

import (
	"bytes"
	"compress/gzip"
	"testing"

	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack"
)

func TestDecodeNatsMessage(t *testing.T) {
	//  no encoding
	data := []byte(`{"name":"test"}`)
	m := nats.NewMsg("test")
	m.Data = data
	o := make(map[string]any)
	err := DecodeNatsMsg(m, &o)
	assert.Nil(t, err)
	assert.Equal(t, "test", o["name"])

	// gzip encoding
	var gzipBuf bytes.Buffer
	gz := gzip.NewWriter(&gzipBuf)
	_, err = gz.Write(data)
	assert.NoError(t, err)
	gz.Close()
	m.Data = gzipBuf.Bytes()
	m.Header.Set("content-encoding", "gzip/json")
	o = make(map[string]any)
	err = DecodeNatsMsg(m, &o)
	assert.Nil(t, err)
	assert.Equal(t, "test", o["name"])

	// msg pack encoding
	msgpackData, err := msgpack.Marshal(o)
	assert.NoError(t, err)
	m.Data = msgpackData
	m.Header.Set("content-encoding", "msgpack")
	o = make(map[string]any)
	err = DecodeNatsMsg(m, &o)
	assert.Nil(t, err)
	assert.Equal(t, "test", o["name"])
}
