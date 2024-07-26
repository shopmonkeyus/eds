// SOME CODE COPIED/MODIFIED FROM
// https://github.com/pingcap/tidb/blob/eafb78a9c6b8ff6f9c00188ca16fc63b41ae4e56/pkg/util/sqlexec/utils.go
//
// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"
	"unsafe"
)

// Slice converts string to slice without copy.
// Use at your own risk.
func slice(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

// escapeBytesBackslash will escape []byte into the buffer, with backslash.
func escapeBytesBackslash(buf []byte, v []byte) []byte {

	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// EscapeString is used by session/bootstrap.go, which has some
// dynamic query building cases not well handled by this package.
// For normal usage, please use EscapeSQL instead!
func EscapeString(s string) string {
	buf := make([]byte, 0, len(s))
	return string(escapeStringBackslash(buf, s))
}

// escapeStringBackslash will escape string into the buffer, with backslash.
func escapeStringBackslash(buf []byte, v string) []byte {
	return escapeBytesBackslash(buf, slice(v))
}

var looksLikeJSONTimestamp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d{1,})?Z$`)

func quoteValue(arg any) string {
	var buf []byte
	switch v := arg.(type) {
	case nil:
		return "NULL"
	case int:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int8:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int16:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int32:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int64:
		buf = strconv.AppendInt(buf, v, 10)
	case uint:
		buf = strconv.AppendUint(buf, uint64(v), 10)
	case uint8:
		buf = strconv.AppendUint(buf, uint64(v), 10)
	case uint16:
		buf = strconv.AppendUint(buf, uint64(v), 10)
	case uint32:
		buf = strconv.AppendUint(buf, uint64(v), 10)
	case uint64:
		buf = strconv.AppendUint(buf, v, 10)
	case float32:
		buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 32)
	case float64:
		buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
	case bool:
		buf = appendSQLArgBool(buf, v)
	case time.Time:
		if v.IsZero() {
			buf = append(buf, "'0000-00-00'"...)
		} else {
			buf = append(buf, '\'')
			buf = v.AppendFormat(buf, "2006-01-02 15:04:05.999999")
			buf = append(buf, '\'')
		}
	case *time.Time:
		if v.IsZero() {
			buf = append(buf, "'0000-00-00'"...)
		} else {
			buf = append(buf, '\'')
			buf = v.AppendFormat(buf, "2006-01-02 15:04:05.999999")
			buf = append(buf, '\'')
		}
	case json.RawMessage:
		buf = append(buf, '\'')
		buf = escapeBytesBackslash(buf, v)
		buf = append(buf, '\'')
	case []byte:
		if v == nil {
			buf = append(buf, "NULL"...)
		} else {
			buf = append(buf, "_binary'"...)
			buf = escapeBytesBackslash(buf, v)
			buf = append(buf, '\'')
		}
	case string:
		if looksLikeJSONTimestamp.MatchString(v) {
			tv, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				panic(fmt.Errorf("error parsing: %v. %w", v, err))
			}
			if tv.Year() < 1970 {
				// sqlserver timestamp range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.499999'
				tv = time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC)
			}
			buf = append(buf, '\'')
			buf = tv.AppendFormat(buf, "2006-01-02 15:04:05.999999")
			buf = append(buf, '\'')
		} else {
			buf = appendSQLArgString(buf, v)
		}
	case []string:
		for i, k := range v {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, '\'')
			buf = escapeStringBackslash(buf, k)
			buf = append(buf, '\'')
		}
	case []float32:
		for i, k := range v {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = strconv.AppendFloat(buf, float64(k), 'g', -1, 32)
		}
	case []float64:
		for i, k := range v {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = strconv.AppendFloat(buf, k, 'g', -1, 64)
		}
	case map[string]interface{}:

		jv, _ := json.Marshal(v)
		//buf = doubleSingleQuotes(jv)
		buf = append(buf, '\'')
		buf = escapeBytesBackslash(buf, jv)
		buf = append(buf, '\'')
	case []interface{}:
		jv, _ := json.Marshal(v)
		buf = append(buf, '\'')
		buf = escapeBytesBackslash(buf, jv)

		buf = append(buf, '\'')
	default:
		// slow path based on reflection
		reflectTp := reflect.TypeOf(arg)
		kind := reflectTp.Kind()
		switch kind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			buf = strconv.AppendInt(buf, reflect.ValueOf(arg).Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			buf = strconv.AppendUint(buf, reflect.ValueOf(arg).Uint(), 10)
		case reflect.Float32:
			buf = strconv.AppendFloat(buf, reflect.ValueOf(arg).Float(), 'g', -1, 32)
		case reflect.Float64:
			buf = strconv.AppendFloat(buf, reflect.ValueOf(arg).Float(), 'g', -1, 64)
		case reflect.Bool:
			buf = appendSQLArgBool(buf, reflect.ValueOf(arg).Bool())
		case reflect.String:
			buf = appendSQLArgString(buf, reflect.ValueOf(arg).String())
		default:
			panic(fmt.Errorf("unsupported argument: %v (%s)", arg, reflect.TypeOf(arg)))
		}
	}
	return string(buf)
}

func doubleSingleQuotes(input []byte) []byte {
	var result bytes.Buffer
	for _, b := range input {
		if b == '\'' {
			result.WriteByte('\'')
		}
		result.WriteByte(b)
	}
	return result.Bytes()
}

func appendSQLArgBool(buf []byte, v bool) []byte {
	if v {
		return append(buf, '1')
	}
	return append(buf, '0')
}

func appendSQLArgString(buf []byte, s string) []byte {
	buf = append(buf, '\'')
	buf = escapeStringBackslash(buf, s)
	buf = append(buf, '\'')
	return buf
}
