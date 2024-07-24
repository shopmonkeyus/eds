package util

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"regexp"

	"strings"
)

func JSONStringify(val any) string {
	buf, _ := json.Marshal(val)
	return string(buf)
}

// Exists returns true if the filename or directory specified by fn exists
func Exists(fn string) bool {
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		return false
	}
	return true
}

func SliceContains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

// ToFileURI converts a directory and file to a file URI in a cross-platform way.
func ToFileURI(dir string, file string) string {
	absDir := filepath.Clean(dir)
	if os.PathSeparator == '\\' {
		// if windows replace the backslashes
		return fmt.Sprintf("file://%s/%s", strings.ReplaceAll(absDir, "\\", "/"), file)
	}
	return fmt.Sprintf("file://%s/%s", absDir, file)
}

// IsLocalhost returns true if the URL is localhost or 127.0.0.1
func IsLocalhost(url string) bool {
	return strings.Contains(url, "localhost") || strings.Contains(url, "127.0.0.1")
}

// GetFreePort asks the kernel for a free open port that is ready to use.
func GetFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return
}

// ListDir will return an array of files recursively walking into sub directories
func ListDir(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for _, file := range files {
		if file.IsDir() {
			newres, err := ListDir(filepath.Join(dir, file.Name()))
			if err != nil {
				return nil, err
			}
			res = append(res, newres...)
		} else {
			if file.Name() == ".DS_Store" {
				continue
			}
			res = append(res, filepath.Join(dir, file.Name()))
		}
	}
	return res, nil
}

type ExportFileInformation struct {
	Table     string `json:"table"`
	Timestamp string `json:"timestamp"`
}

func ParseMVCCTimestamp(timestamp string) (*big.Int, error) {
	// remove decimal
	ts := strings.ReplaceAll(timestamp, ".", "")
	return ParseBigIntTimestamp(ts)
}

func ParseBigIntTimestamp(timestamp string) (*big.Int, error) {
	i, ok := big.NewInt(int64(0)).SetString(timestamp, 10)
	if !ok {
		return nil, fmt.Errorf("unable to parse timestamp %s", timestamp)
	}
	return i, nil
}

// BigIntIsLess returns true if a is less than b
func BigIntIsLess(a, b *big.Int) bool {
	return a.Cmp(b) < 0
}

func (e *ExportFileInformation) Less(other *ExportFileInformation) bool {
	return strings.Compare(e.Timestamp, other.Timestamp) < 0
}

// https://www.cockroachlabs.com/docs/v24.1/create-changefeed#general-file-format
// /[date]/[timestamp]-[uniquer]-[topic]-[schema-id]
var crdbExportFileRegex = regexp.MustCompile(`^(\d+)-\w+-[\w-]+-([a-z0-9_]+)-(\w+)\.ndjson\.gz`)

func ParseCRDBExportFile(file string) (*ExportFileInformation, bool) {
	filename := filepath.Base(file)
	if !crdbExportFileRegex.MatchString(filename) {
		return nil, false
	}
	matches := crdbExportFileRegex.FindStringSubmatch(filename)
	return &ExportFileInformation{
		Table:     matches[2],
		Timestamp: matches[1],
	}, true
}
