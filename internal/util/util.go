package util

import (
	"encoding/json"
	"os"

	"strings"
)

func JSONStringify(val any) string {
	buf, _ := json.Marshal(val)
	return string(buf)
}

func ExtractCompanyIdFromSubscription(sub string) string {
	parts := strings.Split(sub, ".")
	if len(parts) > 3 {
		return parts[3]
	}
	return ""
}

// Exists returns true if the filename or directory specified by fn exists
func Exists(fn string) bool {
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		return false
	}
	return true
}
