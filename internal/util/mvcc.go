package util

import (
	"strconv"
	"strings"
	"time"

	"github.com/shopmonkeyus/eds/internal"
)

// Versions arrive in two shapes: the changefeed sends "walltime.logical", the
// importer sends whole nanoseconds. Padding both to a fixed width lets us
// compare them as text, which a float64 cannot do without losing digits.
const (
	mvccWalltimeWidth = 20
	mvccLogicalWidth  = 10
)

// NormalizeMVCC pads a raw mvccTimestamp so that comparing two of them as text
// gives the same answer as comparing the versions.
func NormalizeMVCC(raw string) string {
	if raw == "" {
		return ""
	}
	walltime, logical, _ := strings.Cut(raw, ".")
	return padLeft(walltime, mvccWalltimeWidth) + padLeft(logical, mvccLogicalWidth)
}

// EventVersion returns the version to compare an event on, falling back to its
// timestamp when there is no mvcc value.
func EventVersion(event *internal.DBChangeEvent) string {
	if event == nil {
		return ""
	}
	if event.MVCCTimestamp != "" {
		return NormalizeMVCC(event.MVCCTimestamp)
	}
	if event.Timestamp > 0 {
		nanos := strconv.FormatInt(event.Timestamp*int64(time.Millisecond), 10)
		return padLeft(nanos, mvccWalltimeWidth) + padLeft("", mvccLogicalWidth)
	}
	return ""
}

func padLeft(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return strings.Repeat("0", width-len(s)) + s
}
