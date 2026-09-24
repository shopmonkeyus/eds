package util

import (
	"strconv"
	"strings"
	"time"

	"github.com/shopmonkeyus/eds/internal"
)

// MVCC versions arrive in two shapes: the CRDB changefeed emits "walltime.logical"
// while the importer emits pure integer nanos. Zero-padding both to a fixed width
// lets a lexical compare stand in for a numeric one without losing precision to
// float64 (a 19-digit nanos value overflows the float64 mantissa).
const (
	mvccWalltimeWidth = 20
	mvccLogicalWidth  = 10
)

// NormalizeMVCC converts a raw mvccTimestamp into a fixed-width, zero-padded
// "walltime+logical" string such that a > b lexically iff version(a) > version(b).
func NormalizeMVCC(raw string) string {
	if raw == "" {
		return ""
	}
	walltime, logical, _ := strings.Cut(raw, ".")
	return padLeft(walltime, mvccWalltimeWidth) + padLeft(logical, mvccLogicalWidth)
}

// EventVersion returns the comparable version string for an event, falling back
// to the event timestamp (ms, scaled to nanos) when the mvcc clock is absent.
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
